use crate::{
    executor::{Executor, ExecutorContext},
    parser::{Deallocate, Expression, Parser, ParserResult, Statement, TransactionControl},
    planner::{self, Plan},
    rows::{Column, Rows},
    storage::{Storage, Transaction},
    types::NullableType,
    Database, Error, Result, Row, Type,
};
use std::collections::HashMap;

pub struct Connection<'a, T: Storage> {
    db: &'a Database<T>,
    txn_status: TransactionState<'a, T>,
    prepared_statements: HashMap<String, Statement>,
}

impl<'a, T: Storage> Connection<'a, T> {
    pub(crate) fn new(db: &'a Database<T>) -> Self {
        Self {
            db,
            txn_status: TransactionState::Inactive,
            prepared_statements: HashMap::new(),
        }
    }

    pub fn execute(&mut self, sql: &str) -> Result<()> {
        let parser = Parser::new(sql);
        for statement in parser {
            self.execute_statement(statement?)?;
        }
        Ok(())
    }

    pub fn query(&mut self, sql: &str) -> Result<Rows> {
        let mut statements = Parser::new(sql).collect::<ParserResult<Vec<_>>>()?;
        let Some(last_statement) = statements.pop() else {
            return Ok(Rows::empty());
        };
        for statement in statements {
            self.execute_statement(statement)?;
        }
        self.execute_statement(last_statement)
    }

    fn execute_statement(&mut self, statement: Statement) -> Result<Rows> {
        self.execute_statement_with_params(statement, Vec::new())
    }

    fn execute_statement_with_params(
        &mut self,
        statement: Statement,
        params: Vec<Expression>,
    ) -> Result<Rows> {
        match statement {
            Statement::Prepare(prepare) => {
                // Perform only parsing and no planning for now.
                self.prepared_statements
                    .insert(prepare.name, *prepare.statement);
                return Ok(Rows::empty());
            }
            Statement::Execute(execute) => {
                let prepared_statement = self
                    .prepared_statements
                    .get(&execute.name)
                    .ok_or_else(|| Error::UnknownPreparedStatement(execute.name))?;
                return self
                    .execute_statement_with_params(prepared_statement.clone(), execute.params);
            }
            Statement::Deallocate(deallocate) => match deallocate {
                Deallocate::All => {
                    self.prepared_statements.clear();
                    return Ok(Rows::empty());
                }
                Deallocate::Name(name) => {
                    return if self.prepared_statements.remove(&name).is_some() {
                        Ok(Rows::empty())
                    } else {
                        Err(Error::UnknownPreparedStatement(name))
                    };
                }
            },
            Statement::Transaction(txn_control) => {
                self.handle_transaction_control(txn_control)?;
                return Ok(Rows::empty());
            }
            _ => (),
        }

        let mut implicit_txn = None;
        let txn = match &self.txn_status {
            TransactionState::Active(txn) => txn,
            TransactionState::Aborted => return Err(TransactionError::TransactionAborted.into()),
            TransactionState::Inactive => implicit_txn.insert(self.db.storage.transaction()),
        };

        let catalog = self.db.catalog.with(txn);

        let mut param_values = Vec::with_capacity(params.len());
        for expr in params {
            param_values.push(planner::bind_expr(&catalog, expr)?.eval(&Row::empty())?);
        }

        let plan = planner::plan(&catalog, statement, param_values)?;
        match self.execute_plan(txn, plan) {
            Ok(rows) => {
                if let Some(txn) = implicit_txn {
                    txn.commit(); // Auto commit
                }
                Ok(rows)
            }
            Err(e) => {
                if implicit_txn.is_none() {
                    self.txn_status = TransactionState::Aborted;
                }
                Err(e)
            }
        }
    }

    fn handle_transaction_control(
        &mut self,
        txn_control: TransactionControl,
    ) -> std::result::Result<(), TransactionError> {
        match (&self.txn_status, txn_control) {
            (TransactionState::Active(_), TransactionControl::Begin) => {
                Err(TransactionError::NestedTransaction)
            }
            (TransactionState::Active(_), TransactionControl::Commit) => {
                match std::mem::replace(&mut self.txn_status, TransactionState::Inactive) {
                    TransactionState::Active(txn) => txn.commit(),
                    _ => unreachable!(),
                }
                Ok(())
            }
            (TransactionState::Active(_), TransactionControl::Rollback)
            | (
                TransactionState::Aborted,
                TransactionControl::Commit | TransactionControl::Rollback,
            ) => {
                self.txn_status = TransactionState::Inactive;
                Ok(())
            }
            (TransactionState::Aborted, TransactionControl::Begin) => {
                Err(TransactionError::TransactionAborted)
            }
            (TransactionState::Inactive, TransactionControl::Begin) => {
                self.txn_status = TransactionState::Active(self.db.storage.transaction());
                Ok(())
            }
            (
                TransactionState::Inactive,
                TransactionControl::Commit | TransactionControl::Rollback,
            ) => Err(TransactionError::NoActiveTransaction),
        }
    }

    fn execute_plan(&self, txn: &T::Transaction<'a>, plan: Plan<'_, 'a, T>) -> Result<Rows> {
        let Plan { node, schema } = plan;
        let columns: Vec<_> = schema
            .0
            .into_iter()
            .map(|column| {
                Column {
                    name: column.column_name,
                    ty: match column.ty {
                        NullableType::NonNull(ty) => ty,
                        NullableType::Null => Type::Integer, // Arbitrarily choose INTEGER
                    },
                }
            })
            .collect();
        let ctx = ExecutorContext::new(self.db.catalog.with(txn));
        let executor = Executor::new(&ctx, node)?;
        let mut rows = Vec::new();
        for row in executor {
            let row = row?;
            assert_eq!(row.columns().len(), columns.len());
            for (value, column) in row.columns().iter().zip(&columns) {
                assert!(value.ty().is_compatible_with(column.ty));
            }
            rows.push(row);
        }
        Ok(Rows {
            iter: rows.into_iter(),
            columns,
        })
    }
}

enum TransactionState<'a, T: Storage + 'a> {
    /// We are in an explicit transaction started with BEGIN.
    Active(T::Transaction<'a>),

    /// The explicit transaction has been aborted and waiting for
    /// COMMIT or ROLLBACK.
    Aborted,

    /// There is no active explicit transaction. We are in auto-commit mode.
    Inactive,
}

#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
    #[error("Cannot begin a transaction within a transaction")]
    NestedTransaction,

    #[error("No active transaction")]
    NoActiveTransaction,

    #[error("The current transaction has been aborted. Commands are ignored until end of transaction block.")]
    TransactionAborted,
}
