use crate::{
    catalog::{Catalog, CatalogRef},
    executor::{Executor, ExecutorResult},
    parser::{Parser, ParserResult, Statement, TransactionControl},
    planner::{plan, TypedPlanNode},
    rows::{Column, Row, Rows},
    storage::{KeyValueStore, Transaction},
    Database, Result, Type,
};

pub struct Connection<'a, T: KeyValueStore> {
    db: &'a Database<T>,
    txn_status: TransactionState<'a, T>,
}

impl<'a, T: KeyValueStore> Connection<'a, T> {
    pub(crate) fn new(db: &'a Database<T>) -> Self {
        Self {
            db,
            txn_status: TransactionState::Inactive,
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
        if let Statement::Transaction(txn_control) = statement {
            self.handle_transaction_control(txn_control)?;
            return Ok(Rows::empty());
        }
        let mut implicit_txn = None;
        let txn = match &self.txn_status {
            TransactionState::Active(txn) => txn,
            TransactionState::Aborted => return Err(TransactionError::TransactionAborted.into()),
            TransactionState::Inactive => implicit_txn.insert(self.db.storage.transaction()),
        };
        let ctx = QueryContext {
            txn,
            catalog: &self.db.catalog,
        };
        let plan = plan(&ctx, statement)?;
        match execute_plan(&ctx, plan) {
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
}

fn execute_plan<T: KeyValueStore>(
    ctx: &QueryContext<'_, '_, T>,
    plan: TypedPlanNode<T>,
) -> Result<Rows> {
    let columns: Vec<_> = plan
        .columns()
        .iter()
        .map(|column| {
            Column {
                name: column.name.clone(),
                ty: column.ty.unwrap_or(Type::Integer), // Arbitrarily choose integer
            }
        })
        .collect();
    let num_columns = plan.columns().len();
    let rows = Executor::new(ctx, plan.into_node())?
        .map(|columns| {
            columns.map(|columns| {
                assert_eq!(columns.len(), num_columns);
                Row { columns }
            })
        })
        .collect::<ExecutorResult<Vec<_>>>()?;
    Ok(Rows {
        iter: rows.into_iter(),
        columns,
    })
}

pub struct QueryContext<'txn, 'db, T: KeyValueStore> {
    txn: &'txn Transaction<'db, T>,
    catalog: &'db Catalog<T>,
}

impl<'txn, 'db, T: KeyValueStore> QueryContext<'txn, 'db, T> {
    pub fn transaction(&self) -> &'txn Transaction<'db, T> {
        self.txn
    }

    pub fn catalog(&self) -> CatalogRef<'txn, 'db, T> {
        self.catalog.with(self.txn)
    }
}

enum TransactionState<'a, T: KeyValueStore> {
    /// We are in an explicit transaction started with BEGIN.
    Active(Transaction<'a, T>),

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
