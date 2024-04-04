use crate::{
    catalog::CatalogRef,
    executor::Executor,
    parser::{Deallocate, Expression, Parser, Statement, TransactionControl},
    planner::{self, Plan},
    rows::{Column, Rows},
    storage::{Storage, Transaction},
    types::{NullableType, Params},
    Database, Error, Result, Row, Type,
};
use fastrand::Rng;
use std::{cell::RefCell, collections::HashMap};

pub struct Connection<'db, T: Storage> {
    db: &'db Database<T>,
    txn_status: RefCell<TransactionState<'db, T>>,
    prepared_statements: RefCell<HashMap<String, Statement>>,
    rng: RefCell<Rng>,
}

impl<'db, T: Storage> Connection<'db, T> {
    pub(crate) fn new(db: &'db Database<T>) -> Self {
        Self {
            db,
            txn_status: TransactionState::Inactive.into(),
            prepared_statements: HashMap::new().into(),
            rng: Rng::new().into(),
        }
    }

    /// Execute a single SQL statement.
    ///
    /// On success, returns the number of rows that were changed, inserted,
    /// or deleted.
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> Result<usize> {
        self.prepare(sql)?.execute(params)
    }

    /// Execute a single SQL query, returning the resulting rows.
    pub fn query<P: Params>(&self, sql: &str, params: P) -> Result<Rows> {
        self.prepare(sql)?.query(params)
    }

    /// Prepare a single SQL statement.
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement<'_, 'db, T>> {
        let mut parser = Parser::new(sql);
        let statement = parser.next().transpose()?.ok_or(Error::NoStatement)?;
        if parser.next().is_some() {
            return Err(Error::MultipleStatements);
        }
        Ok(PreparedStatement {
            conn: self,
            statement,
        })
    }

    fn execute_statement(&self, statement: Statement, params: Vec<Expression>) -> Result<Rows> {
        match statement {
            Statement::Prepare(prepare) => {
                // Perform only parsing and no planning for now.
                self.prepared_statements
                    .borrow_mut()
                    .insert(prepare.name, *prepare.statement);
                return Ok(Rows::empty());
            }
            Statement::Execute(execute) => {
                let prepared_statement = self
                    .prepared_statements
                    .borrow()
                    .get(&execute.name)
                    .ok_or_else(|| Error::UnknownPreparedStatement(execute.name))?
                    .clone();
                return self.execute_statement(prepared_statement, execute.params);
            }
            Statement::Deallocate(deallocate) => match deallocate {
                Deallocate::All => {
                    self.prepared_statements.borrow_mut().clear();
                    return Ok(Rows::empty());
                }
                Deallocate::Name(name) => {
                    return if self
                        .prepared_statements
                        .borrow_mut()
                        .remove(&name)
                        .is_some()
                    {
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
        let mut txn_status = self.txn_status.borrow_mut();
        let txn = match &*txn_status {
            TransactionState::Active(txn) => txn,
            TransactionState::Aborted => return Err(TransactionError::TransactionAborted.into()),
            TransactionState::Inactive => implicit_txn.insert(self.db.storage.transaction()),
        };

        let catalog = self.db.catalog.with(txn);
        let ctx = ConnectionContext::new(&catalog, &self.rng);

        let mut param_values = Vec::with_capacity(params.len());
        for expr in params {
            param_values.push(planner::bind_expr(&ctx, expr)?.eval(&ctx, &Row::empty())?);
        }

        let plan = planner::plan(&ctx, statement, param_values)?;
        match execute_plan(&ctx, plan) {
            Ok(rows) => {
                if let Some(txn) = implicit_txn {
                    txn.commit(); // Auto commit
                }
                Ok(rows)
            }
            Err(e) => {
                if implicit_txn.is_none() {
                    *txn_status = TransactionState::Aborted;
                }
                Err(e)
            }
        }
    }

    fn handle_transaction_control(
        &self,
        txn_control: TransactionControl,
    ) -> std::result::Result<(), TransactionError> {
        let mut txn_status = self.txn_status.borrow_mut();
        match (&*txn_status, txn_control) {
            (TransactionState::Active(_), TransactionControl::Begin) => {
                Err(TransactionError::NestedTransaction)
            }
            (TransactionState::Active(_), TransactionControl::Commit) => {
                match std::mem::replace(&mut *txn_status, TransactionState::Inactive) {
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
                *txn_status = TransactionState::Inactive;
                Ok(())
            }
            (TransactionState::Aborted, TransactionControl::Begin) => {
                Err(TransactionError::TransactionAborted)
            }
            (TransactionState::Inactive, TransactionControl::Begin) => {
                *txn_status = TransactionState::Active(self.db.storage.transaction());
                Ok(())
            }
            (
                TransactionState::Inactive,
                TransactionControl::Commit | TransactionControl::Rollback,
            ) => Err(TransactionError::NoActiveTransaction),
        }
    }
}

fn execute_plan<'db, T: Storage>(
    ctx: &ConnectionContext<'_, 'db, T>,
    plan: Plan<'_, 'db, T>,
) -> Result<Rows> {
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
    let executor = Executor::new(ctx, node)?;
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

pub struct PreparedStatement<'conn, 'db, T: Storage> {
    conn: &'conn Connection<'db, T>,
    statement: Statement,
}

impl<T: Storage> PreparedStatement<'_, '_, T> {
    pub fn execute<P: Params>(&self, params: P) -> Result<usize> {
        let is_modification = self.statement.is_modification();
        let params = params
            .into_values()
            .into_iter()
            .map(Expression::Constant)
            .collect();
        let mut rows = self
            .conn
            .execute_statement(self.statement.clone(), params)?;
        let num_affected_rows = if is_modification {
            rows.next().map_or(0, |row| row.get(0).unwrap())
        } else {
            0
        };
        Ok(num_affected_rows)
    }

    pub fn query<P: Params>(&self, params: P) -> Result<Rows> {
        let is_modification = self.statement.is_modification();
        let params = params
            .into_values()
            .into_iter()
            .map(Expression::Constant)
            .collect();
        let rows = self
            .conn
            .execute_statement(self.statement.clone(), params)?;
        Ok(if is_modification { Rows::empty() } else { rows })
    }
}

pub struct ConnectionContext<'txn, 'db, T: Storage> {
    catalog: &'txn CatalogRef<'txn, 'db, T>,
    rng: &'txn RefCell<Rng>,
}

impl<'txn, 'db: 'txn, T: Storage> ConnectionContext<'txn, 'db, T> {
    pub fn new(catalog: &'txn CatalogRef<'txn, 'db, T>, rng: &'txn RefCell<Rng>) -> Self {
        Self { catalog, rng }
    }

    pub fn catalog(&self) -> &CatalogRef<'txn, 'db, T> {
        self.catalog
    }

    pub fn random(&self) -> f64 {
        self.rng.borrow_mut().f64()
    }

    pub fn set_seed(&self, seed: f64) {
        self.rng.borrow_mut().seed(seed.to_bits());
    }
}
