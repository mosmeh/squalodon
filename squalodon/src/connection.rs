use crate::{
    catalog::{self, Catalog, CatalogRef},
    executor::Executor,
    parser::{Deallocate, Expression, Parser, Statement, TransactionControl},
    planner::{self, Plan},
    rows::{self, Rows},
    storage::{Storage, Transaction},
    types::{NullableType, Params},
    Database, Error, ExecutorError, Result, Row, Type,
};
use fastrand::Rng;
use std::{cell::RefCell, collections::HashMap};

pub struct Connection<'a, T: Storage> {
    db: &'a Database<'a, T>,
    txn_status: RefCell<TransactionState<T::Transaction<'a>>>,
    prepared_statements: RefCell<HashMap<String, Statement>>,
    rng: RefCell<Rng>,
}

impl<'a, T: Storage> Connection<'a, T> {
    pub(crate) fn new(db: &'a Database<'a, T>) -> Self {
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
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement<'_, 'a, T>> {
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

    /// Creates a new inserter for the table.
    ///
    /// The inserter works in a new transaction separate from the connection's
    /// current transaction. The transaction is committed when the inserter is
    /// dropped.
    ///
    /// When any insert fails, the transaction is rolled back and the inserter
    /// is marked as failed. Further inserts return errors immediately.
    pub fn inserter(&self, table: &str) -> Result<Inserter<'_, T::Transaction<'a>>> {
        let table_name = table.to_owned();
        let txn = self.db.storage.transaction();
        let catalog = self.db.catalog.with(&txn);
        let table = catalog.table(&table_name)?;
        let columns = table.columns().to_vec();
        Ok(Inserter {
            txn: Some(txn),
            catalog: &self.db.catalog,
            table_name,
            columns,
            rows: Vec::new(),
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
            Statement::Deallocate(deallocate) => {
                let mut prepared_statements = self.prepared_statements.borrow_mut();
                match deallocate {
                    Deallocate::All => prepared_statements.clear(),
                    Deallocate::Name(name) => {
                        if prepared_statements.remove(&name).is_none() {
                            return Err(Error::UnknownPreparedStatement(name));
                        }
                    }
                }
                return Ok(Rows::empty());
            }
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
            param_values.push(planner::plan_expr(&ctx, expr)?.eval(&ctx, &Row::empty())?);
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

fn execute_plan<T: Transaction>(ctx: &ConnectionContext<'_, T>, plan: Plan<'_, T>) -> Result<Rows> {
    let Plan { node, schema } = plan;
    let columns: Vec<_> = schema
        .into_iter()
        .map(|column| {
            rows::Column {
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

enum TransactionState<T> {
    /// We are in an explicit transaction started with BEGIN.
    Active(T),

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
        let is_mutation = self.statement.is_mutation();
        let params = params
            .into_values()
            .into_iter()
            .map(Expression::Constant)
            .collect();
        let mut rows = self
            .conn
            .execute_statement(self.statement.clone(), params)?;
        let num_affected_rows = if is_mutation {
            rows.next().unwrap().get(0).unwrap()
        } else {
            0
        };
        Ok(num_affected_rows)
    }

    pub fn query<P: Params>(&self, params: P) -> Result<Rows> {
        let is_mutation = self.statement.is_mutation();
        let params = params
            .into_values()
            .into_iter()
            .map(Expression::Constant)
            .collect();
        let rows = self
            .conn
            .execute_statement(self.statement.clone(), params)?;
        Ok(if is_mutation { Rows::empty() } else { rows })
    }
}

pub struct Inserter<'a, T: Transaction> {
    txn: Option<T>,
    catalog: &'a Catalog<T>,
    table_name: String,
    columns: Vec<catalog::Column>,
    rows: Vec<Row>,
}

impl<T: Transaction> Inserter<'_, T> {
    pub fn insert<P: Params>(&mut self, params: P) -> Result<()> {
        self.try_operation(|inserter| inserter.insert_inner(params))
    }

    fn insert_inner<P: Params>(&mut self, params: P) -> Result<()> {
        let values = params.into_values();
        if values.len() != self.columns.len() {
            return Err(Error::ParameterCountMismatch {
                expected: self.columns.len(),
                actual: values.len(),
            });
        }
        let mut row = Vec::new();
        for (value, column) in values.into_iter().zip(&self.columns) {
            let value = value
                .cast(column.ty)
                .ok_or(Error::Executor(ExecutorError::TypeError))?;
            row.push(value);
        }
        self.rows.push(Row::new(row));

        // Arbitrary threshold
        if self.rows.len() >= 16384 {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.try_operation(Self::flush_inner)
    }

    fn flush_inner(&mut self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let txn = self.txn.as_ref().unwrap();
        let table = self.catalog.with(txn).table(&self.table_name)?;
        for row in std::mem::take(&mut self.rows) {
            table.insert(&row)?;
        }
        Ok(())
    }

    fn try_operation(&mut self, op: impl FnOnce(&mut Self) -> Result<()>) -> Result<()> {
        if self.txn.is_none() {
            return Err(Error::Transaction(TransactionError::TransactionAborted));
        }
        match op(self) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.txn.take();
                self.rows.clear();
                Err(e)
            }
        }
    }
}

impl<T: Transaction> Drop for Inserter<'_, T> {
    fn drop(&mut self) {
        let _ = self.flush();
        if let Some(txn) = self.txn.take() {
            txn.commit();
        }
    }
}

pub struct ConnectionContext<'a, T> {
    catalog: &'a CatalogRef<'a, T>,
    rng: &'a RefCell<Rng>,
}

impl<'a, T> ConnectionContext<'a, T> {
    pub fn new(catalog: &'a CatalogRef<'a, T>, rng: &'a RefCell<Rng>) -> Self {
        Self { catalog, rng }
    }

    pub fn catalog(&self) -> &CatalogRef<'a, T> {
        self.catalog
    }

    pub fn random(&self) -> f64 {
        self.rng.borrow_mut().f64()
    }

    pub fn set_seed(&self, seed: f64) {
        self.rng.borrow_mut().seed(seed.to_bits());
    }
}
