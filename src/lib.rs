pub mod storage;

mod catalog;
mod executor;
mod memcomparable;
mod parser;
mod planner;
mod types;

pub use catalog::CatalogError;
pub use executor::ExecutorError;
pub use parser::{LexerError, ParserError};
pub use planner::PlannerError;
pub(crate) use storage::StorageError;
pub use types::{TryFromValueError, Type, Value};

use catalog::{Catalog, CatalogRef};
use executor::{Executor, ExecutorResult};
use parser::{Parser, ParserResult, Statement, TransactionControl};
use storage::{KeyValueStore, Storage, Transaction};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Parser error: {0}")]
    Parse(#[from] ParserError),

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),

    #[error("Planner error: {0}")]
    Planner(#[from] PlannerError),

    #[error("Executor error: {0}")]
    Executor(#[from] ExecutorError),

    #[error("Transaction error: {0}")]
    Transaction(#[from] TransactionError),

    #[error("Invalid encoding")]
    InvalidEncoding,

    #[error("Column index out of range")]
    ColumnIndexOutOfRange,

    #[error(transparent)]
    TryFromValue(#[from] TryFromValueError),
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Database<T: KeyValueStore> {
    storage: Storage<T>,
    catalog: Catalog<T>,
}

impl<T: KeyValueStore> Database<T> {
    pub fn new(storage: T) -> Result<Self> {
        let catalog = Catalog::load(&storage)?;
        Ok(Self {
            storage: Storage::new(storage),
            catalog,
        })
    }

    pub fn connect(&self) -> Connection<T> {
        Connection {
            db: self,
            txn_status: TransactionState::Inactive,
        }
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

struct QueryContext<'txn, 'db, T: KeyValueStore> {
    txn: &'txn Transaction<'db, T>,
    catalog: &'db Catalog<T>,
}

impl<'txn, 'db, T: KeyValueStore> QueryContext<'txn, 'db, T> {
    fn transaction(&self) -> &'txn Transaction<'db, T> {
        self.txn
    }

    fn catalog(&self) -> CatalogRef<'txn, 'db, T> {
        self.catalog.with(self.txn)
    }
}

pub struct Connection<'a, T: KeyValueStore> {
    db: &'a Database<T>,
    txn_status: TransactionState<'a, T>,
}

impl<'db, T: KeyValueStore> Connection<'db, T> {
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
        let plan = planner::plan(&ctx, statement)?;
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
    plan: planner::TypedPlanNode<T>,
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

#[derive(Debug, Clone)]
pub struct Column {
    name: String,
    ty: Type,
}

impl Column {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn ty(&self) -> Type {
        self.ty
    }
}

pub struct Rows {
    iter: std::vec::IntoIter<Row>,
    columns: Vec<Column>,
}

impl Rows {
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    fn empty() -> Self {
        Self {
            iter: Vec::new().into_iter(),
            columns: Vec::new(),
        }
    }
}

impl Iterator for Rows {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub struct Row {
    columns: Vec<Value>,
}

impl Row {
    pub fn get<T>(&self, column: usize) -> Result<T>
    where
        T: TryFrom<Value, Error = TryFromValueError>,
    {
        self.columns
            .get(column)
            .ok_or(Error::ColumnIndexOutOfRange)?
            .clone()
            .try_into()
            .map_err(Into::into)
    }

    pub fn columns(&self) -> &[Value] {
        &self.columns
    }
}

#[derive(Debug, Clone, Copy)]
enum UnaryOp {
    Plus,
    Minus,
    Not,
}

impl std::fmt::Display for UnaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Plus => "+",
            Self::Minus => "-",
            Self::Not => "NOT",
        })
    }
}

#[derive(Debug, Clone, Copy)]
enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    Concat,
}

impl std::fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::Mod => "%",
            Self::Eq => "=",
            Self::Ne => "<>",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Gt => ">",
            Self::Ge => ">=",
            Self::And => "AND",
            Self::Or => "OR",
            Self::Concat => "||",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Order {
    Asc,
    Desc,
}

impl Default for Order {
    fn default() -> Self {
        Self::Asc
    }
}

impl std::fmt::Display for Order {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Asc => "ASC",
            Self::Desc => "DESC",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NullOrder {
    NullsFirst,
    NullsLast,
}

impl Default for NullOrder {
    fn default() -> Self {
        Self::NullsLast
    }
}

impl std::fmt::Display for NullOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::NullsFirst => "NULLS FIRST",
            Self::NullsLast => "NULLS LAST",
        })
    }
}
