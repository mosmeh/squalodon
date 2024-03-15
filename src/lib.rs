pub mod storage;

mod executor;
mod memcomparable;
mod parser;
mod planner;
mod types;

pub use executor::Error as ExecutorError;
pub use parser::{LexerError, ParserError};
pub use planner::Error as PlannerError;
pub use types::{Type, Value};

pub(crate) use storage::Error as StorageError;

use executor::Executor;
use parser::{Parser, Statement, TransactionControl};
use storage::{KeyValueStore, Storage, Transaction};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Incompatible type")]
    IncompatibleType,

    #[error("Out of range")]
    OutOfRange,

    #[error("Invalid encoding")]
    InvalidEncoding,

    #[error("Cannot begin a transaction within a transaction")]
    NestedTransaction,

    #[error("No active transaction")]
    NoActiveTransaction,

    #[error("The current transaction has been aborted. Commands are ignored until end of transaction block.")]
    TransactionAborted,

    #[error("Parser error: {0}")]
    Parse(#[from] ParserError),

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Planner error: {0}")]
    Planner(#[from] PlannerError),

    #[error("Executor error: {0}")]
    Executor(#[from] ExecutorError),
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Database<S> {
    storage: Storage<S>,
}

impl<S: KeyValueStore> Database<S> {
    pub fn new(storage: S) -> Result<Self> {
        Ok(Self {
            storage: Storage::new(storage)?,
        })
    }

    pub fn connect(&self) -> Connection<S> {
        Connection {
            db: self,
            txn_status: TransactionState::Inactive,
        }
    }
}

enum TransactionState<'a, S: KeyValueStore> {
    /// We are in an explicit transaction started with BEGIN.
    Active(Transaction<'a, S>),

    /// The explicit transaction has been aborted and waiting for
    /// COMMIT or ROLLBACK.
    Aborted,

    /// There is no active explicit transaction. We are in auto-commit mode.
    Inactive,
}

pub struct Connection<'a, S: KeyValueStore> {
    db: &'a Database<S>,
    txn_status: TransactionState<'a, S>,
}

impl<S: KeyValueStore> Connection<'_, S> {
    pub fn execute(&mut self, sql: &str) -> Result<()> {
        let parser = Parser::new(sql);
        for statement in parser {
            self.execute_statement(statement?)?;
        }
        Ok(())
    }

    pub fn query(&mut self, sql: &str) -> Result<Rows> {
        let mut statements = Parser::new(sql).collect::<parser::Result<Vec<_>>>()?;
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
            TransactionState::Aborted => return Err(Error::TransactionAborted),
            TransactionState::Inactive => implicit_txn.insert(self.db.storage.transaction()),
        };
        let plan = planner::plan(txn, statement)?;
        match execute_plan(txn, plan) {
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

    fn handle_transaction_control(&mut self, txn_control: TransactionControl) -> Result<()> {
        match (&self.txn_status, txn_control) {
            (TransactionState::Active(_), TransactionControl::Begin) => {
                Err(Error::NestedTransaction)
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
                Err(Error::TransactionAborted)
            }
            (TransactionState::Inactive, TransactionControl::Begin) => {
                self.txn_status = TransactionState::Active(self.db.storage.transaction());
                Ok(())
            }
            (
                TransactionState::Inactive,
                TransactionControl::Commit | TransactionControl::Rollback,
            ) => Err(Error::NoActiveTransaction),
        }
    }
}

fn execute_plan<S: KeyValueStore>(
    txn: &Transaction<S>,
    plan: planner::TypedPlanNode,
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
    let rows = Executor::new(txn, plan.into_node())?
        .map(|columns| {
            columns.map(|columns| {
                assert_eq!(columns.len(), num_columns);
                Row { columns }
            })
        })
        .collect::<executor::Result<Vec<_>>>()?;
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
    pub fn get<T: TryFrom<Value>>(&self, column: usize) -> Result<T> {
        self.columns
            .get(column)
            .ok_or(Error::OutOfRange)?
            .clone()
            .try_into()
            .map_err(|_| Error::IncompatibleType)
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
