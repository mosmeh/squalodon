pub mod lexer;
pub mod storage;

mod builtin;
mod catalog;
mod connection;
mod executor;
mod memcomparable;
mod parser;
mod planner;
mod rows;
mod types;

pub use catalog::CatalogError;
pub use connection::{Connection, TransactionError};
pub use executor::ExecutorError;
pub use parser::ParserError;
pub use planner::PlannerError;
pub use rows::{Column, Row, Rows};
pub(crate) use storage::StorageError;
pub use types::{TryFromValueError, Type, Value};

use catalog::Catalog;
use storage::Storage;

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

    #[error("Unknown prepared statement {0:?}")]
    UnknownPreparedStatement(String),

    #[error(
        "Wrong number of parameters for prepared statement: expected {expected}, got {actual}"
    )]
    ParameterCountMismatch { expected: usize, actual: usize },

    #[error(transparent)]
    TryFromValue(#[from] TryFromValueError),
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Database<T: Storage> {
    storage: T,
    catalog: Catalog<T>,
}

impl<T: Storage> Database<T> {
    pub fn new(storage: T) -> Result<Self> {
        let catalog = Catalog::load(&storage)?;
        Ok(Self { storage, catalog })
    }

    pub fn connect(&self) -> Connection<T> {
        Connection::new(self)
    }
}
