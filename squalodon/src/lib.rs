pub mod lexer;
pub mod storage;

mod builtin;
mod catalog;
mod connection;
mod executor;
mod memcomparable;
mod optimizer;
mod parser;
mod planner;
mod rows;
mod types;

pub use catalog::CatalogError;
pub use connection::{Connection, Inserter, PreparedStatement, TransactionError};
pub use executor::ExecutorError;
pub use parser::ParserError;
pub use planner::PlannerError;
pub use rows::{Column, Row, Rows};
pub(crate) use storage::StorageError;
pub use types::{Null, Params, TryFromValueError, Type, Value};

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

    #[error("The query contains no statement")]
    NoStatement,

    #[error("The query contains multiple statements")]
    MultipleStatements,

    #[error("Column index out of range")]
    ColumnIndexOutOfRange,

    #[error("Wrong number of parameters: expected {expected}, got {actual}")]
    ParameterCountMismatch { expected: usize, actual: usize },

    #[error("Unknown prepared statement {0:?}")]
    UnknownPreparedStatement(String),

    #[error(transparent)]
    TryFromValue(#[from] TryFromValueError),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Default)]
pub struct Database<T> {
    storage: T,
    catalog: Catalog,
}

impl<T> Database<T> {
    pub fn new(storage: T) -> Self {
        Self {
            storage,
            catalog: Catalog::default(),
        }
    }
}

impl<T: Storage> Database<T> {
    pub fn connect(&self) -> Connection<T> {
        Connection::new(self)
    }
}
