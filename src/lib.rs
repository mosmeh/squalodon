mod executor;
mod memcomparable;
mod parser;
mod planner;
mod storage;
mod types;

pub use executor::Error as ExecutorError;
pub use parser::{LexerError, ParserError};
pub use planner::Error as PlannerError;
pub use storage::{Error as StorageError, KeyValueStore, Memory};
pub use types::{Type, Value};

use executor::Executor;
use parser::Parser;
use storage::Storage;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Incompatible type")]
    IncompatibleType,

    #[error("Out of range")]
    OutOfRange,

    #[error("Invalid encoding")]
    InvalidEncoding,

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

    pub fn execute(&self, sql: &str) -> Result<()> {
        let parser = Parser::new(sql);
        for statement in parser {
            let statement = statement?;
            let txn = self.storage.transaction();
            let plan = planner::plan(&txn, statement)?.into_node();
            let executor = Executor::new(&txn, plan)?;
            for row in executor {
                row?;
            }
            txn.commit();
        }
        Ok(())
    }

    pub fn query(&self, sql: &str) -> Result<Rows> {
        let mut parser = Parser::new(sql);
        let statement = parser.next().unwrap()?;
        assert!(
            parser.next().is_none(),
            "multiple statements are not supported"
        );
        let txn = self.storage.transaction();
        let plan = planner::plan(&txn, statement)?;
        let columns = plan
            .columns()
            .iter()
            .map(|column| {
                Column {
                    name: column.name.clone(),
                    ty: column.ty.unwrap_or(Type::Integer), // Arbitrarily choose integer
                }
            })
            .collect();
        let rows = Executor::new(&txn, plan.into_node())?
            .map(|columns| columns.map(|columns| Row { columns }))
            .collect::<executor::Result<Vec<_>>>()?;
        txn.commit();
        Ok(Rows {
            iter: rows.into_iter(),
            columns,
        })
    }
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
