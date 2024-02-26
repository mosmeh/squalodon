mod executor;
mod parser;
mod planner;
mod storage;
mod types;

pub use executor::Error as ExecutorError;
pub use parser::{LexerError, ParserError};
pub use planner::Error as PlannerError;
pub use storage::{Error as StorageError, Memory, Storage};
pub use types::{Type, Value};

use executor::Executor;
use parser::Parser;
use storage::{Catalog, SchemaAwareTransaction};

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
    Executor(#[from] executor::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Database<S> {
    storage: S,
    catalog: Catalog,
}

impl<S: Storage> Database<S> {
    pub fn new(storage: S) -> Result<Self> {
        let catalog = Catalog::load(&storage.transaction())?;
        Ok(Self { storage, catalog })
    }

    pub fn execute(&mut self, sql: &str) -> Result<()> {
        let parser = Parser::new(sql);
        for statement in parser {
            let statement = statement?;
            let plan = planner::plan(&self.catalog, statement)?;
            let mut txn = SchemaAwareTransaction::new(self.storage.transaction());
            let executor = Executor::new(&mut txn, &mut self.catalog, plan)?;
            for row in executor {
                row?;
            }
            txn.commit();
        }
        Ok(())
    }

    pub fn query(&mut self, sql: &str) -> Result<Rows> {
        let mut parser = Parser::new(sql);
        let statement = parser.next().unwrap()?;
        assert!(
            parser.next().is_none(),
            "multiple statements are not supported"
        );
        let plan = planner::plan(&self.catalog, statement)?;
        let columns = plan
            .schema
            .columns
            .iter()
            .map(|column| {
                Column {
                    name: column.name.clone(),
                    ty: column.ty.unwrap_or(Type::Integer), // Arbitrarily choose integer
                }
            })
            .collect();
        let mut txn = SchemaAwareTransaction::new(self.storage.transaction());
        let rows = Executor::new(&mut txn, &mut self.catalog, plan)?
            .map(|columns| columns.map(|columns| Row { columns }))
            .collect::<std::result::Result<Vec<_>, _>>()?;
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
