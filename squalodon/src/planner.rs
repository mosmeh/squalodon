mod aggregate;
mod ddl;
mod expression;
mod modification;
mod query;

pub use aggregate::Aggregate;
pub use expression::Expression;
pub use modification::{Delete, Insert, Update};
pub use query::{CrossProduct, Filter, Limit, OrderBy, Project, Scan, Sort, Values};

use crate::{
    catalog::CatalogRef,
    lexer,
    parser::{self, ColumnRef},
    rows::ColumnIndex,
    types::{NullableType, Type},
    CatalogError, Storage, StorageError,
};
use ddl::{CreateTable, DropTable};

#[derive(Debug, thiserror::Error)]
pub enum PlannerError {
    #[error("Table {0:?} already exists")]
    TableAlreadyExists(String),

    #[error("Unknown column {0:?}")]
    UnknownColumn(String),

    #[error("Ambiguous column {0:?}")]
    AmbiguousColumn(String),

    #[error("Duplicate column {0:?}")]
    DuplicateColumn(String),

    #[error("Expected {expected} columns but got {actual}")]
    ColumnCountMismatch { expected: usize, actual: usize },

    #[error("Multiple primary keys are not allowed")]
    MultiplePrimaryKeys,

    #[error("Primary key is required")]
    NoPrimaryKey,

    #[error("Aggregate function is not allowed in this context")]
    AggregateNotAllowed,

    #[error("Subquery returns more than one column")]
    MultipleColumnsFromSubquery,

    #[error("Type error")]
    TypeError,

    #[error("Arity error")]
    ArityError,

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),
}

pub type PlannerResult<T> = std::result::Result<T, PlannerError>;

pub fn plan<'txn, 'db, T: Storage>(
    catalog: &'txn CatalogRef<'txn, 'db, T>,
    statement: parser::Statement,
) -> PlannerResult<Plan<'txn, 'db, T>> {
    Binder::new(catalog).bind(statement)
}

#[derive(Clone)]
pub struct PlanSchema(pub Vec<Column>);

impl PlanSchema {
    fn empty() -> Self {
        Self(Vec::new())
    }

    fn resolve_column(&self, column_ref: &ColumnRef) -> PlannerResult<(ColumnIndex, &Column)> {
        let mut candidates = self.0.iter().enumerate().filter(|(_, column)| {
            if column.column_name != column_ref.column_name {
                return false;
            }
            match (&column.table_name, &column_ref.table_name) {
                (Some(a), Some(b)) => a == b,
                (_, None) => {
                    // If the column reference does not specify
                    // a table name, it ambiguously matches any column
                    // with the same name.
                    true
                }
                (None, Some(_)) => false,
            }
        });
        let (i, column) = candidates
            .next()
            .ok_or_else(|| PlannerError::UnknownColumn(column_ref.column_name.clone()))?;
        if candidates.next().is_some() {
            return Err(PlannerError::AmbiguousColumn(
                column_ref.column_name.clone(),
            ));
        }
        Ok((ColumnIndex(i), column))
    }
}

impl From<Vec<Column>> for PlanSchema {
    fn from(columns: Vec<Column>) -> Self {
        Self(columns)
    }
}

pub struct Plan<'txn, 'db, T: Storage> {
    pub node: PlanNode<'txn, 'db, T>,
    pub schema: PlanSchema,
}

impl<'txn, 'db, T: Storage> Plan<'txn, 'db, T> {
    fn empty_source() -> Self {
        Self {
            node: PlanNode::Values(Values::one_empty_row()),
            schema: PlanSchema::empty(),
        }
    }

    /// Creates a plan that does not produce any rows.
    fn sink(node: PlanNode<'txn, 'db, T>) -> Self {
        Self {
            node,
            schema: PlanSchema::empty(),
        }
    }

    fn inherit_schema<F>(self, f: F) -> Self
    where
        F: FnOnce(PlanNode<'txn, 'db, T>) -> PlanNode<'txn, 'db, T>,
    {
        Self {
            node: f(self.node),
            schema: self.schema,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub table_name: Option<String>,
    pub column_name: String,
    pub ty: NullableType,
}

impl Column {
    pub fn new(name: &str, ty: Type) -> Self {
        Self {
            table_name: None,
            column_name: name.to_owned(),
            ty: ty.into(),
        }
    }
}

trait Explain {
    fn visit(&self, visitor: &mut ExplainVisitor);
}

#[derive(Default)]
struct ExplainVisitor {
    rows: Vec<String>,
    depth: isize,
}

impl ExplainVisitor {
    fn write_str(&mut self, s: &str) {
        let mut row = String::new();
        for _ in 0..(self.depth - 1) {
            row.push_str("  ");
        }
        row.push_str(s);
        self.rows.push(row);
    }

    fn write_fmt(&mut self, fmt: std::fmt::Arguments) {
        self.write_str(&fmt.to_string());
    }
}

pub enum PlanNode<'txn, 'db, T: Storage> {
    Explain(Box<PlanNode<'txn, 'db, T>>),
    CreateTable(CreateTable),
    DropTable(DropTable),
    Values(Values),
    Scan(Scan<'txn, 'db, T>),
    Project(Project<'txn, 'db, T>),
    Filter(Filter<'txn, 'db, T>),
    Sort(Sort<'txn, 'db, T>),
    Limit(Limit<'txn, 'db, T>),
    CrossProduct(CrossProduct<'txn, 'db, T>),
    Aggregate(Aggregate<'txn, 'db, T>),
    Insert(Insert<'txn, 'db, T>),
    Update(Update<'txn, 'db, T>),
    Delete(Delete<'txn, 'db, T>),
}

impl<T: Storage> PlanNode<'_, '_, T> {
    pub fn explain(&self) -> Vec<String> {
        let mut visitor = ExplainVisitor::default();
        self.visit(&mut visitor);
        visitor.rows
    }
}

impl<T: Storage> Explain for PlanNode<'_, '_, T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        visitor.depth += 1;
        match self {
            Self::Explain(n) => n.visit(visitor),
            Self::CreateTable(n) => n.visit(visitor),
            Self::DropTable(n) => n.visit(visitor),
            Self::Values(n) => n.visit(visitor),
            Self::Scan(n) => n.visit(visitor),
            Self::Project(n) => n.visit(visitor),
            Self::Filter(n) => n.visit(visitor),
            Self::Sort(n) => n.visit(visitor),
            Self::Limit(n) => n.visit(visitor),
            Self::CrossProduct(n) => n.visit(visitor),
            Self::Aggregate(n) => n.visit(visitor),
            Self::Insert(n) => n.visit(visitor),
            Self::Update(n) => n.visit(visitor),
            Self::Delete(n) => n.visit(visitor),
        }
        visitor.depth -= 1;
    }
}

struct Binder<'txn, 'db, T: Storage> {
    catalog: &'txn CatalogRef<'txn, 'db, T>,
}

impl<'txn, 'db, T: Storage> Binder<'txn, 'db, T> {
    fn new(catalog: &'txn CatalogRef<'txn, 'db, T>) -> Self {
        Self { catalog }
    }

    fn bind(&self, statement: parser::Statement) -> PlannerResult<Plan<'txn, 'db, T>> {
        match statement {
            parser::Statement::Explain(statement) => self.bind_explain(*statement),
            parser::Statement::Transaction(_) => unreachable!("handled before binding"),
            parser::Statement::ShowTables => {
                self.rewrite_to("SELECT * FROM squalodon_tables() ORDER BY name")
            }
            parser::Statement::Describe(name) => self.rewrite_to(&format!(
                "SELECT column_name, type, is_nullable, is_primary_key
                FROM squalodon_columns()
                WHERE table_name = {}",
                lexer::quote(&name, '\'')
            )),
            parser::Statement::CreateTable(create_table) => self.bind_create_table(create_table),
            parser::Statement::DropTable(drop_table) => self.bind_drop_table(drop_table),
            parser::Statement::Select(select) => self.bind_select(select),
            parser::Statement::Insert(insert) => self.bind_insert(insert),
            parser::Statement::Update(update) => self.bind_update(update),
            parser::Statement::Delete(delete) => self.bind_delete(delete),
        }
    }

    fn rewrite_to(&self, sql: &str) -> PlannerResult<Plan<'txn, 'db, T>> {
        let mut parser = parser::Parser::new(sql);
        let statement = parser.next().unwrap().unwrap();
        assert!(parser.next().is_none());
        self.bind(statement)
    }

    fn bind_explain(&self, statement: parser::Statement) -> PlannerResult<Plan<'txn, 'db, T>> {
        let plan = self.bind(statement)?;
        Ok(Plan {
            node: PlanNode::Explain(Box::new(plan.node)),
            schema: vec![Column::new("plan", Type::Text)].into(),
        })
    }
}
