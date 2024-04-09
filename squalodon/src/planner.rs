mod aggregate;
mod ddl;
mod expression;
mod modification;
mod query;

pub use aggregate::{Aggregate, AggregateOp, ApplyAggregateOp};
pub use expression::{CaseBranch, Expression};
pub use modification::{Delete, Insert, Update};
pub use query::{CrossProduct, Filter, Limit, OrderBy, Project, Scan, Sort, Union, Values};

use crate::{
    connection::ConnectionContext,
    parser,
    rows::ColumnIndex,
    types::{NullableType, Params, Type},
    CatalogError, Storage, StorageError, Value,
};
use ddl::{CreateIndex, CreateTable, DropObject};
use expression::{ExpressionBinder, TypedExpression};
use std::{
    cell::{RefCell, RefMut},
    num::NonZeroUsize,
    rc::Rc,
};

#[derive(Debug, thiserror::Error)]
pub enum PlannerError {
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

    #[error("Invalid argument")]
    InvalidArgument,

    #[error("Aggregate function is not allowed in this context")]
    AggregateNotAllowed,

    #[error("Subquery returns more than one column")]
    MultipleColumnsFromSubquery,

    #[error("Type error")]
    TypeError,

    #[error("Arity error")]
    ArityError,

    #[error("Parameter ${0} not provided")]
    ParameterNotProvided(NonZeroUsize),

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),
}

pub type PlannerResult<T> = std::result::Result<T, PlannerError>;

pub fn plan_expr<'txn, T: Storage>(
    ctx: &'txn ConnectionContext<'txn, '_, T>,
    expr: parser::Expression,
) -> PlannerResult<Expression<'txn, T, ColumnId>> {
    let planner = Planner::new(ctx);
    let TypedExpression { expr, .. } = ExpressionBinder::new(&planner).bind_without_source(expr)?;
    Ok(expr)
}

pub fn plan<'txn, 'db, T: Storage>(
    ctx: &'txn ConnectionContext<'txn, 'db, T>,
    statement: parser::Statement,
    params: Vec<Value>,
) -> PlannerResult<Plan<'txn, 'db, T>> {
    let planner = Planner::new(ctx).with_params(params);
    let plan = planner.plan(statement)?;
    let column_map = planner.column_map();
    let schema: Vec<_> = plan
        .outputs()
        .into_iter()
        .map(|id| column_map[id].clone())
        .collect();
    Ok(Plan { node: plan, schema })
}

pub struct Plan<'txn, 'db, T: Storage> {
    pub node: PlanNode<'txn, 'db, T>,
    pub schema: Vec<Column>,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub table_name: Option<String>,
    pub column_name: String,
    pub ty: NullableType,
}

impl Column {
    pub fn new(name: impl Into<String>, ty: impl Into<NullableType>) -> Self {
        Self {
            table_name: None,
            column_name: name.into(),
            ty: ty.into(),
        }
    }
}

trait Node {
    fn fmt_explain(&self, f: &mut ExplainFormatter);
    fn append_outputs(&self, columns: &mut Vec<ColumnId>);
}

#[derive(Default)]
struct ExplainFormatter {
    rows: Vec<String>,
    depth: isize,
}

impl ExplainFormatter {
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
    Explain(Explain<'txn, 'db, T>),
    CreateTable(CreateTable),
    CreateIndex(CreateIndex),
    Drop(DropObject),
    Values(Values<'txn, T>),
    Scan(Scan<'txn, 'db, T>),
    Project(Project<'txn, 'db, T>),
    Filter(Filter<'txn, 'db, T>),
    Sort(Sort<'txn, 'db, T>),
    Limit(Limit<'txn, 'db, T>),
    CrossProduct(CrossProduct<'txn, 'db, T>),
    Aggregate(Aggregate<'txn, 'db, T>),
    Union(Union<'txn, 'db, T>),
    Spool(Spool<'txn, 'db, T>),
    Insert(Insert<'txn, 'db, T>),
    Update(Update<'txn, 'db, T>),
    Delete(Delete<'txn, 'db, T>),
}

impl<'txn, 'db, T: Storage> PlanNode<'txn, 'db, T> {
    pub fn outputs(&self) -> Vec<ColumnId> {
        let mut columns = Vec::new();
        self.append_outputs(&mut columns);
        columns
    }

    fn explain(self, column_map: &mut ColumnMap) -> Self {
        Self::Explain(Explain {
            source: Box::new(self),
            output: column_map.insert(Column::new("plan", Type::Text)),
        })
    }

    fn spool(self) -> Self {
        Self::Spool(Spool {
            source: Box::new(self),
        })
    }
}

impl<T: Storage> Node for PlanNode<'_, '_, T> {
    fn fmt_explain(&self, f: &mut ExplainFormatter) {
        f.depth += 1;
        match self {
            Self::Explain(n) => n.fmt_explain(f),
            Self::CreateTable(n) => n.fmt_explain(f),
            Self::CreateIndex(n) => n.fmt_explain(f),
            Self::Drop(n) => n.fmt_explain(f),
            Self::Values(n) => n.fmt_explain(f),
            Self::Scan(n) => n.fmt_explain(f),
            Self::Project(n) => n.fmt_explain(f),
            Self::Filter(n) => n.fmt_explain(f),
            Self::Sort(n) => n.fmt_explain(f),
            Self::Limit(n) => n.fmt_explain(f),
            Self::CrossProduct(n) => n.fmt_explain(f),
            Self::Aggregate(n) => n.fmt_explain(f),
            Self::Union(n) => n.fmt_explain(f),
            Self::Spool(n) => n.fmt_explain(f),
            Self::Insert(n) => n.fmt_explain(f),
            Self::Update(n) => n.fmt_explain(f),
            Self::Delete(n) => n.fmt_explain(f),
        }
        f.depth -= 1;
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        match self {
            Self::Explain(n) => n.append_outputs(columns),
            Self::CreateTable(n) => n.append_outputs(columns),
            Self::CreateIndex(n) => n.append_outputs(columns),
            Self::Drop(n) => n.append_outputs(columns),
            Self::Values(n) => n.append_outputs(columns),
            Self::Scan(n) => n.append_outputs(columns),
            Self::Project(n) => n.append_outputs(columns),
            Self::Filter(n) => n.append_outputs(columns),
            Self::Sort(n) => n.append_outputs(columns),
            Self::Limit(n) => n.append_outputs(columns),
            Self::CrossProduct(n) => n.append_outputs(columns),
            Self::Aggregate(n) => n.append_outputs(columns),
            Self::Union(n) => n.append_outputs(columns),
            Self::Spool(n) => n.append_outputs(columns),
            Self::Insert(n) => n.append_outputs(columns),
            Self::Update(n) => n.append_outputs(columns),
            Self::Delete(n) => n.append_outputs(columns),
        }
    }
}

pub struct Explain<'txn, 'db, T: Storage> {
    pub source: Box<PlanNode<'txn, 'db, T>>,
    output: ColumnId,
}

impl<T: Storage> Node for Explain<'_, '_, T> {
    fn fmt_explain(&self, f: &mut ExplainFormatter) {
        f.write_str("Explain");
        self.source.fmt_explain(f);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        columns.push(self.output);
    }
}

impl<T: Storage> Explain<'_, '_, T> {
    pub fn dump(&self) -> Vec<String> {
        let mut f = ExplainFormatter::default();
        self.source.fmt_explain(&mut f);
        f.rows
    }
}

pub struct Spool<'txn, 'db, T: Storage> {
    pub source: Box<PlanNode<'txn, 'db, T>>,
}

impl<T: Storage> Node for Spool<'_, '_, T> {
    fn fmt_explain(&self, f: &mut ExplainFormatter) {
        f.write_str("Spool");
        self.source.fmt_explain(f);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        self.source.append_outputs(columns);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnId(usize);

impl std::fmt::Display for ColumnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "%{}", self.0)
    }
}

impl ColumnId {
    pub fn to_index(self, columns: &[Self]) -> ColumnIndex {
        let index = columns.iter().position(|id| id.0 == self.0).unwrap();
        ColumnIndex(index)
    }
}

struct ColumnMap<'a>(RefMut<'a, Vec<Column>>);

impl ColumnMap<'_> {
    fn insert(&mut self, column: Column) -> ColumnId {
        let id = ColumnId(self.0.len());
        self.0.push(column);
        id
    }
}

impl std::ops::Index<ColumnId> for ColumnMap<'_> {
    type Output = Column;

    fn index(&self, index: ColumnId) -> &Self::Output {
        &self.0[index.0]
    }
}

struct Planner<'txn, 'db, T: Storage> {
    ctx: &'txn ConnectionContext<'txn, 'db, T>,
    params: Vec<Value>,
    columns: Rc<RefCell<Vec<Column>>>,
}

impl<'txn, 'db, T: Storage> Planner<'txn, 'db, T> {
    fn new(ctx: &'txn ConnectionContext<'txn, 'db, T>) -> Self {
        Self {
            ctx,
            params: Vec::new(),
            columns: Default::default(),
        }
    }

    fn with_params(&self, params: Vec<Value>) -> Self {
        Self {
            ctx: self.ctx,
            params,
            columns: self.columns.clone(),
        }
    }

    fn column_map(&self) -> ColumnMap<'_> {
        ColumnMap(self.columns.borrow_mut())
    }

    fn plan(&self, statement: parser::Statement) -> PlannerResult<PlanNode<'txn, 'db, T>> {
        match statement {
            parser::Statement::Explain(statement) => self.plan_explain(*statement),
            parser::Statement::Prepare(_)
            | parser::Statement::Execute(_)
            | parser::Statement::Deallocate(_)
            | parser::Statement::Transaction(_) => unreachable!("handled before planning"),
            parser::Statement::ShowTables => {
                self.rewrite_to("SELECT * FROM squalodon_tables() ORDER BY name", [])
            }
            parser::Statement::Describe(name) => {
                self.ctx.catalog().table(name.clone())?; // Check if the table exists
                self.rewrite_to(
                    "SELECT column_name, type, is_nullable, is_primary_key
                    FROM squalodon_columns()
                    WHERE table_name = $1",
                    Value::from(name),
                )
            }
            parser::Statement::CreateTable(create_table) => self.plan_create_table(create_table),
            parser::Statement::CreateIndex(create_index) => self.plan_create_index(create_index),
            parser::Statement::Drop(drop_object) => Ok(self.plan_drop(drop_object)),
            parser::Statement::Query(query) => self.plan_query(query),
            parser::Statement::Insert(insert) => self.plan_insert(insert),
            parser::Statement::Update(update) => self.plan_update(update),
            parser::Statement::Delete(delete) => self.plan_delete(delete),
        }
    }

    fn rewrite_to<P: Params>(&self, sql: &str, params: P) -> PlannerResult<PlanNode<'txn, 'db, T>> {
        let mut parser = parser::Parser::new(sql);
        let statement = parser.next().unwrap().unwrap();
        assert!(parser.next().is_none());
        self.with_params(params.into_values()).plan(statement)
    }

    fn plan_explain(&self, statement: parser::Statement) -> PlannerResult<PlanNode<'txn, 'db, T>> {
        Ok(self.plan(statement)?.explain(&mut self.column_map()))
    }
}
