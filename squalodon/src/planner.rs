mod aggregate;
mod ddl;
mod explain;
mod expression;
mod filter;
mod mutation;
mod query;
mod sort;

pub use aggregate::{Aggregate, AggregateOp, ApplyAggregateOp};
pub use expression::{CaseBranch, Expression};
pub use filter::Filter;
pub use mutation::{Delete, Insert, Update};
pub use query::{CrossProduct, Limit, Project, Scan, Union, Values};
pub use sort::{OrderBy, Sort, TopN};

use crate::{
    connection::ConnectionContext,
    parser,
    rows::ColumnIndex,
    storage::Transaction,
    types::{NullableType, Params, Type},
    CatalogError, StorageError, Value,
};
use ddl::{CreateIndex, CreateTable, DropObject};
use explain::{Explain, ExplainFormatter};
use expression::{ExpressionBinder, TypedExpression};
use std::{
    borrow::Cow,
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

    #[error("LIMIT/OFFSET cannot be negative")]
    NegativeLimitOrOffset,

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

pub fn plan_expr<'a, T: Transaction>(
    ctx: &'a ConnectionContext<'a, T>,
    expr: parser::Expression,
) -> PlannerResult<Expression<'a, T, ColumnId>> {
    let planner = Planner::new(ctx);
    let TypedExpression { expr, .. } = ExpressionBinder::new(&planner).bind_without_source(expr)?;
    Ok(expr)
}

pub fn plan<'a, T: Transaction>(
    ctx: &'a ConnectionContext<'a, T>,
    statement: parser::Statement,
    params: Vec<Value>,
) -> PlannerResult<Plan<'a, T>> {
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

pub struct Plan<'a, T> {
    pub node: PlanNode<'a, T>,
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

    pub fn name(&self) -> Cow<str> {
        self.table_name.as_ref().map_or_else(
            || Cow::Borrowed(self.column_name.as_str()),
            |table_name| Cow::Owned(format!("{}.{}", table_name, self.column_name)),
        )
    }
}

trait Node {
    fn fmt_explain(&self, f: &ExplainFormatter);
    fn append_outputs(&self, columns: &mut Vec<ColumnId>);
}

pub enum PlanNode<'a, T> {
    Explain(Explain<'a, T>),
    CreateTable(CreateTable),
    CreateIndex(CreateIndex),
    Drop(DropObject),
    Values(Values<'a, T>),
    Scan(Scan<'a, T>),
    Project(Project<'a, T>),
    Filter(Filter<'a, T>),
    Sort(Sort<'a, T>),
    Limit(Limit<'a, T>),
    TopN(TopN<'a, T>),
    CrossProduct(CrossProduct<'a, T>),
    Aggregate(Aggregate<'a, T>),
    Union(Union<'a, T>),
    Spool(Spool<'a, T>),
    Insert(Insert<'a, T>),
    Update(Update<'a, T>),
    Delete(Delete<'a, T>),
}

impl<'a, T> PlanNode<'a, T> {
    pub fn outputs(&self) -> Vec<ColumnId> {
        let mut columns = Vec::new();
        self.append_outputs(&mut columns);
        columns
    }

    fn produces_no_rows(&self) -> bool {
        if let Self::Values(Values { rows, .. }) = self {
            rows.is_empty()
        } else {
            false
        }
    }

    fn explain(self, planner: &Planner<'a, T>) -> Self {
        Self::Explain(Explain {
            source: Box::new(self),
            output: planner.column_map().insert(Column::new("plan", Type::Text)),
            column_map: planner.column_map.clone(),
        })
    }

    fn spool(self) -> Self {
        Self::Spool(Spool {
            source: Box::new(self),
        })
    }
}

impl<T> Node for PlanNode<'_, T> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
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
            Self::TopN(n) => n.fmt_explain(f),
            Self::CrossProduct(n) => n.fmt_explain(f),
            Self::Aggregate(n) => n.fmt_explain(f),
            Self::Union(n) => n.fmt_explain(f),
            Self::Spool(n) => n.fmt_explain(f),
            Self::Insert(n) => n.fmt_explain(f),
            Self::Update(n) => n.fmt_explain(f),
            Self::Delete(n) => n.fmt_explain(f),
        }
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
            Self::TopN(n) => n.append_outputs(columns),
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

pub struct Spool<'a, T> {
    pub source: Box<PlanNode<'a, T>>,
}

impl<T> Node for Spool<'_, T> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        f.node("Spool").child(&self.source);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        self.source.append_outputs(columns);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnId(usize);

impl ColumnId {
    pub fn to_index(self, columns: &[Self]) -> ColumnIndex {
        let index = columns.iter().position(|id| id.0 == self.0).unwrap();
        ColumnIndex(index)
    }
}

struct ColumnMap<'a>(RefMut<'a, Vec<Column>>);

impl ColumnMap<'_> {
    fn view(&self) -> ColumnMapView<'_> {
        ColumnMapView(&self.0)
    }

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

impl std::ops::Index<&ColumnId> for ColumnMap<'_> {
    type Output = Column;

    fn index(&self, index: &ColumnId) -> &Self::Output {
        &self.0[index.0]
    }
}

struct ColumnMapView<'a>(&'a [Column]);

impl std::ops::Index<ColumnId> for ColumnMapView<'_> {
    type Output = Column;

    fn index(&self, index: ColumnId) -> &Self::Output {
        &self.0[index.0]
    }
}

impl std::ops::Index<&ColumnId> for ColumnMapView<'_> {
    type Output = Column;

    fn index(&self, index: &ColumnId) -> &Self::Output {
        &self.0[index.0]
    }
}

struct Planner<'a, T> {
    ctx: &'a ConnectionContext<'a, T>,
    params: Vec<Value>,
    column_map: Rc<RefCell<Vec<Column>>>,
}

impl<'a, T> Planner<'a, T> {
    fn new(ctx: &'a ConnectionContext<'a, T>) -> Self {
        Self {
            ctx,
            params: Vec::new(),
            column_map: Default::default(),
        }
    }

    fn with_params(&self, params: Vec<Value>) -> Self {
        Self {
            ctx: self.ctx,
            params,
            column_map: self.column_map.clone(),
        }
    }

    fn column_map(&self) -> ColumnMap<'_> {
        ColumnMap(self.column_map.borrow_mut())
    }
}

impl<'a, T: Transaction> Planner<'a, T> {
    fn plan(&self, statement: parser::Statement) -> PlannerResult<PlanNode<'a, T>> {
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
                    "SELECT column_name, type, is_nullable, is_primary_key, default_value
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

    fn rewrite_to<P: Params>(&self, sql: &str, params: P) -> PlannerResult<PlanNode<'a, T>> {
        let mut parser = parser::Parser::new(sql);
        let statement = parser.next().unwrap().unwrap();
        assert!(parser.next().is_none());
        self.with_params(params.into_values()).plan(statement)
    }

    fn plan_explain(&self, statement: parser::Statement) -> PlannerResult<PlanNode<'a, T>> {
        Ok(self.plan(statement)?.explain(self))
    }
}
