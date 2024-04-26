mod aggregate;
mod column;
mod ddl;
mod explain;
mod expression;
mod filter;
mod mutation;
mod query;
mod scan;
mod sort;

pub use aggregate::{Aggregate, AggregateOp, ApplyAggregateOp};
pub use column::{Column, ColumnId};
pub use expression::{CaseBranch, Expression};
pub use filter::Filter;
pub use mutation::{Delete, Insert, Update};
pub use query::{CrossProduct, Limit, Project, Union, Values};
pub use scan::Scan;
pub use sort::{OrderBy, Sort, TopN};

use crate::{
    connection::ConnectionContext,
    parser,
    types::{Params, Type},
    CatalogError, StorageError, Value,
};
use column::{ColumnMap, ColumnRef};
use ddl::{CreateIndex, CreateTable, DropObject};
use explain::{Explain, ExplainFormatter};
use expression::{ExpressionBinder, TypedExpression};
use std::{cell::RefCell, num::NonZeroUsize, rc::Rc};

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

pub fn plan_expr<'a>(
    ctx: &'a ConnectionContext<'a>,
    expr: parser::Expression,
) -> PlannerResult<Expression<'a, ColumnId>> {
    let planner = Planner::new(ctx);
    let TypedExpression { expr, .. } = ExpressionBinder::new(&planner).bind_without_source(expr)?;
    Ok(expr)
}

pub fn plan<'a>(
    ctx: &'a ConnectionContext<'a>,
    statement: parser::Statement,
    params: Vec<Value>,
) -> PlannerResult<Plan<'a>> {
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

pub struct Plan<'a> {
    pub node: PlanNode<'a>,
    pub schema: Vec<Column>,
}

trait Node {
    fn fmt_explain(&self, f: &ExplainFormatter);
    fn append_outputs(&self, columns: &mut Vec<ColumnId>);
}

pub enum PlanNode<'a> {
    Explain(Explain<'a>),
    CreateTable(CreateTable),
    CreateIndex(CreateIndex),
    Drop(DropObject),
    Values(Values<'a>),
    Scan(Scan<'a>),
    Project(Project<'a>),
    Filter(Filter<'a>),
    Sort(Sort<'a>),
    Limit(Limit<'a>),
    TopN(TopN<'a>),
    CrossProduct(CrossProduct<'a>),
    Aggregate(Aggregate<'a>),
    Union(Union<'a>),
    Spool(Spool<'a>),
    Insert(Insert<'a>),
    Update(Update<'a>),
    Delete(Delete<'a>),
}

impl<'a> PlanNode<'a> {
    pub fn outputs(&self) -> Vec<ColumnId> {
        let mut columns = Vec::new();
        self.append_outputs(&mut columns);
        columns
    }

    fn resolve_column(
        &self,
        column_map: &ColumnMap,
        column_ref: impl ColumnRef,
    ) -> PlannerResult<TypedExpression<'a>> {
        let mut candidates = self.outputs().into_iter().filter_map(|id| {
            let column = &column_map[id];
            if column.column_name != column_ref.column_name() {
                return None;
            }
            match (&column.table_name, column_ref.table_name()) {
                (Some(a), Some(b)) if a == b => Some((id, column)),
                (_, None) => {
                    // If the column reference does not specify
                    // a table name, it ambiguously matches any column
                    // with the same name.
                    Some((id, column))
                }
                (_, Some(_)) => None,
            }
        });
        let (id, column) = candidates
            .next()
            .ok_or_else(|| PlannerError::UnknownColumn(column_ref.column_name().to_owned()))?;
        if candidates.next().is_some() {
            return Err(PlannerError::AmbiguousColumn(
                column_ref.column_name().to_owned(),
            ));
        }
        Ok(Expression::ColumnRef(id).into_typed(column.ty))
    }

    fn produces_no_rows(&self) -> bool {
        if let Self::Values(Values { rows, .. }) = self {
            rows.is_empty()
        } else {
            false
        }
    }

    fn explain(self, planner: &Planner<'a>) -> Self {
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

impl Node for PlanNode<'_> {
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

pub struct Spool<'a> {
    pub source: Box<PlanNode<'a>>,
}

impl Node for Spool<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        f.node("Spool").child(&self.source);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        self.source.append_outputs(columns);
    }
}

struct Planner<'a> {
    ctx: &'a ConnectionContext<'a>,
    params: Vec<Value>,
    column_map: Rc<RefCell<Vec<Column>>>,
}

impl<'a> Planner<'a> {
    fn new(ctx: &'a ConnectionContext<'a>) -> Self {
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

    fn column_map(&self) -> ColumnMap {
        ColumnMap::from(self.column_map.as_ref())
    }
}

impl<'a> Planner<'a> {
    fn plan(&self, statement: parser::Statement) -> PlannerResult<PlanNode<'a>> {
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
                self.ctx.catalog().table(&name)?; // Check if the table exists
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

    fn rewrite_to<P: Params>(&self, sql: &str, params: P) -> PlannerResult<PlanNode<'a>> {
        let mut parser = parser::Parser::new(sql);
        let statement = parser.next().unwrap().unwrap();
        assert!(parser.next().is_none());
        self.with_params(params.into_values()).plan(statement)
    }

    fn plan_explain(&self, statement: parser::Statement) -> PlannerResult<PlanNode<'a>> {
        Ok(self.plan(statement)?.explain(self))
    }
}
