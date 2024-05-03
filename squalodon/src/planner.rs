mod aggregate;
mod column;
mod ddl;
mod explain;
mod expression;
mod filter;
mod join;
mod limit;
mod mutation;
mod project;
mod query;
mod scan;
mod sort;
mod union;

pub use aggregate::{Aggregate, AggregateOp, ApplyAggregateOp};
pub use column::{Column, ColumnId, ColumnMap};
pub use ddl::{Analyze, Constraint, CreateIndex, CreateTable, DropObject, Reindex, Truncate};
pub use explain::Explain;
pub use expression::{CaseBranch, ExecutableExpression, Expression, PlanExpression};
pub use filter::Filter;
pub use join::{CompareOp, CrossProduct, Join};
pub use limit::Limit;
pub use mutation::{Delete, Insert, Update};
pub use project::Project;
pub use scan::Scan;
pub use sort::{ExecutableOrderBy, Sort, TopN};
pub use union::Union;

use crate::{
    catalog::CatalogRef, executor::ExecutionContext, parser, types::Params, CatalogError, Result,
    Row, StorageError, Value,
};
use column::ColumnRef;
use explain::ExplainFormatter;
use expression::{ExpressionBinder, TypedExpression};
use std::{
    cell::{Ref, RefCell, RefMut},
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

    #[error("Parameter ${0} not provided")]
    ParameterNotProvided(NonZeroUsize),

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),
}

pub type PlannerResult<T> = std::result::Result<T, PlannerError>;

pub fn plan<'a>(
    ctx: &ExecutionContext<'a>,
    statement: parser::Statement,
    params: Vec<parser::Expression>,
) -> Result<Plan<'a>> {
    let planner = Planner::new(ctx.catalog().clone());
    let mut param_values = Vec::with_capacity(params.len());
    for expr in params {
        let TypedExpression { expr, .. } =
            ExpressionBinder::new(&planner).bind_without_source(expr)?;
        let value = expr.into_executable(&[]).eval(ctx, &Row::empty())?;
        param_values.push(value);
    }
    Ok(Plan {
        node: planner.with_params(param_values).plan(statement)?,
        column_map: planner.column_map,
    })
}

pub struct Plan<'a> {
    node: PlanNode<'a>,
    column_map: Rc<RefCell<ColumnMap>>,
}

impl<'a> Plan<'a> {
    pub fn into_node(self) -> PlanNode<'a> {
        self.node
    }

    pub fn map<F>(self, f: F) -> PlannerResult<Self>
    where
        F: FnOnce(PlanNode<'a>, &RefCell<ColumnMap>) -> PlannerResult<PlanNode<'a>>,
    {
        Ok(Plan {
            node: f(self.node, &self.column_map)?,
            column_map: self.column_map,
        })
    }

    pub fn columns(&self) -> impl Iterator<Item = Column> + '_ {
        let column_map = self.column_map.borrow();
        self.node
            .outputs()
            .into_iter()
            .map(move |id| column_map[id].clone())
    }
}

pub trait Node {
    fn fmt_explain(&self, f: &ExplainFormatter);
    fn append_outputs(&self, columns: &mut Vec<ColumnId>);

    /// Estimated number of rows produced by the node.
    fn num_rows(&self) -> usize;

    /// Cost of executing this node.
    fn cost(&self) -> f64;
}

macro_rules! nodes {
    ($($variant:ident: $ty:ty)*) => {
        #[derive(Clone)]
        pub enum PlanNode<'a> {
            $($variant($ty),)*
        }

        impl Node for PlanNode<'_> {
            fn fmt_explain(&self, f: &ExplainFormatter) {
                match self {
                    $(Self::$variant(n) => n.fmt_explain(f),)*
                }
            }

            fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
                match self {
                    $(Self::$variant(n) => n.append_outputs(columns),)*
                }
            }

            fn num_rows(&self) -> usize {
                match self {
                    $(Self::$variant(n) => n.num_rows(),)*
                }
            }

            fn cost(&self) -> f64 {
                match self {
                    $(Self::$variant(n) => n.cost(),)*
                }
            }
        }
    }
}

nodes! {
    Explain: Explain<'a>
    CreateTable: CreateTable
    CreateIndex: CreateIndex<'a>
    Drop: DropObject<'a>
    Truncate: Truncate<'a>
    Analyze: Analyze<'a>
    Reindex: Reindex<'a>
    Scan: Scan<'a>
    Project: Project<'a>
    Filter: Filter<'a>
    Sort: Sort<'a>
    Limit: Limit<'a>
    TopN: TopN<'a>
    CrossProduct: CrossProduct<'a>
    Join: Join<'a>
    Aggregate: Aggregate<'a>
    Union: Union<'a>
    Spool: Spool<'a>
    Insert: Insert<'a>
    Update: Update<'a>
    Delete: Delete<'a>
}

impl<'a> PlanNode<'a> {
    /// Default cost for executing a node when there is no basis for a better
    /// estimate.
    const DEFAULT_COST: f64 = 1.0;

    /// Default cost for processing a row when there is no basis for a better
    /// estimate.
    const DEFAULT_ROW_COST: f64 = 0.01;

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
        Ok(PlanExpression::ColumnRef(id).into_typed(column.ty))
    }

    fn produces_no_rows(&self) -> bool {
        if let Self::Scan(Scan::Expression { rows, .. }) = self {
            rows.is_empty()
        } else {
            false
        }
    }

    fn spool(self) -> Self {
        Self::Spool(Spool {
            source: Box::new(self),
        })
    }
}

#[derive(Clone)]
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

    fn num_rows(&self) -> usize {
        self.source.num_rows()
    }

    fn cost(&self) -> f64 {
        let spool_cost = self.source.num_rows() as f64 * PlanNode::DEFAULT_ROW_COST;
        self.source.cost() + spool_cost
    }
}

struct Planner<'a> {
    catalog: CatalogRef<'a>,
    params: Vec<Value>,
    column_map: Rc<RefCell<ColumnMap>>,
}

impl<'a> Planner<'a> {
    fn new(catalog: CatalogRef<'a>) -> Self {
        Self {
            catalog,
            params: Vec::new(),
            column_map: Default::default(),
        }
    }

    fn with_params(&self, params: Vec<Value>) -> Self {
        Self {
            catalog: self.catalog.clone(),
            params,
            column_map: self.column_map.clone(),
        }
    }

    fn column_map(&self) -> Ref<ColumnMap> {
        self.column_map.borrow()
    }

    fn column_map_mut(&self) -> RefMut<ColumnMap> {
        self.column_map.borrow_mut()
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
                self.catalog.table(&name)?; // Check if the table exists
                self.rewrite_to(
                    "SELECT column_name, type, is_nullable, is_primary_key, default_value
                    FROM squalodon_columns()
                    WHERE table_name = $1",
                    Value::from(name),
                )
            }
            parser::Statement::CreateTable(create_table) => self.plan_create_table(create_table),
            parser::Statement::CreateIndex(create_index) => self.plan_create_index(create_index),
            parser::Statement::Drop(drop_object) => self.plan_drop(drop_object),
            parser::Statement::Truncate(table_name) => self.plan_truncate(&table_name),
            parser::Statement::Analyze(analyze) => self.plan_analyze(analyze),
            parser::Statement::Reindex(reindex) => self.plan_reindex(reindex),
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
}
