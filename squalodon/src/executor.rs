mod aggregate;
mod ddl;
mod expression;
mod filter;
mod join;
mod limit;
mod mutation;
mod project;
mod scan;
mod sort;
mod union;

use crate::{
    catalog::CatalogRef,
    connection::ConnectionLocal,
    planner::{self, ExecutableExpression, Plan, PlanNode},
    storage,
    types::NullableType,
    CatalogError, Column, Row, Rows, StorageError, Type, Value,
};
use aggregate::{HashAggregate, UngroupedAggregate};
use filter::Filter;
use join::{CrossProduct, HashJoin, NestedLoopJoin};
use limit::Limit;
use mutation::{Delete, Insert, Update};
use project::Project;
use scan::{ExpressionScan, FunctionScan, IndexOnlyScan, IndexScan, SeqScan};
use sort::{Sort, TopN};
use std::cell::RefCell;
use union::Union;

#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    #[error("Out of range")]
    OutOfRange,

    #[error("Type error")]
    TypeError,

    #[error("Subquery returned more than one row")]
    MultipleRowsFromSubquery,

    #[error("Invalid LIKE pattern")]
    InvalidLikePattern,

    #[error("Cannot evaluate the expression in the current context")]
    EvaluationError,

    #[error("Values from the sequence are yet to be fetched in the current connection")]
    SequenceNotFetched,

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),
}

pub type ExecutorResult<T> = std::result::Result<T, ExecutorError>;

pub struct ExecutionContext<'a> {
    catalog: CatalogRef<'a>,
    local: &'a RefCell<ConnectionLocal>,
}

impl<'a> ExecutionContext<'a> {
    pub fn new(catalog: CatalogRef<'a>, local: &'a RefCell<ConnectionLocal>) -> Self {
        Self { catalog, local }
    }

    pub fn catalog(&self) -> &CatalogRef<'a> {
        &self.catalog
    }

    pub fn local(&self) -> &RefCell<ConnectionLocal> {
        self.local
    }

    pub fn execute(&self, plan: Plan) -> ExecutorResult<Rows> {
        let columns: Vec<_> = plan
            .columns()
            .map(|column| {
                Column {
                    name: column.simple_name().to_owned(),
                    ty: match column.ty() {
                        NullableType::NonNull(ty) => ty,
                        NullableType::Null => Type::Integer, // Arbitrarily choose INTEGER
                    },
                }
            })
            .collect();
        let executor = Executor::new(self, plan.into_node())?;
        let mut rows = Vec::new();
        for row in executor {
            let row = row?;
            assert_eq!(row.columns().len(), columns.len());
            for (value, column) in row.columns().iter().zip(&columns) {
                assert!(value.ty().is_compatible_with(column.ty));
            }
            rows.push(row);
        }
        Ok(Rows {
            iter: rows.into_iter(),
            columns,
        })
    }
}

trait Node {
    fn next_row(&mut self) -> Output;
}

#[derive(Debug, thiserror::Error)]
enum NodeError {
    #[error(transparent)]
    Error(#[from] ExecutorError),

    // HACK: This is not a real error, but treating it as one allows us to use
    // the ? operator to exit early when reaching the end of the rows.
    #[error("End of rows")]
    EndOfRows,
}

impl From<storage::StorageError> for NodeError {
    fn from(e: storage::StorageError) -> Self {
        Self::Error(e.into())
    }
}

type Output = std::result::Result<Row, NodeError>;

trait IntoOutput {
    fn into_output(self) -> Output;
}

impl<E: Into<ExecutorError>> IntoOutput for std::result::Result<Row, E> {
    fn into_output(self) -> Output {
        self.map_err(|e| NodeError::Error(e.into()))
    }
}

impl IntoOutput for Option<Row> {
    fn into_output(self) -> Output {
        self.ok_or(NodeError::EndOfRows)
    }
}

impl<E: Into<ExecutorError>> IntoOutput for std::result::Result<Option<Row>, E> {
    fn into_output(self) -> Output {
        match self {
            Ok(Some(row)) => Ok(row),
            Ok(None) => Err(NodeError::EndOfRows),
            Err(e) => Err(NodeError::Error(e.into())),
        }
    }
}

impl<E: Into<ExecutorError>> IntoOutput for Option<std::result::Result<Row, E>> {
    fn into_output(self) -> Output {
        match self {
            Some(Ok(row)) => Ok(row),
            Some(Err(e)) => Err(NodeError::Error(e.into())),
            None => Err(NodeError::EndOfRows),
        }
    }
}

pub struct Executor<'a>(ExecutorNode<'a>);

impl<'a> Executor<'a> {
    pub fn new(ctx: &'a ExecutionContext<'a>, plan_node: PlanNode<'a>) -> ExecutorResult<Self> {
        ExecutorNode::new(ctx, plan_node).map(Self)
    }
}

impl Iterator for Executor<'_> {
    type Item = ExecutorResult<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next_row() {
            Ok(row) => Some(Ok(row)),
            Err(NodeError::Error(e)) => Some(Err(e)),
            Err(NodeError::EndOfRows) => None,
        }
    }
}

macro_rules! nodes {
    ($($variant:ident$(<$lt:lifetime>)?)*) => {
        enum ExecutorNode<'a> {
            $($variant($variant$(<$lt>)?),)*
        }

        impl Node for ExecutorNode<'_> {
            fn next_row(&mut self) -> Output {
                match self {
                    $(Self::$variant(e) => e.next_row(),)*
                }
            }
        }
    }
}

nodes! {
    SeqScan<'a>
    IndexScan<'a>
    IndexOnlyScan<'a>
    FunctionScan<'a>
    ExpressionScan<'a>
    Project<'a>
    Filter<'a>
    Sort
    Limit<'a>
    TopN
    CrossProduct<'a>
    NestedLoopJoin<'a>
    HashJoin<'a>
    UngroupedAggregate
    HashAggregate
    Union<'a>
    Spool
    Insert
    Update
    Delete
}

impl<'a> ExecutorNode<'a> {
    fn new(ctx: &'a ExecutionContext, plan: PlanNode<'a>) -> ExecutorResult<Self> {
        match plan {
            PlanNode::Explain(plan) => {
                let rows = plan
                    .explain()
                    .into_iter()
                    .map(|row| vec![ExecutableExpression::Constant(Value::Text(row))])
                    .collect();
                Ok(Self::ExpressionScan(ExpressionScan::new(ctx, rows)))
            }
            PlanNode::CreateTable(plan) => Self::create_table(ctx, plan),
            PlanNode::CreateIndex(plan) => Self::create_index(plan),
            PlanNode::CreateSequence(plan) => Self::create_sequence(ctx, plan),
            PlanNode::CreateView(plan) => Self::create_view(ctx, plan),
            PlanNode::Drop(plan) => Self::drop_object(ctx, plan),
            PlanNode::Truncate(plan) => Self::truncate(plan),
            PlanNode::Analyze(plan) => Self::analyze(ctx, plan),
            PlanNode::Reindex(plan) => Self::reindex(plan),
            PlanNode::Scan(plan) => Self::scan(ctx, plan),
            PlanNode::Project(plan) => Self::project(ctx, plan),
            PlanNode::Filter(plan) => Self::filter(ctx, plan),
            PlanNode::Sort(plan) => Self::sort(ctx, plan),
            PlanNode::Limit(plan) => Self::limit(ctx, plan),
            PlanNode::TopN(plan) => Self::top_n(ctx, plan),
            PlanNode::CrossProduct(plan) => Self::cross_product(ctx, plan),
            PlanNode::Join(plan) => Self::join(ctx, plan),
            PlanNode::Aggregate(plan) => Self::aggregate(ctx, plan),
            PlanNode::Union(plan) => Self::union(ctx, plan),
            PlanNode::Spool(planner::Spool { source }) => {
                Ok(Self::Spool(Spool::new(Self::new(ctx, *source)?)?))
            }
            PlanNode::Insert(plan) => Self::insert(ctx, plan),
            PlanNode::Update(plan) => Self::update(ctx, plan),
            PlanNode::Delete(plan) => Self::delete(ctx, plan),
        }
    }

    fn empty_result() -> Self {
        Self::ExpressionScan(ExpressionScan::no_rows())
    }
}

impl Iterator for ExecutorNode<'_> {
    type Item = ExecutorResult<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_row() {
            Ok(row) => Some(Ok(row)),
            Err(NodeError::Error(e)) => Some(Err(e)),
            Err(NodeError::EndOfRows) => None,
        }
    }
}

pub struct Spool {
    rows: std::vec::IntoIter<Row>,
}

impl Spool {
    fn new(source: ExecutorNode) -> ExecutorResult<Self> {
        let rows: Vec<_> = source.into_iter().collect::<ExecutorResult<_>>()?;
        Ok(Self {
            rows: rows.into_iter(),
        })
    }
}

impl Node for Spool {
    fn next_row(&mut self) -> Output {
        self.rows.next().into_output()
    }
}
