mod expression;
mod modification;
mod query;

use crate::{
    catalog::CatalogRef,
    planner::{self, Expression, PlanNode},
    storage, CatalogError, Row, Storage, StorageError, Value,
};
use modification::{Delete, Insert, Update};
use query::{
    CrossProduct, Filter, FunctionScan, HashAggregate, Limit, Project, SeqScan, Sort,
    UngroupedAggregate, Values,
};

#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    #[error("Out of range")]
    OutOfRange,

    #[error("Type error")]
    TypeError,

    #[error("Subquery returned more than one row")]
    MultipleRowsFromSubquery,

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),
}

pub type ExecutorResult<T> = std::result::Result<T, ExecutorError>;

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

pub struct ExecutorContext<'txn, 'db, T: Storage> {
    catalog: CatalogRef<'txn, 'db, T>,
}

impl<'txn, 'db: 'txn, T: Storage> ExecutorContext<'txn, 'db, T> {
    pub fn new(catalog: CatalogRef<'txn, 'db, T>) -> Self {
        Self { catalog }
    }

    pub fn catalog(&self) -> &CatalogRef<'txn, 'db, T> {
        &self.catalog
    }
}

pub struct Executor<'txn, 'db, T: Storage>(ExecutorNode<'txn, 'db, T>);

impl<'txn, 'db, T: Storage> Executor<'txn, 'db, T> {
    pub fn new(
        ctx: &'txn ExecutorContext<'txn, 'db, T>,
        plan_node: PlanNode<'txn, 'db, T>,
    ) -> ExecutorResult<Self> {
        ExecutorNode::new(ctx, plan_node).map(Self)
    }
}

impl<T: Storage> Iterator for Executor<'_, '_, T> {
    type Item = ExecutorResult<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next_row() {
            Ok(row) => Some(Ok(row)),
            Err(NodeError::Error(e)) => Some(Err(e)),
            Err(NodeError::EndOfRows) => None,
        }
    }
}

enum ExecutorNode<'txn, 'db, T: Storage> {
    Values(Values),
    SeqScan(SeqScan<'txn>),
    FunctionScan(FunctionScan<'txn, 'db, T>),
    Project(Project<'txn, 'db, T>),
    Filter(Filter<'txn, 'db, T>),
    Sort(Sort),
    Limit(Limit<'txn, 'db, T>),
    CrossProduct(CrossProduct<'txn, 'db, T>),
    UngroupedAggregate(UngroupedAggregate),
    HashAggregate(HashAggregate),
    Insert(Insert<'txn, 'db, T>),
    Update(Update<'txn, 'db, T>),
    Delete(Delete<'txn, 'db, T>),
}

impl<'txn, 'db, T: Storage> ExecutorNode<'txn, 'db, T> {
    fn new(
        ctx: &'txn ExecutorContext<'txn, 'db, T>,
        plan_node: PlanNode<'txn, 'db, T>,
    ) -> ExecutorResult<Self> {
        let executor = match plan_node {
            PlanNode::Explain(plan) => {
                let rows = plan
                    .explain()
                    .into_iter()
                    .map(|row| vec![Expression::Constact(Value::Text(row))])
                    .collect();
                Self::Values(Values::new(rows))
            }
            PlanNode::CreateTable(create_table) => {
                ctx.catalog().create_table(
                    &create_table.name,
                    &create_table.columns,
                    &create_table.constraints,
                )?;
                Self::Values(Values::one_empty_row())
            }
            PlanNode::DropTable(drop_table) => {
                ctx.catalog().drop_table(&drop_table.name)?;
                Self::Values(Values::one_empty_row())
            }
            PlanNode::Values(planner::Values { rows }) => Self::Values(Values::new(rows)),
            PlanNode::Scan(planner::Scan::SeqScan { table }) => Self::SeqScan(SeqScan::new(table)),
            PlanNode::Scan(planner::Scan::FunctionScan { source, fn_ptr }) => {
                let source = Self::new(ctx, *source)?;
                Self::FunctionScan(FunctionScan::new(ctx, source, fn_ptr))
            }
            PlanNode::Project(planner::Project { source, exprs }) => Self::Project(Project {
                source: Self::new(ctx, *source)?.into(),
                exprs,
            }),
            PlanNode::Filter(planner::Filter { source, cond }) => Self::Filter(Filter {
                source: Self::new(ctx, *source)?.into(),
                cond,
            }),
            PlanNode::Sort(planner::Sort { source, order_by }) => {
                Self::Sort(Sort::new(Self::new(ctx, *source)?, order_by)?)
            }
            PlanNode::Limit(planner::Limit {
                source,
                limit,
                offset,
            }) => Self::Limit(Limit::new(Self::new(ctx, *source)?, limit, offset)?),
            PlanNode::CrossProduct(planner::CrossProduct { left, right }) => Self::CrossProduct(
                CrossProduct::new(Self::new(ctx, *left)?, Self::new(ctx, *right)?)?,
            ),
            PlanNode::Aggregate(planner::Aggregate::Ungrouped {
                source,
                init_functions,
            }) => Self::UngroupedAggregate(UngroupedAggregate::new(
                Self::new(ctx, *source)?,
                &init_functions,
            )?),
            PlanNode::Aggregate(planner::Aggregate::Hash {
                source,
                init_functions,
            }) => Self::HashAggregate(HashAggregate::new(
                Self::new(ctx, *source)?,
                &init_functions,
            )?),
            PlanNode::Insert(planner::Insert { source, table }) => Self::Insert(Insert {
                source: Box::new(Self::new(ctx, *source)?),
                table,
            }),
            PlanNode::Update(planner::Update { source, table }) => Self::Update(Update {
                source: Box::new(Self::new(ctx, *source)?),
                table,
            }),
            PlanNode::Delete(planner::Delete { source, table }) => Self::Delete(Delete {
                source: Box::new(Self::new(ctx, *source)?),
                table,
            }),
        };
        Ok(executor)
    }
}

impl<T: Storage> Node for ExecutorNode<'_, '_, T> {
    fn next_row(&mut self) -> Output {
        match self {
            Self::Values(e) => e.next_row(),
            Self::SeqScan(e) => e.next_row(),
            Self::FunctionScan(e) => e.next_row(),
            Self::Project(e) => e.next_row(),
            Self::Filter(e) => e.next_row(),
            Self::Sort(e) => e.next_row(),
            Self::Limit(e) => e.next_row(),
            Self::CrossProduct(e) => e.next_row(),
            Self::UngroupedAggregate(e) => e.next_row(),
            Self::HashAggregate(e) => e.next_row(),
            Self::Insert(e) => e.next_row(),
            Self::Update(e) => e.next_row(),
            Self::Delete(e) => e.next_row(),
        }
    }
}

impl<T: Storage> Iterator for ExecutorNode<'_, '_, T> {
    type Item = ExecutorResult<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_row() {
            Ok(row) => Some(Ok(row)),
            Err(NodeError::Error(e)) => Some(Err(e)),
            Err(NodeError::EndOfRows) => None,
        }
    }
}
