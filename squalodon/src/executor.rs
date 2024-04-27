mod expression;
mod mutation;
mod query;

use crate::{
    connection::ConnectionContext,
    parser::ObjectKind,
    planner::{self, Expression, PlanNode},
    storage, CatalogError, Row, StorageError, Value,
};
use mutation::{Delete, Insert, Update};
use query::{
    AggregateOp, ApplyAggregateOp, CrossProduct, Filter, FunctionScan, HashAggregate, HashJoin,
    IndexOnlyScan, IndexScan, Limit, Project, SeqScan, Sort, TopN, UngroupedAggregate, Union,
    Values,
};

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

pub struct Executor<'a>(ExecutorNode<'a>);

impl<'a> Executor<'a> {
    pub fn new(ctx: &'a ConnectionContext<'a>, plan_node: PlanNode<'a>) -> ExecutorResult<Self> {
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
    Values<'a>
    SeqScan<'a>
    IndexScan<'a>
    IndexOnlyScan<'a>
    FunctionScan<'a>
    Project<'a>
    Filter<'a>
    Sort
    Limit<'a>
    TopN
    CrossProduct<'a>
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
    fn new(ctx: &'a ConnectionContext<'a>, plan_node: PlanNode<'a>) -> ExecutorResult<Self> {
        let executor = match plan_node {
            PlanNode::Explain(explain) => {
                let rows = explain
                    .dump()
                    .into_iter()
                    .map(|row| vec![Expression::Constant(Value::Text(row))])
                    .collect();
                Self::Values(Values::new(ctx, rows))
            }
            PlanNode::CreateTable(create_table) => {
                let result = ctx.catalog().create_table(
                    &create_table.name,
                    &create_table.columns,
                    &create_table.primary_keys,
                    &create_table.constraints,
                );
                match result {
                    Ok(_) => (),
                    Err(CatalogError::DuplicateEntry(_, _)) if create_table.if_not_exists => (),
                    Err(e) => return Err(e.into()),
                }
                for index in create_table.create_indexes {
                    ctx.catalog().create_index(
                        index.name,
                        index.table_name,
                        &index.column_indexes,
                        index.is_unique,
                    )?;
                }
                Self::Values(Values::one_empty_row())
            }
            PlanNode::CreateIndex(create_index) => {
                ctx.catalog().create_index(
                    create_index.name,
                    create_index.table_name,
                    &create_index.column_indexes,
                    create_index.is_unique,
                )?;
                Self::Values(Values::one_empty_row())
            }
            PlanNode::Drop(drop_object) => {
                let catalog = ctx.catalog();
                let result = match drop_object.0.kind {
                    ObjectKind::Table => catalog.drop_table(&drop_object.0.name),
                    ObjectKind::Index => catalog.drop_index(&drop_object.0.name),
                };
                match result {
                    Ok(()) => (),
                    Err(CatalogError::UnknownEntry(_, _)) if drop_object.0.if_exists => (),
                    Err(e) => return Err(e.into()),
                }
                Self::Values(Values::one_empty_row())
            }
            PlanNode::Truncate(planner::Truncate { table }) => {
                table.truncate()?;
                Self::Values(Values::one_empty_row())
            }
            PlanNode::Reindex(reindex) => {
                match reindex {
                    planner::Reindex::Table(table) => table.reindex()?,
                    planner::Reindex::Index(index) => index.reindex()?,
                }
                Self::Values(Values::one_empty_row())
            }
            PlanNode::Values(planner::Values { rows, .. }) => {
                let rows = rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|expr| expr.into_executable(&[]))
                            .collect()
                    })
                    .collect();
                Self::Values(Values::new(ctx, rows))
            }
            PlanNode::Scan(planner::Scan::Seq { table, .. }) => Self::SeqScan(SeqScan::new(table)),
            PlanNode::Scan(planner::Scan::Index { index, range, .. }) => {
                let range = (
                    range.0.as_ref().map(Vec::as_slice),
                    range.1.as_ref().map(Vec::as_slice),
                );
                Self::IndexScan(IndexScan::new(index, range))
            }
            PlanNode::Scan(planner::Scan::IndexOnly { index, range, .. }) => {
                let range = (
                    range.0.as_ref().map(Vec::as_slice),
                    range.1.as_ref().map(Vec::as_slice),
                );
                Self::IndexOnlyScan(IndexOnlyScan::new(index, range))
            }
            PlanNode::Scan(planner::Scan::Function {
                source, function, ..
            }) => {
                let source = Self::new(ctx, *source)?;
                Self::FunctionScan(FunctionScan::new(ctx, source, function.fn_ptr))
            }
            PlanNode::Project(planner::Project { source, outputs }) => {
                let source_outputs = source.outputs();
                let exprs = outputs
                    .into_iter()
                    .map(|(_, expr)| expr.into_executable(&source_outputs))
                    .collect();
                Self::Project(Project {
                    ctx,
                    source: Self::new(ctx, *source)?.into(),
                    exprs,
                })
            }
            PlanNode::Filter(planner::Filter { source, conjuncts }) => {
                let outputs = source.outputs();
                let conjuncts = conjuncts
                    .into_iter()
                    .map(|conjunct| conjunct.into_executable(&outputs))
                    .collect();
                Self::Filter(Filter {
                    ctx,
                    source: Self::new(ctx, *source)?.into(),
                    conjuncts,
                })
            }
            PlanNode::Sort(planner::Sort { source, order_by }) => {
                let outputs = source.outputs();
                let order_by = order_by
                    .into_iter()
                    .map(|order_by| order_by.into_executable(&outputs))
                    .collect();
                Self::Sort(Sort::new(ctx, Self::new(ctx, *source)?, order_by)?)
            }
            PlanNode::Limit(planner::Limit {
                source,
                limit,
                offset,
            }) => {
                let outputs = source.outputs();
                let limit = limit.map(|expr| expr.into_executable(&outputs));
                let offset = offset.map(|expr| expr.into_executable(&outputs));
                Self::Limit(Limit::new(ctx, Self::new(ctx, *source)?, limit, offset)?)
            }
            PlanNode::TopN(planner::TopN {
                source,
                limit,
                offset,
                order_by,
            }) => {
                let outputs = source.outputs();
                let limit = limit.into_executable(&outputs);
                let offset = offset.map(|expr| expr.into_executable(&outputs));
                let order_by = order_by
                    .into_iter()
                    .map(|order_by| order_by.into_executable(&outputs))
                    .collect();
                Self::TopN(TopN::new(
                    ctx,
                    Self::new(ctx, *source)?,
                    limit,
                    offset,
                    order_by,
                )?)
            }
            PlanNode::CrossProduct(planner::CrossProduct { left, right }) => Self::CrossProduct(
                CrossProduct::new(Self::new(ctx, *left)?, Self::new(ctx, *right)?)?,
            ),
            PlanNode::Join(planner::Join::Hash { left, right, keys }) => {
                let left_outputs = left.outputs();
                let right_outputs = right.outputs();
                let keys = keys
                    .into_iter()
                    .map(|(left_key, right_key)| {
                        (
                            left_key.into_executable(&left_outputs),
                            right_key.into_executable(&right_outputs),
                        )
                    })
                    .collect();
                Self::HashJoin(HashJoin::new(
                    ctx,
                    Self::new(ctx, *left)?,
                    Self::new(ctx, *right)?,
                    keys,
                )?)
            }
            PlanNode::Aggregate(planner::Aggregate::Ungrouped { source, ops, .. }) => {
                let outputs = source.outputs();
                let ops = ops
                    .into_iter()
                    .map(|op| ApplyAggregateOp::from_plan(&op, &outputs))
                    .collect();
                Self::UngroupedAggregate(UngroupedAggregate::new(Self::new(ctx, *source)?, ops)?)
            }
            PlanNode::Aggregate(planner::Aggregate::Hash { source, ops, .. }) => {
                let outputs = source.outputs();
                let ops = ops
                    .into_iter()
                    .map(|op| AggregateOp::from_plan(&op, &outputs))
                    .collect();
                Self::HashAggregate(HashAggregate::new(Self::new(ctx, *source)?, ops)?)
            }
            PlanNode::Union(planner::Union { left, right, .. }) => Self::Union(Union {
                left: Box::new(Self::new(ctx, *left)?),
                right: Box::new(Self::new(ctx, *right)?),
            }),
            PlanNode::Spool(planner::Spool { source }) => {
                Self::Spool(Spool::new(Self::new(ctx, *source)?)?)
            }
            PlanNode::Insert(planner::Insert { source, table, .. }) => {
                Self::Insert(Insert::new(Self::new(ctx, *source)?, table)?)
            }
            PlanNode::Update(planner::Update { source, table, .. }) => {
                Self::Update(Update::new(Self::new(ctx, *source)?, table)?)
            }
            PlanNode::Delete(planner::Delete { source, table, .. }) => {
                Self::Delete(Delete::new(Self::new(ctx, *source)?, table)?)
            }
        };
        Ok(executor)
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
