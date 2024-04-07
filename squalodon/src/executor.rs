mod expression;
mod modification;
mod query;

use crate::{
    connection::ConnectionContext,
    parser::ObjectKind,
    planner::{self, Expression, PlanNode},
    storage, CatalogError, Row, Storage, StorageError, Value,
};
use modification::{Delete, Insert, Update};
use query::{
    CrossProduct, Filter, FunctionScan, HashAggregate, Limit, Project, SeqScan, Sort,
    UngroupedAggregate, Union, Values,
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

pub struct Executor<'txn, 'db, T: Storage>(ExecutorNode<'txn, 'db, T>);

impl<'txn, 'db, T: Storage> Executor<'txn, 'db, T> {
    pub fn new(
        ctx: &'txn ConnectionContext<'txn, 'db, T>,
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
    Values(Values<'txn>),
    SeqScan(SeqScan<'txn>),
    FunctionScan(FunctionScan<'txn, 'db, T>),
    Project(Project<'txn, 'db, T>),
    Filter(Filter<'txn, 'db, T>),
    Sort(Sort),
    Limit(Limit<'txn, 'db, T>),
    CrossProduct(CrossProduct<'txn, 'db, T>),
    UngroupedAggregate(UngroupedAggregate),
    HashAggregate(HashAggregate),
    Union(Union<'txn, 'db, T>),
    Spool(Spool),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
}

impl<'txn, 'db, T: Storage> ExecutorNode<'txn, 'db, T> {
    fn new(
        ctx: &'txn ConnectionContext<'txn, 'db, T>,
        plan_node: PlanNode<'txn, 'db, T>,
    ) -> ExecutorResult<Self> {
        let executor = match plan_node {
            PlanNode::Explain(plan) => {
                let rows = plan
                    .explain()
                    .into_iter()
                    .map(|row| vec![Expression::Constant(Value::Text(row))])
                    .collect();
                Self::Values(Values::new(ctx, rows))
            }
            PlanNode::CreateTable(create_table) => {
                let result = ctx.catalog().create_table(
                    &create_table.name,
                    &create_table.columns,
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
            PlanNode::Values(planner::Values { rows }) => Self::Values(Values::new(ctx, rows)),
            PlanNode::Scan(planner::Scan::SeqScan { table }) => Self::SeqScan(SeqScan::new(table)),
            PlanNode::Scan(planner::Scan::FunctionScan { source, function }) => {
                let source = Self::new(ctx, *source)?;
                Self::FunctionScan(FunctionScan::new(ctx, source, function.fn_ptr))
            }
            PlanNode::Project(planner::Project { source, exprs }) => Self::Project(Project {
                ctx,
                source: Self::new(ctx, *source)?.into(),
                exprs,
            }),
            PlanNode::Filter(planner::Filter { source, cond }) => Self::Filter(Filter {
                ctx,
                source: Self::new(ctx, *source)?.into(),
                cond,
            }),
            PlanNode::Sort(planner::Sort { source, order_by }) => {
                Self::Sort(Sort::new(ctx, Self::new(ctx, *source)?, order_by)?)
            }
            PlanNode::Limit(planner::Limit {
                source,
                limit,
                offset,
            }) => Self::Limit(Limit::new(ctx, Self::new(ctx, *source)?, limit, offset)?),
            PlanNode::CrossProduct(planner::CrossProduct { left, right }) => Self::CrossProduct(
                CrossProduct::new(Self::new(ctx, *left)?, Self::new(ctx, *right)?)?,
            ),
            PlanNode::Aggregate(planner::Aggregate::Ungrouped { source, column_ops }) => {
                Self::UngroupedAggregate(UngroupedAggregate::new(
                    Self::new(ctx, *source)?,
                    column_ops,
                )?)
            }
            PlanNode::Aggregate(planner::Aggregate::Hash {
                source,
                column_ops: column_roles,
            }) => Self::HashAggregate(HashAggregate::new(Self::new(ctx, *source)?, column_roles)?),
            PlanNode::Union(planner::Union { left, right }) => Self::Union(Union {
                left: Box::new(Self::new(ctx, *left)?),
                right: Box::new(Self::new(ctx, *right)?),
            }),
            PlanNode::Spool(planner::Spool { source }) => {
                Self::Spool(Spool::new(Self::new(ctx, *source)?)?)
            }
            PlanNode::Insert(planner::Insert { source, table }) => {
                Self::Insert(Insert::new(Box::new(Self::new(ctx, *source)?), table)?)
            }
            PlanNode::Update(planner::Update { source, table }) => {
                Self::Update(Update::new(Box::new(Self::new(ctx, *source)?), table)?)
            }
            PlanNode::Delete(planner::Delete { source, table }) => {
                Self::Delete(Delete::new(Box::new(Self::new(ctx, *source)?), table)?)
            }
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
            Self::Union(e) => e.next_row(),
            Self::Spool(e) => e.next_row(),
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

pub struct Spool {
    rows: std::vec::IntoIter<Row>,
}

impl Spool {
    fn new<T: Storage>(source: ExecutorNode<'_, '_, T>) -> ExecutorResult<Self> {
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
