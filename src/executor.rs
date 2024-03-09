use crate::{
    planner::{self, ColumnIndex, Expression, OrderBy, PlanNode},
    serde::SerdeOptions,
    storage::{Catalog, SchemaAwareTransaction, StorageError, TableId, Transaction},
    BinaryOp, Value,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Out of range")]
    OutOfRange,

    #[error("Type error")]
    TypeError,

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
}

type Result<T> = std::result::Result<T, Error>;

trait Node {
    fn next_row(&mut self) -> Output;
}

#[derive(Debug, thiserror::Error)]
enum NodeError {
    #[error(transparent)]
    Error(#[from] Error),

    // HACK: This is not a real error, but treating it as one allows us to use
    // the ? operator to exit early when reaching the end of the rows.
    #[error("End of rows")]
    EndOfRows,
}

type Output = std::result::Result<Vec<Value>, NodeError>;

trait IntoOutput {
    fn into_output(self) -> Output;
}

impl IntoOutput for Result<Vec<Value>> {
    fn into_output(self) -> Output {
        self.map_err(NodeError::Error)
    }
}

impl IntoOutput for Option<Vec<Value>> {
    fn into_output(self) -> Output {
        self.ok_or(NodeError::EndOfRows)
    }
}

impl IntoOutput for Result<Option<Vec<Value>>> {
    fn into_output(self) -> Output {
        match self {
            Ok(Some(row)) => Ok(row),
            Ok(None) => Err(NodeError::EndOfRows),
            Err(e) => Err(NodeError::Error(e)),
        }
    }
}

impl IntoOutput for Option<Result<Vec<Value>>> {
    fn into_output(self) -> Output {
        match self {
            Some(Ok(row)) => Ok(row),
            Some(Err(e)) => Err(NodeError::Error(e)),
            None => Err(NodeError::EndOfRows),
        }
    }
}

pub struct Executor<'a>(ExecutorNode<'a>);

impl<'a> Executor<'a> {
    pub fn new<T: Transaction>(
        txn: &'a mut SchemaAwareTransaction<T>,
        catalog: &mut Catalog,
        plan: PlanNode,
    ) -> Result<Self> {
        ExecutorNode::new(txn, catalog, plan).map(Self)
    }
}

impl Iterator for Executor<'_> {
    type Item = Result<Vec<Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next_row() {
            Ok(row) => Some(Ok(row)),
            Err(NodeError::Error(e)) => Some(Err(e)),
            Err(NodeError::EndOfRows) => None,
        }
    }
}

enum ExecutorNode<'a> {
    SeqScan(SeqScan<'a>),
    Project(Project<'a>),
    Filter(Filter<'a>),
    Sort(Sort<'a>),
    Limit(Limit<'a>),
    OneRow(OneRow),
    NoRow,
}

impl<'a> ExecutorNode<'a> {
    fn new<T: Transaction>(
        txn: &'a mut SchemaAwareTransaction<T>,
        catalog: &mut Catalog,
        plan: PlanNode,
    ) -> Result<Self> {
        let executor = match plan {
            PlanNode::Explain(plan) => {
                println!("{plan}");
                Self::NoRow
            }
            PlanNode::CreateTable(create_table) => {
                catalog.create_table(
                    txn.inner_mut(),
                    create_table.0.name,
                    create_table.0.columns,
                )?;
                Self::NoRow
            }
            PlanNode::Insert(insert) => {
                let columns = insert
                    .exprs
                    .into_iter()
                    .map(|expr| expr.eval(&[]))
                    .collect::<Result<Vec<_>>>()?;
                txn.update(
                    insert.table,
                    &columns[insert.primary_key_column.0],
                    &columns,
                );
                Self::NoRow
            }
            PlanNode::Scan(planner::Scan::SeqScan { table, columns }) => {
                Self::SeqScan(SeqScan::new(txn, table, columns))
            }
            PlanNode::Project(planner::Project { source, exprs }) => Self::Project(Project {
                source: Self::new(txn, catalog, *source)?.into(),
                exprs,
            }),
            PlanNode::Filter(planner::Filter { source, cond }) => Self::Filter(Filter {
                source: Self::new(txn, catalog, *source)?.into(),
                cond,
            }),
            PlanNode::Sort(planner::Sort { source, order_by }) => {
                Self::Sort(Sort::new(Self::new(txn, catalog, *source)?, order_by))
            }
            PlanNode::Limit(planner::Limit {
                source,
                limit,
                offset,
            }) => Self::Limit(Limit::new(
                Self::new(txn, catalog, *source)?,
                limit,
                offset,
            )?),
            PlanNode::OneRow(one_row) => Self::OneRow(OneRow::new(one_row.columns)?),
        };
        Ok(executor)
    }
}

impl Node for ExecutorNode<'_> {
    fn next_row(&mut self) -> Output {
        match self {
            Self::SeqScan(e) => e.next_row(),
            Self::Project(e) => e.next_row(),
            Self::Filter(e) => e.next_row(),
            Self::Sort(e) => e.next_row(),
            Self::Limit(e) => e.next_row(),
            Self::OneRow(e) => e.next_row(),
            Self::NoRow => Err(NodeError::EndOfRows),
        }
    }
}

impl Iterator for ExecutorNode<'_> {
    type Item = Result<Vec<Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_row() {
            Ok(row) => Some(Ok(row)),
            Err(NodeError::Error(e)) => Some(Err(e)),
            Err(NodeError::EndOfRows) => None,
        }
    }
}

struct SeqScan<'a> {
    iter: Box<dyn Iterator<Item = Result<Vec<Value>>> + 'a>,
    columns: Vec<ColumnIndex>,
}

impl<'a> SeqScan<'a> {
    fn new<T: Transaction>(
        txn: &'a mut SchemaAwareTransaction<T>,
        table: TableId,
        columns: Vec<ColumnIndex>,
    ) -> Self {
        Self {
            iter: Box::new(txn.scan_table(table).map(|row| row.map_err(Into::into))),
            columns,
        }
    }
}

impl Node for SeqScan<'_> {
    fn next_row(&mut self) -> Output {
        let row = self.iter.next().into_output()?;
        let columns = self.columns.iter().map(|i| row[i.0].clone()).collect();
        Ok(columns)
    }
}

struct Project<'a> {
    source: Box<ExecutorNode<'a>>,
    exprs: Vec<Expression>,
}

impl Node for Project<'_> {
    fn next_row(&mut self) -> Output {
        let row = self.source.next_row()?;
        self.exprs
            .iter()
            .map(|expr| expr.eval(&row))
            .collect::<Result<Vec<_>>>()
            .into_output()
    }
}

struct Filter<'a> {
    source: Box<ExecutorNode<'a>>,
    cond: Expression,
}

impl Node for Filter<'_> {
    fn next_row(&mut self) -> Output {
        loop {
            let row = self.source.next_row()?;
            if self.cond.eval(&row)?.as_bool() {
                return Ok(row);
            }
        }
    }
}

enum Sort<'a> {
    Collect {
        source: Box<ExecutorNode<'a>>,
        order_by: Vec<OrderBy>,
    },
    Output {
        rows: Box<dyn Iterator<Item = Vec<Value>>>,
    },
}

impl<'a> Sort<'a> {
    fn new(source: ExecutorNode<'a>, order_by: Vec<OrderBy>) -> Self {
        Self::Collect {
            source: source.into(),
            order_by,
        }
    }
}

impl Node for Sort<'_> {
    fn next_row(&mut self) -> Output {
        match self {
            Self::Collect { source, order_by } => {
                let mut rows = Vec::new();
                for row in source {
                    let row = row?;
                    let mut sort_key = Vec::new();
                    for order_by in order_by.iter() {
                        let value = order_by.expr.eval(&row)?;
                        SerdeOptions::new()
                            .order(order_by.order)
                            .null_order(order_by.null_order)
                            .serialize_into(&value, &mut sort_key);
                    }
                    rows.push((row, sort_key));
                }
                rows.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));
                *self = Self::Output {
                    rows: Box::new(rows.into_iter().map(|(row, _)| row)),
                };
                self.next_row()
            }
            Self::Output { rows } => rows.next().into_output(),
        }
    }
}

struct Limit<'a> {
    source: Box<ExecutorNode<'a>>,
    limit: Option<usize>,
    offset: usize,
    cursor: usize,
}

impl<'a> Limit<'a> {
    fn new(
        source: ExecutorNode<'a>,
        limit: Option<Expression>,
        offset: Option<Expression>,
    ) -> Result<Self> {
        fn eval(expr: Option<Expression>) -> Result<Option<usize>> {
            let Some(expr) = expr else {
                return Ok(None);
            };
            let i = expr.eval(&[])?.as_integer().ok_or(Error::TypeError)?;
            i.try_into()
                .map_or_else(|_| Err(Error::OutOfRange), |i| Ok(Some(i)))
        }

        Ok(Self {
            source: source.into(),
            limit: eval(limit)?,
            offset: eval(offset)?.unwrap_or(0),
            cursor: 0,
        })
    }
}

impl Node for Limit<'_> {
    fn next_row(&mut self) -> Output {
        loop {
            let row = self.source.next_row()?;
            if self.cursor < self.offset {
                self.cursor += 1;
                continue;
            }
            match self.limit {
                Some(limit) if self.cursor < self.offset + limit => {
                    self.cursor += 1;
                    return Ok(row);
                }
                Some(_) => return Err(NodeError::EndOfRows),
                None => return Ok(row),
            }
        }
    }
}

#[derive(Default)]
struct OneRow {
    row: Option<Vec<Value>>,
}

impl OneRow {
    fn new(columns: Vec<Expression>) -> Result<Self> {
        let row = columns
            .into_iter()
            .map(|expr| expr.eval(&[]))
            .collect::<Result<_>>()?;
        Ok(Self { row: Some(row) })
    }
}

impl Node for OneRow {
    fn next_row(&mut self) -> Output {
        self.row.take().into_output()
    }
}

impl Expression {
    fn eval(&self, row: &[Value]) -> Result<Value> {
        let value = match self {
            Self::Constact(v) => v.clone(),
            Self::ColumnRef { column } => row[column.0].clone(),
            Self::BinaryOp { op, lhs, rhs } => {
                let lhs = lhs.eval(row)?;
                if lhs == Value::Null {
                    return Ok(Value::Null);
                }
                match op {
                    BinaryOp::And if !lhs.as_bool() => return Ok(Value::Boolean(false)),
                    BinaryOp::Or if lhs.as_bool() => return Ok(Value::Boolean(true)),
                    _ => (),
                }
                let rhs = rhs.eval(row)?;
                if rhs == Value::Null {
                    return Ok(Value::Null);
                }
                match (op, lhs, rhs) {
                    (BinaryOp::Add, Value::Integer(lhs), Value::Integer(rhs)) => {
                        Value::Integer(lhs.checked_add(rhs).ok_or(Error::OutOfRange)?)
                    }
                    (BinaryOp::Sub, Value::Integer(lhs), Value::Integer(rhs)) => {
                        Value::Integer(lhs.checked_sub(rhs).ok_or(Error::OutOfRange)?)
                    }
                    (BinaryOp::Mul, Value::Integer(lhs), Value::Integer(rhs)) => {
                        Value::Integer(lhs.checked_mul(rhs).ok_or(Error::OutOfRange)?)
                    }
                    (BinaryOp::Div, Value::Integer(lhs), Value::Integer(rhs)) => {
                        Value::Integer(lhs.checked_div(rhs).ok_or(Error::OutOfRange)?)
                    }
                    (BinaryOp::Mod, Value::Integer(lhs), Value::Integer(rhs)) => {
                        Value::Integer(lhs.checked_rem(rhs).ok_or(Error::OutOfRange)?)
                    }
                    (BinaryOp::Eq, lhs, rhs) => Value::Boolean(lhs == rhs),
                    (BinaryOp::Ne, lhs, rhs) => Value::Boolean(lhs != rhs),
                    (BinaryOp::Lt, lhs, rhs) => Value::Boolean(lhs < rhs),
                    (BinaryOp::Le, lhs, rhs) => Value::Boolean(lhs <= rhs),
                    (BinaryOp::Gt, lhs, rhs) => Value::Boolean(lhs > rhs),
                    (BinaryOp::Ge, lhs, rhs) => Value::Boolean(lhs >= rhs),
                    (BinaryOp::And, Value::Boolean(lhs), Value::Boolean(rhs)) => {
                        Value::Boolean(lhs && rhs)
                    }
                    (BinaryOp::Or, Value::Boolean(lhs), Value::Boolean(rhs)) => {
                        Value::Boolean(lhs || rhs)
                    }
                    _ => return Err(Error::TypeError),
                }
            }
        };
        Ok(value)
    }
}
