use crate::{
    planner::{self, ColumnIndex, Expression, PlanKind, PlanNode},
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

pub struct Executor<'a>(ExecutorKind<'a>);

impl<'a> Executor<'a> {
    pub fn new<T: Transaction>(
        txn: &'a mut SchemaAwareTransaction<T>,
        catalog: &mut Catalog,
        plan: PlanNode,
    ) -> Result<Self> {
        ExecutorKind::new(txn, catalog, plan).map(Self)
    }
}

impl Iterator for Executor<'_> {
    type Item = Result<Vec<Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

enum ExecutorKind<'a> {
    SeqScan(SeqScan<'a>),
    Project(Project<'a>),
    Filter(Filter<'a>),
    OneRow(OneRow),
    NoRow,
}

impl<'a> ExecutorKind<'a> {
    fn new<T: Transaction>(
        txn: &'a mut SchemaAwareTransaction<T>,
        catalog: &mut Catalog,
        plan: PlanNode,
    ) -> Result<Self> {
        let executor = match plan.kind {
            PlanKind::Explain(plan) => {
                println!("{plan}");
                Self::NoRow
            }
            PlanKind::CreateTable(create_table) => {
                catalog.create_table(
                    txn.inner_mut(),
                    create_table.0.name,
                    create_table.0.columns,
                )?;
                Self::NoRow
            }
            PlanKind::Insert(insert) => {
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
            PlanKind::Scan(planner::Scan::SeqScan { table, columns }) => {
                Self::SeqScan(SeqScan::new(txn, table, columns))
            }
            PlanKind::Project(planner::Project { source, exprs }) => Self::Project(Project {
                source: Self::new(txn, catalog, *source)?.into(),
                exprs,
            }),
            PlanKind::Filter(planner::Filter { source, cond }) => Self::Filter(Filter {
                source: Self::new(txn, catalog, *source)?.into(),
                cond,
            }),
            PlanKind::OneRow(one_row) => Self::OneRow(OneRow::new(one_row.columns)?),
        };
        Ok(executor)
    }
}

impl Iterator for ExecutorKind<'_> {
    type Item = Result<Vec<Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::SeqScan(e) => e.next(),
            Self::Project(e) => e.next(),
            Self::Filter(e) => e.next(),
            Self::OneRow(e) => e.next().map(Ok),
            Self::NoRow => None,
        }
    }
}

struct SeqScan<'a> {
    iter: Box<dyn Iterator<Item = std::result::Result<Vec<Value>, StorageError>> + 'a>,
    columns: Vec<ColumnIndex>,
}

impl<'a> SeqScan<'a> {
    fn new<T: Transaction>(
        txn: &'a mut SchemaAwareTransaction<T>,
        table: TableId,
        columns: Vec<ColumnIndex>,
    ) -> Self {
        Self {
            iter: Box::new(txn.scan_table(table)),
            columns,
        }
    }
}

impl Iterator for SeqScan<'_> {
    type Item = Result<Vec<Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = match self.iter.next()? {
            Ok(row) => row,
            Err(e) => return Some(Err(e.into())),
        };
        let columns = self.columns.iter().map(|i| row[i.0].clone()).collect();
        Some(Ok(columns))
    }
}

struct Project<'a> {
    source: Box<ExecutorKind<'a>>,
    exprs: Vec<Expression>,
}

impl Iterator for Project<'_> {
    type Item = Result<Vec<Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = match self.source.next()? {
            Ok(row) => row,
            Err(e) => return Some(Err(e)),
        };
        let columns: Result<Vec<_>> = self.exprs.iter().map(|expr| expr.eval(&row)).collect();
        Some(columns)
    }
}

struct Filter<'a> {
    source: Box<ExecutorKind<'a>>,
    cond: Expression,
}

impl Iterator for Filter<'_> {
    type Item = Result<Vec<Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let row = match self.source.next()? {
                Ok(row) => row,
                Err(e) => return Some(Err(e)),
            };
            match self.cond.eval(&row) {
                Ok(x) if x.as_bool() => return Some(Ok(row)),
                Ok(_) => continue,
                Err(e) => return Some(Err(e)),
            }
        }
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

impl Iterator for OneRow {
    type Item = Vec<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        self.row.take()
    }
}
