use crate::{
    memcomparable::MemcomparableSerde,
    planner::{self, ColumnIndex, Expression, OrderBy, PlanNode},
    storage::{self, StorageError, TableId, Transaction},
    BinaryOp, KeyValueStore, Value,
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

pub type Result<T> = std::result::Result<T, Error>;

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

impl From<storage::Error> for NodeError {
    fn from(e: storage::Error) -> Self {
        Self::Error(e.into())
    }
}

type Output = std::result::Result<Vec<Value>, NodeError>;

trait IntoOutput {
    fn into_output(self) -> Output;
}

impl<E: Into<Error>> IntoOutput for std::result::Result<Vec<Value>, E> {
    fn into_output(self) -> Output {
        self.map_err(|e| NodeError::Error(e.into()))
    }
}

impl IntoOutput for Option<Vec<Value>> {
    fn into_output(self) -> Output {
        self.ok_or(NodeError::EndOfRows)
    }
}

impl<E: Into<Error>> IntoOutput for std::result::Result<Option<Vec<Value>>, E> {
    fn into_output(self) -> Output {
        match self {
            Ok(Some(row)) => Ok(row),
            Ok(None) => Err(NodeError::EndOfRows),
            Err(e) => Err(NodeError::Error(e.into())),
        }
    }
}

impl<E: Into<Error>> IntoOutput for Option<std::result::Result<Vec<Value>, E>> {
    fn into_output(self) -> Output {
        match self {
            Some(Ok(row)) => Ok(row),
            Some(Err(e)) => Err(NodeError::Error(e.into())),
            None => Err(NodeError::EndOfRows),
        }
    }
}

pub struct Executor<'txn, 'storage, T: KeyValueStore>(ExecutorNode<'txn, 'storage, T>);

impl<'txn, 'storage, T: KeyValueStore> Executor<'txn, 'storage, T> {
    pub fn new(txn: &'txn Transaction<'storage, T>, plan: PlanNode) -> Result<Self> {
        ExecutorNode::new(txn, plan).map(Self)
    }
}

impl<T: KeyValueStore> Iterator for Executor<'_, '_, T> {
    type Item = Result<Vec<Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next_row() {
            Ok(row) => Some(Ok(row)),
            Err(NodeError::Error(e)) => Some(Err(e)),
            Err(NodeError::EndOfRows) => None,
        }
    }
}

enum ExecutorNode<'txn, 'storage, T: KeyValueStore> {
    Insert(Insert<'txn, 'storage, T>),
    Update(Update<'txn, 'storage, T>),
    Delete(Delete<'txn, 'storage, T>),
    Constant(Constant),
    SeqScan(SeqScan<'txn>),
    Project(Project<'txn, 'storage, T>),
    Filter(Filter<'txn, 'storage, T>),
    Sort(Sort<'txn, 'storage, T>),
    Limit(Limit<'txn, 'storage, T>),
}

impl<'txn, 'storage, T: KeyValueStore> ExecutorNode<'txn, 'storage, T> {
    fn new(txn: &'txn Transaction<'storage, T>, plan: PlanNode) -> Result<Self> {
        let executor = match plan {
            PlanNode::Explain(plan) => {
                let rows = plan
                    .explain()
                    .into_iter()
                    .map(|row| vec![Value::Text(row)])
                    .collect();
                Self::Constant(Constant::new(rows))
            }
            PlanNode::CreateTable(create_table) => {
                txn.create_table(&create_table.0.name, &create_table.0.columns)?;
                Self::Constant(Constant::one_empty_row())
            }
            PlanNode::DropTable(drop_table) => {
                txn.drop_table(&drop_table.0.name)?;
                Self::Constant(Constant::one_empty_row())
            }
            PlanNode::Insert(insert) => Self::Insert(Insert {
                txn,
                source: Box::new(Self::new(txn, *insert.source)?),
                table: insert.table,
                primary_key_column: insert.primary_key_column,
            }),
            PlanNode::Update(update) => Self::Update(Update {
                txn,
                source: Box::new(Self::new(txn, *update.source)?),
                table: update.table,
                primary_key_column: update.primary_key_column,
            }),
            PlanNode::Delete(delete) => Self::Delete(Delete {
                txn,
                source: Box::new(Self::new(txn, *delete.source)?),
                table: delete.table,
                primary_key_column: delete.primary_key_column,
            }),
            PlanNode::Constant(planner::Constant { rows }) => Self::Constant(Constant::new(rows)),
            PlanNode::Scan(planner::Scan::SeqScan { table, columns }) => {
                Self::SeqScan(SeqScan::new(txn, table, columns))
            }
            PlanNode::Project(planner::Project { source, exprs }) => Self::Project(Project {
                source: Self::new(txn, *source)?.into(),
                exprs,
            }),
            PlanNode::Filter(planner::Filter { source, cond }) => Self::Filter(Filter {
                source: Self::new(txn, *source)?.into(),
                cond,
            }),
            PlanNode::Sort(planner::Sort { source, order_by }) => {
                Self::Sort(Sort::new(Self::new(txn, *source)?, order_by))
            }
            PlanNode::Limit(planner::Limit {
                source,
                limit,
                offset,
            }) => Self::Limit(Limit::new(Self::new(txn, *source)?, limit, offset)?),
        };
        Ok(executor)
    }
}

impl<T: KeyValueStore> Node for ExecutorNode<'_, '_, T> {
    fn next_row(&mut self) -> Output {
        match self {
            Self::SeqScan(e) => e.next_row(),
            Self::Project(e) => e.next_row(),
            Self::Filter(e) => e.next_row(),
            Self::Sort(e) => e.next_row(),
            Self::Limit(e) => e.next_row(),
            Self::Insert(e) => e.next_row(),
            Self::Update(e) => e.next_row(),
            Self::Delete(e) => e.next_row(),
            Self::Constant(e) => e.next_row(),
        }
    }
}

impl<T: KeyValueStore> Iterator for ExecutorNode<'_, '_, T> {
    type Item = Result<Vec<Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_row() {
            Ok(row) => Some(Ok(row)),
            Err(NodeError::Error(e)) => Some(Err(e)),
            Err(NodeError::EndOfRows) => None,
        }
    }
}

struct Insert<'txn, 'storage, T: KeyValueStore> {
    txn: &'txn Transaction<'storage, T>,
    source: Box<ExecutorNode<'txn, 'storage, T>>,
    table: TableId,
    primary_key_column: ColumnIndex,
}

impl<T: KeyValueStore> Node for Insert<'_, '_, T> {
    fn next_row(&mut self) -> Output {
        for row in self.source.by_ref() {
            let row = row?;
            let primary_key = &row[self.primary_key_column.0];
            self.txn.insert(self.table, primary_key, &row)?;
        }
        Err(NodeError::EndOfRows)
    }
}

struct Update<'txn, 'storage, T: KeyValueStore> {
    txn: &'txn Transaction<'storage, T>,
    source: Box<ExecutorNode<'txn, 'storage, T>>,
    table: TableId,
    primary_key_column: ColumnIndex,
}

impl<T: KeyValueStore> Node for Update<'_, '_, T> {
    fn next_row(&mut self) -> Output {
        for row in self.source.by_ref() {
            let row = row?;
            let primary_key = &row[self.primary_key_column.0];
            self.txn.update(self.table, primary_key, &row)?;
        }
        Err(NodeError::EndOfRows)
    }
}

struct Delete<'txn, 'storage, T: KeyValueStore> {
    txn: &'txn Transaction<'storage, T>,
    source: Box<ExecutorNode<'txn, 'storage, T>>,
    table: TableId,
    primary_key_column: ColumnIndex,
}

impl<T: KeyValueStore> Node for Delete<'_, '_, T> {
    fn next_row(&mut self) -> Output {
        for row in self.source.by_ref() {
            let row = row?;
            let primary_key = &row[self.primary_key_column.0];
            self.txn.delete(self.table, primary_key);
        }
        Err(NodeError::EndOfRows)
    }
}

#[derive(Default)]
struct Constant {
    iter: std::vec::IntoIter<Vec<Value>>,
}

impl Constant {
    fn new(rows: Vec<Vec<Value>>) -> Self {
        Self {
            iter: rows.into_iter(),
        }
    }

    fn one_empty_row() -> Self {
        Self {
            iter: vec![Vec::new()].into_iter(),
        }
    }
}

impl Node for Constant {
    fn next_row(&mut self) -> Output {
        self.iter.next().into_output()
    }
}

impl Expression {
    fn eval(&self, row: &[Value]) -> Result<Value> {
        let value = match self {
            Self::Constact(v) => v.clone(),
            Self::ColumnRef { column } => row[column.0].clone(),
            Self::BinaryOp { op, lhs, rhs } => eval_binary_op(*op, row, lhs, rhs)?,
        };
        Ok(value)
    }
}

struct SeqScan<'a> {
    iter: Box<dyn Iterator<Item = storage::Result<Vec<Value>>> + 'a>,
    columns: Vec<ColumnIndex>,
}

impl<'a> SeqScan<'a> {
    fn new<T: KeyValueStore>(
        txn: &'a Transaction<T>,
        table: TableId,
        columns: Vec<ColumnIndex>,
    ) -> Self {
        Self {
            iter: txn.scan_table(table),
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

struct Project<'txn, 'storage, T: KeyValueStore> {
    source: Box<ExecutorNode<'txn, 'storage, T>>,
    exprs: Vec<Expression>,
}

impl<T: KeyValueStore> Node for Project<'_, '_, T> {
    fn next_row(&mut self) -> Output {
        let row = self.source.next_row()?;
        self.exprs
            .iter()
            .map(|expr| expr.eval(&row))
            .collect::<Result<Vec<_>>>()
            .into_output()
    }
}

struct Filter<'txn, 'storage, T: KeyValueStore> {
    source: Box<ExecutorNode<'txn, 'storage, T>>,
    cond: Expression,
}

impl<T: KeyValueStore> Node for Filter<'_, '_, T> {
    fn next_row(&mut self) -> Output {
        loop {
            let row = self.source.next_row()?;
            match self.cond.eval(&row)? {
                Value::Boolean(true) => return Ok(row),
                Value::Boolean(false) => continue,
                _ => return Err(Error::TypeError.into()),
            }
        }
    }
}

enum Sort<'txn, 'storage, T: KeyValueStore> {
    Collect {
        source: Box<ExecutorNode<'txn, 'storage, T>>,
        order_by: Vec<OrderBy>,
    },
    Output {
        rows: Box<dyn Iterator<Item = Vec<Value>>>,
    },
}

impl<'txn, 'storage, T: KeyValueStore> Sort<'txn, 'storage, T> {
    fn new(source: ExecutorNode<'txn, 'storage, T>, order_by: Vec<OrderBy>) -> Self {
        Self::Collect {
            source: source.into(),
            order_by,
        }
    }
}

impl<T: KeyValueStore> Node for Sort<'_, '_, T> {
    fn next_row(&mut self) -> Output {
        match self {
            Self::Collect { source, order_by } => {
                let mut rows = Vec::new();
                for row in source {
                    let row = row?;
                    let mut sort_key = Vec::new();
                    for order_by in order_by.iter() {
                        let value = order_by.expr.eval(&row)?;
                        MemcomparableSerde::new()
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

struct Limit<'txn, 'storage, T: KeyValueStore> {
    source: Box<ExecutorNode<'txn, 'storage, T>>,
    limit: Option<usize>,
    offset: usize,
    cursor: usize,
}

impl<'txn, 'storage, T: KeyValueStore> Limit<'txn, 'storage, T> {
    fn new(
        source: ExecutorNode<'txn, 'storage, T>,
        limit: Option<Expression>,
        offset: Option<Expression>,
    ) -> Result<Self> {
        fn eval(expr: Option<Expression>) -> Result<Option<usize>> {
            let Some(expr) = expr else {
                return Ok(None);
            };
            let Value::Integer(i) = expr.eval(&[])? else {
                return Err(Error::TypeError);
            };
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

impl<T: KeyValueStore> Node for Limit<'_, '_, T> {
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

fn eval_binary_op(
    op: BinaryOp,
    row: &[Value],
    lhs: &Expression,
    rhs: &Expression,
) -> Result<Value> {
    let lhs = lhs.eval(row)?;
    if lhs == Value::Null {
        return Ok(Value::Null);
    }
    match (op, &lhs) {
        (BinaryOp::And, Value::Boolean(false)) => return Ok(Value::Boolean(false)),
        (BinaryOp::Or, Value::Boolean(true)) => return Ok(Value::Boolean(true)),
        _ => (),
    }
    let rhs = rhs.eval(row)?;
    if rhs == Value::Null {
        return Ok(Value::Null);
    }
    match op {
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
            eval_binary_op_number(op, lhs, rhs)
        }
        BinaryOp::Eq => Ok(Value::Boolean(lhs == rhs)),
        BinaryOp::Ne => Ok(Value::Boolean(lhs != rhs)),
        BinaryOp::Lt => Ok(Value::Boolean(lhs < rhs)),
        BinaryOp::Le => Ok(Value::Boolean(lhs <= rhs)),
        BinaryOp::Gt => Ok(Value::Boolean(lhs > rhs)),
        BinaryOp::Ge => Ok(Value::Boolean(lhs >= rhs)),
        BinaryOp::And => {
            if let (Value::Boolean(lhs), Value::Boolean(rhs)) = (lhs, rhs) {
                Ok(Value::Boolean(lhs && rhs))
            } else {
                Err(Error::TypeError)
            }
        }
        BinaryOp::Or => {
            if let (Value::Boolean(lhs), Value::Boolean(rhs)) = (lhs, rhs) {
                Ok(Value::Boolean(lhs || rhs))
            } else {
                Err(Error::TypeError)
            }
        }
    }
}

fn eval_binary_op_number(op: BinaryOp, lhs: Value, rhs: Value) -> Result<Value> {
    match (lhs, rhs) {
        (Value::Integer(lhs), Value::Integer(rhs)) => eval_binary_op_integer(op, lhs, rhs),
        (Value::Real(lhs), Value::Real(rhs)) => Ok(eval_binary_op_real(op, lhs, rhs)),
        (Value::Real(lhs), Value::Integer(rhs)) => Ok(eval_binary_op_real(op, lhs, rhs as f64)),
        (Value::Integer(lhs), Value::Real(rhs)) => Ok(eval_binary_op_real(op, lhs as f64, rhs)),
        _ => Err(Error::TypeError),
    }
}

fn eval_binary_op_integer(op: BinaryOp, lhs: i64, rhs: i64) -> Result<Value> {
    let f = match op {
        BinaryOp::Add => i64::checked_add,
        BinaryOp::Sub => i64::checked_sub,
        BinaryOp::Mul => i64::checked_mul,
        BinaryOp::Div => i64::checked_div,
        BinaryOp::Mod => i64::checked_rem,
        _ => unreachable!(),
    };
    f(lhs, rhs).map_or_else(|| Err(Error::OutOfRange), |v| Ok(Value::Integer(v)))
}

fn eval_binary_op_real(op: BinaryOp, lhs: f64, rhs: f64) -> Value {
    let f = match op {
        BinaryOp::Add => std::ops::Add::add,
        BinaryOp::Sub => std::ops::Sub::sub,
        BinaryOp::Mul => std::ops::Mul::mul,
        BinaryOp::Div => std::ops::Div::div,
        BinaryOp::Mod => std::ops::Rem::rem,
        _ => unreachable!(),
    };
    Value::Real(f(lhs, rhs))
}
