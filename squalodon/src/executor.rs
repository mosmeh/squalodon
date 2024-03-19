use crate::{
    catalog::TableFnPtr,
    connection::QueryContext,
    memcomparable::MemcomparableSerde,
    parser::{BinaryOp, UnaryOp},
    planner::{self, Expression, OrderBy, PlanNode},
    storage::{self, Table},
    CatalogError, Row, Storage, StorageError, Value,
};

#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    #[error("Out of range")]
    OutOfRange,

    #[error("Type error")]
    TypeError,

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
        ctx: &'txn QueryContext<'txn, 'db, T>,
        plan: PlanNode<'txn, 'db, T>,
    ) -> ExecutorResult<Self> {
        ExecutorNode::new(ctx, plan).map(Self)
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
    Insert(Insert<'txn, 'db, T>),
    Update(Update<'txn, 'db, T>),
    Delete(Delete<'txn, 'db, T>),
}

impl<'txn, 'db, T: Storage> ExecutorNode<'txn, 'db, T> {
    fn new(
        ctx: &'txn QueryContext<'txn, 'db, T>,
        plan: PlanNode<'txn, 'db, T>,
    ) -> ExecutorResult<Self> {
        let executor = match plan {
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
            PlanNode::Insert(insert) => Self::Insert(Insert {
                source: Box::new(Self::new(ctx, *insert.source)?),
                table: insert.table,
            }),
            PlanNode::Update(update) => Self::Update(Update {
                source: Box::new(Self::new(ctx, *update.source)?),
                table: update.table,
            }),
            PlanNode::Delete(delete) => Self::Delete(Delete {
                source: Box::new(Self::new(ctx, *delete.source)?),
                table: delete.table,
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

#[derive(Default)]
struct Values {
    iter: std::vec::IntoIter<Vec<Expression>>,
}

impl Values {
    fn new(rows: Vec<Vec<Expression>>) -> Self {
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

impl Node for Values {
    fn next_row(&mut self) -> Output {
        self.iter
            .next()
            .map(|row| -> ExecutorResult<Row> {
                let columns = row
                    .into_iter()
                    .map(|expr| expr.eval(&Row::empty()))
                    .collect::<ExecutorResult<_>>()?;
                Ok(Row(columns))
            })
            .into_output()
    }
}

struct SeqScan<'a> {
    iter: Box<dyn Iterator<Item = storage::StorageResult<Row>> + 'a>,
}

impl<'a> SeqScan<'a> {
    fn new<T: Storage>(table: Table<'a, '_, T>) -> Self {
        Self { iter: table.scan() }
    }
}

impl Node for SeqScan<'_> {
    fn next_row(&mut self) -> Output {
        self.iter.next().into_output()
    }
}

struct FunctionScan<'txn, 'db, T: Storage> {
    ctx: &'txn QueryContext<'txn, 'db, T>,
    source: Box<ExecutorNode<'txn, 'db, T>>,
    fn_ptr: TableFnPtr<T>,
    rows: Box<dyn Iterator<Item = Row>>,
}

impl<'txn, 'db, T: Storage> FunctionScan<'txn, 'db, T> {
    fn new(
        ctx: &'txn QueryContext<'txn, 'db, T>,
        source: ExecutorNode<'txn, 'db, T>,
        fn_ptr: TableFnPtr<T>,
    ) -> Self {
        Self {
            ctx,
            source: source.into(),
            fn_ptr,
            rows: Box::new(std::iter::empty()),
        }
    }
}

impl<T: Storage> Node for FunctionScan<'_, '_, T> {
    fn next_row(&mut self) -> Output {
        loop {
            if let Some(row) = self.rows.next() {
                return Ok(row);
            }
            let row = self.source.next_row()?;
            self.rows = (self.fn_ptr)(self.ctx, &row)?;
        }
    }
}

struct Project<'txn, 'db, T: Storage> {
    source: Box<ExecutorNode<'txn, 'db, T>>,
    exprs: Vec<Expression>,
}

impl<T: Storage> Node for Project<'_, '_, T> {
    fn next_row(&mut self) -> Output {
        let row = self.source.next_row()?;
        let columns = self
            .exprs
            .iter()
            .map(|expr| expr.eval(&row))
            .collect::<ExecutorResult<_>>()?;
        Ok(Row(columns))
    }
}

struct Filter<'txn, 'db, T: Storage> {
    source: Box<ExecutorNode<'txn, 'db, T>>,
    cond: Expression,
}

impl<T: Storage> Node for Filter<'_, '_, T> {
    fn next_row(&mut self) -> Output {
        loop {
            let row = self.source.next_row()?;
            match self.cond.eval(&row)? {
                Value::Boolean(true) => return Ok(row),
                Value::Boolean(false) => continue,
                _ => return Err(ExecutorError::TypeError.into()),
            }
        }
    }
}

struct Sort {
    rows: Box<dyn Iterator<Item = Row>>,
}

impl Sort {
    fn new<T: Storage>(
        source: ExecutorNode<'_, '_, T>,
        order_by: Vec<OrderBy>,
    ) -> ExecutorResult<Self> {
        let mut rows = Vec::new();
        for row in source {
            let row = row?;
            let mut sort_key = Vec::new();
            for order_by in &order_by {
                let value = order_by.expr.eval(&row)?;
                MemcomparableSerde::new()
                    .order(order_by.order)
                    .null_order(order_by.null_order)
                    .serialize_into(&value, &mut sort_key);
            }
            rows.push((row, sort_key));
        }
        rows.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));
        Ok(Self {
            rows: Box::new(rows.into_iter().map(|(row, _)| row)),
        })
    }
}

impl Node for Sort {
    fn next_row(&mut self) -> Output {
        self.rows.next().into_output()
    }
}

struct Limit<'txn, 'db, T: Storage> {
    source: Box<ExecutorNode<'txn, 'db, T>>,
    limit: Option<usize>,
    offset: usize,
    cursor: usize,
}

impl<'txn, 'db, T: Storage> Limit<'txn, 'db, T> {
    fn new(
        source: ExecutorNode<'txn, 'db, T>,
        limit: Option<Expression>,
        offset: Option<Expression>,
    ) -> ExecutorResult<Self> {
        fn eval(expr: Option<Expression>) -> ExecutorResult<Option<usize>> {
            let Some(expr) = expr else {
                return Ok(None);
            };
            let Value::Integer(i) = expr.eval(&Row::empty())? else {
                return Err(ExecutorError::TypeError);
            };
            i.try_into()
                .map_or_else(|_| Err(ExecutorError::OutOfRange), |i| Ok(Some(i)))
        }

        Ok(Self {
            source: source.into(),
            limit: eval(limit)?,
            offset: eval(offset)?.unwrap_or(0),
            cursor: 0,
        })
    }
}

impl<T: Storage> Node for Limit<'_, '_, T> {
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

struct CrossProduct<'txn, 'db, T: Storage> {
    left_source: Box<ExecutorNode<'txn, 'db, T>>,
    left_row: Option<Row>,
    right_rows: Vec<Row>,
    right_cursor: usize,
}

impl<'txn, 'db, T: Storage> CrossProduct<'txn, 'db, T> {
    fn new(
        left_source: ExecutorNode<'txn, 'db, T>,
        right_source: ExecutorNode<'txn, 'db, T>,
    ) -> ExecutorResult<Self> {
        Ok(Self {
            left_source: left_source.into(),
            left_row: None,
            right_rows: right_source.collect::<ExecutorResult<_>>()?,
            right_cursor: 0,
        })
    }
}

impl<T: Storage> Node for CrossProduct<'_, '_, T> {
    fn next_row(&mut self) -> Output {
        if self.right_rows.is_empty() {
            return Err(NodeError::EndOfRows);
        }
        loop {
            let left_row = match &self.left_row {
                Some(row) => row,
                None => self.left_row.insert(self.left_source.next_row()?),
            };
            if let Some(right_row) = self.right_rows.get(self.right_cursor) {
                let mut row = left_row.clone();
                row.0.extend(right_row.0.clone());
                self.right_cursor += 1;
                return Ok(row);
            }
            self.left_row = None;
            self.right_cursor = 0;
        }
    }
}

struct Insert<'txn, 'db, T: Storage> {
    source: Box<ExecutorNode<'txn, 'db, T>>,
    table: Table<'txn, 'db, T>,
}

impl<T: Storage> Node for Insert<'_, '_, T> {
    fn next_row(&mut self) -> Output {
        for row in self.source.by_ref() {
            self.table.insert(&row?)?;
        }
        Err(NodeError::EndOfRows)
    }
}

struct Update<'txn, 'db, T: Storage> {
    source: Box<ExecutorNode<'txn, 'db, T>>,
    table: Table<'txn, 'db, T>,
}

impl<T: Storage> Node for Update<'_, '_, T> {
    fn next_row(&mut self) -> Output {
        for row in self.source.by_ref() {
            self.table.update(&row?)?;
        }
        Err(NodeError::EndOfRows)
    }
}

struct Delete<'txn, 'db, T: Storage> {
    source: Box<ExecutorNode<'txn, 'db, T>>,
    table: Table<'txn, 'db, T>,
}

impl<T: Storage> Node for Delete<'_, '_, T> {
    fn next_row(&mut self) -> Output {
        for row in self.source.by_ref() {
            self.table.delete(&row?)?;
        }
        Err(NodeError::EndOfRows)
    }
}

impl Expression {
    fn eval(&self, row: &Row) -> ExecutorResult<Value> {
        let value = match self {
            Self::Constact(v) => v.clone(),
            Self::ColumnRef { column } => row[column].clone(),
            Self::UnaryOp { op, expr } => eval_unary_op(*op, row, expr)?,
            Self::BinaryOp { op, lhs, rhs } => eval_binary_op(*op, row, lhs, rhs)?,
        };
        Ok(value)
    }
}

fn eval_unary_op(op: UnaryOp, row: &Row, expr: &Expression) -> ExecutorResult<Value> {
    let expr = expr.eval(row)?;
    match (op, expr) {
        (UnaryOp::Plus, Value::Integer(v)) => Ok(Value::Integer(v)),
        (UnaryOp::Plus, Value::Real(v)) => Ok(Value::Real(v)),
        (UnaryOp::Minus, Value::Integer(v)) => Ok(Value::Integer(-v)),
        (UnaryOp::Minus, Value::Real(v)) => Ok(Value::Real(-v)),
        (UnaryOp::Not, Value::Boolean(v)) => Ok(Value::Boolean(!v)),
        _ => Err(ExecutorError::TypeError),
    }
}

fn eval_binary_op(
    op: BinaryOp,
    row: &Row,
    lhs: &Expression,
    rhs: &Expression,
) -> ExecutorResult<Value> {
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
                Err(ExecutorError::TypeError)
            }
        }
        BinaryOp::Or => {
            if let (Value::Boolean(lhs), Value::Boolean(rhs)) = (lhs, rhs) {
                Ok(Value::Boolean(lhs || rhs))
            } else {
                Err(ExecutorError::TypeError)
            }
        }
        BinaryOp::Concat => {
            if let (Value::Text(lhs), Value::Text(rhs)) = (lhs, rhs) {
                Ok(Value::Text(lhs + &rhs))
            } else {
                Err(ExecutorError::TypeError)
            }
        }
    }
}

fn eval_binary_op_number(op: BinaryOp, lhs: Value, rhs: Value) -> ExecutorResult<Value> {
    match (lhs, rhs) {
        (Value::Integer(lhs), Value::Integer(rhs)) => eval_binary_op_integer(op, lhs, rhs),
        (Value::Real(lhs), Value::Real(rhs)) => Ok(eval_binary_op_real(op, lhs, rhs)),
        (Value::Real(lhs), Value::Integer(rhs)) => Ok(eval_binary_op_real(op, lhs, rhs as f64)),
        (Value::Integer(lhs), Value::Real(rhs)) => Ok(eval_binary_op_real(op, lhs as f64, rhs)),
        _ => Err(ExecutorError::TypeError),
    }
}

fn eval_binary_op_integer(op: BinaryOp, lhs: i64, rhs: i64) -> ExecutorResult<Value> {
    let f = match op {
        BinaryOp::Add => i64::checked_add,
        BinaryOp::Sub => i64::checked_sub,
        BinaryOp::Mul => i64::checked_mul,
        BinaryOp::Div => i64::checked_div,
        BinaryOp::Mod => i64::checked_rem,
        _ => unreachable!(),
    };
    f(lhs, rhs).map_or_else(|| Err(ExecutorError::OutOfRange), |v| Ok(Value::Integer(v)))
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
