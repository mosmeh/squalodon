use super::{ConnectionContext, ExecutorNode, ExecutorResult, IntoOutput, Node, NodeError, Output};
use crate::{
    catalog::{AggregateInitFnPtr, Aggregator, TableFnPtr},
    memcomparable::MemcomparableSerde,
    planner::{self, ColumnId, Expression, OrderBy},
    rows::ColumnIndex,
    storage::{StorageResult, Table, Transaction},
    ExecutorError, Row, Value,
};
use std::collections::{BinaryHeap, HashMap, HashSet};

pub struct Values<'a> {
    rows: Box<dyn Iterator<Item = ExecutorResult<Row>> + 'a>,
}

impl<'a> Values<'a> {
    pub fn new<T>(
        ctx: &'a ConnectionContext<'a, T>,
        rows: Vec<Vec<Expression<'a, T, ColumnIndex>>>,
    ) -> Self {
        let rows = rows.into_iter().map(|row| {
            let columns = row
                .into_iter()
                .map(|expr| expr.eval(ctx, &Row::empty()))
                .collect::<ExecutorResult<_>>()?;
            Ok(Row(columns))
        });
        Self {
            rows: Box::new(rows),
        }
    }

    pub fn one_empty_row() -> Self {
        Self {
            rows: Box::new(vec![Ok(Row::empty())].into_iter()),
        }
    }
}

impl Node for Values<'_> {
    fn next_row(&mut self) -> Output {
        self.rows.next().into_output()
    }
}

pub struct SeqScan<'a> {
    iter: Box<dyn Iterator<Item = StorageResult<Row>> + 'a>,
}

impl<'a> SeqScan<'a> {
    pub fn new<T: Transaction>(table: Table<'a, T>) -> Self {
        Self { iter: table.scan() }
    }
}

impl Node for SeqScan<'_> {
    fn next_row(&mut self) -> Output {
        self.iter.next().into_output()
    }
}

pub struct FunctionScan<'a, T> {
    ctx: &'a ConnectionContext<'a, T>,
    source: Box<ExecutorNode<'a, T>>,
    fn_ptr: TableFnPtr<T>,
    rows: Box<dyn Iterator<Item = Row> + 'a>,
}

impl<'a, T> FunctionScan<'a, T> {
    pub fn new(
        ctx: &'a ConnectionContext<'a, T>,
        source: ExecutorNode<'a, T>,
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

impl<T> Node for FunctionScan<'_, T> {
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

pub struct Project<'a, T> {
    pub ctx: &'a ConnectionContext<'a, T>,
    pub source: Box<ExecutorNode<'a, T>>,
    pub exprs: Vec<Expression<'a, T, ColumnIndex>>,
}

impl<T> Node for Project<'_, T> {
    fn next_row(&mut self) -> Output {
        let row = self.source.next_row()?;
        let columns = self
            .exprs
            .iter()
            .map(|expr| expr.eval(self.ctx, &row))
            .collect::<ExecutorResult<_>>()?;
        Ok(Row(columns))
    }
}

pub struct Filter<'a, T> {
    pub ctx: &'a ConnectionContext<'a, T>,
    pub source: Box<ExecutorNode<'a, T>>,
    pub conjuncts: Vec<Expression<'a, T, ColumnIndex>>,
}

impl<T> Node for Filter<'_, T> {
    fn next_row(&mut self) -> Output {
        loop {
            let row = self.source.next_row()?;
            if self.is_match(self.ctx, &row)? {
                return Ok(row);
            }
        }
    }
}

impl<T> Filter<'_, T> {
    fn is_match(&self, ctx: &ConnectionContext<'_, T>, row: &Row) -> ExecutorResult<bool> {
        for conjunct in &self.conjuncts {
            match conjunct.eval(ctx, row)? {
                Value::Boolean(true) => {}
                Value::Null | Value::Boolean(false) => return Ok(false),
                _ => return Err(ExecutorError::TypeError),
            }
        }
        Ok(true)
    }
}

pub struct Sort {
    rows: Box<dyn Iterator<Item = Row>>,
}

impl Sort {
    pub fn new<T>(
        ctx: &ConnectionContext<'_, T>,
        source: ExecutorNode<'_, T>,
        order_by: Vec<OrderBy<T, ColumnIndex>>,
    ) -> ExecutorResult<Self> {
        let mut rows = Vec::new();
        for row in source {
            let row = row?;
            let mut sort_key = Vec::new();
            for order_by in &order_by {
                let value = order_by.expr.eval(ctx, &row)?;
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

pub struct Limit<'a, T> {
    source: Box<ExecutorNode<'a, T>>,
    limit: Option<usize>,
    offset: usize,
    cursor: usize,
}

impl<'a, T> Limit<'a, T> {
    pub fn new(
        ctx: &'a ConnectionContext<'a, T>,
        source: ExecutorNode<'a, T>,
        limit: Option<Expression<T, ColumnIndex>>,
        offset: Option<Expression<T, ColumnIndex>>,
    ) -> ExecutorResult<Self> {
        fn eval<T>(
            ctx: &ConnectionContext<'_, T>,
            expr: Option<Expression<T, ColumnIndex>>,
        ) -> ExecutorResult<Option<usize>> {
            let Some(expr) = expr else {
                return Ok(None);
            };
            let Value::Integer(i) = expr.eval(ctx, &Row::empty())? else {
                return Err(ExecutorError::TypeError);
            };
            i.try_into()
                .map_or(Err(ExecutorError::OutOfRange), |i| Ok(Some(i)))
        }

        Ok(Self {
            source: source.into(),
            limit: eval(ctx, limit)?,
            offset: eval(ctx, offset)?.unwrap_or(0),
            cursor: 0,
        })
    }
}

impl<T> Node for Limit<'_, T> {
    fn next_row(&mut self) -> Output {
        loop {
            let row = self.source.next_row()?;
            if self.cursor < self.offset {
                self.cursor += 1;
                continue;
            }
            return match self.limit {
                Some(limit) if self.cursor < self.offset + limit => {
                    self.cursor += 1;
                    Ok(row)
                }
                Some(_) => Err(NodeError::EndOfRows),
                None => Ok(row),
            };
        }
    }
}

pub struct TopN {
    rows: Box<dyn Iterator<Item = Row>>,
}

impl TopN {
    pub fn new<T>(
        ctx: &ConnectionContext<'_, T>,
        source: ExecutorNode<'_, T>,
        limit: Expression<T, ColumnIndex>,
        offset: Option<Expression<T, ColumnIndex>>,
        order_by: Vec<OrderBy<T, ColumnIndex>>,
    ) -> ExecutorResult<Self> {
        fn eval<T>(
            ctx: &ConnectionContext<'_, T>,
            expr: Option<Expression<T, ColumnIndex>>,
        ) -> ExecutorResult<usize> {
            let Some(expr) = expr else {
                return Ok(0);
            };
            let Value::Integer(i) = expr.eval(ctx, &Row::empty())? else {
                return Err(ExecutorError::TypeError);
            };
            i.try_into().map_err(|_| ExecutorError::OutOfRange)
        }

        let limit = eval(ctx, Some(limit))?;
        let offset = eval(ctx, offset)?;
        let mut heap = BinaryHeap::with_capacity(limit + offset);
        for row in source {
            let row = row?;
            let mut sort_key = Vec::new();
            for order_by in &order_by {
                let value = order_by.expr.eval(ctx, &row)?;
                MemcomparableSerde::new()
                    .order(order_by.order)
                    .null_order(order_by.null_order)
                    .serialize_into(&value, &mut sort_key);
            }
            heap.push(TopNRow { row, sort_key });
            if heap.len() > limit + offset {
                // BinaryHeap is a max heap, so this pops the last element in
                // the sorted order.
                heap.pop().unwrap();
            }
        }
        let rows = heap
            .into_sorted_vec()
            .into_iter()
            .skip(offset)
            .map(|TopNRow { row, .. }| row);
        Ok(Self {
            rows: Box::new(rows),
        })
    }
}

impl Node for TopN {
    fn next_row(&mut self) -> Output {
        self.rows.next().into_output()
    }
}

struct TopNRow {
    row: Row,
    sort_key: Vec<u8>,
}

impl PartialEq for TopNRow {
    fn eq(&self, other: &Self) -> bool {
        self.sort_key == other.sort_key
    }
}

impl Eq for TopNRow {}

impl PartialOrd for TopNRow {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TopNRow {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sort_key.cmp(&other.sort_key)
    }
}

pub struct CrossProduct<'a, T> {
    outer_source: Box<ExecutorNode<'a, T>>,
    outer_row: Option<Row>,
    inner_rows: Vec<Row>,
    inner_cursor: usize,
}

impl<'a, T> CrossProduct<'a, T> {
    pub fn new(
        outer_source: ExecutorNode<'a, T>,
        inner_source: ExecutorNode<'a, T>,
    ) -> ExecutorResult<Self> {
        Ok(Self {
            outer_source: outer_source.into(),
            outer_row: None,
            inner_rows: inner_source.collect::<ExecutorResult<_>>()?,
            inner_cursor: 0,
        })
    }
}

impl<T> Node for CrossProduct<'_, T> {
    fn next_row(&mut self) -> Output {
        if self.inner_rows.is_empty() {
            return Err(NodeError::EndOfRows);
        }
        loop {
            let outer_row = match &self.outer_row {
                Some(row) => row,
                None => self.outer_row.insert(self.outer_source.next_row()?),
            };
            if let Some(inner_row) = self.inner_rows.get(self.inner_cursor) {
                let mut row = outer_row.clone();
                row.0.extend(inner_row.0.clone());
                self.inner_cursor += 1;
                return Ok(row);
            }
            self.outer_row = None;
            self.inner_cursor = 0;
        }
    }
}

pub struct UngroupedAggregate {
    row: Option<Row>,
}

impl UngroupedAggregate {
    pub fn new<T>(source: ExecutorNode<'_, T>, ops: Vec<ApplyAggregateOp>) -> ExecutorResult<Self> {
        let mut aggregators: Vec<_> = ops.iter().map(GroupAggregator::new).collect();
        for row in source {
            let row = row?;
            for (op, aggregator) in ops.iter().zip(aggregators.iter_mut()) {
                aggregator.update(&row[op.input])?;
            }
        }
        let columns = aggregators
            .into_iter()
            .map(GroupAggregator::finish)
            .collect();
        Ok(Self {
            row: Some(Row(columns)),
        })
    }
}

impl Node for UngroupedAggregate {
    fn next_row(&mut self) -> Output {
        self.row.take().into_output()
    }
}

pub struct HashAggregate {
    rows: Box<dyn Iterator<Item = Row>>,
}

impl HashAggregate {
    pub fn new<T>(source: ExecutorNode<'_, T>, ops: Vec<AggregateOp>) -> ExecutorResult<Self> {
        let mut aggregated = Vec::new();
        let mut group_by = Vec::new();
        for op in &ops {
            match op {
                AggregateOp::ApplyAggregate(op) => aggregated.push(op),
                AggregateOp::GroupBy { target } => group_by.push(*target),
            }
        }

        let serde = MemcomparableSerde::new();
        let mut groups = HashMap::new();
        for row in source {
            let row = row?;
            let mut key = Vec::new();
            for column_index in &group_by {
                serde.serialize_into(&row[column_index], &mut key);
            }
            let aggregators = groups.entry(key).or_insert_with(|| {
                aggregated
                    .iter()
                    .map(|op| GroupAggregator::new(op))
                    .collect::<Vec<_>>()
            });
            for (aggregator, op) in aggregators.iter_mut().zip(&aggregated) {
                aggregator.update(&row[op.input])?;
            }
        }
        let rows = groups.into_iter().map(move |(key, aggregators)| {
            let mut group_by_iter = serde.deserialize_seq_from(&key);
            let mut aggregator_iter = aggregators.into_iter().map(GroupAggregator::finish);
            let columns = ops
                .iter()
                .map(|op| {
                    match op {
                        AggregateOp::ApplyAggregate(_) => aggregator_iter.next(),
                        AggregateOp::GroupBy { .. } => group_by_iter.next().transpose().unwrap(),
                    }
                    .unwrap()
                })
                .collect();
            Row(columns)
        });
        Ok(Self {
            rows: Box::new(rows),
        })
    }
}

impl Node for HashAggregate {
    fn next_row(&mut self) -> Output {
        self.rows.next().into_output()
    }
}

pub struct ApplyAggregateOp {
    pub init: AggregateInitFnPtr,
    pub is_distinct: bool,
    pub input: ColumnIndex,
}

impl ApplyAggregateOp {
    pub fn from_plan(plan: &planner::ApplyAggregateOp, inputs: &[ColumnId]) -> Self {
        Self {
            init: plan.function.init,
            is_distinct: plan.is_distinct,
            input: plan.input.to_index(inputs),
        }
    }
}

pub enum AggregateOp {
    ApplyAggregate(ApplyAggregateOp),
    GroupBy { target: ColumnIndex },
}

impl AggregateOp {
    pub fn from_plan(plan: &planner::AggregateOp, inputs: &[ColumnId]) -> Self {
        match plan {
            planner::AggregateOp::ApplyAggregate(op) => {
                Self::ApplyAggregate(ApplyAggregateOp::from_plan(op, inputs))
            }
            planner::AggregateOp::GroupBy { target, .. } => Self::GroupBy {
                target: target.to_index(inputs),
            },
        }
    }
}

struct GroupAggregator {
    aggregator: Box<dyn Aggregator>,
    dedup_set: Option<HashSet<Value>>,
}

impl GroupAggregator {
    fn new(op: &ApplyAggregateOp) -> Self {
        Self {
            aggregator: (op.init)(),
            dedup_set: op.is_distinct.then(HashSet::new),
        }
    }

    fn update(&mut self, value: &Value) -> ExecutorResult<()> {
        if let Some(set) = &mut self.dedup_set {
            if !set.insert(value.clone()) {
                return Ok(());
            }
        }
        self.aggregator.update(value)
    }

    fn finish(self) -> Value {
        self.aggregator.finish()
    }
}

pub struct Union<'a, T> {
    pub left: Box<ExecutorNode<'a, T>>,
    pub right: Box<ExecutorNode<'a, T>>,
}

impl<T> Node for Union<'_, T> {
    fn next_row(&mut self) -> Output {
        self.left.next_row().or_else(|_| self.right.next_row())
    }
}
