use super::{ExecutionContext, ExecutorNode, ExecutorResult, IntoOutput, Node, Output};
use crate::{
    memcomparable::MemcomparableSerde,
    planner::{self, ExecutableExpression, ExecutableOrderBy},
    ExecutorError, Row, Value,
};
use std::collections::BinaryHeap;

pub struct Sort {
    rows: Box<dyn Iterator<Item = Row>>,
}

impl Sort {
    fn new(
        ctx: &ExecutionContext,
        source: ExecutorNode,
        order_by: Vec<ExecutableOrderBy>,
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

pub struct TopN {
    rows: Box<dyn Iterator<Item = Row>>,
}

impl TopN {
    fn new(
        ctx: &ExecutionContext,
        source: ExecutorNode,
        limit: ExecutableExpression,
        offset: Option<ExecutableExpression>,
        order_by: Vec<ExecutableOrderBy>,
    ) -> ExecutorResult<Self> {
        fn eval(
            ctx: &ExecutionContext,
            expr: Option<ExecutableExpression>,
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

impl<'a> ExecutorNode<'a> {
    pub fn sort(ctx: &'a ExecutionContext, plan: planner::Sort<'a>) -> ExecutorResult<Self> {
        let planner::Sort { source, order_by } = plan;
        let outputs = source.outputs();
        let order_by = order_by
            .into_iter()
            .map(|order_by| order_by.into_executable(&outputs))
            .collect();
        Ok(Self::Sort(Sort::new(
            ctx,
            Self::new(ctx, *source)?,
            order_by,
        )?))
    }

    pub fn top_n(ctx: &'a ExecutionContext, plan: planner::TopN<'a>) -> ExecutorResult<Self> {
        let planner::TopN {
            source,
            limit,
            offset,
            order_by,
        } = plan;
        let outputs = source.outputs();
        let limit = limit.into_executable(&outputs);
        let offset = offset.map(|expr| expr.into_executable(&outputs));
        let order_by = order_by
            .into_iter()
            .map(|order_by| order_by.into_executable(&outputs))
            .collect();
        Ok(Self::TopN(TopN::new(
            ctx,
            Self::new(ctx, *source)?,
            limit,
            offset,
            order_by,
        )?))
    }
}
