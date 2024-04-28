use super::{ConnectionContext, ExecutorNode, ExecutorResult, Node, NodeError, Output};
use crate::{
    memcomparable::MemcomparableSerde,
    parser::BinaryOp,
    planner::{self, CompareOp, ExecutableExpression, Join},
    Row, Value,
};
use std::collections::HashMap;

pub struct CrossProduct<'a> {
    outer_source: Box<ExecutorNode<'a>>,
    outer_row: Option<Row>,
    inner_rows: Vec<Row>,
    inner_cursor: usize,
}

impl<'a> CrossProduct<'a> {
    fn new(outer_source: ExecutorNode<'a>, inner_source: ExecutorNode<'a>) -> ExecutorResult<Self> {
        Ok(Self {
            outer_source: outer_source.into(),
            outer_row: None,
            inner_rows: inner_source.collect::<ExecutorResult<_>>()?,
            inner_cursor: 0,
        })
    }
}

impl Node for CrossProduct<'_> {
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
                let mut row = outer_row.0.clone().into_vec();
                row.extend(inner_row.0.iter().cloned());
                self.inner_cursor += 1;
                return Ok(Row::new(row));
            }
            self.outer_row = None;
            self.inner_cursor = 0;
        }
    }
}

pub struct NestedLoopJoin<'a> {
    ctx: &'a ConnectionContext<'a>,
    outer_source: Box<ExecutorNode<'a>>,
    outer_row: Option<JoinRow>,
    inner_rows: Vec<JoinRow>,
    inner_cursor: usize,
    comparisons: Vec<(BinaryOp, ExecutableExpression<'a>)>,
}

impl<'a> NestedLoopJoin<'a> {
    pub fn new(
        ctx: &'a ConnectionContext<'a>,
        outer_source: ExecutorNode<'a>,
        inner_source: ExecutorNode<'a>,
        comparisons: Vec<(
            CompareOp,
            ExecutableExpression<'a>,
            ExecutableExpression<'a>,
        )>,
    ) -> ExecutorResult<Self> {
        Ok(Self {
            ctx,
            outer_source: outer_source.into(),
            outer_row: None,
            inner_rows: inner_source
                .map(|row| {
                    let row = row?;
                    let keys = comparisons
                        .iter()
                        .map(|(_, _, k)| k.eval(ctx, &row))
                        .collect::<ExecutorResult<_>>()?;
                    Ok(JoinRow { row, keys })
                })
                .collect::<ExecutorResult<_>>()?,
            inner_cursor: 0,
            comparisons: comparisons
                .into_iter()
                .map(|(op, k, _)| (op.to_binary_op(), k))
                .collect(),
        })
    }
}

impl Node for NestedLoopJoin<'_> {
    fn next_row(&mut self) -> Output {
        if self.inner_rows.is_empty() {
            return Err(NodeError::EndOfRows);
        }
        'outer: loop {
            let outer_row = match &self.outer_row {
                Some(row) => row,
                None => {
                    let row = self.outer_source.next_row()?;
                    let keys = self
                        .comparisons
                        .iter()
                        .map(|(_, k)| k.eval(self.ctx, &row))
                        .collect::<ExecutorResult<_>>()?;
                    self.outer_row.insert(JoinRow { row, keys })
                }
            };
            if let Some(inner_row) = self.inner_rows.get(self.inner_cursor) {
                for ((lhs, rhs), (op, _)) in outer_row
                    .keys
                    .iter()
                    .zip(inner_row.keys.iter())
                    .zip(&self.comparisons)
                {
                    match op.eval_const(lhs, rhs)? {
                        Value::Boolean(true) => (),
                        Value::Null | Value::Boolean(false) => {
                            self.inner_cursor += 1;
                            continue 'outer;
                        }
                        _ => unreachable!(),
                    }
                }
                let mut row = outer_row.row.0.clone().into_vec();
                row.extend(inner_row.row.0.iter().cloned());
                self.inner_cursor += 1;
                return Ok(Row::new(row));
            }
            self.outer_row = None;
            self.inner_cursor = 0;
        }
    }
}

struct JoinRow {
    row: Row,
    keys: Box<[Value]>,
}

pub struct HashJoin<'a> {
    ctx: &'a ConnectionContext<'a>,
    outer_source: Box<ExecutorNode<'a>>,
    map: HashMap<Vec<u8>, Vec<Row>>,
    keys: Vec<ExecutableExpression<'a>>,
    outer_row: Option<Row>,
    inner_rows: std::vec::IntoIter<Row>,
}

impl<'a> HashJoin<'a> {
    fn new(
        ctx: &'a ConnectionContext,
        outer_source: ExecutorNode<'a>,
        inner_source: ExecutorNode<'a>,
        keys: Vec<(ExecutableExpression<'a>, ExecutableExpression<'a>)>,
    ) -> ExecutorResult<Self> {
        let mut map = HashMap::new();
        let serde = MemcomparableSerde::new();
        'row: for row in inner_source {
            let row = row?;
            let mut key = Vec::new();
            for (_, expr) in &keys {
                let value = expr.eval(ctx, &row)?;
                if value.is_null() {
                    continue 'row;
                }
                serde.serialize_into(&value, &mut key);
            }
            map.entry(key).or_insert_with(Vec::new).push(row);
        }
        Ok(Self {
            ctx,
            outer_source: outer_source.into(),
            map,
            keys: keys.into_iter().map(|(k, _)| k).collect(),
            outer_row: None,
            inner_rows: Vec::new().into_iter(),
        })
    }
}

impl Node for HashJoin<'_> {
    fn next_row(&mut self) -> Output {
        'outer: loop {
            let outer_row = match &self.outer_row {
                Some(row) => row,
                None => {
                    let outer_row = self.outer_row.insert(self.outer_source.next_row()?);
                    let serde = MemcomparableSerde::new();
                    let mut key = Vec::new();
                    for expr in &self.keys {
                        let value = expr.eval(self.ctx, outer_row)?;
                        if value.is_null() {
                            continue 'outer;
                        }
                        serde.serialize_into(&value, &mut key);
                    }
                    assert!(self.inner_rows.next().is_none());
                    if let Some(inner_rows) = self.map.get(&key) {
                        self.inner_rows = inner_rows.clone().into_iter();
                    }
                    outer_row
                }
            };
            if let Some(inner_row) = self.inner_rows.next() {
                let mut row = outer_row.0.clone().into_vec();
                row.extend(inner_row.0.iter().cloned());
                return Ok(Row::new(row));
            }
            self.outer_row = None;
        }
    }
}

impl<'a> ExecutorNode<'a> {
    pub fn cross_product(
        ctx: &'a ConnectionContext,
        plan: planner::CrossProduct<'a>,
    ) -> ExecutorResult<Self> {
        let planner::CrossProduct { left, right } = plan;
        Ok(Self::CrossProduct(CrossProduct::new(
            Self::new(ctx, *left)?,
            Self::new(ctx, *right)?,
        )?))
    }

    pub fn join(ctx: &'a ConnectionContext, plan: Join<'a>) -> ExecutorResult<Self> {
        match plan {
            Join::NestedLoop {
                left,
                right,
                comparisons,
            } => {
                let left_outputs = left.outputs();
                let right_outputs = right.outputs();
                let comparisons = comparisons
                    .into_iter()
                    .map(|(op, left_key, right_key)| {
                        (
                            op,
                            left_key.into_executable(&left_outputs),
                            right_key.into_executable(&right_outputs),
                        )
                    })
                    .collect();
                Ok(Self::NestedLoopJoin(NestedLoopJoin::new(
                    ctx,
                    Self::new(ctx, *left)?,
                    Self::new(ctx, *right)?,
                    comparisons,
                )?))
            }
            Join::Hash { left, right, keys } => {
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
                Ok(Self::HashJoin(HashJoin::new(
                    ctx,
                    Self::new(ctx, *left)?,
                    Self::new(ctx, *right)?,
                    keys,
                )?))
            }
        }
    }
}
