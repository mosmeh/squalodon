use super::{ExecutorContext, ExecutorNode, ExecutorResult, IntoOutput, Node, NodeError, Output};
use crate::{
    catalog::{AggregateInitFnPtr, Aggregator, TableFnPtr},
    memcomparable::MemcomparableSerde,
    planner::{AggregateColumn, Expression, OrderBy},
    rows::ColumnIndex,
    storage::{self, Table},
    ExecutorError, Row, Storage, Value,
};
use std::collections::HashMap;

#[derive(Default)]
pub struct Values {
    iter: std::vec::IntoIter<Vec<Expression>>,
}

impl Values {
    pub fn new(rows: Vec<Vec<Expression>>) -> Self {
        Self {
            iter: rows.into_iter(),
        }
    }

    pub fn one_empty_row() -> Self {
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

pub struct SeqScan<'a> {
    iter: Box<dyn Iterator<Item = storage::StorageResult<Row>> + 'a>,
}

impl<'a> SeqScan<'a> {
    pub fn new<T: Storage>(table: Table<'a, '_, T>) -> Self {
        Self { iter: table.scan() }
    }
}

impl Node for SeqScan<'_> {
    fn next_row(&mut self) -> Output {
        self.iter.next().into_output()
    }
}

pub struct FunctionScan<'txn, 'db, T: Storage> {
    ctx: &'txn ExecutorContext<'txn, 'db, T>,
    source: Box<ExecutorNode<'txn, 'db, T>>,
    fn_ptr: TableFnPtr<T>,
    rows: Box<dyn Iterator<Item = Row>>,
}

impl<'txn, 'db, T: Storage> FunctionScan<'txn, 'db, T> {
    pub fn new(
        ctx: &'txn ExecutorContext<'txn, 'db, T>,
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

pub struct Project<'txn, 'db, T: Storage> {
    pub source: Box<ExecutorNode<'txn, 'db, T>>,
    pub exprs: Vec<Expression>,
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

pub struct Filter<'txn, 'db, T: Storage> {
    pub source: Box<ExecutorNode<'txn, 'db, T>>,
    pub cond: Expression,
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

pub struct Sort {
    rows: Box<dyn Iterator<Item = Row>>,
}

impl Sort {
    pub fn new<T: Storage>(
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

pub struct Limit<'txn, 'db, T: Storage> {
    source: Box<ExecutorNode<'txn, 'db, T>>,
    limit: Option<usize>,
    offset: usize,
    cursor: usize,
}

impl<'txn, 'db, T: Storage> Limit<'txn, 'db, T> {
    pub fn new(
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

pub struct CrossProduct<'txn, 'db, T: Storage> {
    outer_source: Box<ExecutorNode<'txn, 'db, T>>,
    outer_row: Option<Row>,
    inner_rows: Vec<Row>,
    inner_cursor: usize,
}

impl<'txn, 'db, T: Storage> CrossProduct<'txn, 'db, T> {
    pub fn new(
        outer_source: ExecutorNode<'txn, 'db, T>,
        inner_source: ExecutorNode<'txn, 'db, T>,
    ) -> ExecutorResult<Self> {
        Ok(Self {
            outer_source: outer_source.into(),
            outer_row: None,
            inner_rows: inner_source.collect::<ExecutorResult<_>>()?,
            inner_cursor: 0,
        })
    }
}

impl<T: Storage> Node for CrossProduct<'_, '_, T> {
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
    pub fn new<T: Storage>(
        source: ExecutorNode<'_, '_, T>,
        init_functions: &[AggregateInitFnPtr],
    ) -> ExecutorResult<Self> {
        let mut aggs: Vec<_> = init_functions.iter().map(|init| init()).collect();
        for row in source {
            let row = row?;
            assert_eq!(row.0.len(), aggs.len());
            for (agg, value) in aggs.iter_mut().zip(&row.0) {
                agg.update(value)?;
            }
        }
        let row = Row(aggs.into_iter().map(|agg| agg.finish()).collect());
        Ok(Self { row: Some(row) })
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
    pub fn new<T: Storage>(
        source: ExecutorNode<'_, '_, T>,
        columns: Vec<AggregateColumn>,
    ) -> ExecutorResult<Self> {
        struct Group {
            aggregators: Vec<Box<dyn Aggregator>>,
            non_aggregated: Vec<Value>,
        }

        let mut aggregates = Vec::new();
        let mut group_by = Vec::new();
        let mut non_aggregated = Vec::new();
        for (i, column) in columns.iter().enumerate() {
            match column {
                AggregateColumn::Aggregated(init) => aggregates.push((ColumnIndex(i), init)),
                AggregateColumn::GroupBy => group_by.push(ColumnIndex(i)),
                AggregateColumn::NonAggregated => non_aggregated.push(ColumnIndex(i)),
            }
        }

        let serde = MemcomparableSerde::new();
        let mut groups = HashMap::new();
        for row in source {
            let row = row?;
            assert_eq!(row.0.len(), columns.len());
            let mut key = Vec::new();
            for column_index in &group_by {
                serde.serialize_into(&row[column_index], &mut key);
            }
            let group = groups.entry(key).or_insert_with(|| Group {
                aggregators: aggregates.iter().map(|(_, init)| init()).collect(),
                non_aggregated: Vec::with_capacity(non_aggregated.len()),
            });
            for (aggregator, (column_index, _)) in group.aggregators.iter_mut().zip(&aggregates) {
                aggregator.update(&row[column_index])?;
            }
            for index in &non_aggregated {
                group.non_aggregated.push(row[index].clone());
            }
        }
        let rows = groups.into_iter().map(move |(key, group)| {
            let mut aggregator_iter = group.aggregators.into_iter().map(|agg| agg.finish());
            let mut group_by_iter = serde.deserialize_seq_from(&key);
            let mut non_aggregated_iter = group.non_aggregated.into_iter();
            let mut row = Vec::with_capacity(columns.len());
            for column in &columns {
                let value = match column {
                    AggregateColumn::Aggregated(_) => aggregator_iter.next(),
                    AggregateColumn::GroupBy => group_by_iter.next(),
                    AggregateColumn::NonAggregated => non_aggregated_iter.next(),
                }
                .unwrap();
                row.push(value);
            }
            Row(row)
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
