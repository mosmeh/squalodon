use super::{ExecutionContext, ExecutorNode, ExecutorResult, IntoOutput, Node, Output};
use crate::{
    catalog::{AggregateInitFnPtr, Aggregator},
    memcomparable::MemcomparableSerde,
    planner::{self, Aggregate, ColumnId},
    rows::ColumnIndex,
    Row, Value,
};
use std::collections::{HashMap, HashSet};

pub struct UngroupedAggregate {
    row: Option<Row>,
}

impl UngroupedAggregate {
    fn new(source: ExecutorNode, ops: Vec<ApplyAggregateOp>) -> ExecutorResult<Self> {
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
    fn new(source: ExecutorNode, ops: Vec<AggregateOp>) -> ExecutorResult<Self> {
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

struct ApplyAggregateOp {
    init: AggregateInitFnPtr,
    is_distinct: bool,
    input: ColumnIndex,
}

impl ApplyAggregateOp {
    fn from_plan(plan: &planner::ApplyAggregateOp, inputs: &[ColumnId]) -> Self {
        Self {
            init: plan.function.init,
            is_distinct: plan.is_distinct,
            input: plan.input.to_index(inputs),
        }
    }
}

enum AggregateOp {
    ApplyAggregate(ApplyAggregateOp),
    GroupBy { target: ColumnIndex },
}

impl AggregateOp {
    fn from_plan(plan: &planner::AggregateOp, inputs: &[ColumnId]) -> Self {
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

impl<'a> ExecutorNode<'a> {
    pub fn aggregate(ctx: &'a ExecutionContext, plan: Aggregate<'a>) -> ExecutorResult<Self> {
        match plan {
            Aggregate::Ungrouped { source, ops } => {
                let outputs = source.outputs();
                let ops = ops
                    .into_iter()
                    .map(|op| ApplyAggregateOp::from_plan(&op, &outputs))
                    .collect();
                Ok(Self::UngroupedAggregate(UngroupedAggregate::new(
                    Self::new(ctx, *source)?,
                    ops,
                )?))
            }
            Aggregate::Hash { source, ops } => {
                let outputs = source.outputs();
                let ops = ops
                    .into_iter()
                    .map(|op| AggregateOp::from_plan(&op, &outputs))
                    .collect();
                Ok(Self::HashAggregate(HashAggregate::new(
                    Self::new(ctx, *source)?,
                    ops,
                )?))
            }
        }
    }
}
