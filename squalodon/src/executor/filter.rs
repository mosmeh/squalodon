use super::{ExecutionContext, ExecutorNode, ExecutorResult, Node, Output};
use crate::{
    planner::{self, ExecutableExpression},
    Row, Value,
};

pub struct Filter<'a> {
    ctx: &'a ExecutionContext<'a>,
    source: Box<ExecutorNode<'a>>,
    conjuncts: Vec<ExecutableExpression<'a>>,
}

impl Node for Filter<'_> {
    fn next_row(&mut self) -> Output {
        loop {
            let row = self.source.next_row()?;
            if self.is_match(self.ctx, &row)? {
                return Ok(row);
            }
        }
    }
}

impl Filter<'_> {
    fn is_match(&self, ctx: &ExecutionContext, row: &Row) -> ExecutorResult<bool> {
        for conjunct in &self.conjuncts {
            match conjunct.eval(ctx, row)? {
                Value::Boolean(true) => {}
                Value::Null | Value::Boolean(false) => return Ok(false),
                _ => unreachable!(),
            }
        }
        Ok(true)
    }
}

impl<'a> ExecutorNode<'a> {
    pub fn filter(ctx: &'a ExecutionContext, plan: planner::Filter<'a>) -> ExecutorResult<Self> {
        let planner::Filter {
            source, conjuncts, ..
        } = plan;
        let outputs = source.outputs();
        let conjuncts = conjuncts
            .into_iter()
            .map(|conjunct| conjunct.into_executable(&outputs))
            .collect();
        Ok(Self::Filter(Filter {
            ctx,
            source: Self::new(ctx, *source)?.into(),
            conjuncts,
        }))
    }
}
