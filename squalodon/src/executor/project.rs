use super::{ConnectionContext, ExecutorNode, ExecutorResult, Node, Output};
use crate::{
    planner::{self, Expression},
    rows::ColumnIndex,
    Row,
};

pub struct Project<'a> {
    ctx: &'a ConnectionContext<'a>,
    source: Box<ExecutorNode<'a>>,
    exprs: Vec<Expression<'a, ColumnIndex>>,
}

impl Node for Project<'_> {
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

impl<'a> ExecutorNode<'a> {
    pub fn project(ctx: &'a ConnectionContext, plan: planner::Project<'a>) -> ExecutorResult<Self> {
        let planner::Project {
            source,
            projections,
            ..
        } = plan;
        let source_outputs = source.outputs();
        let exprs = projections
            .into_iter()
            .map(|(_, expr)| expr.into_executable(&source_outputs))
            .collect();
        Ok(Self::Project(Project {
            ctx,
            source: Self::new(ctx, *source)?.into(),
            exprs,
        }))
    }
}
