use super::{ExecutionContext, ExecutorNode, ExecutorResult, Node, NodeError, Output};
use crate::planner;

pub struct Union<'a> {
    left: Box<ExecutorNode<'a>>,
    right: Box<ExecutorNode<'a>>,
}

impl Node for Union<'_> {
    fn next_row(&mut self) -> Output {
        match self.left.next_row() {
            Ok(row) => Ok(row),
            Err(NodeError::EndOfRows) => self.right.next_row(),
            Err(e) => Err(e),
        }
    }
}

impl<'a> ExecutorNode<'a> {
    pub fn union(ctx: &'a ExecutionContext, plan: planner::Union<'a>) -> ExecutorResult<Self> {
        let planner::Union { left, right, .. } = plan;
        Ok(Self::Union(Union {
            left: Box::new(Self::new(ctx, *left)?),
            right: Box::new(Self::new(ctx, *right)?),
        }))
    }
}
