use super::{ExecutionContext, ExecutorNode, ExecutorResult, Node, Output};
use crate::planner;

pub struct Union<'a> {
    left: Box<ExecutorNode<'a>>,
    right: Box<ExecutorNode<'a>>,
}

impl Node for Union<'_> {
    fn next_row(&mut self) -> Output {
        self.left.next_row().or_else(|_| self.right.next_row())
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
