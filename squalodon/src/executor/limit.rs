use super::{ConnectionContext, ExecutorNode, ExecutorResult, Node, NodeError, Output};
use crate::{
    planner::{self, ExecutableExpression},
    ExecutorError, Row, Value,
};

pub struct Limit<'a> {
    source: Box<ExecutorNode<'a>>,
    limit: Option<usize>,
    offset: usize,
    cursor: usize,
}

impl<'a> Limit<'a> {
    fn new(
        ctx: &'a ConnectionContext,
        source: ExecutorNode<'a>,
        limit: Option<ExecutableExpression>,
        offset: Option<ExecutableExpression>,
    ) -> ExecutorResult<Self> {
        fn eval(
            ctx: &ConnectionContext,
            expr: Option<ExecutableExpression>,
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

impl Node for Limit<'_> {
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

impl<'a> ExecutorNode<'a> {
    pub fn limit(ctx: &'a ConnectionContext, plan: planner::Limit<'a>) -> ExecutorResult<Self> {
        let planner::Limit {
            source,
            limit,
            offset,
        } = plan;
        let outputs = source.outputs();
        let limit = limit.map(|expr| expr.into_executable(&outputs));
        let offset = offset.map(|expr| expr.into_executable(&outputs));
        Ok(Self::Limit(Limit::new(
            ctx,
            Self::new(ctx, *source)?,
            limit,
            offset,
        )?))
    }
}
