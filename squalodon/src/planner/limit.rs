use super::{
    expression::{ExpressionBinder, PlanExpression, TypedExpression},
    sort::TopN,
    ColumnId, ColumnMap, ExplainFormatter, Node, PlanNode, Planner, PlannerResult, Project, Sort,
};
use crate::{parser, PlannerError, Type, Value};

#[derive(Clone)]
pub struct Limit<'a> {
    pub source: Box<PlanNode<'a>>,
    pub limit: Option<PlanExpression<'a>>,
    pub offset: Option<PlanExpression<'a>>,
}

impl Node for Limit<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        let mut node = f.node("Limit");
        let column_map = f.column_map();
        if let Some(limit) = &self.limit {
            node.field("limit", limit.display(&column_map));
        }
        if let Some(offset) = &self.offset {
            node.field("offset", offset.display(&column_map));
        }
        node.child(&self.source);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        self.source.append_outputs(columns);
    }

    fn num_rows(&self) -> usize {
        let mut num_rows = self.source.num_rows();
        if let Some(Ok(Value::Integer(limit))) = self.limit.as_ref().map(PlanExpression::eval_const)
        {
            num_rows = num_rows.min(limit as usize);
        }
        num_rows
    }

    fn cost(&self) -> f64 {
        let num_rows = self.num_rows();
        let source_cost =
            self.source.cost() * (num_rows + 1) as f64 / (self.source.num_rows() + 1) as f64;
        let limit_cost = num_rows as f64 * PlanNode::DEFAULT_ROW_COST;
        source_cost + limit_cost
    }
}

impl<'a> PlanNode<'a> {
    pub(super) fn limit(
        self,
        column_map: &mut ColumnMap,
        limit: Option<TypedExpression<'a>>,
        offset: Option<TypedExpression<'a>>,
    ) -> PlannerResult<Self> {
        let mut limit = limit
            .map(|limit| limit.expect_type(Type::Integer))
            .transpose()?;
        let mut offset = offset
            .map(|offset| offset.expect_type(Type::Integer))
            .transpose()?;
        if let Some(expr) = &limit {
            match expr.eval_const() {
                Ok(Value::Integer(limit)) if limit < 0 => {
                    return Err(PlannerError::NegativeLimitOrOffset)
                }
                Ok(Value::Integer(0)) => return Ok(self.into_no_rows()),
                Ok(Value::Null) => limit = None,
                _ => (),
            }
        }
        if let Some(expr) = &offset {
            match expr.eval_const() {
                Ok(Value::Integer(offset)) if offset < 0 => {
                    return Err(PlannerError::NegativeLimitOrOffset)
                }
                Ok(Value::Null | Value::Integer(0)) => offset = None,
                _ => (),
            }
        }

        if self.produces_no_rows() || (limit.is_none() && offset.is_none()) {
            return Ok(self);
        }

        // Push down
        if let PlanNode::Project(Project {
            source,
            projections,
        }) = self
        {
            let limit = limit.map(|limit| limit.into_typed(Type::Integer));
            let offset = offset.map(|offset| offset.into_typed(Type::Integer));
            return Ok(source
                .limit(column_map, limit, offset)?
                .project_with_column_ids(column_map, projections));
        }

        // Turn Sort + Limit into TopN
        let limit = match limit {
            Some(limit) => {
                if let Self::Sort(Sort { source, order_by }) = self {
                    return Ok(Self::TopN(TopN {
                        source,
                        limit,
                        offset,
                        order_by,
                    }));
                }
                Some(limit)
            }
            None => None,
        };

        Ok(Self::Limit(Limit {
            source: Box::new(self),
            limit,
            offset,
        }))
    }
}

impl<'a> Planner<'a> {
    pub fn plan_limit(
        &self,
        expr_binder: &ExpressionBinder<'_, 'a>,
        source: PlanNode<'a>,
        limit: Option<parser::Expression>,
        offset: Option<parser::Expression>,
    ) -> PlannerResult<PlanNode<'a>> {
        let limit = limit
            .map(|expr| expr_binder.bind_without_source(expr))
            .transpose()?;
        let offset = offset
            .map(|expr| expr_binder.bind_without_source(expr))
            .transpose()?;
        source.limit(&mut self.column_map_mut(), limit, offset)
    }
}
