use super::{
    expression::ExpressionBinder, ColumnId, ColumnMap, ExplainFormatter, Node, PlanNode, Planner,
    PlannerResult,
};
use crate::{parser, planner, PlannerError};

#[derive(Clone)]
pub struct Union<'a> {
    pub left: Box<PlanNode<'a>>,
    pub right: Box<PlanNode<'a>>,
    pub outputs: Vec<ColumnId>,
}

impl Node for Union<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        f.node("Union").child(&self.left).child(&self.right);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        columns.extend(self.outputs.iter());
    }

    fn num_rows(&self) -> usize {
        self.left.num_rows() + self.right.num_rows()
    }

    fn cost(&self) -> f64 {
        self.left.cost() + self.right.cost()
    }
}

impl<'a> PlanNode<'a> {
    pub(super) fn union(self, column_map: &mut ColumnMap, other: Self) -> PlannerResult<Self> {
        let left_outputs = self.outputs();
        let right_outputs = other.outputs();
        if left_outputs.len() != right_outputs.len() {
            return Err(PlannerError::ColumnCountMismatch {
                expected: left_outputs.len(),
                actual: right_outputs.len(),
            });
        }
        for (left, right) in left_outputs.iter().zip(right_outputs.iter()) {
            if !column_map[left].ty.is_compatible_with(column_map[right].ty) {
                return Err(PlannerError::TypeError);
            }
        }
        if other.produces_no_rows() {
            return Ok(self);
        }
        if self.produces_no_rows() {
            return Ok(other);
        }
        let outputs = left_outputs
            .into_iter()
            .map(|id| column_map.insert(column_map[id].clone()))
            .collect();
        Ok(Self::Union(Union {
            left: Box::new(self),
            right: Box::new(other),
            outputs,
        }))
    }
}

impl<'a> Planner<'a> {
    pub fn plan_union(
        &self,
        all: bool,
        left: parser::Query,
        right: parser::Query,
        modifier: parser::QueryModifier,
    ) -> PlannerResult<PlanNode<'a>> {
        let left = self.plan_query(left)?;
        let right = self.plan_query(right)?;
        let mut plan = left.union(&mut self.column_map_mut(), right)?;
        if !all {
            let ops = plan
                .outputs()
                .into_iter()
                .map(|target| planner::AggregateOp::GroupBy { target })
                .collect();
            plan = plan.hash_aggregate(ops);
        }
        let expr_binder = ExpressionBinder::new(self);
        let plan = self.plan_order_by(&expr_binder, plan, modifier.order_by)?;
        self.plan_limit(&expr_binder, plan, modifier.limit, modifier.offset)
    }
}
