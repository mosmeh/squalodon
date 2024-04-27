use super::{
    explain::ExplainFormatter,
    expression::{ExpressionBinder, TypedExpression},
    ColumnId, Node, PlanNode, Planner, PlannerResult,
};
use crate::{parser, planner, Value};

pub struct CrossProduct<'a> {
    pub left: Box<PlanNode<'a>>,
    pub right: Box<PlanNode<'a>>,
}

impl Node for CrossProduct<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        f.node("CrossProduct").child(&self.left).child(&self.right);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        self.left.append_outputs(columns);
        self.right.append_outputs(columns);
    }
}

pub enum Join<'a> {
    Hash {
        left: Box<PlanNode<'a>>,
        right: Box<PlanNode<'a>>,
        keys: Vec<(
            planner::Expression<'a, ColumnId>,
            planner::Expression<'a, ColumnId>,
        )>,
    },
}

impl Node for Join<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        match self {
            Self::Hash { left, right, keys } => {
                let mut node = f.node("HashJoin");
                let column_map = f.column_map();
                for (left_key, right_key) in keys {
                    let left_key = left_key.clone().into_display(&column_map);
                    let right_key = right_key.clone().into_display(&column_map);
                    node.field("condition", format!("{left_key} = {right_key}"));
                }
                node.child(left).child(right);
            }
        }
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        match self {
            Self::Hash { left, right, .. } => {
                left.append_outputs(columns);
                right.append_outputs(columns);
            }
        }
    }
}

impl PlanNode<'_> {
    pub(super) fn cross_product(self, other: Self) -> Self {
        if self.produces_no_rows() || other.produces_no_rows() {
            let mut outputs = self.outputs();
            outputs.append(&mut other.outputs());
            return PlanNode::new_no_rows(outputs);
        }
        if self.outputs().is_empty() {
            return other;
        }
        if other.outputs().is_empty() {
            return self;
        }
        Self::CrossProduct(CrossProduct {
            left: Box::new(self),
            right: Box::new(other),
        })
    }
}

impl<'a> Planner<'a> {
    pub fn plan_join(
        &self,
        expr_binder: &ExpressionBinder<'_, 'a>,
        join: parser::Join,
    ) -> PlannerResult<PlanNode<'a>> {
        let left = self.plan_table_ref(expr_binder, join.left)?;
        let right = self.plan_table_ref(expr_binder, join.right)?;

        let (plan, condition) = match join.condition {
            parser::JoinCondition::On(condition) => {
                let plan = left.cross_product(right);
                expr_binder.bind(plan, condition)?
            }
            parser::JoinCondition::Using(column_names) => {
                let column_map = self.column_map();
                let mut condition = TypedExpression::from(Value::from(true));
                for column_name in column_names {
                    let left_column_ref = left.resolve_column(&column_map, &column_name)?;
                    let right_column_ref = right.resolve_column(&column_map, &column_name)?;
                    condition =
                        condition.and(self.ctx, left_column_ref.eq(self.ctx, right_column_ref)?)?;
                }
                let plan = left.cross_product(right);
                (plan, condition)
            }
        };
        plan.filter(self.ctx, condition)
    }
}
