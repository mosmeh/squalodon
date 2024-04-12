use super::{
    explain::ExplainFormatter,
    expression::{ExpressionBinder, TypedExpression},
    ColumnId, Node, PlanNode, Planner, PlannerResult,
};
use crate::{
    connection::ConnectionContext,
    parser::{self, BinaryOp},
    planner::{self, CrossProduct},
    storage::Transaction,
    Row, Type, Value,
};
use std::collections::HashSet;

pub struct Filter<'a, T> {
    pub source: Box<PlanNode<'a, T>>,
    pub conjuncts: HashSet<planner::Expression<'a, T, ColumnId>>,
}

impl<T> Node for Filter<'_, T> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        let mut node = f.node("Filter");
        for conjunct in &self.conjuncts {
            node.field("filter", conjunct.clone().into_display(&f.column_map()));
        }
        node.child(&self.source);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        self.source.append_outputs(columns);
    }
}

impl<'a, T> PlanNode<'a, T> {
    pub(super) fn filter(
        self,
        ctx: &ConnectionContext<'a, T>,
        condition: TypedExpression<'a, T>,
    ) -> PlannerResult<Self> {
        let condition = condition.expect_type(Type::Boolean)?;
        self.filter_inner(ctx, [condition].into())
    }

    fn filter_inner(
        self,
        ctx: &ConnectionContext<'a, T>,
        conjuncts: HashSet<planner::Expression<'a, T, ColumnId>>,
    ) -> PlannerResult<Self> {
        if self.produces_no_rows() {
            return Ok(self);
        }

        let mut normalized_conjuncts = HashSet::new();
        for conjunct in conjuncts {
            if !collect_conjuncts(ctx, &mut normalized_conjuncts, conjunct) {
                return Ok(self.into_no_rows());
            }
        }
        if normalized_conjuncts.is_empty() {
            // All conjuncts evaluated to true
            return Ok(self);
        }

        let plan = match self {
            PlanNode::Filter(Filter {
                source,
                mut conjuncts,
            }) => {
                // Merge nested filters
                conjuncts.extend(normalized_conjuncts);
                return source.filter_inner(ctx, conjuncts);
            }
            PlanNode::CrossProduct(CrossProduct { left, right }) => {
                // Push down filters
                let left_outputs = left.outputs().into_iter().collect();
                let right_outputs = right.outputs().into_iter().collect();
                for conjunct in &normalized_conjuncts {
                    if conjunct.referenced_columns().is_subset(&left_outputs) {
                        let conjunct = conjunct.clone();
                        normalized_conjuncts.remove(&conjunct);
                        return left
                            .filter_inner(ctx, [conjunct].into())?
                            .cross_product(*right)
                            .filter_inner(ctx, normalized_conjuncts);
                    }
                    if conjunct.referenced_columns().is_subset(&right_outputs) {
                        let conjunct = conjunct.clone();
                        normalized_conjuncts.remove(&conjunct);
                        return left
                            .cross_product(right.filter_inner(ctx, [conjunct].into())?)
                            .filter_inner(ctx, normalized_conjuncts);
                    }
                }
                PlanNode::CrossProduct(CrossProduct { left, right })
            }
            plan => plan,
        };

        Ok(PlanNode::Filter(Filter {
            source: Box::new(plan),
            conjuncts: normalized_conjuncts,
        }))
    }
}

/// Collects conjuncts from an expression.
///
/// Returns false if the expression evaluates to false.
fn collect_conjuncts<'a, T>(
    ctx: &ConnectionContext<'a, T>,
    conjuncts: &mut HashSet<planner::Expression<'a, T, ColumnId>>,
    expr: planner::Expression<'a, T, ColumnId>,
) -> bool {
    if let planner::Expression::BinaryOp {
        op: BinaryOp::And,
        lhs,
        rhs,
    } = expr
    {
        if !collect_conjuncts(ctx, conjuncts, *lhs) {
            return false;
        }
        return collect_conjuncts(ctx, conjuncts, *rhs);
    }
    if let Ok(value) = expr.eval(ctx, &Row::empty()) {
        match value {
            Value::Boolean(true) => return true,
            Value::Null | Value::Boolean(false) => return false,
            _ => (),
        }
    }
    conjuncts.insert(expr);
    true
}

impl<'a, T: Transaction> Planner<'a, T> {
    pub fn plan_filter(
        &self,
        expr_binder: &ExpressionBinder<'_, 'a, T>,
        source: PlanNode<'a, T>,
        expr: parser::Expression,
    ) -> PlannerResult<PlanNode<'a, T>> {
        let (plan, condition) = expr_binder.bind(source, expr)?;
        plan.filter(self.ctx, condition)
    }
}
