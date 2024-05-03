use super::{
    explain::ExplainFormatter,
    expression::{ExpressionBinder, PlanExpression, TypedExpression},
    ColumnId, CompareOp, Node, PlanNode, Planner, PlannerResult, Scan,
};
use crate::{
    parser::{self, BinaryOp},
    planner, Type, Value,
};
use std::{collections::HashSet, ops::Bound};

pub struct Filter<'a> {
    pub source: Box<PlanNode<'a>>,
    pub conjuncts: HashSet<PlanExpression<'a>>,
}

impl Node for Filter<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        let mut node = f.node("Filter");
        for conjunct in &self.conjuncts {
            node.field("filter", conjunct.display(&f.column_map()));
        }
        node.child(&self.source);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        self.source.append_outputs(columns);
    }
}

impl<'a> PlanNode<'a> {
    pub(super) fn filter(self, condition: TypedExpression<'a>) -> PlannerResult<Self> {
        let condition = condition.expect_type(Type::Boolean)?;
        self.filter_inner([condition].into())
    }

    fn filter_inner(self, conjuncts: HashSet<PlanExpression<'a>>) -> PlannerResult<Self> {
        if self.produces_no_rows() {
            return Ok(self);
        }

        let mut normalized_conjuncts = HashSet::new();
        for conjunct in conjuncts {
            if !collect_conjuncts(&mut normalized_conjuncts, conjunct) {
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
                return source.filter_inner(conjuncts);
            }
            PlanNode::CrossProduct(planner::CrossProduct { left, right }) => {
                let left_outputs = left.outputs().into_iter().collect();
                let right_outputs = right.outputs().into_iter().collect();

                // Push down filters
                for conjunct in &normalized_conjuncts {
                    if conjunct.referenced_columns().is_subset(&left_outputs) {
                        let conjunct = conjunct.clone();
                        normalized_conjuncts.remove(&conjunct);
                        return left
                            .filter_inner([conjunct].into())?
                            .cross_product(*right)
                            .filter_inner(normalized_conjuncts);
                    }
                    if conjunct.referenced_columns().is_subset(&right_outputs) {
                        let conjunct = conjunct.clone();
                        normalized_conjuncts.remove(&conjunct);
                        return left
                            .cross_product(right.filter_inner([conjunct].into())?)
                            .filter_inner(normalized_conjuncts);
                    }
                }

                // Nested loop join or hash join
                let comparisons: Vec<_> = normalized_conjuncts
                    .iter()
                    .filter_map(|conjunct| {
                        let PlanExpression::BinaryOp { op, lhs, rhs } = conjunct else {
                            return None;
                        };
                        let op = CompareOp::from_binary_op(*op)?;
                        let lhs_refs = lhs.referenced_columns();
                        let rhs_refs = rhs.referenced_columns();
                        let (op, left, right) = if lhs_refs.is_subset(&left_outputs)
                            && rhs_refs.is_subset(&right_outputs)
                        {
                            (op, lhs, rhs)
                        } else if lhs_refs.is_subset(&right_outputs)
                            && rhs_refs.is_subset(&left_outputs)
                        {
                            (op.flip(), rhs, lhs)
                        } else {
                            return None;
                        };
                        Some((conjunct.clone(), op, (**left).clone(), (**right).clone()))
                    })
                    .collect();
                if !comparisons.is_empty() {
                    for (conjunct, _, _, _) in &comparisons {
                        normalized_conjuncts.remove(conjunct);
                    }
                    let has_inequality =
                        comparisons.iter().any(|(_, op, _, _)| *op != CompareOp::Eq);
                    let join = if has_inequality {
                        // TODO: compare costs of NestedLoopJoin and
                        //       HashJoin + Filter when some of the conjuncts
                        //       are equalities and others are inequalities
                        planner::Join::NestedLoop {
                            left,
                            right,
                            comparisons: comparisons
                                .into_iter()
                                .map(|(_, op, left, right)| (op, left, right))
                                .collect(),
                        }
                    } else {
                        planner::Join::Hash {
                            left,
                            right,
                            keys: comparisons
                                .into_iter()
                                .map(|(_, _, left, right)| (left, right))
                                .collect(),
                        }
                    };
                    return PlanNode::Join(join).filter_inner(normalized_conjuncts);
                }

                PlanNode::CrossProduct(planner::CrossProduct { left, right })
            }
            PlanNode::Scan(Scan::Seq { table, outputs }) => {
                for conjunct in &normalized_conjuncts {
                    let PlanExpression::BinaryOp { op, lhs, rhs } = conjunct else {
                        continue;
                    };
                    let (PlanExpression::ColumnRef(id), PlanExpression::Constant(value)) =
                        (lhs.as_ref(), rhs.as_ref())
                    else {
                        continue;
                    };
                    for index in table.indexes() {
                        let [column_index] = index.column_indexes() else {
                            // TODO: Make use of multi-column indexes
                            continue;
                        };
                        if *id != outputs[column_index.0] {
                            continue;
                        }

                        let value = vec![value.clone()];
                        let range = match op {
                            BinaryOp::Eq => {
                                (Bound::Included(value.clone()), Bound::Included(value))
                            }
                            BinaryOp::Gt => (Bound::Excluded(value), Bound::Unbounded),
                            BinaryOp::Ge => (Bound::Included(value), Bound::Unbounded),
                            BinaryOp::Lt => (Bound::Unbounded, Bound::Excluded(value)),
                            BinaryOp::Le => (Bound::Unbounded, Bound::Included(value)),
                            _ => continue,
                        };

                        let index = index.clone();
                        let conjunct = conjunct.clone();
                        normalized_conjuncts.remove(&conjunct);

                        // TODO: support pushing down more than one conjunct
                        return PlanNode::Scan(Scan::Index {
                            index,
                            range,
                            outputs,
                        })
                        .filter_inner(normalized_conjuncts);
                    }
                }
                PlanNode::Scan(Scan::Seq { table, outputs })
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
fn collect_conjuncts<'a>(
    conjuncts: &mut HashSet<PlanExpression<'a>>,
    expr: PlanExpression<'a>,
) -> bool {
    if let PlanExpression::BinaryOp {
        op: BinaryOp::And,
        lhs,
        rhs,
    } = expr
    {
        if !collect_conjuncts(conjuncts, *lhs) {
            return false;
        }
        return collect_conjuncts(conjuncts, *rhs);
    }
    if let Ok(value) = expr.eval_const() {
        match value {
            Value::Boolean(true) => return true,
            Value::Null | Value::Boolean(false) => return false,
            _ => (),
        }
    }
    conjuncts.insert(expr);
    true
}

impl<'a> Planner<'a> {
    #[allow(clippy::unused_self)]
    pub fn plan_filter(
        &self,
        expr_binder: &ExpressionBinder<'_, 'a>,
        source: PlanNode<'a>,
        expr: parser::Expression,
    ) -> PlannerResult<PlanNode<'a>> {
        let (plan, condition) = expr_binder.bind(source, expr)?;
        plan.filter(condition)
    }
}
