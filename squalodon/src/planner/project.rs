use super::{
    expression::{ExpressionBinder, TypedExpression},
    scan::Scan,
    Column, ColumnId, ColumnMap, ExplainFormatter, Node, PlanNode, Planner, PlannerResult,
};
use crate::{
    catalog::{AggregateFunction, Aggregator},
    executor::ExecutorResult,
    parser,
    planner::{self, ApplyAggregateOp},
    Value,
};
use std::collections::HashSet;

pub struct Project<'a> {
    pub source: Box<PlanNode<'a>>,
    pub outputs: Vec<(ColumnId, planner::Expression<'a, ColumnId>)>,
}

impl Node for Project<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        let mut node = f.node("Project");
        for (output, _) in &self.outputs {
            node.field("expression", f.column_map()[output].name());
        }
        node.child(&self.source);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        columns.extend(self.outputs.iter().map(|(id, _)| *id));
    }
}

impl<'a> PlanNode<'a> {
    pub(super) fn project(
        self,
        column_map: &mut ColumnMap,
        exprs: Vec<TypedExpression<'a>>,
    ) -> Self {
        if self.produces_no_rows() {
            return self;
        }
        let outputs: Vec<_> = exprs
            .iter()
            .map(|expr| {
                let TypedExpression { expr, ty } = expr;
                let id = match expr {
                    planner::Expression::ColumnRef(id) => *id,
                    _ => column_map.insert(Column::new(
                        expr.display(&column_map.view()).to_string(),
                        *ty,
                    )),
                };
                (id, expr.clone())
            })
            .collect();

        if self
            .outputs()
            .into_iter()
            .eq(outputs.iter().map(|(output, _)| *output))
        {
            // Identity projection
            return self;
        }

        let plan = match self {
            PlanNode::Scan(Scan::Index {
                index,
                range,
                outputs,
                ..
            }) => {
                let indexed_column_ids: Vec<_> = index
                    .column_indexes()
                    .iter()
                    .map(|i| outputs[i.0])
                    .collect();
                let indexed: HashSet<_> = indexed_column_ids.iter().copied().collect();
                let is_covered = exprs
                    .iter()
                    .all(|expr| expr.expr.referenced_columns().is_subset(&indexed));
                if is_covered {
                    return PlanNode::Scan(Scan::IndexOnly {
                        index,
                        range,
                        outputs: indexed_column_ids,
                    })
                    .project(column_map, exprs);
                }
                PlanNode::Scan(Scan::Index {
                    index,
                    range,
                    outputs,
                })
            }
            plan => plan,
        };

        Self::Project(Project {
            source: Box::new(plan),
            outputs,
        })
    }
}

impl<'a> Planner<'a> {
    pub fn plan_projections(
        &self,
        expr_binder: &ExpressionBinder<'_, 'a>,
        source: PlanNode<'a>,
        projection_exprs: Vec<parser::Expression>,
        distinct: Option<parser::Distinct>,
    ) -> PlannerResult<PlanNode<'a>> {
        let num_projected_columns = projection_exprs.len();

        let mut plan = source;
        let mut exprs = Vec::new();
        for expr in projection_exprs {
            let (new_plan, bound_expr) = expr_binder.bind(plan, expr.clone())?;
            plan = new_plan;
            exprs.push(bound_expr);
        }

        match distinct {
            Some(parser::Distinct { on: Some(on) }) => {
                /// An aggregator that returns the first row it sees.
                #[derive(Default)]
                struct First {
                    value: Option<Value>,
                }

                impl Aggregator for First {
                    fn update(&mut self, value: &Value) -> ExecutorResult<()> {
                        if self.value.is_none() {
                            self.value = Some(value.clone());
                        }
                        Ok(())
                    }

                    fn finish(&self) -> Value {
                        self.value.clone().unwrap_or(Value::Null)
                    }
                }

                static FIRST: AggregateFunction = AggregateFunction::new_internal::<First>();

                for expr in on {
                    let (new_plan, expr) = expr_binder.bind(plan, expr)?;
                    plan = new_plan;
                    exprs.push(expr);
                }

                let mut column_map = self.column_map();
                let plan = plan.project(&mut column_map, exprs);
                let outputs = plan.outputs();

                let projected = outputs.iter().take(num_projected_columns).map(|target| {
                    planner::AggregateOp::ApplyAggregate(ApplyAggregateOp {
                        function: &FIRST,
                        is_distinct: false,
                        input: *target,
                        output: *target,
                    })
                });
                let on = outputs
                    .iter()
                    .skip(num_projected_columns)
                    .map(|target| planner::AggregateOp::GroupBy { target: *target });
                let ops = projected.chain(on).collect();
                let plan = plan.hash_aggregate(ops);

                let exprs = outputs
                    .into_iter()
                    .take(num_projected_columns)
                    .map(|id| planner::Expression::ColumnRef(id).into_typed(column_map[id].ty))
                    .collect();
                Ok(plan.project(&mut column_map, exprs))
            }
            Some(parser::Distinct { on: None }) => {
                let plan = plan.project(&mut self.column_map(), exprs);
                let ops = plan
                    .outputs()
                    .into_iter()
                    .map(|target| planner::AggregateOp::GroupBy { target })
                    .collect();
                Ok(plan.hash_aggregate(ops))
            }
            None => Ok(plan.project(&mut self.column_map(), exprs)),
        }
    }
}
