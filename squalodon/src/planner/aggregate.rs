use super::{
    expression::{ExpressionBinder, PlanExpression},
    Column, ColumnId, ExplainFormatter, Node, PlanNode, Planner, PlannerError, PlannerResult,
};
use crate::{catalog::AggregateFunction, parser, CatalogError, Type};
use std::collections::HashMap;

pub enum Aggregate<'a> {
    Ungrouped {
        source: Box<PlanNode<'a>>,
        ops: Vec<ApplyAggregateOp<'a>>,
    },
    Hash {
        source: Box<PlanNode<'a>>,
        ops: Vec<AggregateOp<'a>>,
    },
}

impl Node for Aggregate<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        match self {
            Self::Ungrouped { source, ops } => {
                let mut node = f.node("UngroupedAggregate");
                for op in ops {
                    node.field("aggregate", f.column_map()[op.output].name());
                }
                node.child(source);
            }
            Self::Hash { source, ops, .. } => {
                let mut node = f.node("HashAggregate");
                for op in ops {
                    if let AggregateOp::ApplyAggregate(ApplyAggregateOp { output, .. }) = op {
                        node.field("aggregate", f.column_map()[output].name());
                    }
                }
                for op in ops {
                    if let AggregateOp::GroupBy { target } = op {
                        node.field("group by", f.column_map()[target].name());
                    }
                }
                node.child(source);
            }
        }
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        match self {
            Self::Ungrouped { ops, .. } => {
                for op in ops {
                    columns.push(op.output);
                }
            }
            Self::Hash { ops, .. } => {
                for op in ops {
                    match op {
                        AggregateOp::ApplyAggregate(ApplyAggregateOp { output, .. }) => {
                            columns.push(*output);
                        }
                        AggregateOp::GroupBy { target } => {
                            columns.push(*target);
                        }
                    }
                }
            }
        }
    }
}

pub struct ApplyAggregateOp<'a> {
    pub function: &'a AggregateFunction,
    pub is_distinct: bool,
    pub input: ColumnId,
    pub output: ColumnId,
}

pub enum AggregateOp<'a> {
    ApplyAggregate(ApplyAggregateOp<'a>),
    GroupBy { target: ColumnId },
}

impl<'a> PlanNode<'a> {
    pub(super) fn ungrouped_aggregate(self, ops: Vec<ApplyAggregateOp<'a>>) -> Self {
        if self.produces_no_rows() {
            return self;
        }
        PlanNode::Aggregate(Aggregate::Ungrouped {
            source: Box::new(self),
            ops,
        })
    }

    pub(super) fn hash_aggregate(self, ops: Vec<AggregateOp<'a>>) -> Self {
        if self.produces_no_rows() {
            return self;
        }
        PlanNode::Aggregate(Aggregate::Hash {
            source: Box::new(self),
            ops,
        })
    }
}

pub struct AggregateCollection<'a, 'b> {
    planner: &'a Planner<'b>,
    aggregates: HashMap<parser::FunctionCall, BoundAggregate<'b>>,
}

impl<'a, 'b> AggregateCollection<'a, 'b> {
    pub fn new(planner: &'a Planner<'b>) -> Self {
        Self {
            planner,
            aggregates: HashMap::new(),
        }
    }

    pub fn finish(self) -> AggregatePlanner<'a, 'b> {
        AggregatePlanner {
            collected: self,
            group_by: HashMap::new(),
        }
    }
}

impl<'a, 'b> AggregateCollection<'a, 'b> {
    pub fn gather(
        &mut self,
        source: PlanNode<'b>,
        expr: &parser::Expression,
    ) -> PlannerResult<PlanNode<'b>> {
        self.gather_inner(source, expr, false)
    }

    fn gather_inner(
        &mut self,
        source: PlanNode<'b>,
        expr: &parser::Expression,
        in_aggregate_args: bool,
    ) -> PlannerResult<PlanNode<'b>> {
        match expr {
            parser::Expression::Constant(_)
            | parser::Expression::ColumnRef(_)
            | parser::Expression::ScalarSubquery(_)
            | parser::Expression::Exists(_)
            | parser::Expression::Parameter(_) => Ok(source),
            parser::Expression::Cast { expr, .. } | parser::Expression::UnaryOp { expr, .. } => {
                self.gather_inner(source, expr, in_aggregate_args)
            }
            parser::Expression::BinaryOp { lhs, rhs, .. } => {
                let plan = self.gather_inner(source, lhs, in_aggregate_args)?;
                self.gather_inner(plan, rhs, in_aggregate_args)
            }
            parser::Expression::Case {
                branches,
                else_branch,
            } => {
                let mut plan = source;
                for branch in branches {
                    plan = self.gather_inner(plan, &branch.condition, in_aggregate_args)?;
                    plan = self.gather_inner(plan, &branch.result, in_aggregate_args)?;
                }
                if let Some(else_branch) = else_branch {
                    plan = self.gather_inner(plan, else_branch, in_aggregate_args)?;
                }
                Ok(plan)
            }
            parser::Expression::Like {
                str_expr, pattern, ..
            } => {
                let plan = self.gather_inner(source, str_expr, in_aggregate_args)?;
                self.gather_inner(plan, pattern, in_aggregate_args)
            }
            parser::Expression::Function(function_call) => {
                match self.planner.catalog.aggregate_function(&function_call.name) {
                    Ok(function) => {
                        if in_aggregate_args {
                            // Nested aggregate functions are not allowed.
                            return Err(PlannerError::AggregateNotAllowed);
                        }

                        let (plan, bound_expr) = match &function_call.args {
                            parser::FunctionArgs::Wildcard
                                if function_call.name.eq_ignore_ascii_case("count") =>
                            {
                                // `count(*)` is a special case equivalent to `count(1)`.
                                (
                                    source,
                                    PlanExpression::Constant(1.into()).into_typed(Type::Integer),
                                )
                            }
                            parser::FunctionArgs::Expressions(args) if args.len() == 1 => {
                                let plan = self.gather_inner(source, &args[0], true)?;
                                ExpressionBinder::new(self.planner).bind(plan, args[0].clone())?
                            }
                            _ => return Err(PlannerError::ArityError),
                        };

                        let std::collections::hash_map::Entry::Vacant(entry) =
                            self.aggregates.entry(function_call.clone())
                        else {
                            return Ok(plan);
                        };

                        let output = self.planner.column_map().insert(Column::new(
                            expr.to_string(),
                            (function.bind)(bound_expr.ty)?,
                        ));
                        entry.insert(BoundAggregate {
                            function,
                            arg: bound_expr.expr,
                            output,
                        });
                        return Ok(plan);
                    }
                    Err(CatalogError::UnknownEntry(_, _)) => (),
                    Err(err) => return Err(err.into()),
                }

                self.planner.catalog.scalar_function(&function_call.name)?; // Check if the function exists
                if function_call.is_distinct {
                    return Err(PlannerError::InvalidArgument);
                }
                match &function_call.args {
                    parser::FunctionArgs::Wildcard => Err(PlannerError::InvalidArgument),
                    parser::FunctionArgs::Expressions(args) => {
                        let mut plan = source;
                        for arg in args {
                            plan = self.gather_inner(plan, arg, in_aggregate_args)?;
                        }
                        Ok(plan)
                    }
                }
            }
        }
    }
}

pub struct AggregatePlanner<'a, 'b> {
    collected: AggregateCollection<'a, 'b>,
    group_by: HashMap<parser::Expression, ColumnId>,
}

impl<'b> AggregatePlanner<'_, 'b> {
    pub fn has_aggregates(&self) -> bool {
        !self.collected.aggregates.is_empty()
    }

    pub fn resolve_aggregate_function(
        &self,
        function_call: &parser::FunctionCall,
    ) -> Option<ColumnId> {
        self.collected
            .aggregates
            .get(function_call)
            .map(|aggregate| aggregate.output)
    }

    pub fn resolve_group_by(&self, expr: &parser::Expression) -> Option<ColumnId> {
        self.group_by.get(expr).copied()
    }
}

impl<'a> AggregatePlanner<'_, 'a> {
    pub fn plan(
        &mut self,
        expr_binder: &ExpressionBinder<'_, 'a>,
        source: PlanNode<'a>,
        group_by: Vec<parser::Expression>,
    ) -> PlannerResult<PlanNode<'a>> {
        let num_aggregates = self.collected.aggregates.len();
        let mut exprs = Vec::with_capacity(num_aggregates + group_by.len());

        // The first `aggregates.len()` columns are the aggregated columns
        // that are passed to the respective aggregate functions.
        {
            let column_map = self.collected.planner.column_map();
            for aggregate in self.collected.aggregates.values() {
                let expr = aggregate
                    .arg
                    .clone()
                    .into_typed(column_map[aggregate.output].ty);
                exprs.push(expr);
            }
        }

        // The rest of the columns are the columns in the GROUP BY clause.
        let mut plan = source;
        let has_group_by = !group_by.is_empty();
        for group_by in &group_by {
            let (new_plan, expr) = expr_binder.bind(plan, group_by.clone())?;
            plan = new_plan;
            exprs.push(expr);
        }

        let plan = plan.project(&mut self.collected.planner.column_map(), exprs);
        let outputs = plan.outputs();
        let (aggregate_outputs, group_by_outputs) = outputs.split_at(num_aggregates);
        if has_group_by {
            for (group_by, output) in group_by.into_iter().zip(group_by_outputs) {
                self.group_by.insert(group_by, *output);
            }
        }

        let ops = self.collected.aggregates.iter().zip(aggregate_outputs).map(
            |((func_call, aggregate), input)| ApplyAggregateOp {
                function: aggregate.function,
                is_distinct: func_call.is_distinct,
                input: *input,
                output: aggregate.output,
            },
        );
        if has_group_by {
            let group_by_ops = group_by_outputs
                .iter()
                .map(|target| AggregateOp::GroupBy { target: *target });
            let ops = ops
                .map(AggregateOp::ApplyAggregate)
                .chain(group_by_ops)
                .collect();
            Ok(plan.hash_aggregate(ops))
        } else {
            Ok(plan.ungrouped_aggregate(ops.collect()))
        }
    }
}
struct BoundAggregate<'a> {
    function: &'a AggregateFunction,
    arg: PlanExpression<'a>,
    output: ColumnId,
}
