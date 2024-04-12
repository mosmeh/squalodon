use super::{
    expression::{ExpressionBinder, TypedExpression},
    Column, ColumnId, ExplainFormatter, Node, PlanNode, Planner, PlannerError, PlannerResult,
};
use crate::{
    catalog::AggregateFunction, parser, planner, storage::Transaction, CatalogError, Type,
};
use std::collections::HashMap;

pub enum Aggregate<'a, T> {
    Ungrouped {
        source: Box<PlanNode<'a, T>>,
        ops: Vec<ApplyAggregateOp<'a>>,
    },
    Hash {
        source: Box<PlanNode<'a, T>>,
        ops: Vec<AggregateOp<'a>>,
    },
}

impl<T> Node for Aggregate<'_, T> {
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
                for op in ops {
                    if let AggregateOp::Passthrough { target } = op {
                        node.field("passthrough", f.column_map()[target].name());
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
                        AggregateOp::GroupBy { target } | AggregateOp::Passthrough { target } => {
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
    Passthrough { target: ColumnId },
}

impl<'a, T> PlanNode<'a, T> {
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

pub struct AggregateCollection<'a, 'b, T> {
    planner: &'a Planner<'b, T>,
    aggregates: HashMap<parser::FunctionCall, BoundAggregate<'b, T>>,
}

impl<'a, 'b, T> AggregateCollection<'a, 'b, T> {
    pub fn new(planner: &'a Planner<'b, T>) -> Self {
        Self {
            planner,
            aggregates: HashMap::new(),
        }
    }

    pub fn finish(self) -> AggregatePlanner<'a, 'b, T> {
        AggregatePlanner {
            collected: self,
            group_by: HashMap::new(),
        }
    }
}

impl<'a, 'b, T: Transaction> AggregateCollection<'a, 'b, T> {
    pub fn gather(
        &mut self,
        source: PlanNode<'b, T>,
        expr: &parser::Expression,
    ) -> PlannerResult<PlanNode<'b, T>> {
        self.gather_inner(source, expr, false)
    }

    fn gather_inner(
        &mut self,
        source: PlanNode<'b, T>,
        expr: &parser::Expression,
        in_aggregate_args: bool,
    ) -> PlannerResult<PlanNode<'b, T>> {
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
                match self
                    .planner
                    .ctx
                    .catalog()
                    .aggregate_function(&function_call.name)
                {
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
                                    TypedExpression {
                                        expr: planner::Expression::Constant(1.into()),
                                        ty: Type::Integer.into(),
                                    },
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

                self.planner
                    .ctx
                    .catalog()
                    .scalar_function(&function_call.name)?; // Check if the function exists
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

pub struct AggregatePlanner<'a, 'b, T> {
    collected: AggregateCollection<'a, 'b, T>,
    group_by: HashMap<parser::Expression, ColumnId>,
}

impl<'b, T> AggregatePlanner<'_, 'b, T> {
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

impl<'a, T: Transaction> AggregatePlanner<'_, 'a, T> {
    pub fn plan(
        &mut self,
        expr_binder: &ExpressionBinder<'_, 'a, T>,
        source: PlanNode<'a, T>,
        group_by: Vec<parser::Expression>,
    ) -> PlannerResult<PlanNode<'a, T>> {
        let num_aggregates = self.collected.aggregates.len();
        let mut exprs = Vec::with_capacity(num_aggregates + group_by.len());

        // The first `aggregates.len()` columns are the aggregated columns
        // that are passed to the respective aggregate functions.
        {
            let column_map = self.collected.planner.column_map();
            for aggregate in self.collected.aggregates.values() {
                exprs.push(TypedExpression {
                    expr: aggregate.arg.clone(),
                    ty: column_map[aggregate.output].ty,
                });
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
struct BoundAggregate<'a, T> {
    function: &'a AggregateFunction,
    arg: planner::Expression<'a, T, ColumnId>,
    output: ColumnId,
}
