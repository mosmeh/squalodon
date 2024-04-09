use super::{
    expression::{ExpressionBinder, TypedExpression},
    Column, ColumnId, ExplainFormatter, Node, PlanNode, Planner, PlannerError, PlannerResult,
};
use crate::{
    catalog::{AggregateFunction, AggregateInitFnPtr},
    parser, planner, CatalogError, Storage, Type,
};
use std::{collections::HashMap, fmt::Write};

pub enum Aggregate<'txn, 'db, T: Storage> {
    Ungrouped {
        source: Box<PlanNode<'txn, 'db, T>>,
        ops: Vec<ApplyAggregateOp>,
    },
    Hash {
        source: Box<PlanNode<'txn, 'db, T>>,
        ops: Vec<AggregateOp>,
    },
}

impl<T: Storage> Node for Aggregate<'_, '_, T> {
    fn fmt_explain(&self, f: &mut ExplainFormatter) {
        match self {
            Self::Ungrouped { source, ops } => {
                let mut s = "UngroupedAggregate".to_owned();
                for (i, op) in ops.iter().enumerate() {
                    s.push_str(if i == 0 { " " } else { ", " });
                    write!(&mut s, "{} -> {}", op.input, op.output).unwrap();
                }
                f.write_str(&s);
                source.fmt_explain(f);
            }
            Self::Hash { source, ops, .. } => {
                let mut s = "HashAggregate".to_owned();
                let iter = ops.iter().filter_map(|op| match op {
                    AggregateOp::ApplyAggregate(ApplyAggregateOp { input, output, .. }) => {
                        Some((*input, *output))
                    }
                    _ => None,
                });
                for (i, (input, output)) in iter.enumerate() {
                    s.push_str(if i == 0 { " " } else { ", " });
                    write!(&mut s, "{input} -> {output}").unwrap();
                }
                f.write_str(&s);
                source.fmt_explain(f);
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

pub struct ApplyAggregateOp {
    pub input: ColumnId,
    pub output: ColumnId,
    pub init: AggregateInitFnPtr,
    pub is_distinct: bool,
}

pub enum AggregateOp {
    ApplyAggregate(ApplyAggregateOp),
    GroupBy { target: ColumnId },
    Passthrough { target: ColumnId },
}

impl<T: Storage> PlanNode<'_, '_, T> {
    pub(super) fn ungrouped_aggregate(self, ops: Vec<ApplyAggregateOp>) -> Self {
        PlanNode::Aggregate(Aggregate::Ungrouped {
            source: Box::new(self),
            ops,
        })
    }

    pub(super) fn hash_aggregate(self, ops: Vec<AggregateOp>) -> Self {
        PlanNode::Aggregate(Aggregate::Hash {
            source: Box::new(self),
            ops,
        })
    }
}

pub struct AggregateCollection<'a, 'txn, 'db, T: Storage> {
    planner: &'a Planner<'txn, 'db, T>,
    aggregates: HashMap<parser::FunctionCall, BoundAggregate<'txn, T>>,
}

impl<'a, 'txn, 'db, T: Storage> AggregateCollection<'a, 'txn, 'db, T> {
    pub fn new(planner: &'a Planner<'txn, 'db, T>) -> Self {
        Self {
            planner,
            aggregates: HashMap::new(),
        }
    }

    pub fn finish(self) -> AggregatePlanner<'a, 'txn, 'db, T> {
        AggregatePlanner(self)
    }

    pub fn gather(
        &mut self,
        source: PlanNode<'txn, 'db, T>,
        expr: &parser::Expression,
    ) -> PlannerResult<PlanNode<'txn, 'db, T>> {
        self.gather_inner(source, expr, false)
    }

    fn gather_inner(
        &mut self,
        source: PlanNode<'txn, 'db, T>,
        expr: &parser::Expression,
        in_aggregate_args: bool,
    ) -> PlannerResult<PlanNode<'txn, 'db, T>> {
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

pub struct AggregatePlanner<'a, 'txn, 'db, T: Storage>(AggregateCollection<'a, 'txn, 'db, T>);

impl<'txn, 'db, T: Storage> AggregatePlanner<'_, 'txn, 'db, T> {
    pub fn has_aggregates(&self) -> bool {
        !self.0.aggregates.is_empty()
    }

    pub fn plan(
        &self,
        expr_binder: &ExpressionBinder<'_, 'txn, 'db, T>,
        source: PlanNode<'txn, 'db, T>,
        group_by: Vec<parser::Expression>,
    ) -> PlannerResult<PlanNode<'txn, 'db, T>> {
        let mut plan = source;
        let mut exprs = Vec::with_capacity(self.0.aggregates.len() + group_by.len());

        // The first `aggregates.len()` columns are the aggregated columns
        // that are passed to the respective aggregate functions.
        {
            let column_map = self.0.planner.column_map();
            for aggregate in self.0.aggregates.values() {
                exprs.push(TypedExpression {
                    expr: aggregate.arg.clone(),
                    ty: column_map[aggregate.output].ty,
                });
            }
        }

        // The rest of the columns are the columns in the GROUP BY clause.
        let has_group_by = !group_by.is_empty();
        for group_by in group_by {
            let (new_plan, expr) = expr_binder.bind(plan, group_by)?;
            plan = new_plan;
            exprs.push(expr);
        }

        let plan = plan.project(&mut self.0.planner.column_map(), exprs);
        let outputs = plan.outputs();

        let ops = self
            .0
            .aggregates
            .iter()
            .zip(outputs.iter().take(self.0.aggregates.len()))
            .map(|((func_call, aggregate), input)| ApplyAggregateOp {
                init: aggregate.function.init,
                is_distinct: func_call.is_distinct,
                input: *input,
                output: aggregate.output,
            });
        if has_group_by {
            let group_by_ops = outputs
                .iter()
                .skip(self.0.aggregates.len())
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

    pub fn resolve(&self, function_call: &parser::FunctionCall) -> Option<ColumnId> {
        self.0
            .aggregates
            .get(function_call)
            .map(|aggregate| aggregate.output)
    }
}

struct BoundAggregate<'a, T: Storage> {
    function: &'a AggregateFunction,
    arg: planner::Expression<'a, T, ColumnId>,
    output: ColumnId,
}
