use super::{
    expression::{ExpressionBinder, TypedExpression},
    Explain, ExplainVisitor, Plan, PlanNode, Planner, PlannerError, PlannerResult,
};
use crate::{
    catalog::{AggregateFunction, AggregateInitFnPtr},
    parser, planner,
    rows::ColumnIndex,
    CatalogError, Storage, Type,
};
use std::collections::HashMap;

pub enum Aggregate<'txn, 'db, T: Storage> {
    Ungrouped {
        source: Box<PlanNode<'txn, 'db, T>>,
        column_ops: Vec<ApplyAggregateOp>,
    },
    Hash {
        source: Box<PlanNode<'txn, 'db, T>>,
        column_ops: Vec<AggregateOp>,
    },
}

impl<T: Storage> Explain for Aggregate<'_, '_, T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        match self {
            Self::Ungrouped { source, column_ops } => {
                let mut s = "UngroupedAggregate ".to_owned();
                for (i, aggregation) in column_ops.iter().enumerate() {
                    if i > 0 {
                        s.push_str(", ");
                    }
                    s.push_str(if aggregation.is_distinct {
                        "Distinct"
                    } else {
                        "NonDistinct"
                    });
                }
                visitor.write_str(&s);
                source.visit(visitor);
            }
            Self::Hash { source, column_ops } => {
                let mut s = "HashAggregate ".to_owned();
                for (i, op) in column_ops.iter().enumerate() {
                    if i > 0 {
                        s.push_str(", ");
                    }
                    match op {
                        AggregateOp::GroupBy => s.push_str("GroupBy"),
                        AggregateOp::ApplyAggregate(ApplyAggregateOp { is_distinct, .. }) => {
                            s.push_str("ApplyAggregate");
                            if *is_distinct {
                                s.push_str("(Distinct)");
                            }
                        }
                        AggregateOp::Passthrough => s.push_str("Passthrough"),
                    }
                }
                visitor.write_str(&s);
                source.visit(visitor);
            }
        }
    }
}

pub struct ApplyAggregateOp {
    pub init: AggregateInitFnPtr,
    pub is_distinct: bool,
}

pub enum AggregateOp {
    GroupBy,
    ApplyAggregate(ApplyAggregateOp),
    Passthrough,
}

pub struct AggregateCollection<'a, 'txn, 'db, T: Storage> {
    planner: &'a Planner<'txn, 'db, T>,
    aggregate_calls: HashMap<AggregateCall, usize>,
    bound_aggregates: Vec<BoundAggregate<'txn, T>>,
}

impl<'a, 'txn, 'db, T: Storage> AggregateCollection<'a, 'txn, 'db, T> {
    pub fn new(planner: &'a Planner<'txn, 'db, T>) -> Self {
        Self {
            planner,
            aggregate_calls: HashMap::new(),
            bound_aggregates: Vec::new(),
        }
    }

    pub fn finish(self) -> AggregatePlanner<'a, 'txn, 'db, T> {
        AggregatePlanner(self)
    }

    pub fn gather(
        &mut self,
        source: Plan<'txn, 'db, T>,
        expr: &parser::Expression,
    ) -> PlannerResult<Plan<'txn, 'db, T>> {
        self.gather_inner(source, expr, false)
    }

    fn gather_inner(
        &mut self,
        source: Plan<'txn, 'db, T>,
        expr: &parser::Expression,
        in_aggregate_args: bool,
    ) -> PlannerResult<Plan<'txn, 'db, T>> {
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
            parser::Expression::Like {
                str_expr, pattern, ..
            } => {
                let plan = self.gather_inner(source, str_expr, in_aggregate_args)?;
                self.gather_inner(plan, pattern, in_aggregate_args)
            }
            parser::Expression::Function {
                name,
                args,
                is_distinct,
            } => {
                match self.planner.catalog.aggregate_function(name) {
                    Ok(function) => {
                        if in_aggregate_args {
                            // Nested aggregate functions are not allowed.
                            return Err(PlannerError::AggregateNotAllowed);
                        }

                        let (plan, bound_expr) = match args {
                            parser::FunctionArgs::Wildcard
                                if name.eq_ignore_ascii_case("count") =>
                            {
                                // `count(*)` is a special case equivalent to `count(1)`.
                                (
                                    source,
                                    TypedExpression {
                                        expr: planner::Expression::Constact(1.into()),
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

                        let aggregate = AggregateCall {
                            function_name: name.clone(),
                            args: args.clone(),
                            is_distinct: *is_distinct,
                        };
                        let std::collections::hash_map::Entry::Vacant(entry) =
                            self.aggregate_calls.entry(aggregate)
                        else {
                            return Ok(plan);
                        };

                        let index = self.bound_aggregates.len();
                        self.bound_aggregates.push(BoundAggregate {
                            function,
                            arg: bound_expr.expr,
                            is_distinct: *is_distinct,
                            result_column: planner::Column {
                                table_name: None,
                                column_name: expr.to_string(),
                                ty: (function.bind)(bound_expr.ty)?,
                            },
                        });
                        entry.insert(index);
                        return Ok(plan);
                    }
                    Err(CatalogError::UnknownEntry(_, _)) => (),
                    Err(err) => return Err(err.into()),
                }

                self.planner.catalog.scalar_function(name)?; // Check if the function exists
                if *is_distinct {
                    return Err(PlannerError::InvalidArgument);
                }
                match args {
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
        !self.0.aggregate_calls.is_empty()
    }

    pub fn plan(
        &self,
        expr_binder: &ExpressionBinder<'_, 'txn, 'db, T>,
        source: Plan<'txn, 'db, T>,
        group_by: Vec<parser::Expression>,
    ) -> PlannerResult<Plan<'txn, 'db, T>> {
        let mut plan = source;
        let mut exprs = Vec::with_capacity(self.0.bound_aggregates.len());
        let mut columns = Vec::with_capacity(self.0.bound_aggregates.len());

        // The first `bound_aggregates.len()` columns are the aggregated columns
        // that are passed to the respective aggregate functions.
        for bound_aggregate in &self.0.bound_aggregates {
            exprs.push(bound_aggregate.arg.clone());
            columns.push(bound_aggregate.result_column.clone());
        }

        // The rest of the columns are the columns in the GROUP BY clause.
        for group_by in &group_by {
            let table_name;
            let column_name;
            if let parser::Expression::ColumnRef(column_ref) = group_by {
                table_name = column_ref.table_name.clone();
                column_name = column_ref.column_name.clone();
            } else {
                table_name = None;
                column_name = group_by.to_string();
            };

            let (new_plan, TypedExpression { expr, ty }) =
                expr_binder.bind(plan, group_by.clone())?;
            plan = new_plan;

            exprs.push(expr);
            columns.push(planner::Column {
                table_name,
                column_name,
                ty,
            });
        }

        let node = PlanNode::Project(planner::Project {
            source: Box::new(plan.node),
            exprs,
        });

        let column_ops = self
            .0
            .bound_aggregates
            .iter()
            .map(|bound_aggregate| ApplyAggregateOp {
                init: bound_aggregate.function.init,
                is_distinct: bound_aggregate.is_distinct,
            });
        let node = if group_by.is_empty() {
            Aggregate::Ungrouped {
                source: Box::new(node),
                column_ops: column_ops.collect(),
            }
        } else {
            let column_ops = column_ops
                .map(AggregateOp::ApplyAggregate)
                .chain(group_by.iter().map(|_| AggregateOp::GroupBy))
                .collect();
            Aggregate::Hash {
                source: Box::new(node),
                column_ops,
            }
        };
        Ok(Plan {
            node: PlanNode::Aggregate(node),
            schema: columns.into(),
        })
    }

    pub fn resolve(
        &self,
        function_name: String,
        args: parser::FunctionArgs,
        is_distinct: bool,
    ) -> Option<(ColumnIndex, &planner::Column)> {
        self.0
            .aggregate_calls
            .get(&AggregateCall {
                function_name,
                args,
                is_distinct,
            })
            .map(|&index| {
                let bound_aggregate = &self.0.bound_aggregates[index];
                (ColumnIndex(index), &bound_aggregate.result_column)
            })
    }
}

#[derive(PartialEq, Eq, Hash)]
struct AggregateCall {
    function_name: String,
    args: parser::FunctionArgs,
    is_distinct: bool,
}

struct BoundAggregate<'a, T: Storage> {
    function: &'a AggregateFunction,
    arg: planner::Expression<T>,
    is_distinct: bool,
    result_column: planner::Column,
}
