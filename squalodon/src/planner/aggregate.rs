use super::{
    expression::{ExpressionBinder, TypedExpression},
    Explain, ExplainVisitor, Plan, PlanNode, PlannerError, PlannerResult,
};
use crate::{
    catalog::{AggregateFunction, AggregateInitFnPtr, CatalogRef},
    parser, planner,
    rows::ColumnIndex,
    CatalogError, Storage, Type,
};
use std::collections::HashMap;

pub enum Aggregate<'txn, 'db, T: Storage> {
    Ungrouped {
        source: Box<PlanNode<'txn, 'db, T>>,
        init_functions: Vec<AggregateInitFnPtr>,
    },
    Hash {
        source: Box<PlanNode<'txn, 'db, T>>,
        init_functions: Vec<AggregateInitFnPtr>,
    },
}

impl<T: Storage> Explain for Aggregate<'_, '_, T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        match self {
            Self::Ungrouped {
                source,
                init_functions,
            } => {
                write!(
                    visitor,
                    "UngroupedAggregate #aggregated={}",
                    init_functions.len()
                );
                source.visit(visitor);
            }
            Self::Hash {
                source,
                init_functions,
            } => {
                write!(
                    visitor,
                    "HashAggregate #aggregated={}",
                    init_functions.len()
                );
                source.visit(visitor);
            }
        }
    }
}

#[derive(Default)]
pub struct AggregateContext<'txn> {
    aggregate_calls: HashMap<AggregateCall, usize>,
    bound_aggregates: Vec<BoundAggregate<'txn>>,
}

impl<'txn> AggregateContext<'txn> {
    pub fn has_aggregates(&self) -> bool {
        !self.aggregate_calls.is_empty()
    }

    pub fn gather_aggregates<'db, T: Storage>(
        &mut self,
        catalog: &'txn CatalogRef<'txn, 'db, T>,
        source: Plan<'txn, 'db, T>,
        expr: &parser::Expression,
    ) -> PlannerResult<Plan<'txn, 'db, T>> {
        self.gather_aggregates_inner(catalog, source, expr, false)
    }

    fn gather_aggregates_inner<'db, T: Storage>(
        &mut self,
        catalog: &'txn CatalogRef<'txn, 'db, T>,
        source: Plan<'txn, 'db, T>,
        expr: &parser::Expression,
        in_aggregate_args: bool,
    ) -> PlannerResult<Plan<'txn, 'db, T>> {
        match expr {
            parser::Expression::Constant(_)
            | parser::Expression::ColumnRef(_)
            | parser::Expression::ScalarSubquery(_)
            | parser::Expression::Exists(_) => Ok(source),
            parser::Expression::UnaryOp { expr, .. } => {
                self.gather_aggregates_inner(catalog, source, expr, in_aggregate_args)
            }
            parser::Expression::BinaryOp { lhs, rhs, .. } => {
                let plan = self.gather_aggregates_inner(catalog, source, lhs, in_aggregate_args)?;
                self.gather_aggregates_inner(catalog, plan, rhs, in_aggregate_args)
            }
            parser::Expression::Function { ref name, ref args } => {
                let function = match catalog.aggregate_function(name) {
                    Ok(func) => func,
                    Err(CatalogError::UnknownEntry(_, _)) => return Ok(source),
                    Err(err) => return Err(err.into()),
                };
                if in_aggregate_args {
                    // Nested aggregate functions are not allowed.
                    return Err(PlannerError::AggregateNotAllowed);
                }

                let (plan, bound_expr) = match args {
                    parser::FunctionArgs::Wildcard if name.eq_ignore_ascii_case("count") => {
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
                        let plan = self.gather_aggregates_inner(catalog, source, &args[0], true)?;
                        ExpressionBinder::new(catalog).bind(plan, args[0].clone())?
                    }
                    _ => return Err(PlannerError::ArityError),
                };

                let aggregate = AggregateCall {
                    function_name: name.clone(),
                    args: args.clone(),
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
                    result_column: planner::Column {
                        table_name: None,
                        column_name: expr.to_string(),
                        ty: (function.bind)(bound_expr.ty)?,
                    },
                });
                entry.insert(index);
                Ok(plan)
            }
        }
    }

    pub fn bind_aggregates<'db, T: Storage>(
        &self,
        catalog: &'txn CatalogRef<'txn, 'db, T>,
        source: Plan<'txn, 'db, T>,
        group_by: Vec<parser::Expression>,
    ) -> PlannerResult<Plan<'txn, 'db, T>> {
        let mut plan = source;
        let mut exprs = Vec::with_capacity(self.bound_aggregates.len());
        let mut columns = Vec::with_capacity(self.bound_aggregates.len());

        // The first `bound_aggregates.len()` columns are the aggregated columns
        // that are passed to the respective aggregate functions.
        for bound_aggregate in &self.bound_aggregates {
            exprs.push(bound_aggregate.arg.clone());
            columns.push(bound_aggregate.result_column.clone());
        }

        // The rest of the columns are the columns in the GROUP BY clause.
        let expr_binder = ExpressionBinder::new(catalog);
        for group_by in &group_by {
            let (new_plan, TypedExpression { expr, ty }) =
                expr_binder.bind(plan, group_by.clone())?;
            plan = new_plan;
            let column = if let planner::Expression::ColumnRef { index } = expr {
                plan.schema.0[index.0].clone()
            } else {
                planner::Column {
                    table_name: None,
                    column_name: group_by.to_string(),
                    ty,
                }
            };
            exprs.push(expr);
            columns.push(column);
        }

        let node = PlanNode::Project(planner::Project {
            source: Box::new(plan.node),
            exprs,
        });

        let init_functions = self
            .bound_aggregates
            .iter()
            .map(|bound_aggregate| bound_aggregate.function.init)
            .collect();

        let node = if group_by.is_empty() {
            Aggregate::Ungrouped {
                source: Box::new(node),
                init_functions,
            }
        } else {
            Aggregate::Hash {
                source: Box::new(node),
                init_functions,
            }
        };
        Ok(Plan {
            node: PlanNode::Aggregate(node),
            schema: columns.into(),
        })
    }

    pub fn resolve_aggregate(
        &self,
        function_name: String,
        args: parser::FunctionArgs,
    ) -> Option<(ColumnIndex, &planner::Column)> {
        let index = self.aggregate_calls.get(&AggregateCall {
            function_name,
            args,
        });
        index.map(|&index| {
            let bound_aggregate = &self.bound_aggregates[index];
            (ColumnIndex(index), &bound_aggregate.result_column)
        })
    }
}

#[derive(PartialEq, Eq, Hash)]
struct AggregateCall {
    function_name: String,
    args: parser::FunctionArgs,
}

struct BoundAggregate<'a> {
    function: &'a AggregateFunction,
    arg: planner::Expression,
    result_column: planner::Column,
}
