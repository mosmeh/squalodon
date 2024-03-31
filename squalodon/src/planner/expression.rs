use super::{aggregate::AggregatePlanner, Plan, PlanNode, Planner, PlannerError, PlannerResult};
use crate::{
    catalog::{Aggregator, ScalarEvalFnPtr},
    executor::{ExecutorError, ExecutorResult},
    parser::{self, BinaryOp, FunctionArgs, UnaryOp},
    planner::{self, aggregate::ApplyAggregateOp},
    rows::ColumnIndex,
    types::{NullableType, Type},
    CatalogError, Storage, Value,
};
use std::collections::HashMap;

pub enum Expression<T: Storage> {
    Constact(Value),
    ColumnRef(ColumnIndex),
    Cast {
        expr: Box<Expression<T>>,
        ty: Type,
    },
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expression<T>>,
    },
    BinaryOp {
        op: BinaryOp,
        lhs: Box<Expression<T>>,
        rhs: Box<Expression<T>>,
    },
    Like {
        str_expr: Box<Expression<T>>,
        pattern: Box<Expression<T>>,
        case_insensitive: bool,
    },
    Function {
        eval: ScalarEvalFnPtr<T>,
        args: Vec<Expression<T>>,
    },
}

impl<T: Storage> Clone for Expression<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Constact(value) => Self::Constact(value.clone()),
            Self::ColumnRef(index) => Self::ColumnRef(*index),
            Self::Cast { expr, ty } => Self::Cast {
                expr: expr.clone(),
                ty: *ty,
            },
            Self::UnaryOp { op, expr } => Self::UnaryOp {
                op: *op,
                expr: expr.clone(),
            },
            Self::BinaryOp { op, lhs, rhs } => Self::BinaryOp {
                op: *op,
                lhs: lhs.clone(),
                rhs: rhs.clone(),
            },
            Self::Like {
                str_expr,
                pattern,
                case_insensitive,
            } => Self::Like {
                str_expr: str_expr.clone(),
                pattern: pattern.clone(),
                case_insensitive: *case_insensitive,
            },
            Self::Function { eval, args } => Self::Function {
                eval: *eval,
                args: args.clone(),
            },
        }
    }
}

impl<T: Storage> std::fmt::Display for Expression<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Constact(value) => write!(f, "{value:?}"),
            Self::ColumnRef(index) => write!(f, "{index}"),
            Self::Cast { expr, ty } => write!(f, "CAST({expr} AS {ty})"),
            Self::UnaryOp { op, expr } => write!(f, "({op} {expr})"),
            Self::BinaryOp { op, lhs, rhs } => {
                write!(f, "({lhs} {op} {rhs})")
            }
            Self::Like {
                str_expr,
                pattern,
                case_insensitive,
            } => {
                write!(f, "({str_expr} ")?;
                f.write_str(if *case_insensitive { "ILIKE" } else { "LIKE" })?;
                write!(f, " {pattern})")
            }
            Self::Function { eval, args } => {
                write!(f, "({eval:?})(")?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{arg}")?;
                }
                f.write_str(")")
            }
        }
    }
}

pub struct TypedExpression<T: Storage> {
    pub expr: planner::Expression<T>,
    pub ty: NullableType,
}

impl<T: Storage> Clone for TypedExpression<T> {
    fn clone(&self) -> Self {
        Self {
            expr: self.expr.clone(),
            ty: self.ty,
        }
    }
}

impl<T: Storage> From<Value> for TypedExpression<T> {
    fn from(value: Value) -> Self {
        let ty = value.ty();
        Self {
            expr: planner::Expression::Constact(value),
            ty,
        }
    }
}

impl<T: Storage> TypedExpression<T> {
    pub fn expect_type<I: Into<NullableType>>(
        self,
        expected: I,
    ) -> PlannerResult<planner::Expression<T>> {
        if self.ty.is_compatible_with(expected.into()) {
            Ok(self.expr)
        } else {
            Err(PlannerError::TypeError)
        }
    }
}

pub struct ExpressionBinder<'a, 'txn, 'db, T: Storage> {
    planner: &'a Planner<'txn, 'db, T>,
    aliases: Option<&'a HashMap<String, TypedExpression<T>>>,
    aggregates: Option<&'a AggregatePlanner<'a, 'txn, 'db, T>>,
}

impl<'a, 'txn, 'db, T: Storage> ExpressionBinder<'a, 'txn, 'db, T> {
    pub fn new(planner: &'a Planner<'txn, 'db, T>) -> Self {
        Self {
            planner,
            aliases: None,
            aggregates: None,
        }
    }

    pub fn with_aliases(&self, aliases: &'a HashMap<String, TypedExpression<T>>) -> Self {
        Self {
            aliases: Some(aliases),
            ..*self
        }
    }

    pub fn with_aggregates(&self, aggregates: &'a AggregatePlanner<'a, 'txn, 'db, T>) -> Self {
        Self {
            aggregates: Some(aggregates),
            ..*self
        }
    }

    pub fn bind(
        &self,
        source: Plan<'txn, 'db, T>,
        expr: parser::Expression,
    ) -> PlannerResult<(Plan<'txn, 'db, T>, TypedExpression<T>)> {
        match expr {
            parser::Expression::Constant(value) => Ok((source, value.into())),
            parser::Expression::ColumnRef(column_ref) => {
                if column_ref.table_name.is_none() {
                    if let Some(expr) = self
                        .aliases
                        .and_then(|aliases| aliases.get(&column_ref.column_name))
                    {
                        return Ok((source, expr.clone()));
                    }
                }
                let (index, column) = source.schema.resolve_column(&column_ref)?;
                let expr = planner::Expression::ColumnRef(index);
                let ty = column.ty;
                Ok((source, TypedExpression { expr, ty }))
            }
            parser::Expression::Cast { expr, ty } => {
                let (plan, TypedExpression { expr, .. }) = self.bind(source, *expr)?;
                let expr = planner::Expression::Cast {
                    expr: expr.into(),
                    ty,
                };
                let ty = ty.into();
                Ok((plan, TypedExpression { expr, ty }))
            }
            parser::Expression::UnaryOp { op, expr } => {
                let (plan, TypedExpression { expr, ty }) = self.bind(source, *expr)?;
                let ty = match (op, ty) {
                    (_, NullableType::Null) => return Ok((plan, Value::Null.into())),
                    (UnaryOp::Not, _) => NullableType::NonNull(Type::Boolean),
                    (UnaryOp::Plus | UnaryOp::Minus, NullableType::NonNull(ty))
                        if ty.is_numeric() =>
                    {
                        NullableType::NonNull(ty)
                    }
                    _ => return Err(PlannerError::TypeError),
                };
                let expr = planner::Expression::UnaryOp {
                    op,
                    expr: expr.into(),
                };
                Ok((plan, TypedExpression { expr, ty }))
            }
            parser::Expression::BinaryOp { op, lhs, rhs } => {
                let (plan, lhs) = self.bind(source, *lhs)?;
                let (plan, rhs) = self.bind(plan, *rhs)?;
                let ty = match op {
                    BinaryOp::Add
                    | BinaryOp::Sub
                    | BinaryOp::Mul
                    | BinaryOp::Div
                    | BinaryOp::Mod => match (lhs.ty, rhs.ty) {
                        (NullableType::Null, _) | (_, NullableType::Null) => {
                            return Ok((plan, Value::Null.into()))
                        }
                        (
                            NullableType::NonNull(Type::Integer),
                            NullableType::NonNull(Type::Integer),
                        ) => NullableType::NonNull(Type::Integer),
                        (NullableType::NonNull(ty), NullableType::NonNull(Type::Real))
                        | (NullableType::NonNull(Type::Real), NullableType::NonNull(ty))
                            if ty.is_numeric() =>
                        {
                            NullableType::NonNull(Type::Real)
                        }
                        _ => return Err(PlannerError::TypeError),
                    },
                    BinaryOp::Eq
                    | BinaryOp::Ne
                    | BinaryOp::Lt
                    | BinaryOp::Le
                    | BinaryOp::Gt
                    | BinaryOp::Ge
                    | BinaryOp::And
                    | BinaryOp::Or => match (lhs.ty, rhs.ty) {
                        (NullableType::Null, _) | (_, NullableType::Null) => NullableType::Null,
                        (NullableType::NonNull(lhs_ty), NullableType::NonNull(rhs_ty))
                            if lhs_ty == rhs_ty =>
                        {
                            NullableType::NonNull(Type::Boolean)
                        }
                        _ => return Err(PlannerError::TypeError),
                    },
                    BinaryOp::Concat => match (lhs.ty, rhs.ty) {
                        (NullableType::Null, _) | (_, NullableType::Null) => {
                            return Ok((plan, Value::Null.into()))
                        }
                        (NullableType::NonNull(Type::Text), NullableType::NonNull(Type::Text)) => {
                            NullableType::NonNull(Type::Text)
                        }
                        _ => return Err(PlannerError::TypeError),
                    },
                };
                let expr = planner::Expression::BinaryOp {
                    op,
                    lhs: lhs.expr.into(),
                    rhs: rhs.expr.into(),
                };
                Ok((plan, TypedExpression { expr, ty }))
            }
            parser::Expression::Like {
                str_expr,
                pattern,
                case_insensitive,
            } => {
                let (
                    plan,
                    TypedExpression {
                        expr: str_expr,
                        ty: expr_ty,
                    },
                ) = self.bind(source, *str_expr)?;
                let (
                    plan,
                    TypedExpression {
                        expr: pattern,
                        ty: pattern_ty,
                    },
                ) = self.bind(plan, *pattern)?;
                if matches!(expr_ty, NullableType::Null) || matches!(pattern_ty, NullableType::Null)
                {
                    return Ok((plan, Value::Null.into()));
                }
                if !matches!(expr_ty, NullableType::NonNull(Type::Text))
                    || !matches!(pattern_ty, NullableType::NonNull(Type::Text))
                {
                    return Err(PlannerError::TypeError);
                }
                let expr = planner::Expression::Like {
                    str_expr: str_expr.into(),
                    pattern: pattern.into(),
                    case_insensitive,
                };
                Ok((
                    plan,
                    TypedExpression {
                        expr,
                        ty: Type::Boolean.into(),
                    },
                ))
            }
            parser::Expression::Function {
                name,
                args,
                is_distinct,
            } => {
                match self.planner.catalog.aggregate_function(&name) {
                    Ok(_) => {
                        let Some(aggregates) = &self.aggregates else {
                            return Err(PlannerError::AggregateNotAllowed);
                        };
                        let (index, column) = aggregates
                            .resolve(name, args, is_distinct)
                            .ok_or_else(|| PlannerError::UnknownColumn("(aggregate)".to_owned()))?;
                        let expr = TypedExpression {
                            expr: planner::Expression::ColumnRef(index),
                            ty: column.ty,
                        };
                        return Ok((source, expr));
                    }
                    Err(CatalogError::UnknownEntry(_, _)) => (),
                    Err(e) => return Err(e.into()),
                }

                let function = self.planner.catalog.scalar_function(&name)?;
                if is_distinct {
                    return Err(PlannerError::InvalidArgument);
                }
                let args = match args {
                    FunctionArgs::Expressions(args) => args,
                    FunctionArgs::Wildcard => return Err(PlannerError::InvalidArgument),
                };
                let mut arg_exprs = Vec::with_capacity(args.len());
                let mut arg_types = Vec::with_capacity(args.len());
                let mut plan = source;
                for arg in args {
                    let (new_plan, TypedExpression { expr, ty }) = self.bind(plan, arg)?;
                    plan = new_plan;
                    arg_exprs.push(expr);
                    arg_types.push(ty);
                }
                let ty = (function.bind)(&arg_types)?;
                let expr = planner::Expression::Function {
                    eval: function.eval,
                    args: arg_exprs,
                };
                Ok((plan, TypedExpression { expr, ty }))
            }
            parser::Expression::ScalarSubquery(query) => {
                /// An aggregator that asserts that the subquery returns
                /// a single row.
                #[derive(Default)]
                struct AssertSingleRow {
                    value: Option<Value>,
                }

                impl Aggregator for AssertSingleRow {
                    fn update(&mut self, value: &Value) -> ExecutorResult<()> {
                        if self.value.is_some() {
                            Err(ExecutorError::MultipleRowsFromSubquery)
                        } else {
                            self.value = Some(value.clone());
                            Ok(())
                        }
                    }

                    fn finish(&self) -> Value {
                        self.value.clone().unwrap_or(Value::Null)
                    }
                }

                let column_name = query.to_string();
                let subquery_plan = self.planner.plan_query(*query)?;
                let [subquery_result_column] = subquery_plan
                    .schema
                    .0
                    .try_into()
                    .map_err(|_| PlannerError::MultipleColumnsFromSubquery)?;

                // Equivalent to `SELECT assert_single_row(subquery)`
                let subquery_plan = Plan {
                    node: PlanNode::Aggregate(planner::Aggregate::Ungrouped {
                        source: Box::new(subquery_plan.node),
                        column_ops: vec![ApplyAggregateOp {
                            init: || Box::<AssertSingleRow>::default(),
                            is_distinct: false,
                        }],
                    }),
                    schema: vec![planner::Column {
                        table_name: None,
                        column_name,
                        ty: subquery_result_column.ty,
                    }]
                    .into(),
                };
                Ok(attach_subquery(source, subquery_plan))
            }
            parser::Expression::Exists(query) => {
                /// An aggregator that produces a boolean indicating whether
                /// the subquery returns any rows.
                #[derive(Default)]
                struct Exists {
                    yes: bool,
                }

                impl Aggregator for Exists {
                    fn update(&mut self, _: &Value) -> ExecutorResult<()> {
                        self.yes = true;
                        Ok(())
                    }

                    fn finish(&self) -> Value {
                        Value::from(self.yes)
                    }
                }

                let column_name = format!("EXISTS ({query})");
                let subquery_plan = self.planner.plan_query(*query)?;

                // Equivalent to `SELECT exists(SELECT * FROM subquery LIMIT 1)`
                let subquery_node = PlanNode::Limit(planner::Limit {
                    source: Box::new(subquery_plan.node),
                    limit: Some(planner::Expression::Constact(1.into())),
                    offset: None,
                });
                let subquery_plan = Plan {
                    node: PlanNode::Aggregate(planner::Aggregate::Ungrouped {
                        source: Box::new(subquery_node),
                        column_ops: vec![ApplyAggregateOp {
                            init: || Box::<Exists>::default(),
                            is_distinct: false,
                        }],
                    }),
                    schema: vec![planner::Column {
                        table_name: None,
                        column_name,
                        ty: Type::Boolean.into(),
                    }]
                    .into(),
                };
                Ok(attach_subquery(source, subquery_plan))
            }
            parser::Expression::Parameter(i) => {
                let value = match self.planner.params.get(i.get() - 1) {
                    Some(value) => value.clone(),
                    None => return Err(PlannerError::ParameterNotProvided(i)),
                };
                Ok((source, value.into()))
            }
        }
    }

    pub fn bind_without_source(
        &self,
        expr: parser::Expression,
    ) -> PlannerResult<TypedExpression<T>> {
        self.bind(Plan::empty_source(), expr).map(|(_, e)| e)
    }
}

/// Attaches a subquery to the source plan and returns the new plan and
/// an expression that references the first column of the subquery.
fn attach_subquery<'txn, 'db, T: Storage>(
    source: Plan<'txn, 'db, T>,
    subquery: Plan<'txn, 'db, T>,
) -> (Plan<'txn, 'db, T>, TypedExpression<T>) {
    let subquery_column_index = ColumnIndex(source.schema.0.len());
    let subquery_column_type = subquery.schema.0[0].ty;

    let mut columns = source.schema.0;
    columns.extend(subquery.schema.0);

    let plan = Plan {
        node: PlanNode::CrossProduct(planner::CrossProduct {
            left: Box::new(source.node),
            right: Box::new(subquery.node),
        }),
        schema: columns.into(),
    };
    let expr = TypedExpression {
        expr: planner::Expression::ColumnRef(subquery_column_index),
        ty: subquery_column_type,
    };
    (plan, expr)
}
