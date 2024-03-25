use super::{aggregate::AggregateContext, Plan, PlanNode, Planner, PlannerError, PlannerResult};
use crate::{
    catalog::Aggregator,
    executor::{ExecutorError, ExecutorResult},
    parser::{self, BinaryOp, UnaryOp},
    planner,
    rows::ColumnIndex,
    types::{NullableType, Type},
    Storage, Value,
};

#[derive(Debug, Clone)]
pub enum Expression {
    Constact(Value),
    ColumnRef {
        index: ColumnIndex,
    },
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expression>,
    },
    BinaryOp {
        op: BinaryOp,
        lhs: Box<Expression>,
        rhs: Box<Expression>,
    },
}

impl std::fmt::Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Constact(value) => write!(f, "{value:?}"),
            Self::ColumnRef { index } => write!(f, "{index}"),
            Self::UnaryOp { op, expr } => write!(f, "({op} {expr})"),
            Self::BinaryOp { op, lhs, rhs } => {
                write!(f, "({lhs} {op} {rhs})")
            }
        }
    }
}

pub struct TypedExpression {
    pub expr: planner::Expression,
    pub ty: NullableType,
}

impl From<Value> for TypedExpression {
    fn from(value: Value) -> Self {
        let ty = value.ty();
        Self {
            expr: planner::Expression::Constact(value),
            ty,
        }
    }
}

impl TypedExpression {
    pub fn expect_type<T: Into<NullableType>>(
        self,
        expected: T,
    ) -> PlannerResult<planner::Expression> {
        if self.ty.is_compatible_with(expected.into()) {
            Ok(self.expr)
        } else {
            Err(PlannerError::TypeError)
        }
    }
}

impl<'txn, 'db, T: Storage> Planner<'txn, 'db, T> {
    pub fn bind_expr(
        &self,
        source: Plan<'txn, 'db, T>,
        expr: parser::Expression,
    ) -> PlannerResult<(Plan<'txn, 'db, T>, TypedExpression)> {
        ExpressionBinder::new(self).bind(source, expr)
    }

    pub fn bind_expr_without_source(
        &self,
        expr: parser::Expression,
    ) -> PlannerResult<TypedExpression> {
        ExpressionBinder::new(self).bind_without_source(expr)
    }
}

pub struct ExpressionBinder<'a, 'txn, 'db, T: Storage> {
    planner: &'a Planner<'txn, 'db, T>,
    aggregate_ctx: Option<AggregateContext<'txn>>,
}

impl<'a, 'txn, 'db, T: Storage> ExpressionBinder<'a, 'txn, 'db, T> {
    pub fn new(planner: &'a Planner<'txn, 'db, T>) -> Self {
        Self {
            planner,
            aggregate_ctx: None,
        }
    }

    pub fn with_aggregate_context(self, aggregate_ctx: AggregateContext<'txn>) -> Self {
        Self {
            aggregate_ctx: Some(aggregate_ctx),
            ..self
        }
    }

    pub fn bind(
        &self,
        source: Plan<'txn, 'db, T>,
        expr: parser::Expression,
    ) -> PlannerResult<(Plan<'txn, 'db, T>, TypedExpression)> {
        match expr {
            parser::Expression::Constant(value) => Ok((source, value.into())),
            parser::Expression::ColumnRef(column_ref) => {
                let (index, column) = source.schema.resolve_column(&column_ref)?;
                let expr = planner::Expression::ColumnRef { index };
                let ty = column.ty;
                Ok((source, TypedExpression { expr, ty }))
            }
            parser::Expression::UnaryOp { op, expr } => {
                let (plan, TypedExpression { expr, ty }) = self.bind(source, *expr)?;
                let ty = match (op, ty) {
                    (_, NullableType::Null) => NullableType::Null,
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
                        (NullableType::Null, _) | (_, NullableType::Null) => NullableType::Null,
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
                        (NullableType::Null, _) | (_, NullableType::Null) => NullableType::Null,
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
            parser::Expression::Function { name, args } => {
                // We currently assume all functions in expressions are
                // aggregate functions.
                self.planner.catalog.aggregate_function(&name)?;
                Ok((source, self.resolve_aggregate(name, args)?))
            }
            parser::Expression::ScalarSubquery(select) => {
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

                fn init_agg() -> Box<dyn Aggregator> {
                    Box::<AssertSingleRow>::default()
                }

                let column_name = select.to_string();
                let subquery_plan = self.planner.plan_select(*select)?;
                let [subquery_result_column] = subquery_plan
                    .schema
                    .0
                    .try_into()
                    .map_err(|_| PlannerError::MultipleColumnsFromSubquery)?;

                // Equivalent to `SELECT assert_single_row(subquery)`
                let subquery_plan = Plan {
                    node: PlanNode::Aggregate(planner::Aggregate::Ungrouped {
                        source: Box::new(subquery_plan.node),
                        init_functions: vec![init_agg],
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
            parser::Expression::Exists(select) => {
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

                fn init_agg() -> Box<dyn Aggregator> {
                    Box::<Exists>::default()
                }

                let column_name = format!("EXISTS ({select})");
                let subquery_plan = self.planner.plan_select(*select)?;

                // Equivalent to `SELECT exists(SELECT * FROM subquery LIMIT 1)`
                let subquery_node = PlanNode::Limit(planner::Limit {
                    source: Box::new(subquery_plan.node),
                    limit: Some(planner::Expression::Constact(1.into())),
                    offset: None,
                });
                let subquery_plan = Plan {
                    node: PlanNode::Aggregate(planner::Aggregate::Ungrouped {
                        source: Box::new(subquery_node),
                        init_functions: vec![init_agg],
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

    pub fn bind_without_source(&self, expr: parser::Expression) -> PlannerResult<TypedExpression> {
        self.bind(Plan::empty_source(), expr).map(|(_, e)| e)
    }

    fn resolve_aggregate(
        &self,
        name: String,
        args: parser::FunctionArgs,
    ) -> PlannerResult<TypedExpression> {
        let Some(aggregate_ctx) = &self.aggregate_ctx else {
            return Err(PlannerError::AggregateNotAllowed);
        };
        let (index, column) = aggregate_ctx.resolve_aggregate(name, args).unwrap();
        Ok(TypedExpression {
            expr: planner::Expression::ColumnRef { index },
            ty: column.ty,
        })
    }
}

/// Attaches a subquery to the source plan and returns the new plan and
/// an expression that references the first column of the subquery.
fn attach_subquery<'txn, 'db, T: Storage>(
    source: Plan<'txn, 'db, T>,
    subquery: Plan<'txn, 'db, T>,
) -> (Plan<'txn, 'db, T>, TypedExpression) {
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
        expr: planner::Expression::ColumnRef {
            index: subquery_column_index,
        },
        ty: subquery_column_type,
    };
    (plan, expr)
}
