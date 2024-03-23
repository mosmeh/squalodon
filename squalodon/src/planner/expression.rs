use super::{aggregate::AggregateContext, Binder, Plan, PlanNode, PlannerError, PlannerResult};
use crate::{
    catalog::{Aggregator, CatalogRef},
    executor::{ExecutorError, ExecutorResult},
    parser::{self, BinaryOp, UnaryOp},
    planner,
    rows::ColumnIndex,
    types::Type,
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
    pub ty: Option<Type>,
}

impl TypedExpression {
    pub fn expect_type<T: Into<Option<Type>>>(
        self,
        expected: T,
    ) -> PlannerResult<planner::Expression> {
        if let (Some(actual), Some(expected)) = (self.ty, expected.into()) {
            if actual != expected {
                return Err(PlannerError::TypeError);
            }
        }
        Ok(self.expr)
    }
}

impl<'txn, 'db, T: Storage> Binder<'txn, 'db, T> {
    pub fn bind_expr(
        &self,
        source: Plan<'txn, 'db, T>,
        expr: parser::Expression,
    ) -> PlannerResult<(Plan<'txn, 'db, T>, TypedExpression)> {
        ExpressionBinder::new(self.catalog).bind(source, expr)
    }

    pub fn bind_expr_without_source(
        &self,
        expr: parser::Expression,
    ) -> PlannerResult<TypedExpression> {
        ExpressionBinder::new(self.catalog).bind_without_source(expr)
    }
}

pub struct ExpressionBinder<'txn, 'db, T: Storage> {
    catalog: &'txn CatalogRef<'txn, 'db, T>,
    aggregate_ctx: Option<AggregateContext<'txn>>,
}

impl<'txn, 'db, T: Storage> ExpressionBinder<'txn, 'db, T> {
    pub fn new(catalog: &'txn CatalogRef<'txn, 'db, T>) -> Self {
        Self {
            catalog,
            aggregate_ctx: None,
        }
    }

    pub fn with_aggregate_context(self, aggregate_ctx: AggregateContext<'txn>) -> Self {
        Self {
            catalog: self.catalog,
            aggregate_ctx: Some(aggregate_ctx),
        }
    }

    pub fn bind(
        &self,
        source: Plan<'txn, 'db, T>,
        expr: parser::Expression,
    ) -> PlannerResult<(Plan<'txn, 'db, T>, TypedExpression)> {
        match expr {
            parser::Expression::Constant(value) => {
                let ty = value.ty();
                Ok((
                    source,
                    TypedExpression {
                        expr: planner::Expression::Constact(value),
                        ty,
                    },
                ))
            }
            parser::Expression::ColumnRef(column_ref) => {
                let (index, column) = source.schema.resolve_column(&column_ref)?;
                let expr = planner::Expression::ColumnRef { index };
                let ty = column.ty;
                Ok((source, TypedExpression { expr, ty }))
            }
            parser::Expression::UnaryOp { op, expr } => {
                let (plan, TypedExpression { expr, ty }) = self.bind(source, *expr)?;
                let ty = match (op, ty) {
                    (UnaryOp::Not, _) => Some(Type::Boolean),
                    (UnaryOp::Plus | UnaryOp::Minus, Some(ty)) if ty.is_numeric() => Some(ty),
                    (UnaryOp::Plus | UnaryOp::Minus, None) => None,
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
                        (Some(Type::Integer), Some(Type::Integer)) => Some(Type::Integer),
                        (Some(ty), Some(Type::Real)) | (Some(Type::Real), Some(ty))
                            if ty.is_numeric() =>
                        {
                            Some(Type::Real)
                        }
                        (None, _) => rhs.ty,
                        (_, None) => lhs.ty,
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
                        (Some(lhs_ty), Some(rhs_ty)) if lhs_ty != rhs_ty => {
                            return Err(PlannerError::TypeError)
                        }
                        _ => Some(Type::Boolean),
                    },
                    BinaryOp::Concat => match (lhs.ty, rhs.ty) {
                        (Some(Type::Text) | None, Some(Type::Text) | None) => Some(Type::Text),
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
                self.catalog.aggregate_function(&name)?;
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

                let subquery_plan = Binder::new(self.catalog).bind_select(*select)?;
                let [subquery_result_column] = subquery_plan
                    .schema
                    .0
                    .try_into()
                    .map_err(|_| PlannerError::MultipleColumnsFromSubquery)?;

                let subquery_node = PlanNode::Aggregate(planner::Aggregate {
                    source: Box::new(subquery_plan.node),
                    init_fn_ptrs: vec![init_agg],
                });

                let mut columns = source.schema.0.clone();
                columns.push(planner::Column {
                    table_name: None,
                    column_name,
                    ty: subquery_result_column.ty,
                });

                let plan = Plan {
                    node: PlanNode::CrossProduct(planner::CrossProduct {
                        left: Box::new(source.node),
                        right: Box::new(subquery_node),
                    }),
                    schema: columns.into(),
                };

                // CrossProduct appends the subquery result column to the right
                // of the source schema.
                let column_index = ColumnIndex(source.schema.0.len());

                Ok((
                    plan,
                    TypedExpression {
                        expr: planner::Expression::ColumnRef {
                            index: column_index,
                        },
                        ty: subquery_result_column.ty,
                    },
                ))
            }
        }
    }

    pub fn bind_without_source(&self, expr: parser::Expression) -> PlannerResult<TypedExpression> {
        self.bind(Plan::empty_source(), expr).map(|(_, e)| e)
    }

    fn resolve_aggregate(
        &self,
        name: String,
        args: Vec<parser::Expression>,
    ) -> PlannerResult<TypedExpression> {
        let Some(aggregate_ctx) = &self.aggregate_ctx else {
            return Err(PlannerError::AggregateNotAllowed);
        };
        let [arg] = args.try_into().map_err(|_| PlannerError::ArityError)?;
        let (index, column) = aggregate_ctx.resolve_aggregate(name, arg).unwrap();
        Ok(TypedExpression {
            expr: planner::Expression::ColumnRef { index },
            ty: column.ty,
        })
    }
}
