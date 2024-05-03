use super::{
    aggregate::AggregatePlanner, column::ColumnMapView, Column, ColumnId, PlanNode, Planner,
    PlannerError, PlannerResult,
};
use crate::{
    catalog::{AggregateFunction, Aggregator, ScalarFunction},
    executor::{ExecutorError, ExecutorResult},
    parser::{self, BinaryOp, FunctionArgs, UnaryOp},
    planner::aggregate::ApplyAggregateOp,
    rows::ColumnIndex,
    types::{NullableType, Type},
    CatalogError, Value,
};
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    hash::Hash,
};

pub type PlanExpression<'a> = Expression<'a, ColumnId>;
pub type ExecutableExpression<'a> = Expression<'a, ColumnIndex>;
pub type ExpressionDisplay<'a, 'b> = Expression<'a, Cow<'b, str>>;

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Expression<'a, C> {
    Constant(Value),
    ColumnRef(C),
    Cast {
        expr: Box<Expression<'a, C>>,
        ty: Type,
    },
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expression<'a, C>>,
    },
    BinaryOp {
        op: BinaryOp,
        lhs: Box<Expression<'a, C>>,
        rhs: Box<Expression<'a, C>>,
    },
    Case {
        branches: Vec<CaseBranch<'a, C>>,
        else_branch: Option<Box<Expression<'a, C>>>,
    },
    Like {
        str_expr: Box<Expression<'a, C>>,
        pattern: Box<Expression<'a, C>>,
        case_insensitive: bool,
    },
    Function {
        function: &'a ScalarFunction,
        args: Vec<Expression<'a, C>>,
    },
}

impl std::fmt::Display for ExpressionDisplay<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Constant(value) => std::fmt::Debug::fmt(value, f),
            Self::ColumnRef(c) => c.fmt(f),
            Self::Cast { expr, ty } => write!(f, "CAST({expr} AS {ty})"),
            Self::UnaryOp { op, expr } => write!(f, "({op} {expr})"),
            Self::BinaryOp { op, lhs, rhs } => {
                write!(f, "({lhs} {op} {rhs})")
            }
            Self::Case {
                branches,
                else_branch,
            } => {
                f.write_str("CASE")?;
                for branch in branches {
                    write!(f, " WHEN {} THEN {}", branch.condition, branch.result)?;
                }
                if let Some(else_branch) = else_branch {
                    write!(f, " ELSE {else_branch}")?;
                }
                f.write_str(" END")
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
            Self::Function { function, args } => {
                write!(f, "{}(", function.name)?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    arg.fmt(f)?;
                }
                f.write_str(")")
            }
        }
    }
}

impl<'a> PlanExpression<'a> {
    pub fn cast(self, ty: Type) -> Self {
        if let Self::Constant(value) = &self {
            if let Some(value) = value.cast(ty) {
                return Self::Constant(value);
            }
        }
        Self::Cast {
            expr: Box::new(self),
            ty,
        }
    }

    fn unary_op(self, op: UnaryOp) -> Self {
        if let Ok(value) = op.eval_const(&self) {
            return Self::Constant(value);
        }
        match (op, self) {
            (UnaryOp::Plus, expr) => expr,
            (
                UnaryOp::Minus,
                Self::UnaryOp {
                    op: UnaryOp::Minus,
                    expr,
                },
            )
            | (
                UnaryOp::Not,
                Self::UnaryOp {
                    op: UnaryOp::Not,
                    expr,
                },
            ) => *expr,
            (
                UnaryOp::Not,
                Self::BinaryOp {
                    op: BinaryOp::Eq,
                    lhs,
                    rhs,
                },
            ) => lhs.binary_op(BinaryOp::Ne, *rhs),
            (
                UnaryOp::Not,
                Self::BinaryOp {
                    op: BinaryOp::Ne,
                    lhs,
                    rhs,
                },
            ) => lhs.binary_op(BinaryOp::Eq, *rhs),
            (
                UnaryOp::Not,
                Self::BinaryOp {
                    op: BinaryOp::Gt,
                    lhs,
                    rhs,
                },
            ) => lhs.binary_op(BinaryOp::Le, *rhs),
            (
                UnaryOp::Not,
                Self::BinaryOp {
                    op: BinaryOp::Ge,
                    lhs,
                    rhs,
                },
            ) => lhs.binary_op(BinaryOp::Lt, *rhs),
            (
                UnaryOp::Not,
                Self::BinaryOp {
                    op: BinaryOp::Lt,
                    lhs,
                    rhs,
                },
            ) => lhs.binary_op(BinaryOp::Ge, *rhs),
            (
                UnaryOp::Not,
                Self::BinaryOp {
                    op: BinaryOp::Le,
                    lhs,
                    rhs,
                },
            ) => lhs.binary_op(BinaryOp::Gt, *rhs),
            (op, expr) => Self::UnaryOp {
                op,
                expr: Box::new(expr),
            },
        }
    }

    fn binary_op(self, op: BinaryOp, other: Self) -> Self {
        if let Ok(value) = op.eval_const(&self, &other) {
            return Self::Constant(value);
        }

        // Make sure the constant is on the right hand side.
        match (&self, &other) {
            (Self::Constant(_), Self::Constant(_)) => (), // To avoid swapping constants forever
            (Self::Constant(_), _) => match op {
                BinaryOp::Add
                | BinaryOp::Mul
                | BinaryOp::Eq
                | BinaryOp::Ne
                | BinaryOp::And
                | BinaryOp::Or => {
                    return other.binary_op(op, self);
                }
                BinaryOp::Gt => return other.binary_op(BinaryOp::Lt, self),
                BinaryOp::Ge => return other.binary_op(BinaryOp::Le, self),
                BinaryOp::Lt => return other.binary_op(BinaryOp::Gt, self),
                BinaryOp::Le => return other.binary_op(BinaryOp::Ge, self),
                _ => (),
            },
            _ => (),
        }

        match (op, &self, &other) {
            (BinaryOp::Add | BinaryOp::Sub, _, Self::Constant(Value::Integer(0)))
            | (BinaryOp::Mul, _, Self::Constant(Value::Integer(1)))
            | (BinaryOp::And, _, Self::Constant(Value::Boolean(true)))
            | (BinaryOp::Or, _, Self::Constant(Value::Boolean(false))) => return self,
            (BinaryOp::Mul, _, Self::Constant(Value::Integer(0))) => {
                return Self::Constant(Value::Integer(0))
            }
            (BinaryOp::And, _, Self::Constant(Value::Boolean(false))) => {
                return Self::Constant(Value::Boolean(false))
            }
            (BinaryOp::Or, _, Self::Constant(Value::Boolean(true))) => {
                return Self::Constant(Value::Boolean(true))
            }
            _ => (),
        }
        Self::BinaryOp {
            op,
            lhs: Box::new(self),
            rhs: Box::new(other),
        }
    }

    pub fn into_typed(self, ty: impl Into<NullableType>) -> TypedExpression<'a> {
        TypedExpression {
            expr: self,
            ty: ty.into(),
        }
    }

    pub fn into_executable(self, columns: &[ColumnId]) -> ExecutableExpression<'a> {
        self.map_column_ref(|id| id.to_index(columns))
    }

    pub(super) fn display<'b>(&self, column_map: &'b ColumnMapView) -> ExpressionDisplay<'a, 'b> {
        self.clone().map_column_ref(|id| column_map[id].name())
    }

    fn map_column_ref<T, F>(self, f: F) -> Expression<'a, T>
    where
        F: FnOnce(ColumnId) -> T + Copy,
    {
        match self {
            Self::Constant(value) => Expression::Constant(value),
            Self::ColumnRef(id) => Expression::ColumnRef(f(id)),
            Self::Cast { expr, ty } => Expression::Cast {
                expr: Box::new(expr.map_column_ref(f)),
                ty,
            },
            Self::UnaryOp { op, expr } => Expression::UnaryOp {
                op,
                expr: Box::new(expr.map_column_ref(f)),
            },
            Self::BinaryOp { op, lhs, rhs } => Expression::BinaryOp {
                op,
                lhs: Box::new(lhs.map_column_ref(f)),
                rhs: Box::new(rhs.map_column_ref(f)),
            },
            Self::Case {
                branches,
                else_branch,
            } => {
                let branches = branches
                    .into_iter()
                    .map(|branch| CaseBranch {
                        condition: branch.condition.map_column_ref(f),
                        result: branch.result.map_column_ref(f),
                    })
                    .collect();
                let else_branch =
                    else_branch.map(|else_branch| Box::new(else_branch.map_column_ref(f)));
                Expression::Case {
                    branches,
                    else_branch,
                }
            }
            Self::Like {
                str_expr,
                pattern,
                case_insensitive,
            } => Expression::Like {
                str_expr: Box::new(str_expr.map_column_ref(f)),
                pattern: Box::new(pattern.map_column_ref(f)),
                case_insensitive,
            },
            Self::Function { function, args } => {
                let args = args.into_iter().map(|arg| arg.map_column_ref(f)).collect();
                Expression::Function { function, args }
            }
        }
    }

    pub(super) fn referenced_columns(&self) -> HashSet<ColumnId> {
        let mut free = HashSet::new();
        self.insert_referenced_columns(&mut free);
        free
    }

    fn insert_referenced_columns(&self, columns: &mut HashSet<ColumnId>) {
        match self {
            Self::Constant(_) => (),
            Self::ColumnRef(id) => {
                columns.insert(*id);
            }
            Self::Cast { expr, .. } | Self::UnaryOp { expr, .. } => {
                expr.insert_referenced_columns(columns);
            }
            Self::BinaryOp { lhs, rhs, .. } => {
                lhs.insert_referenced_columns(columns);
                rhs.insert_referenced_columns(columns);
            }
            Self::Case {
                branches,
                else_branch,
            } => {
                for branch in branches {
                    branch.condition.insert_referenced_columns(columns);
                    branch.result.insert_referenced_columns(columns);
                }
                if let Some(else_branch) = else_branch {
                    else_branch.insert_referenced_columns(columns);
                }
            }
            Self::Like {
                str_expr, pattern, ..
            } => {
                str_expr.insert_referenced_columns(columns);
                pattern.insert_referenced_columns(columns);
            }
            Self::Function { args, .. } => {
                for arg in args {
                    arg.insert_referenced_columns(columns);
                }
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CaseBranch<'a, C> {
    pub condition: Expression<'a, C>,
    pub result: Expression<'a, C>,
}

#[derive(Clone)]
pub struct TypedExpression<'a> {
    pub expr: PlanExpression<'a>,
    pub ty: NullableType,
}

impl From<Value> for TypedExpression<'_> {
    fn from(value: Value) -> Self {
        let ty = value.ty();
        Self {
            expr: PlanExpression::Constant(value),
            ty,
        }
    }
}

impl<'a> TypedExpression<'a> {
    pub fn expect_type<T: Into<NullableType>>(
        self,
        expected: T,
    ) -> PlannerResult<PlanExpression<'a>> {
        if self.ty.is_compatible_with(expected.into()) {
            Ok(self.expr)
        } else {
            Err(PlannerError::TypeError)
        }
    }

    fn unary_op(self, op: UnaryOp) -> PlannerResult<Self> {
        let ty = match (op, self.ty) {
            (_, NullableType::Null) => return Ok(Value::Null.into()),
            (UnaryOp::Not, _) => NullableType::NonNull(Type::Boolean),
            (UnaryOp::Plus | UnaryOp::Minus, NullableType::NonNull(ty)) if ty.is_numeric() => {
                NullableType::NonNull(ty)
            }
            _ => return Err(PlannerError::TypeError),
        };
        Ok(self.expr.unary_op(op).into_typed(ty))
    }

    fn binary_op(self, op: BinaryOp, other: Self) -> PlannerResult<Self> {
        let ty = match op {
            BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
                match (self.ty, other.ty) {
                    (NullableType::Null, _) | (_, NullableType::Null) => {
                        return Ok(Value::Null.into())
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
                }
            }
            BinaryOp::Eq
            | BinaryOp::Ne
            | BinaryOp::Lt
            | BinaryOp::Le
            | BinaryOp::Gt
            | BinaryOp::Ge
            | BinaryOp::And
            | BinaryOp::Or => match (self.ty, other.ty) {
                (NullableType::Null, _) | (_, NullableType::Null) => NullableType::Null,
                (NullableType::NonNull(lhs_ty), NullableType::NonNull(rhs_ty))
                    if lhs_ty == rhs_ty =>
                {
                    NullableType::NonNull(Type::Boolean)
                }
                _ => return Err(PlannerError::TypeError),
            },
            BinaryOp::Concat => match (self.ty, other.ty) {
                (NullableType::Null, _) | (_, NullableType::Null) => return Ok(Value::Null.into()),
                (NullableType::NonNull(Type::Text), NullableType::NonNull(Type::Text)) => {
                    NullableType::NonNull(Type::Text)
                }
                _ => return Err(PlannerError::TypeError),
            },
        };
        Ok(self.expr.binary_op(op, other.expr).into_typed(ty))
    }

    pub fn eq(self, other: Self) -> PlannerResult<Self> {
        self.binary_op(BinaryOp::Eq, other)
    }

    pub fn and(self, other: Self) -> PlannerResult<Self> {
        self.binary_op(BinaryOp::And, other)
    }
}

pub struct ExpressionBinder<'a, 'b> {
    planner: &'a Planner<'b>,
    aliases: Option<&'a HashMap<String, TypedExpression<'b>>>,
    aggregates: Option<&'a AggregatePlanner<'a, 'b>>,
}

impl<'a, 'b> ExpressionBinder<'a, 'b> {
    pub fn new(planner: &'a Planner<'b>) -> Self {
        Self {
            planner,
            aliases: None,
            aggregates: None,
        }
    }

    pub fn with_aliases(&self, aliases: &'a HashMap<String, TypedExpression<'b>>) -> Self {
        Self {
            aliases: Some(aliases),
            ..*self
        }
    }

    pub fn with_aggregates(&self, aggregates: &'a AggregatePlanner<'a, 'b>) -> Self {
        Self {
            aggregates: Some(aggregates),
            ..*self
        }
    }
}

impl<'a, 'b> ExpressionBinder<'a, 'b> {
    pub fn bind(
        &self,
        source: PlanNode<'b>,
        expr: parser::Expression,
    ) -> PlannerResult<(PlanNode<'b>, TypedExpression<'b>)> {
        if let Some(column_id) = self
            .aggregates
            .and_then(|aggregates| aggregates.resolve_group_by(&expr))
        {
            let expr = PlanExpression::ColumnRef(column_id)
                .into_typed(self.planner.column_map()[column_id].ty);
            return Ok((source, expr));
        }

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
                let expr = source.resolve_column(&self.planner.column_map(), &column_ref)?;
                Ok((source, expr))
            }
            parser::Expression::Cast { expr, ty } => {
                let (plan, TypedExpression { expr, .. }) = self.bind(source, *expr)?;
                let expr = expr.cast(ty);
                Ok((plan, expr.into_typed(ty)))
            }
            parser::Expression::UnaryOp { op, expr } => {
                let (plan, expr) = self.bind(source, *expr)?;
                Ok((plan, expr.unary_op(op)?))
            }
            parser::Expression::BinaryOp { op, lhs, rhs } => {
                let (plan, lhs) = self.bind(source, *lhs)?;
                let (plan, rhs) = self.bind(plan, *rhs)?;
                Ok((plan, lhs.binary_op(op, rhs)?))
            }
            parser::Expression::Case {
                branches,
                else_branch,
            } => {
                let mut plan = source;
                let mut branch_exprs = Vec::with_capacity(branches.len());
                let mut result_type = NullableType::Null;
                for branch in branches {
                    let (
                        new_plan,
                        TypedExpression {
                            expr: condition,
                            ty,
                        },
                    ) = self.bind(plan, branch.condition)?;
                    plan = new_plan;
                    if ty.is_null() {
                        // This branch never matches.
                        continue;
                    }
                    if !matches!(ty, NullableType::NonNull(Type::Boolean)) {
                        return Err(PlannerError::TypeError);
                    }
                    let (new_plan, TypedExpression { expr: result, ty }) =
                        self.bind(plan, branch.result)?;
                    plan = new_plan;
                    if !ty.is_compatible_with(result_type) {
                        return Err(PlannerError::TypeError);
                    }
                    result_type = ty;
                    branch_exprs.push(CaseBranch { condition, result });
                }
                let else_branch = match else_branch {
                    Some(else_branch) => {
                        let (new_plan, TypedExpression { expr, ty }) =
                            self.bind(plan, *else_branch)?;
                        plan = new_plan;
                        if !ty.is_compatible_with(result_type) {
                            return Err(PlannerError::TypeError);
                        }
                        result_type = ty;
                        Some(Box::new(expr))
                    }
                    None => None,
                };
                Ok((
                    plan,
                    PlanExpression::Case {
                        branches: branch_exprs,
                        else_branch,
                    }
                    .into_typed(result_type),
                ))
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
                if expr_ty.is_null() || pattern_ty.is_null() {
                    return Ok((plan, Value::Null.into()));
                }
                if !matches!(expr_ty, NullableType::NonNull(Type::Text))
                    || !matches!(pattern_ty, NullableType::NonNull(Type::Text))
                {
                    return Err(PlannerError::TypeError);
                }
                let expr = PlanExpression::Like {
                    str_expr: str_expr.into(),
                    pattern: pattern.into(),
                    case_insensitive,
                };
                Ok((plan, expr.into_typed(Type::Boolean)))
            }
            parser::Expression::Function(function_call) => {
                match self.planner.catalog.aggregate_function(&function_call.name) {
                    Ok(_) => {
                        let Some(aggregates) = &self.aggregates else {
                            return Err(PlannerError::AggregateNotAllowed);
                        };
                        let id = aggregates
                            .resolve_aggregate_function(&function_call)
                            .ok_or_else(|| PlannerError::UnknownColumn("(aggregate)".to_owned()))?;
                        let ty = self.planner.column_map()[id].ty;
                        let expr = PlanExpression::ColumnRef(id).into_typed(ty);
                        return Ok((source, expr));
                    }
                    Err(CatalogError::UnknownEntry(_, _)) => (),
                    Err(e) => return Err(e.into()),
                }

                let function = self.planner.catalog.scalar_function(&function_call.name)?;
                if function_call.is_distinct {
                    return Err(PlannerError::InvalidArgument);
                }
                let args = match function_call.args {
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
                let expr = PlanExpression::Function {
                    function,
                    args: arg_exprs,
                };
                Ok((plan, expr.into_typed(ty)))
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

                static ASSERT_SINGLE_ROW: AggregateFunction =
                    AggregateFunction::new_internal::<AssertSingleRow>();

                let column_name = query.to_string();
                let subquery = self.planner.plan_query(*query)?;

                let [input] = subquery
                    .outputs()
                    .try_into()
                    .map_err(|_| PlannerError::MultipleColumnsFromSubquery)?;

                // Equivalent to `SELECT assert_single_row(subquery)`
                let mut column_map = self.planner.column_map();
                let subquery_type = column_map[input].ty;
                let output = column_map.insert(Column::new(column_name, subquery_type));
                let subquery = subquery.ungrouped_aggregate(vec![ApplyAggregateOp {
                    function: &ASSERT_SINGLE_ROW,
                    is_distinct: false,
                    input,
                    output,
                }]);

                let plan = source.cross_product(subquery);
                let expr = PlanExpression::ColumnRef(output).into_typed(subquery_type);
                Ok((plan, expr))
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

                static EXISTS: AggregateFunction = AggregateFunction::new_internal::<Exists>();

                let column_name = format!("EXISTS ({query})");
                let subquery = self.planner.plan_query(*query)?;

                // Equivalent to `SELECT exists(SELECT first_column FROM subquery LIMIT 1)`
                let subquery = subquery.limit(
                    &mut self.planner.column_map(),
                    Some(Value::from(1).into()),
                    None,
                )?;
                let input = *subquery.outputs().first().unwrap();
                let output = self
                    .planner
                    .column_map()
                    .insert(Column::new(column_name, Type::Boolean));
                let subquery = subquery.ungrouped_aggregate(vec![ApplyAggregateOp {
                    function: &EXISTS,
                    is_distinct: false,
                    input,
                    output,
                }]);

                let plan = source.cross_product(subquery);
                let expr = PlanExpression::ColumnRef(output).into_typed(Type::Boolean);
                Ok((plan, expr))
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
    ) -> PlannerResult<TypedExpression<'b>> {
        self.bind(PlanNode::new_empty_row(), expr)
            .map(|(_, expr)| expr)
    }
}
