use super::{ExecutionContext, ExecutorResult};
use crate::{
    catalog::{BoxedScalarFn, ScalarFunction, ScalarImpureFn},
    parser::{BinaryOp, UnaryOp},
    planner::{CaseBranch, ColumnId, Expression, PlanExpression},
    rows::ColumnIndex,
    ExecutorError, Row, Value,
};

pub trait EvalContext {
    type Column;

    fn column(column: &Self::Column, row: &Row) -> ExecutorResult<Value>;
    fn eval_impure_scalar_function(
        &self,
        eval: &ScalarImpureFn,
        args: &[Value],
    ) -> ExecutorResult<Value>;
}

impl EvalContext for ExecutionContext<'_> {
    type Column = ColumnIndex;

    fn column(column: &Self::Column, row: &Row) -> ExecutorResult<Value> {
        Ok(row[column].clone())
    }

    fn eval_impure_scalar_function(
        &self,
        eval: &ScalarImpureFn,
        args: &[Value],
    ) -> ExecutorResult<Value> {
        eval(self, args)
    }
}

struct ConstantFoldingContext;

// EvaluationError signals that the expression is not constant-foldable.
impl EvalContext for ConstantFoldingContext {
    type Column = ColumnId;

    fn column(_: &Self::Column, _: &Row) -> ExecutorResult<Value> {
        Err(ExecutorError::EvaluationError)
    }

    fn eval_impure_scalar_function(
        &self,
        _: &ScalarImpureFn,
        _: &[Value],
    ) -> ExecutorResult<Value> {
        Err(ExecutorError::EvaluationError)
    }
}

impl<C> Expression<'_, C> {
    pub fn eval<T>(&self, ctx: &T, row: &Row) -> ExecutorResult<Value>
    where
        T: EvalContext<Column = C>,
    {
        match self {
            Self::Constant(v) => Ok(v.clone()),
            Self::ColumnRef(c) => T::column(c, row),
            Self::Cast { expr, ty } => expr
                .eval(ctx, row)?
                .cast(*ty)
                .ok_or(ExecutorError::TypeError),
            Self::UnaryOp { op, expr } => op.eval(ctx, row, expr),
            Self::BinaryOp { op, lhs, rhs } => op.eval(ctx, row, lhs, rhs),
            Self::Case {
                branches,
                else_branch,
            } => eval_case(ctx, row, branches, else_branch.as_deref()),
            Self::Like {
                str_expr,
                pattern,
                case_insensitive,
            } => eval_like(ctx, row, str_expr, pattern, *case_insensitive),
            Self::Function { function, args } => eval_function(ctx, row, function, args),
        }
    }
}

impl Expression<'_, ColumnId> {
    pub fn eval_const(&self) -> ExecutorResult<Value> {
        self.eval(&ConstantFoldingContext, &Row::empty())
    }
}

impl UnaryOp {
    pub fn eval<T: EvalContext>(
        self,
        ctx: &T,
        row: &Row,
        expr: &Expression<T::Column>,
    ) -> ExecutorResult<Value> {
        let value = expr.eval(ctx, row)?;
        self.eval_value(&value)
    }

    pub fn eval_const(self, expr: &PlanExpression) -> ExecutorResult<Value> {
        self.eval(&ConstantFoldingContext, &Row::empty(), expr)
    }

    pub fn eval_value(self, value: &Value) -> ExecutorResult<Value> {
        match (self, value) {
            (Self::Plus, Value::Integer(v)) => Ok(Value::Integer(*v)),
            (Self::Plus, Value::Real(v)) => Ok(Value::Real(*v)),
            (Self::Minus, Value::Integer(v)) => Ok(Value::Integer(-v)),
            (Self::Minus, Value::Real(v)) => Ok(Value::Real(-v)),
            (Self::Not, Value::Boolean(v)) => Ok(Value::Boolean(!v)),
            _ => Err(ExecutorError::TypeError),
        }
    }
}

impl BinaryOp {
    pub fn eval<T: EvalContext>(
        self,
        ctx: &T,
        row: &Row,
        lhs: &Expression<T::Column>,
        rhs: &Expression<T::Column>,
    ) -> ExecutorResult<Value> {
        let lhs = lhs.eval(ctx, row)?;
        if lhs == Value::Null {
            return Ok(Value::Null);
        }
        match (self, &lhs) {
            (Self::And, Value::Boolean(false)) => return Ok(Value::Boolean(false)),
            (Self::Or, Value::Boolean(true)) => return Ok(Value::Boolean(true)),
            _ => (),
        }
        let rhs = rhs.eval(ctx, row)?;
        if rhs == Value::Null {
            return Ok(Value::Null);
        }
        self.eval_values(&lhs, &rhs)
    }

    pub fn eval_const(self, lhs: &PlanExpression, rhs: &PlanExpression) -> ExecutorResult<Value> {
        self.eval(&ConstantFoldingContext, &Row::empty(), lhs, rhs)
    }

    pub fn eval_values(self, lhs: &Value, rhs: &Value) -> ExecutorResult<Value> {
        if lhs.is_null() || rhs.is_null() {
            return Ok(Value::Null);
        }
        match self {
            Self::Add | Self::Sub | Self::Mul | Self::Div | Self::Mod => self.eval_number(lhs, rhs),
            Self::Eq => Ok(Value::Boolean(lhs == rhs)),
            Self::Ne => Ok(Value::Boolean(lhs != rhs)),
            Self::Lt => Ok(Value::Boolean(lhs < rhs)),
            Self::Le => Ok(Value::Boolean(lhs <= rhs)),
            Self::Gt => Ok(Value::Boolean(lhs > rhs)),
            Self::Ge => Ok(Value::Boolean(lhs >= rhs)),
            Self::And => {
                if let (Value::Boolean(lhs), Value::Boolean(rhs)) = (lhs, rhs) {
                    Ok(Value::Boolean(*lhs && *rhs))
                } else {
                    Err(ExecutorError::TypeError)
                }
            }
            Self::Or => {
                if let (Value::Boolean(lhs), Value::Boolean(rhs)) = (lhs, rhs) {
                    Ok(Value::Boolean(*lhs || *rhs))
                } else {
                    Err(ExecutorError::TypeError)
                }
            }
            Self::Concat => {
                if let (Value::Text(lhs), Value::Text(rhs)) = (lhs, rhs) {
                    Ok(Value::Text(lhs.to_owned() + rhs))
                } else {
                    Err(ExecutorError::TypeError)
                }
            }
        }
    }

    fn eval_number(self, lhs: &Value, rhs: &Value) -> ExecutorResult<Value> {
        match (lhs, rhs) {
            (Value::Integer(lhs), Value::Integer(rhs)) => self.eval_integer(*lhs, *rhs),
            (Value::Real(lhs), Value::Real(rhs)) => Ok(self.eval_real(*lhs, *rhs)),
            (Value::Real(lhs), Value::Integer(rhs)) => Ok(self.eval_real(*lhs, *rhs as f64)),
            (Value::Integer(lhs), Value::Real(rhs)) => Ok(self.eval_real(*lhs as f64, *rhs)),
            _ => Err(ExecutorError::TypeError),
        }
    }

    fn eval_integer(self, lhs: i64, rhs: i64) -> ExecutorResult<Value> {
        let f = match self {
            Self::Add => i64::checked_add,
            Self::Sub => i64::checked_sub,
            Self::Mul => i64::checked_mul,
            Self::Div => i64::checked_div,
            Self::Mod => i64::checked_rem,
            _ => unreachable!(),
        };
        f(lhs, rhs).map_or(Err(ExecutorError::OutOfRange), |v| Ok(Value::Integer(v)))
    }

    fn eval_real(self, lhs: f64, rhs: f64) -> Value {
        let f = match self {
            Self::Add => std::ops::Add::add,
            Self::Sub => std::ops::Sub::sub,
            Self::Mul => std::ops::Mul::mul,
            Self::Div => std::ops::Div::div,
            Self::Mod => std::ops::Rem::rem,
            _ => unreachable!(),
        };
        Value::Real(f(lhs, rhs))
    }
}

fn eval_case<T: EvalContext>(
    ctx: &T,
    row: &Row,
    branches: &[CaseBranch<T::Column>],
    else_branch: Option<&Expression<T::Column>>,
) -> ExecutorResult<Value> {
    for CaseBranch { condition, result } in branches {
        match condition.eval(ctx, row)? {
            Value::Null | Value::Boolean(false) => continue,
            Value::Boolean(true) => return result.eval(ctx, row),
            _ => return Err(ExecutorError::TypeError),
        }
    }
    else_branch.map_or(Ok(Value::Null), |else_branch| else_branch.eval(ctx, row))
}

fn eval_like<T: EvalContext>(
    ctx: &T,
    row: &Row,
    str_expr: &Expression<T::Column>,
    pattern: &Expression<T::Column>,
    case_insensitive: bool,
) -> ExecutorResult<Value> {
    let string = match str_expr.eval(ctx, row)? {
        Value::Null => return Ok(Value::Null),
        Value::Text(string) => string,
        _ => return Err(ExecutorError::TypeError),
    };
    let pattern = match pattern.eval(ctx, row)? {
        Value::Null => return Ok(Value::Null),
        Value::Text(pattern) => pattern,
        _ => return Err(ExecutorError::TypeError),
    };
    let mut regex = "^".to_owned();
    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            '\\' => {
                if let Some(ch) = chars.next() {
                    regex.push_str(&regex_lite::escape(&ch.to_string()));
                } else {
                    return Err(ExecutorError::InvalidLikePattern);
                }
            }
            ch => regex.push_str(&regex_lite::escape(&ch.to_string())),
        }
    }
    regex.push('$');
    let is_match = regex_lite::RegexBuilder::new(&regex)
        .case_insensitive(case_insensitive)
        .build()
        .map_err(|_| ExecutorError::InvalidLikePattern)?
        .is_match(&string);
    Ok(is_match.into())
}

fn eval_function<T: EvalContext>(
    ctx: &T,
    row: &Row,
    function: &ScalarFunction,
    args: &[Expression<T::Column>],
) -> ExecutorResult<Value> {
    let mut arg_values = Vec::with_capacity(args.len());
    for arg in args {
        match arg.eval(ctx, row) {
            Ok(Value::Null) => return Ok(Value::Null),
            Ok(value) => arg_values.push(value),
            Err(e) => return Err(e),
        }
    }
    match &function.eval {
        BoxedScalarFn::Pure(eval) => eval(&arg_values),
        BoxedScalarFn::Impure(eval) => ctx.eval_impure_scalar_function(eval, &arg_values),
    }
}
