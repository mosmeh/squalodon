use super::ExecutorResult;
use crate::{
    connection::ConnectionContext,
    parser::{BinaryOp, UnaryOp},
    planner::{CaseBranch, Expression},
    storage::Storage,
    ExecutorError, Row, Value,
};

impl<T: Storage> Expression<'_, T> {
    pub fn eval(&self, ctx: &ConnectionContext<T>, row: &Row) -> ExecutorResult<Value> {
        match self {
            Self::Constant(v) => Ok(v.clone()),
            Self::ColumnRef(index) => row.0.get(index.0).cloned().ok_or(ExecutorError::OutOfRange),
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
            Self::Function { function, args } => {
                let args: Vec<_> = args
                    .iter()
                    .map(|arg| arg.eval(ctx, row))
                    .collect::<ExecutorResult<_>>()?;
                (function.eval)(ctx, &args)
            }
        }
    }
}

impl UnaryOp {
    pub fn eval<T: Storage>(
        self,
        ctx: &ConnectionContext<T>,
        row: &Row,
        expr: &Expression<T>,
    ) -> ExecutorResult<Value> {
        let expr = expr.eval(ctx, row)?;
        match (self, expr) {
            (Self::Plus, Value::Integer(v)) => Ok(Value::Integer(v)),
            (Self::Plus, Value::Real(v)) => Ok(Value::Real(v)),
            (Self::Minus, Value::Integer(v)) => Ok(Value::Integer(-v)),
            (Self::Minus, Value::Real(v)) => Ok(Value::Real(-v)),
            (Self::Not, Value::Boolean(v)) => Ok(Value::Boolean(!v)),
            _ => Err(ExecutorError::TypeError),
        }
    }
}

impl BinaryOp {
    pub fn eval<T: Storage>(
        self,
        ctx: &ConnectionContext<T>,
        row: &Row,
        lhs: &Expression<T>,
        rhs: &Expression<T>,
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
                    Ok(Value::Boolean(lhs && rhs))
                } else {
                    Err(ExecutorError::TypeError)
                }
            }
            Self::Or => {
                if let (Value::Boolean(lhs), Value::Boolean(rhs)) = (lhs, rhs) {
                    Ok(Value::Boolean(lhs || rhs))
                } else {
                    Err(ExecutorError::TypeError)
                }
            }
            Self::Concat => {
                if let (Value::Text(lhs), Value::Text(rhs)) = (lhs, rhs) {
                    Ok(Value::Text(lhs + &rhs))
                } else {
                    Err(ExecutorError::TypeError)
                }
            }
        }
    }

    fn eval_number(self, lhs: Value, rhs: Value) -> ExecutorResult<Value> {
        match (lhs, rhs) {
            (Value::Integer(lhs), Value::Integer(rhs)) => self.eval_integer(lhs, rhs),
            (Value::Real(lhs), Value::Real(rhs)) => Ok(self.eval_real(lhs, rhs)),
            (Value::Real(lhs), Value::Integer(rhs)) => Ok(self.eval_real(lhs, rhs as f64)),
            (Value::Integer(lhs), Value::Real(rhs)) => Ok(self.eval_real(lhs as f64, rhs)),
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

fn eval_case<T: Storage>(
    ctx: &ConnectionContext<T>,
    row: &Row,
    branches: &[CaseBranch<T>],
    else_branch: Option<&Expression<T>>,
) -> ExecutorResult<Value> {
    for CaseBranch { condition, result } in branches {
        if condition.eval(ctx, row)? == Value::Boolean(true) {
            return result.eval(ctx, row);
        }
    }
    else_branch.map_or(Ok(Value::Null), |else_branch| else_branch.eval(ctx, row))
}

fn eval_like<T: Storage>(
    ctx: &ConnectionContext<T>,
    row: &Row,
    str_expr: &Expression<T>,
    pattern: &Expression<T>,
    case_insensitive: bool,
) -> ExecutorResult<Value> {
    let Value::Text(string) = str_expr.eval(ctx, row)? else {
        return Err(ExecutorError::TypeError);
    };
    let Value::Text(pattern) = pattern.eval(ctx, row)? else {
        return Err(ExecutorError::TypeError);
    };
    let mut regex = String::new();
    for ch in pattern.chars() {
        match ch {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            ch => regex.push_str(&regex_lite::escape(&ch.to_string())),
        }
    }
    let is_match = regex_lite::RegexBuilder::new(&regex)
        .case_insensitive(case_insensitive)
        .build()
        .map_err(|_| ExecutorError::InvalidLikePattern)?
        .is_match(&string);
    Ok(is_match.into())
}
