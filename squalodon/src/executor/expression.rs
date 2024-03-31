use super::{ExecutorContext, ExecutorResult};
use crate::{
    parser::{BinaryOp, UnaryOp},
    planner::{CaseBranch, Expression},
    storage::Storage,
    ExecutorError, Row, Value,
};

impl<T: Storage> Expression<T> {
    pub fn eval(&self, ctx: &ExecutorContext<T>, row: &Row) -> ExecutorResult<Value> {
        match self {
            Self::Constact(v) => Ok(v.clone()),
            Self::ColumnRef(index) => Ok(row[index].clone()),
            Self::Cast { expr, ty } => expr
                .eval(ctx, row)?
                .cast(*ty)
                .ok_or(ExecutorError::TypeError),
            Self::UnaryOp { op, expr } => eval_unary_op(ctx, *op, row, expr),
            Self::BinaryOp { op, lhs, rhs } => eval_binary_op(ctx, *op, row, lhs, rhs),
            Self::Case {
                branches,
                else_branch,
            } => eval_case(ctx, row, branches, else_branch.as_deref()),
            Self::Like {
                str_expr,
                pattern,
                case_insensitive,
            } => eval_like(ctx, row, str_expr, pattern, *case_insensitive),
            Self::Function { eval, args } => {
                let args: Vec<_> = args
                    .iter()
                    .map(|arg| arg.eval(ctx, row))
                    .collect::<ExecutorResult<_>>()?;
                eval(ctx, &args)
            }
        }
    }
}

fn eval_unary_op<T: Storage>(
    ctx: &ExecutorContext<T>,
    op: UnaryOp,
    row: &Row,
    expr: &Expression<T>,
) -> ExecutorResult<Value> {
    let expr = expr.eval(ctx, row)?;
    match (op, expr) {
        (UnaryOp::Plus, Value::Integer(v)) => Ok(Value::Integer(v)),
        (UnaryOp::Plus, Value::Real(v)) => Ok(Value::Real(v)),
        (UnaryOp::Minus, Value::Integer(v)) => Ok(Value::Integer(-v)),
        (UnaryOp::Minus, Value::Real(v)) => Ok(Value::Real(-v)),
        (UnaryOp::Not, Value::Boolean(v)) => Ok(Value::Boolean(!v)),
        _ => Err(ExecutorError::TypeError),
    }
}

fn eval_binary_op<T: Storage>(
    ctx: &ExecutorContext<T>,
    op: BinaryOp,
    row: &Row,
    lhs: &Expression<T>,
    rhs: &Expression<T>,
) -> ExecutorResult<Value> {
    let lhs = lhs.eval(ctx, row)?;
    if lhs == Value::Null {
        return Ok(Value::Null);
    }
    match (op, &lhs) {
        (BinaryOp::And, Value::Boolean(false)) => return Ok(Value::Boolean(false)),
        (BinaryOp::Or, Value::Boolean(true)) => return Ok(Value::Boolean(true)),
        _ => (),
    }
    let rhs = rhs.eval(ctx, row)?;
    if rhs == Value::Null {
        return Ok(Value::Null);
    }
    match op {
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
            eval_binary_op_number(op, lhs, rhs)
        }
        BinaryOp::Eq => Ok(Value::Boolean(lhs == rhs)),
        BinaryOp::Ne => Ok(Value::Boolean(lhs != rhs)),
        BinaryOp::Lt => Ok(Value::Boolean(lhs < rhs)),
        BinaryOp::Le => Ok(Value::Boolean(lhs <= rhs)),
        BinaryOp::Gt => Ok(Value::Boolean(lhs > rhs)),
        BinaryOp::Ge => Ok(Value::Boolean(lhs >= rhs)),
        BinaryOp::And => {
            if let (Value::Boolean(lhs), Value::Boolean(rhs)) = (lhs, rhs) {
                Ok(Value::Boolean(lhs && rhs))
            } else {
                Err(ExecutorError::TypeError)
            }
        }
        BinaryOp::Or => {
            if let (Value::Boolean(lhs), Value::Boolean(rhs)) = (lhs, rhs) {
                Ok(Value::Boolean(lhs || rhs))
            } else {
                Err(ExecutorError::TypeError)
            }
        }
        BinaryOp::Concat => {
            if let (Value::Text(lhs), Value::Text(rhs)) = (lhs, rhs) {
                Ok(Value::Text(lhs + &rhs))
            } else {
                Err(ExecutorError::TypeError)
            }
        }
    }
}

fn eval_binary_op_number(op: BinaryOp, lhs: Value, rhs: Value) -> ExecutorResult<Value> {
    match (lhs, rhs) {
        (Value::Integer(lhs), Value::Integer(rhs)) => eval_binary_op_integer(op, lhs, rhs),
        (Value::Real(lhs), Value::Real(rhs)) => Ok(eval_binary_op_real(op, lhs, rhs)),
        (Value::Real(lhs), Value::Integer(rhs)) => Ok(eval_binary_op_real(op, lhs, rhs as f64)),
        (Value::Integer(lhs), Value::Real(rhs)) => Ok(eval_binary_op_real(op, lhs as f64, rhs)),
        _ => Err(ExecutorError::TypeError),
    }
}

fn eval_binary_op_integer(op: BinaryOp, lhs: i64, rhs: i64) -> ExecutorResult<Value> {
    let f = match op {
        BinaryOp::Add => i64::checked_add,
        BinaryOp::Sub => i64::checked_sub,
        BinaryOp::Mul => i64::checked_mul,
        BinaryOp::Div => i64::checked_div,
        BinaryOp::Mod => i64::checked_rem,
        _ => unreachable!(),
    };
    f(lhs, rhs).map_or(Err(ExecutorError::OutOfRange), |v| Ok(Value::Integer(v)))
}

fn eval_binary_op_real(op: BinaryOp, lhs: f64, rhs: f64) -> Value {
    let f = match op {
        BinaryOp::Add => std::ops::Add::add,
        BinaryOp::Sub => std::ops::Sub::sub,
        BinaryOp::Mul => std::ops::Mul::mul,
        BinaryOp::Div => std::ops::Div::div,
        BinaryOp::Mod => std::ops::Rem::rem,
        _ => unreachable!(),
    };
    Value::Real(f(lhs, rhs))
}

fn eval_case<T: Storage>(
    ctx: &ExecutorContext<T>,
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
    ctx: &ExecutorContext<T>,
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
