use super::ExecutorResult;
use crate::{
    parser::{BinaryOp, UnaryOp},
    planner::Expression,
    ExecutorError, Row, Value,
};

impl Expression {
    pub fn eval(&self, row: &Row) -> ExecutorResult<Value> {
        match self {
            Self::Constact(v) => Ok(v.clone()),
            Self::ColumnRef { index } => Ok(row[index].clone()),
            Self::Cast { expr, ty } => expr.eval(row)?.cast(*ty).ok_or(ExecutorError::TypeError),
            Self::UnaryOp { op, expr } => eval_unary_op(*op, row, expr),
            Self::BinaryOp { op, lhs, rhs } => eval_binary_op(*op, row, lhs, rhs),
            Self::Like {
                str_expr,
                pattern,
                case_insensitive,
            } => eval_like(row, str_expr, pattern, *case_insensitive),
        }
    }
}

fn eval_unary_op(op: UnaryOp, row: &Row, expr: &Expression) -> ExecutorResult<Value> {
    let expr = expr.eval(row)?;
    match (op, expr) {
        (UnaryOp::Plus, Value::Integer(v)) => Ok(Value::Integer(v)),
        (UnaryOp::Plus, Value::Real(v)) => Ok(Value::Real(v)),
        (UnaryOp::Minus, Value::Integer(v)) => Ok(Value::Integer(-v)),
        (UnaryOp::Minus, Value::Real(v)) => Ok(Value::Real(-v)),
        (UnaryOp::Not, Value::Boolean(v)) => Ok(Value::Boolean(!v)),
        _ => Err(ExecutorError::TypeError),
    }
}

fn eval_binary_op(
    op: BinaryOp,
    row: &Row,
    lhs: &Expression,
    rhs: &Expression,
) -> ExecutorResult<Value> {
    let lhs = lhs.eval(row)?;
    if lhs == Value::Null {
        return Ok(Value::Null);
    }
    match (op, &lhs) {
        (BinaryOp::And, Value::Boolean(false)) => return Ok(Value::Boolean(false)),
        (BinaryOp::Or, Value::Boolean(true)) => return Ok(Value::Boolean(true)),
        _ => (),
    }
    let rhs = rhs.eval(row)?;
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
    f(lhs, rhs).map_or_else(|| Err(ExecutorError::OutOfRange), |v| Ok(Value::Integer(v)))
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

fn eval_like(
    row: &Row,
    str_expr: &Expression,
    pattern: &Expression,
    case_insensitive: bool,
) -> ExecutorResult<Value> {
    let Value::Text(string) = str_expr.eval(row)? else {
        return Err(ExecutorError::TypeError);
    };
    let Value::Text(pattern) = pattern.eval(row)? else {
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
