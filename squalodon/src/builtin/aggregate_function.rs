use crate::{
    catalog::{AggregateFunction, Aggregator},
    executor::ExecutorResult,
    planner::PlannerResult,
    types::NullableType,
    PlannerError, Type, Value,
};
use std::ops::Add;

pub fn load() -> impl Iterator<Item = (&'static str, AggregateFunction)> {
    [
        (
            "avg",
            AggregateFunction {
                bind: |ty| match ty {
                    NullableType::Null => Ok(NullableType::Null),
                    NullableType::NonNull(ty) if ty.is_numeric() => Ok(Type::Real.into()),
                    NullableType::NonNull(_) => Err(PlannerError::TypeError),
                },
                init: || Box::<Average>::default(),
            },
        ),
        (
            "count",
            AggregateFunction {
                bind: |_| Ok(Type::Integer.into()),
                init: || Box::<Count>::default(),
            },
        ),
        (
            "max",
            AggregateFunction {
                bind: bind_numeric,
                init: || Box::<Max>::default(),
            },
        ),
        (
            "min",
            AggregateFunction {
                bind: bind_numeric,
                init: || Box::<Min>::default(),
            },
        ),
        (
            "sum",
            AggregateFunction {
                bind: bind_numeric,
                init: || Box::<Sum>::default(),
            },
        ),
    ]
    .into_iter()
}

fn bind_numeric(ty: NullableType) -> PlannerResult<NullableType> {
    match ty {
        NullableType::Null => Ok(NullableType::Null),
        NullableType::NonNull(ty) if ty.is_numeric() => Ok(NullableType::NonNull(ty)),
        NullableType::NonNull(_) => Err(PlannerError::TypeError),
    }
}

#[derive(Default)]
struct Average {
    sum: Number,
    count: usize,
}

impl Aggregator for Average {
    fn update(&mut self, value: &Value) -> ExecutorResult<()> {
        if !matches!(value, Value::Null) {
            self.sum += value;
            self.count += 1;
        }
        Ok(())
    }

    fn finish(&self) -> Value {
        self.sum
            .as_f64()
            .map_or(Value::Null, |sum| Value::Real(sum / self.count as f64))
    }
}

#[derive(Default)]
struct Count {
    count: usize,
}

impl Aggregator for Count {
    fn update(&mut self, value: &Value) -> ExecutorResult<()> {
        if !matches!(value, Value::Null) {
            self.count += 1;
        }
        Ok(())
    }

    fn finish(&self) -> Value {
        Value::Integer(self.count as i64)
    }
}

#[derive(Default)]
struct Max {
    max: Number,
}

impl Aggregator for Max {
    fn update(&mut self, value: &Value) -> ExecutorResult<()> {
        if !matches!(value, Value::Null) {
            self.max.update(value, i64::max, f64::max);
        }
        Ok(())
    }

    fn finish(&self) -> Value {
        self.max.clone().into()
    }
}

#[derive(Default)]
struct Min {
    min: Number,
}

impl Aggregator for Min {
    fn update(&mut self, value: &Value) -> ExecutorResult<()> {
        if !matches!(value, Value::Null) {
            self.min.update(value, i64::min, f64::min);
        }
        Ok(())
    }

    fn finish(&self) -> Value {
        self.min.clone().into()
    }
}

#[derive(Default)]
struct Sum {
    sum: Number,
}

impl Aggregator for Sum {
    fn update(&mut self, value: &Value) -> ExecutorResult<()> {
        if !matches!(value, Value::Null) {
            self.sum += value;
        }
        Ok(())
    }

    fn finish(&self) -> Value {
        self.sum.clone().into()
    }
}

/// A number whose type is lazily determined.
#[derive(Clone)]
enum Number {
    Null,
    Integer(i64),
    Real(f64),
}

impl Default for Number {
    fn default() -> Self {
        Self::Null
    }
}

impl From<Number> for Value {
    fn from(n: Number) -> Self {
        match n {
            Number::Null => Self::Null,
            Number::Integer(i) => Self::Integer(i),
            Number::Real(r) => Self::Real(r),
        }
    }
}

impl std::ops::AddAssign<&Value> for Number {
    fn add_assign(&mut self, rhs: &Value) {
        self.update(rhs, i64::add, f64::add);
    }
}

impl Number {
    fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Null => None,
            Self::Integer(i) => Some(*i as f64),
            Self::Real(r) => Some(*r),
        }
    }

    fn update<I, R>(&mut self, rhs: &Value, integer_f: I, real_f: R)
    where
        I: FnOnce(i64, i64) -> i64,
        R: FnOnce(f64, f64) -> f64,
    {
        match (self, rhs) {
            (s @ Self::Null, Value::Integer(x)) => *s = Self::Integer(*x),
            (s @ Self::Null, Value::Real(x)) => *s = Self::Real(*x),
            (Self::Integer(s), Value::Integer(x)) => *s = integer_f(*s, *x),
            (Self::Real(s), Value::Real(x)) => *s = real_f(*s, *x),
            _ => unreachable!("Unexpected non-numeric value or mixed types in a single column"),
        }
    }
}
