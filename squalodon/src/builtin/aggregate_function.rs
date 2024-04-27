use crate::{
    catalog::{AggregateFunction, Aggregator},
    executor::ExecutorResult,
    types::NullableType,
    ExecutorError, PlannerError, Type, Value,
};
use std::ops::Add;

pub fn load() -> impl Iterator<Item = AggregateFunction> {
    [
        AggregateFunction {
            name: "avg",
            bind: |ty| match ty {
                NullableType::Null => Ok(NullableType::Null),
                NullableType::NonNull(ty) if ty.is_numeric() => Ok(Type::Real.into()),
                NullableType::NonNull(_) => Err(PlannerError::TypeError),
            },
            init: || Box::<Average>::default(),
        },
        AggregateFunction {
            name: "count",
            bind: |_| Ok(Type::Integer.into()),
            init: || Box::<Count>::default(),
        },
        AggregateFunction {
            name: "max",
            bind: Ok,
            init: || Box::<Max>::default(),
        },
        AggregateFunction {
            name: "min",
            bind: Ok,
            init: || Box::<Min>::default(),
        },
        AggregateFunction {
            name: "sum",
            bind: |ty| match ty {
                NullableType::Null => Ok(NullableType::Null),
                NullableType::NonNull(ty) if ty.is_numeric() => Ok(NullableType::NonNull(ty)),
                NullableType::NonNull(_) => Err(PlannerError::TypeError),
            },
            init: || Box::<Sum>::default(),
        },
    ]
    .into_iter()
}

#[derive(Default)]
struct Average {
    sum: Number,
    count: usize,
}

impl Aggregator for Average {
    fn update(&mut self, value: &Value) -> ExecutorResult<()> {
        if !value.is_null() {
            self.sum.checked_update(value, i64::checked_add, f64::add)?;
            self.count = self.count.checked_add(1).ok_or(ExecutorError::OutOfRange)?;
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
    count: i64,
}

impl Aggregator for Count {
    fn update(&mut self, value: &Value) -> ExecutorResult<()> {
        if !value.is_null() {
            self.count = self.count.checked_add(1).ok_or(ExecutorError::OutOfRange)?;
        }
        Ok(())
    }

    fn finish(&self) -> Value {
        Value::Integer(self.count)
    }
}

struct Max {
    max: Value,
}

impl Default for Max {
    fn default() -> Self {
        Self { max: Value::Null }
    }
}

impl Aggregator for Max {
    fn update(&mut self, value: &Value) -> ExecutorResult<()> {
        if !value.is_null() && (self.max.is_null() || value > &self.max) {
            self.max = value.clone();
        }
        Ok(())
    }

    fn finish(&self) -> Value {
        self.max.clone()
    }
}

struct Min {
    min: Value,
}

impl Default for Min {
    fn default() -> Self {
        Self { min: Value::Null }
    }
}

impl Aggregator for Min {
    fn update(&mut self, value: &Value) -> ExecutorResult<()> {
        if !value.is_null() && (self.min.is_null() || value < &self.min) {
            self.min = value.clone();
        }
        Ok(())
    }

    fn finish(&self) -> Value {
        self.min.clone()
    }
}

#[derive(Default)]
struct Sum {
    sum: Number,
}

impl Aggregator for Sum {
    fn update(&mut self, value: &Value) -> ExecutorResult<()> {
        if !value.is_null() {
            self.sum.checked_update(value, i64::checked_add, f64::add)?;
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

impl Number {
    fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Null => None,
            Self::Integer(i) => Some(*i as f64),
            Self::Real(r) => Some(*r),
        }
    }

    fn checked_update<I, R>(&mut self, rhs: &Value, integer_f: I, real_f: R) -> ExecutorResult<()>
    where
        I: FnOnce(i64, i64) -> Option<i64>,
        R: FnOnce(f64, f64) -> f64,
    {
        match (self, rhs) {
            (s @ Self::Null, Value::Integer(x)) => *s = Self::Integer(*x),
            (s @ Self::Null, Value::Real(x)) => *s = Self::Real(*x),
            (Self::Integer(s), Value::Integer(x)) => {
                *s = integer_f(*s, *x).ok_or(ExecutorError::OutOfRange)?;
            }
            (Self::Real(s), Value::Real(x)) => *s = real_f(*s, *x),
            _ => unreachable!("Unexpected non-numeric value or mixed types in a single column"),
        }
        Ok(())
    }
}
