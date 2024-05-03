use crate::{
    catalog::{AggregateFunction, Aggregator, TypedAggregator},
    executor::ExecutorResult,
    ExecutorError, Type, Value,
};

pub fn load() -> impl Iterator<Item = AggregateFunction> {
    Type::ALL
        .iter()
        .flat_map(|&ty| {
            [
                AggregateFunction {
                    name: "count",
                    input_type: ty,
                    output_type: Type::Integer,
                    init: || Box::<Count>::default(),
                },
                AggregateFunction {
                    name: "max",
                    input_type: ty,
                    output_type: ty,
                    init: || Box::<Max>::default(),
                },
                AggregateFunction {
                    name: "min",
                    input_type: ty,
                    output_type: ty,
                    init: || Box::<Min>::default(),
                },
            ]
            .into_iter()
        })
        .chain([
            AggregateFunction {
                name: "avg",
                input_type: Type::Integer,
                output_type: Type::Real,
                init: || Box::<Average<i64>>::default(),
            },
            AggregateFunction {
                name: "avg",
                input_type: Type::Real,
                output_type: Type::Real,
                init: || Box::<Average<f64>>::default(),
            },
            AggregateFunction {
                name: "sum",
                input_type: Type::Integer,
                output_type: Type::Integer,
                init: || Box::<Sum<i64>>::default(),
            },
            AggregateFunction {
                name: "sum",
                input_type: Type::Real,
                output_type: Type::Real,
                init: || Box::<Sum<f64>>::default(),
            },
        ])
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
struct Average<T> {
    sum: T,
    count: usize,
}

impl TypedAggregator for Average<i64> {
    type Input = i64;
    type Output = Option<f64>;

    fn update(&mut self, value: i64) -> ExecutorResult<()> {
        self.sum = self
            .sum
            .checked_add(value)
            .ok_or(ExecutorError::OutOfRange)?;
        self.count = self.count.checked_add(1).ok_or(ExecutorError::OutOfRange)?;
        Ok(())
    }

    fn finish(&self) -> Option<f64> {
        (self.count > 0).then(|| self.sum as f64 / self.count as f64)
    }
}

impl TypedAggregator for Average<f64> {
    type Input = f64;
    type Output = Option<f64>;

    fn update(&mut self, value: f64) -> ExecutorResult<()> {
        self.sum += value;
        self.count = self.count.checked_add(1).ok_or(ExecutorError::OutOfRange)?;
        Ok(())
    }

    fn finish(&self) -> Option<f64> {
        (self.count > 0).then(|| self.sum / self.count as f64)
    }
}

#[derive(Default)]
struct Sum<T> {
    sum: Option<T>,
}

impl TypedAggregator for Sum<i64> {
    type Input = i64;
    type Output = Option<i64>;

    fn update(&mut self, value: i64) -> ExecutorResult<()> {
        match &mut self.sum {
            Some(sum) => *sum = sum.checked_add(value).ok_or(ExecutorError::OutOfRange)?,
            None => self.sum = Some(value),
        }
        Ok(())
    }

    fn finish(&self) -> Option<i64> {
        self.sum
    }
}

impl TypedAggregator for Sum<f64> {
    type Input = f64;
    type Output = Option<f64>;

    fn update(&mut self, value: f64) -> ExecutorResult<()> {
        match &mut self.sum {
            Some(sum) => *sum += value,
            None => self.sum = Some(value),
        }
        Ok(())
    }

    fn finish(&self) -> Option<f64> {
        self.sum
    }
}
