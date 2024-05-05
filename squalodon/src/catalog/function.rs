use super::{CatalogEntryKind, CatalogError, CatalogRef, CatalogResult};
use crate::{
    executor::{ExecutionContext, ExecutorResult},
    planner,
    types::{Args, NullableType, Type},
    Row, TryFromValueError, Value,
};
use std::ops::Deref;

pub enum Function<'a> {
    Scalar(&'a ScalarFunction),
    Aggregate(&'a AggregateFunction),
    Table(&'a TableFunction),
}

impl<'a> Function<'a> {
    pub fn name(&self) -> &str {
        match self {
            Self::Scalar(f) => f.name,
            Self::Aggregate(f) => f.name,
            Self::Table(f) => f.name,
        }
    }
}

pub struct ScalarFunction {
    pub name: &'static str,
    pub argument_types: &'static [Type],
    pub return_type: NullableType,
    pub eval: BoxedScalarFn,
}

impl PartialEq for ScalarFunction {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.argument_types == other.argument_types
    }
}

impl Eq for ScalarFunction {}

impl std::hash::Hash for ScalarFunction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.argument_types.hash(state);
    }
}

pub type ScalarPureFn = dyn Fn(&[Value]) -> ExecutorResult<Value> + Send + Sync;
pub type ScalarImpureFn =
    dyn Fn(&ExecutionContext, &[Value]) -> ExecutorResult<Value> + Send + Sync;

pub enum BoxedScalarFn {
    Pure(Box<ScalarPureFn>),
    Impure(Box<ScalarImpureFn>),
}

impl BoxedScalarFn {
    pub fn pure<F, A, R>(f: F) -> Self
    where
        F: Fn(A) -> ExecutorResult<R> + Send + Sync + 'static,
        A: Args,
        R: Into<Value>,
    {
        Self::Pure(Box::new(move |args| {
            let args = A::from_values(args).expect("Declared and actual argument types must match");
            f(args).map(Into::into)
        }))
    }

    pub fn impure<F, A, R>(f: F) -> Self
    where
        F: Fn(&ExecutionContext, A) -> ExecutorResult<R> + Send + Sync + 'static,
        A: Args,
        R: Into<Value>,
    {
        Self::Impure(Box::new(move |ctx, args| {
            let args = A::from_values(args).expect("Declared and actual argument types must match");
            f(ctx, args).map(Into::into)
        }))
    }
}

pub struct AggregateFunction {
    pub name: &'static str,
    pub input_type: Type,
    pub output_type: Type,
    pub init: AggregateInitFnPtr,
}

impl AggregateFunction {
    /// Creates an aggregate function that is not exposed to the user.
    pub const fn new_internal<T: Aggregator + Default + 'static>(name: &'static str) -> Self {
        Self {
            name,
            input_type: Type::Text,  // dummy
            output_type: Type::Text, // dummy
            init: || Box::<T>::default(),
        }
    }
}

pub type AggregateInitFnPtr = fn() -> Box<dyn Aggregator>;

pub trait Aggregator {
    fn update(&mut self, value: &Value) -> ExecutorResult<()>;
    fn finish(&self) -> Value;
}

pub trait TypedAggregator {
    type Input: TryFrom<Value, Error = TryFromValueError>;
    type Output: Into<Value>;

    fn update(&mut self, value: Self::Input) -> ExecutorResult<()>;
    fn finish(&self) -> Self::Output;
}

impl<T: TypedAggregator> Aggregator for T {
    fn update(&mut self, value: &Value) -> ExecutorResult<()> {
        if !value.is_null() {
            let value = value
                .clone()
                .try_into()
                .expect("Declared and actual input types must match");
            TypedAggregator::update(self, value)?;
        }
        Ok(())
    }

    fn finish(&self) -> Value {
        TypedAggregator::finish(self).into()
    }
}

pub struct TableFunction {
    pub name: &'static str,
    pub argument_types: &'static [Type],
    pub result_columns: Vec<planner::Column>,
    pub eval: BoxedTableFn,
}

pub type TableFn = dyn Fn(&ExecutionContext, &[Value]) -> ExecutorResult<Box<dyn Iterator<Item = Row>>>
    + Send
    + Sync;

pub struct BoxedTableFn(Box<TableFn>);

impl Deref for BoxedTableFn {
    type Target = TableFn;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl BoxedTableFn {
    pub fn new<F, A, R>(f: F) -> Self
    where
        F: Fn(&ExecutionContext, A) -> ExecutorResult<R> + Send + Sync + 'static,
        A: Args,
        R: Iterator<Item = Row> + 'static,
    {
        Self(Box::new(move |ctx, args| {
            let args = A::from_values(args).expect("Declared and actual argument types must match");
            Ok(Box::new(f(ctx, args)?))
        }))
    }
}

impl CatalogError {
    fn unknown_function(
        kind: CatalogEntryKind,
        name: String,
        argument_types: &[NullableType],
    ) -> Self {
        let args = argument_types
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        Self::UnknownEntry(kind, format!("{name}({args})"))
    }
}

impl<'a> CatalogRef<'a> {
    pub fn functions(&self) -> impl Iterator<Item = Function> + '_ {
        let scalar = self
            .catalog
            .scalar_functions
            .values()
            .flatten()
            .map(Function::Scalar);
        let aggregate = self
            .catalog
            .aggregate_functions
            .values()
            .flatten()
            .map(Function::Aggregate);
        let table = self
            .catalog
            .table_functions
            .values()
            .flatten()
            .map(Function::Table);
        scalar.chain(aggregate).chain(table)
    }

    pub fn scalar_function(
        &self,
        name: &str,
        argument_types: &[NullableType],
    ) -> CatalogResult<&'a ScalarFunction> {
        self.catalog
            .scalar_functions
            .get(name)
            .and_then(|functions| {
                functions.iter().find(|f| {
                    if f.argument_types.len() != argument_types.len() {
                        return false;
                    }
                    argument_types
                        .iter()
                        .zip(f.argument_types)
                        .all(|(a, b)| a.is_compatible_with(*b))
                })
            })
            .ok_or_else(|| {
                CatalogError::unknown_function(
                    CatalogEntryKind::ScalarFunction,
                    name.to_owned(),
                    argument_types,
                )
            })
    }

    pub fn aggregate_function(
        &self,
        name: &str,
        input_type: NullableType,
    ) -> CatalogResult<&'a AggregateFunction> {
        self.catalog
            .aggregate_functions
            .get(name)
            .and_then(|functions| {
                functions
                    .iter()
                    .find(|f| input_type.is_compatible_with(f.input_type))
            })
            .ok_or_else(|| {
                CatalogError::unknown_function(
                    CatalogEntryKind::AggregateFunction,
                    name.to_owned(),
                    &[input_type],
                )
            })
    }

    pub fn table_function(
        &self,
        name: &str,
        argument_types: &[NullableType],
    ) -> CatalogResult<&'a TableFunction> {
        self.catalog
            .table_functions
            .get(name)
            .and_then(|functions| {
                functions.iter().find(|f| {
                    if f.argument_types.len() != argument_types.len() {
                        return false;
                    }
                    argument_types
                        .iter()
                        .zip(f.argument_types.iter())
                        .all(|(a, b)| a.is_compatible_with(*b))
                })
            })
            .ok_or_else(|| {
                CatalogError::unknown_function(
                    CatalogEntryKind::TableFunction,
                    name.to_owned(),
                    argument_types,
                )
            })
    }
}
