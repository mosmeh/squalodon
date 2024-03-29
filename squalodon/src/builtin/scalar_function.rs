use crate::{
    catalog::ScalarFunction, storage::Storage, types::NullableType, ExecutorError, PlannerError,
    Type, Value,
};

pub fn load<T: Storage>() -> impl Iterator<Item = (&'static str, ScalarFunction<T>)> {
    [(
        "length",
        ScalarFunction {
            bind: |types| match types {
                [NullableType::Null] => Ok(NullableType::Null),
                [NullableType::NonNull(Type::Text)] => Ok(Type::Integer.into()),
                _ => Err(PlannerError::InvalidArgument),
            },
            eval: |_, args| match args {
                [Value::Null] => Ok(Value::Null),
                [Value::Text(s)] => {
                    let i = s.len().try_into().map_err(|_| ExecutorError::OutOfRange)?;
                    Ok(Value::Integer(i))
                }
                _ => unreachable!(),
            },
        },
    )]
    .into_iter()
}
