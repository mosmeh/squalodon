use crate::{
    catalog::ScalarFunction, storage::Storage, types::NullableType, ExecutorError, PlannerError,
    Type, Value,
};

pub fn load<T: Storage>() -> impl Iterator<Item = ScalarFunction<T>> {
    [
        ScalarFunction {
            name: "length",
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
        ScalarFunction {
            name: "random",
            bind: |types| {
                if types.is_empty() {
                    Ok(Type::Real.into())
                } else {
                    Err(PlannerError::ArityError)
                }
            },
            eval: |ctx, _| Ok(ctx.random().into()),
        },
        ScalarFunction {
            name: "setseed",
            bind: |types| match types {
                [NullableType::Null | NullableType::NonNull(Type::Real)] => Ok(NullableType::Null),
                _ => Err(PlannerError::InvalidArgument),
            },
            eval: |ctx, args| match args {
                [Value::Null] => Ok(Value::Null),
                [Value::Real(seed)] => {
                    ctx.set_seed(*seed);
                    Ok(Value::Null)
                }
                _ => unreachable!(),
            },
        },
    ]
    .into_iter()
}
