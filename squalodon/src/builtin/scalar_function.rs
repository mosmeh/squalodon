use crate::{
    catalog::{BoxedScalarFn, ScalarFunction},
    types::NullableType,
    ExecutorError, Null, Type,
};

pub fn load() -> impl Iterator<Item = ScalarFunction> {
    [
        ScalarFunction {
            name: "length",
            argument_types: &[Type::Text],
            return_type: Type::Integer.into(),
            eval: BoxedScalarFn::pure(|s: String| {
                i64::try_from(s.len()).map_err(|_| ExecutorError::OutOfRange)
            }),
        },
        ScalarFunction {
            name: "random",
            argument_types: &[],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::impure(|ctx, ()| Ok(ctx.random())),
        },
        ScalarFunction {
            name: "setseed",
            argument_types: &[Type::Real],
            return_type: NullableType::Null,
            eval: BoxedScalarFn::impure(|ctx, seed| {
                ctx.set_seed(seed);
                Ok(Null)
            }),
        },
    ]
    .into_iter()
}
