use crate::{
    catalog::{BoxedScalarFn, ScalarFunction},
    types::NullableType,
    ExecutorError, Null, Type,
};
use std::num::FpCategory;

pub fn load() -> impl Iterator<Item = ScalarFunction> {
    math()
        .chain(random())
        .chain(trigonometric())
        .chain(hyperbolic())
        .chain(string())
}

fn math() -> impl Iterator<Item = ScalarFunction> {
    [
        ScalarFunction {
            name: "abs",
            argument_types: &[Type::Integer],
            return_type: Type::Integer.into(),
            eval: BoxedScalarFn::pure(|x: i64| x.checked_abs().ok_or(ExecutorError::OutOfRange)),
        },
        ScalarFunction {
            name: "abs",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.abs())),
        },
        ScalarFunction {
            name: "ceil",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.ceil())),
        },
        ScalarFunction {
            name: "ceiling",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.ceil())),
        },
        ScalarFunction {
            name: "degrees",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.to_degrees())),
        },
        ScalarFunction {
            name: "exp",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.exp())),
        },
        ScalarFunction {
            name: "floor",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.floor())),
        },
        ScalarFunction {
            name: "ln",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.ln())),
        },
        ScalarFunction {
            name: "log",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.log10())),
        },
        ScalarFunction {
            name: "log",
            argument_types: &[Type::Real, Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|(b, x): (f64, f64)| Ok(x.log(b))),
        },
        ScalarFunction {
            name: "log10",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.log10())),
        },
        ScalarFunction {
            name: "mod",
            argument_types: &[Type::Integer, Type::Integer],
            return_type: Type::Integer.into(),
            eval: BoxedScalarFn::pure(|(x, y): (i64, i64)| {
                x.checked_rem(y).ok_or(ExecutorError::OutOfRange)
            }),
        },
        ScalarFunction {
            name: "mod",
            argument_types: &[Type::Real, Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|(x, y): (f64, f64)| Ok(x % y)),
        },
        ScalarFunction {
            name: "pi",
            argument_types: &[],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|()| Ok(std::f64::consts::PI)),
        },
        ScalarFunction {
            name: "power",
            argument_types: &[Type::Real, Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|(x, y): (f64, f64)| Ok(x.powf(y))),
        },
        ScalarFunction {
            name: "radians",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.to_radians())),
        },
        ScalarFunction {
            name: "round",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.round())),
        },
        ScalarFunction {
            name: "sign",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| {
                Ok(match x.classify() {
                    FpCategory::Zero | FpCategory::Nan => 0.0,
                    _ => x.signum(),
                })
            }),
        },
        ScalarFunction {
            name: "sqrt",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.sqrt())),
        },
        ScalarFunction {
            name: "trunc",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.trunc())),
        },
    ]
    .into_iter()
}

fn random() -> impl Iterator<Item = ScalarFunction> {
    [
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

fn trigonometric() -> impl Iterator<Item = ScalarFunction> {
    [
        ScalarFunction {
            name: "acos",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.acos())),
        },
        ScalarFunction {
            name: "asin",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.asin())),
        },
        ScalarFunction {
            name: "atan",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.atan())),
        },
        ScalarFunction {
            name: "atan2",
            argument_types: &[Type::Real, Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|(y, x): (f64, f64)| Ok(y.atan2(x))),
        },
        ScalarFunction {
            name: "cos",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.cos())),
        },
        ScalarFunction {
            name: "sin",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.sin())),
        },
        ScalarFunction {
            name: "tan",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.tan())),
        },
    ]
    .into_iter()
}

fn hyperbolic() -> impl Iterator<Item = ScalarFunction> {
    [
        ScalarFunction {
            name: "acosh",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.acosh())),
        },
        ScalarFunction {
            name: "asinh",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.asinh())),
        },
        ScalarFunction {
            name: "atanh",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.atanh())),
        },
        ScalarFunction {
            name: "cosh",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.cosh())),
        },
        ScalarFunction {
            name: "sinh",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.sinh())),
        },
        ScalarFunction {
            name: "tanh",
            argument_types: &[Type::Real],
            return_type: Type::Real.into(),
            eval: BoxedScalarFn::pure(|x: f64| Ok(x.tanh())),
        },
    ]
    .into_iter()
}

fn string() -> impl Iterator<Item = ScalarFunction> {
    [
        ScalarFunction {
            name: "length",
            argument_types: &[Type::Text],
            return_type: Type::Integer.into(),
            eval: BoxedScalarFn::pure(|s: String| {
                i64::try_from(s.chars().count()).map_err(|_| ExecutorError::OutOfRange)
            }),
        },
        ScalarFunction {
            name: "lower",
            argument_types: &[Type::Text],
            return_type: Type::Text.into(),
            eval: BoxedScalarFn::pure(|s: String| Ok(s.to_lowercase())),
        },
        ScalarFunction {
            name: "octet_length",
            argument_types: &[Type::Text],
            return_type: Type::Integer.into(),
            eval: BoxedScalarFn::pure(|s: String| {
                i64::try_from(s.len()).map_err(|_| ExecutorError::OutOfRange)
            }),
        },
        ScalarFunction {
            name: "replace",
            argument_types: &[Type::Text, Type::Text, Type::Text],
            return_type: Type::Text.into(),
            eval: BoxedScalarFn::pure(|(string, from, to): (String, String, String)| {
                Ok(string.replace(&from, &to))
            }),
        },
        ScalarFunction {
            name: "upper",
            argument_types: &[Type::Text],
            return_type: Type::Text.into(),
            eval: BoxedScalarFn::pure(|s: String| Ok(s.to_uppercase())),
        },
    ]
    .into_iter()
}
