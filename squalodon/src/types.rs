use crate::lexer;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Type {
    Integer,
    Real,
    Boolean,
    Text,
    Blob,
}

impl Type {
    pub(crate) const ALL: &'static [Self] = &[
        Self::Integer,
        Self::Real,
        Self::Boolean,
        Self::Text,
        Self::Blob,
    ];

    pub(crate) fn is_numeric(self) -> bool {
        matches!(self, Self::Integer | Self::Real)
    }
}

impl std::fmt::Debug for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Integer => f.write_str("INTEGER"),
            Self::Real => f.write_str("REAL"),
            Self::Boolean => f.write_str("BOOLEAN"),
            Self::Text => f.write_str("TEXT"),
            Self::Blob => f.write_str("BLOB"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum NullableType {
    Null,
    NonNull(Type),
}

impl From<Type> for NullableType {
    fn from(ty: Type) -> Self {
        Self::NonNull(ty)
    }
}

impl std::fmt::Display for NullableType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Null => f.write_str("NULL"),
            Self::NonNull(ty) => ty.fmt(f),
        }
    }
}

impl NullableType {
    pub fn is_null(self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn is_compatible_with<T: Into<Self>>(self, other: T) -> bool {
        match (self, other.into()) {
            (Self::Null, _) | (_, Self::Null) => true,
            (Self::NonNull(a), Self::NonNull(b)) => a == b,
        }
    }

    pub fn can_cast_to<T: Into<Self>>(self, other: T) -> bool {
        match (self, other.into()) {
            (Self::Null | Self::NonNull(Type::Text), _)
            | (_, Self::Null | Self::NonNull(Type::Text))
            | (Self::NonNull(Type::Integer), Self::NonNull(Type::Real | Type::Boolean))
            | (Self::NonNull(Type::Real | Type::Boolean), Self::NonNull(Type::Integer)) => true,
            (Self::NonNull(a), Self::NonNull(b)) if a == b => true,
            _ => false,
        }
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Boolean(bool),
    Text(String),
    Blob(Vec<u8>),
}

impl std::fmt::Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Null => f.write_str("NULL"),
            Self::Integer(i) => i.fmt(f),
            Self::Real(r) => r.fmt(f),
            Self::Boolean(b) => f.write_str(if *b { "TRUE" } else { "FALSE" }),
            Self::Text(s) => std::fmt::Display::fmt(&lexer::quote(s, '\''), f),
            Self::Blob(v) => std::iter::once("E'\\x".to_owned())
                .chain(v.iter().map(|b| format!("{b:02X}")))
                .chain(std::iter::once("'".to_owned()))
                .collect::<String>()
                .fmt(f),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Null => f.write_str("NULL"),
            Self::Integer(i) => i.fmt(f),
            Self::Real(r) => r.fmt(f),
            Self::Boolean(b) => f.write_str(if *b { "t" } else { "f" }),
            Self::Text(s) => s.fmt(f),
            Self::Blob(v) => std::iter::once("\\x".to_owned())
                .chain(v.iter().map(|b| format!("{b:02x}")))
                .collect::<String>()
                .fmt(f),
        }
    }
}

impl Value {
    pub(crate) fn ty(&self) -> NullableType {
        match self {
            Self::Null => NullableType::Null,
            Self::Integer(_) => Type::Integer.into(),
            Self::Real(_) => Type::Real.into(),
            Self::Boolean(_) => Type::Boolean.into(),
            Self::Text(_) => Type::Text.into(),
            Self::Blob(_) => Type::Blob.into(),
        }
    }

    pub(crate) fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub(crate) fn cast(&self, ty: Type) -> Option<Self> {
        match (self, ty) {
            (Self::Null, _) => Some(Self::Null),
            (Self::Integer(_), Type::Integer)
            | (Self::Real(_), Type::Real)
            | (Self::Boolean(_), Type::Boolean)
            | (Self::Text(_), Type::Text)
            | (Self::Blob(_), Type::Blob) => Some(self.clone()),
            (Self::Integer(i), Type::Real) => Some(Self::Real(*i as f64)),
            (Self::Integer(i), Type::Boolean) => Some(Self::Boolean(*i != 0)),
            (Self::Integer(i), Type::Text) => Some(Self::Text(i.to_string())),
            (Self::Real(r), Type::Integer) => Some(Self::Integer(r.round() as i64)),
            (Self::Real(r), Type::Text) => Some(Self::Text(r.to_string())),
            (Self::Boolean(b), Type::Integer) => Some(Self::Integer(i64::from(*b))),
            (Self::Boolean(b), Type::Text) => Some(Self::Text(b.to_string())),
            (Self::Text(s), Type::Integer) => s.parse().ok().map(Self::Integer),
            (Self::Text(s), Type::Real) => s.parse().ok().map(Self::Real),
            (Self::Text(s), Type::Boolean) => {
                if s == "1"
                    || s.eq_ignore_ascii_case("t")
                    || s.eq_ignore_ascii_case("true")
                    || s.eq_ignore_ascii_case("y")
                    || s.eq_ignore_ascii_case("yes")
                    || s.eq_ignore_ascii_case("on")
                {
                    Some(Self::Boolean(true))
                } else if s == "0"
                    || s.eq_ignore_ascii_case("f")
                    || s.eq_ignore_ascii_case("false")
                    || s.eq_ignore_ascii_case("n")
                    || s.eq_ignore_ascii_case("no")
                    || s.eq_ignore_ascii_case("off")
                {
                    Some(Self::Boolean(false))
                } else {
                    None
                }
            }
            (Self::Text(s), Type::Blob) => Some(Self::Blob(s.as_bytes().to_vec())),
            (Self::Blob(b), Type::Text) => String::from_utf8(b.clone()).ok().map(Self::Text),
            _ => None,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Integer(a), Self::Integer(b)) => a.partial_cmp(b),
            (Self::Real(a), Self::Real(b)) => a.partial_cmp(b),
            (Self::Boolean(a), Self::Boolean(b)) => a.partial_cmp(b),
            (Self::Text(a), Self::Text(b)) => a.partial_cmp(b),
            (Self::Blob(a), Self::Blob(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

impl Eq for Value {}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Null => (),
            Self::Integer(i) => i.hash(state),
            Self::Real(r) => r.to_bits().hash(state),
            Self::Boolean(b) => b.hash(state),
            Self::Text(s) => s.hash(state),
            Self::Blob(b) => b.hash(state),
        }
    }
}

impl<T: Into<Self>> From<Option<T>> for Value {
    fn from(v: Option<T>) -> Self {
        v.map_or(Self::Null, Into::into)
    }
}

macro_rules! impl_from_value {
    ($ty:ty, $variant:ident) => {
        impl From<$ty> for Value {
            fn from(v: $ty) -> Self {
                Self::$variant(v)
            }
        }
    };
}

impl_from_value!(i64, Integer);
impl_from_value!(f64, Real);
impl_from_value!(bool, Boolean);
impl_from_value!(String, Text);

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Self::Text(v.to_owned())
    }
}

pub struct Null;

impl From<Null> for Value {
    fn from(_: Null) -> Self {
        Self::Null
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TryFromValueError {
    #[error("Incompatible type")]
    IncompatibleType,

    #[error("Out of range")]
    OutOfRange,
}

macro_rules! impl_try_from_value {
    ($ty:ty, $variant:ident) => {
        impl TryFrom<Value> for $ty {
            type Error = TryFromValueError;

            fn try_from(value: Value) -> Result<Self, TryFromValueError> {
                match value {
                    Value::$variant(v) => v.try_into().map_err(|_| TryFromValueError::OutOfRange),
                    _ => Err(TryFromValueError::IncompatibleType),
                }
            }
        }

        impl TryFrom<Value> for Option<$ty> {
            type Error = TryFromValueError;

            fn try_from(value: Value) -> Result<Self, TryFromValueError> {
                Ok(match value {
                    Value::Null => None,
                    _ => Some(value.try_into()?),
                })
            }
        }
    };
}

impl_try_from_value!(String, Text);
impl_try_from_value!(Vec<u8>, Blob);

macro_rules! impl_try_from_value_with_from_str {
    ($ty:ty, $variant:ident) => {
        impl TryFrom<Value> for $ty {
            type Error = TryFromValueError;

            fn try_from(value: Value) -> Result<Self, TryFromValueError> {
                match value {
                    Value::$variant(v) => v.try_into().map_err(|_| TryFromValueError::OutOfRange),
                    Value::Text(v) => v.parse().map_err(|_| TryFromValueError::OutOfRange),
                    _ => Err(TryFromValueError::IncompatibleType),
                }
            }
        }

        impl TryFrom<Value> for Option<$ty> {
            type Error = TryFromValueError;

            fn try_from(value: Value) -> Result<Self, TryFromValueError> {
                Ok(match value {
                    Value::Null => None,
                    _ => Some(value.try_into()?),
                })
            }
        }
    };
}

impl_try_from_value_with_from_str!(i8, Integer);
impl_try_from_value_with_from_str!(u8, Integer);
impl_try_from_value_with_from_str!(i16, Integer);
impl_try_from_value_with_from_str!(u16, Integer);
impl_try_from_value_with_from_str!(i32, Integer);
impl_try_from_value_with_from_str!(u32, Integer);
impl_try_from_value_with_from_str!(i64, Integer);
impl_try_from_value_with_from_str!(u64, Integer);
impl_try_from_value_with_from_str!(i128, Integer);
impl_try_from_value_with_from_str!(u128, Integer);
impl_try_from_value_with_from_str!(isize, Integer);
impl_try_from_value_with_from_str!(usize, Integer);
impl_try_from_value_with_from_str!(bool, Boolean);

impl TryFrom<Value> for f64 {
    type Error = TryFromValueError;

    fn try_from(value: Value) -> Result<Self, TryFromValueError> {
        match value {
            Value::Integer(v) => Ok(v as Self),
            Value::Real(v) => Ok(v),
            Value::Text(v) => v.parse().map_err(|_| TryFromValueError::OutOfRange),
            _ => Err(TryFromValueError::IncompatibleType),
        }
    }
}

impl TryFrom<Value> for Option<f64> {
    type Error = TryFromValueError;

    fn try_from(value: Value) -> Result<Self, TryFromValueError> {
        Ok(match value {
            Value::Null => None,
            _ => Some(value.try_into()?),
        })
    }
}

impl TryFrom<Value> for Null {
    type Error = TryFromValueError;

    fn try_from(value: Value) -> Result<Self, TryFromValueError> {
        match value {
            Value::Null => Ok(Self),
            _ => Err(TryFromValueError::IncompatibleType),
        }
    }
}

pub trait Params {
    fn into_values(self) -> Vec<Value>;
}

impl<T: Into<Value>> Params for T {
    fn into_values(self) -> Vec<Value> {
        vec![self.into()]
    }
}

impl Params for Vec<Value> {
    fn into_values(self) -> Vec<Value> {
        self
    }
}

impl Params for &[Value] {
    fn into_values(self) -> Vec<Value> {
        self.to_vec()
    }
}

impl Params for &[&Value] {
    fn into_values(self) -> Vec<Value> {
        self.iter().map(|v| (*v).clone()).collect()
    }
}

impl<const N: usize> Params for [Value; N] {
    fn into_values(self) -> Vec<Value> {
        self.to_vec()
    }
}

impl<const N: usize> Params for &[Value; N] {
    fn into_values(self) -> Vec<Value> {
        self.to_vec()
    }
}

macro_rules! impl_params_for_tuple {
    ($($i:tt $t:ident)*) => {
        impl<$($t,)*> Params for ($($t,)*)
        where
            $($t: Into<Value>,)*
        {
            fn into_values(self) -> Vec<Value> {
                vec![$(self.$i.into(),)*]
            }
        }
    };
}

impl_params_for_tuple!();
impl_params_for_tuple!(0 T);
impl_params_for_tuple!(0 T0 1 T1);
impl_params_for_tuple!(0 T0 1 T1 2 T2);
impl_params_for_tuple!(0 T0 1 T1 2 T2 3 T3);
impl_params_for_tuple!(0 T0 1 T1 2 T2 3 T3 4 T4);
impl_params_for_tuple!(0 T0 1 T1 2 T2 3 T3 4 T4 5 T5);
impl_params_for_tuple!(0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6);
impl_params_for_tuple!(0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6 7 T7);
impl_params_for_tuple!(0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6 7 T7 8 T8);
impl_params_for_tuple!(0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6 7 T7 8 T8 9 T9);
impl_params_for_tuple!(0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6 7 T7 8 T8 9 T9 10 T10);
impl_params_for_tuple!(0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6 7 T7 8 T8 9 T9 10 T10 11 T11);
impl_params_for_tuple!(0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6 7 T7 8 T8 9 T9 10 T10 11 T11 12 T12);
impl_params_for_tuple!(0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6 7 T7 8 T8 9 T9 10 T10 11 T11 12 T12 13 T13);
impl_params_for_tuple!(0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6 7 T7 8 T8 9 T9 10 T10 11 T11 12 T12 13 T13 14 T14);
impl_params_for_tuple!(0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6 7 T7 8 T8 9 T9 10 T10 11 T11 12 T12 13 T13 14 T14 15 T15);

pub trait Args {
    fn from_values(values: &[Value]) -> Result<Self, TryFromValueError>
    where
        Self: Sized;
}

impl<T> Args for T
where
    T: TryFrom<Value, Error = TryFromValueError>,
{
    fn from_values(values: &[Value]) -> Result<Self, TryFromValueError> {
        match values {
            [v] => v.clone().try_into(),
            _ => Err(TryFromValueError::IncompatibleType),
        }
    }
}

macro_rules! impl_args_for_tuple {
    ($($t:ident)*) => {
        impl<$($t,)*> Args for ($($t,)*)
        where
            $($t: TryFrom<Value, Error = TryFromValueError>,)*
        {
            fn from_values(values: &[Value]) -> Result<Self, TryFromValueError> {
                match values {
                    #[allow(non_snake_case)]
                    [$($t,)*] => Ok(($($t.clone().try_into()?,)*)),
                    _ => Err(TryFromValueError::IncompatibleType),
                }
            }
        }
    };
}

impl_args_for_tuple!();
impl_args_for_tuple!(T);
impl_args_for_tuple!(T0 T1);
impl_args_for_tuple!(T0 T1 T2);
impl_args_for_tuple!(T0 T1 T2 T3);
impl_args_for_tuple!(T0 T1 T2 T3 T4);
impl_args_for_tuple!(T0 T1 T2 T3 T4 T5);
impl_args_for_tuple!(T0 T1 T2 T3 T4 T5 T6);
impl_args_for_tuple!(T0 T1 T2 T3 T4 T5 T6 T7);
impl_args_for_tuple!(T0 T1 T2 T3 T4 T5 T6 T7 T8);
impl_args_for_tuple!(T0 T1 T2 T3 T4 T5 T6 T7 T8 T9);
impl_args_for_tuple!(T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 T10);
impl_args_for_tuple!(T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11);
impl_args_for_tuple!(T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11 T12);
impl_args_for_tuple!(T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11 T12 T13);
impl_args_for_tuple!(T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11 T12 T13 T14);
impl_args_for_tuple!(T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11 T12 T13 T14 T15);
