use crate::lexer;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Type {
    Integer,
    Real,
    Boolean,
    Text,
}

impl Type {
    pub(crate) fn is_numeric(self) -> bool {
        matches!(self, Self::Integer | Self::Real)
    }
}

impl std::fmt::Debug for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer => f.write_str("INTEGER"),
            Self::Real => f.write_str("REAL"),
            Self::Boolean => f.write_str("BOOLEAN"),
            Self::Text => f.write_str("TEXT"),
        }
    }
}

impl std::fmt::Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer => f.write_str("INTEGER"),
            Self::Real => f.write_str("REAL"),
            Self::Boolean => f.write_str("BOOLEAN"),
            Self::Text => f.write_str("TEXT"),
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => f.write_str("NULL"),
            Self::NonNull(ty) => ty.fmt(f),
        }
    }
}

impl NullableType {
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
}

impl std::fmt::Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => f.write_str("NULL"),
            Self::Integer(i) => i.fmt(f),
            Self::Real(r) => r.fmt(f),
            Self::Boolean(b) => b.fmt(f),
            Self::Text(s) => std::fmt::Display::fmt(&lexer::quote(s, '\''), f),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => f.write_str("NULL"),
            Self::Integer(i) => i.fmt(f),
            Self::Real(r) => r.fmt(f),
            Self::Boolean(b) => b.fmt(f),
            Self::Text(s) => s.fmt(f),
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
        }
    }

    pub(crate) fn cast(&self, ty: Type) -> Option<Self> {
        match (self, ty) {
            (Self::Null, _) => Some(Self::Null),
            (Self::Integer(_), Type::Integer)
            | (Self::Real(_), Type::Real)
            | (Self::Boolean(_), Type::Boolean)
            | (Self::Text(_), Type::Text) => Some(self.clone()),
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
        }
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Self::Integer(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Self::Real(v)
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Self::Boolean(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Self::Text(v)
    }
}

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
                match value {
                    Value::Null => Ok(None),
                    Value::$variant(v) => v
                        .try_into()
                        .map_err(|_| TryFromValueError::OutOfRange)
                        .map(Some),
                    _ => Err(TryFromValueError::IncompatibleType),
                }
            }
        }
    };
}

impl_try_from_value!(i8, Integer);
impl_try_from_value!(u8, Integer);
impl_try_from_value!(i16, Integer);
impl_try_from_value!(u16, Integer);
impl_try_from_value!(i32, Integer);
impl_try_from_value!(u32, Integer);
impl_try_from_value!(i64, Integer);
impl_try_from_value!(u64, Integer);
impl_try_from_value!(i128, Integer);
impl_try_from_value!(u128, Integer);
impl_try_from_value!(isize, Integer);
impl_try_from_value!(usize, Integer);
impl_try_from_value!(f64, Real);
impl_try_from_value!(bool, Boolean);
impl_try_from_value!(String, Text);
impl_try_from_value!(Vec<u8>, Text);

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
