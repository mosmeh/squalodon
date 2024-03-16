use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Type {
    Integer,
    Real,
    Boolean,
    Text,
}

impl Type {
    pub fn is_numeric(self) -> bool {
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
            Self::Text(s) => s.fmt(f),
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
    pub fn ty(&self) -> Option<Type> {
        match self {
            Self::Null => None,
            Self::Integer(_) => Some(Type::Integer),
            Self::Real(_) => Some(Type::Real),
            Self::Boolean(_) => Some(Type::Boolean),
            Self::Text(_) => Some(Type::Text),
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
