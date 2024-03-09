use crate::{Error, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Type {
    Integer,
    Boolean,
    Text,
}

impl std::fmt::Debug for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer => f.write_str("INTEGER"),
            Self::Boolean => f.write_str("BOOLEAN"),
            Self::Text => f.write_str("TEXT"),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Integer(i64),
    Boolean(bool),
    Text(String),
}

impl std::fmt::Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => f.write_str("NULL"),
            Self::Integer(i) => i.fmt(f),
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
            Self::Boolean(_) => Some(Type::Boolean),
            Self::Text(_) => Some(Type::Text),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Integer(a), Self::Integer(b)) => a.partial_cmp(b),
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

macro_rules! impl_try_from_value {
    ($ty:ty, $variant:ident) => {
        impl TryFrom<Value> for $ty {
            type Error = Error;

            fn try_from(value: Value) -> Result<Self> {
                match value {
                    Value::$variant(v) => v.try_into().map_err(|_| Error::OutOfRange),
                    _ => Err(Error::IncompatibleType),
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
impl_try_from_value!(bool, Boolean);
impl_try_from_value!(String, Text);
impl_try_from_value!(Vec<u8>, Text);

impl Value {
    pub fn as_bool(&self) -> bool {
        !matches!(self, Self::Boolean(false) | Self::Integer(0) | Self::Null)
    }
}
