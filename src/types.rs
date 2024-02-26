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

const NULL_TAG: u8 = 0;
const INTEGER_TAG: u8 = 1;
const BOOLEAN_TAG: u8 = 2;
const TEXT_TAG: u8 = 3;

const CHUNK_SIZE: usize = 8;
const UNIT_SIZE: usize = CHUNK_SIZE + 1;
const CHUNK_SIZE_U8: u8 = CHUNK_SIZE as u8;
const UNIT_SIZE_U8: u8 = UNIT_SIZE as u8;

impl Value {
    pub fn as_bool(&self) -> bool {
        !matches!(self, Self::Boolean(false) | Self::Integer(0) | Self::Null)
    }

    /// Serializes the value into `buf` in a memcomparable format.
    pub fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            Self::Null => buf.push(NULL_TAG),
            Self::Integer(i) => {
                buf.push(INTEGER_TAG);
                buf.extend_from_slice(&(*i as u64 ^ (1 << (u64::BITS - 1))).to_be_bytes());
            }
            Self::Boolean(b) => {
                buf.push(BOOLEAN_TAG);
                buf.push(u8::from(*b));
            }
            Self::Text(s) => {
                buf.push(TEXT_TAG);
                buf.push(u8::from(!s.is_empty()));
                let mut len = 0;
                for chunk in s.as_bytes().chunks(CHUNK_SIZE) {
                    buf.extend_from_slice(chunk);
                    for _ in chunk.len()..CHUNK_SIZE {
                        buf.push(0);
                    }
                    len += chunk.len();
                    buf.push(if len == s.len() {
                        chunk.len() as u8
                    } else {
                        UNIT_SIZE_U8
                    });
                }
            }
        }
    }

    pub fn deserialize_from(buf: &[u8]) -> Result<Self> {
        match buf {
            [NULL_TAG] => Ok(Self::Null),
            [INTEGER_TAG, serialized @ ..] => {
                let serialized = serialized.try_into().map_err(|_| Error::InvalidEncoding)?;
                let i = (u64::from_be_bytes(serialized) ^ (1 << (u64::BITS - 1))) as i64;
                Ok(Self::Integer(i))
            }
            [BOOLEAN_TAG, 0] => Ok(Self::Boolean(false)),
            [BOOLEAN_TAG, 1] => Ok(Self::Boolean(true)),
            [TEXT_TAG, 0] => Ok(Self::Text(String::new())),
            [TEXT_TAG, 1, serialized @ ..] => {
                let mut s = Vec::with_capacity(serialized.len() / UNIT_SIZE);
                let mut serialized = serialized;
                loop {
                    let (unit, rest) = serialized.split_at(UNIT_SIZE);
                    match unit[CHUNK_SIZE] {
                        len @ 1..=CHUNK_SIZE_U8 => {
                            s.extend_from_slice(&unit[..len as usize]);
                            break;
                        }
                        UNIT_SIZE_U8 => {
                            s.extend_from_slice(&unit[..CHUNK_SIZE]);
                            serialized = rest;
                        }
                        _ => return Err(Error::InvalidEncoding),
                    }
                }
                let s = String::from_utf8(s).map_err(|_| Error::InvalidEncoding)?;
                Ok(Self::Text(s))
            }
            _ => Err(Error::InvalidEncoding),
        }
    }
}
