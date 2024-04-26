//! Encoding and decoding of memcomparable format.
//!
//! See:
//! <https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format>
//! <https://github.com/risingwavelabs/memcomparable>

use crate::{
    parser::{NullOrder, Order},
    Value,
};
use std::borrow::Cow;

const TAG_SIZE: usize = std::mem::size_of::<u8>();
const NULL_FIRST_TAG: u8 = 0;
const NULL_LAST_TAG: u8 = u8::MAX;
const INTEGER_TAG: u8 = 1;
const REAL_TAG: u8 = 2;
const BOOLEAN_TAG: u8 = 3;
const TEXT_TAG: u8 = 4;
const BLOB_TAG: u8 = 5;

const CHUNK_SIZE: usize = 8;
const UNIT_SIZE: usize = CHUNK_SIZE + 1;
const CHUNK_SIZE_U8: u8 = CHUNK_SIZE as u8;
const UNIT_SIZE_U8: u8 = UNIT_SIZE as u8;

pub struct MemcomparableSerde {
    order: Order,
    null_order: NullOrder,
}

impl Default for MemcomparableSerde {
    fn default() -> Self {
        Self {
            order: Order::Asc,
            null_order: Order::Asc.default_null_order(),
        }
    }
}

impl MemcomparableSerde {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn order(mut self, order: Order) -> Self {
        self.order = order;
        self
    }

    pub fn null_order(mut self, null_order: NullOrder) -> Self {
        self.null_order = null_order;
        self
    }

    /// Serialize a value into bytes, appending them to `buf`.
    pub fn serialize_into(&self, value: &Value, buf: &mut Vec<u8>) {
        let start = buf.len();
        match value {
            Value::Null => buf.push(match self.null_order {
                NullOrder::NullsFirst => NULL_FIRST_TAG,
                NullOrder::NullsLast => NULL_LAST_TAG,
            }),
            Value::Integer(i) => {
                buf.push(INTEGER_TAG);
                buf.extend_from_slice(&(*i as u64 ^ (1 << (u64::BITS - 1))).to_be_bytes());
            }
            Value::Real(mut r) => {
                if r.is_nan() {
                    r = f64::NAN;
                } else if r == 0.0 {
                    r = 0.0;
                }
                let mut v = r.to_bits();
                if r.is_sign_positive() {
                    v |= 1 << (u64::BITS - 1);
                } else {
                    v = !v;
                }
                buf.push(REAL_TAG);
                buf.extend_from_slice(&v.to_be_bytes());
            }
            Value::Boolean(b) => {
                buf.push(BOOLEAN_TAG);
                buf.push(u8::from(*b));
            }
            Value::Text(s) => {
                buf.push(TEXT_TAG);
                serialize_bytes_into(buf, s.as_bytes());
            }
            Value::Blob(b) => {
                buf.push(BLOB_TAG);
                serialize_bytes_into(buf, b);
            }
        }
        match self.order {
            Order::Asc => {}
            Order::Desc => {
                for i in &mut buf[start + TAG_SIZE..] {
                    *i = !*i;
                }
            }
        }
    }

    /// Deserialize a value at the beginning of the buffer.
    ///
    /// When successful, returns the value and the number of bytes consumed.
    pub fn deserialize_from(&self, buf: &[u8]) -> Result<(Value, usize), DeserializeError> {
        self.deserialize_from_inner(buf).ok_or(DeserializeError)
    }

    /// Deserialize a sequence of values from the buffer.
    ///
    /// Invalid or incomplete trailing bytes are ignored.
    pub fn deserialize_seq_from<'a>(
        &'a self,
        buf: &'a [u8],
    ) -> impl Iterator<Item = Result<Value, DeserializeError>> + '_ {
        let mut buf = buf;
        std::iter::from_fn(move || {
            if buf.is_empty() {
                return None;
            }
            match self.deserialize_from(buf) {
                Ok((value, len)) => {
                    buf = &buf[len..];
                    Some(Ok(value))
                }
                Err(DeserializeError) => {
                    buf = &[];
                    Some(Err(DeserializeError))
                }
            }
        })
    }

    fn deserialize_from_inner(&self, buf: &[u8]) -> Option<(Value, usize)> {
        let (&tag, buf) = buf.split_first()?;
        let buf = match self.order {
            Order::Desc if !buf.is_empty() => Cow::Owned(buf.iter().map(|i| !i).collect()),
            Order::Asc | Order::Desc => Cow::Borrowed(buf),
        };
        match (tag, buf.as_ref()) {
            (NULL_FIRST_TAG, _) => match self.null_order {
                NullOrder::NullsFirst => Some((Value::Null, TAG_SIZE)),
                NullOrder::NullsLast => None,
            },
            (NULL_LAST_TAG, _) => match self.null_order {
                NullOrder::NullsFirst => None,
                NullOrder::NullsLast => Some((Value::Null, TAG_SIZE)),
            },
            (INTEGER_TAG, serialized) => {
                let serialized = *serialized.first_chunk()?;
                let i = (u64::from_be_bytes(serialized) ^ (1 << (u64::BITS - 1))) as i64;
                Some((Value::Integer(i), TAG_SIZE + serialized.len()))
            }
            (REAL_TAG, serialized) => {
                let serialized = *serialized.first_chunk()?;
                let mut v = u64::from_be_bytes(serialized);
                if v & (1 << (u64::BITS - 1)) != 0 {
                    v &= !(1 << (u64::BITS - 1));
                } else {
                    v = !v;
                }
                Some((Value::Real(f64::from_bits(v)), TAG_SIZE + serialized.len()))
            }
            (BOOLEAN_TAG, [0, ..]) => Some((Value::Boolean(false), TAG_SIZE + 1)),
            (BOOLEAN_TAG, [1, ..]) => Some((Value::Boolean(true), TAG_SIZE + 1)),
            (TEXT_TAG, [0, ..]) => Some((Value::Text(String::new()), TAG_SIZE + 1)),
            (TEXT_TAG, [1, serialized @ ..]) => {
                let (s, len) = deserialize_bytes_from(serialized)?;
                let s = String::from_utf8(s).ok()?;
                Some((Value::Text(s), len))
            }
            (BLOB_TAG, [0, ..]) => Some((Value::Blob(Vec::new()), TAG_SIZE + 1)),
            (BLOB_TAG, [1, serialized @ ..]) => {
                let (b, len) = deserialize_bytes_from(serialized)?;
                Some((Value::Blob(b), len))
            }
            _ => None,
        }
    }
}

fn serialize_bytes_into(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.push(u8::from(!bytes.is_empty()));
    let mut len = 0;
    for chunk in bytes.chunks(CHUNK_SIZE) {
        buf.extend_from_slice(chunk);
        for _ in chunk.len()..CHUNK_SIZE {
            buf.push(0);
        }
        len += chunk.len();
        buf.push(if len == bytes.len() {
            chunk.len() as u8
        } else {
            UNIT_SIZE_U8
        });
    }
}

fn deserialize_bytes_from(mut buf: &[u8]) -> Option<(Vec<u8>, usize)> {
    let mut s = Vec::new();
    let mut consumed_len = TAG_SIZE + 1;
    loop {
        if buf.len() < UNIT_SIZE {
            return None;
        }
        let (unit, rest) = buf.split_at(UNIT_SIZE);
        consumed_len += UNIT_SIZE;
        match unit[CHUNK_SIZE] {
            len @ 1..=CHUNK_SIZE_U8 => {
                s.extend_from_slice(&unit[..len as usize]);
                break;
            }
            UNIT_SIZE_U8 => {
                s.extend_from_slice(&unit[..CHUNK_SIZE]);
                buf = rest;
            }
            _ => return None,
        }
    }
    Some((s, consumed_len))
}

#[derive(Debug, thiserror::Error)]
#[error("Deserialization failed")]
pub struct DeserializeError;
