use crate::{Error, NullOrder, Order, Result, Value};
use std::borrow::Cow;

const TAG_SIZE: usize = std::mem::size_of::<u8>();
const NULL_FIRST_TAG: u8 = 0;
const NULL_LAST_TAG: u8 = u8::MAX;
const INTEGER_TAG: u8 = 1;
const REAL_TAG: u8 = 2;
const BOOLEAN_TAG: u8 = 3;
const TEXT_TAG: u8 = 4;

const CHUNK_SIZE: usize = 8;
const UNIT_SIZE: usize = CHUNK_SIZE + 1;
const CHUNK_SIZE_U8: u8 = CHUNK_SIZE as u8;
const UNIT_SIZE_U8: u8 = UNIT_SIZE as u8;

#[derive(Default)]
pub struct SerdeOptions {
    order: Order,
    null_order: NullOrder,
}

impl SerdeOptions {
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
        match self.order {
            Order::Asc => {}
            Order::Desc => {
                for i in &mut buf[start + TAG_SIZE..] {
                    *i = !*i;
                }
            }
        }
    }

    #[allow(dead_code)]
    pub fn deserialize_from(&self, buf: &[u8]) -> Result<Value> {
        let (&tag, buf) = buf.split_first().ok_or(Error::InvalidEncoding)?;
        let buf = match self.order {
            Order::Desc if !buf.is_empty() => Cow::Owned(buf.iter().map(|i| !i).collect()),
            Order::Asc | Order::Desc => Cow::Borrowed(buf),
        };
        match (tag, buf.as_ref()) {
            (NULL_FIRST_TAG, []) => match self.null_order {
                NullOrder::NullsFirst => Ok(Value::Null),
                NullOrder::NullsLast => Err(Error::InvalidEncoding),
            },
            (NULL_LAST_TAG, []) => match self.null_order {
                NullOrder::NullsFirst => Err(Error::InvalidEncoding),
                NullOrder::NullsLast => Ok(Value::Null),
            },
            (INTEGER_TAG, serialized) => {
                let serialized = serialized.try_into().map_err(|_| Error::InvalidEncoding)?;
                let i = (u64::from_be_bytes(serialized) ^ (1 << (u64::BITS - 1))) as i64;
                Ok(Value::Integer(i))
            }
            (REAL_TAG, serialized) => {
                let serialized = serialized.try_into().map_err(|_| Error::InvalidEncoding)?;
                let mut v = u64::from_be_bytes(serialized);
                if v & (1 << (u64::BITS - 1)) != 0 {
                    v &= !(1 << (u64::BITS - 1));
                } else {
                    v = !v;
                }
                Ok(Value::Real(f64::from_bits(v)))
            }
            (BOOLEAN_TAG, [0]) => Ok(Value::Boolean(false)),
            (BOOLEAN_TAG, [1]) => Ok(Value::Boolean(true)),
            (TEXT_TAG, [0]) => Ok(Value::Text(String::new())),
            (TEXT_TAG, [1, serialized @ ..]) => {
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
                Ok(Value::Text(s))
            }
            _ => Err(Error::InvalidEncoding),
        }
    }
}
