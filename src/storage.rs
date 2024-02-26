mod catalog;
mod memory;
mod transaction;

pub use catalog::{Catalog, Column};
pub use memory::Memory;
pub use transaction::SchemaAwareTransaction;
pub use Error as StorageError;

use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Invalid encoding")]
    InvalidEncoding,

    #[error("Table already exists")]
    TableAlreadyExists,

    #[error("Unknown table")]
    UnknownTable,

    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
}

type Result<T> = std::result::Result<T, Error>;

pub trait Storage {
    type Transaction<'a>: Transaction + 'a
    where
        Self: 'a;

    fn transaction(&self) -> Self::Transaction<'_>;
}

pub trait Transaction {
    fn scan(&self, start: &[u8], end: &[u8]) -> impl Iterator<Item = (&[u8], &[u8])>;
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>);
    fn commit(self);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableId(pub u64);

const TABLE_SCHEMA_PREFIX: u8 = b't';
const TABLE_DATA_PREFIX: u8 = b'd';

enum TableKeyPrefix {
    TableSchema(TableId),
    TableData(TableId),
}

impl TableKeyPrefix {
    const ENCODED_LEN: usize = std::mem::size_of::<u8>() + std::mem::size_of::<u64>();

    fn serialize(&self) -> [u8; Self::ENCODED_LEN] {
        let mut buf = [0; Self::ENCODED_LEN];
        self.serialize_into(&mut buf);
        buf
    }

    fn serialize_into(&self, buf: &mut [u8; Self::ENCODED_LEN]) {
        let (prefix, id) = match self {
            Self::TableSchema(TableId(id)) => (TABLE_SCHEMA_PREFIX, id),
            Self::TableData(TableId(id)) => (TABLE_DATA_PREFIX, id),
        };
        buf[0] = prefix;
        buf[1..].copy_from_slice(&id.to_be_bytes());
    }

    fn deserialize_from(buf: &[u8]) -> Result<Self> {
        if buf.len() != Self::ENCODED_LEN {
            return Err(Error::InvalidEncoding);
        }
        let id = u64::from_be_bytes([
            buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8],
        ]);
        match buf[0] {
            TABLE_SCHEMA_PREFIX => Ok(Self::TableSchema(TableId(id))),
            TABLE_DATA_PREFIX => Ok(Self::TableData(TableId(id))),
            _ => Err(Error::InvalidEncoding),
        }
    }
}
