mod blackhole;
mod memory;

pub use blackhole::Blackhole;
pub use memory::Memory;

use crate::{catalog::TableId, memcomparable::MemcomparableSerde, Value};

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Unknown table {0:?}")]
    UnknownTable(String),

    #[error("Duplicate key {0:?}")]
    DuplicateKey(Value),

    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
}

pub(crate) type StorageResult<T> = std::result::Result<T, StorageError>;

pub trait KeyValueStore {
    type Transaction<'a>: KeyValueTransaction
    where
        Self: 'a;

    fn transaction(&self) -> Self::Transaction<'_>;
}

pub trait KeyValueTransaction {
    /// Returns None if the key does not exist.
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;

    /// Returns an iterator over the key-value pairs in the range [start, end).
    fn scan<const N: usize>(
        &self,
        start: [u8; N],
        end: [u8; N],
    ) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)>;

    /// Returns true if the key was inserted, false if it already existed.
    #[must_use]
    fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> bool;

    /// Returns true if the key was updated, false if it did not exist.
    #[must_use]
    fn update(&self, key: Vec<u8>, value: Vec<u8>) -> bool;

    /// Returns the value if the key was removed, None if it did not exist.
    #[must_use]
    fn remove(&self, key: Vec<u8>) -> Option<Vec<u8>>;

    /// Commits the transaction.
    ///
    /// The transaction rolls back if it is dropped without being committed.
    fn commit(self);
}

pub(crate) struct Storage<T: KeyValueStore> {
    kvs: T,
}

impl<T: KeyValueStore> Storage<T> {
    pub fn new(kvs: T) -> Self {
        Self { kvs }
    }

    pub fn transaction(&self) -> Transaction<T> {
        Transaction {
            raw: self.kvs.transaction(),
        }
    }
}

pub(crate) struct Transaction<'a, T: KeyValueStore + 'a> {
    raw: T::Transaction<'a>,
}

impl<'a, T: KeyValueStore> Transaction<'a, T> {
    pub fn raw(&self) -> &T::Transaction<'a> {
        &self.raw
    }

    pub fn scan_table(
        &self,
        table: TableId,
    ) -> Box<dyn Iterator<Item = StorageResult<Vec<Value>>> + '_> {
        let start = table.serialize();
        let mut end = start;
        end[end.len() - 1] += 1;
        let iter = self
            .raw
            .scan(start, end)
            .map(|(_, v)| bincode::deserialize(&v).map_err(Into::into));
        Box::new(iter)
    }

    pub fn insert(
        &self,
        table: TableId,
        primary_key: &Value,
        columns: &[Value],
    ) -> StorageResult<()> {
        let mut key = table.serialize().to_vec();
        MemcomparableSerde::new().serialize_into(primary_key, &mut key);
        if self.raw.insert(key, bincode::serialize(columns)?) {
            Ok(())
        } else {
            Err(StorageError::DuplicateKey(primary_key.clone()))
        }
    }

    pub fn update(
        &self,
        table: TableId,
        primary_key: &Value,
        columns: &[Value],
    ) -> StorageResult<()> {
        let mut key = table.serialize().to_vec();
        MemcomparableSerde::new().serialize_into(primary_key, &mut key);
        let updated = self.raw.update(key, bincode::serialize(columns)?);
        assert!(updated);
        Ok(())
    }

    pub fn delete(&self, table: TableId, primary_key: &Value) {
        let mut key = table.serialize().to_vec();
        MemcomparableSerde::new().serialize_into(primary_key, &mut key);
        let deleted = self.raw.remove(key).is_some();
        assert!(deleted);
    }

    pub fn commit(self) {
        self.raw.commit();
    }
}
