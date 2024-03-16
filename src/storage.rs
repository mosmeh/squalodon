mod blackhole;
mod memory;
mod schema;

pub use blackhole::Blackhole;
pub use memory::Memory;
pub(crate) use schema::{Column, Table, TableId};

use crate::{memcomparable::MemcomparableSerde, Value};
use schema::TableRef;
use std::sync::atomic::AtomicU64;

#[cfg(feature = "rocksdb")]
mod rocks;
#[cfg(feature = "rocksdb")]
pub use rocks::RocksDB;
#[cfg(feature = "rocksdb")]
pub use rocksdb;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Unknown table {0:?}")]
    UnknownTable(String),

    #[error("Duplicate key {0:?}")]
    DuplicateKey(Value),

    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
}

pub(crate) type Result<T> = std::result::Result<T, Error>;

pub trait KeyValueStore {
    type Transaction<'a>: KeyValueTransaction + 'a
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

pub(crate) struct Storage<T> {
    kvs: T,
    next_table_id: AtomicU64,
}

impl<T: KeyValueStore> Storage<T> {
    pub fn new(kvs: T) -> Result<Self> {
        let mut max_table_id = TableId::MAX_SYSTEM.0;
        let start = TableId::CATALOG.serialize();
        let mut end = start;
        end[end.len() - 1] += 1;

        let txn = kvs.transaction();
        for (_, value) in txn.scan(start, end) {
            let table: Table = bincode::deserialize(&value)?;
            max_table_id = max_table_id.max(table.id.0);
        }
        txn.commit();

        Ok(Self {
            kvs,
            next_table_id: (max_table_id + 1).into(),
        })
    }

    pub fn transaction(&self) -> Transaction<T> {
        Transaction {
            storage: self,
            kv_txn: self.kvs.transaction(),
        }
    }
}

pub(crate) struct Transaction<'a, T: KeyValueStore> {
    storage: &'a Storage<T>,
    kv_txn: T::Transaction<'a>,
}

impl<'a, T: KeyValueStore> Transaction<'a, T> {
    pub fn create_table(&self, name: &str, columns: &[Column]) -> Result<TableId> {
        let id = self
            .storage
            .next_table_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let id = TableId(id);

        let mut key = TableId::CATALOG.serialize().to_vec();
        key.extend_from_slice(name.as_bytes());

        let table = TableRef { id, name, columns };
        let inserted = self.kv_txn.insert(key, bincode::serialize(&table)?);
        assert!(inserted);

        Ok(id)
    }

    pub fn drop_table(&self, name: &str) -> Result<()> {
        let mut key = TableId::CATALOG.serialize().to_vec();
        key.extend_from_slice(name.as_bytes());
        let bytes = self.kv_txn.remove(key).unwrap();
        let table: Table = bincode::deserialize(&bytes)?;
        assert_eq!(table.name, name);
        let start = table.id.serialize();
        let mut end = start;
        end[end.len() - 1] += 1;
        for (key, _) in self.kv_txn.scan(start, end) {
            self.kv_txn.remove(key).unwrap();
        }
        Ok(())
    }

    pub fn table(&self, name: &str) -> Result<Table> {
        let mut key = TableId::CATALOG.serialize().to_vec();
        key.extend_from_slice(name.as_bytes());
        match self.kv_txn.get(&key) {
            Some(data) => Ok(bincode::deserialize(&data)?),
            None => Err(Error::UnknownTable(name.to_owned())),
        }
    }

    pub fn scan_table(&self, table: TableId) -> Box<dyn Iterator<Item = Result<Vec<Value>>> + '_> {
        let start = table.serialize();
        let mut end = start;
        end[end.len() - 1] += 1;
        let iter = self
            .kv_txn
            .scan(start, end)
            .map(|(_, v)| bincode::deserialize(&v).map_err(Into::into));
        Box::new(iter)
    }

    pub fn insert(&self, table: TableId, primary_key: &Value, columns: &[Value]) -> Result<()> {
        let mut key = table.serialize().to_vec();
        MemcomparableSerde::new().serialize_into(primary_key, &mut key);
        if self.kv_txn.insert(key, bincode::serialize(columns)?) {
            Ok(())
        } else {
            Err(Error::DuplicateKey(primary_key.clone()))
        }
    }

    pub fn update(&self, table: TableId, primary_key: &Value, columns: &[Value]) -> Result<()> {
        let mut key = table.serialize().to_vec();
        MemcomparableSerde::new().serialize_into(primary_key, &mut key);
        let updated = self.kv_txn.update(key, bincode::serialize(columns)?);
        assert!(updated);
        Ok(())
    }

    pub fn delete(&self, table: TableId, primary_key: &Value) {
        let mut key = table.serialize().to_vec();
        MemcomparableSerde::new().serialize_into(primary_key, &mut key);
        let deleted = self.kv_txn.remove(key).is_some();
        assert!(deleted);
    }

    pub fn commit(self) {
        self.kv_txn.commit();
    }
}
