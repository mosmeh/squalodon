mod memory;
mod schema;

pub use memory::Memory;
pub use schema::{Column, Table, TableId};
pub use Error as StorageError;

use crate::{memcomparable::MemcomparableSerde, Value};
use std::sync::atomic::AtomicU64;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Unknown table {0:?}")]
    UnknownTable(String),

    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait KeyValueStore {
    type Transaction<'a>: KeyValueTransaction + 'a
    where
        Self: 'a;

    fn transaction(&self) -> Self::Transaction<'_>;
}

pub trait KeyValueTransaction {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
    fn scan<const N: usize>(
        &self,
        start: [u8; N],
        end: [u8; N],
    ) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)>;
    fn insert(&self, key: Vec<u8>, value: Vec<u8>);
    fn remove(&self, key: Vec<u8>);
    fn commit(self);
}

pub struct Storage<T> {
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

pub struct Transaction<'a, T: KeyValueStore> {
    storage: &'a Storage<T>,
    kv_txn: T::Transaction<'a>,
}

impl<'a, T: KeyValueStore> Transaction<'a, T> {
    pub fn create_table(&self, name: String, columns: Vec<Column>) -> Result<TableId> {
        let id = self
            .storage
            .next_table_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let id = TableId(id);

        let mut key = TableId::CATALOG.serialize().to_vec();
        key.extend_from_slice(name.as_bytes());

        let table = Table { id, name, columns };
        self.kv_txn.insert(key, bincode::serialize(&table)?);

        Ok(id)
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

    pub fn update(&self, table: TableId, primary_key: &Value, columns: &[Value]) -> Result<()> {
        let mut key = table.serialize().to_vec();
        MemcomparableSerde::new().serialize_into(primary_key, &mut key);
        self.kv_txn.insert(key, bincode::serialize(columns)?);
        Ok(())
    }

    pub fn delete(&self, table: TableId, primary_key: &Value) {
        let mut key = table.serialize().to_vec();
        MemcomparableSerde::new().serialize_into(primary_key, &mut key);
        self.kv_txn.remove(key);
    }

    pub fn commit(self) {
        self.kv_txn.commit();
    }
}
