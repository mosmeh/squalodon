mod blackhole;
mod memory;

pub use blackhole::Blackhole;
pub use memory::Memory;

use crate::{
    catalog::{self, Column, Constraint},
    memcomparable::MemcomparableSerde,
    Row, Value,
};

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Unknown table {0:?}")]
    UnknownTable(String),

    #[error("Duplicate key")]
    DuplicateKey,

    #[error("NOT NULL constraint violated at column {0:?}")]
    NotNullConstraintViolation(String),

    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
}

pub(crate) type StorageResult<T> = std::result::Result<T, StorageError>;

pub trait Storage {
    type Transaction<'a>: Transaction
    where
        Self: 'a;

    fn transaction(&self) -> Self::Transaction<'_>;
}

pub trait Transaction {
    /// Returns the value associated with the key.
    ///
    /// Returns None if the key does not exist.
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;

    /// Returns an iterator over the key-value pairs
    /// in the key range `[start, end)`.
    fn scan<const N: usize>(
        &self,
        start: [u8; N],
        end: [u8; N],
    ) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)>;

    /// Inserts a key-value pair only if the key does not exist.
    ///
    /// Returns true if the key was inserted, false if it already existed.
    #[must_use]
    fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> bool;

    /// Removes a key from the storage.
    ///
    /// Returns the value if the key was removed, None if it did not exist.
    #[must_use]
    fn remove(&self, key: Vec<u8>) -> Option<Vec<u8>>;

    /// Commits the transaction.
    ///
    /// The transaction rolls back if it is dropped without being committed.
    fn commit(self);
}

pub(crate) struct Table<'txn, 'db, T: Storage + 'db> {
    txn: &'txn T::Transaction<'db>,
    name: String,
    def: catalog::Table,
}

impl<T: Storage> Clone for Table<'_, '_, T> {
    fn clone(&self) -> Self {
        Self {
            txn: self.txn,
            name: self.name.clone(),
            def: self.def.clone(),
        }
    }
}

impl<'txn, 'db, T: Storage> Table<'txn, 'db, T> {
    pub fn new(txn: &'txn T::Transaction<'db>, name: String, def: catalog::Table) -> Self {
        Self { txn, name, def }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[Column] {
        &self.def.columns
    }

    pub fn constraints(&self) -> &[Constraint] {
        &self.def.constraints
    }

    pub fn scan(&self) -> Box<dyn Iterator<Item = StorageResult<Row>> + 'txn> {
        let start = self.def.id.serialize();
        let mut end = start;
        end[end.len() - 1] += 1;
        let iter = self
            .txn
            .scan(start, end)
            .map(|(_, v)| bincode::deserialize(&v).map_err(Into::into));
        Box::new(iter)
    }

    pub fn insert(&self, row: &Row) -> StorageResult<()> {
        let key = self.prepare_for_write(row)?;
        if self.txn.insert(key, bincode::serialize(row)?) {
            Ok(())
        } else {
            Err(StorageError::DuplicateKey)
        }
    }

    pub fn update(&self, row: &Row) -> StorageResult<()> {
        let key = self.prepare_for_write(row)?;
        let removed = self.txn.remove(key.clone()).is_some();
        assert!(removed);
        let updated = self.txn.insert(key, bincode::serialize(row)?);
        assert!(updated);
        Ok(())
    }

    pub fn delete(&self, row: &Row) -> StorageResult<()> {
        let key = self.prepare_for_write(row)?;
        let removed = self.txn.remove(key).is_some();
        assert!(removed);
        Ok(())
    }

    /// Performs integrity checks before writing a row to the storage.
    ///
    /// Returns the serialized key if the row passes the checks.
    fn prepare_for_write(&self, row: &Row) -> StorageResult<Vec<u8>> {
        assert_eq!(row.columns().len(), self.def.columns.len());
        for (value, column) in row.columns().iter().zip(&self.def.columns) {
            if let Some(ty) = value.ty() {
                assert_eq!(ty, column.ty);
            }
        }
        let mut key = self.def.id.serialize().to_vec();
        let mut has_primary_key = false;
        for constraint in &self.def.constraints {
            match constraint {
                Constraint::PrimaryKey(columns) => {
                    assert!(!has_primary_key);
                    has_primary_key = true;
                    let serde = MemcomparableSerde::new();
                    for column in columns {
                        serde.serialize_into(&row[column], &mut key);
                    }
                    // Uniqueness of primary key is checked when inserting
                }
                Constraint::NotNull(column) => {
                    if matches!(row[column], Value::Null) {
                        return Err(StorageError::NotNullConstraintViolation(
                            self.def.columns[column.0].name.clone(),
                        ));
                    }
                }
            }
        }
        assert!(has_primary_key);
        Ok(key)
    }
}
