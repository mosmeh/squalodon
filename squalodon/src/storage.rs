mod blackhole;
mod memory;

pub use blackhole::Blackhole;
pub use memory::Memory;

use crate::{
    catalog::{Constraint, Index, Table},
    memcomparable::MemcomparableSerde,
    Row, Value,
};

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
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
    fn scan(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + '_>;

    /// Inserts a key-value pair.
    ///
    /// Returns true if the key was newly inserted, false if it already existed.
    #[must_use]
    fn insert(&self, key: &[u8], value: &[u8]) -> bool;

    /// Removes a key from the storage.
    ///
    /// Returns the value if the key was removed, None if it did not exist.
    #[must_use]
    fn remove(&self, key: &[u8]) -> Option<Vec<u8>>;

    /// Commits the transaction.
    ///
    /// The transaction rolls back if it is dropped without being committed.
    fn commit(self);
}

pub(crate) trait TransactionExt {
    fn prefix_scan(&self, prefix: Vec<u8>) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + '_;
}

impl<T: ?Sized + Transaction> TransactionExt for T {
    fn prefix_scan(&self, prefix: Vec<u8>) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + '_ {
        let mut end = prefix.clone();
        if let Some(last) = end.last_mut() {
            *last += 1;
        }
        self.scan(prefix, end)
    }
}

impl<'a> Table<'a> {
    pub fn scan(&self) -> impl Iterator<Item = StorageResult<Row>> + 'a {
        let prefix = self.id().serialize();
        self.transaction()
            .prefix_scan(prefix)
            .map(|(_, v)| bincode::deserialize(&v).map_err(Into::into))
    }

    pub fn insert<'r, R: Into<&'r [Value]>>(&self, row: R) -> StorageResult<()> {
        let row = row.into();
        let key = self.prepare_for_write(row)?;
        if !self.transaction().insert(&key, &bincode::serialize(row)?) {
            return Err(StorageError::DuplicateKey);
        }
        self.update_indexes(None, Some((row, &key)))
    }

    pub fn update<'r1, 'r2, R, S>(&self, old_row: R, new_row: S) -> StorageResult<()>
    where
        R: Into<&'r1 [Value]>,
        S: Into<&'r2 [Value]>,
    {
        let old_row = old_row.into();
        let old_key = self.prepare_for_write(old_row)?;
        let new_row = new_row.into();
        let new_key = self.prepare_for_write(new_row)?;

        let serialized = bincode::serialize(new_row)?;
        if new_key == old_key {
            let new = self.transaction().insert(&new_key, &serialized);
            assert!(!new);
        } else {
            self.transaction().remove(&old_key).unwrap();
            if !self.transaction().insert(&new_key, &serialized) {
                return Err(StorageError::DuplicateKey);
            }
        }

        self.update_indexes(Some((old_row, &old_key)), Some((new_row, &new_key)))
    }

    pub fn delete<'r, R: Into<&'r [Value]>>(&self, row: R) -> StorageResult<()> {
        let row = row.into();
        let key = self.prepare_for_write(row)?;
        self.transaction().remove(&key).unwrap();
        self.update_indexes(Some((row, &key)), None)
    }

    /// Performs integrity checks before writing a row to the storage.
    ///
    /// Returns the serialized key if the row passes the checks.
    fn prepare_for_write(&self, row: &[Value]) -> StorageResult<Vec<u8>> {
        assert_eq!(row.len(), self.columns().len());
        for (value, column) in row.iter().zip(self.columns()) {
            assert!(value.ty().is_compatible_with(column.ty));
        }
        let mut key = self.id().serialize();
        let mut has_primary_key = false;
        for constraint in self.constraints() {
            match constraint {
                Constraint::PrimaryKey(columns) => {
                    assert!(!has_primary_key);
                    has_primary_key = true;
                    let serde = MemcomparableSerde::new();
                    for column in columns {
                        serde.serialize_into(&row[column.0], &mut key);
                    }
                    // Uniqueness of primary key is checked when inserting
                }
                Constraint::NotNull(column) => {
                    if matches!(row[column.0], Value::Null) {
                        return Err(StorageError::NotNullConstraintViolation(
                            self.columns()[column.0].name.clone(),
                        ));
                    }
                }
            }
        }
        assert!(has_primary_key);
        Ok(key)
    }

    fn update_indexes(
        &self,
        old: Option<(&[Value], &[u8])>,
        new: Option<(&[Value], &[u8])>,
    ) -> StorageResult<()> {
        for index in self.indexes() {
            let old_index_key = old
                .map(|(row, table_key)| index.encode_key(row, table_key))
                .unwrap_or_default();
            let new_index_key = new
                .map(|(row, table_key)| index.encode_key(row, table_key))
                .unwrap_or_default();
            if old_index_key == new_index_key {
                // No change in the indexed columns
                continue;
            }
            if old.is_some() {
                self.transaction().remove(&old_index_key).unwrap();
            }
            if let Some((_, new_table_key)) = new {
                let value = if index.is_unique() {
                    new_table_key.to_vec()
                } else {
                    // For non-unique indexes, the table key is a part of
                    // the index key.
                    Vec::new()
                };
                if !self.transaction().insert(&new_index_key, &value) {
                    return Err(StorageError::DuplicateKey);
                }
            }
        }
        Ok(())
    }
}

impl Index {
    fn encode_key(&self, row: &[Value], table_key: &[u8]) -> Vec<u8> {
        let serde = MemcomparableSerde::new();
        let mut index_key = Vec::new();
        for column_index in self.column_indexes() {
            serde.serialize_into(&row[column_index.0], &mut index_key);
        }
        if !self.is_unique() {
            // If the indexed columns are not unique, append the primary key
            // to make the key unique.
            index_key.extend_from_slice(table_key);
        }
        index_key
    }
}
