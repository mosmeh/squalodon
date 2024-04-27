mod blackhole;
mod memory;

pub use blackhole::Blackhole;
pub use memory::Memory;

use crate::{
    catalog::{Constraint, Index, ObjectId, Table},
    memcomparable::MemcomparableSerde,
    Row, Value,
};
use std::ops::{Bound, RangeBounds};

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Duplicate key")]
    DuplicateKey,

    #[error("NOT NULL constraint violated at column {0:?}")]
    NotNullConstraintViolation(String),

    #[error("Invalid encoding")]
    InvalidEncoding,

    #[error("Storage is in an inconsistent state")]
    Inconsistent,

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
    fn prefix_range_scan<R: RangeBounds<Vec<u8>>>(
        &self,
        prefix: Vec<u8>,
        range: R,
    ) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + '_;
}

impl<T: ?Sized + Transaction> TransactionExt for T {
    fn prefix_scan(&self, prefix: Vec<u8>) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + '_ {
        let mut end = prefix.clone();
        if let Some(last) = end.last_mut() {
            *last += 1;
        }
        self.scan(prefix, end)
    }

    fn prefix_range_scan<R: RangeBounds<Vec<u8>>>(
        &self,
        prefix: Vec<u8>,
        range: R,
    ) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + '_ {
        let mut start = prefix.clone();
        if let Bound::Included(s) | Bound::Excluded(s) = range.start_bound() {
            start.extend_from_slice(s);
        }
        if let Bound::Excluded(_) = range.start_bound() {
            if let Some(last) = start.last_mut() {
                *last += 1;
            }
        }

        let mut end = prefix;
        if let Bound::Included(e) | Bound::Excluded(e) = range.end_bound() {
            end.extend_from_slice(e);
        }
        if let Bound::Included(_) | Bound::Unbounded = range.end_bound() {
            if let Some(last) = end.last_mut() {
                *last += 1;
            }
        }

        self.scan(start, end)
    }
}

impl<'a> Table<'a> {
    pub fn scan(&self) -> impl Iterator<Item = StorageResult<Row>> + 'a {
        let row_key_prefix = self.id().serialize();
        self.transaction()
            .prefix_scan(row_key_prefix)
            .map(|(_, v)| bincode::deserialize(&v).map_err(Into::into))
    }

    pub fn insert<'r, R: Into<&'r [Value]>>(&self, row: R) -> StorageResult<()> {
        let row = row.into();
        let row_key = self.prepare_for_write(row)?;
        if !self
            .transaction()
            .insert(&row_key, &bincode::serialize(row)?)
        {
            return Err(StorageError::DuplicateKey);
        }
        self.update_indexes(None, Some((row, &row_key)))
    }

    pub fn update<'r1, 'r2, R, S>(&self, old_row: R, new_row: S) -> StorageResult<()>
    where
        R: Into<&'r1 [Value]>,
        S: Into<&'r2 [Value]>,
    {
        let old_row = old_row.into();
        let old_row_key = self.prepare_for_write(old_row)?;
        let new_row = new_row.into();
        let new_row_key = self.prepare_for_write(new_row)?;

        let serialized = bincode::serialize(new_row)?;
        if new_row_key == old_row_key {
            let new = self.transaction().insert(&new_row_key, &serialized);
            assert!(!new);
        } else {
            self.transaction()
                .remove(&old_row_key)
                .ok_or(StorageError::Inconsistent)?;
            if !self.transaction().insert(&new_row_key, &serialized) {
                return Err(StorageError::DuplicateKey);
            }
        }

        self.update_indexes(Some((old_row, &old_row_key)), Some((new_row, &new_row_key)))
    }

    pub fn delete<'r, R: Into<&'r [Value]>>(&self, row: R) -> StorageResult<()> {
        let row = row.into();
        let row_key = self.prepare_for_write(row)?;
        self.transaction()
            .remove(&row_key)
            .ok_or(StorageError::Inconsistent)?;
        self.update_indexes(Some((row, &row_key)), None)
    }

    pub fn truncate(&self) -> StorageResult<()> {
        let txn = self.transaction();
        for row in self.scan() {
            let row = row?;
            let row_key = self.prepare_for_write(&row.0)?;
            txn.remove(&row_key).ok_or(StorageError::Inconsistent)?;
        }
        for index in self.indexes() {
            index.clear()?;
        }
        Ok(())
    }

    pub fn reindex(&self) -> StorageResult<()> {
        for index in self.indexes() {
            index.reindex()?;
        }
        Ok(())
    }

    /// Performs integrity checks before writing a row to the storage.
    ///
    /// Returns the serialized row key if the row passes the checks.
    fn prepare_for_write(&self, row: &[Value]) -> StorageResult<Vec<u8>> {
        assert_eq!(row.len(), self.columns().len());
        for (value, column) in row.iter().zip(self.columns()) {
            assert!(value.ty().is_compatible_with(column.ty));
        }
        for constraint in self.constraints() {
            match constraint {
                Constraint::NotNull(column) => {
                    if matches!(row[column.0], Value::Null) {
                        return Err(StorageError::NotNullConstraintViolation(
                            self.columns()[column.0].name.clone(),
                        ));
                    }
                }
            }
        }

        let serde = MemcomparableSerde::new();
        let mut row_key = self.id().serialize();
        for column in self.primary_keys() {
            serde.serialize_into(&row[column.0], &mut row_key);
        }
        Ok(row_key)
    }

    fn update_indexes(
        &self,
        old: Option<(&[Value], &[u8])>,
        new: Option<(&[Value], &[u8])>,
    ) -> StorageResult<()> {
        for index in self.indexes() {
            if let Some((row, row_key)) = old {
                index.remove(row, row_key)?;
            }
            if let Some((row, row_key)) = new {
                index.insert(row, row_key)?;
            }
        }
        Ok(())
    }
}

impl<'a> Index<'a> {
    /// Scans the index in `range`, returning the table rows.
    pub fn scan<'r, R: RangeBounds<&'r [Value]>>(
        &self,
        range: R,
    ) -> impl Iterator<Item = StorageResult<Row>> + 'a {
        let txn = self.transaction();
        self.scan_inner(range).map(move |row| {
            let (_, row_key) = row?;
            let bytes = txn.get(&row_key).ok_or(StorageError::Inconsistent)?;
            bincode::deserialize(&bytes).map_err(Into::into)
        })
    }

    /// Scans the index in `range`, returning the indexed columns.
    pub fn scan_index_only<'r, R: RangeBounds<&'r [Value]>>(
        &self,
        range: R,
    ) -> impl Iterator<Item = StorageResult<Vec<Value>>> + 'a {
        self.scan_inner(range).map(move |row| {
            let (indexed, _) = row?;
            Ok(indexed)
        })
    }

    /// Scans the index in `range`, returning (indexed column values, row key)
    fn scan_inner<'r, R: RangeBounds<&'r [Value]>>(
        &self,
        range: R,
    ) -> impl Iterator<Item = StorageResult<(Vec<Value>, Vec<u8>)>> + 'a {
        fn encode_key(key: &[Value]) -> Vec<u8> {
            let serde = MemcomparableSerde::new();
            let mut buf = Vec::new();
            for value in key {
                serde.serialize_into(value, &mut buf);
            }
            buf
        }

        let start = range.start_bound().map(|v| {
            assert_eq!(v.len(), self.column_indexes().len());
            encode_key(v)
        });
        let end = range.end_bound().map(|v| {
            assert_eq!(v.len(), self.column_indexes().len());
            encode_key(v)
        });

        let is_unique = self.is_unique();
        let column_indexes = self.column_indexes().to_vec();
        self.transaction()
            .prefix_range_scan(self.id().serialize(), (start, end))
            .map(move |(k, v)| {
                let serde = MemcomparableSerde::new();
                let mut indexed = Vec::with_capacity(column_indexes.len());
                let mut slice = &k[ObjectId::SERIALIZED_LEN..];
                for _ in &column_indexes {
                    let (value, len) = serde
                        .deserialize_from(slice)
                        .map_err(|_| StorageError::InvalidEncoding)?;
                    indexed.push(value);
                    slice = &slice[len..];
                }

                let row_key = if is_unique {
                    assert!(slice.is_empty());
                    v
                } else {
                    assert!(v.is_empty());
                    slice.to_vec()
                };
                Ok((indexed, row_key))
            })
    }

    fn insert(&self, row: &[Value], row_key: &[u8]) -> StorageResult<()> {
        let key = self.key(row, row_key);
        let value = if self.is_unique() {
            row_key.to_vec()
        } else {
            // For non-unique indexes, the row key is a part of the key of
            // the index.
            Vec::new()
        };
        if self.transaction().insert(&key, &value) {
            Ok(())
        } else {
            Err(StorageError::DuplicateKey)
        }
    }

    fn remove(&self, row: &[Value], row_key: &[u8]) -> StorageResult<()> {
        let key = self.key(row, row_key);
        match self.transaction().remove(&key) {
            Some(_) => Ok(()),
            None => Err(StorageError::Inconsistent),
        }
    }

    pub fn reindex(&self) -> StorageResult<()> {
        self.clear()?;
        let table = self.table();
        for row in table.scan() {
            let row = row?;
            let row_key = table.prepare_for_write(&row.0)?;
            self.insert(&row.0, &row_key)?;
        }
        Ok(())
    }

    fn clear(&self) -> StorageResult<()> {
        let prefix = self.id().serialize();
        let txn = self.transaction();
        for (k, _) in txn.prefix_scan(prefix) {
            txn.remove(&k).ok_or(StorageError::Inconsistent)?;
        }
        Ok(())
    }

    fn key(&self, row: &[Value], row_key: &[u8]) -> Vec<u8> {
        let serde = MemcomparableSerde::new();
        let mut key = self.id().serialize();
        for column_index in self.column_indexes() {
            serde.serialize_into(&row[column_index.0], &mut key);
        }
        if !self.is_unique() {
            // If the indexed columns are not unique, append the row key
            // to make the key unique.
            key.extend_from_slice(row_key);
        }
        key
    }
}
