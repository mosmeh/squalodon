use super::StorageResult;
use crate::{
    catalog::{Index, ObjectId},
    memcomparable::MemcomparableSerde,
    storage::TransactionExt,
    Row, StorageError, Value,
};
use std::ops::RangeBounds;

impl<'a> Index<'a> {
    /// Scans the index in `range`, returning the table rows.
    pub fn scan<'r, R: RangeBounds<&'r [Value]>>(
        &self,
        range: R,
    ) -> impl Iterator<Item = StorageResult<Row>> + 'a {
        let txn = self.transaction();
        self.scan_inner(range).map(move |row| {
            let (_, row_key) = row?;
            let bytes = txn
                .get(&row_key)
                .map_err(StorageError::Backend)?
                .ok_or(StorageError::Inconsistent)?;
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
        let num_columns = self.column_indexes().len();
        self.transaction()
            .prefix_range_scan(self.id().serialize(), (start, end))
            .map(move |r| {
                let (k, v) = r.map_err(StorageError::Backend)?;
                let serde = MemcomparableSerde::new();
                let mut indexed = Vec::with_capacity(num_columns);
                let mut slice = &k[ObjectId::SERIALIZED_LEN..];
                for _ in 0..num_columns {
                    let (value, len) = serde
                        .deserialize_from(slice)
                        .map_err(|_| StorageError::InvalidEncoding)?;
                    indexed.push(value);
                    slice = &slice[len..];
                }

                let row_key = if is_unique {
                    if !slice.is_empty() {
                        return Err(StorageError::InvalidEncoding);
                    }
                    v
                } else {
                    if !v.is_empty() {
                        return Err(StorageError::InvalidEncoding);
                    }
                    slice.to_vec()
                };
                Ok((indexed, row_key))
            })
    }

    pub(super) fn insert(&self, row: &[Value], row_key: &[u8]) -> StorageResult<()> {
        let key = self.key(row, row_key);
        let value = if self.is_unique() {
            row_key.to_vec()
        } else {
            // For non-unique indexes, the row key is a part of the key of
            // the index.
            Vec::new()
        };
        if self
            .transaction()
            .insert(&key, &value)
            .map_err(StorageError::Backend)?
        {
            Ok(())
        } else {
            Err(StorageError::DuplicateKey)
        }
    }

    pub(super) fn remove(&self, row: &[Value], row_key: &[u8]) -> StorageResult<()> {
        let key = self.key(row, row_key);
        match self
            .transaction()
            .remove(&key)
            .map_err(StorageError::Backend)?
        {
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

    pub fn clear(&self) -> StorageResult<()> {
        let prefix = self.id().serialize();
        let txn = self.transaction();
        for r in txn.prefix_scan(prefix) {
            let (k, _) = r.map_err(StorageError::Backend)?;
            txn.remove(&k)
                .map_err(StorageError::Backend)?
                .ok_or(StorageError::Inconsistent)?;
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
