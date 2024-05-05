use super::{StorageResult, TransactionExt};
use crate::{catalog::Table, memcomparable::MemcomparableSerde, Row, StorageError, Value};

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
    pub(super) fn prepare_for_write(&self, row: &[Value]) -> StorageResult<Vec<u8>> {
        assert_eq!(row.len(), self.columns().len());
        for (value, column) in row.iter().zip(self.columns()) {
            assert!(value.ty().is_compatible_with(column.ty));
        }
        for (value, column) in row.iter().zip(self.columns()) {
            if column.is_nullable {
                continue;
            }
            if value.is_null() {
                return Err(StorageError::NotNullConstraintViolation(
                    column.name.clone(),
                ));
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
