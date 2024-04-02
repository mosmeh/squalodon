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
    indexes: Vec<Index<'txn, 'db, T>>,
}

impl<T: Storage> Clone for Table<'_, '_, T> {
    fn clone(&self) -> Self {
        Self {
            txn: self.txn,
            name: self.name.clone(),
            def: self.def.clone(),
            indexes: self.indexes.clone(),
        }
    }
}

impl<'txn, 'db, T: Storage> Table<'txn, 'db, T> {
    pub fn new(
        txn: &'txn T::Transaction<'db>,
        name: String,
        def: catalog::Table,
        indexes: Vec<catalog::Index>,
    ) -> Self {
        let indexes = indexes
            .into_iter()
            .zip(def.index_names.iter())
            .map(|(def, name)| Index::new(txn, name.clone(), def))
            .collect();
        Self {
            txn,
            name,
            def,
            indexes,
        }
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

    pub fn indexes(&self) -> &[Index<'txn, 'db, T>] {
        &self.indexes
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

    pub fn insert<'a, R: Into<&'a [Value]>>(&self, row: R) -> StorageResult<()> {
        let row = row.into();
        let key = self.prepare_for_write(row)?;
        if !self.txn.insert(key.clone(), bincode::serialize(row)?) {
            return Err(StorageError::DuplicateKey);
        }
        self.update_indexes(None, Some((row, &key)))
    }

    pub fn update<'a, 'b, R, S>(&self, old_row: R, new_row: S) -> StorageResult<()>
    where
        R: Into<&'a [Value]>,
        S: Into<&'b [Value]>,
    {
        let old_row = old_row.into();
        let old_key = self.prepare_for_write(old_row)?;
        let new_row = new_row.into();
        let new_key = self.prepare_for_write(new_row)?;

        self.txn.remove(old_key.clone()).unwrap();
        let inserted = self
            .txn
            .insert(new_key.clone(), bincode::serialize(new_row)?);
        assert!(inserted);

        self.update_indexes(Some((old_row, &old_key)), Some((new_row, &new_key)))
    }

    pub fn delete<'a, R: Into<&'a [Value]>>(&self, row: R) -> StorageResult<()> {
        let row = row.into();
        let key = self.prepare_for_write(row)?;
        self.txn.remove(key.clone()).unwrap();
        self.update_indexes(Some((row, &key)), None)
    }

    /// Performs integrity checks before writing a row to the storage.
    ///
    /// Returns the serialized key if the row passes the checks.
    fn prepare_for_write(&self, row: &[Value]) -> StorageResult<Vec<u8>> {
        assert_eq!(row.len(), self.def.columns.len());
        for (value, column) in row.iter().zip(&self.def.columns) {
            assert!(value.ty().is_compatible_with(column.ty));
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
                        serde.serialize_into(&row[column.0], &mut key);
                    }
                    // Uniqueness of primary key is checked when inserting
                }
                Constraint::NotNull(column) => {
                    if matches!(row[column.0], Value::Null) {
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

    fn update_indexes(
        &self,
        old: Option<(&[Value], &[u8])>,
        new: Option<(&[Value], &[u8])>,
    ) -> StorageResult<()> {
        for index in &self.indexes {
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
                self.txn.remove(old_index_key).unwrap();
            }
            if let Some((_, new_table_key)) = new {
                let value = if index.is_unique() {
                    new_table_key.to_vec()
                } else {
                    // For non-unique indexes, the table key is a part of
                    // the index key.
                    Vec::new()
                };
                if !self.txn.insert(new_index_key, value) {
                    return Err(StorageError::DuplicateKey);
                }
            }
        }
        Ok(())
    }
}

pub(crate) struct Index<'txn, 'db, T: Storage + 'db> {
    txn: &'txn T::Transaction<'db>,
    name: String,
    def: catalog::Index,
}

impl<T: Storage> Clone for Index<'_, '_, T> {
    fn clone(&self) -> Self {
        Self {
            txn: self.txn,
            name: self.name.clone(),
            def: self.def.clone(),
        }
    }
}

impl<'txn, 'db, T: Storage> Index<'txn, 'db, T> {
    pub fn new(txn: &'txn T::Transaction<'db>, name: String, def: catalog::Index) -> Self {
        Self { txn, name, def }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn is_unique(&self) -> bool {
        self.def.is_unique
    }

    fn encode_key(&self, row: &[Value], table_key: &[u8]) -> Vec<u8> {
        let serde = MemcomparableSerde::new();
        let mut index_key = Vec::new();
        for column_index in &self.def.column_indexes {
            serde.serialize_into(&row[column_index.0], &mut index_key);
        }
        if !self.def.is_unique {
            // If the indexed columns are not unique, append the primary key
            // to make the key unique.
            index_key.extend_from_slice(table_key);
        }
        index_key
    }
}
