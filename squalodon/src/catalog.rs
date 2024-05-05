mod function;
mod index;
mod table;

pub use function::{
    AggregateFunction, AggregateInitFnPtr, Aggregator, BoxedScalarFn, BoxedTableFn, Function,
    ScalarFunction, ScalarImpureFn, TableFn, TableFunction, TypedAggregator,
};
pub use index::Index;
pub use table::{Column, Table};

use crate::{
    builtin,
    storage::{StorageError, Transaction, TransactionExt},
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("Duplicate {0} {1:?}")]
    DuplicateEntry(CatalogEntryKind, String),

    #[error("Unknown {0} {1:?}")]
    UnknownEntry(CatalogEntryKind, String),

    #[error("Invalid encoding")]
    InvalidEncoding,

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
}

pub type CatalogResult<T> = std::result::Result<T, CatalogError>;

#[derive(Debug, Clone, Copy)]
pub enum CatalogEntryKind {
    Metadata,
    Table,
    Index,
    ScalarFunction,
    AggregateFunction,
    TableFunction,
}

impl std::fmt::Display for CatalogEntryKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(match self {
            Self::Metadata => "metadata",
            Self::Table => "table",
            Self::Index => "index",
            Self::ScalarFunction => "scalar function",
            Self::AggregateFunction => "aggregate function",
            Self::TableFunction => "table function",
        })
    }
}

impl CatalogEntryKind {
    fn key(self, name: &str) -> Vec<u8> {
        let mut key = ObjectId::CATALOG.serialize();
        key.extend_from_slice(&(self as u64).to_be_bytes());
        key.extend_from_slice(name.as_bytes());
        key
    }

    fn prefix(self) -> Vec<u8> {
        self.key("")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectId(u64);

impl ObjectId {
    pub const SERIALIZED_LEN: usize = std::mem::size_of::<u64>();

    const CATALOG: Self = Self(0);

    pub fn serialize(self) -> Vec<u8> {
        self.0.to_be_bytes().to_vec()
    }
}

pub struct Catalog {
    scalar_functions: HashMap<&'static str, Vec<ScalarFunction>>,
    aggregate_functions: HashMap<&'static str, Vec<AggregateFunction>>,
    table_functions: HashMap<&'static str, Vec<TableFunction>>,
}

impl Default for Catalog {
    fn default() -> Self {
        let mut scalar_functions = HashMap::new();
        for f in builtin::scalar_function::load() {
            scalar_functions
                .entry(f.name)
                .or_insert_with(Vec::new)
                .push(f);
        }

        let mut aggregate_functions = HashMap::new();
        for f in builtin::aggregate_function::load() {
            aggregate_functions
                .entry(f.name)
                .or_insert_with(Vec::new)
                .push(f);
        }

        let mut table_functions = HashMap::new();
        for f in builtin::table_function::load() {
            table_functions
                .entry(f.name)
                .or_insert_with(Vec::new)
                .push(f);
        }

        Self {
            scalar_functions,
            aggregate_functions,
            table_functions,
        }
    }
}

impl Catalog {
    pub fn with<'a>(&'a self, txn: &'a dyn Transaction) -> CatalogRef<'a> {
        CatalogRef { catalog: self, txn }
    }
}

#[derive(Clone)]
pub struct CatalogRef<'a> {
    catalog: &'a Catalog,
    txn: &'a dyn Transaction,
}

impl<'a> CatalogRef<'a> {
    fn entry<T: DeserializeOwned>(&self, kind: CatalogEntryKind, name: &str) -> CatalogResult<T> {
        let key = kind.key(name);
        let bytes = self
            .txn
            .get(&key)
            .ok_or_else(|| CatalogError::UnknownEntry(kind, name.to_owned()))?;
        bincode::deserialize(&bytes).map_err(Into::into)
    }

    fn entries<T: DeserializeOwned>(
        &self,
        kind: CatalogEntryKind,
    ) -> impl Iterator<Item = CatalogResult<T>> + '_ {
        let prefix = kind.prefix();
        self.txn
            .prefix_scan(prefix)
            .map(|(_, v)| bincode::deserialize(&v).map_err(Into::into))
    }

    fn insert_entry<T: Serialize>(
        &self,
        kind: CatalogEntryKind,
        name: &str,
        value: &T,
    ) -> CatalogResult<()> {
        let key = kind.key(name);
        let value = bincode::serialize(value)?;
        if self.txn.insert(&key, &value) {
            Ok(())
        } else {
            Err(CatalogError::DuplicateEntry(kind, name.to_owned()))
        }
    }

    fn put_entry<T: Serialize>(
        &self,
        kind: CatalogEntryKind,
        name: &str,
        value: &T,
    ) -> CatalogResult<()> {
        let key = kind.key(name);
        let value = bincode::serialize(value)?;
        let _ = self.txn.insert(&key, &value);
        Ok(())
    }

    fn remove_entry(&self, kind: CatalogEntryKind, name: &str) -> CatalogResult<()> {
        let key = kind.key(name);
        self.txn
            .remove(&key)
            .ok_or_else(|| CatalogError::UnknownEntry(kind, name.to_owned()))?;
        Ok(())
    }

    fn generate_object_id(&self) -> CatalogResult<ObjectId> {
        const NAME: &str = "next_id";
        let mut next_id: u64 = match self.entry(CatalogEntryKind::Metadata, NAME) {
            Ok(id) => id,
            Err(CatalogError::UnknownEntry(_, _)) => 1,
            Err(e) => return Err(e),
        };
        assert!(next_id > ObjectId::CATALOG.0);
        let id = ObjectId(next_id);
        next_id += 1;
        self.put_entry(CatalogEntryKind::Metadata, NAME, &next_id)?;
        Ok(id)
    }
}
