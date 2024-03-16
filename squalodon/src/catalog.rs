use crate::{
    builtin,
    connection::QueryContext,
    executor::ExecutorResult,
    planner,
    storage::{KeyValueStore, KeyValueTransaction},
    types::Type,
    Transaction, Value,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::atomic::AtomicU64};

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("Unknown {0} {1:?}")]
    UnknownEntry(CatalogEntryKind, String),

    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
}

pub type CatalogResult<T> = std::result::Result<T, CatalogError>;

#[derive(Debug)]
pub enum CatalogEntryKind {
    Table,
    TableFunction,
}

impl std::fmt::Display for CatalogEntryKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Table => "table",
            Self::TableFunction => "table function",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableId(pub u64);

impl TableId {
    pub const CATALOG: Self = Self(0);
    pub const MAX_SYSTEM: Self = Self::CATALOG;

    const ENCODED_LEN: usize = std::mem::size_of::<u64>();

    pub fn serialize(self) -> [u8; Self::ENCODED_LEN] {
        let mut buf = [0; Self::ENCODED_LEN];
        self.serialize_into(&mut buf);
        buf
    }

    pub fn serialize_into(self, buf: &mut [u8; Self::ENCODED_LEN]) {
        buf.copy_from_slice(&self.0.to_be_bytes());
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, Serialize)]
struct TableRef<'a> {
    id: TableId,
    name: &'a str,
    columns: &'a [Column],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub ty: Type,
    pub is_primary_key: bool,
    pub is_nullable: bool,
}

pub struct TableFunction<T: KeyValueStore> {
    pub fn_ptr: TableFnPtr<T>,
    pub result_columns: Vec<planner::Column>,
}

impl<T: KeyValueStore> Clone for TableFunction<T> {
    fn clone(&self) -> Self {
        Self {
            fn_ptr: self.fn_ptr,
            result_columns: self.result_columns.clone(),
        }
    }
}

pub type TableFnPtr<T> = for<'a> fn(
    &QueryContext<'_, 'a, T>,
    &[Value],
) -> ExecutorResult<Box<dyn Iterator<Item = Vec<Value>>>>;

pub struct Catalog<T: KeyValueStore> {
    next_table_id: AtomicU64,
    table_functions: HashMap<String, TableFunction<T>>,
}

impl<T: KeyValueStore> Catalog<T> {
    pub fn load(kvs: &T) -> CatalogResult<Self> {
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
            next_table_id: (max_table_id + 1).into(),
            table_functions: builtin::load_table_functions().collect(),
        })
    }

    pub fn with<'txn, 'db>(&'db self, txn: &'txn Transaction<'db, T>) -> CatalogRef<'txn, 'db, T> {
        CatalogRef {
            catalog: self,
            txn: txn.raw(),
        }
    }
}

pub struct CatalogRef<'txn, 'db, T: KeyValueStore + 'db> {
    catalog: &'txn Catalog<T>,
    txn: &'txn T::Transaction<'db>,
}

impl<'txn, 'db, T: KeyValueStore> CatalogRef<'txn, 'db, T> {
    pub fn table(&self, name: &str) -> CatalogResult<Table> {
        let mut key = TableId::CATALOG.serialize().to_vec();
        key.extend_from_slice(name.as_bytes());
        match self.txn.get(&key) {
            Some(data) => Ok(bincode::deserialize(&data)?),
            None => Err(CatalogError::UnknownEntry(
                CatalogEntryKind::Table,
                name.to_owned(),
            )),
        }
    }

    pub fn tables(&self) -> Box<dyn Iterator<Item = CatalogResult<Table>> + '_> {
        let start = TableId::CATALOG.serialize();
        let mut end = start;
        end[end.len() - 1] += 1;
        let iter = self
            .txn
            .scan(start, end)
            .map(|(_, v)| bincode::deserialize(&v).map_err(Into::into));
        Box::new(iter)
    }

    pub fn create_table(&self, name: &str, columns: &[Column]) -> CatalogResult<TableId> {
        let id = self
            .catalog
            .next_table_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let id = TableId(id);

        let mut key = TableId::CATALOG.serialize().to_vec();
        key.extend_from_slice(name.as_bytes());

        let table = TableRef { id, name, columns };
        let inserted = self.txn.insert(key, bincode::serialize(&table)?);
        assert!(inserted);

        Ok(id)
    }

    pub fn drop_table(&self, name: &str) -> CatalogResult<()> {
        let mut key = TableId::CATALOG.serialize().to_vec();
        key.extend_from_slice(name.as_bytes());
        let bytes = self.txn.remove(key).unwrap();
        let table: Table = bincode::deserialize(&bytes)?;
        assert_eq!(table.name, name);
        let start = table.id.serialize();
        let mut end = start;
        end[end.len() - 1] += 1;
        for (key, _) in self.txn.scan(start, end) {
            self.txn.remove(key).unwrap();
        }
        Ok(())
    }

    pub fn table_function(&self, name: &str) -> CatalogResult<TableFunction<T>> {
        self.catalog
            .table_functions
            .get(name)
            .cloned()
            .ok_or_else(|| {
                CatalogError::UnknownEntry(CatalogEntryKind::TableFunction, name.to_owned())
            })
    }
}
