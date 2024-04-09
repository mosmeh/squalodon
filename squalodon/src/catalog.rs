use crate::{
    builtin,
    connection::ConnectionContext,
    executor::ExecutorResult,
    planner::{self, PlannerResult},
    rows::ColumnIndex,
    storage::{self, Storage, Transaction},
    types::{NullableType, Type},
    Row, Value,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::atomic::AtomicU64};

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("Duplicate {0} {1:?}")]
    DuplicateEntry(CatalogEntryKind, String),

    #[error("Unknown {0} {1:?}")]
    UnknownEntry(CatalogEntryKind, String),

    #[error("Invalid encoding")]
    InvalidEncoding,

    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
}

pub type CatalogResult<T> = std::result::Result<T, CatalogError>;

#[derive(Debug)]
pub enum CatalogEntryKind {
    Table,
    Index,
    ScalarFunction,
    AggregateFunction,
    TableFunction,
}

impl std::fmt::Display for CatalogEntryKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Table => "table",
            Self::Index => "index",
            Self::ScalarFunction => "scalar function",
            Self::AggregateFunction => "aggregate function",
            Self::TableFunction => "table function",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectId(pub u64);

impl ObjectId {
    pub const TABLE_CATALOG: Self = Self(0);
    pub const INDEX_CATALOG: Self = Self(1);
    pub const MAX_SYSTEM: Self = Self::INDEX_CATALOG;

    const SERIALIZED_LEN: usize = std::mem::size_of::<u64>();

    pub fn serialize(self) -> [u8; Self::SERIALIZED_LEN] {
        let mut buf = [0; Self::SERIALIZED_LEN];
        self.serialize_into(&mut buf);
        buf
    }

    pub fn serialize_into(self, buf: &mut [u8; Self::SERIALIZED_LEN]) {
        buf.copy_from_slice(&self.0.to_be_bytes());
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Table {
    pub id: ObjectId,
    pub columns: Vec<Column>,
    pub constraints: Vec<Constraint>,
    pub index_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub ty: Type,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Constraint {
    PrimaryKey(Vec<ColumnIndex>),
    NotNull(ColumnIndex),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Index {
    pub id: ObjectId,
    pub column_indexes: Vec<ColumnIndex>,
    pub is_unique: bool,
}

pub enum Function<'a, T: Storage> {
    Scalar(&'a ScalarFunction<T>),
    Aggregate(&'a AggregateFunction),
    Table(&'a TableFunction<T>),
}

impl<'a, T: Storage> Function<'a, T> {
    pub fn name(&self) -> &str {
        match self {
            Self::Scalar(f) => f.name,
            Self::Aggregate(f) => f.name,
            Self::Table(f) => f.name,
        }
    }
}

pub struct ScalarFunction<T: Storage> {
    pub name: &'static str,
    pub bind: ScalarBindFnPtr,
    pub eval: ScalarEvalFnPtr<T>,
}

pub type ScalarBindFnPtr = fn(&[NullableType]) -> PlannerResult<NullableType>;
pub type ScalarEvalFnPtr<T> = fn(&ConnectionContext<'_, '_, T>, &[Value]) -> ExecutorResult<Value>;

pub struct AggregateFunction {
    pub name: &'static str,
    pub bind: AggregateBindFnPtr,
    pub init: AggregateInitFnPtr,
}

pub type AggregateBindFnPtr = fn(NullableType) -> PlannerResult<NullableType>;
pub type AggregateInitFnPtr = fn() -> Box<dyn Aggregator>;

pub trait Aggregator {
    fn update(&mut self, value: &Value) -> ExecutorResult<()>;
    fn finish(&self) -> Value;
}

pub struct TableFunction<T: Storage> {
    pub name: &'static str,
    pub fn_ptr: TableFnPtr<T>,
    pub result_columns: Vec<planner::Column>,
}

pub type TableFnPtr<T> = for<'a> fn(
    &'a ConnectionContext<'a, '_, T>,
    &Row,
) -> ExecutorResult<Box<dyn Iterator<Item = Row> + 'a>>;

pub struct Catalog<T: Storage> {
    next_object_id: AtomicU64,
    scalar_functions: HashMap<&'static str, ScalarFunction<T>>,
    aggregate_functions: HashMap<&'static str, AggregateFunction>,
    table_functions: HashMap<&'static str, TableFunction<T>>,
}

impl<T: Storage> Catalog<T> {
    pub fn load(storage: &T) -> CatalogResult<Self> {
        let mut max_object_id = ObjectId::MAX_SYSTEM.0;

        let txn = storage.transaction();

        let start = ObjectId::TABLE_CATALOG.serialize();
        let mut end = start;
        end[end.len() - 1] += 1;
        for (_, value) in txn.scan(start, end) {
            let table: Table = bincode::deserialize(&value)?;
            max_object_id = max_object_id.max(table.id.0);
        }

        let start = ObjectId::INDEX_CATALOG.serialize();
        let mut end = start;
        end[end.len() - 1] += 1;
        for (_, value) in txn.scan(start, end) {
            let index: Index = bincode::deserialize(&value)?;
            max_object_id = max_object_id.max(index.id.0);
        }

        txn.commit();

        Ok(Self {
            next_object_id: (max_object_id + 1).into(),
            scalar_functions: builtin::scalar_function::load()
                .map(|f| (f.name, f))
                .collect(),
            aggregate_functions: builtin::aggregate_function::load()
                .map(|f| (f.name, f))
                .collect(),
            table_functions: builtin::table_function::load()
                .map(|f| (f.name, f))
                .collect(),
        })
    }

    pub fn with<'txn, 'db: 'txn>(
        &'db self,
        txn: &'txn T::Transaction<'db>,
    ) -> CatalogRef<'txn, 'db, T> {
        CatalogRef { catalog: self, txn }
    }

    fn generate_object_id(&self) -> ObjectId {
        ObjectId(
            self.next_object_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        )
    }
}

pub struct CatalogRef<'txn, 'db, T: Storage + 'db> {
    catalog: &'txn Catalog<T>,
    txn: &'txn T::Transaction<'db>,
}

impl<'txn, 'db, T: Storage> CatalogRef<'txn, 'db, T> {
    pub fn table(&self, name: String) -> CatalogResult<storage::Table<'txn, 'db, T>> {
        let mut key = ObjectId::TABLE_CATALOG.serialize().to_vec();
        key.extend_from_slice(name.as_bytes());
        self.txn.get(&key).map_or(
            Err(CatalogError::UnknownEntry(CatalogEntryKind::Table, name)),
            |value| self.read_table(&key, &value),
        )
    }

    pub fn tables(
        &self,
    ) -> Box<dyn Iterator<Item = CatalogResult<storage::Table<'txn, 'db, T>>> + '_> {
        let start = ObjectId::TABLE_CATALOG.serialize();
        let mut end = start;
        end[end.len() - 1] += 1;
        let iter = self
            .txn
            .scan(start, end)
            .map(|(k, v)| self.read_table(&k, &v));
        Box::new(iter)
    }

    fn read_table(&self, key: &[u8], value: &[u8]) -> CatalogResult<storage::Table<'txn, 'db, T>> {
        if key.len() < ObjectId::SERIALIZED_LEN {
            return Err(CatalogError::InvalidEncoding);
        }
        let name = String::from_utf8(key[ObjectId::SERIALIZED_LEN..].to_vec())
            .map_err(|_| CatalogError::InvalidEncoding)?;
        let table: Table = bincode::deserialize(value)?;
        let mut indexes = Vec::with_capacity(table.index_names.len());
        for index_name in &table.index_names {
            let mut key = ObjectId::INDEX_CATALOG.serialize().to_vec();
            key.extend_from_slice(index_name.as_bytes());
            let value = self.txn.get(&key).ok_or_else(|| {
                CatalogError::UnknownEntry(CatalogEntryKind::Index, index_name.to_owned())
            })?;
            let index: Index = bincode::deserialize(&value)?;
            indexes.push(index);
        }
        Ok(storage::Table::new(self.txn, name, table, indexes))
    }

    pub fn create_table(
        &self,
        name: &str,
        columns: &[Column],
        constraints: &[Constraint],
    ) -> CatalogResult<ObjectId> {
        let id = self.catalog.generate_object_id();

        let mut key = ObjectId::TABLE_CATALOG.serialize().to_vec();
        key.extend_from_slice(name.as_bytes());

        let table = Table {
            id,
            columns: columns.to_owned(),
            constraints: constraints.to_owned(),
            index_names: Vec::new(),
        };
        let inserted = self.txn.insert(key, bincode::serialize(&table)?);
        if inserted {
            Ok(id)
        } else {
            Err(CatalogError::DuplicateEntry(
                CatalogEntryKind::Table,
                name.to_owned(),
            ))
        }
    }

    pub fn drop_table(&self, name: &str) -> CatalogResult<()> {
        let mut key = ObjectId::TABLE_CATALOG.serialize().to_vec();
        key.extend_from_slice(name.as_bytes());
        let bytes = self
            .txn
            .remove(key)
            .ok_or_else(|| CatalogError::UnknownEntry(CatalogEntryKind::Table, name.to_owned()))?;
        let table: Table = bincode::deserialize(&bytes)?;
        let start = table.id.serialize();
        let mut end = start;
        end[end.len() - 1] += 1;
        for (key, _) in self.txn.scan(start, end) {
            self.txn.remove(key).unwrap();
        }
        for index_name in table.index_names {
            self.drop_index(&index_name)?;
        }
        Ok(())
    }

    pub fn create_index(
        &self,
        name: String,
        table_name: String,
        column_indexes: &[ColumnIndex],
        is_unique: bool,
    ) -> CatalogResult<ObjectId> {
        assert!(!column_indexes.is_empty());

        let mut table_key = ObjectId::TABLE_CATALOG.serialize().to_vec();
        table_key.extend_from_slice(table_name.as_bytes());
        let mut table: Table = match self.txn.get(&table_key) {
            Some(v) => bincode::deserialize(&v)?,
            None => {
                return Err(CatalogError::UnknownEntry(
                    CatalogEntryKind::Table,
                    table_name,
                ))
            }
        };
        assert!(column_indexes.iter().all(|i| i.0 < table.columns.len()));

        let id = self.catalog.generate_object_id();

        let mut index_key = ObjectId::INDEX_CATALOG.serialize().to_vec();
        index_key.extend_from_slice(name.as_bytes());

        let index = Index {
            id,
            column_indexes: column_indexes.to_owned(),
            is_unique,
        };
        let inserted = self.txn.insert(index_key, bincode::serialize(&index)?);
        if !inserted {
            return Err(CatalogError::DuplicateEntry(
                CatalogEntryKind::Index,
                name.clone(),
            ));
        }
        table.index_names.push(name);

        self.txn.remove(table_key.clone()).unwrap();
        let inserted = self.txn.insert(table_key, bincode::serialize(&table)?);
        assert!(inserted);

        Ok(id)
    }

    pub fn drop_index(&self, name: &str) -> CatalogResult<()> {
        let mut key = ObjectId::INDEX_CATALOG.serialize().to_vec();
        key.extend_from_slice(name.as_bytes());
        let bytes = self
            .txn
            .remove(key)
            .ok_or_else(|| CatalogError::UnknownEntry(CatalogEntryKind::Index, name.to_owned()))?;
        let index: Index = bincode::deserialize(&bytes)?;
        let start = index.id.serialize();
        let mut end = start;
        end[end.len() - 1] += 1;
        for (key, _) in self.txn.scan(start, end) {
            self.txn.remove(key).unwrap();
        }
        Ok(())
    }

    pub fn functions(&self) -> impl Iterator<Item = Function<T>> + '_ {
        let scalar = self.catalog.scalar_functions.values().map(Function::Scalar);
        let aggregate = self
            .catalog
            .aggregate_functions
            .values()
            .map(Function::Aggregate);
        let table = self.catalog.table_functions.values().map(Function::Table);
        scalar.chain(aggregate).chain(table)
    }

    pub fn scalar_function(&self, name: &str) -> CatalogResult<&ScalarFunction<T>> {
        self.catalog.scalar_functions.get(name).ok_or_else(|| {
            CatalogError::UnknownEntry(CatalogEntryKind::ScalarFunction, name.to_owned())
        })
    }

    pub fn aggregate_function(&self, name: &str) -> CatalogResult<&AggregateFunction> {
        self.catalog.aggregate_functions.get(name).ok_or_else(|| {
            CatalogError::UnknownEntry(CatalogEntryKind::AggregateFunction, name.to_owned())
        })
    }

    pub fn table_function(&self, name: &str) -> CatalogResult<&TableFunction<T>> {
        self.catalog.table_functions.get(name).ok_or_else(|| {
            CatalogError::UnknownEntry(CatalogEntryKind::TableFunction, name.to_owned())
        })
    }
}
