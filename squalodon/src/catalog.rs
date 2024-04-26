use crate::{
    builtin,
    connection::ConnectionContext,
    executor::ExecutorResult,
    parser::Expression,
    planner::{self, PlannerResult},
    rows::ColumnIndex,
    storage::{Transaction, TransactionExt},
    types::{NullableType, Type},
    Row, Value,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ObjectId(u64);

impl ObjectId {
    const CATALOG: Self = Self(0);

    pub fn serialize(self) -> Vec<u8> {
        self.0.to_be_bytes().to_vec()
    }
}

#[derive(Clone)]
pub struct Table<'a> {
    txn: &'a dyn Transaction,
    def: TableDef,
    indexes: Vec<Index>,
}

impl<'a> Table<'a> {
    fn new(txn: &'a dyn Transaction, def: TableDef, indexes: Vec<IndexDef>) -> Self {
        let indexes = indexes
            .into_iter()
            .zip(def.index_names.iter())
            .map(|(def, name)| Index {
                name: name.clone(),
                def,
            })
            .collect();
        Self { txn, def, indexes }
    }

    pub fn id(&self) -> ObjectId {
        self.def.id
    }

    pub fn name(&self) -> &str {
        &self.def.name
    }

    pub fn columns(&self) -> &[Column] {
        &self.def.columns
    }

    pub fn constraints(&self) -> &[Constraint] {
        &self.def.constraints
    }

    pub fn indexes(&self) -> &[Index] {
        &self.indexes
    }

    pub fn transaction(&self) -> &'a dyn Transaction {
        self.txn
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct TableDef {
    id: ObjectId,
    name: String,
    columns: Vec<Column>,
    constraints: Vec<Constraint>,
    index_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub ty: Type,
    pub default_value: Option<Expression>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Constraint {
    PrimaryKey(Vec<ColumnIndex>),
    NotNull(ColumnIndex),
}

#[derive(Clone)]
pub struct Index {
    name: String,
    def: IndexDef,
}

impl Index {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn column_indexes(&self) -> &[ColumnIndex] {
        &self.def.column_indexes
    }

    pub fn is_unique(&self) -> bool {
        self.def.is_unique
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct IndexDef {
    id: ObjectId,
    table_name: String,
    column_indexes: Vec<ColumnIndex>,
    is_unique: bool,
}

pub enum Function<'a> {
    Scalar(&'a ScalarFunction),
    Aggregate(&'a AggregateFunction),
    Table(&'a TableFunction),
}

impl<'a> Function<'a> {
    pub fn name(&self) -> &str {
        match self {
            Self::Scalar(f) => f.name,
            Self::Aggregate(f) => f.name,
            Self::Table(f) => f.name,
        }
    }
}

#[derive(PartialEq, Eq, Hash)]
pub struct ScalarFunction {
    pub name: &'static str,
    pub bind: ScalarBindFnPtr,
    pub eval: ScalarEvalFnPtr,
}

pub type ScalarBindFnPtr = fn(&[NullableType]) -> PlannerResult<NullableType>;
pub type ScalarEvalFnPtr = fn(&ConnectionContext, &[Value]) -> ExecutorResult<Value>;

pub struct AggregateFunction {
    pub name: &'static str,
    pub bind: AggregateBindFnPtr,
    pub init: AggregateInitFnPtr,
}

impl AggregateFunction {
    /// Creates an aggregate function that is not exposed to the user.
    pub const fn new_internal<T: Aggregator + Default + 'static>() -> Self {
        Self {
            name: "(internal)",
            bind: |_| unreachable!(),
            init: || Box::<T>::default(),
        }
    }
}

pub type AggregateBindFnPtr = fn(NullableType) -> PlannerResult<NullableType>;
pub type AggregateInitFnPtr = fn() -> Box<dyn Aggregator>;

pub trait Aggregator {
    fn update(&mut self, value: &Value) -> ExecutorResult<()>;
    fn finish(&self) -> Value;
}

pub struct TableFunction {
    pub name: &'static str,
    pub fn_ptr: TableFnPtr,
    pub result_columns: Vec<planner::Column>,
}

pub type TableFnPtr =
    for<'a> fn(&'a ConnectionContext, &Row) -> ExecutorResult<Box<dyn Iterator<Item = Row> + 'a>>;

pub struct Catalog {
    scalar_functions: HashMap<&'static str, ScalarFunction>,
    aggregate_functions: HashMap<&'static str, AggregateFunction>,
    table_functions: HashMap<&'static str, TableFunction>,
}

impl Default for Catalog {
    fn default() -> Self {
        Self {
            scalar_functions: builtin::scalar_function::load()
                .map(|f| (f.name, f))
                .collect(),
            aggregate_functions: builtin::aggregate_function::load()
                .map(|f| (f.name, f))
                .collect(),
            table_functions: builtin::table_function::load()
                .map(|f| (f.name, f))
                .collect(),
        }
    }
}

impl Catalog {
    pub fn with<'a>(&'a self, txn: &'a dyn Transaction) -> CatalogRef<'a> {
        CatalogRef { catalog: self, txn }
    }
}

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
        const KEY: &str = "next_id";
        let mut next_id: u64 = match self.entry(CatalogEntryKind::Metadata, KEY) {
            Ok(id) => id,
            Err(CatalogError::UnknownEntry(_, _)) => 1,
            Err(e) => return Err(e),
        };
        assert!(next_id > ObjectId::CATALOG.0);
        let id = ObjectId(next_id);
        next_id += 1;
        self.put_entry(CatalogEntryKind::Metadata, KEY, &next_id)?;
        Ok(id)
    }

    pub fn table(&self, name: &str) -> CatalogResult<Table<'a>> {
        self.entry(CatalogEntryKind::Table, name)
            .and_then(|def| self.read_table(def))
    }

    pub fn tables(&self) -> impl Iterator<Item = CatalogResult<Table<'a>>> + '_ {
        self.entries(CatalogEntryKind::Table)
            .map(|def| self.read_table(def?))
    }

    fn read_table(&self, def: TableDef) -> CatalogResult<Table<'a>> {
        let mut indexes = Vec::with_capacity(def.index_names.len());
        for index_name in &def.index_names {
            indexes.push(self.entry(CatalogEntryKind::Index, index_name)?);
        }
        Ok(Table::new(self.txn, def, indexes))
    }

    pub fn create_table(
        &self,
        name: &str,
        columns: &[Column],
        constraints: &[Constraint],
    ) -> CatalogResult<ObjectId> {
        let id = self.generate_object_id()?;
        let table_def = TableDef {
            id,
            name: name.to_owned(),
            columns: columns.to_owned(),
            constraints: constraints.to_owned(),
            index_names: Vec::new(),
        };
        self.insert_entry(CatalogEntryKind::Table, name, &table_def)?;
        Ok(id)
    }

    pub fn drop_table(&self, name: &str) -> CatalogResult<()> {
        let table_def: TableDef = self.entry(CatalogEntryKind::Table, name)?;
        let prefix = table_def.id.serialize();
        for (key, _) in self.txn.prefix_scan(prefix) {
            self.txn.remove(&key).unwrap();
        }
        for index_name in table_def.index_names {
            self.drop_index(&index_name)?;
        }
        self.remove_entry(CatalogEntryKind::Table, name)
    }

    pub fn create_index(
        &self,
        name: String,
        table_name: String,
        column_indexes: &[ColumnIndex],
        is_unique: bool,
    ) -> CatalogResult<ObjectId> {
        assert!(!column_indexes.is_empty());

        let mut table_def: TableDef = self.entry(CatalogEntryKind::Table, &table_name)?;
        assert!(column_indexes.iter().all(|i| i.0 < table_def.columns.len()));

        let id = self.generate_object_id()?;
        let index_def = IndexDef {
            id,
            table_name: table_name.clone(),
            column_indexes: column_indexes.to_owned(),
            is_unique,
        };
        self.insert_entry(CatalogEntryKind::Index, &name, &index_def)?;

        table_def.index_names.push(name);
        self.put_entry(CatalogEntryKind::Table, &table_name, &table_def)?;

        Ok(id)
    }

    pub fn drop_index(&self, name: &str) -> CatalogResult<()> {
        let index_def: IndexDef = self.entry(CatalogEntryKind::Index, name)?;

        let mut table_def: TableDef = self.entry(CatalogEntryKind::Table, &index_def.table_name)?;
        table_def.index_names.retain(|n| n != name);
        self.put_entry(CatalogEntryKind::Table, &index_def.table_name, &table_def)?;

        let prefix = index_def.id.serialize();
        for (key, _) in self.txn.prefix_scan(prefix) {
            self.txn.remove(&key).unwrap();
        }

        self.remove_entry(CatalogEntryKind::Index, name)
    }

    pub fn functions(&self) -> impl Iterator<Item = Function> + '_ {
        let scalar = self.catalog.scalar_functions.values().map(Function::Scalar);
        let aggregate = self
            .catalog
            .aggregate_functions
            .values()
            .map(Function::Aggregate);
        let table = self.catalog.table_functions.values().map(Function::Table);
        scalar.chain(aggregate).chain(table)
    }

    pub fn scalar_function(&self, name: &str) -> CatalogResult<&ScalarFunction> {
        self.catalog.scalar_functions.get(name).ok_or_else(|| {
            CatalogError::UnknownEntry(CatalogEntryKind::ScalarFunction, name.to_owned())
        })
    }

    pub fn aggregate_function(&self, name: &str) -> CatalogResult<&AggregateFunction> {
        self.catalog.aggregate_functions.get(name).ok_or_else(|| {
            CatalogError::UnknownEntry(CatalogEntryKind::AggregateFunction, name.to_owned())
        })
    }

    pub fn table_function(&self, name: &str) -> CatalogResult<&TableFunction> {
        self.catalog.table_functions.get(name).ok_or_else(|| {
            CatalogError::UnknownEntry(CatalogEntryKind::TableFunction, name.to_owned())
        })
    }
}
