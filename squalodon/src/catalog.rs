use crate::{
    builtin,
    executor::{ExecutionContext, ExecutorResult},
    parser::Expression,
    planner,
    rows::ColumnIndex,
    storage::{StorageError, Transaction, TransactionExt},
    types::{Args, NullableType, Type},
    Row, TryFromValueError, Value,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{collections::HashMap, ops::Deref};

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

impl CatalogError {
    fn unknown_function(
        kind: CatalogEntryKind,
        name: String,
        argument_types: &[NullableType],
    ) -> Self {
        let args = argument_types
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        Self::UnknownEntry(kind, format!("{name}({args})"))
    }
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

#[derive(Clone)]
pub struct Table<'a> {
    catalog: CatalogRef<'a>,
    def: TableDef,
    index_defs: Vec<IndexDef>,
}

impl<'a> Table<'a> {
    pub fn id(&self) -> ObjectId {
        self.def.id
    }

    pub fn name(&self) -> &str {
        &self.def.name
    }

    pub fn columns(&self) -> &[Column] {
        &self.def.columns
    }

    pub fn primary_keys(&self) -> &[ColumnIndex] {
        &self.def.primary_keys
    }

    pub fn indexes(&self) -> impl Iterator<Item = Index<'a>> + '_ {
        self.index_defs.iter().map(|def| Index {
            def: def.clone(),
            table: self.clone(),
        })
    }

    pub fn create_index(
        &mut self,
        name: String,
        column_indexes: &[ColumnIndex],
        is_unique: bool,
    ) -> CatalogResult<ObjectId> {
        assert!(!column_indexes.is_empty());

        let id = self.catalog.generate_object_id()?;
        let index_def = IndexDef {
            id,
            name: name.clone(),
            table_name: self.name().to_owned(),
            column_indexes: column_indexes.to_owned(),
            is_unique,
        };
        self.catalog
            .insert_entry(CatalogEntryKind::Index, &name, &index_def)?;

        assert!(column_indexes.iter().all(|i| i.0 < self.columns().len()));
        self.def.index_names.push(name);
        self.save()?;

        let index = Index {
            def: index_def,
            table: self.clone(),
        };
        index.reindex()?;
        Ok(id)
    }

    pub fn drop_index(&mut self, name: &str) -> CatalogResult<()> {
        self.def.index_names.retain(|n| *n != name);
        self.save()?;

        self.catalog.index(name)?.clear()?;
        self.catalog.remove_entry(CatalogEntryKind::Index, name)
    }

    pub fn drop_it(mut self) -> CatalogResult<()> {
        let index_names = self.def.index_names.clone();
        for name in index_names {
            self.drop_index(&name)?;
        }
        self.def.index_names.clear();

        self.truncate()?;
        self.catalog
            .remove_entry(CatalogEntryKind::Table, self.name())
    }

    pub fn analyze(&mut self) -> CatalogResult<()> {
        let mut num_rows = 0;
        for row in self.scan() {
            row?;
            num_rows += 1;
        }
        self.def.statistics = Statistics { num_rows };
        self.save()
    }

    pub fn statistics(&self) -> &Statistics {
        &self.def.statistics
    }

    pub fn transaction(&self) -> &'a dyn Transaction {
        self.catalog.txn
    }

    fn save(&self) -> CatalogResult<()> {
        self.catalog
            .put_entry(CatalogEntryKind::Table, self.name(), &self.def)
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct TableDef {
    id: ObjectId,
    name: String,
    columns: Vec<Column>,
    primary_keys: Vec<ColumnIndex>,
    index_names: Vec<String>,
    statistics: Statistics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub ty: Type,
    pub is_nullable: bool,
    pub default_value: Option<Expression>,
}

#[derive(Clone)]
pub struct Index<'a> {
    def: IndexDef,
    table: Table<'a>,
}

impl<'a> Index<'a> {
    pub fn id(&self) -> ObjectId {
        self.def.id
    }

    pub fn name(&self) -> &str {
        &self.def.name
    }

    pub fn column_indexes(&self) -> &[ColumnIndex] {
        &self.def.column_indexes
    }

    pub fn is_unique(&self) -> bool {
        self.def.is_unique
    }

    pub fn table(&self) -> &Table<'a> {
        &self.table
    }

    pub fn transaction(&self) -> &'a dyn Transaction {
        self.table.transaction()
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct IndexDef {
    id: ObjectId,
    name: String,
    table_name: String,
    column_indexes: Vec<ColumnIndex>,
    is_unique: bool,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Statistics {
    num_rows: u64,
}

impl Statistics {
    pub fn num_rows(&self) -> u64 {
        self.num_rows
    }
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

pub struct ScalarFunction {
    pub name: &'static str,
    pub argument_types: &'static [Type],
    pub return_type: NullableType,
    pub eval: BoxedScalarFn,
}

impl PartialEq for ScalarFunction {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.argument_types == other.argument_types
    }
}

impl Eq for ScalarFunction {}

impl std::hash::Hash for ScalarFunction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.argument_types.hash(state);
    }
}

pub type ScalarPureFn = dyn Fn(&[Value]) -> ExecutorResult<Value> + Send + Sync;
pub type ScalarImpureFn =
    dyn Fn(&ExecutionContext, &[Value]) -> ExecutorResult<Value> + Send + Sync;

pub enum BoxedScalarFn {
    Pure(Box<ScalarPureFn>),
    Impure(Box<ScalarImpureFn>),
}

impl BoxedScalarFn {
    pub fn pure<F, A, R>(f: F) -> Self
    where
        F: Fn(A) -> ExecutorResult<R> + Send + Sync + 'static,
        A: Args,
        R: Into<Value>,
    {
        Self::Pure(Box::new(move |args| {
            let args = A::from_values(args).expect("Declared and actual argument types must match");
            f(args).map(Into::into)
        }))
    }

    pub fn impure<F, A, R>(f: F) -> Self
    where
        F: Fn(&ExecutionContext, A) -> ExecutorResult<R> + Send + Sync + 'static,
        A: Args,
        R: Into<Value>,
    {
        Self::Impure(Box::new(move |ctx, args| {
            let args = A::from_values(args).expect("Declared and actual argument types must match");
            f(ctx, args).map(Into::into)
        }))
    }
}

pub struct AggregateFunction {
    pub name: &'static str,
    pub input_type: Type,
    pub output_type: Type,
    pub init: AggregateInitFnPtr,
}

impl AggregateFunction {
    /// Creates an aggregate function that is not exposed to the user.
    pub const fn new_internal<T: Aggregator + Default + 'static>(name: &'static str) -> Self {
        Self {
            name,
            input_type: Type::Text,  // dummy
            output_type: Type::Text, // dummy
            init: || Box::<T>::default(),
        }
    }
}

pub type AggregateInitFnPtr = fn() -> Box<dyn Aggregator>;

pub trait Aggregator {
    fn update(&mut self, value: &Value) -> ExecutorResult<()>;
    fn finish(&self) -> Value;
}

pub trait TypedAggregator {
    type Input: TryFrom<Value, Error = TryFromValueError>;
    type Output: Into<Value>;

    fn update(&mut self, value: Self::Input) -> ExecutorResult<()>;
    fn finish(&self) -> Self::Output;
}

impl<T: TypedAggregator> Aggregator for T {
    fn update(&mut self, value: &Value) -> ExecutorResult<()> {
        if !value.is_null() {
            let value = value
                .clone()
                .try_into()
                .expect("Declared and actual input types must match");
            TypedAggregator::update(self, value)?;
        }
        Ok(())
    }

    fn finish(&self) -> Value {
        TypedAggregator::finish(self).into()
    }
}

pub struct TableFunction {
    pub name: &'static str,
    pub argument_types: &'static [Type],
    pub result_columns: Vec<planner::Column>,
    pub eval: BoxedTableFn,
}

pub type TableFn = dyn Fn(&ExecutionContext, &[Value]) -> ExecutorResult<Box<dyn Iterator<Item = Row>>>
    + Send
    + Sync;

pub struct BoxedTableFn(Box<TableFn>);

impl Deref for BoxedTableFn {
    type Target = TableFn;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl BoxedTableFn {
    pub fn new<F, A, R>(f: F) -> Self
    where
        F: Fn(&ExecutionContext, A) -> ExecutorResult<R> + Send + Sync + 'static,
        A: Args,
        R: Iterator<Item = Row> + 'static,
    {
        Self(Box::new(move |ctx, args| {
            let args = A::from_values(args).expect("Declared and actual argument types must match");
            Ok(Box::new(f(ctx, args)?))
        }))
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

    pub fn table(&self, name: &str) -> CatalogResult<Table<'a>> {
        self.entry(CatalogEntryKind::Table, name)
            .and_then(|def| self.read_table(def))
    }

    pub fn tables(&self) -> impl Iterator<Item = CatalogResult<Table<'a>>> + '_ {
        self.entries(CatalogEntryKind::Table)
            .map(|def| self.read_table(def?))
    }

    fn read_table(&self, def: TableDef) -> CatalogResult<Table<'a>> {
        let mut index_defs = Vec::with_capacity(def.index_names.len());
        for index_name in &def.index_names {
            index_defs.push(self.entry(CatalogEntryKind::Index, index_name)?);
        }
        Ok(Table {
            catalog: self.clone(),
            def,
            index_defs,
        })
    }

    pub fn create_table(
        &self,
        name: &str,
        columns: &[Column],
        primary_keys: &[ColumnIndex],
    ) -> CatalogResult<ObjectId> {
        let id = self.generate_object_id()?;
        let table_def = TableDef {
            id,
            name: name.to_owned(),
            columns: columns.to_owned(),
            primary_keys: primary_keys.to_owned(),
            index_names: Vec::new(),
            statistics: Statistics::default(),
        };
        self.insert_entry(CatalogEntryKind::Table, name, &table_def)?;
        Ok(id)
    }

    pub fn index(&self, name: &str) -> CatalogResult<Index<'a>> {
        let def: IndexDef = self.entry(CatalogEntryKind::Index, name)?;
        let table = self.table(&def.table_name)?;
        Ok(Index { def, table })
    }

    pub fn functions(&self) -> impl Iterator<Item = Function> + '_ {
        let scalar = self
            .catalog
            .scalar_functions
            .values()
            .flatten()
            .map(Function::Scalar);
        let aggregate = self
            .catalog
            .aggregate_functions
            .values()
            .flatten()
            .map(Function::Aggregate);
        let table = self
            .catalog
            .table_functions
            .values()
            .flatten()
            .map(Function::Table);
        scalar.chain(aggregate).chain(table)
    }

    pub fn scalar_function(
        &self,
        name: &str,
        argument_types: &[NullableType],
    ) -> CatalogResult<&'a ScalarFunction> {
        self.catalog
            .scalar_functions
            .get(name)
            .and_then(|functions| {
                functions.iter().find(|f| {
                    if f.argument_types.len() != argument_types.len() {
                        return false;
                    }
                    argument_types
                        .iter()
                        .zip(f.argument_types)
                        .all(|(a, b)| a.is_compatible_with(*b))
                })
            })
            .ok_or_else(|| {
                CatalogError::unknown_function(
                    CatalogEntryKind::ScalarFunction,
                    name.to_owned(),
                    argument_types,
                )
            })
    }

    pub fn aggregate_function(
        &self,
        name: &str,
        input_type: NullableType,
    ) -> CatalogResult<&'a AggregateFunction> {
        self.catalog
            .aggregate_functions
            .get(name)
            .and_then(|functions| {
                functions
                    .iter()
                    .find(|f| input_type.is_compatible_with(f.input_type))
            })
            .ok_or_else(|| {
                CatalogError::unknown_function(
                    CatalogEntryKind::AggregateFunction,
                    name.to_owned(),
                    &[input_type],
                )
            })
    }

    pub fn table_function(
        &self,
        name: &str,
        argument_types: &[NullableType],
    ) -> CatalogResult<&'a TableFunction> {
        self.catalog
            .table_functions
            .get(name)
            .and_then(|functions| {
                functions.iter().find(|f| {
                    if f.argument_types.len() != argument_types.len() {
                        return false;
                    }
                    argument_types
                        .iter()
                        .zip(f.argument_types.iter())
                        .all(|(a, b)| a.is_compatible_with(*b))
                })
            })
            .ok_or_else(|| {
                CatalogError::unknown_function(
                    CatalogEntryKind::TableFunction,
                    name.to_owned(),
                    argument_types,
                )
            })
    }
}
