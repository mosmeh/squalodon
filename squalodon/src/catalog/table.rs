use super::{CatalogRef, CatalogResult, Index, ObjectId};
use crate::{
    catalog::{index::IndexDef, CatalogEntryKind},
    parser::Expression,
    rows::ColumnIndex,
    storage::Transaction,
    types::Type,
};
use serde::{Deserialize, Serialize};

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
        self.index_defs
            .iter()
            .map(|def| Index::new(def.clone(), self.clone()))
    }

    pub fn create_index(
        &mut self,
        name: String,
        column_indexes: &[ColumnIndex],
        is_unique: bool,
    ) -> CatalogResult<Index<'a>> {
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

        let index = Index::new(index_def, self.clone());
        index.reindex()?;
        Ok(index)
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

impl Column {
    pub const RESERVED_NAMES: &'static [&'static str] = &[Self::ROWID];
    pub const ROWID: &'static str = "rowid";

    pub fn is_hidden(&self) -> bool {
        self.name == Self::ROWID
    }
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

impl<'a> CatalogRef<'a> {
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
    ) -> CatalogResult<Table<'a>> {
        let def = TableDef {
            id: self.generate_object_id()?,
            name: name.to_owned(),
            columns: columns.to_owned(),
            primary_keys: primary_keys.to_owned(),
            index_names: Vec::new(),
            statistics: Statistics::default(),
        };
        self.insert_entry(CatalogEntryKind::Table, name, &def)?;
        Ok(Table {
            catalog: self.clone(),
            def,
            index_defs: Vec::new(),
        })
    }
}
