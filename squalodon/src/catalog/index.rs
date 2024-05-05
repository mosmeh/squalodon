use super::{CatalogEntryKind, CatalogRef, CatalogResult, ObjectId, Table};
use crate::{rows::ColumnIndex, storage::Transaction};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct Index<'a> {
    def: IndexDef,
    table: Table<'a>,
}

impl<'a> Index<'a> {
    pub(super) fn new(def: IndexDef, table: Table<'a>) -> Self {
        Self { def, table }
    }

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
pub(super) struct IndexDef {
    pub id: ObjectId,
    pub name: String,
    pub table_name: String,
    pub column_indexes: Vec<ColumnIndex>,
    pub is_unique: bool,
}

impl<'a> CatalogRef<'a> {
    pub fn index(&self, name: &str) -> CatalogResult<Index<'a>> {
        let def: IndexDef = self.entry(CatalogEntryKind::Index, name)?;
        let table = self.table(&def.table_name)?;
        Ok(Index::new(def, table))
    }
}
