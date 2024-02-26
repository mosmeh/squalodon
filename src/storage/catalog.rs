use super::{Error, Result, TableId, TableKeyPrefix, Transaction, TABLE_SCHEMA_PREFIX};
use crate::types::Type;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub ty: Type,
    pub is_primary_key: bool,
    pub is_not_null: bool,
}

pub struct Catalog {
    tables: HashMap<TableId, Table>,
    table_names: HashMap<String, TableId>,
    next_table_id: TableId,
}

impl Catalog {
    pub fn load<T: Transaction>(txn: &T) -> Result<Self> {
        let mut tables = HashMap::new();
        let mut table_names = HashMap::new();
        let mut max_table_id = 0;
        for (k, v) in txn.scan(&[TABLE_SCHEMA_PREFIX], &[TABLE_SCHEMA_PREFIX + 1]) {
            let TableKeyPrefix::TableSchema(id) = TableKeyPrefix::deserialize_from(k)? else {
                panic!("underlying storage corrupted");
            };
            let table: Table = bincode::deserialize(v)?;
            table_names.insert(table.name.clone(), id);
            tables.insert(id, table);
            max_table_id = max_table_id.max(id.0);
        }
        Ok(Self {
            tables,
            table_names,
            next_table_id: TableId(max_table_id + 1),
        })
    }

    pub fn create_table<T: Transaction>(
        &mut self,
        txn: &mut T,
        name: String,
        columns: Vec<Column>,
    ) -> Result<TableId> {
        let entry = match self.table_names.entry(name.clone()) {
            std::collections::hash_map::Entry::Occupied(_) => {
                return Err(Error::TableAlreadyExists)
            }
            std::collections::hash_map::Entry::Vacant(entry) => entry,
        };

        let id = self.next_table_id;
        let table = Table { id, name, columns };
        txn.insert(
            TableKeyPrefix::TableSchema(id).serialize().to_vec(),
            bincode::serialize(&table).unwrap(),
        );

        self.next_table_id.0 += 1;

        self.tables.insert(id, table);
        entry.insert(id);
        Ok(id)
    }

    pub fn table_id(&self, name: &str) -> Result<TableId> {
        self.table_names
            .get(name)
            .map_or(Err(Error::UnknownTable), |id| Ok(*id))
    }

    pub fn table(&self, id: TableId) -> Result<Table> {
        self.tables
            .get(&id)
            .map_or(Err(Error::UnknownTable), |table| Ok(table.clone()))
    }
}
