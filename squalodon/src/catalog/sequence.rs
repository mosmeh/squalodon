use super::{CatalogEntryKind, CatalogRef, CatalogResult, ObjectId};
use crate::storage::Transaction;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct Sequence<'a> {
    catalog: CatalogRef<'a>,
    def: SequenceDef,
}

impl Sequence<'_> {
    pub fn id(&self) -> ObjectId {
        self.def.id
    }

    pub fn name(&self) -> &str {
        &self.def.name
    }

    pub fn increment_by(&self) -> i64 {
        self.def.increment_by
    }

    pub fn min_value(&self) -> i64 {
        self.def.min_value
    }

    pub fn max_value(&self) -> i64 {
        self.def.max_value
    }

    pub fn start_value(&self) -> i64 {
        self.def.start_value
    }

    pub fn cycle(&self) -> bool {
        self.def.cycle
    }

    pub fn drop_it(self) -> CatalogResult<()> {
        self.reset();
        self.catalog
            .remove_entry(CatalogEntryKind::Sequence, &self.def.name)
    }

    pub fn transaction(&self) -> &dyn Transaction {
        self.catalog.txn
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct SequenceDef {
    id: ObjectId,
    name: String,
    increment_by: i64,
    min_value: i64,
    max_value: i64,
    start_value: i64,
    cycle: bool,
}

impl<'a> CatalogRef<'a> {
    pub fn sequence(&self, name: &str) -> CatalogResult<Sequence<'a>> {
        let def: SequenceDef = self.entry(CatalogEntryKind::Sequence, name)?;
        Ok(Sequence {
            catalog: self.clone(),
            def,
        })
    }

    pub fn sequences(&self) -> impl Iterator<Item = CatalogResult<Sequence<'a>>> + '_ {
        self.entries(CatalogEntryKind::Sequence).map(move |result| {
            result.map(|def| Sequence {
                catalog: self.clone(),
                def,
            })
        })
    }

    pub fn create_sequence(
        &self,
        name: &str,
        increment_by: i64,
        min_value: i64,
        max_value: i64,
        start_value: i64,
        cycle: bool,
    ) -> CatalogResult<ObjectId> {
        let id = self.generate_object_id()?;
        let def = SequenceDef {
            id,
            name: name.to_owned(),
            increment_by,
            min_value,
            max_value,
            start_value,
            cycle,
        };
        self.insert_entry(CatalogEntryKind::Sequence, name, &def)?;
        Ok(id)
    }
}
