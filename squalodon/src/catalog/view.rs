use super::{CatalogEntryKind, CatalogRef, CatalogResult};
use crate::parser::Query;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct View<'a> {
    catalog: CatalogRef<'a>,
    def: ViewDef,
}

impl View<'_> {
    pub fn name(&self) -> &str {
        &self.def.name
    }

    pub fn column_names(&self) -> Option<&[String]> {
        self.def.column_names.as_deref()
    }

    pub fn query(&self) -> &Query {
        &self.def.query
    }

    pub fn drop_it(self) -> CatalogResult<()> {
        self.catalog
            .remove_entry(CatalogEntryKind::View, &self.def.name)
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ViewDef {
    name: String,
    column_names: Option<Vec<String>>,
    query: Query,
}

impl<'a> CatalogRef<'a> {
    pub fn view(&self, name: &str) -> CatalogResult<View<'a>> {
        let def: ViewDef = self.entry(CatalogEntryKind::View, name)?;
        Ok(View {
            catalog: self.clone(),
            def,
        })
    }

    pub fn views(&self) -> impl Iterator<Item = CatalogResult<View<'a>>> + '_ {
        self.entries(CatalogEntryKind::View).map(|r| {
            r.map(|def| View {
                catalog: self.clone(),
                def,
            })
        })
    }

    pub fn create_view(
        &self,
        name: &str,
        column_names: Option<Vec<String>>,
        query: Query,
    ) -> CatalogResult<View<'a>> {
        let def = ViewDef {
            name: name.to_owned(),
            column_names,
            query,
        };
        self.insert_entry(CatalogEntryKind::View, name, &def)?;
        Ok(View {
            catalog: self.clone(),
            def,
        })
    }
}
