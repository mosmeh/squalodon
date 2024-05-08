use crate::{parser, rows::ColumnIndex, types::NullableType};
use std::borrow::Cow;

#[derive(Debug, Clone)]
pub struct Column {
    canonical_name: parser::ColumnRef,
    alias: Option<parser::ColumnRef>,
    ty: NullableType,
    is_hidden: bool,
}

impl Column {
    pub fn new(name: impl Into<String>, ty: impl Into<NullableType>) -> Self {
        Self {
            canonical_name: parser::ColumnRef {
                table_name: None,
                column_name: name.into(),
            },
            alias: None,
            ty: ty.into(),
            is_hidden: false,
        }
    }

    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.canonical_name.table_name = Some(table_name.into());
        self
    }

    pub fn with_hidden(mut self, yes: bool) -> Self {
        self.is_hidden = yes;
        self
    }

    pub fn canonical_name(&self) -> Cow<str> {
        let canonical = &self.canonical_name;
        canonical.table_name.as_ref().map_or_else(
            || Cow::Borrowed(canonical.column_name.as_str()),
            |table_name| Cow::Owned(format!("{}.{}", table_name, canonical.column_name)),
        )
    }

    pub fn simple_name(&self) -> &str {
        &self
            .alias
            .as_ref()
            .unwrap_or(&self.canonical_name)
            .column_name
    }

    pub fn column_ref(&self) -> &parser::ColumnRef {
        self.alias.as_ref().unwrap_or(&self.canonical_name)
    }

    pub fn is_hidden(&self) -> bool {
        self.is_hidden
    }

    pub fn is_match<T: ColumnRef>(&self, column_ref: &T) -> bool {
        let r = self.alias.as_ref().unwrap_or(&self.canonical_name);
        if r.column_name != column_ref.column_name() {
            return false;
        }
        match (&r.table_name, column_ref.table_name()) {
            (Some(a), Some(b)) if a == b => true,
            (_, None) => {
                // If the column reference does not specify
                // a table name, it ambiguously matches any column
                // with the same column name.
                true
            }
            (_, Some(_)) => false,
        }
    }

    pub fn set_table_alias<T: Into<Option<String>>>(&mut self, alias: T) {
        let alias = alias.into();
        match &mut self.alias {
            Some(a) => a.table_name = alias,
            None => {
                self.alias = Some(parser::ColumnRef {
                    table_name: alias,
                    ..self.canonical_name.clone()
                });
            }
        }
    }

    pub fn set_column_alias<T: Into<String>>(&mut self, alias: T) {
        let alias = alias.into();
        match &mut self.alias {
            Some(a) => a.column_name = alias,
            None => {
                self.alias = Some(parser::ColumnRef {
                    column_name: alias,
                    ..self.canonical_name.clone()
                });
            }
        }
    }

    pub fn ty(&self) -> NullableType {
        self.ty
    }
}

pub trait ColumnRef {
    fn table_name(&self) -> Option<&str>;
    fn column_name(&self) -> &str;
}

impl ColumnRef for parser::ColumnRef {
    fn table_name(&self) -> Option<&str> {
        self.table_name.as_deref()
    }

    fn column_name(&self) -> &str {
        &self.column_name
    }
}

impl ColumnRef for &parser::ColumnRef {
    fn table_name(&self) -> Option<&str> {
        self.table_name.as_deref()
    }

    fn column_name(&self) -> &str {
        &self.column_name
    }
}

impl<T: AsRef<str>> ColumnRef for T {
    fn table_name(&self) -> Option<&str> {
        None
    }

    fn column_name(&self) -> &str {
        self.as_ref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnId(usize);

impl ColumnId {
    pub fn to_index(self, columns: &[Self]) -> ColumnIndex {
        let index = columns.iter().position(|id| id.0 == self.0).unwrap();
        ColumnIndex(index)
    }
}

#[derive(Default)]
pub struct ColumnMap(Vec<Column>);

impl ColumnMap {
    pub fn insert(&mut self, column: Column) -> ColumnId {
        let id = ColumnId(self.0.len());
        self.0.push(column);
        id
    }
}

impl std::ops::Index<ColumnId> for ColumnMap {
    type Output = Column;

    fn index(&self, index: ColumnId) -> &Self::Output {
        &self.0[index.0]
    }
}

impl std::ops::Index<&ColumnId> for ColumnMap {
    type Output = Column;

    fn index(&self, index: &ColumnId) -> &Self::Output {
        &self.0[index.0]
    }
}

impl std::ops::IndexMut<ColumnId> for ColumnMap {
    fn index_mut(&mut self, index: ColumnId) -> &mut Self::Output {
        &mut self.0[index.0]
    }
}

impl std::ops::IndexMut<&ColumnId> for ColumnMap {
    fn index_mut(&mut self, index: &ColumnId) -> &mut Self::Output {
        &mut self.0[index.0]
    }
}
