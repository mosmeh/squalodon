use crate::{parser, rows::ColumnIndex, types::NullableType};
use std::borrow::Cow;

#[derive(Debug, Clone)]
pub struct Column {
    pub table_name: Option<String>,
    pub column_name: String,
    pub ty: NullableType,
}

impl Column {
    pub fn new(name: impl Into<String>, ty: impl Into<NullableType>) -> Self {
        Self {
            table_name: None,
            column_name: name.into(),
            ty: ty.into(),
        }
    }

    pub fn name(&self) -> Cow<str> {
        self.table_name.as_ref().map_or_else(
            || Cow::Borrowed(self.column_name.as_str()),
            |table_name| Cow::Owned(format!("{}.{}", table_name, self.column_name)),
        )
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
