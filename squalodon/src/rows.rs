use crate::{Error, Result, TryFromValueError, Type, Value};
use serde::{Deserialize, Serialize};
use std::ops::Index;

#[derive(Debug, Clone)]
pub struct Column {
    pub(crate) name: String,
    pub(crate) ty: Type,
}

impl Column {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn ty(&self) -> Type {
        self.ty
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnIndex(pub usize);

impl std::fmt::Display for ColumnIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "#{}", self.0)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Row(pub(crate) Box<[Value]>);

impl Row {
    pub(crate) fn new(values: Vec<Value>) -> Self {
        Self(values.into_boxed_slice())
    }

    pub(crate) fn empty() -> Self {
        Self(Default::default())
    }

    pub fn get<T>(&self, column: usize) -> Result<T>
    where
        T: TryFrom<Value, Error = TryFromValueError>,
    {
        self.0
            .get(column)
            .ok_or(Error::ColumnIndexOutOfRange)?
            .clone()
            .try_into()
            .map_err(Into::into)
    }

    pub fn columns(&self) -> &[Value] {
        &self.0
    }
}

impl<'a> From<&'a Row> for &'a [Value] {
    fn from(row: &'a Row) -> Self {
        &row.0
    }
}

impl Index<usize> for Row {
    type Output = Value;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl Index<ColumnIndex> for Row {
    type Output = Value;

    fn index(&self, index: ColumnIndex) -> &Self::Output {
        &self.0[index.0]
    }
}

impl Index<&ColumnIndex> for Row {
    type Output = Value;

    fn index(&self, index: &ColumnIndex) -> &Self::Output {
        &self.0[index.0]
    }
}

pub struct Rows {
    pub(crate) iter: std::vec::IntoIter<Row>,
    pub(crate) columns: Vec<Column>,
}

impl Rows {
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub(crate) fn empty() -> Self {
        Self {
            iter: Vec::new().into_iter(),
            columns: Vec::new(),
        }
    }
}

impl Iterator for Rows {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}
