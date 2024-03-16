use crate::{Error, Result, TryFromValueError, Type, Value};

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

pub struct Row {
    pub(crate) columns: Vec<Value>,
}

impl Row {
    pub fn get<T>(&self, column: usize) -> Result<T>
    where
        T: TryFrom<Value, Error = TryFromValueError>,
    {
        self.columns
            .get(column)
            .ok_or(Error::ColumnIndexOutOfRange)?
            .clone()
            .try_into()
            .map_err(Into::into)
    }

    pub fn columns(&self) -> &[Value] {
        &self.columns
    }
}
