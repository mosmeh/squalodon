use super::{ExecutorNode, Node, NodeError, Output};
use crate::{storage::Table, Storage};

pub struct Insert<'txn, 'db, T: Storage> {
    pub source: Box<ExecutorNode<'txn, 'db, T>>,
    pub table: Table<'txn, 'db, T>,
}

impl<T: Storage> Node for Insert<'_, '_, T> {
    fn next_row(&mut self) -> Output {
        for row in self.source.by_ref() {
            self.table.insert(&row?)?;
        }
        Err(NodeError::EndOfRows)
    }
}

pub struct Update<'txn, 'db, T: Storage> {
    pub source: Box<ExecutorNode<'txn, 'db, T>>,
    pub table: Table<'txn, 'db, T>,
}

impl<T: Storage> Node for Update<'_, '_, T> {
    fn next_row(&mut self) -> Output {
        for row in self.source.by_ref() {
            self.table.update(&row?)?;
        }
        Err(NodeError::EndOfRows)
    }
}

pub struct Delete<'txn, 'db, T: Storage> {
    pub source: Box<ExecutorNode<'txn, 'db, T>>,
    pub table: Table<'txn, 'db, T>,
}

impl<T: Storage> Node for Delete<'_, '_, T> {
    fn next_row(&mut self) -> Output {
        for row in self.source.by_ref() {
            self.table.delete(&row?)?;
        }
        Err(NodeError::EndOfRows)
    }
}
