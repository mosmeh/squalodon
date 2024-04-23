use super::Storage;
use crossbeam_skiplist::SkipMap;
use std::{
    cell::RefCell,
    collections::BTreeMap,
    sync::{Mutex, MutexGuard},
};

/// In-memory storage engine.
#[derive(Default)]
pub struct Memory {
    data: SkipMap<Box<[u8]>, Box<[u8]>>,
    txn: Mutex<()>,
}

impl Memory {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Storage for Memory {
    type Transaction<'a> = Transaction<'a>;

    fn transaction(&self) -> Transaction {
        Transaction {
            data: &self.data,
            undo_set: BTreeMap::new().into(),
            is_committed: false,
            _guard: self.txn.lock().unwrap(),
        }
    }
}

type UndoSet = BTreeMap<Box<[u8]>, Option<Box<[u8]>>>;

pub struct Transaction<'a> {
    data: &'a SkipMap<Box<[u8]>, Box<[u8]>>,
    undo_set: RefCell<UndoSet>,
    is_committed: bool,
    _guard: MutexGuard<'a, ()>,
}

impl super::Transaction for Transaction<'_> {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.data
            .get(key)
            .map(|entry| entry.value().clone().into_vec())
    }

    fn scan(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + '_> {
        let iter = self
            .data
            .range(start.into_boxed_slice()..end.into_boxed_slice())
            .map(|entry| {
                (
                    entry.key().clone().into_vec(),
                    entry.value().clone().into_vec(),
                )
            });
        Box::new(iter)
    }

    fn insert(&self, key: &[u8], value: &[u8]) -> bool {
        let prev_value = self.data.get(key).map(|entry| entry.value().clone());
        let was_present = prev_value.is_some();
        self.data.insert(
            key.to_vec().into_boxed_slice(),
            value.to_vec().into_boxed_slice(),
        );
        self.undo_set
            .borrow_mut()
            .entry(key.to_vec().into_boxed_slice())
            .or_insert(prev_value);
        !was_present
    }

    fn remove(&self, key: &[u8]) -> Option<Vec<u8>> {
        let value = self.data.remove(key).map(|entry| entry.value().clone());
        self.undo_set
            .borrow_mut()
            .entry(key.to_vec().into_boxed_slice())
            .or_insert_with(|| value.clone());
        value.map(<[_]>::into_vec)
    }

    fn commit(mut self) {
        self.is_committed = true;
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if self.is_committed {
            return;
        }
        for (key, value) in std::mem::take(&mut self.undo_set).into_inner() {
            match value {
                Some(value) => {
                    self.data.insert(key, value);
                }
                None => {
                    self.data.remove(&key);
                }
            }
        }
    }
}
