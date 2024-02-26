use super::Storage;
use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::{Mutex, MutexGuard},
};

#[derive(Default)]
pub struct Memory {
    data: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
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
            data: self.data.lock().unwrap(),
            undo_set: BTreeMap::new(),
            is_committed: false,
        }
    }
}

pub struct Transaction<'a> {
    data: MutexGuard<'a, BTreeMap<Vec<u8>, Vec<u8>>>,
    undo_set: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    is_committed: bool,
}

impl super::Transaction for Transaction<'_> {
    fn scan(&self, start: &[u8], end: &[u8]) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.data
            .range(start.to_vec()..end.to_vec())
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
    }

    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        match self.data.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                self.undo_set
                    .entry(key)
                    .or_insert_with(|| Some(entry.get().clone()));
                entry.insert(value);
            }
            Entry::Vacant(entry) => {
                self.undo_set.entry(key).or_insert(None);
                entry.insert(value);
            }
        }
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
        for (key, value) in std::mem::take(&mut self.undo_set) {
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
