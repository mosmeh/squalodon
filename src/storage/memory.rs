use super::KeyValueStore;
use crossbeam_skiplist::SkipMap;
use std::{
    cell::RefCell,
    collections::BTreeMap,
    sync::{Mutex, MutexGuard},
};

#[derive(Default)]
pub struct Memory {
    data: SkipMap<Vec<u8>, Vec<u8>>,
    txn: Mutex<()>,
}

impl Memory {
    pub fn new() -> Self {
        Self::default()
    }
}

impl KeyValueStore for Memory {
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

pub struct Transaction<'a> {
    data: &'a SkipMap<Vec<u8>, Vec<u8>>,
    undo_set: RefCell<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
    is_committed: bool,
    _guard: MutexGuard<'a, ()>,
}

impl super::KeyValueTransaction for Transaction<'_> {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.data.get(key).map(|entry| entry.value().clone())
    }

    fn scan<const N: usize>(
        &self,
        start: [u8; N],
        end: [u8; N],
    ) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> {
        self.data
            .range(start.to_vec()..end.to_vec())
            .map(|entry| (entry.key().clone(), entry.value().clone()))
    }

    fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> bool {
        if self.data.contains_key(&key) {
            return false;
        }
        self.data.insert(key.clone(), value);
        self.undo_set.borrow_mut().entry(key).or_insert(None);
        true
    }

    fn update(&self, key: Vec<u8>, value: Vec<u8>) -> bool {
        let Some(prev_value) = self.data.get(&key) else {
            return false;
        };
        self.data.insert(key.clone(), value);
        self.undo_set
            .borrow_mut()
            .entry(key)
            .or_insert_with(|| Some(prev_value.value().clone()));
        true
    }

    fn remove(&self, key: Vec<u8>) -> Option<Vec<u8>> {
        let value = self.data.remove(&key).map(|entry| entry.value().clone());
        self.undo_set
            .borrow_mut()
            .entry(key)
            .or_insert_with(|| value.clone());
        value
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
