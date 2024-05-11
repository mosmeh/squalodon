use super::{BackendResult, ScanIter, Storage};
use std::marker::PhantomData;

/// A storage engine that discards all data.
#[derive(Default)]
pub struct Blackhole {
    _dummy: (),
}

impl Blackhole {
    pub fn new() -> Self {
        Default::default()
    }
}

impl Storage for Blackhole {
    type Transaction<'a> = Transaction<'a>;

    fn transaction(&self) -> BackendResult<Transaction> {
        Ok(Transaction {
            phantom: PhantomData,
        })
    }
}

pub struct Transaction<'a> {
    phantom: PhantomData<&'a Blackhole>,
}

impl super::Transaction for Transaction<'_> {
    fn get(&self, _key: &[u8]) -> BackendResult<Option<Vec<u8>>> {
        Ok(None)
    }

    fn scan(&self, _start: Vec<u8>, _end: Vec<u8>) -> Box<ScanIter> {
        Box::new(std::iter::empty())
    }

    fn insert(&self, _key: &[u8], _value: &[u8]) -> BackendResult<bool> {
        Ok(true)
    }

    fn remove(&self, _key: &[u8]) -> BackendResult<Option<Vec<u8>>> {
        Ok(None)
    }

    fn commit(self) -> BackendResult<()> {
        Ok(())
    }
}
