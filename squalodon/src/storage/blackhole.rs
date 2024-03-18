use super::Storage;
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

    fn transaction(&self) -> Transaction {
        Transaction {
            phantom: PhantomData,
        }
    }
}

pub struct Transaction<'a> {
    phantom: PhantomData<&'a Blackhole>,
}

impl super::Transaction for Transaction<'_> {
    fn get(&self, _key: &[u8]) -> Option<Vec<u8>> {
        None
    }

    fn scan<const N: usize>(
        &self,
        _start: [u8; N],
        _end: [u8; N],
    ) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> {
        std::iter::empty()
    }

    fn insert(&self, _key: Vec<u8>, _value: Vec<u8>) -> bool {
        true
    }

    fn remove(&self, _key: Vec<u8>) -> Option<Vec<u8>> {
        None
    }

    fn commit(self) {}
}
