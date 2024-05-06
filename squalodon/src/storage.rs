mod blackhole;
mod index;
mod memory;
mod sequence;
mod table;

pub use blackhole::Blackhole;
pub use memory::Memory;

use std::ops::{Bound, RangeBounds};

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Duplicate key")]
    DuplicateKey,

    #[error("NOT NULL constraint violated at column {0:?}")]
    NotNullConstraintViolation(String),

    #[error("Reached maximum or minimum value of sequence")]
    SequenceOverflow,

    #[error("Invalid encoding")]
    InvalidEncoding,

    #[error("Storage is in an inconsistent state")]
    Inconsistent,

    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
}

pub(crate) type StorageResult<T> = std::result::Result<T, StorageError>;

pub trait Storage {
    type Transaction<'a>: Transaction
    where
        Self: 'a;

    fn transaction(&self) -> Self::Transaction<'_>;
}

pub trait Transaction {
    /// Returns the value associated with the key.
    ///
    /// Returns None if the key does not exist.
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;

    /// Returns an iterator over the key-value pairs
    /// in the key range `[start, end)`.
    fn scan(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + '_>;

    /// Inserts a key-value pair.
    ///
    /// Returns true if the key was newly inserted, false if it already existed.
    #[must_use]
    fn insert(&self, key: &[u8], value: &[u8]) -> bool;

    /// Removes a key from the storage.
    ///
    /// Returns the value if the key was removed, None if it did not exist.
    #[must_use]
    fn remove(&self, key: &[u8]) -> Option<Vec<u8>>;

    /// Commits the transaction.
    ///
    /// The transaction rolls back if it is dropped without being committed.
    fn commit(self);
}

pub(crate) trait TransactionExt {
    fn prefix_scan(&self, prefix: Vec<u8>) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + '_;
    fn prefix_range_scan<R: RangeBounds<Vec<u8>>>(
        &self,
        prefix: Vec<u8>,
        range: R,
    ) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + '_;
}

impl<T: ?Sized + Transaction> TransactionExt for T {
    fn prefix_scan(&self, prefix: Vec<u8>) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + '_ {
        let mut end = prefix.clone();
        if let Some(last) = end.last_mut() {
            *last += 1;
        }
        self.scan(prefix, end)
    }

    fn prefix_range_scan<R: RangeBounds<Vec<u8>>>(
        &self,
        prefix: Vec<u8>,
        range: R,
    ) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + '_ {
        let mut start = prefix.clone();
        if let Bound::Included(s) | Bound::Excluded(s) = range.start_bound() {
            start.extend_from_slice(s);
        }
        if let Bound::Excluded(_) = range.start_bound() {
            if let Some(last) = start.last_mut() {
                *last += 1;
            }
        }

        let mut end = prefix;
        if let Bound::Included(e) | Bound::Excluded(e) = range.end_bound() {
            end.extend_from_slice(e);
        }
        if let Bound::Included(_) | Bound::Unbounded = range.end_bound() {
            if let Some(last) = end.last_mut() {
                *last += 1;
            }
        }

        self.scan(start, end)
    }
}
