mod blackhole;
mod index;
mod memory;
mod sequence;
mod table;

pub use blackhole::Blackhole;
pub use memory::Memory;

use std::{
    error::Error,
    ops::{Bound, RangeBounds},
};

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

    #[error("Storage backend error: {0}")]
    Backend(#[source] BackendError),

    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
}

pub(crate) type StorageResult<T> = Result<T, StorageError>;

pub type BackendResult<T> = Result<T, BackendError>;
pub type BackendError = Box<dyn Error + Send + Sync>;

pub trait Storage {
    type Transaction<'a>: Transaction
    where
        Self: 'a;

    fn transaction(&self) -> BackendResult<Self::Transaction<'_>>;
}

pub type ScanIter<'a> = dyn Iterator<Item = BackendResult<(Vec<u8>, Vec<u8>)>> + 'a;

pub trait Transaction {
    /// Returns the value associated with the key.
    ///
    /// Returns None if the key does not exist.
    fn get(&self, key: &[u8]) -> BackendResult<Option<Vec<u8>>>;

    /// Returns an iterator over the key-value pairs
    /// in the key range `[start, end)`.
    fn scan(&self, start: Vec<u8>, end: Vec<u8>) -> Box<ScanIter>;

    /// Inserts a key-value pair.
    ///
    /// Returns true if the key was newly inserted, false if it already existed.
    fn insert(&self, key: &[u8], value: &[u8]) -> BackendResult<bool>;

    /// Removes a key from the storage.
    ///
    /// Returns the value if the key was removed, None if it did not exist.
    fn remove(&self, key: &[u8]) -> BackendResult<Option<Vec<u8>>>;

    /// Commits the transaction.
    ///
    /// The transaction rolls back if it is dropped without being committed.
    fn commit(self) -> BackendResult<()>;
}

pub(crate) trait TransactionExt {
    fn prefix_scan(
        &self,
        prefix: Vec<u8>,
    ) -> impl Iterator<Item = Result<(Vec<u8>, Vec<u8>), BackendError>> + '_;
    fn prefix_range_scan<R: RangeBounds<Vec<u8>>>(
        &self,
        prefix: Vec<u8>,
        range: R,
    ) -> impl Iterator<Item = Result<(Vec<u8>, Vec<u8>), BackendError>> + '_;
}

impl<T: ?Sized + Transaction> TransactionExt for T {
    fn prefix_scan(
        &self,
        prefix: Vec<u8>,
    ) -> impl Iterator<Item = Result<(Vec<u8>, Vec<u8>), BackendError>> + '_ {
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
    ) -> impl Iterator<Item = Result<(Vec<u8>, Vec<u8>), BackendError>> + '_ {
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
