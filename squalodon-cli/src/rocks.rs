use rocksdb::{Direction, IteratorMode, TransactionDB};
use squalodon::storage::{BackendResult, ScanIter, Storage};

pub struct RocksDB {
    db: TransactionDB,
}

impl RocksDB {
    pub fn new(db: TransactionDB) -> Self {
        Self { db }
    }
}

impl Storage for RocksDB {
    type Transaction<'a> = Transaction<'a>;

    fn transaction(&self) -> BackendResult<Self::Transaction<'_>> {
        Ok(Transaction(Some(self.db.transaction())))
    }
}

pub struct Transaction<'a>(Option<rocksdb::Transaction<'a, TransactionDB>>);

impl squalodon::storage::Transaction for Transaction<'_> {
    fn get(&self, key: &[u8]) -> BackendResult<Option<Vec<u8>>> {
        self.inner().get(key).map_err(Into::into)
    }

    fn scan(&self, start: Vec<u8>, end: Vec<u8>) -> Box<ScanIter> {
        let iter = self
            .inner()
            .iterator(IteratorMode::From(&start, Direction::Forward))
            .take_while(move |r| match r {
                Ok((k, _)) => k.as_ref() < &end,
                Err(_) => true,
            })
            .map(|r| match r {
                Ok((k, v)) => Ok((k.to_vec(), v.to_vec())),
                Err(e) => Err(e.into()),
            });
        Box::new(iter)
    }

    fn insert(&self, key: &[u8], value: &[u8]) -> BackendResult<bool> {
        let inner = self.inner();
        let was_present = inner.get(key)?.is_some();
        inner.put(key, value)?;
        Ok(!was_present)
    }

    fn remove(&self, key: &[u8]) -> BackendResult<Option<Vec<u8>>> {
        let inner = self.inner();
        inner
            .get(key)?
            .map(|value| {
                inner.delete(key)?;
                Ok(value)
            })
            .transpose()
    }

    fn commit(mut self) -> BackendResult<()> {
        self.0.take().unwrap().commit().map_err(Into::into)
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if let Some(inner) = self.0.take() {
            let _ = inner.rollback();
        }
    }
}

impl Transaction<'_> {
    fn inner(&self) -> &rocksdb::Transaction<TransactionDB> {
        self.0.as_ref().unwrap()
    }
}
