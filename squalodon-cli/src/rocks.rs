use rocksdb::{Direction, IteratorMode, TransactionDB};
use squalodon::storage::Storage;

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

    fn transaction(&self) -> Self::Transaction<'_> {
        Transaction(Some(self.db.transaction()))
    }
}

pub struct Transaction<'a>(Option<rocksdb::Transaction<'a, TransactionDB>>);

impl squalodon::storage::Transaction for Transaction<'_> {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.inner().get(key).unwrap()
    }

    fn scan(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + '_> {
        let iter = self
            .inner()
            .iterator(IteratorMode::From(&start, Direction::Forward))
            .map(std::result::Result::unwrap)
            .take_while(move |(k, _)| k.as_ref() < &end)
            .map(|(k, v)| (k.to_vec(), v.to_vec()));
        Box::new(iter)
    }

    fn insert(&self, key: &[u8], value: &[u8]) -> bool {
        let inner = self.inner();
        let was_present = inner.get(key).unwrap().is_some();
        inner.put(key, value).unwrap();
        !was_present
    }

    fn remove(&self, key: &[u8]) -> Option<Vec<u8>> {
        let inner = self.inner();
        inner.get(key).unwrap().map(|value| {
            inner.delete(key).unwrap();
            value
        })
    }

    fn commit(mut self) {
        self.0.take().unwrap().commit().unwrap();
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if let Some(inner) = self.0.take() {
            inner.rollback().unwrap();
        }
    }
}

impl Transaction<'_> {
    fn inner(&self) -> &rocksdb::Transaction<TransactionDB> {
        self.0.as_ref().unwrap()
    }
}
