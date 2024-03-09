use super::{Result, TableId, TableKeyPrefix, Transaction};
use crate::{serde::SerdeOptions, Value};

pub struct SchemaAwareTransaction<T> {
    inner: T,
    table_scan_start_key: [u8; TableKeyPrefix::ENCODED_LEN],
    table_scan_end_key: [u8; TableKeyPrefix::ENCODED_LEN],
}

impl<T> SchemaAwareTransaction<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            table_scan_start_key: [0; TableKeyPrefix::ENCODED_LEN],
            table_scan_end_key: [0; TableKeyPrefix::ENCODED_LEN],
        }
    }

    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T: Transaction> SchemaAwareTransaction<T> {
    pub fn scan_table(&mut self, table: TableId) -> impl Iterator<Item = Result<Vec<Value>>> + '_ {
        self.table_scan_start_key
            .copy_from_slice(&TableKeyPrefix::TableData(table).serialize());
        self.table_scan_end_key
            .copy_from_slice(&self.table_scan_start_key);
        self.table_scan_end_key[self.table_scan_end_key.len() - 1] += 1;
        self.inner
            .scan(&self.table_scan_start_key, &self.table_scan_end_key)
            .map(|(_, v)| bincode::deserialize(v).map_err(Into::into))
    }

    pub fn update(&mut self, table: TableId, primary_key: &Value, columns: &[Value]) {
        let mut key = TableKeyPrefix::TableData(table).serialize().to_vec();
        SerdeOptions::new().serialize_into(primary_key, &mut key);
        self.inner
            .insert(key, bincode::serialize(&columns).unwrap());
    }

    pub fn commit(self) {
        self.inner.commit();
    }
}
