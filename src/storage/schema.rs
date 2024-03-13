use crate::types::Type;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableId(pub u64);

impl TableId {
    pub const CATALOG: Self = Self(0);
    pub const MAX_SYSTEM: Self = Self::CATALOG;

    const ENCODED_LEN: usize = std::mem::size_of::<u64>();

    pub fn serialize(self) -> [u8; Self::ENCODED_LEN] {
        let mut buf = [0; Self::ENCODED_LEN];
        self.serialize_into(&mut buf);
        buf
    }

    pub fn serialize_into(self, buf: &mut [u8; Self::ENCODED_LEN]) {
        buf.copy_from_slice(&self.0.to_be_bytes());
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableRef<'a> {
    pub id: TableId,
    pub name: &'a str,
    pub columns: &'a [Column],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub ty: Type,
    pub is_primary_key: bool,
    pub is_nullable: bool,
}
