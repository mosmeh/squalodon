use super::StorageResult;
use crate::{catalog::Sequence, StorageError};

impl Sequence<'_> {
    /// Increments the sequence and returns the next value.
    pub fn next_value(&self) -> StorageResult<i64> {
        let next_value = match self.current_value()? {
            Some(value) => {
                let next = value + self.increment_by();
                if self.min_value() <= next && next <= self.max_value() {
                    next
                } else if self.cycle() {
                    self.start_value()
                } else {
                    return Err(StorageError::SequenceOverflow);
                }
            }
            None => self.start_value(),
        };
        let key = self.id().serialize();
        let _ = self.transaction().insert(&key, &next_value.to_be_bytes());
        Ok(next_value)
    }

    /// Returns the current value of the sequence.
    ///
    /// Returns None if the sequence has not been incremented yet.
    fn current_value(&self) -> StorageResult<Option<i64>> {
        let key = self.id().serialize();
        let Some(bytes) = self.transaction().get(&key) else {
            return Ok(None);
        };
        let bytes = bytes
            .try_into()
            .map_err(|_| StorageError::InvalidEncoding)?;
        Ok(Some(i64::from_be_bytes(bytes)))
    }

    /// Resets the sequence to the state that it has not been incremented yet.
    pub fn reset(&self) {
        let key = self.id().serialize();
        let _ = self.transaction().remove(&key);
    }
}
