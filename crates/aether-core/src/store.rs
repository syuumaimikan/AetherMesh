//! Content-addressed byte store.
//!
//! Keys are content hashes, so storing the same bytes twice is a no-op — which
//! is what keeps the mesh from transferring identical data more than once.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::data::{DataDescriptor, DataId};

/// Shared store of data blobs. Cheap to clone.
#[derive(Debug, Clone, Default)]
pub struct DataStore {
    inner: Arc<Mutex<HashMap<DataId, Arc<[u8]>>>>,
}

impl DataStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Stores bytes under their own hash and returns the descriptor.
    pub fn put(&self, bytes: Vec<u8>) -> DataDescriptor {
        let descriptor = DataDescriptor::of(&bytes);
        self.lock()
            .entry(descriptor.id)
            .or_insert_with(|| bytes.into());
        descriptor
    }

    /// Stores bytes that arrived with a descriptor already attached.
    ///
    /// Returns `false` when the data was already present, and
    /// [`DataStoreError::HashMismatch`] when the bytes do not match the descriptor.
    pub fn insert(
        &self,
        descriptor: DataDescriptor,
        bytes: Vec<u8>,
    ) -> Result<bool, DataStoreError> {
        let actual = DataDescriptor::of(&bytes);
        if actual.id != descriptor.id {
            return Err(DataStoreError::HashMismatch {
                expected: descriptor.id,
                actual: actual.id,
            });
        }

        let mut store = self.lock();
        if store.contains_key(&descriptor.id) {
            return Ok(false);
        }
        store.insert(descriptor.id, bytes.into());
        Ok(true)
    }

    pub fn get(&self, data_id: DataId) -> Option<Arc<[u8]>> {
        self.lock().get(&data_id).cloned()
    }

    pub fn contains(&self, data_id: DataId) -> bool {
        self.lock().contains_key(&data_id)
    }

    pub fn remove(&self, data_id: DataId) -> bool {
        self.lock().remove(&data_id).is_some()
    }

    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// Bytes held across every blob.
    pub fn total_bytes(&self) -> u64 {
        self.lock().values().map(|bytes| bytes.len() as u64).sum()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<DataId, Arc<[u8]>>> {
        self.inner.lock().expect("data store mutex poisoned")
    }
}

/// Storing data failed.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DataStoreError {
    #[error("data hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: DataId, actual: DataId },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_bytes_are_stored_once() {
        let store = DataStore::new();
        let first = store.put(b"payload".to_vec());
        let second = store.put(b"payload".to_vec());

        assert_eq!(first, second);
        assert_eq!(store.len(), 1);
        assert_eq!(store.total_bytes(), 7);
    }

    #[test]
    fn different_bytes_get_different_ids() {
        let store = DataStore::new();
        let a = store.put(b"a".to_vec());
        let b = store.put(b"b".to_vec());

        assert_ne!(a.id, b.id);
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn stored_bytes_can_be_read_back() {
        let store = DataStore::new();
        let descriptor = store.put(b"aethermesh".to_vec());

        assert_eq!(store.get(descriptor.id).unwrap().as_ref(), b"aethermesh");
        assert!(store.contains(descriptor.id));
        assert!(store.remove(descriptor.id));
        assert!(store.is_empty());
    }

    #[test]
    fn insert_reports_whether_the_data_was_new() {
        let store = DataStore::new();
        let descriptor = DataDescriptor::of(b"chunk");

        assert!(store.insert(descriptor, b"chunk".to_vec()).unwrap());
        assert!(!store.insert(descriptor, b"chunk".to_vec()).unwrap());
    }

    #[test]
    fn insert_rejects_bytes_that_do_not_match_the_descriptor() {
        let store = DataStore::new();
        let descriptor = DataDescriptor::of(b"expected");

        let error = store.insert(descriptor, b"tampered".to_vec()).unwrap_err();
        assert!(matches!(error, DataStoreError::HashMismatch { .. }));
        assert!(store.is_empty());
    }
}
