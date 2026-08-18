//! Content-addressed byte store.
//!
//! Keys are content hashes, so storing the same bytes twice is a no-op — which
//! is what keeps the mesh from transferring identical data more than once.
//!
//! A store can be given a byte budget. Without one it grows forever, which is
//! fine for a controller that is publishing what it was asked to publish, and
//! not fine for an agent on a Raspberry Pi that has been receiving other
//! people's datasets for a week. Over budget, the least recently used blobs go
//! first, and the caller is told which ones so it can say so.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::data::{DataDescriptor, DataId};

/// Shared store of data blobs. Cheap to clone.
#[derive(Debug, Clone, Default)]
pub struct DataStore {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug, Default)]
struct Inner {
    blobs: HashMap<DataId, Entry>,
    /// Bytes currently held, kept incrementally so a budget check is not a scan.
    bytes: u64,
    /// `None` means unbounded.
    budget: Option<u64>,
    /// Monotonic tick stamped on each access. Cheaper than a linked list, and
    /// this store holds thousands of blobs at most, not millions.
    clock: u64,
}

#[derive(Debug)]
struct Entry {
    bytes: Arc<[u8]>,
    used_at: u64,
}

impl DataStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// A store that holds at most `budget` bytes, evicting least-recently-used
    /// blobs to stay under it.
    ///
    /// The budget is not a hard ceiling on the process: a blob larger than the
    /// whole budget is still stored, because refusing it would fail a task the
    /// mesh has already decided to run here. It is evicted at the next
    /// opportunity like anything else.
    pub fn with_budget(budget: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                budget: Some(budget),
                ..Inner::default()
            })),
        }
    }

    /// The byte budget, if this store has one.
    pub fn budget(&self) -> Option<u64> {
        self.lock().budget
    }

    /// Stores bytes under their own hash and returns the descriptor.
    ///
    /// Use [`DataStore::put_evicting`] when something needs to know what was
    /// dropped to make room.
    pub fn put(&self, bytes: Vec<u8>) -> DataDescriptor {
        self.put_evicting(bytes).0
    }

    /// Same, also returning the ids evicted to stay inside the budget.
    pub fn put_evicting(&self, bytes: Vec<u8>) -> (DataDescriptor, Vec<DataId>) {
        let descriptor = DataDescriptor::of(&bytes);
        let mut store = self.lock();
        let evicted = store.store(descriptor.id, bytes);
        (descriptor, evicted)
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
        self.insert_evicting(descriptor, bytes)
            .map(|(stored, _)| stored)
    }

    /// Same, also returning the ids evicted to stay inside the budget.
    pub fn insert_evicting(
        &self,
        descriptor: DataDescriptor,
        bytes: Vec<u8>,
    ) -> Result<(bool, Vec<DataId>), DataStoreError> {
        let actual = DataDescriptor::of(&bytes);
        if actual.id != descriptor.id {
            return Err(DataStoreError::HashMismatch {
                expected: descriptor.id,
                actual: actual.id,
            });
        }

        let mut store = self.lock();
        if store.blobs.contains_key(&descriptor.id) {
            store.touch(descriptor.id);
            return Ok((false, Vec::new()));
        }

        let evicted = store.store(descriptor.id, bytes);
        Ok((true, evicted))
    }

    /// Reads a blob, marking it as recently used.
    ///
    /// The returned handle keeps the bytes alive even if the blob is evicted
    /// while a task is still reading it.
    pub fn get(&self, data_id: DataId) -> Option<Arc<[u8]>> {
        let mut store = self.lock();
        store.touch(data_id);
        store.blobs.get(&data_id).map(|entry| entry.bytes.clone())
    }

    pub fn contains(&self, data_id: DataId) -> bool {
        self.lock().blobs.contains_key(&data_id)
    }

    pub fn remove(&self, data_id: DataId) -> bool {
        self.lock().drop_blob(data_id)
    }

    pub fn len(&self) -> usize {
        self.lock().blobs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().blobs.is_empty()
    }

    /// What this store holds, as the controller's catalog describes datasets.
    ///
    /// Ordered by id so two calls agree, which matters because this is what a
    /// node tells a controller it has after reconnecting.
    pub fn descriptors(&self) -> Vec<DataDescriptor> {
        let store = self.lock();
        let mut held: Vec<DataDescriptor> = store
            .blobs
            .iter()
            .map(|(id, entry)| DataDescriptor::new(*id, entry.bytes.len() as u64))
            .collect();
        held.sort_unstable_by_key(|descriptor| descriptor.id);
        held
    }

    /// Bytes held across every blob.
    pub fn total_bytes(&self) -> u64 {
        self.lock().bytes
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("data store mutex poisoned")
    }
}

impl Inner {
    /// Inserts a blob and evicts until the budget is satisfied.
    fn store(&mut self, data_id: DataId, bytes: Vec<u8>) -> Vec<DataId> {
        if self.blobs.contains_key(&data_id) {
            self.touch(data_id);
            return Vec::new();
        }

        self.clock += 1;
        let used_at = self.clock;
        self.bytes += bytes.len() as u64;
        self.blobs.insert(
            data_id,
            Entry {
                bytes: bytes.into(),
                used_at,
            },
        );

        self.evict_over_budget(data_id)
    }

    /// Drops least-recently-used blobs until the budget is met.
    ///
    /// `keep` is the blob just stored: evicting it immediately would fail the
    /// task it was fetched for, so it survives even if it alone is over budget.
    fn evict_over_budget(&mut self, keep: DataId) -> Vec<DataId> {
        let Some(budget) = self.budget else {
            return Vec::new();
        };

        let mut evicted = Vec::new();
        while self.bytes > budget {
            let oldest = self
                .blobs
                .iter()
                .filter(|(id, _)| **id != keep)
                .min_by_key(|(_, entry)| entry.used_at)
                .map(|(id, _)| *id);

            match oldest {
                Some(id) => {
                    self.drop_blob(id);
                    evicted.push(id);
                }
                // Only the blob we must keep is left, and it is still too big.
                None => break,
            }
        }
        evicted
    }

    fn drop_blob(&mut self, data_id: DataId) -> bool {
        match self.blobs.remove(&data_id) {
            Some(entry) => {
                self.bytes -= entry.bytes.len() as u64;
                true
            }
            None => false,
        }
    }

    fn touch(&mut self, data_id: DataId) {
        self.clock += 1;
        let clock = self.clock;
        if let Some(entry) = self.blobs.get_mut(&data_id) {
            entry.used_at = clock;
        }
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
    fn a_store_can_describe_what_it_holds() {
        let store = DataStore::new();
        let first = store.put(vec![1u8; 100]);
        let second = store.put(vec![2u8; 250]);

        let held = store.descriptors();

        assert_eq!(held.len(), 2);
        assert_eq!(held.iter().map(|d| d.size_bytes).sum::<u64>(), 350);
        assert!(held.contains(&first) && held.contains(&second));
        // Ordered, so a node reporting twice reports the same thing twice.
        assert_eq!(held, store.descriptors());
    }

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
        assert_eq!(store.total_bytes(), 0);
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

    #[test]
    fn without_a_budget_nothing_is_ever_evicted() {
        let store = DataStore::new();
        assert_eq!(store.budget(), None);

        for index in 0u32..50 {
            let (_, evicted) = store.put_evicting(vec![index as u8; 1024]);
            assert!(evicted.is_empty());
        }
        assert_eq!(store.len(), 50);
    }

    #[test]
    fn a_budget_evicts_the_least_recently_used_blob() {
        let store = DataStore::with_budget(300);
        let first = store.put(vec![1u8; 100]);
        let second = store.put(vec![2u8; 100]);
        let third = store.put(vec![3u8; 100]);

        // Reading the first one makes the second the oldest.
        store.get(first.id);
        let (fourth, evicted) = store.put_evicting(vec![4u8; 100]);

        assert_eq!(evicted, vec![second.id]);
        assert!(store.contains(first.id));
        assert!(store.contains(third.id));
        assert!(store.contains(fourth.id));
        assert_eq!(store.total_bytes(), 300);
    }

    #[test]
    fn several_blobs_go_if_one_large_arrival_needs_the_room() {
        let store = DataStore::with_budget(300);
        let small = [
            store.put(vec![1u8; 100]),
            store.put(vec![2u8; 100]),
            store.put(vec![3u8; 100]),
        ];

        let (big, evicted) = store.put_evicting(vec![9u8; 250]);

        assert_eq!(evicted.len(), 3, "all three had to go to fit 250 bytes");
        assert_eq!(evicted[0], small[0].id, "oldest first");
        assert!(store.contains(big.id));
        assert_eq!(store.total_bytes(), 250);
    }

    #[test]
    fn a_blob_larger_than_the_whole_budget_is_still_stored() {
        let store = DataStore::with_budget(100);
        let (big, evicted) = store.put_evicting(vec![7u8; 4096]);

        // Refusing it would fail a task the mesh already decided to run here.
        assert!(store.contains(big.id));
        assert!(evicted.is_empty());
        assert_eq!(store.total_bytes(), 4096);

        // It is not privileged afterwards: the next arrival displaces it.
        let (next, evicted) = store.put_evicting(vec![8u8; 50]);
        assert_eq!(evicted, vec![big.id]);
        assert!(store.contains(next.id));
    }

    #[test]
    fn an_evicted_blob_stays_readable_through_a_handle_already_taken() {
        let store = DataStore::with_budget(100);
        let held = store.put(vec![1u8; 100]);
        let bytes = store.get(held.id).expect("just stored");

        store.put(vec![2u8; 100]);

        // A task running on a blocking thread must not lose its input midway.
        assert!(!store.contains(held.id));
        assert_eq!(bytes.len(), 100);
        assert!(bytes.iter().all(|byte| *byte == 1));
    }

    #[test]
    fn re_receiving_a_held_blob_refreshes_it_rather_than_duplicating_it() {
        let store = DataStore::with_budget(250);
        let first = store.put(vec![1u8; 100]);
        let second = store.put(vec![2u8; 100]);

        let descriptor = DataDescriptor::of(&[1u8; 100]);
        let (stored, evicted) = store
            .insert_evicting(descriptor, vec![1u8; 100])
            .expect("hash matches");
        assert!(!stored, "already held");
        assert!(evicted.is_empty());
        assert_eq!(store.total_bytes(), 200);

        // The refresh made the second blob the oldest.
        let (_, evicted) = store.put_evicting(vec![3u8; 100]);
        assert_eq!(evicted, vec![second.id]);
        assert!(store.contains(first.id));
    }
}
