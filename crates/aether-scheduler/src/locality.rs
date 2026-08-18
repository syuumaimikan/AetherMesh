//! Which node holds which dataset.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use aether_core::{DataDescriptor, DataId, NodeId};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    descriptor: DataDescriptor,
    locations: HashSet<NodeId>,
}

/// Tracks where each dataset currently lives. Cheap to clone: the controller
/// updates it while the scheduler reads it.
#[derive(Debug, Clone, Default)]
pub struct DataCatalog {
    inner: Arc<Mutex<HashMap<DataId, Entry>>>,
}

impl DataCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records that `node_id` holds this dataset.
    pub fn record(&self, descriptor: DataDescriptor, node_id: NodeId) {
        self.lock()
            .entry(descriptor.id)
            .or_insert_with(|| Entry {
                descriptor,
                locations: HashSet::new(),
            })
            .locations
            .insert(node_id);
    }

    /// Records that `node_id` no longer holds this dataset.
    pub fn remove(&self, data_id: DataId, node_id: NodeId) {
        let mut catalog = self.lock();
        if let Some(entry) = catalog.get_mut(&data_id) {
            entry.locations.remove(&node_id);
            if entry.locations.is_empty() {
                catalog.remove(&data_id);
            }
        }
    }

    /// Drops every dataset held only by a node that left the mesh.
    pub fn forget_node(&self, node_id: NodeId) {
        let mut catalog = self.lock();
        catalog.retain(|_, entry| {
            entry.locations.remove(&node_id);
            !entry.locations.is_empty()
        });
    }

    pub fn descriptor(&self, data_id: DataId) -> Option<DataDescriptor> {
        self.lock().get(&data_id).map(|entry| entry.descriptor)
    }

    /// Nodes currently holding a dataset.
    pub fn locations(&self, data_id: DataId) -> Vec<NodeId> {
        self.lock()
            .get(&data_id)
            .map(|entry| entry.locations.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn holds(&self, data_id: DataId, node_id: NodeId) -> bool {
        self.lock()
            .get(&data_id)
            .is_some_and(|entry| entry.locations.contains(&node_id))
    }

    /// Total bytes of `inputs` that `node_id` already has locally.
    pub fn local_bytes(&self, node_id: NodeId, inputs: &[DataId]) -> u64 {
        let catalog = self.lock();
        inputs
            .iter()
            .filter_map(|data_id| catalog.get(data_id))
            .filter(|entry| entry.locations.contains(&node_id))
            .map(|entry| entry.descriptor.size_bytes)
            .sum()
    }

    /// Datasets this node holds, and their total size.
    ///
    /// A dataset counted here is one this node will not have to be sent again,
    /// which is what makes it worth showing next to the node.
    pub fn held_by(&self, node_id: NodeId) -> (usize, u64) {
        self.lock()
            .values()
            .filter(|entry| entry.locations.contains(&node_id))
            .fold((0, 0), |(count, bytes), entry| {
                (count + 1, bytes + entry.descriptor.size_bytes)
            })
    }

    /// Datasets known to the mesh, and their total size.
    pub fn totals(&self) -> (usize, u64) {
        let catalog = self.lock();
        (
            catalog.len(),
            catalog
                .values()
                .map(|entry| entry.descriptor.size_bytes)
                .sum(),
        )
    }

    /// Number of datasets known to the mesh.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<DataId, Entry>> {
        self.inner.lock().expect("data catalog mutex poisoned")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data(seed: u8, size: u64) -> DataDescriptor {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        DataDescriptor::new(DataId::from_bytes(bytes), size)
    }

    #[test]
    fn a_dataset_can_live_on_several_nodes() {
        let catalog = DataCatalog::new();
        let descriptor = data(1, 4096);
        let first = NodeId::generate();
        let second = NodeId::generate();

        catalog.record(descriptor, first);
        catalog.record(descriptor, second);

        let mut locations = catalog.locations(descriptor.id);
        locations.sort();
        let mut expected = vec![first, second];
        expected.sort();
        assert_eq!(locations, expected);
        assert_eq!(catalog.descriptor(descriptor.id), Some(descriptor));
    }

    #[test]
    fn removing_the_last_location_forgets_the_dataset() {
        let catalog = DataCatalog::new();
        let descriptor = data(2, 10);
        let node_id = NodeId::generate();
        catalog.record(descriptor, node_id);

        catalog.remove(descriptor.id, node_id);

        assert!(catalog.is_empty());
        assert_eq!(catalog.descriptor(descriptor.id), None);
    }

    #[test]
    fn forgetting_a_node_keeps_data_that_lives_elsewhere() {
        let catalog = DataCatalog::new();
        let shared = data(3, 10);
        let only_here = data(4, 20);
        let leaving = NodeId::generate();
        let staying = NodeId::generate();

        catalog.record(shared, leaving);
        catalog.record(shared, staying);
        catalog.record(only_here, leaving);

        catalog.forget_node(leaving);

        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog.locations(shared.id), vec![staying]);
        assert!(!catalog.holds(only_here.id, leaving));
    }

    #[test]
    fn local_bytes_sums_only_what_the_node_holds() {
        let catalog = DataCatalog::new();
        let here = data(5, 1000);
        let elsewhere = data(6, 500);
        let node_id = NodeId::generate();
        catalog.record(here, node_id);
        catalog.record(elsewhere, NodeId::generate());

        let inputs = vec![here.id, elsewhere.id];
        assert_eq!(catalog.local_bytes(node_id, &inputs), 1000);
    }

    #[test]
    fn held_by_counts_only_what_that_node_has() {
        let catalog = DataCatalog::new();
        let here = DataDescriptor::new(DataId::of(b"here"), 1_000);
        let both = DataDescriptor::new(DataId::of(b"both"), 500);
        let elsewhere = DataDescriptor::new(DataId::of(b"elsewhere"), 9_000);

        let node = NodeId::generate();
        let other = NodeId::generate();
        catalog.record(here, node);
        catalog.record(both, node);
        catalog.record(both, other);
        catalog.record(elsewhere, other);

        assert_eq!(catalog.held_by(node), (2, 1_500));
        assert_eq!(catalog.held_by(other), (2, 9_500));
        assert_eq!(catalog.held_by(NodeId::generate()), (0, 0));
    }
}
