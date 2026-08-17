//! Splitting large datasets into content-addressed chunks and putting them
//! back together.
//!
//! Every chunk carries its own hash and index, so chunks may travel in any
//! order, over any number of connections, and a chunk the receiver already has
//! never needs to be sent again.

use std::collections::HashMap;
use std::ops::Range;

use serde::{Deserialize, Serialize};

use crate::data::{DataDescriptor, DataId};
use crate::store::DataStore;

/// Chunk size used unless the caller picks another one.
pub const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024;

/// Describes a dataset as an ordered list of chunks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkManifest {
    /// The whole dataset.
    pub data: DataDescriptor,
    /// Size of every chunk except possibly the last one.
    pub chunk_size: u32,
    /// Chunk descriptors, in order.
    pub chunks: Vec<DataDescriptor>,
}

impl ChunkManifest {
    /// Splits `bytes` into chunks of at most `chunk_size`.
    ///
    /// A `chunk_size` of zero is treated as [`DEFAULT_CHUNK_SIZE`].
    pub fn split(bytes: &[u8], chunk_size: usize) -> Self {
        let chunk_size = if chunk_size == 0 {
            DEFAULT_CHUNK_SIZE
        } else {
            chunk_size
        };

        let chunks = bytes.chunks(chunk_size).map(DataDescriptor::of).collect();
        Self {
            data: DataDescriptor::of(bytes),
            chunk_size: chunk_size as u32,
            chunks,
        }
    }

    /// Number of chunks.
    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Byte range of one chunk within the whole dataset.
    pub fn chunk_range(&self, index: u32) -> Option<Range<usize>> {
        let descriptor = self.chunks.get(index as usize)?;
        let start = index as usize * self.chunk_size as usize;
        Some(start..start + descriptor.size_bytes as usize)
    }

    /// The chunk descriptors paired with their indexes.
    pub fn indexed(&self) -> impl Iterator<Item = (u32, DataDescriptor)> + '_ {
        self.chunks
            .iter()
            .enumerate()
            .map(|(index, descriptor)| (index as u32, *descriptor))
    }
}

/// A chunk could not be accepted.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ChunkError {
    #[error("no manifest received for data {0}")]
    UnknownData(DataId),
    #[error("chunk index {index} is out of range for data {data_id} ({len} chunks)")]
    IndexOutOfRange {
        data_id: DataId,
        index: u32,
        len: usize,
    },
    #[error("chunk {index} of {data_id} does not match its descriptor")]
    ChunkMismatch { data_id: DataId, index: u32 },
    #[error("reassembled data {expected} hashed as {actual}")]
    DataMismatch { expected: DataId, actual: DataId },
}

#[derive(Debug)]
struct Pending {
    manifest: ChunkManifest,
    chunks: HashMap<u32, Vec<u8>>,
}

/// Collects chunks until a dataset is complete.
#[derive(Debug, Default)]
pub struct ChunkAssembler {
    pending: HashMap<DataId, Pending>,
}

impl ChunkAssembler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts (or restarts) collecting the dataset described by `manifest`.
    pub fn begin(&mut self, manifest: ChunkManifest) {
        self.pending.insert(
            manifest.data.id,
            Pending {
                manifest,
                chunks: HashMap::new(),
            },
        );
    }

    /// Starts collecting, filling in chunks `store` already holds.
    ///
    /// This is what lets a sender skip chunks the receiver has seen before,
    /// whether from an earlier dataset or from a repeat inside this one.
    /// Returns the dataset when the store already had every chunk.
    pub fn begin_with(
        &mut self,
        manifest: ChunkManifest,
        store: &DataStore,
    ) -> Result<Option<Vec<u8>>, ChunkError> {
        let data_id = manifest.data.id;
        self.begin(manifest);
        self.fill_from_store(data_id, store)
    }

    /// Accepts a chunk and keeps a copy in `store`, so the same chunk never has
    /// to be transferred to this node again.
    ///
    /// Chunks repeated inside the dataset are filled in from the store as soon
    /// as one copy arrives, which is what lets the sender skip them.
    pub fn add_stored(
        &mut self,
        store: &DataStore,
        data_id: DataId,
        index: u32,
        bytes: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ChunkError> {
        store.put(bytes.clone());
        match self.add(data_id, index, bytes)? {
            Some(assembled) => Ok(Some(assembled)),
            None => self.fill_from_store(data_id, store),
        }
    }

    /// Supplies every still-missing chunk that the store already holds.
    fn fill_from_store(
        &mut self,
        data_id: DataId,
        store: &DataStore,
    ) -> Result<Option<Vec<u8>>, ChunkError> {
        let Some(pending) = self.pending.get(&data_id) else {
            return Ok(None);
        };

        let known: Vec<(u32, Vec<u8>)> = pending
            .manifest
            .indexed()
            .filter(|(index, _)| !pending.chunks.contains_key(index))
            .filter_map(|(index, chunk)| store.get(chunk.id).map(|bytes| (index, bytes.to_vec())))
            .collect();

        let mut assembled = None;
        for (index, bytes) in known {
            assembled = self.add(data_id, index, bytes)?;
        }
        Ok(assembled)
    }

    pub fn is_pending(&self, data_id: DataId) -> bool {
        self.pending.contains_key(&data_id)
    }

    /// Chunks still missing for a dataset, in order.
    pub fn missing(&self, data_id: DataId) -> Vec<u32> {
        self.pending
            .get(&data_id)
            .map(|pending| {
                (0..pending.manifest.len() as u32)
                    .filter(|index| !pending.chunks.contains_key(index))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Accepts one chunk. Returns the whole dataset once the last chunk lands.
    ///
    /// Chunks may arrive in any order; duplicates are ignored.
    pub fn add(
        &mut self,
        data_id: DataId,
        index: u32,
        bytes: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ChunkError> {
        let pending = self
            .pending
            .get_mut(&data_id)
            .ok_or(ChunkError::UnknownData(data_id))?;

        let expected =
            pending
                .manifest
                .chunks
                .get(index as usize)
                .ok_or(ChunkError::IndexOutOfRange {
                    data_id,
                    index,
                    len: pending.manifest.len(),
                })?;

        if DataDescriptor::of(&bytes) != *expected {
            return Err(ChunkError::ChunkMismatch { data_id, index });
        }

        pending.chunks.insert(index, bytes);
        if pending.chunks.len() < pending.manifest.len() {
            return Ok(None);
        }

        let pending = self.pending.remove(&data_id).expect("just looked it up");
        let mut assembled = Vec::with_capacity(pending.manifest.data.size_bytes as usize);
        let mut chunks = pending.chunks;
        for index in 0..pending.manifest.len() as u32 {
            assembled.extend_from_slice(&chunks.remove(&index).expect("all chunks present"));
        }

        let actual = DataId::of(&assembled);
        if actual != data_id {
            return Err(ChunkError::DataMismatch {
                expected: data_id,
                actual,
            });
        }
        Ok(Some(assembled))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dataset(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    /// Feeds a manifest's chunks to an assembler in the given order.
    fn feed(
        assembler: &mut ChunkAssembler,
        manifest: &ChunkManifest,
        bytes: &[u8],
        order: impl IntoIterator<Item = u32>,
    ) -> Result<Option<Vec<u8>>, ChunkError> {
        let mut last = Ok(None);
        for index in order {
            let range = manifest.chunk_range(index).unwrap();
            last = assembler.add(manifest.data.id, index, bytes[range].to_vec());
        }
        last
    }

    #[test]
    fn splitting_covers_the_whole_dataset() {
        let bytes = dataset(2500);
        let manifest = ChunkManifest::split(&bytes, 1000);

        assert_eq!(manifest.len(), 3);
        assert_eq!(manifest.chunks[2].size_bytes, 500);
        assert_eq!(manifest.data, DataDescriptor::of(&bytes));
        assert_eq!(manifest.chunk_range(1), Some(1000..2000));
        assert_eq!(manifest.chunk_range(3), None);
    }

    #[test]
    fn identical_chunks_share_an_id() {
        let manifest = ChunkManifest::split(&[7u8; 300], 100);
        assert_eq!(manifest.chunks[0], manifest.chunks[1]);
        assert_eq!(manifest.chunks[1], manifest.chunks[2]);
    }

    #[test]
    fn chunks_reassemble_in_order() {
        let bytes = dataset(4096);
        let manifest = ChunkManifest::split(&bytes, 512);
        let mut assembler = ChunkAssembler::new();
        assembler.begin(manifest.clone());

        let assembled = feed(&mut assembler, &manifest, &bytes, 0..manifest.len() as u32).unwrap();

        assert_eq!(assembled.unwrap(), bytes);
        assert!(!assembler.is_pending(manifest.data.id));
    }

    #[test]
    fn chunks_may_arrive_in_any_order() {
        let bytes = dataset(1000);
        let manifest = ChunkManifest::split(&bytes, 100);
        let mut assembler = ChunkAssembler::new();
        assembler.begin(manifest.clone());

        let order = [9, 4, 0, 7, 1, 8, 2, 6, 3, 5];
        let assembled = feed(&mut assembler, &manifest, &bytes, order).unwrap();

        assert_eq!(assembled.unwrap(), bytes);
    }

    #[test]
    fn missing_reports_what_is_still_needed() {
        let bytes = dataset(300);
        let manifest = ChunkManifest::split(&bytes, 100);
        let mut assembler = ChunkAssembler::new();
        assembler.begin(manifest.clone());

        feed(&mut assembler, &manifest, &bytes, [1]).unwrap();

        assert_eq!(assembler.missing(manifest.data.id), vec![0, 2]);
    }

    #[test]
    fn a_duplicate_chunk_does_not_break_assembly() {
        let bytes = dataset(200);
        let manifest = ChunkManifest::split(&bytes, 100);
        let mut assembler = ChunkAssembler::new();
        assembler.begin(manifest.clone());

        let assembled = feed(&mut assembler, &manifest, &bytes, [0, 0, 1]).unwrap();
        assert_eq!(assembled.unwrap(), bytes);
    }

    #[test]
    fn a_corrupted_chunk_is_rejected() {
        let bytes = dataset(200);
        let manifest = ChunkManifest::split(&bytes, 100);
        let mut assembler = ChunkAssembler::new();
        assembler.begin(manifest.clone());

        let error = assembler
            .add(manifest.data.id, 0, vec![0xff; 100])
            .unwrap_err();
        assert_eq!(
            error,
            ChunkError::ChunkMismatch {
                data_id: manifest.data.id,
                index: 0
            }
        );
    }

    #[test]
    fn chunks_without_a_manifest_are_rejected() {
        let mut assembler = ChunkAssembler::new();
        let data_id = DataId::of(b"unannounced");

        assert_eq!(
            assembler.add(data_id, 0, vec![1]),
            Err(ChunkError::UnknownData(data_id))
        );
    }

    #[test]
    fn chunks_already_in_the_store_do_not_need_resending() {
        let bytes = dataset(400);
        let manifest = ChunkManifest::split(&bytes, 100);
        let store = DataStore::new();
        // The node saw chunks 0 and 2 as part of some earlier transfer.
        store.put(bytes[0..100].to_vec());
        store.put(bytes[200..300].to_vec());

        let mut assembler = ChunkAssembler::new();
        assert!(
            assembler
                .begin_with(manifest.clone(), &store)
                .unwrap()
                .is_none()
        );
        assert_eq!(assembler.missing(manifest.data.id), vec![1, 3]);

        let assembled = feed(&mut assembler, &manifest, &bytes, [1, 3]).unwrap();
        assert_eq!(assembled.unwrap(), bytes);
    }

    #[test]
    fn a_dataset_of_repeated_chunks_needs_one_transfer() {
        let bytes = vec![42u8; 400];
        let manifest = ChunkManifest::split(&bytes, 100);
        let store = DataStore::new();
        let mut assembler = ChunkAssembler::new();
        assembler.begin_with(manifest.clone(), &store).unwrap();

        // One chunk arrives; the other three have the same content, so the
        // dataset completes without them being sent.
        let assembled = assembler
            .add_stored(&store, manifest.data.id, 0, bytes[0..100].to_vec())
            .unwrap();

        assert_eq!(assembled.unwrap(), bytes);
        assert!(!assembler.is_pending(manifest.data.id));
    }

    #[test]
    fn an_out_of_range_index_is_rejected() {
        let bytes = dataset(100);
        let manifest = ChunkManifest::split(&bytes, 100);
        let mut assembler = ChunkAssembler::new();
        assembler.begin(manifest.clone());

        let error = assembler.add(manifest.data.id, 5, bytes).unwrap_err();
        assert!(matches!(
            error,
            ChunkError::IndexOutOfRange { index: 5, .. }
        ));
    }
}
