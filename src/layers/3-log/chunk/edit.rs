use super::{state::ChunkAllocState, ChunkId};
use crate::{layers::edit::Edit, tx::TxData};

use alloc::collections::BTreeMap;

/// A persistent edit to the state of a chunk allocator.
pub struct ChunkAllocEdit {
    chunk_table: BTreeMap<ChunkId, ChunkEdit>,
}

/// The smallest unit of a persistent edit to the
/// state of a chunk allocator, which is
/// a chunk being either allocated or deallocated.
enum ChunkEdit {
    Alloc,
    Dealloc,
}

impl ChunkAllocEdit {
    /// Creates a new empty edit.
    pub fn new() -> Self {
        Self {
            chunk_table: BTreeMap::new(),
        }
    }

    /// Records a chunk allocation in the edit.
    pub fn alloc(&mut self, chunk_id: ChunkId) {
        let old_edit = self.chunk_table.insert(chunk_id, ChunkEdit::Alloc);

        // There must be a logical error if an edit has been recorded
        // for the chunk. If the chunk edit is `ChunkEdit::Alloc`, then
        // it is double allocations. If the chunk edit is `ChunkEdit::Dealloc`,
        // then such deallocations can only take effect after the edit is
        // committed. Thus, it is impossible to allocate the chunk again now.
        assert!(old_edit.is_none());
    }

    /// Records a chunk deallocation in the edit.
    ///
    /// The return value indicates whether the chunk being deallocated
    /// is previously recorded in the edit as being allocated.
    /// If so, the chunk can be deallocated in the `ChunkAllocState`.
    pub fn dealloc(&mut self, chunk_id: ChunkId) -> bool {
        match self.chunk_table.get(&chunk_id).to_owned() {
            None => {
                self.chunk_table.insert(chunk_id, ChunkEdit::Dealloc);
                false
            }
            Some(ChunkEdit::Alloc) => {
                self.chunk_table.remove(&chunk_id);
                true
            }
            Some(ChunkEdit::Dealloc) => {
                panic!("a chunk must not be deallocated twice");
            }
        }
    }

    pub fn iter_allocated_chunks<F>(&self, mut f: F)
    where
        Self: Sized,
        F: FnMut(ChunkId),
    {
        for chunk_id in self.chunk_table.keys() {
            f(*chunk_id);
        }
    }
}

impl Edit<ChunkAllocState> for ChunkAllocEdit {
    fn apply_to(&self, state: &mut ChunkAllocState) {
        for (chunk_id, chunk_edit) in &self.chunk_table {
            match chunk_edit {
                ChunkEdit::Alloc => {
                    // Nothing needs to be done
                }
                ChunkEdit::Dealloc => {
                    state.dealloc(*chunk_id);
                }
            }
        }
    }
}

impl TxData for ChunkAllocEdit {}
