//! Chunk-based storage management.
//!
//! A chunk is a group of consecutive blocks.
//! As the size of a chunk is much greater than that of a block,
//! the number of chunks is naturally far smaller than that of blocks.
//! This makes it possible to keep all metadata for chunks in memory.
//! Thus, managing chunks is more efficient than managing blocks.
//!
//! The primary API provided by this module is chunk allocators,
//! `ChunkAlloc`, which tracks whether chunks are free or not.
//!
//! # Examples
//!
//! Chunk allocators are used within transactions.
//!
//! ```
//! fn alloc_chunks(chunk_alloc: &ChunkAlloc, num_chunks: usize) -> Option<Vec<ChunkId>> {
//!     let mut tx = chunk_alloc.new_tx();
//!     let res: Option<Vec<ChunkId>> = tx.context(|| {
//!         let mut chunk_ids = Vec::new();
//!         for _ in 0..num_chunks {
//!             chunk_ids.push(chunk_alloc.alloc()?);
//!         }
//!         Some(chunk_ids)
//!     });
//!     if res.is_some() {
//!         tx.commit().ok()?;
//!     } else {
//!         tx.abort();
//!     }
//!     res
//! }
//! ```
//!
//! This above example showcases the power of transaction atomicity:
//! if anything goes wrong (e.g., allocation failures) during the transaction,
//! then the transaction can be aborted and all changes made to `chuck_alloc`
//! during the transaction will be rolled back automatically.
use crate::layers::edit::Edit;
use crate::os::{HashMap, Mutex};
use crate::prelude::*;
use crate::tx::{CurrentTx, Tx, TxData, TxProvider};
use crate::util::BitMap;

use serde::{Deserialize, Serialize};

/// The ID of a chunk.
pub type ChunkId = usize;

/// Number of blocks of a chunk.
pub const CHUNK_NBLOCKS: usize = 1024;
/// The chunk size is a multiple of the block size.
pub const CHUNK_SIZE: usize = CHUNK_NBLOCKS * BLOCK_SIZE;

/// A chunk allocator tracks which chunks are free.
#[derive(Clone)]
pub struct ChunkAlloc {
    state: Arc<Mutex<ChunkAllocState>>,
    tx_provider: Arc<TxProvider>,
}

impl ChunkAlloc {
    /// Creates a chunk allocator that manages a specified number of
    /// chunks (`capacity`). Initially, all chunks are free.
    pub fn new(capacity: usize, tx_provider: Arc<TxProvider>) -> Self {
        let state = ChunkAllocState::new(capacity);
        Self::from_parts(state, tx_provider)
    }

    /// Constructs a `ChunkAlloc` from its parts.
    pub(super) fn from_parts(state: ChunkAllocState, tx_provider: Arc<TxProvider>) -> Self {
        let new_self = Self {
            state: Arc::new(Mutex::new(state)),
            tx_provider,
        };

        // TX data
        new_self
            .tx_provider
            .register_data_initializer(Box::new(|| ChunkAllocEdit::new()));

        // Commit handler
        new_self.tx_provider.register_commit_handler({
            let state = new_self.state.clone();
            move |current: CurrentTx<'_>| {
                let state = state.clone();
                current.data_with(move |edit: &ChunkAllocEdit| {
                    if edit.edit_table.is_empty() {
                        return;
                    }

                    let mut state = state.lock();
                    edit.apply_to(&mut state);
                });
            }
        });

        // Abort handler
        new_self.tx_provider.register_abort_handler({
            let state = new_self.state.clone();
            move |current: CurrentTx<'_>| {
                let state = state.clone();
                current.data_with(move |edit: &ChunkAllocEdit| {
                    let mut state = state.lock();
                    for chunk_id in edit.iter_allocated_chunks() {
                        state.dealloc(chunk_id);
                    }
                });
            }
        });

        new_self
    }

    /// Creates a new transaction for the chunk allocator.
    pub fn new_tx(&self) -> Tx {
        self.tx_provider.new_tx()
    }

    /// Allocates a chunk, returning its ID.
    pub fn alloc(&self) -> Option<ChunkId> {
        let chunk_id = {
            let mut state = self.state.lock();
            state.alloc()? // Update global state immediately
        };

        let mut current_tx = self.tx_provider.current();
        current_tx.data_mut_with(|edit: &mut ChunkAllocEdit| {
            edit.alloc(chunk_id);
        });

        Some(chunk_id)
    }

    /// Allocates `count` number of chunks. Returns IDs of newly-allocated
    /// chunks, returns `None` if any allocation fails.
    pub fn alloc_batch(&self, count: usize) -> Option<Vec<ChunkId>> {
        let chunk_ids = {
            let mut ids = Vec::with_capacity(count);
            let mut state = self.state.lock();
            for _ in 0..count {
                match state.alloc() {
                    Some(id) => ids.push(id),
                    None => {
                        ids.iter().for_each(|id| state.dealloc(*id));
                        return None;
                    }
                }
            }
            ids
        };

        let mut current_tx = self.tx_provider.current();
        current_tx.data_mut_with(|edit: &mut ChunkAllocEdit| {
            for chunk_id in &chunk_ids {
                edit.alloc(*chunk_id);
            }
        });

        Some(chunk_ids)
    }

    /// Deallocates the chunk of a given ID.
    ///
    /// # Panic
    ///
    /// Deallocating a free chunk causes panic.
    pub fn dealloc(&self, chunk_id: ChunkId) {
        let mut current_tx = self.tx_provider.current();
        current_tx.data_mut_with(|edit: &mut ChunkAllocEdit| {
            let should_dealloc_now = edit.dealloc(chunk_id);

            if should_dealloc_now {
                let mut state = self.state.lock();
                state.dealloc(chunk_id);
            }
        });
    }

    /// Deallocates the set of chunks of given IDs.
    ///
    /// # Panic
    ///
    /// Deallocating a free chunk causes panic.
    pub fn dealloc_batch<I>(&self, chunk_ids: I)
    where
        I: Iterator<Item = ChunkId>,
    {
        let mut current_tx = self.tx_provider.current();
        current_tx.data_mut_with(|edit: &mut ChunkAllocEdit| {
            let mut state = self.state.lock();
            for chunk_id in chunk_ids {
                let should_dealloc_now = edit.dealloc(chunk_id);

                if should_dealloc_now {
                    state.dealloc(chunk_id);
                }
            }
        });
    }

    /// Returns the capacity of the allocator, which is the number of chunks.
    pub fn capacity(&self) -> usize {
        self.state.lock().capacity()
    }

    /// Returns the number of free chunks.
    pub fn free_count(&self) -> usize {
        self.state.lock().free_count()
    }
}

impl Debug for ChunkAlloc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.lock();
        f.debug_struct("ChunkAlloc")
            .field("bitmap_free_count", &state.free_count)
            .field("bitmap_min_free", &state.min_free)
            .finish()
    }
}

////////////////////////////////////////////////////////////////////////////////
// Persistent State
////////////////////////////////////////////////////////////////////////////////

/// The persistent state of a chunk allocator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkAllocState {
    // A bitmap where each bit indicates whether a corresponding chunk
    // has been allocated.
    alloc_map: BitMap,
    // The number of free chunks.
    free_count: usize,
    // The minimum free chunk Id. Useful to narrow the scope of searching for
    // free chunk IDs.
    min_free: usize,
}
// TODO: Separate persistent and volatile state of `ChunkAlloc`

impl ChunkAllocState {
    /// Creates a persistent state for managing chunks of the specified number.
    /// Initially, all chunks are free.
    pub fn new(capacity: usize) -> Self {
        Self {
            alloc_map: BitMap::repeat(false, capacity),
            free_count: capacity,
            min_free: 0,
        }
    }

    /// Allocates a chunk, returning its ID.
    pub fn alloc(&mut self) -> Option<ChunkId> {
        let min_free = self.min_free;
        if min_free >= self.alloc_map.len() {
            return None;
        }

        let free_chunk_id = self
            .alloc_map
            .first_zero(min_free)
            .expect("there must exists a zero");
        self.alloc_map.set(free_chunk_id, true);
        self.free_count -= 1;

        // Keep the invariance that all free chunk IDs are no less than `min_free`
        self.min_free = free_chunk_id + 1;

        Some(free_chunk_id)
    }

    /// Deallocates the chunk of a given ID.
    ///
    /// # Panic
    ///
    /// Deallocating a free chunk causes panic.
    pub fn dealloc(&mut self, chunk_id: ChunkId) {
        // debug_assert_eq!(self.alloc_map[chunk_id], true); // may fail in journal's commit
        self.alloc_map.set(chunk_id, false);
        self.free_count += 1;

        // Keep the invariance that all free chunk IDs are no less than min_free
        if chunk_id < self.min_free {
            self.min_free = chunk_id;
        }
    }

    /// Returns the total number of chunks.
    pub fn capacity(&self) -> usize {
        self.alloc_map.len()
    }

    /// Returns the number of free chunks.
    pub fn free_count(&self) -> usize {
        self.free_count
    }

    /// Returns whether a specific chunk is allocated.
    pub fn is_chunk_allocated(&self, chunk_id: ChunkId) -> bool {
        self.alloc_map[chunk_id] == true
    }
}

////////////////////////////////////////////////////////////////////////////////
// Persistent Edit
////////////////////////////////////////////////////////////////////////////////

/// A persistent edit to the state of a chunk allocator.
#[derive(Clone, Serialize, Deserialize)]
pub struct ChunkAllocEdit {
    edit_table: HashMap<ChunkId, ChunkEdit>,
}

/// The smallest unit of a persistent edit to the
/// state of a chunk allocator, which is
/// a chunk being either allocated or deallocated.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum ChunkEdit {
    Alloc,
    Dealloc,
}

impl ChunkAllocEdit {
    /// Creates a new empty edit table.
    pub fn new() -> Self {
        Self {
            edit_table: HashMap::new(),
        }
    }

    /// Records a chunk allocation in the edit.
    pub fn alloc(&mut self, chunk_id: ChunkId) {
        let old_edit = self.edit_table.insert(chunk_id, ChunkEdit::Alloc);

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
        match self.edit_table.get(&chunk_id) {
            None => {
                self.edit_table.insert(chunk_id, ChunkEdit::Dealloc);
                false
            }
            Some(&ChunkEdit::Alloc) => {
                self.edit_table.remove(&chunk_id);
                true
            }
            Some(&ChunkEdit::Dealloc) => {
                panic!("a chunk must not be deallocated twice");
            }
        }
    }

    /// Returns an iterator over all allocated chunks.
    pub fn iter_allocated_chunks(&self) -> impl Iterator<Item = ChunkId> + '_ {
        self.edit_table.iter().filter_map(|(id, edit)| {
            if *edit == ChunkEdit::Alloc {
                Some(*id)
            } else {
                None
            }
        })
    }

    pub fn is_empty(&self) -> bool {
        self.edit_table.is_empty()
    }
}

impl Edit<ChunkAllocState> for ChunkAllocEdit {
    fn apply_to(&self, state: &mut ChunkAllocState) {
        for (&chunk_id, chunk_edit) in &self.edit_table {
            match chunk_edit {
                ChunkEdit::Alloc => {
                    // Journal's state also needs to be updated
                    if !state.is_chunk_allocated(chunk_id) {
                        let _allocated_id = state.alloc().unwrap();
                        // `_allocated_id` may not be equal to `chunk_id` due to concurrent TXs,
                        // but eventually the state will be consistent
                    }

                    // Except journal, nothing needs to be done
                }
                ChunkEdit::Dealloc => {
                    state.dealloc(chunk_id);
                }
            }
        }
    }
}

impl TxData for ChunkAllocEdit {}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_chunk_alloc() -> ChunkAlloc {
        let cap = 1024_usize;
        let tx_provider = TxProvider::new();
        let chunk_alloc = ChunkAlloc::new(cap, tx_provider);
        assert_eq!(chunk_alloc.capacity(), cap);
        assert_eq!(chunk_alloc.free_count(), cap);
        chunk_alloc
    }

    fn do_alloc_dealloc_tx(chunk_alloc: &ChunkAlloc, alloc_cnt: usize, dealloc_cnt: usize) -> Tx {
        debug_assert!(alloc_cnt <= chunk_alloc.capacity() && dealloc_cnt <= alloc_cnt);
        let mut tx = chunk_alloc.new_tx();
        tx.context(|| {
            let chunk_id = chunk_alloc.alloc().unwrap();
            let chunk_ids = chunk_alloc.alloc_batch(alloc_cnt - 1).unwrap();
            let allocated_chunk_ids: Vec<ChunkId> = core::iter::once(chunk_id)
                .chain(chunk_ids.into_iter())
                .collect();

            chunk_alloc.dealloc(allocated_chunk_ids[0]);
            chunk_alloc.dealloc_batch(
                allocated_chunk_ids[alloc_cnt - dealloc_cnt + 1..alloc_cnt]
                    .iter()
                    .cloned(),
            );
        });
        tx
    }

    #[test]
    fn chunk_alloc_dealloc_tx_commit() -> Result<()> {
        let chunk_alloc = new_chunk_alloc();
        let cap = chunk_alloc.capacity();
        let (alloc_cnt, dealloc_cnt) = (cap, cap);

        let mut tx = do_alloc_dealloc_tx(&chunk_alloc, alloc_cnt, dealloc_cnt);
        tx.commit()?;
        assert_eq!(chunk_alloc.free_count(), cap - alloc_cnt + dealloc_cnt);
        Ok(())
    }

    #[test]
    fn chunk_alloc_dealloc_tx_abort() -> Result<()> {
        let chunk_alloc = new_chunk_alloc();
        let cap = chunk_alloc.capacity();
        let (alloc_cnt, dealloc_cnt) = (cap / 2, cap / 4);

        let mut tx = do_alloc_dealloc_tx(&chunk_alloc, alloc_cnt, dealloc_cnt);
        tx.abort();
        assert_eq!(chunk_alloc.free_count(), cap);
        Ok(())
    }
}
