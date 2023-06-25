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

/// The ID of a chunk.
pub type ChunkId = usize;

/// The chunk size is a multiple of the block size.
pub const CHUNK_SIZE: usize = 1024 * BLOCK_SIZE;

/// A chunk allocator tracks which chunks are free.
pub struct ChunkAlloc {
    state: Arc<Mutex<ChunkAllocState>>,
    tx_provider: Arc<TxProvider>,
}

impl ChunkAlloc {
    /// Creates a chunk manager that manages a specified number of 
    /// chunks (`capacity`). Initially, all chunks are free.
    pub fn new(capacity: usize, tx_provider: Arc<TxProvider>) -> Self {
        let new_self = {
            let state = Arc::new(Mutex::new(ChunkAllocState::new(capacity)));
            Self {
                state,
                tx_provider,
            }
        };

        new_self.tx_provider.register_data_initializer(|| {
            ChunkAllocEdit::new()
        });
        new_self.tx_provider.register_commit_handler({
            let state = new_self.state.clone();
            move |current: CurrentTx<'_>| {
                let mut state = state.lock();
                current.data_with(|edit: &ChunkAllocEdit| {
                    edit.apply_to(&mut state);
                });
            }
        });
        new_self.tx_provider.register_abort_handler({
            let state = new_self.state.clone();
            move |current: CurrentTx<'_>| {
                let mut state = state.lock();
                current.data_with(|edit: &ChunkAllocEdit| {
                    edit.iter_allocated_chunks(|chunk_id| {
                        state.dealloc(chunk_id);
                    });
                });
            }
        });

        new_self
    }

    /// Reconstructs a `ChunkAlloc` from its parts.
    pub(super) fn from_parts(state: ChunkAllocState, tx_provider: Arc<TxProvider>) -> Self {
        Self {
            state: Arc::new(Mutex::new(state)),
            tx_provider,
        }
    }

    /// Creates a new transaction for the chunk allocator.
    pub fn new_tx(&self) -> Tx {
        self.tx_provider.new_tx()
    }

    /// Allocate a chunk, returning its ID. 
    pub fn alloc(&self) -> Option<ChunkId> {
        let chunk_id = {
            let mut state = self.state.lock();
            state.alloc()?
        };

        let mut current_tx = self.tx_provider.current();
        current_tx.data_mut_with(|edit: &mut ChunkAllocEdit| {
            edit.alloc(chunk_id);
        });

        Some(chunk_id)
    }

    /// Deallocate the chunk of a given ID.
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

    /// Deallocate the set of chunks of given IDs.
    /// 
    /// # Panic
    /// 
    /// Deallocating a free chunk causes panic.
    pub fn dealloc_batch<I>(&self, chunk_ids: I) 
    where
        I: Iterator<Item = &ChunkId> 
    {
        let mut current_tx = self.tx_provider.current();
        current_tx.data_mut_with(|edit: &mut ChunkAllocEdit| {
            let mut state = self.state.lock();
            for chunk_id in chunk_ids {
                let should_dealloc_now = edit.dealloc(*chunk_id);

                if should_dealloc_now {
                    state.dealloc(*chunk_id);
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

/// The persistent state of a chunk allocator.
pub(super) struct ChunkAllocState {
    // A bitmap where each bit indicates whether a corresponding chunk has been
    // allocated.
    alloc_map: BitVec<usize, Lsb0>,
    // The number of free chunks.
    free_count: usize,
    // The minimum free chunk Id. Useful to narrow the scope of searching for 
    // free chunk IDs.
    min_free: usize,
}

impl ChunkAllocState {
    /// Creates a persistent state for managing chunks of the specified number.
    /// Initially, all chunks are free.
    pub fn new(capacity: usize) -> Self {
        Self {
            alloc_map: bitvec![usize, Lsb0; 0; capacity],
            free_count: capacity,
            min_free: 0,
        }
    }

    /// Allocate a chunk, returning its ID. 
    pub fn alloc(&mut self) -> Option<ChunkId> {
        if self.min_free >= self.alloc_map.len() {
            return None; 
        }

        let free_chunk_id = self.alloc_map[min_free..]
            .first_zero()
            .expect("there must exist a zero");
        self.alloc_map[free_chunk_id] = true;

        // Keep the invariance that all free chunk IDs are no less than min_free
        self.min_free = free_chunk_id + 1;

        Some(free_chunk_id)
    }

    /// Deallocate the chunk of a given ID.
    /// 
    /// # Panic
    /// 
    /// Deallocating a free chunk causes panic.
    pub fn dealloc(&mut self, chunk_id: ChunkId) {
        debug_assert!(self.alloc_map[chunk_id] == true);
        self.alloc_map[chunk_id] = false;

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
}

impl Edit<ChunkAllocState> for ChunkAllocEdit {
    fn apply_to(&self, state: &mut ChunkAllocState) {
        for (chunk_id, chunk_edit) in &self.chunk_table {
            match chunk_edit {
                ChunkEdit::Alloc => {
                    // Nothing needs to be done
                }
                ChunkEdit::Dealloc => {
                    state.dealloc(chunk_id);
                }
            }
        }
    }
} 

/// A persistent edit to the state of a chunk allocator.
pub(super) struct ChunkAllocEdit {
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
        panic!(old_edit.is_some());
    }

    /// Records a chunk deallocation in the edit. 
    /// 
    /// The return value indicates whether the chunk being deallocated 
    /// is previously recored in the edit as being allocated. 
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
}
