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
use crate::os::Mutex;
use crate::prelude::*;
use crate::{
    layers::{bio::BLOCK_SIZE, edit::Edit},
    tx::{CurrentTx, Tx, TxProvider},
};

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
            Self { state, tx_provider }
        };

        new_self
            .tx_provider
            .register_data_initializer(Box::new(|| ChunkAllocEdit::new()));
        new_self.tx_provider.register_commit_handler({
            let state = new_self.state.clone();
            move |mut current: CurrentTx<'_>| {
                let mut state = state.lock();
                current.data_with(|edit: &ChunkAllocEdit| {
                    edit.apply_to(&mut state);
                });
            }
        });
        new_self.tx_provider.register_abort_handler({
            let state = new_self.state.clone();
            move |mut current: CurrentTx<'_>| {
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
            state.alloc()? // Update global state immediately
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

mod edit;
mod state;

pub use self::edit::ChunkAllocEdit;
pub use self::state::ChunkAllocState;
