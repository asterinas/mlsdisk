//! A store of raw (untrusted) logs.
//!
//! `RawLogStore<D>` allows creating, deleting, reading and writing
//! `RawLog<D>`. Each raw log is uniquely identified by its ID (`RawLogId`).
//! Writing to a raw log is append only.
//!
//! `RawLogStore<D>` stores raw logs on a disk of `D: BlockSet`.
//! Internally, `RawLogStore<D>` manages the disk space with `ChunkAlloc`
//! so that the disk space can be allocated and deallocated in the units of
//! chunk. An allocated chunk belongs to exactly one raw log. And one raw log
//! may be backed by multiple chunks.
//!
//! # Examples
//!
//! Raw logs are manipulated and accessed within transactions.
//!
//! ```
//! fn concat_logs<D>(
//!     log_store: &RawLogStore<D>,
//!     log_ids: &[RawLogId]
//! ) -> Result<RawLogId> {
//!     let mut tx = log_store.new_tx();
//!     let res = tx.context(|| {
//!         let mut buffer = BlockBuf::new_boxed();
//!         let output_log = log_store.create_log();
//!         for log_id in log_ids {
//!             let input_log = log_store.open_log(log_id, false)?;
//!             let input_len = input_log.num_blocks();
//!             let mut pos = 0;
//!             while pos < input_len {
//!                 let read_len = buffer.len().min(input_len - pos);
//!                 input_log.read(&mut buffer[..read_len])?;
//!                 output_log.write(&buffer[..read_len])?;
//!             }
//!         }
//!         Ok(output_log.id())
//!     });
//!     if res.is_ok() {
//!         tx.commit()?;
//!     } else {
//!         tx.abort();
//!     }
//!     res
//! }
//! ```
//!
//! If any error occurs (e.g., failures to open, read, or write a log) during
//! the transaction, then all prior changes to raw logs shall have no
//! effects. On the other hand, if the commit operation succeeds, then
//! all changes made in the transaction shall take effect as a whole.
//!
//! # Expected behaviors
//!
//! We provide detailed descriptions about the expected behaviors of raw log
//! APIs under transactions.
//!
//! 1. The local changes made (e.g., creations, deletions, writes) in a Tx are
//! immediately visible to the Tx, but not other Tx until the Tx is committed.
//! For example, a newly-created log within Tx A is immediately usable within Tx,
//! but becomes visible to other Tx only until A is committed.
//! As another example, when a log is deleted within a Tx, then the Tx can no
//! longer open the log. But other concurrent Tx can still open the log.
//!
//! 2. If a Tx is aborted, then all the local changes made in the TX will be
//! discarded.
//!
//! 3. At any given time, a log can have at most one writer Tx.
//! A Tx becomes the writer of a log when the log is opened with the write
//! permission in the Tx. And it stops being the writer Tx of the log only when
//! the Tx is terminated (not when the log is closed within Tx).
//! This single-writer rule avoids potential conflicts between concurrent
//! writing to the same log.
//!
//! 4. Log creation does not conflict with log deleation, read, or write as
//! every newly-created log is assigned a unique ID automatically.
//!
//! 4. Deleting a log does not affect any opened instance of the log in the Tx
//! or other Tx (similar to deleting a file in a UNIX-style system).
//! It is only until the deleting Tx is committed and the last
//! instance of the log is closed shall the log be deleted and its disk space
//! be freed.
//!
//! 5. The Tx commitment will not fail due to conflicts between concurrent
//! operations in different Tx.
use self::{
    edit::{RawLogEdit, RawLogTail},
    state::{RawLogEntry, State},
};
use super::chunk::{ChunkAlloc, ChunkId, CHUNK_SIZE};
use crate::os::{Mutex, MutexGuard};
use crate::prelude::*;
use crate::{
    layers::{
        bio::{BlockBuf, BlockId, BlockLog, BlockSet, BLOCK_SIZE},
        edit::Edit,
    },
    tx::{CurrentTx, TxId, TxProvider},
};

use alloc::sync::Weak;
use core::ops::Range;

pub type RawLogId = u64;

/// Store for raw logs.
pub struct RawLogStore<D> {
    state: Arc<Mutex<State>>,
    disk: D,
    chunk_alloc: ChunkAlloc, // Mapping: ChunkId * CHUNK_SIZE = disk position (BlockId)
    tx_provider: Arc<TxProvider>,
    weak_self: Weak<Self>,
}

// Note: edit's fn are called in store's fn (data_mut_with), state's fn are called in Edit::apply_to(), apply_to is called in commit handler
impl<D: BlockSet> RawLogStore<D> {
    pub fn new(disk: D, tx_provider: Arc<TxProvider>, chunk_alloc: ChunkAlloc) -> Arc<Self> {
        let new_self = Arc::new_cyclic(|weak_self| Self {
            state: Arc::new(Mutex::new(State::new(RawLogStoreState::new()))),
            disk,
            chunk_alloc,
            tx_provider,
            weak_self: weak_self.clone(),
        });

        new_self
            .tx_provider
            .register_data_initializer(Box::new(|| RawLogStoreEdit::new()));

        new_self.tx_provider.register_commit_handler({
            let state = new_self.state.clone();
            move |mut current: CurrentTx<'_>| {
                let mut state = state.lock();
                current.data_with(|edit: &RawLogStoreEdit| {
                    state.apply(&edit);
                });
            }
        });

        // No precommit or abort handler

        new_self
    }

    pub(super) fn from_parts(
        state: RawLogStoreState,
        disk: D,
        chunk_alloc: ChunkAlloc,
        tx_provider: Arc<TxProvider>,
    ) -> Arc<Self> {
        let new_self = Arc::new_cyclic(|weak_self| Self {
            state: Arc::new(Mutex::new(State::new(state))),
            disk,
            chunk_alloc,
            tx_provider,
            weak_self: weak_self.clone(),
        });

        new_self
    }

    /// Create a new raw log with a new log id.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn create_log(&self) -> Result<RawLog<D>> {
        let mut state = self.state.lock();
        let mut current_tx = self.tx_provider.current();

        let new_log_id = state.persistent().alloc_log_id();
        state.add_to_append_set(new_log_id).unwrap();
        state.add_lazy_delete(new_log_id);
        current_tx.data_mut_with(|edit: &mut RawLogStoreEdit| {
            edit.create_log(new_log_id);
        });

        // let tx_id = current_tx.id();
        Ok(RawLog {
            log_id: new_log_id,
            tx_provider: self.tx_provider.clone(),
            log_store: self.weak_self.upgrade().unwrap(),
            log_entry: None,
            can_append: true,
        })
    }

    /// Open the raw log of a given ID.
    ///
    /// For any log at any time, there can be at most one Tx that opens the log
    /// in the appendable mode.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn open_log(&self, log_id: u64, can_append: bool) -> Result<RawLog<D>> {
        let mut state = self.state.lock();
        let mut current_tx = self.tx_provider.current();

        let log_entry_opt = state.persistent().find_log(log_id);
        // The log is already created by other Tx
        if log_entry_opt.is_some() {
            let log_entry = log_entry_opt.as_ref().unwrap().clone();
            if can_append {
                // Prevent other TX from opening this log in the write mode.
                state.add_to_append_set(log_id)?;

                // If the log is open in the write mode, edit must be prepared
                current_tx.data_mut_with(|edit: &mut RawLogStoreEdit| {
                    edit.open_log(log_id, &log_entry.lock());
                });
            }
        }
        // The log must has been created by this Tx
        else {
            let is_log_created =
                current_tx.data_mut_with(|edit: &mut RawLogStoreEdit| edit.is_log_created(log_id));
            if !is_log_created {
                return_errno!(NotFound);
            }
        }

        // let tx_id = current_tx.id();
        Ok(RawLog {
            log_id,
            tx_provider: self.tx_provider.clone(),
            log_store: self.weak_self.upgrade().unwrap(),
            log_entry: log_entry_opt,
            can_append,
        })
    }

    /// Delete the raw log of a given ID.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn delete_log(&self, log_id: RawLogId) -> Result<()> {
        let mut current_tx = self.tx_provider.current();
        // Free tail chunks
        let tail_freed_chunks =
            current_tx.data_mut_with(|edit: &mut RawLogStoreEdit| edit.delete_log(log_id));
        tail_freed_chunks.map(|chunks| self.chunk_alloc.dealloc_batch(chunks.iter().map(|id| *id)));

        // Free head chunks
        let log_entry = self.state.lock().persistent().find_log(log_id).unwrap();
        let head_freed_chunks = &log_entry.lock().head.chunks;
        self.chunk_alloc
            .dealloc_batch(head_freed_chunks.iter().map(|id| *id));

        self.state.lock().remove_from_append_set(log_id);
        Ok(())
    }
}

/// A raw(untrusted) log.
pub struct RawLog<D> {
    log_id: RawLogId,
    // tx_id: TxId, // Why tx_id? tx_provider.current seems more ease-of-use
    // current: CurrentTx<'a>,
    tx_provider: Arc<TxProvider>,
    log_store: Arc<RawLogStore<D>>,
    log_entry: Option<Arc<Mutex<RawLogEntry>>>,
    can_append: bool,
}

impl<D: BlockSet> BlockLog for RawLog<D> {
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    fn read(&self, pos: BlockId, buf: &mut impl BlockBuf) -> Result<()> {
        let nblocks = buf.nblocks();
        debug_assert!(nblocks > 0);
        let log_ref = self.as_ref();

        let head_opt = log_ref.log_head;
        let tail_opt = log_ref.log_tail;
        let head_len = head_opt.as_ref().map_or(0, |head| head.len());
        let tail_len = tail_opt.as_ref().map_or(0, |tail| tail.len());
        let total_len = head_len + tail_len;

        // Do not allow "short read"
        if pos + nblocks > total_len {
            return_errno!(InvalidArgs);
        }

        let mut offset = pos;
        // Read from the head if possible and necessary
        if let Some(head) = head_opt && pos < head_len {
            let read_len = nblocks.min(head_len - pos);
            head.read(pos, &mut buf[..read_len], &self.log_store.disk)?;

            offset += read_len;
            buf = &mut buf[read_len..];
        }
        // Read from the tail if possible and necessary
        if let Some(tail) = tail_opt && offset >= head_len {
            let read_len = nblocks.min(total_len - offset);
            tail.read(pos, &mut buf[..read_len], &self.log_store.disk)?;
        }
        Ok(())
    }

    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    fn append(&self, buf: &impl BlockBuf) -> Result<BlockId> {
        let nblocks = buf.nblocks();
        debug_assert!(nblocks > 0);

        if !self.can_append {
            return_errno!(NotPermitted);
        }

        self.as_ref()
            .log_tail
            .unwrap()
            .append(buf, &self.log_store.disk)?;

        // TODO: Decide returned pos
        Ok(0)
    }

    fn flush(&self) -> Result<()> {
        self.log_store.disk.flush()
    }

    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    fn nblocks(&self) -> usize {
        let log_ref = self.as_ref();
        let head_opt = log_ref.log_head;
        let tail_opt = log_ref.log_tail;
        let head_len = head_opt.map_or(0, |head| head.len());
        let tail_len = tail_opt.map_or(0, |tail| tail.len());
        head_len + tail_len
    }
}

impl<D> RawLog<D> {
    fn id(&self) -> RawLogId {
        self.log_id
    }

    /// Get the reference(handle) of raw log.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    fn as_ref<'a>(&'a self) -> RawLogRef<D> {
        let log_tail = self.tx_provider.current().data_mut_with(
            |store_edit: &mut RawLogStoreEdit| -> Option<RawLogTailRef<'a>> {
                let edit = store_edit.get_edit(self.log_id).unwrap();
                match edit {
                    RawLogEdit::Create(create) => Some(RawLogTailRef {
                        tail: &mut create.tail,
                    }),
                    RawLogEdit::Append(append) => Some(RawLogTailRef {
                        tail: &mut append.tail,
                    }),
                    RawLogEdit::Delete => None,
                }
            },
        );

        RawLogRef {
            log_store: &self.log_store,
            log_head: Some(RawLogHeadRef {
                entry: self.log_entry.as_ref().unwrap().lock(),
            }),
            log_tail: todo!(),
        }
    }
}

impl<D> Drop for RawLog<D> {
    fn drop(&mut self) {
        if self.can_append {
            let mut state = self.log_store.state.lock();
            state.remove_from_append_set(self.log_id);
        }
    }
}

struct RawLogRef<'a, D> {
    log_store: &'a RawLogStore<D>,
    log_head: Option<RawLogHeadRef<'a>>,
    log_tail: Option<RawLogTailRef<'a>>,
}

struct RawLogHeadRef<'a> {
    entry: MutexGuard<'a, RawLogEntry>,
}

struct RawLogTailRef<'a> {
    tail: &'a mut RawLogTail,
}

impl<'a, D: BlockSet> RawLogRef<'a, D> {
    fn read(&self, mut pos: BlockId, mut buf: &mut impl BlockBuf) -> Result<()> {
        let nblocks = buf.nblocks();
        let head_opt = &self.log_head;
        let tail_opt = &self.log_tail;
        let head_len = head_opt.as_ref().map_or(0, |head| head.len());
        let tail_len = tail_opt.as_ref().map_or(0, |tail| tail.len());
        let total_len = head_len + tail_len;

        // Do not allow "short read"
        if pos + nblocks > total_len {
            return_errno!(InvalidArgs);
        }

        let disk = &self.log_store.disk;
        // Read from the head if possible and necessary
        if let Some(head) = head_opt && pos < head_len {
            let read_len = nblocks.min(head_len - pos);
            head.read(pos, &mut buf.as_slice_mut()[..read_len], &disk)?;

            pos += read_len;
            buf = &mut buf[read_len..];
        }
        // Read from the tail if possible and necessary
        if let Some(tail) = tail_opt && pos >= head_len {
            let read_len = nblocks.min(total_len - pos);
            tail.read(pos - head_len, &mut buf[..read_len], &disk)?;
        }
        Ok(())
    }

    fn append(&mut self, buf: &impl BlockBuf) -> Result<()> {
        let nblocks = buf.nblocks();
        let log_tail = self.log_tail.as_mut().unwrap();
        let tail = log_tail.tail();
        let avail_blocks = tail.head_last_chunk_free_blocks as usize
            + tail.chunks.len() * CHUNK_SIZE
            - tail.num_blocks;
        let append_blocks = nblocks;
        if append_blocks > avail_blocks {
            let chunks_needed = (append_blocks - avail_blocks) / CHUNK_SIZE + 1; // TODO: align up

            // Allocate new chunks
            for _ in 0..chunks_needed {
                let new_chunk = self
                    .log_store
                    .chunk_alloc
                    .alloc()
                    .ok_or(Error::new(NoMemory))?;
                tail.chunks.push(new_chunk);
            }
        }

        // Update tail metadata
        if append_blocks <= tail.head_last_chunk_free_blocks as _ {
            debug_assert!(tail.num_blocks == 0);
            tail.head_last_chunk_free_blocks -= append_blocks as u16;
        } else {
            tail.head_last_chunk_free_blocks = 0;
            tail.num_blocks += append_blocks - tail.head_last_chunk_free_blocks as usize;
        }

        let disk = &self.log_store.disk;
        log_tail.append(buf, &disk)?;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.log_head.as_ref().map_or(0, |head| head.len())
            + self.log_tail.as_ref().map_or(0, |tail| tail.len())
    }

    /// Get a number of consecutive blocks starting at a specified position.
    ///
    /// The start of the returned range is always equal to `pos`.
    /// The length of the returned range is not greater than `max_len`.
    ///
    /// If `pos` is smaller than the length of the raw log,
    /// then the returned range is non-empty. Otherwise, the returned range
    /// is empty.
    fn get_consecutive(&self, pos: BlockId, max_len: usize) -> Range<BlockId> {
        todo!() // Use `prepare_blocks()` instead
    }

    /// Extend the length of `ChunkBackedBlockVec` by allocating some
    /// consecutive blocks.
    ///
    /// The newly-allocated consecutive blocks are returned as a block range,
    /// whose length is not greater than `max_len`. If the allocation fails,
    /// then a zero-length range shall be returned.
    fn extend_consecutive(&mut self, max_len: usize) -> Option<Range<BlockId>> {
        todo!() // Use `prepare_blocks()` instead
    }
}

impl<'a> RawLogHeadRef<'a> {
    fn len(&self) -> usize {
        self.entry.head.num_blocks
    }

    fn read<D: BlockSet>(&self, offset: BlockId, buf: &mut impl BlockBuf, disk: &D) -> Result<()> {
        let nblocks = buf.nblocks();
        debug_assert!(offset + nblocks <= self.entry.head.num_blocks);
        let mut off = 0;
        // TODO: Batch read
        for pos in self.prepare_blocks(offset, nblocks) {
            disk.read(pos, &mut buf[off..off + BLOCK_SIZE])?;
            off += BLOCK_SIZE;
        }
        Ok(())
    }

    /// Collect and prepare a set of blocks in head
    fn prepare_blocks(&self, offset: BlockId, nblocks: usize) -> Vec<BlockId> {
        let chunks = &self.entry.head.chunks;
        let mut res_blocks = Vec::with_capacity(nblocks);

        let mut curr_chunk_idx = offset / CHUNK_SIZE;
        let mut curr_chunk_inner_offset = offset % CHUNK_SIZE;
        while res_blocks.len() <= nblocks {
            res_blocks.push(chunks[curr_chunk_idx] * CHUNK_SIZE + curr_chunk_inner_offset);
            curr_chunk_inner_offset += 1;
            if curr_chunk_inner_offset == CHUNK_SIZE {
                curr_chunk_inner_offset = 0;
                curr_chunk_idx += 1;
            }
        }
        res_blocks
    }
}

impl<'a> RawLogTailRef<'a> {
    fn tail(&mut self) -> &mut RawLogTail {
        &mut self.tail
    }

    fn len(&self) -> usize {
        self.tail.num_blocks
    }

    fn read<D: BlockSet>(&self, offset: BlockId, buf: &mut impl BlockBuf, disk: &D) -> Result<()> {
        let nblocks = buf.nblocks();
        debug_assert!(offset + nblocks <= self.tail.num_blocks);
        let mut off = 0;
        // TODO: Batch read
        for pos in self.prepare_blocks(offset, nblocks) {
            disk.read(pos, &mut buf.as_slice_mut()[off..off + BLOCK_SIZE])?;
            off += BLOCK_SIZE;
        }
        Ok(())
    }

    fn append<D: BlockSet>(&self, buf: &impl BlockBuf, disk: &D) -> Result<()> {
        let nblocks = buf.nblocks();
        let mut off = 0;
        // TODO: Batch write
        for pos in self.prepare_blocks(self.tail.num_blocks, nblocks) {
            disk.write(pos, &mut buf[off..off + BLOCK_SIZE])?;
            off += BLOCK_SIZE;
        }
        Ok(())
    }

    /// Collect and prepare a set of blocks in tail
    fn prepare_blocks(&self, offset: BlockId, nblocks: usize) -> Vec<BlockId> {
        let chunks = &self.tail.chunks;
        let mut res_blocks = Vec::with_capacity(nblocks);
        if offset == 0 {
            // Push head free blocks first if needed
            for i in 0..self.tail.head_last_chunk_free_blocks {
                res_blocks.push(
                    self.tail.head_last_chunk_id * CHUNK_SIZE
                        + (CHUNK_SIZE - self.tail.head_last_chunk_free_blocks as usize
                            + i as usize),
                );
            }
        }

        let mut curr_chunk_idx = offset / CHUNK_SIZE;
        let mut curr_chunk_inner_offset = offset % CHUNK_SIZE;
        while res_blocks.len() <= nblocks {
            res_blocks.push(chunks[curr_chunk_idx] * CHUNK_SIZE + curr_chunk_inner_offset);
            curr_chunk_inner_offset += 1;
            if curr_chunk_inner_offset == CHUNK_SIZE {
                curr_chunk_inner_offset = 0;
                curr_chunk_idx += 1;
            }
        }
        res_blocks
    }
}

unsafe impl<D> Send for RawLogStore<D> {}
unsafe impl<D> Sync for RawLogStore<D> {}

mod edit;
mod state;

pub use self::edit::RawLogStoreEdit;
pub use self::state::RawLogStoreState;
