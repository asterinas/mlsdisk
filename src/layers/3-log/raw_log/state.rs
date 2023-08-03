use super::{edit::RawLogTail, RawLogId, RawLogStoreEdit};
use super::{ChunkId, CHUNK_SIZE};
use crate::os::Mutex;
use crate::prelude::*;
use crate::{layers::edit::Edit, util::LazyDelete};

use alloc::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// The volatile and persistent state of a `RawLogStore`.
pub(super) struct State {
    persistent: RawLogStoreState,
    append_set: BTreeSet<RawLogId>,
    lazy_deletes: BTreeMap<RawLogId, LazyDelete<RawLogId>>,
}

/// The persistent state of a `RawLogStore`.
#[derive(Clone)]
pub struct RawLogStoreState {
    log_table: BTreeMap<RawLogId, Arc<Mutex<RawLogEntry>>>,
    next_free_log_id: u64,
}

pub(super) struct RawLogEntry {
    pub head: RawLogHead,
    // is_deleted: bool, // Use LazyDelete
}

pub(super) struct RawLogHead {
    pub chunks: Vec<ChunkId>,
    pub num_blocks: usize,
}

impl State {
    pub fn new(persistent: RawLogStoreState) -> Self {
        let mut new_self = Self {
            persistent: persistent.clone(),
            append_set: BTreeSet::new(),
            lazy_deletes: BTreeMap::new(),
        };

        // Init lazy-delete
        let delete_fn = |log_id: &mut RawLogId| {
            new_self.persistent.delete_log(*log_id);
            new_self.remove_from_append_set(*log_id);
        };
        for (log_id, _) in new_self.persistent.log_table {
            new_self
                .log_table
                .insert(log_id, LazyDelete::new(log_id, delete_fn));
        }

        new_self
    }

    pub fn apply(&mut self, edit: &RawLogStoreEdit) {
        edit.apply_to(&mut self.persistent);

        // Do lazy deletion of logs
        let deleted_log_ids = edit.iter_deleted_logs();
        for deleted_log_id in deleted_log_ids {
            let Some(lazy_delete) = self.lazy_deletes.remove(&deleted_log_id) else {
                // Other concurrent TXs have deleted the same log
                continue;
            };
            LazyDelete::delete(&lazy_delete);
        }
    }

    pub fn add_to_append_set(&mut self, log_id: RawLogId) -> Result<()> {
        let already_exists = self.append_set.insert(log_id);
        if already_exists {
            // Obey single-writer rule
            return_errno!(NotFound);
        }
        Ok(())
    }

    pub fn remove_from_append_set(&mut self, log_id: RawLogId) {
        let is_removed = self.append_set.remove(&log_id);
        debug_assert!(is_removed == false);
    }

    pub fn add_lazy_delete(&mut self, log_id: RawLogId) {
        let already_exists = self.lazy_deletes.insert(log_id, todo!());
        debug_assert!(already_exists.is_none());
    }

    pub fn persistent(&mut self) -> &mut RawLogStoreState {
        &mut self.persistent
    }
}

impl RawLogStoreState {
    pub fn new() -> Self {
        // let next_free_log_id = persistent.max_log_id.map_or(0, |max| max + 1);
        Self {
            log_table: BTreeMap::new(),
            next_free_log_id: 0,
        }
    }

    pub fn alloc_log_id(&mut self) -> u64 {
        let new_log_id = self.next_free_log_id;
        self.next_free_log_id = self
            .next_free_log_id
            .checked_add(1)
            .expect("64-bit IDs won't be exhausted even though IDs are not recycled");
        new_log_id
    }

    pub(super) fn find_log(&self, log_id: u64) -> Option<Arc<Mutex<RawLogEntry>>> {
        let log_table = &self.log_table;
        log_table.get(&log_id).map(|entry| entry.clone())
    }

    // Do this in `RawLogStoreEdit::apply_to()`
    // pub fn commit(&mut self, edit: &mut RawLogStoreEdit, chunk_alloc: &ChunkAlloc) {
    // }

    pub fn create_log(&mut self, new_log_id: u64) {
        let log_table = &mut self.log_table;
        let new_log_entry = Arc::new(Mutex::new(RawLogEntry { head: todo!() }));
        let already_exists = log_table.insert(new_log_id, new_log_entry).is_some();
        debug_assert!(already_exists == false);
        // TODO: Insert `LazyDelete`
    }

    pub(super) fn append_log(&mut self, log_id: u64, tail: &RawLogTail) {
        let mut log_entry = self.log_table.get_mut(&log_id).unwrap().lock();
        log_entry.head.append(tail);
    }

    pub fn delete_log(&mut self, log_id: u64) {
        let Some(_) = self.log_table.remove(&log_id) else {
            return;
        };
        // Donot need dealloc here, no tx in state, dealloc in `RawLogStore::delete_log()`
        // self.chunk_alloc.dealloc_batch(log_entry.head.chunks.iter());
    }
}

impl RawLogHead {
    pub fn append(&mut self, tail: &RawLogTail) {
        // Update head
        self.chunks.extend(tail.chunks.iter());
        self.num_blocks += tail.num_blocks;
        // Update tail
        // Should we still need to update tail?
        let head_last_chunk_free_blocks = self.num_blocks % CHUNK_SIZE;
        tail.head_last_chunk_free_blocks = head_last_chunk_free_blocks as _;
        tail.head_last_chunk_id = *self.chunks.last().unwrap();
        tail.num_blocks = 0;
    }
}
