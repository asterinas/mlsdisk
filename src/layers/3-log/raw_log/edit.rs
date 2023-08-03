use super::{
    state::{RawLogEntry, RawLogStoreState},
    RawLogId,
};
use crate::{
    layers::log::chunk::CHUNK_SIZE,
    layers::{edit::Edit, log::chunk::ChunkId},
    prelude::*,
    tx::TxData,
};

use alloc::collections::BTreeMap;

pub struct RawLogStoreEdit {
    edit_table: BTreeMap<RawLogId, RawLogEdit>,
}

pub(super) enum RawLogEdit {
    Create(RawLogCreate),
    Append(RawLogAppend),
    Delete,
}

pub(super) struct RawLogCreate {
    pub tail: RawLogTail,
    // Whether the log is deleted after being created in a TX.
    // is_deleted: bool, // Use LazyDelete
}

pub(super) struct RawLogAppend {
    pub tail: RawLogTail,
    // Whether the log is deleted after being appended in a TX.
    // is_deleted: bool, // Use LazyDelete
}

pub(super) struct RawLogTail {
    // The last chunk of the head. If it is partially filled
    // (head_last_chunk_free_blocks > 0), then the tail should write to the
    // free blocks in the last chunk of the head.
    pub head_last_chunk_id: ChunkId,
    pub head_last_chunk_free_blocks: u16,
    // The chunks allocated and owned by the tail
    pub chunks: Vec<ChunkId>,
    // The total number of blocks in the tail, including the blocks written to
    // the last chunk of head and those written to the chunks owned by the tail.
    pub num_blocks: usize,
}

impl RawLogStoreEdit {
    pub fn new() -> Self {
        Self {
            edit_table: BTreeMap::new(),
        }
    }

    pub fn create_log(&mut self, new_log_id: RawLogId) {
        let create_edit = RawLogEdit::Create(RawLogCreate::new());
        let edit_exists = self.edit_table.insert(new_log_id, create_edit);
        debug_assert!(edit_exists.is_none());
    }

    pub(super) fn open_log(&mut self, log_id: RawLogId, log_entry: &RawLogEntry) -> Result<()> {
        match self.edit_table.get(&log_id) {
            None => {
                // insert an Append
                let tail = {
                    let head = &log_entry.head;
                    RawLogTail {
                        head_last_chunk_id: *head.chunks.last().unwrap_or(&0),
                        head_last_chunk_free_blocks: (head.chunks.len() * CHUNK_SIZE
                            - head.num_blocks as usize)
                            as _,
                        chunks: Vec::new(),
                        num_blocks: 0,
                    }
                };
                self.edit_table
                    .insert(log_id, RawLogEdit::Append(RawLogAppend { tail }));
            }
            Some(edit) => {
                // if edit == create, unreachable: there can't be a persistent log entry,
                // log creation tx still ongoing
                if let RawLogEdit::Create(_) = edit {
                    unreachable!();
                }
                // if edit == append, do nothing
                // if edit == delete, panic
                if let RawLogEdit::Delete = edit {
                    panic!("try to open a deleted log!");
                }
            }
        }

        Ok(())
    }

    /// Return the chunks of the deleted log
    pub fn delete_log(&mut self, log_id: RawLogId) -> Option<Vec<ChunkId>> {
        match self.edit_table.insert(log_id, RawLogEdit::Delete) {
            None => None,
            Some(RawLogEdit::Create(create)) => {
                // Seems no need to panic in create, just delete it (no new chunk allocated)
                Some(create.tail.chunks.clone())
            }
            Some(RawLogEdit::Append(_)) => {
                panic!("can't delete an appended log");
            }
            Some(RawLogEdit::Delete) => panic!("can't delete a deleted log"),
        }
    }

    pub fn is_log_created(&self, log_id: RawLogId) -> bool {
        match self.edit_table.get(&log_id) {
            Some(RawLogEdit::Create(_)) | Some(RawLogEdit::Append(_)) => true,
            Some(RawLogEdit::Delete) | None => false,
        }
    }

    pub fn iter_deleted_logs(&self) -> impl Iterator<Item = &RawLogId> {
        self.edit_table
            .iter()
            .filter(|(_, edit)| if let RawLogEdit = edit { true } else { false })
            .map(|(id, _)| id)
    }

    pub(super) fn get_edit(&mut self, log_id: RawLogId) -> Option<&mut RawLogEdit> {
        self.edit_table.get_mut(&log_id)
    }
}

impl Edit<RawLogStoreState> for RawLogStoreEdit {
    fn apply_to(&self, state: &mut RawLogStoreState) {
        for (&log_id, log_edit) in self.edit_table.iter() {
            match log_edit {
                RawLogEdit::Create(create) => {
                    let RawLogCreate { tail } = create;
                    state.create_log(log_id);
                    state.append_log(log_id, tail);
                }
                RawLogEdit::Append(append) => {
                    let RawLogAppend { tail } = append;
                    state.append_log(log_id, tail);
                }
                RawLogEdit::Delete => {
                    // Do lazy delete
                    // state.delete_log(log_id),
                }
            }
        }
    }
}

impl RawLogCreate {
    fn new() -> Self {
        Self {
            tail: RawLogTail::new(),
        }
    }
}

impl RawLogTail {
    fn new() -> Self {
        Self {
            head_last_chunk_id: 0,
            head_last_chunk_free_blocks: 0,
            chunks: Vec::new(),
            num_blocks: 0,
        }
    }
}

impl TxData for RawLogStoreEdit {}
