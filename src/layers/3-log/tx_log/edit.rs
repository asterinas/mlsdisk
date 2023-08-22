use super::{state::TxLogStoreState, BucketName, Key, TxLogId, TxLogInner};
use crate::layers::{crypto::RootMhtMeta, edit::Edit};

use std::{collections::BTreeMap, sync::Arc};

pub(super) struct TxLogStoreEdit {
    edit_table: BTreeMap<TxLogId, TxLogEdit>,
}

// Used for per-tx data, track open logs in memory
pub(super) struct OpenLogTable<D> {
    pub log_table: BTreeMap<TxLogId, Arc<TxLogInner<D>>>,
}

pub enum TxLogEdit {
    Create(TxLogCreate),
    Append(TxLogAppend),
    Delete,
}

struct TxLogCreate {
    bucket: BucketName,
    key: Key,
    root_mht: Option<RootMhtMeta>,
}

struct TxLogAppend {
    root_mht: RootMhtMeta,
}

impl TxLogStoreEdit {
    pub fn is_log_deleted(&self, log_id: TxLogId) -> bool {
        match self.edit_table.get(&log_id) {
            Some(TxLogEdit::Delete) => true,
            _ => false,
        }
    }

    pub fn is_log_created(&self, log_id: TxLogId) -> bool {
        match self.edit_table.get(&log_id) {
            Some(TxLogEdit::Create(_)) | Some(TxLogEdit::Append(_)) => true,
            None | Some(TxLogEdit::Delete) => false,
        }
    }

    pub fn delete_log(&mut self, log_id: TxLogId) -> Result<()> {
        match self.edit_table.get_mut(&log_id) {
            None => {
                self.edit_table.insert(log_id, TxLogEdit::Delete);
            }
            Some(TxLogEdit::Create(_)) => {
                self.edit_table.remove(&log_id);
            }
            Some(TxLogEdit::Append(_)) => {
                panic!("TxLogEdit::Append is added at very late stage, after which logs won't get deleted");
            }
            Some(TxLogEdit::Delete) => {
                return Err(ENOENT);
            }
        }
        Ok(())
    }

    pub fn iter_deleted_logs(&self) -> impl Iterator<Item = TxLogId> {
        self.edit_table
            .iter()
            .filter(|edit| edit == TxLogEdit::Delete)
    }

    pub fn update_log_metas<I>(&mut self, iter: I)
    where
        I: Iterator<Item = (TxLogId, RootMhtMeta)>,
    {
        // For newly-created logs, update RootMhtMeta
        // For existing logs that are appended, add TxLogAppend
        for (log_id, root_mht) in iter {
            match self.edit_table.get_mut(&log_id) {
                None | Some(TxLogEdit::Delete) => {
                    panic!("shouldn't happen");
                }
                Some(TxLogEdit::Create(create)) => {
                    create.root_mht.insert(root_mht);
                    // self.edit_table.insert(log_id, TxLogEdit::Append(root_mht));
                }
                Some(TxLogEdit::Append(append)) => {
                    append.root_mht = root_mht;
                }
            }
        }
    }

    pub fn create_log(&mut self, log_id: TxLogId, bucket: BucketName, key: Key) {
        let already_created = self.edit_table.insert(
            key,
            TxLogEdit::Create(TxLogCreate {
                bucket,
                key,
                root_mht: None,
            }),
        );
        debug_assert!(already_created.is_none());
    }

    pub fn append_log(&mut self, log_id: TxLogId, root_mht: RootMhtMeta) {
        self.edit_table
            .insert(log_id, TxLogEdit::Append(TxLogAppend { root_mht }));
    }
}

impl Edit<TxLogStoreState> for TxLogStoreEdit {
    fn apply_to(&self, state: &mut TxLogStoreState) {
        for (log_id, log_edit) in &self.edit_table {
            match log_edit {
                TxLogEdit::Create(create_log) => {
                    let TxLogCreate {
                        bucket,
                        key,
                        root_mht,
                        ..
                    } = create_log;
                    state.create_log(log_id, bucket, key, root_mht.unwrap());
                }
                TxLogEdit::Append(append_log) => {
                    let TxLogAppend { root_mht, .. } = append_log;
                    state.append_log(log_id, root_mht);
                }
                TxLogEdit::Delete => {
                    state.delete_log(log_id);
                }
            }
        }
    }
}
