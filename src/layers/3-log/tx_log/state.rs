use super::{edit::TxLogStoreEdit, BucketName, Key, TxLogId};
use crate::{
    layers::{
        crypto::RootMhtMeta,
        log::{self, raw_log::RawLogStore},
    },
    util::LazyDelete,
};

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// The volatile and persistent state of a `TxLogStore`.
pub(super) struct State {
    persistent: TxLogStoreState,
    log_table: BTreeMap<TxLogId, Arc<LazyDelete<TxLogId>>>,
}

/// The persistent state of a `TxLogStore`.
pub(super) struct TxLogStoreState {
    log_table: BTreeMap<TxLogId, TxLogEntry>,
    bucket_table: BTreeMap<BucketName, Bucket>,
}

pub(super) struct TxLogEntry {
    bucket: BucketName,
    key: Key,
    root_mht: RootMhtMeta,
}

struct Bucket {
    log_ids: BTreeSet<TxLogId>,
}

impl<D> State<D> {
    pub fn new(persistent: TxLogStoreState, raw_log_store: &Arc<RawLogStore<D>>) -> Self {
        let log_table = {
            let mut log_table = BTreeMap::new();
            let delete_fn = |log_id| {
                raw_log_store.delete_log(log_id).unwrap();
            };
            for (log_id, _) in persistent.log_table {
                log_table.insert(log_id, LazyDelete::new(log_id, delete_fn));
            }
            log_table
        };
        Self {
            persistent,
            log_table,
        }
    }

    pub fn apply(&mut self, edit: &TxLogStoreEdit) {
        edit.apply_to(&mut self.persistent); // apply edit to self.persistent

        // Do lazy deletion of logs
        let deleted_log_ids = edit.iter_deleted_logs();
        for deleted_log_id in deleted_log_ids {
            let Some(lazy_delete) = self.log_table.remove(&deleted_log_id) else {
                // Other concurrent TXs have deleted the same log
                continue;
            };
            LazyDelete::delete(&lazy_delete);
        }
    }
}

impl TxLogStoreState {
    pub fn new() -> Self {
        Self {
            log_table: BTreeMap::new(),
            bucket_table: BTreeMap::new(),
        }
    }

    pub fn create_log(
        &mut self,
        new_log_id: TxLogId,
        bucket: BucketName,
        key: Key,
        root_mht: RootMhtMeta,
    ) {
        let already_exists = self.log_table.insert(
            new_log_id,
            TxLogEntry {
                bucket,
                key,
                root_mht,
            },
        );
        debug_assert!(already_exists.is_none());

        match self.bucket_table.get_mut(&bucket) {
            Some(bucket) => bucket.log_ids.insert(new_log_id),
            None => self.bucket_table.insert(
                bucket,
                Bucket {
                    log_ids: BTreeSet::from([new_log_id]),
                },
            ),
        }
        // TODO: Insert `LazyDelete`
    }

    pub fn append_log(&mut self, log_id: TxLogId, root_mht: RootMhtMeta) {
        let entry = self.log_table.get_mut(&log_id).unwrap();
        entry.root_mht = root_mht;
    }

    pub fn list_logs(&self, bucket_name: &str) -> Result<BTreeSet<TxLogId>> {
        let bucket = self.bucket_table.get(bucket_name).ok_or(ENOENT)?;
        Ok(bucket.log_ids.clone())
    }

    pub fn find_log(&self, log_id: TxLogId) -> Result<&TxLogEntry> {
        self.log_table.get(&log_id).ok_or(ENOENT)
    }

    pub fn contains_log(&self, log_id: TxLogId) -> bool {
        self.log_table.contains(&log_id)
    }

    pub fn delete_log(&mut self, log_id: TxLogId) {
        // Do not check the result because concurrent TXs may decide to delete
        // the same logs.
        let _ = self.log_table.remove(&log_id);
    }
}
