//! A store of transactional logs.
use self::edit::{OpenLogTable, TxLogEdit, TxLogStoreEdit};
use self::journaling::{AllState, Journal, JournalCompactPolicy};
use self::state::{State, TxLogEntry, TxLogStoreState};
use super::chunk::{ChunkAlloc, ChunkAllocEdit, ChunkAllocState};
use super::raw_log::{RawLog, RawLogId, RawLogStore, RawLogStoreEdit, RawLogStoreState};
use crate::layers::bio::{BlockBuf, BlockId, BlockSet};
use crate::layers::crypto::{CryptoLog, RootMhtMeta};
use crate::layers::edit::{Edit, EditJournalMeta};
use crate::tx::{CurrentTx, Tx, TxId, TxProvider};
use crate::util::LazyDelete;

use spin::Mutex;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub type TxLogId = RawLogId;

type BucketName = Arc<String>;

/// A store of transactional logs.
pub struct TxLogStore<D> {
    state: Arc<Mutex<State>>,
    key: Key,
    raw_log_store: Arc<RawLogStore<D>>,
    journal: Arc<Mutex<Journal<D>>>,
    tx_provider: Arc<TxProvider>,
}

struct Superblock {
    journal_area_meta: EditJournalMeta,
    chunk_area_nblocks: usize,
}

impl<D: BlockSet> TxLogStore<D> {
    /// Format the disk to create a new instance of `TxLogStore`.
    ///
    /// Each instance will be assigned a unique, automatically-generated root
    /// key.
    pub fn format(disk: D) -> Result<Self> {
        let tx_provider = TxProvider::new();
        let tx_log_store_state = TxLogStoreState::new();

        let chunk_alloc = ChunkAlloc::new(disk.nblocks(), tx_provider);
        let raw_log_store = RawLogStore::new(disk, tx_provider, chunk_alloc);

        let state = State::new(tx_log_store_state, &raw_log_store);
        let all_state = AllState {
            chunk_alloc: ChunkAllocState::new(disk.nblocks()),
            raw_log_store: RawLogStoreState::new(),
            tx_log_store: tx_log_store_state,
        };
        let journal = Journal::format(
            all_state,
            disk,
            std::mem::size_of::<AllState>(),
            JournalCompactPolicy {},
        );

        Ok(Self::from_parts(
            tx_log_store_state,
            Key::gen_unique(),
            raw_log_store,
            journal,
            tx_provider,
        ))
    }

    /// Recover an existing `TxLogStore` from a disk using the given key.
    pub fn recover(disk: D, key: Key) -> Result<Self> {
        let sb: Superblock = Superblock::open(disk.subset(0..1), &key)?;

        if disk.nblocks() < sb.required_nblocks() {
            return Err(ENOSPC);
        }

        let tx_provider = TxProvider::new();

        let journal_area_size = sb.journal_area_meta.total_nblocks();
        let journal = {
            let journal_area = disk.subset(1..1 + journal_area_size);
            let journal =
                Journal::recover(journal_area, &sb.journal_area_meta, JournalCompactPolicy {})?;
            let journal = Arc::new(Mutex::new(journal));
            journal
        };

        let state = journal.state();
        let chunk_alloc = ChunkAlloc::from_parts(state.chunk_alloc.clone(), tx_provider.clone());
        let chunk_area =
            disk.subset(1 + journal_area_size..1 + journal_area_size + sb.chunk_area_nblocks);
        let raw_log_store: RawLogStore<D> = RawLogStore::from_parts(
            state.raw_log_store.clone(),
            chunk_area,
            chunk_alloc,
            tx_provider.clone(),
        );
        let tx_log_store = TxLogStore::from_parts(
            state.tx_log_store.clone(),
            key,
            raw_log_store,
            journal,
            tx_provider,
        );
        Ok(tx_log_store)
    }

    fn from_parts(
        state: TxLogStoreState,
        key: Key,
        raw_log_store: RawLogStore<D>,
        journal: Arc<Mutex<Journal<D>>>,
        tx_provider: Arc<TxProvider>,
    ) -> Self {
        let new_self = Self {
            state: Arc::new(Mutex::new(state)),
            key,
            raw_log_store,
            journal,
            tx_provider,
        };

        tx_provider.register_data_initializer(|| TxLogStoreEdit::new());
        tx_provider.register_data_initializer(|| OpenLogTable::new());
        tx_provider.register_precommit_handler({
            |current| {
                // Do I/O in the pre-commit phase. If any I/O error occured,
                // the TX would be aborted.
                let dirty_log_ids_and_metas =
                    current.data_with(|open_log_table: &OpenLogTable<D>| {
                        let mut ids_and_metas = Vec::new();
                        for (log_id, inner_log) in &open_log_table.log_table {
                            if !inner_log.is_dirty {
                                continue;
                            }

                            let crypto_log = &inner_log.crypto_log;
                            crypto_log.flush()?;
                            let new_log_meta = *crypto_log.root_meta().unwrap();
                            ids_and_metas.push((log_id, new_log_meta));
                        }
                        Ok(ids_and_metas)
                    })?;
                current.data_mut_with(|store_edit: &mut TxLogStoreEdit| {
                    store_edit.update_log_metas(dirty_log_ids_and_metas.iter())
                });
                Ok(())
            }
        });

        // Commit handler for journal
        tx_provider.register_commit_handler({
            let journal = journal.clone();
            |current| {
                let mut journal = journal.lock();
                current.data_with(|edit: &ChunkAllocEdit| {
                    journal.add(edit);
                });
                current.data_with(|edit: &RawLogStoreEdit| {
                    journal.add(edit);
                });
                current.data_with(|edit: &TxLogStoreEdit| {
                    journal.add(edit);
                });
                journal.commit();
            }
        });
        // Commit handler for store
        tx_provider.register_commit_handler({
            let state = new_self.state.clone();
            |current| {
                current.data_with(|store_edit: &TxLogStoreEdit| {
                    let mut state = state.lock();
                    state.apply(&store_edit);
                });
            }
        });

        new_self
    }

    /// List the IDs of all logs in a bucket.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn list_logs(&self, bucket_name: &str) -> Result<Vec<TxLogId>> {
        let mut current_tx = self.tx_provider.current();
        let state = self.state.lock();

        let mut log_id_set = state.list_logs(bucket_name)?;
        current_tx.data_with(|store_edit: &TxLogStoreEdit| {
            for (log_id, log_edit) in &store_edit.edit_table() {
                match log_edit {
                    TxLogEdit::Create(_) => {
                        log_id_set.insert(log_id);
                    }
                    TxLogEdit::Append(_) => {}
                    TxLogEdit::Delete => {
                        log_id_set.remove(log_id);
                    }
                }
            }
        });
        let log_id_vec = log_id_set.iter().collect();
        Ok(log_id_vec)
    }

    /// Creates a new, empty log in a bucket.
    ///
    /// On success, the returned `TxLog` is opened in the appendable mode.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn create_log(&self, bucket: &str) -> Result<TxLog> {
        let raw_log = self.raw_log_store.create_log()?;
        let log_id = raw_log.id();
        let crypto_log = CryptoLog::new(raw_log, self.key);
        let current_tx = self.tx_provider.current();
        let tx_id = current_tx.id();
        let lazy_delete = {
            let raw_log_store = self.raw_log_store.clone();
            let lazy_delete = LazyDelete::new(log_id, move |log_id| {
                raw_log_store.delete_log(log_id).unwrap();
            });
            Arc::new(lazy_delete)
        };
        let inner_log = Arc::new(TxLogInner {
            log_id,
            tx_id,
            bucket,
            crypto_log,
            lazy_delete,
            is_dirty: false,
        });

        current_tx.data_with(|store_edit: &TxLogStoreEdit| {
            store_edit.create_log(log_id, bucket, self.key)
        });

        current_tx.data_mut_with(|open_log_table: &mut OpenLogTable| {
            open_log_table.log_table.insert(log_id, inner_log);
        });

        Ok(TxLog {
            inner_log,
            can_append: true,
        })
    }

    /// Opens the log of a given ID.
    ///
    /// For any log at any time, there can be at most one Tx that opens the log
    /// in the appendable mode.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn open_log(&self, log_id: TxLogId, can_append: bool) -> Result<Arc<TxLog>> {
        let mut current_tx = self.tx_provider.current();
        let inner_log = self.open_inner_log(log_id, can_append, &mut current_tx)?;
        let tx_log = TxLog::new(inner_log, can_append);
        Ok(Arc::new(tx_log))
    }

    fn open_inner_log(
        &self,
        log_id: TxLogId,
        can_append: bool,
        current_tx: &mut CurrentTx<'_>,
    ) -> Result<Arc<TxLogInner<D>>> {
        // Fast path: the log has been opened in this TX
        let has_opened = current_tx.data_with(|table: &OpenLogTable<D>| {
            table.log_table().get(&log_id).map(|log| log.clone())
        });
        if let Some(inner_log) = has_opened {
            return Ok(inner_log);
        }

        // Slow path: the first time a log is to be opened in a TX
        let state = self.state.lock();
        let log_entry = {
            // The log must exist in state...
            let log_entry: TxLogEntry = state.persistent().find_log(log_id)?;
            // ...and not be marked deleted by data.edit
            let is_deleted = current_tx
                .data_with(|store_edit: &TxLogStoreEdit| store_edit.is_log_deleted(log_id));
            if is_deleted {
                return Err(ENOENT);
            }
            log_entry
        };
        let bucket = log_entry.bucket().clone();
        let crypto_log = {
            let raw_log = self.raw_log_store.open_log(log_id, can_append)?;
            let key = log_entry.key();
            let root_meta = log_entry.root_meta;
            CryptoLog::open(raw_log, key, root_meta)?
        };
        let tx_id = current_tx.id();
        let lazy_delete = {
            let raw_log_store = self.raw_log_store.clone();
            let lazy_delete = LazyDelete::new(log_id, move |log_id| {
                raw_log_store.delete_log(log_id).unwrap();
            });
            Arc::new(lazy_delete)
        };

        let inner_log = Arc::new(TxLogInner {
            log_id,
            tx_id,
            bucket,
            crypto_log,
            lazy_delete,
            is_dirty: false,
        });

        current_tx.data_mut_with(|open_table: &mut OpenLogTable<D>| {
            open_table.log_table().insert(log_id, inner_log.clone())
        });
        if can_append {
            current_tx.data_with(|store_edit: &TxLogStoreEdit| {
                store_edit.append_log(log_id, crypto_log.root_meta.unwrap());
            });
        }
        Ok(inner_log)
    }

    /// Opens the log with the maximum ID in a bucket.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn open_log_in(&self, bucket: &str) -> Result<Arc<TxLog<D>>> {
        let log_ids = self.list_logs(bucket)?;
        let max_log_id = log_ids.iter().max().ok_or(ENOENT)?;
        self.open_log(max_log_id, false)
    }

    /// Checks whether the log of a given log ID exists or not.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn contains_log(&self, log_id: TxLogId) -> bool {
        let mut current_tx = self.tx_provider.current();
        let state = self.state.lock();
        self.do_contain_log(log_id, &state, &current_tx)
    }

    fn do_contain_log(&self, log_id: TxLogId, state: &State, current_tx: &CurrentTx<'_>) -> bool {
        if state.contains_log(log_id) {
            let not_deleted = current_tx
                .data_with(|store_edit: &TxLogStoreEdit| !store_edit.is_log_deleted(log_id));
            not_deleted
        } else {
            let is_created = current_tx
                .data_with(|store_edit: &TxLogStoreEdit| store_edit.is_log_created(log_id));
            is_created
        }
    }

    /// Deletes the log of a given ID.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn delete_log(&self, log_id: TxLogId) -> Result<()> {
        let mut current_tx = self.tx_provider.current();
        let state = self.state.lock();

        if !self.do_contain_log(log_id, &state, &current_tx) {
            return Err(ENOENT);
        }

        current_tx
            .data_mut_with(|store_edit: &mut TxLogStoreEdit| store_edit.delete(log_id).unwrap());
        current_tx.data_mut_with(|open_log_table: &mut OpenLogTable| {
            let inner_log_opt = open_log_table.log_table.remove(&log_id);
            let Some(inner_log) = inner_log_opt else {
                // The deleted log has not been opened
                return
            };
            // LazyDelete::delete(&inner_log.lazy_delete); // No need, do this in commit phase
        });
        Ok(())
    }

    /// Returns the root key.
    pub fn key(&self) -> &Key {
        &self.key
    }

    pub fn new_tx(&self) -> Tx {
        self.tx_provider.new_tx()
    }
}

impl Superblock {
    /// Returns the total number of blocks occupied by the `TxLogStore`.
    pub fn total_nblocks(&self) -> usize {
        self.journal_area_meta.total_nblocks() + self.chunk_area_nblocks
    }

    pub fn open<D: BlockSet>(disk: D, key: &Key) -> Result<Self> {
        todo!()
    }
}

/// A transactional log.
#[derive(Clone)]
pub struct TxLog<D> {
    inner_log: Arc<TxLogInner<D>>,
    can_append: bool,
}

struct TxLogInner<D> {
    log_id: TxLogId,
    tx_id: TxId,
    bucket: BucketName,
    crypto_log: CryptoLog<RawLog<D>>,
    lazy_delete: Arc<LazyDelete<u64>>,
    is_dirty: bool,
}

impl<D> TxLog<D> {
    fn new(inner_log: Arc<TxLogInner<D>>, can_append: bool) -> Self {
        Self {
            inner_log,
            can_append,
        }
    }

    /// Returns the log ID.
    pub fn id(&self) -> TxLogId {
        self.log_id
    }

    /// Returns the TX ID.
    pub fn tx_id(&self) -> TxId {
        self.tx_id
    }

    /// Returns the bucket that this log belongs to.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Returns whether the log is opened in the appendable mode.
    pub fn can_append(&self) -> bool {
        return self.can_append;
    }

    /// Reads one or multiple data blocks at a specified position.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn read(&self, pos: BlockId, buf: &mut impl BlockBuf) -> Result<()> {
        self.inner_log.crypto_log.read(pos, buf)
    }

    /// Appends one or multiple data blocks at the end.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn append(&self, buf: &impl BlockBuf) -> Result<()> {
        if !self.can_append {
            return Err(EPERM);
        }

        self.inner_log.is_dirty = true;
        self.inner_log.crypto_log.append(buf)
    }

    /// Returns the length of the log in unit of block.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn nblocks(&self) -> usize {
        self.crypto_log.num_blocks()
    }
}

type Key = [u8; 16];

mod edit;
mod journaling;
mod state;
