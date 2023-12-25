//! A store of transactional logs.
//!
//! `TxLogStore<D>` supports creating, deleting, listing, reading,
//! and writing `TxLog<D>`s within transactions. Each `TxLog<D>`
//! is uniquely identified by its ID (`TxLogId`). Writing to a TX log
//! is append only. TX logs are categorized into pre-determined buckets.
//!
//! File content of `TxLog<D>` is stored securely using a `CryptoLog<RawLog<D>>`,
//! whose storage space is backed by untrusted log `RawLog<D>`,
//! whose host blocks are managed by `ChunkAlloc`. The whole untrusted
//! host disk that `TxLogSore<D>` used is represented by a `BlockSet`.
//!
//! # Examples
//!
//! TX logs are manipulated and accessed within transactions.
//!
//! ```
//! fn create_and_read_log<D: BlockSet>(
//!     tx_log_store: &TxLogStore<D>,
//!     bucket: &str
//! ) -> Result<()> {
//!     let content = 5_u8;
//!
//!     // TX 1: Create then write a new log
//!     let mut tx = tx_log_store.new_tx();
//!     let res: Result<_> = tx.context(|| {
//!         let new_log = tx_log_store.create_log(bucket)?;
//!         let mut buf = Buf::alloc(1)?;
//!         buf.as_mut_slice().fill(content);
//!         new_log.append(buf.as_ref())
//!     });
//!     if res.is_err() {
//!         tx.abort();
//!     }
//!     tx.commit()?;
//!
//!     // TX 2: Open then read the created log
//!     let mut tx = tx_log_store.new_tx();
//!     let res: Result<_> = tx.context(|| {
//!         let log = tx_log_store.open_log_in(bucket)?;
//!         let mut buf = Buf::alloc(1)?;
//!         log.read(0 as BlockId, buf.as_mut())?;
//!         assert_eq!(buf.as_slice()[0], content);
//!         Ok(())
//!     });
//!     if res.is_err() {
//!         tx.abort();
//!     }
//!     tx.commit()
//! }
//! ```
//!
//! `TxLogStore<D>`'s API is designed to be a limited POSIX FS
//! and must be called within transactions (`Tx`). It mitigates user burden by
//! minimizing the odds of conflicts among TXs:
//! 1) Prohibiting concurrent TXs from opening the same log for
//! writing (no write conflicts);
//! 2) Implementing lazy log deletion to avoid interference with
//! other TXs utilizing the log (no deletion conflicts);
//! 3) Identifying logs by system-generated IDs (no name conflicts).
use self::journaling::{AllEdit, AllState, Journal, JournalCompactPolicy};
use super::chunk::{ChunkAlloc, ChunkAllocEdit, ChunkAllocState};
use super::raw_log::{RawLog, RawLogId, RawLogStore, RawLogStoreEdit, RawLogStoreState};
use crate::layers::bio::{BlockId, BlockSet, Buf, BufMut, BufRef};
use crate::layers::crypto::{CryptoLog, NodeCache, RootMhtMeta};
use crate::layers::edit::{CompactPolicy, Edit, EditJournal, EditJournalMeta};
use crate::layers::log::chunk::CHUNK_NBLOCKS;
use crate::os::{AeadKey as Key, Mutex, RwLock, Skcipher, SkcipherIv, SkcipherKey};
use crate::prelude::*;
use crate::tx::{CurrentTx, Tx, TxData, TxId, TxProvider};
use crate::util::{LazyDelete, RandomInit};

use alloc::collections::{BTreeMap, BTreeSet}; // TODO: Find alternatives to adapt 'rust-for-linux'
use core::any::Any;
use core::sync::atomic::{AtomicBool, Ordering};
use lru::LruCache;
use pod::Pod;
use serde::{Deserialize, Serialize};

pub type TxLogId = RawLogId;
type BucketName = String;

/// A store of transactional logs.
///
/// Disk layout:
/// ```text
/// ------------------------------------------------------
/// | Superblock |  RawLogStore region  | Journal region |
/// ------------------------------------------------------
/// ```
#[derive(Clone)]
pub struct TxLogStore<D> {
    state: Arc<Mutex<State>>,
    key: Key,
    raw_log_store: Arc<RawLogStore<D>>,
    journal: Arc<Mutex<Journal<D>>>,
    tx_provider: Arc<TxProvider>,
}

/// Superblock of `TxLogStore`.
#[repr(C)]
#[derive(Clone, Copy, Pod, Debug)]
pub struct Superblock {
    journal_area_meta: EditJournalMeta,
    chunk_area_nblocks: usize,
}

impl<D: BlockSet + 'static> TxLogStore<D> {
    /// Formats the disk to create a new instance of `TxLogStore`.
    ///
    /// Each instance will be assigned a unique, automatically-generated root
    /// key.
    pub fn format(disk: D) -> Result<Self> {
        let total_nblocks = disk.nblocks();
        let (log_store_nblocks, journal_nblocks) =
            Self::calc_store_and_journal_nblocks(total_nblocks);
        let log_store_area = disk.subset(1..1 + log_store_nblocks)?;
        let journal_area =
            disk.subset(1 + log_store_nblocks..1 + log_store_nblocks + journal_nblocks)?;

        let tx_provider = TxProvider::new();

        let nchunks = log_store_nblocks / CHUNK_NBLOCKS;
        let chunk_alloc = ChunkAlloc::new(nchunks, tx_provider.clone());
        let raw_log_store = RawLogStore::new(log_store_area, tx_provider.clone(), chunk_alloc);
        let tx_log_store_state = TxLogStoreState::new();

        let journal = {
            let all_state = AllState {
                chunk_alloc: ChunkAllocState::new(nchunks),
                raw_log_store: RawLogStoreState::new(),
                tx_log_store: tx_log_store_state.clone(),
            };
            Arc::new(Mutex::new(Journal::format(
                journal_area,
                all_state,
                16384, // TBD
                JournalCompactPolicy {},
            )?))
        };

        let superblock = Superblock {
            journal_area_meta: journal.lock().meta(),
            chunk_area_nblocks: log_store_nblocks,
        };
        let key = Key::random();
        superblock.persist(&disk, &key)?;

        Ok(Self::from_parts(
            tx_log_store_state,
            key,
            raw_log_store,
            journal,
            tx_provider,
        ))
    }

    fn calc_store_and_journal_nblocks(total_nblocks: usize) -> (usize, usize) {
        let log_store_nblocks = {
            let nblocks = (total_nblocks - 1) * 9 / 10;
            align_down(nblocks, CHUNK_NBLOCKS)
        };
        let journal_nblocks = total_nblocks - 1 - log_store_nblocks;
        debug_assert!(1 + log_store_nblocks + journal_nblocks <= total_nblocks);
        (log_store_nblocks, journal_nblocks)
    }

    /// Recovers an existing `TxLogStore` from a disk using the given key.
    pub fn recover(disk: D, key: Key) -> Result<Self> {
        let superblock = Superblock::open(&disk.subset(0..1)?, &key)?;
        if disk.nblocks() < superblock.total_nblocks() {
            return_errno_with_msg!(OutOfDisk, "given disk lacks space for recovering");
        }

        let tx_provider = TxProvider::new();

        let journal = {
            let journal_area_meta = &superblock.journal_area_meta;
            let journal_area = disk.subset(
                1 + superblock.chunk_area_nblocks
                    ..1 + superblock.chunk_area_nblocks + journal_area_meta.total_nblocks(),
            )?;
            Journal::recover(journal_area, &journal_area_meta, JournalCompactPolicy {})?
        };
        let all_state = journal.state();

        let chunk_alloc =
            ChunkAlloc::from_parts(all_state.chunk_alloc.clone(), tx_provider.clone());
        let chunk_area = disk.subset(1..1 + superblock.chunk_area_nblocks)?;
        let raw_log_store = RawLogStore::from_parts(
            all_state.raw_log_store.clone(),
            chunk_area,
            chunk_alloc,
            tx_provider.clone(),
        );
        let tx_log_store = TxLogStore::from_parts(
            all_state.tx_log_store.clone(),
            key,
            raw_log_store,
            Arc::new(Mutex::new(journal)),
            tx_provider,
        );

        Ok(tx_log_store)
    }

    fn from_parts(
        state: TxLogStoreState,
        key: Key,
        raw_log_store: Arc<RawLogStore<D>>,
        journal: Arc<Mutex<Journal<D>>>,
        tx_provider: Arc<TxProvider>,
    ) -> Self {
        let new_self = {
            // Prepare lazy deletes and log caches first from persistent state
            let (lazy_deletes, log_caches) = {
                let (mut delete_table, mut cache_table) = (BTreeMap::new(), BTreeMap::new());
                for log_id in state.list_all_logs() {
                    Self::add_lazy_delete(log_id, &mut delete_table, &raw_log_store);
                    cache_table.insert(log_id, Arc::new(CryptoLogCache::new(log_id, &tx_provider)));
                }
                (delete_table, cache_table)
            };

            Self {
                state: Arc::new(Mutex::new(State::new(state, lazy_deletes, log_caches))),
                key,
                raw_log_store,
                journal: journal.clone(),
                tx_provider: tx_provider.clone(),
            }
        };

        // TX data
        tx_provider.register_data_initializer(Box::new(|| TxLogStoreEdit::new()));
        tx_provider.register_data_initializer(Box::new(|| OpenLogTable::<D>::new()));
        tx_provider.register_data_initializer(Box::new(|| OpenLogCache::new()));

        // Precommit handler
        tx_provider.register_precommit_handler({
            move |mut current: CurrentTx<'_>| {
                // Do I/O in the pre-commit phase. If any I/O error occurred,
                // the TX would be aborted.
                Self::update_dirty_log_metas(&mut current)
            }
        });

        // Commit handler for journal
        let journal = journal.clone();
        tx_provider.register_commit_handler({
            move |current: CurrentTx<'_>| {
                let mut journal = journal.lock();
                current.data_with(|chunk_edit: &ChunkAllocEdit| {
                    journal.add(AllEdit::from_chunk_edit(chunk_edit));
                });
                current.data_with(|raw_log_edit: &RawLogStoreEdit| {
                    journal.add(AllEdit::from_raw_log_edit(raw_log_edit));
                });
                current.data_with(|tx_log_edit: &TxLogStoreEdit| {
                    journal.add(AllEdit::from_tx_log_edit(tx_log_edit));
                });
                journal.commit();
                // TODO: Decide when to call `flush()` to ensure journal's persistence
            }
        });

        // Commit handler for log store
        tx_provider.register_commit_handler({
            let state = new_self.state.clone();
            let raw_log_store = new_self.raw_log_store.clone();
            move |mut current: CurrentTx<'_>| {
                Self::do_lazy_deletion(&state, &current);

                current.data_with(|store_edit: &TxLogStoreEdit| {
                    let mut state = state.lock();
                    state.apply(&store_edit);

                    // Add lazy delete for newly created logs
                    for &log_id in store_edit.iter_created_logs() {
                        if state.lazy_deletes.contains_key(&log_id) {
                            continue;
                        }
                        Self::add_lazy_delete(log_id, &mut state.lazy_deletes, &raw_log_store);
                    }
                });

                Self::apply_log_caches(&state, &mut current);
            }
        });

        new_self
    }

    fn update_dirty_log_metas(current_tx: &mut CurrentTx<'_>) -> Result<()> {
        let dirty_logs: Vec<(TxLogId, Arc<TxLogInner<D>>)> =
            current_tx.data_with(|open_log_table: &OpenLogTable<D>| {
                open_log_table
                    .open_table
                    .iter()
                    .filter_map(|(id, inner_log)| {
                        if inner_log.is_dirty.load(Ordering::Relaxed) {
                            Some((*id, inner_log.clone()))
                        } else {
                            None
                        }
                    })
                    .collect()
            });

        for (log_id, inner_log) in dirty_logs {
            let crypto_log = &inner_log.crypto_log;
            crypto_log.flush()?;

            current_tx.data_mut_with(|store_edit: &mut TxLogStoreEdit| {
                store_edit.update_log_meta((log_id, crypto_log.root_meta().unwrap()))
            });
        }
        Ok(())
    }

    fn add_lazy_delete(
        log_id: TxLogId,
        delete_table: &mut BTreeMap<TxLogId, Arc<LazyDelete<TxLogId>>>,
        raw_log_store: &Arc<RawLogStore<D>>,
    ) {
        let raw_log_store = raw_log_store.clone();
        delete_table.insert(
            log_id,
            Arc::new(LazyDelete::new(log_id, move |log_id| {
                raw_log_store.delete_log(*log_id).unwrap();
            })),
        );
    }

    fn do_lazy_deletion(state: &Arc<Mutex<State>>, current_tx: &CurrentTx<'_>) {
        let deleted_logs = current_tx.data_with(|edit: &TxLogStoreEdit| {
            edit.iter_deleted_logs().cloned().collect::<Vec<_>>()
        });

        let mut state = state.lock();
        for deleted_log_id in deleted_logs {
            let Some(lazy_delete) = state.lazy_deletes.remove(&deleted_log_id) else {
                // Other concurrent TXs have deleted the same log
                continue;
            };
            LazyDelete::delete(&lazy_delete);

            // Also remove the cache by the way
            state.log_caches.remove(&deleted_log_id);
        }
    }

    // TODO: Need performance improvement
    fn apply_log_caches(state: &Arc<Mutex<State>>, current_tx: &mut CurrentTx<'_>) {
        // Apply per-TX log cache
        current_tx.data_mut_with(|open_cache_table: &mut OpenLogCache| {
            let mut state = state.lock();
            let log_caches = &mut state.log_caches;
            for (log_id, open_cache) in open_cache_table.open_table.iter_mut() {
                let log_cache = log_caches.get_mut(log_id).unwrap();
                let mut cache_inner = log_cache.inner.write();
                if cache_inner.lru_cache.is_empty() {
                    core::mem::swap(&mut cache_inner.lru_cache, &mut open_cache.lru_cache);
                    return;
                }

                open_cache.lru_cache.iter().for_each(|(&pos, node)| {
                    cache_inner.lru_cache.put(pos, node.clone());
                });
            }
        });
    }

    /// Lists the IDs of all logs in a bucket.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn list_logs(&self, bucket_name: &str) -> Result<Vec<TxLogId>> {
        let state = self.state.lock();
        let mut log_id_set = state.persistent.list_logs(bucket_name)?;
        let current_tx = self.tx_provider.current();
        current_tx.data_with(|store_edit: &TxLogStoreEdit| {
            for (&log_id, log_edit) in &store_edit.edit_table {
                match log_edit {
                    TxLogEdit::Create(_) => {
                        log_id_set.insert(log_id);
                    }
                    TxLogEdit::Append(_) => {}
                    TxLogEdit::Delete => {
                        log_id_set.remove(&log_id);
                    }
                }
            }
        });
        let log_id_vec = log_id_set.into_iter().collect::<Vec<_>>();
        Ok(log_id_vec)
    }

    /// Creates a new, empty log in a bucket.
    ///
    /// On success, the returned `TxLog` is opened in the appendable mode.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn create_log(&self, bucket: &str) -> Result<Arc<TxLog<D>>> {
        let raw_log = self.raw_log_store.create_log()?;
        let log_id = raw_log.id();

        let log_cache = Arc::new(CryptoLogCache::new(log_id, &self.tx_provider));
        self.state
            .lock()
            .log_caches
            .insert(log_id, log_cache.clone());
        let crypto_log = CryptoLog::new(raw_log, self.key, log_cache);

        let mut current_tx = self.tx_provider.current();
        let bucket = bucket.to_string();
        let inner_log = Arc::new(TxLogInner {
            log_id,
            tx_id: current_tx.id(),
            bucket: bucket.clone(),
            crypto_log,
            lazy_delete: None,
            is_dirty: AtomicBool::new(false),
        });

        current_tx.data_mut_with(|store_edit: &mut TxLogStoreEdit| {
            store_edit.create_log(log_id, bucket, self.key);
        });

        current_tx.data_mut_with(|open_log_table: &mut OpenLogTable<D>| {
            let _ = open_log_table.open_table.insert(log_id, inner_log.clone());
        });

        current_tx.data_mut_with(|open_cache_table: &mut OpenLogCache| {
            let _ = open_cache_table
                .open_table
                .insert(log_id, CacheInner::new());
        });

        Ok(Arc::new(TxLog {
            inner_log,
            tx_provider: self.tx_provider.clone(),
            can_append: true,
        }))
    }

    /// Opens the log of a given ID.
    ///
    /// For any log at any time, there can be at most one TX that opens the log
    /// in the appendable mode.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn open_log(&self, log_id: TxLogId, can_append: bool) -> Result<Arc<TxLog<D>>> {
        let mut current_tx = self.tx_provider.current();
        let inner_log = self.open_inner_log(log_id, can_append, &mut current_tx)?;
        let tx_log = TxLog::new(inner_log, self.tx_provider.clone(), can_append);
        Ok(Arc::new(tx_log))
    }

    fn open_inner_log(
        &self,
        log_id: TxLogId,
        can_append: bool,
        current_tx: &mut CurrentTx<'_>,
    ) -> Result<Arc<TxLogInner<D>>> {
        // Fast path: the log has been opened in this TX
        let opened_log_opt = current_tx.data_with(|open_log_table: &OpenLogTable<D>| {
            open_log_table.open_table.get(&log_id).cloned()
        });
        if let Some(inner_log) = opened_log_opt {
            return Ok(inner_log);
        }

        // Slow path: the first time a log is to be opened in a TX
        let state = self.state.lock();
        // Must check lazy deletes first in case concurrent deletion
        let lazy_delete = state
            .lazy_deletes
            .get(&log_id)
            .ok_or(Error::with_msg(NotFound, "log has been deleted"))?
            .clone();
        let log_entry = {
            // The log must exist in state...
            let log_entry: &TxLogEntry = state.persistent.find_log(log_id)?;
            // ...and not be marked deleted by edit
            let is_deleted = current_tx
                .data_with(|store_edit: &TxLogStoreEdit| store_edit.is_log_deleted(log_id));
            if is_deleted {
                return_errno_with_msg!(NotFound, "log has been marked deleted");
            }
            log_entry
        };

        let bucket = log_entry.bucket.clone();
        let crypto_log = {
            let raw_log = self.raw_log_store.open_log(log_id, can_append)?;
            let key = log_entry.key;
            let root_meta = log_entry.root_mht;
            let cache = state.log_caches.get(&log_id).unwrap().clone();
            CryptoLog::open(raw_log, key, root_meta, cache)?
        };

        let root_mht = crypto_log.root_meta().unwrap();
        let inner_log = Arc::new(TxLogInner {
            log_id,
            tx_id: current_tx.id(),
            bucket,
            crypto_log,
            lazy_delete: Some(lazy_delete),
            is_dirty: AtomicBool::new(false),
        });

        current_tx.data_mut_with(|open_log_table: &mut OpenLogTable<D>| {
            open_log_table.open_table.insert(log_id, inner_log.clone());
        });

        current_tx.data_mut_with(|open_cache_table: &mut OpenLogCache| {
            open_cache_table
                .open_table
                .insert(log_id, CacheInner::new());
        });

        if can_append {
            current_tx.data_mut_with(|store_edit: &mut TxLogStoreEdit| {
                store_edit.append_log(log_id, root_mht);
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
        let max_log_id = log_ids
            .iter()
            .max()
            .ok_or(Error::with_msg(NotFound, "tx log not found"))?;
        self.open_log(*max_log_id, false)
    }

    /// Checks whether the log of a given log ID exists or not.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn contains_log(&self, log_id: TxLogId) -> bool {
        let state = self.state.lock();
        let current_tx = self.tx_provider.current();
        self.do_contain_log(log_id, &state, &current_tx)
    }

    fn do_contain_log(&self, log_id: TxLogId, state: &State, current_tx: &CurrentTx<'_>) -> bool {
        if state.persistent.contains_log(log_id) {
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

        current_tx.data_mut_with(|open_log_table: &mut OpenLogTable<D>| {
            let _ = open_log_table.open_table.remove(&log_id);
        });

        current_tx.data_mut_with(|open_cache_table: &mut OpenLogCache| {
            let _ = open_cache_table.open_table.remove(&log_id);
        });

        if !self.do_contain_log(log_id, &self.state.lock(), &current_tx) {
            return_errno_with_msg!(NotFound, "target deleted log not found");
        }

        current_tx.data_mut_with(|store_edit: &mut TxLogStoreEdit| {
            store_edit.delete_log(log_id);
        });

        // Do lazy delete in precommit phase
        Ok(())
    }

    /// Returns the root key.
    pub fn key(&self) -> &Key {
        &self.key
    }

    /// Creates a new transaction.
    pub fn new_tx(&self) -> Tx {
        self.tx_provider.new_tx()
    }

    /// Returns the current transaction.
    pub fn current_tx(&self) -> CurrentTx<'_> {
        self.tx_provider.current()
    }
}

impl Superblock {
    const SUPERBLOCK_SIZE: usize = core::mem::size_of::<Superblock>();

    /// Returns the total number of blocks occupied by the `TxLogStore`.
    pub fn total_nblocks(&self) -> usize {
        self.journal_area_meta.total_nblocks() + self.chunk_area_nblocks
    }

    pub fn open<D: BlockSet>(disk: &D, root_key: &Key) -> Result<Self> {
        let mut cipher = Buf::alloc(1)?;
        disk.read(0, cipher.as_mut())?;
        let mut plain = Buf::alloc(1)?;
        Skcipher::new().decrypt(
            cipher.as_slice(),
            &Self::derive_skcipher_key(root_key),
            &SkcipherIv::new_zeroed(),
            plain.as_mut_slice(),
        )?;
        Ok(Superblock::from_bytes(
            &plain.as_slice()[..Self::SUPERBLOCK_SIZE],
        ))
    }

    fn persist<D: BlockSet>(&self, disk: &D, root_key: &Key) -> Result<()> {
        let mut plain = Buf::alloc(1)?;
        plain.as_mut_slice()[..Self::SUPERBLOCK_SIZE].copy_from_slice(self.as_bytes());
        let mut cipher = Buf::alloc(1)?;
        Skcipher::new().encrypt(
            plain.as_slice(),
            &Self::derive_skcipher_key(&root_key),
            &SkcipherIv::new_zeroed(),
            cipher.as_mut_slice(),
        )?;
        disk.write(0, cipher.as_ref())
    }

    fn derive_skcipher_key(root_key: &Key) -> SkcipherKey {
        SkcipherKey::from_bytes(&root_key.as_bytes())
    }
}

/// A transactional log.
#[derive(Clone)]
pub struct TxLog<D> {
    inner_log: Arc<TxLogInner<D>>,
    tx_provider: Arc<TxProvider>,
    can_append: bool,
}

struct TxLogInner<D> {
    log_id: TxLogId,
    tx_id: TxId,
    bucket: BucketName,
    crypto_log: CryptoLog<RawLog<D>>,
    lazy_delete: Option<Arc<LazyDelete<TxLogId>>>,
    is_dirty: AtomicBool,
}

impl<D: BlockSet + 'static> TxLog<D> {
    fn new(inner_log: Arc<TxLogInner<D>>, tx_provider: Arc<TxProvider>, can_append: bool) -> Self {
        Self {
            inner_log,
            tx_provider,
            can_append,
        }
    }

    /// Returns the log ID.
    pub fn id(&self) -> TxLogId {
        self.inner_log.log_id
    }

    /// Returns the TX ID.
    pub fn tx_id(&self) -> TxId {
        self.inner_log.tx_id
    }

    /// Returns the bucket that this log belongs to.
    pub fn bucket(&self) -> &str {
        &self.inner_log.bucket
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
    pub fn read(&self, pos: BlockId, buf: BufMut) -> Result<()> {
        debug_assert_eq!(self.tx_id(), self.tx_provider.current().id());

        self.inner_log.crypto_log.read(pos, buf)
    }

    /// Appends one or multiple data blocks at the end.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn append(&self, buf: BufRef) -> Result<()> {
        debug_assert_eq!(self.tx_id(), self.tx_provider.current().id());

        if !self.can_append {
            return_errno_with_msg!(PermissionDenied, "tx log not in append mode");
        }

        self.inner_log.is_dirty.store(true, Ordering::Release);
        self.inner_log.crypto_log.append(buf)
    }

    /// Returns the length of the log in unit of block.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn nblocks(&self) -> usize {
        debug_assert_eq!(self.tx_id(), self.tx_provider.current().id());

        self.inner_log.crypto_log.nblocks()
    }
}

pub struct CryptoLogCache {
    inner: RwLock<CacheInner>,
    log_id: TxLogId,
    tx_provider: Arc<TxProvider>,
}

pub(super) struct CacheInner {
    pub lru_cache: LruCache<BlockId, Arc<dyn Any + Send + Sync>>,
}
// TODO: Give the cache a bound

impl CryptoLogCache {
    fn new(log_id: TxLogId, tx_provider: &Arc<TxProvider>) -> Self {
        Self {
            inner: RwLock::new(CacheInner::new()),
            log_id,
            tx_provider: tx_provider.clone(),
        }
    }
}

impl NodeCache for CryptoLogCache {
    fn get(&self, pos: BlockId) -> Option<Arc<dyn Any + Send + Sync>> {
        let mut current = self.tx_provider.current();
        current.data_mut_with(|open_cache_table: &mut OpenLogCache| {
            let open_cache = open_cache_table.open_table.get_mut(&self.log_id)?;
            open_cache.lru_cache.get(&pos).cloned()
        })?;

        let mut inner = self.inner.write();
        inner.lru_cache.get(&pos).cloned()
    }

    fn put(
        &self,
        pos: BlockId,
        value: Arc<dyn Any + Send + Sync>,
    ) -> Option<Arc<dyn Any + Send + Sync>> {
        let mut current = self.tx_provider.current();
        current.data_mut_with(|open_cache_table: &mut OpenLogCache| {
            let open_cache = open_cache_table.open_table.get_mut(&self.log_id)?;
            open_cache.lru_cache.put(pos, value)
        })
    }
}

impl CacheInner {
    pub fn new() -> Self {
        Self {
            lru_cache: LruCache::unbounded(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Persistent State
////////////////////////////////////////////////////////////////////////////////

/// The volatile and persistent state of a `TxLogStore`.
struct State {
    persistent: TxLogStoreState,
    lazy_deletes: BTreeMap<TxLogId, Arc<LazyDelete<TxLogId>>>,
    log_caches: BTreeMap<TxLogId, Arc<CryptoLogCache>>,
}

/// The persistent state of a `TxLogStore`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TxLogStoreState {
    log_table: BTreeMap<TxLogId, TxLogEntry>,
    bucket_table: BTreeMap<BucketName, Bucket>,
}

/// A log entry implies the persistent state of the tx log.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TxLogEntry {
    pub bucket: BucketName,
    pub key: Key,
    pub root_mht: RootMhtMeta,
}

/// A bucket contains a set of logs which have the same name.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Bucket {
    log_ids: BTreeSet<TxLogId>,
}

impl State {
    pub fn new(
        persistent: TxLogStoreState,
        lazy_deletes: BTreeMap<TxLogId, Arc<LazyDelete<TxLogId>>>,
        log_caches: BTreeMap<TxLogId, Arc<CryptoLogCache>>,
    ) -> Self {
        Self {
            persistent,
            lazy_deletes,
            log_caches,
        }
    }

    pub fn apply(&mut self, edit: &TxLogStoreEdit) {
        edit.apply_to(&mut self.persistent);
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
                bucket: bucket.clone(),
                key,
                root_mht,
            },
        );
        debug_assert!(already_exists.is_none());

        match self.bucket_table.get_mut(&bucket) {
            Some(bucket) => {
                bucket.log_ids.insert(new_log_id);
            }
            None => {
                self.bucket_table.insert(
                    bucket,
                    Bucket {
                        log_ids: BTreeSet::from([new_log_id]),
                    },
                );
            }
        }
    }

    pub fn append_log(&mut self, log_id: TxLogId, root_mht: RootMhtMeta) {
        let entry = self.log_table.get_mut(&log_id).unwrap();
        entry.root_mht = root_mht;
    }

    pub fn list_logs(&self, bucket_name: &str) -> Result<BTreeSet<TxLogId>> {
        let bucket = self
            .bucket_table
            .get(&bucket_name.to_string())
            .ok_or(Error::with_msg(NotFound, "bucket not found"))?;
        Ok(bucket.log_ids.clone())
    }

    pub fn list_all_logs(&self) -> impl Iterator<Item = TxLogId> + '_ {
        self.log_table.iter().map(|(id, _)| *id)
    }

    pub fn find_log(&self, log_id: TxLogId) -> Result<&TxLogEntry> {
        self.log_table
            .get(&log_id)
            .ok_or(Error::with_msg(NotFound, "log entry not found"))
    }

    pub fn contains_log(&self, log_id: TxLogId) -> bool {
        self.log_table.contains_key(&log_id)
    }

    pub fn delete_log(&mut self, log_id: TxLogId) {
        // Do not check the result because concurrent TXs
        // may decide to delete the same logs
        let entry_opt = self.log_table.remove(&log_id);
        entry_opt.map(|entry| {
            self.bucket_table
                .get_mut(&entry.bucket)
                .map(|bucket| bucket.log_ids.remove(&log_id));
        });
    }
}

////////////////////////////////////////////////////////////////////////////////
// Persistent Edit
////////////////////////////////////////////////////////////////////////////////

/// A persistent edit to the state of `TxLogStore`.
#[derive(Clone, Serialize, Deserialize)]
pub struct TxLogStoreEdit {
    edit_table: BTreeMap<TxLogId, TxLogEdit>,
}

/// Used for per-TX data, track open logs in memory
pub(super) struct OpenLogTable<D> {
    open_table: BTreeMap<TxLogId, Arc<TxLogInner<D>>>,
}

/// Used for per-TX data, track open log caches in memory
pub(super) struct OpenLogCache {
    open_table: BTreeMap<TxLogId, CacheInner>,
}

/// The basic unit of a persistent edit to the state of `TxLogStore`.
#[derive(Clone, Serialize, Deserialize)]
pub(super) enum TxLogEdit {
    Create(TxLogCreate),
    Append(TxLogAppend),
    Delete,
}

/// An edit that implies a log being created.
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct TxLogCreate {
    bucket: BucketName,
    key: Key,
    root_mht: Option<RootMhtMeta>,
}

/// An edit that implies an existing log being appended.
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct TxLogAppend {
    root_mht: RootMhtMeta,
}

impl TxLogStoreEdit {
    pub fn new() -> Self {
        Self {
            edit_table: BTreeMap::new(),
        }
    }

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

    pub fn delete_log(&mut self, log_id: TxLogId) {
        match self.edit_table.get_mut(&log_id) {
            None => {
                let _ = self.edit_table.insert(log_id, TxLogEdit::Delete);
            }
            Some(TxLogEdit::Create(_)) => {
                let _ = self.edit_table.insert(log_id, TxLogEdit::Delete);
            }
            Some(TxLogEdit::Append(_)) => {
                panic!(
                    "append edit is added at very late stage, after which logs won't get deleted"
                );
            }
            Some(TxLogEdit::Delete) => {
                panic!("can't delete a deleted log");
            }
        }
    }

    pub fn iter_deleted_logs(&self) -> impl Iterator<Item = &TxLogId> {
        self.edit_table
            .iter()
            .filter(|(_, edit)| {
                if let TxLogEdit::Delete = edit {
                    true
                } else {
                    false
                }
            })
            .map(|(id, _)| id)
    }

    pub fn iter_created_logs(&self) -> impl Iterator<Item = &TxLogId> {
        self.edit_table
            .iter()
            .filter(|(_, edit)| {
                if let TxLogEdit::Create(_) = edit {
                    true
                } else {
                    false
                }
            })
            .map(|(id, _)| id)
    }

    pub fn update_log_meta(&mut self, meta: (TxLogId, RootMhtMeta)) {
        // For newly-created logs and existing logs
        // that are appended, update `RootMhtMeta`
        match self.edit_table.get_mut(&meta.0) {
            None | Some(TxLogEdit::Delete) => {
                unreachable!();
            }
            Some(TxLogEdit::Create(create)) => {
                let _ = create.root_mht.insert(meta.1);
            }
            Some(TxLogEdit::Append(append)) => {
                append.root_mht = meta.1;
            }
        }
    }

    pub fn create_log(&mut self, log_id: TxLogId, bucket: BucketName, key: Key) {
        let already_created = self.edit_table.insert(
            log_id,
            TxLogEdit::Create(TxLogCreate {
                bucket,
                key,
                root_mht: None,
            }),
        );
        debug_assert!(already_created.is_none());
    }

    pub fn append_log(&mut self, log_id: TxLogId, root_mht: RootMhtMeta) {
        let already_existed = self
            .edit_table
            .insert(log_id, TxLogEdit::Append(TxLogAppend { root_mht }));
        debug_assert!(already_existed.is_none());
    }
}

impl Edit<TxLogStoreState> for TxLogStoreEdit {
    fn apply_to(&self, state: &mut TxLogStoreState) {
        for (&log_id, log_edit) in &self.edit_table {
            match log_edit {
                TxLogEdit::Create(create_edit) => {
                    let TxLogCreate {
                        bucket,
                        key,
                        root_mht,
                        ..
                    } = create_edit;
                    state.create_log(log_id, bucket.clone(), key.clone(), root_mht.unwrap());
                }
                TxLogEdit::Append(append_edit) => {
                    let TxLogAppend { root_mht, .. } = append_edit;
                    state.append_log(log_id, *root_mht);
                }
                TxLogEdit::Delete => {
                    state.delete_log(log_id);
                }
            }
        }
    }
}

impl TxData for TxLogStoreEdit {}

impl<D> OpenLogTable<D> {
    pub fn new() -> Self {
        Self {
            open_table: BTreeMap::new(),
        }
    }
}

impl OpenLogCache {
    pub fn new() -> Self {
        Self {
            open_table: BTreeMap::new(),
        }
    }
}

impl<D: 'static> TxData for OpenLogTable<D> {}
impl TxData for OpenLogCache {}

////////////////////////////////////////////////////////////////////////////////
// Journaling
////////////////////////////////////////////////////////////////////////////////

mod journaling {
    use super::*;
    use crate::layers::edit::EditGroup;

    pub type Journal<D> = EditJournal<AllEdit, AllState, D, JournalCompactPolicy>;
    pub type JournalCompactPolicy = NeverCompactPolicy;

    #[derive(Clone, Serialize, Deserialize)]
    pub struct AllState {
        pub chunk_alloc: ChunkAllocState,
        pub raw_log_store: RawLogStoreState,
        pub tx_log_store: TxLogStoreState,
    }

    #[derive(Serialize, Deserialize)]
    pub struct AllEdit {
        pub chunk_edit: ChunkAllocEdit,
        pub raw_log_edit: RawLogStoreEdit,
        pub tx_log_edit: TxLogStoreEdit,
    }

    impl Edit<AllState> for AllEdit {
        fn apply_to(&self, state: &mut AllState) {
            self.chunk_edit.apply_to(&mut state.chunk_alloc);
            self.raw_log_edit.apply_to(&mut state.raw_log_store);
            self.tx_log_edit.apply_to(&mut state.tx_log_store);
        }
    }

    impl AllEdit {
        pub fn from_chunk_edit(chunk_edit: &ChunkAllocEdit) -> Self {
            Self {
                chunk_edit: chunk_edit.clone(),
                raw_log_edit: RawLogStoreEdit::new(),
                tx_log_edit: TxLogStoreEdit::new(),
            }
        }

        pub fn from_raw_log_edit(raw_log_edit: &RawLogStoreEdit) -> Self {
            Self {
                chunk_edit: ChunkAllocEdit::new(),
                raw_log_edit: raw_log_edit.clone(),
                tx_log_edit: TxLogStoreEdit::new(),
            }
        }

        pub fn from_tx_log_edit(tx_log_edit: &TxLogStoreEdit) -> Self {
            Self {
                chunk_edit: ChunkAllocEdit::new(),
                raw_log_edit: RawLogStoreEdit::new(),
                tx_log_edit: tx_log_edit.clone(),
            }
        }
    }

    pub struct NeverCompactPolicy;

    impl CompactPolicy<AllEdit, AllState> for JournalCompactPolicy {
        fn on_commit_edits(&mut self, _edits: &EditGroup<AllEdit, AllState>) {}

        fn should_compact(&self) -> bool {
            false
        }

        fn done_compact(&mut self) {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layers::bio::{Buf, MemDisk};

    use std::thread::{self, JoinHandle};

    #[test]
    fn tx_log_store_fns() -> Result<()> {
        let nblocks = 4 * CHUNK_NBLOCKS;
        let mem_disk = MemDisk::create(nblocks)?;
        let disk = mem_disk.clone();
        let tx_log_store = TxLogStore::format(mem_disk)?;
        let bucket = "TEST";
        let content = 5_u8;

        // TX 1: create a new log and append contents (committed)
        let mut tx = tx_log_store.new_tx();
        let res: Result<TxLogId> = tx.context(|| {
            let new_log = tx_log_store.create_log(bucket)?;
            let log_id = new_log.id();
            assert_eq!(log_id, 0);
            assert_eq!(new_log.tx_id(), tx_log_store.current_tx().id());
            assert_eq!(new_log.can_append(), true);
            let mut buf = Buf::alloc(1)?;
            buf.as_mut_slice().fill(content);
            new_log.append(buf.as_ref())?;

            assert_eq!(new_log.nblocks(), 1);
            assert_eq!(new_log.bucket(), bucket);
            Ok(log_id)
        });
        let log_id = res?;
        tx.commit()?;

        // TX 2: open the log then read (committed)
        let mut tx = tx_log_store.new_tx();
        let res: Result<_> = tx.context(|| {
            let log_list = tx_log_store.list_logs(bucket)?;
            assert_eq!(log_list, vec![log_id]);
            assert_eq!(tx_log_store.contains_log(log_id), true);
            assert_eq!(tx_log_store.contains_log(1), false);

            let log = tx_log_store.open_log(0, false)?;
            assert_eq!(log.id(), log_id);
            assert_eq!(log.tx_id(), tx_log_store.current_tx().id());
            let mut buf = Buf::alloc(1)?;
            log.read(0, buf.as_mut())?;
            assert_eq!(buf.as_slice()[0], content);

            let log = tx_log_store.open_log_in(bucket)?;
            assert_eq!(log.id(), log_id);
            log.read(0 as BlockId, buf.as_mut())?;
            assert_eq!(buf.as_slice()[0], content);
            Ok(())
        });
        res?;
        tx.commit()?;

        // Recover the tx log store
        let key = tx_log_store.key().clone();
        let _ = tx_log_store.journal.lock().flush();
        drop(tx_log_store);
        let recovered_store = TxLogStore::recover(disk, key)?;

        // TX 3: create a new log from recovered_store (aborted)
        let tx_log_store = recovered_store.clone();
        let handler = thread::spawn(move || -> Result<TxLogId> {
            let mut tx = tx_log_store.new_tx();
            let res: Result<_> = tx.context(|| {
                let new_log = tx_log_store.create_log(bucket)?;
                assert_eq!(tx_log_store.list_logs(bucket)?.len(), 2);
                Ok(new_log.id())
            });
            tx.abort();
            res
        });
        let new_log_id = handler.join().unwrap()?;

        recovered_store
            .state
            .lock()
            .persistent
            .find_log(new_log_id)
            .expect_err("log not found");

        Ok(())
    }

    #[test]
    fn tx_log_deletion() -> Result<()> {
        let tx_log_store = TxLogStore::format(MemDisk::create(4 * CHUNK_NBLOCKS)?)?;

        let mut tx = tx_log_store.new_tx();
        let content = 5_u8;
        let res: Result<_> = tx.context(|| {
            let new_log = tx_log_store.create_log("TEST")?;
            let mut buf = Buf::alloc(1)?;
            buf.as_mut_slice().fill(content);
            new_log.append(buf.as_ref())?;
            Ok(new_log.id())
        });
        let log_id = res?;
        tx.commit()?;

        let handlers = (0..16)
            .map(|_| {
                let tx_log_store = tx_log_store.clone();
                thread::spawn(move || -> Result<()> {
                    let mut tx = tx_log_store.new_tx();
                    println!(
                        "TX[{:?}] executed on thread[{:?}]",
                        tx.id(),
                        crate::os::CurrentThread::id()
                    );
                    let _ = tx.context(|| {
                        let log = tx_log_store.open_log(log_id, false)?;
                        assert_eq!(log.id(), log_id);
                        assert_eq!(log.tx_id(), tx_log_store.current_tx().id());
                        let mut buf = Buf::alloc(1)?;
                        log.read(0 as BlockId, buf.as_mut())?;
                        assert_eq!(buf.as_slice(), &[content; BLOCK_SIZE]);
                        tx_log_store.delete_log(log_id)
                    });
                    tx.commit()
                })
            })
            .collect::<Vec<JoinHandle<Result<()>>>>();
        for handler in handlers {
            handler.join().unwrap()?;
        }

        let mut tx = tx_log_store.new_tx();
        let _ = tx.context(|| {
            let res = tx_log_store.open_log(log_id, false).map(|_| ());
            res.expect_err("result must be NotFound");
        });
        tx.commit()
    }
}
