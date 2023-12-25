//! Transactional LSM-tree.
//!
//! API: format(), recover(), get(), put(), get_range(), put_range(), commit(), discard()
//!
//! Responisble for managing two `MemTable`s, a `TxLogStore` to
//! manage tx logs (WAL and SSTs). Operations are executed based
//! on internal transactions.
use crate::layers::bio::{BlockBuf, BlockId};
use crate::layers::log::{TxLog, TxLogId, TxLogStore};
use crate::tx::Tx;

use serde::{Deserialize, Serialize};
use spin::{Mutex, RwLock};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Read;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::{Range, RangeBounds};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

const BUCKET_WAL: &str = "WAL";
const BUCKET_L0: &str = "L0";
const BUCKET_L1: &str = "L1";
const BLOCK_SIZE: usize = 0x1000;

static MASTER_COMMIT_ID: AtomicU64 = AtomicU64::new(0);

/// A LSM-tree built upon `TxLogStore`.
pub struct TxLsmTree<K, V, D> {
    mem_tables: [Arc<RwLock<MemTable<K, V>>>; 2],
    immut_idx: AtomicU8,
    tx_log_store: Arc<TxLogStore<D>>,
    append_tx: AppendTx<K, V, D>,
    sst_manager: RwLock<SstManager<K, V>>,
    listener_factory: Arc<dyn TxEventListenerFactory<K, V>>,
}

/// A factory of per-transaction event listeners.
pub trait TxEventListenerFactory<K, V> {
    /// Creates a new event listener for a given transaction.
    fn new_event_listerner(&self, tx_type: TxType) -> Arc<dyn TxEventListener<K, V>>;
}

/// An event listener that get informed when
/// 1. A new record is added,
/// 2. An existing record is dropped,
/// 3. After a tx begined,
/// 4. Before a tx ended,
/// 5. After a tx committed.
/// `tx_type` indicates an internal transaction of `TxLsmTree`.
pub trait TxEventListener<K, V> {
    /// Notify the listener that a new record is added to a LSM-tree.
    fn on_add_record(&self, record: &dyn AsKv<K, V>) -> Result<()>;

    /// Notify the listener that an existing record is dropped from a LSM-tree.
    fn on_drop_record(&self, record: &dyn AsKv<K, V>) -> Result<()>;

    /// Notify the listener after a tx just begined.
    fn on_tx_at_beginning(&self, tx: &Tx) -> Result<()>;

    /// Notify the listener before a tx ended.
    fn on_tx_near_end(&self, tx: &Tx) -> Result<()>;

    /// Notify the listener after a tx comitted.
    fn on_tx_commit(&self);
}

/// Types of `TxLsmTree`'s internal transactions.
#[derive(Copy, Clone, Debug)]
pub enum TxType {
    /// Operations in memtable, not a tx strictly (ACI)
    // MemTable, // deprecated for now
    /// A Read Transaction reads record from Lsm-tree
    // Read, // deprecated for now
    /// An Append Transaction writes records to WAL.
    // Append, // deprecated for now
    /// A Compaction Transaction merges old SSTables into new ones.
    Compaction { to_level: LsmLevel },
    /// A Migration Transaction migrates committed records from old SSTables
    /// (WAL) to new ones during recovery.
    Migration,
}

/// Levels in Lsm-tree.
pub enum LsmLevel {
    L0 = 0,
    L1,
    // TODO: Support variable levels
}

impl<K, V, D> TxLsmTree<K, V, D> {
    const MEMTABLE_CAPACITY: usize = 131072;

    /// Format a `TxLsmTree` from a given `TxLogStore`.
    pub fn format(
        tx_log_store: Arc<TxLogStore<D>>,
        listener_factory: Arc<dyn TxEventListenerFactory<K, V>>,
        on_drop_record_in_memtable: Option<Box<dyn Fn(&dyn AsKv<K, V>)>>,
    ) -> Result<Self> {
        let append_tx = {
            let tx = AppendTx::new();
            tx.prepare(&tx_log_store)?;
            tx
        };

        Ok(Self {
            mem_tables: [Arc::new(RwLock::new(MemTable::new(
                Self::MEMTABLE_CAPACITY,
                on_drop_record_in_memtable,
            ))); 2],
            immut_idx: AtomicU8::new(1),
            tx_log_store,
            append_tx,
            sst_manager: SstManager::new(),
            listener_factory,
        })
    }

    /// Recover a `TxLsmTree` from a given `TxLogStore`.
    pub fn recover(
        tx_log_store: Arc<TxLogStore<D>>,
        listener_factory: Arc<dyn TxEventListenerFactory<K, V>>,
        on_drop_record_in_memtable: Option<Box<dyn Fn(&dyn AsKv<K, V>)>>,
    ) -> Result<Self> {
        // Only committed records count, all uncommitted are discarded
        let committed_records = {
            let tx = tx_log_store.new_tx();
            let res = tx.context(|| {
                let wal = tx_log_store.open_log_in(BUCKET_WAL)?;
                let latest_commit_id: u64 = wal.find_latest_commit_id()?;
                wal.collect_committed_records()
            });
            if res.is_ok() {
                tx.commit()?;
                // TODO: Update master commit id if mismatch
            } else {
                tx.abort();
                return Err(());
            }
            res.unwrap()
        };
        let mem_tables = [Arc::new(RwLock::new(MemTable::new(
            Self::MEMTABLE_CAPACITY,
            on_drop_record_in_memtable,
        ))); 2];
        for record in committed_records {
            mem_tables[0].write().put(record.key(), record.value());
        }

        // Prepare SST manager (load index block to cache)
        let sst_manager = {
            let mut manager = SstManager::new();
            let tx = tx_log_store.new_tx();
            let res = tx.context(|| {
                let l0_log = tx_log_store.open_log_in(BUCKET_L0)?;
                manager.put(SSTable::from_log(&l0_log)?, LsmLevel::L0);
                let l1_log_ids = tx_log_store.list_logs(BUCKET_L1)?;
                for id in l1_log_ids {
                    let l1_log = tx_log_store.open_log(id, false)?;
                    manager.put(SSTable::from_log(&l1_log)?, LsmLevel::L1);
                }
            });
            RwLock::new(manager)
        };

        let recov_self = Self {
            mem_tables: [MemTable::new(Self::MEMTABLE_CAPACITY, on_drop_record_in_memtable); 2],
            immut_idx: AtomicU8::new(1),
            tx_log_store,
            append_tx: AppendTx::new(),
            sst_manager,
            listener_factory,
        };

        recov_self.do_migration_tx()?;
        Ok(recov_self)
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        // 1. Get from MemTables
        if let Some(value) = self.active_mem_table().read().get(key) {
            return Some(value);
        }
        if let Some(value) = self.immut_mem_table().read().get(key) {
            return Some(value);
        }

        // 2. Get from SSTs (do Read Tx)
        self.do_read_tx(key).ok()
    }

    pub fn get_range(&self, range: &Range<K>) -> Range<&dyn AsKv<K, V>> {
        todo!()
    }

    pub fn put(&self, key: K, value: V) -> Option<V> {
        let tx_log_store = self.tx_log_store;
        let record = (key, value);
        // 1. Write WAL
        self.append_tx.append(&record)?;

        // 2. Put into MemTable
        let to_be_replaced = self.active_mem_table().write().put(key, value);
        if !self.active_mem_table().read().at_capacity() {
            return to_be_replaced;
        }

        // 3. Trigger compaction if needed
        self.immut_idx.fetch_xor(1, Ordering::Relaxed);
        // Do major compaction first if needed
        if self.sst_manager.read().get(LsmLevel::L0).len() > 0 {
            self.do_compaction_tx(LsmLevel::L1)?;
        }
        self.do_compaction_tx(LsmLevel::L0)?;
        to_be_replaced
    }

    pub fn put_range(&self, range: &Range<K>) -> Result<()> {
        todo!()
    }

    pub fn discard(&self, keys: &[K]) -> Result<()> {
        todo!()
    }

    /// User-called flush
    pub fn commit(&self) -> Result<()> {
        self.append_tx.commit()
    }

    /// Txs in TxLsmTree

    /// Read Tx
    fn do_read_tx(&self, key: &K) -> Result<&V> {
        // Search in cache
        let tx = self.tx_log_store.new_tx();

        let read_res = tx.context(|| {
            // Search L0
            let l0_sst = self.sst_manager.read().get(LsmLevel::L0)[0];
            let res = if let Some(pos) = l0_sst.search_in_cache(key) {
                let l0_log: TxLog = self.tx_log_store.open_log(l0_sst.id())?;
                debug_assert!(l0_log.bucket() == BUCKET_L0);
                l0_sst.search_in_log(key, pos, &l0_log)
            };

            if res.is_err() {
                // Search L1
                let l1_ssts = self.sst_manager.read().get(LsmLevel::L1);
                for l1_sst in l1_ssts {
                    if let Some(pos) = l0_sst.search_in_cache(key) {
                        let l1_log: TxLog = self.tx_log_store.open_log(l1_sst.id())?;
                        debug_assert!(l1_log.bucket() == BUCKET_L1);
                        return l0_sst.search_in_log(key, pos, &l1_log);
                        break;
                    }
                }
            } else {
                res
            }
        });
        if read_res.is_err() {
            tx.abort();
            return Err(());
        }

        tx.commit()?;
        read_res
    }

    /// Append Tx
    fn do_append_tx(&self, record: &dyn AsKv<K, V>) -> Result<()> {
        self.append_tx.append(record)
    }

    /// Compaction Tx
    fn do_compaction_tx(&self, to_level: LsmLevel) -> Result<()> {
        match to_level {
            LsmLevel::L0 => self.do_minor_compaction(),
            _ => self.do_major_compaction(to_level),
        }
    }

    /// Compaction Tx { to_level: LsmLevel::L0 }
    fn do_minor_compaction(&self) -> Result<()> {
        let tx_log_store = self.tx_log_store.new_tx();
        let tx = tx_log_store.new_tx();
        let tx_type = TxType::Compaction {
            to_level: LsmLevel::L0,
        };
        let event_listener = self.listener_factory.new_event_listerner(tx_type);
        let res = event_listener.on_tx_at_beginning(&tx);
        if res.is_err() {
            tx.abort();
            return Err(());
        }

        let res = tx.context(move || {
            let tx_log = tx_log_store.create_log(BUCKET_L0)?;

            let records = self.immut_mem_table().read().keys_values();
            for record in records {
                event_listener.on_add_record(record)?;
            }
            let sst = SSTable::build(records, tx_log)?;
            self.sst_manager.write().put(sst, LsmLevel::L0);
        });
        if res.is_err() {
            tx.abort();
            return Err(());
        }

        let res = event_listener.on_tx_near_end(&tx);
        if res.is_err() {
            tx.abort();
            return Err(());
        }

        if res.is_ok() {
            tx.commit().ok()?;
            event_listener.on_tx_commit();
            self.immut_mem_table().write().clear();
            // Discard current WAL
            self.append_tx.reset();
        } else {
            tx.abort();
        }
        Ok(())
    }

    /// Compaction Tx { to_level: LsmLevel::L1 }
    fn do_major_compaction(&self, to_level: LsmLevel) -> Result<()> {
        let tx_log_store = self.tx_log_store;
        let tx = tx_log_store.new_tx();
        let tx_type = TxType::Compaction {
            to_level: LsmLevel::L1,
        };
        let event_listener = self.listener_factory.new_event_listerner(tx_type);
        let res = event_listener.on_tx_at_beginning(&tx);
        if res.is_err() {
            tx.abort();
            return Err(());
        }

        let res = tx.context(move || {
            // Collect overlapped SSTs
            let sst_manager = self.sst_manager.read();
            let l0_sst = sst_manager.get(LsmLevel::L0)[0];
            let l0_range = l0_sst.range();
            let l1_ssts = sst_manager.get(LsmLevel::L1);
            let overlapped_l1_ssts = l1_ssts
                .iter()
                .filter_map(|sst| sst.range().overlap_with(l0_range))
                .collect();
            drop(sst_manager);

            // Collect records during compaction
            let l0_log = tx_log_store.open_log(l0_sst.id())?;
            let compacted_records: BTreeMap<K, V> = l0_sst.collect_all_records(l0_log) as _;
            let mut dropped_records = vec![];
            for l1_sst in overlapped_l1_ssts {
                let l1_log = tx_log_store.open_log(l1_sst.id())?;
                let records = l1_sst.collect_all_records(l1_log)?;
                for record in records {
                    if compacted_records.contains_key(record.key()) {
                        event_listener.on_drop_record(record);
                        dropped_records.push(record.clone());
                    } else {
                        compacted_records.insert(record.key(), record.value());
                    }
                }
            }

            let (mut created_ssts, mut deleted_ssts) = (vec![], vec![]);
            // Create new SSTs
            for records in compacted_records.chunks(SST_MAX_RECORD_CAP) {
                let new_log = tx_log_store.create_log(BUCKET_L1)?;
                let new_sst = SSTable::build(records, new_log)?;
                created_ssts.push((new_sst, LsmLevel::L1));
            }

            // Delete old SSTs
            tx_log_store.delete_log(l0_sst.id())?;
            deleted_ssts.push((l0_sst, LsmLevel::L0));
            for l1_sst in overlapped_l1_ssts {
                tx_log_store.delete_log(l1_sst.id())?;
                deleted_ssts.push((l1_sst.id(), LsmLevel::L1));
            }
            (created_ssts, deleted_ssts)
        });

        let res = event_listener.on_tx_near_end(&tx);
        if res.is_err() {
            tx.abort();
            return Err(());
        }

        if res.is_ok() {
            tx.commit().ok()?;
            event_listener.on_tx_commit();
            let (created_ssts, deleted_ssts) = res.unwrap();
            // Update SST cache
            let mut sst_manager = self.sst_manager.write();
            created_ssts.for_each(|(sst, level)| sst_manager.put(sst, level));
            deleted_ssts.for_each(|(id, level)| sst_manager.remove(id, level));
            Ok(())
        } else {
            tx.abort();
            Err(())
        }
    }

    /// Migration Tx
    fn do_migration_tx(&self) -> Result<()> {
        // Discard all uncommitted records in SSTs
        let tx_log_store = self.tx_log_store;
        let tx = tx_log_store.new_tx();
        let tx_type = TxType::Migration;
        let event_listener = self.listener_factory.new_event_listerner(tx_type);
        let res = event_listener.on_tx_at_beginning(&tx);
        if res.is_err() {
            tx.abort();
            return Err(());
        }

        let res = tx.context(move || {
            // Collect overlapped SSTs
            let sst_manager = self.sst_manager.read();
            let l0_sst = sst_manager.get(LsmLevel::L0)[0];
            drop(sst_manager);

            // Collect records
            let l0_log = tx_log_store.open_log(l0_sst.id())?;
            let mut records = l0_sst.collect_all_records(l0_log);
            records.filter(|r| {
                if r.is_uncommitted() {
                    event_listener.on_drop_record(r);
                } else {
                    false
                }
            });

            let (mut created_ssts, mut deleted_ssts) = (vec![], vec![]);
            // Create new migrated SST
            let new_log = tx_log_store.create_log(BUCKET_L0)?;
            let new_sst = SSTable::build(records, new_log)?;
            created_ssts.push((new_sst, LsmLevel::L0));

            // Delete old SST
            tx_log_store.delete_log(l0_sst.id())?;
            deleted_ssts.push((l0_sst, LsmLevel::L0));

            // TODO: Do migration in every SST, from newer to older.
            // If one SST has no uncommitted record at all, stops scanning.
            (created_ssts, deleted_ssts)
        });

        let res = event_listener.on_tx_near_end(&tx);
        if res.is_err() {
            tx.abort();
            return Err(());
        }

        if res.is_ok() {
            tx.commit().ok()?;
            event_listener.on_tx_commit();
            let (created_ssts, deleted_ssts) = res.unwrap();
            // Update SST cache
            let mut sst_manager = self.sst_manager.write();
            created_ssts.for_each(|(sst, level)| sst_manager.put(sst, level));
            deleted_ssts.for_each(|(id, level)| sst_manager.remove(id, level));
            Ok(())
        } else {
            tx.abort();
            Err(())
        }
    }

    fn active_mem_table(&self) -> &Arc<RwLock<MemTable<K, V>>> {
        &self.mem_tables[(self.immut_idx.load(Ordering::Relaxed) as usize) ^ 1]
    }

    fn immut_mem_table(&self) -> &Arc<RwLock<MemTable<K, V>>> {
        &self.mem_tables[self.immut_idx.load(Ordering::Relaxed) as usize]
    }
}

/// An append tx in `TxLsmTree`.
struct AppendTx<K, V, D> {
    // TODO: Use `mem::swap` instead of `Option`
    wal_tx_and_log: Mutex<Option<(Tx, TxLog<D>)>>, // Cache append tx and wal log
    buf: Mutex<Vec<u8>>,                           // Cache appended wal record
}

#[derive(Serialize, Deserialize)]
enum WalAppendType {
    Record = 0,
    Commit,
}

impl<K, V, D> AppendTx<K, V, D> {
    const BUF_CAP: usize = BLOCK_SIZE;

    fn new() -> Self {
        Self {
            wal_tx_and_log: Mutex::new(None),
            buf: Mutex::new(Vec::with_capacity(Self::BUF_CAP)),
        }
    }

    /// Prepare phase for Append Tx, mainly to create new tx and wal log.
    fn prepare(&self, store: &Arc<TxLogStore<D>>) -> Result<()> {
        let wal_tx = store.new_tx();
        let res = wal_tx.context(|| store.create_log(BUCKET_WAL));
        if res.is_err() {
            wal_tx.abort();
            return res;
        }
        let wal_log = res?;
        self.wal_tx_and_log.lock().insert((wal_tx, wal_log));
        Ok(())
    }

    /// Append phase for Append Tx, mainly to append newly records to wal log.
    fn append(&self, record: &dyn AsKv<K, V>) -> Result<()> {
        let mut buf = self.buf.lock();
        buf.push(WalAppendType::Record.to_bytes());
        buf.push(record.to_bytes());

        if buf.len() < Self::BUF_CAP {
            return Ok(());
        }

        // Must call prepare() first
        let (wal_tx, wal_log) = self.wal_tx_and_log.lock().unwrap();

        let res = wal_tx.context(|| wal_log.append(&self.buf.lock().as_block_buf()));
        if res.is_err() {
            wal_tx.abort();
            return res;
        }
        Ok(())
    }

    /// Commmit phase for Append Tx, mainly to commit(or abort) the tx.
    fn commit(&self) -> Result<()> {
        let (wal_tx, wal_log) = self.wal_tx_and_log.lock().unwrap();

        let mut pre_master_commit_id = MASTER_COMMIT_ID.fetch_add(1, Ordering::Relaxed);
        // TODO: Store master commit id to trusted storage
        let res = wal_tx.context(|| {
            // Append master commit id
            wal_log.append(WalAppendType::Commit.to_bytes())?;
            wal_log.append(&(pre_master_commit_id + 1).to_le_bytes())?;
            // Append cached records
            wal_log.append(&self.buf.lock().as_block_buf())
        });
        if res.is_err() {
            wal_tx.abort();
            self.wal_tx_and_log.lock().take();
            self.buf.lock().clear();
            return res;
        }

        wal_tx.commit()?;

        self.wal_tx_and_log.lock().take();
        self.buf.lock().clear();
        Ok(())
    }

    fn reset(&self) {
        self.wal_tx_and_log.lock().take();
        self.buf.lock().clear();
    }
}

/// Represent any type that includes a key and a value.
pub trait AsKv<K, V>: Serialize + Clone + Debug + Sized {
    fn key(&self) -> &K;

    fn value(&self) -> &V;
}

/// MemTable for LSM-tree.
pub struct MemTable<K, V> {
    // Use `ValueEx<V>` instead `V` to maintain multiple
    // values tagged with commit id for each key
    table: BTreeMap<K, ValueEx<V>>,
    size: usize,
    cap: usize,
    commit_id: u64,
    on_drop_record: Option<Box<dyn Fn(&dyn AsKv<K, V>)>>,
}
// TODO: Preallocate capacity at first of `MemTable`

// Value which is commit-aware
// At most one uncommitted&one committed records can coexist at the same time
enum ValueEx<V> {
    Committed(V),
    Uncommitted(V),
    CommittedAndUncommitted(V, V),
}

impl<V> ValueEx<V> {
    fn new(value: V) -> Self {
        Self::Uncommitted(value)
    }

    fn get(&self) -> &V {
        match self {
            ValueEx::Committed(v) => v,
            ValueEx::Uncommitted(v) => v,
            ValueEx::CommittedAndUncommitted(_, v) => v,
        }
    }

    fn put(&mut self, value: V) -> Option<V> {
        // TODO: Optimize this by using `mem::take`
        match self {
            ValueEx::Committed(v) => {
                *self = Self::CommittedAndUncommitted(v, value);
                None
            }
            ValueEx::Uncommitted(v) => {
                *self = Self::Uncommitted(value);
                Some(v)
            }
            ValueEx::CommittedAndUncommitted(cv, ucv) => {
                *self = Self::CommittedAndUncommitted(cv, value);
                Some(cv)
            }
        }
    }

    fn commit(&mut self) -> Option<V> {
        // TODO: Optimize this by using `mem::take`
        match self {
            ValueEx::Committed(v) => None,
            ValueEx::Uncommitted(v) => {
                *self = Self::Committed(v);
                None
            }
            ValueEx::CommittedAndUncommitted(cv, ucv) => {
                *self = Self::Committed(cv);
                Some(ucv)
            }
        }
    }
}

impl<K, V> MemTable<K, V> {
    pub fn new(cap: usize, on_drop_record: Option<Box<dyn Fn(&dyn AsKv<K, V>)>>) -> Self {
        Self {
            table: BTreeMap::new(),
            size: 0,
            cap,
            commit_id: MASTER_COMMIT_ID.load(Ordering::Relaxed),
            on_drop_record,
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let value_ex = self.table.get(key)?;
        // Return value which tagged most latest commit id
        Some(value_ex.get())
    }

    pub fn put(&mut self, key: K, value: V) -> Option<V> {
        if let Some(value_ex) = self.table.get_mut(&key) {
            if let Some(replaced) = value_ex.put(value) {
                self.on_drop_record((key, replaced))?;
                return Some(replaced);
            }
        }
        self.table.insert(key, ValueEx::new(value));
        self.size += 1;
        None
    }

    pub fn commit(&mut self) -> Result<()> {
        for (k, v_ex) in self.table {
            if let Some(replaced) = v_ex.commit() {
                self.on_drop_record((k, replaced))?;
                self.size -= 1;
            }
        }
        self.commit_id = MASTER_COMMIT_ID.load(Ordering::Relaxed);
        Ok(())
    }

    // Records should be tagged with commit id
    pub fn keys_values(&self) -> Vec<&dyn AsKv<K, V>> {
        todo!()
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn at_capacity(&self) -> bool {
        self.size == self.cap
    }

    pub fn clear(&mut self) {
        self.table.clear();
        self.size = 0;
    }
}

impl<K, V> AsKv<K, V> for (K, V) {
    fn key(&self) -> &K {
        &self.0
    }

    fn value(&self) -> &V {
        &self.1
    }
}

static SST_VERSION: AtomicU64 = AtomicU64::new(0);

/// SST manager of the `TxLsmTree`,
/// cache SST's index blocks of every level in memory.
// TODO: Support variable levels
// Issue: Should changes support abort?
struct SstManager<K, V> {
    l0: Option<SSTable<K, V>>,
    // K: Sorted tx log id (alias SST version)
    l1: BTreeMap<TxLogId, SSTable<K, V>>,
}

impl<K, V> SstManager<K, V> {
    fn new() -> Self {
        Self {
            l0: None,
            l1: BTreeMap::new(),
        }
    }

    fn get(&self, level: LsmLevel) -> Vec<&SSTable<K, V>> {
        match level {
            LsmLevel::L0 => vec![self.l0.as_ref().unwrap()],
            // From newer to older
            LsmLevel::L1 => self.l1.iter().rev().collect(),
        }
    }

    fn put(&mut self, sst: SSTable<K, V>, level: LsmLevel) {
        match level {
            LsmLevel::L0 => {
                debug_assert!(self.l0.is_none());
                self.l0.insert(sst);
            }
            LsmLevel::L1 => {
                let replaced = self.l1.insert(sst.id(), sst);
                debug_assert!(replaced.is_none());
            }
        }
    }

    fn remove(&mut self, id: &TxLogId, level: LsmLevel) {
        match level {
            LsmLevel::L0 => {
                debug_assert!(self.l0.is_some() && id == self.l0.unwrap().id());
                self.l0.take();
            }
            LsmLevel::L1 => {
                let removed = self.l1.remove(id);
                debug_assert!(removed.is_some());
            }
        }
    }
}

/// Sorted String Table (SST) for LSM-tree
///
/// format:
/// ```text
/// | records block | records block |...|     Footer                                 |
/// |   [record]    |   [record]    |...| size,key_range,[meta:(pos, range)],padding |
/// |   BLOCK_SIZE  |  BLOCK_SIZE   |...|     BLOCK_SIZE                             |
/// ```
///
// TODO: Add bloom filter and second-level index
// TODO: Constrain encoded KV's length
struct SSTable<K, V> {
    // Cache txlog id, and index block
    id: TxLogId,
    index: Footer<K>,
    phantom: PhantomData<(K, V)>,
}

struct Footer<K> {
    size: u32,
    key_range: Range<K>,
    metas: [FooterMeta<K>], // {size} metas
}

#[derive(Serialize, Deserialize)]
struct FooterMeta<K> {
    pos: BlockId,
    range: Range<K>,
}
const FOOTER_META_SIZE: usize = size_of::<FooterMeta>();

struct RecordsBlock<K, V> {
    records: [dyn AsKv<K, V>],
}
const SST_MAX_RECORD_CAP: usize = 131072; // TBD

impl<K, V> SSTable<K, V> {
    fn id(&self) -> TxLogId {
        self.id
    }

    fn index(&self) -> Footer<K> {
        self.index
    }

    /// Search a target records block position in the SST (from cache).
    fn search_in_cache(&self, key: &K) -> Option<BlockId> {
        let footer = self.index();
        for meta in footer.metas {
            if meta.range.contains(key) {
                return Some(meta.pos);
            }
        }
        None
    }

    /// Search a target record in the SST (from log).
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    fn search_in_log<D>(&self, key: &K, target_pos: BlockId, tx_log: Arc<TxLog<D>>) -> Result<V> {
        debug_assert!(tx_log.id() == self.id());
        let nblocks = tx_log.nblocks();
        let mut block_buf = BlockBuf::new(BLOCK_SIZE);

        // Search in the records block
        tx_log.read(target_pos, &mut block_buf)?;
        let records_block: RecordsBlock = RecordsBlock::from_bytes(&block_buf);
        for record in records_block.records {
            if record.key() == key {
                return Ok(record.value());
            }
        }
        Err(())
    }

    /// Build a SST given a bunch of records, after build, the SST sealed.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    fn build<D>(records: Vec<dyn AsKv<K, V>>, tx_log: Arc<TxLog<D>>) -> Result<Self> {
        let size = records.len();
        let key_range = records.first().unwrap()..records.last().unwrap();
        let mut pos_and_key_ranges = Vec::with_capacity(size);

        let num_records_per_block = BLOCK_SIZE / size_of::<dyn AsKv<K, V>>();
        let mut pos: BlockId = 0;
        for record_batch in records.chunks(num_records_per_block) {
            for record in record_batch {
                // TODO: Need padding
                tx_log.append(record.to_bytes())?;
            }
            pos_and_key_ranges.push((
                pos,
                record_batch.first().unwrap()..record_batch.last().unwrap(),
            ));
        }
        let footer = Footer {
            size,
            key_range,
            metas: pos_and_key_ranges,
        };

        // TODO: Need padding
        tx_log.append(footer.to_bytes())?;

        let built_self = Self {
            id: tx_log.id(),
            index: footer,
            phantom: PhantomData,
        };
        Ok(built_self)
    }

    /// Build a SST from a tx log, load index block to cache.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    fn from_log<D>(tx_log: &Arc<TxLog<D>>) -> Result<Self> {
        let nblocks = tx_log.nblocks();
        let mut block_buf = BlockBuf::new(BLOCK_SIZE);
        // Load footer block (last block)
        tx_log.read(nblocks - 1, &mut block_buf)?;

        let mut offset = 4_usize;
        let size = u32::from_le_bytes(block_buf[..offset]);
        let key_range: Range<K> =
            Range::<K>::from_bytes(block_buf[offset..offset + size_of::<Range<K>>()]);
        // TODO: Load each `FooterMeta<K>`
        let footer = Footer {
            size,
            key_range,
            metas: todo!(),
        };
        Ok(footer)
    }

    /// Collect all records from a SST.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    fn collect_all_records<D>(&self, tx_log: &Arc<TxLog<D>>) -> Result<Vec<dyn AsKv<K, V>>> {
        let mut records = Vec::with_capacity(SST_MAX_RECORD_CAP);
        let mut block_buf = BlockBuf::new(BLOCK_SIZE);
        for meta in self.metas {
            for (pos, block) in meta {
                tx_log.read(pos, block_buf)?;
                let records_block = RecordsBlock::from_bytes(block_buf)?;
                records.push(records_block.records);
            }
        }
        Ok(records)
    }
}

type Key = [u8; 16];
