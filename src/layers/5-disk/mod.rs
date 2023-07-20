//! SwornDisk as a BlockDevice.
//!
//! API: read(), write(), flush(), flush_blocks(), discard(), total_blocks(), open(), create()
//!
//! Responisble for managing a `TxLsmTree`, an untrusted disk
//! storing user data, a `BlockAlloc` for managing data block
//! validity manage tx logs (WAL and SSTs). `TxLsmTree` and
//! `BlockAlloc` are manipulated based on internal transactions.
use super::bio::{BlockBuf, BlockId, BlockSet};
use super::log::{TxLog, TxLogStore};
use super::lsm::{AsKv, LsmLevel, TxEventListener, TxEventListenerFactory, TxLsmTree, TxType};
use crate::tx::Tx;

use serde::{Deserialize, Serialize};
use spin::Mutex;
use std::collections::BTreeMap;
use std::sync::Arc;

type Lba = BlockId;
type Hba = BlockId;

type BitMap = bitvec::prelude::BitVec<u8, bitvec::prelude::Lsb0>;

// ID: Index Disk, DD: Data Disk
// D: one disk generic
pub struct SwornDisk<D: BlockSet> {
    tx_lsm_tree: TxLsmTree<RecordKey, RecordValue, D>,
    user_data: D,
    block_validity_bitmap: Arc<AllocBitmap>,
    root_key: Key,
}

impl<D: BlockSet> SwornDisk<D> {
    /// Read a specified number of block contents at a logical block address on the device.
    pub fn read(&self, lba: Lba, buf: &mut impl BlockBuf) -> Result<usize> {
        // TODO: Iterate each block
        let record = self.tx_lsm_tree.get(lba)?;
        self.user_data.read(record.hba, buf)?;
        // TODO: Decrypt block buf
        util::crypto::decrypt(buf)?;
        Ok(buf.nblocks())
    }

    /// Write a specified number of block contents at a logical block address on the device.
    pub fn write(&self, lba: Lba, buf: &impl BlockBuf) -> Result<usize> {
        let hba = self.alloc_block()?;
        // TODO: Encrypt block buf
        let (key, mac) = util::crypto::encrypt(buf)?;
        // TODO: Handle put error
        self.tx_lsm_tree
            .put(RecordKey { lba }, RecordValue { hba, key, mac });
        Ok(buf.nblocks())
    }

    /// Sync all cached data in the device to the storage medium for durability.
    pub fn flush(&self) -> Result<()> {
        self.tx_lsm_tree.commit()?;
        self.user_data.flush()
    }

    /// Flush specified cached blocks to the underlying device.
    pub fn flush_blocks(&self, blocks: &[Lba]) -> Result<usize> {
        // TODO: Optimize flush_blocks
        self.flush().map(|| blocks.len())
    }

    /// Discard(trim) a specified number of blocks.
    pub fn discard(&self, blocks: &[Lba]) -> Result<usize> {
        self.tx_lsm_tree.discard(blocks)
    }

    /// Return the total number of blocks in the device.
    pub fn total_blocks(&self) -> usize {
        self.user_data.nblocks()
    }

    /// Open the device on a disk, given the root cryption key.
    pub fn open(&self, disk: D, root_key: Key) -> Result<Self> {
        let data_disk = Self::data_disk(&disk)?;
        let index_disk = Self::index_disk(&disk)?;

        let tx_log_store = Arc::new(TxLogStore::recover(index_disk, root_key)?);
        let block_validity_bitmap = Arc::new(Mutex::new(BitMap::repeat(true, data_disk.nblocks())));
        let listener_factory = Arc::new(TxLsmTreeListenerFactory::<RecordKey, RecordValue>::new(
            tx_log_store.clone(),
            block_validity_bitmap.clone(),
        ));
        let on_drop_record_in_memtable = |record: &Record| self.dealloc_block(record.value.hba);
        let tx_lsm_tree =
            TxLsmTree::recover(tx_log_store, listener_factory, on_drop_record_in_memtable)?;

        Ok(Self {
            tx_lsm_tree,
            user_data: data_disk,
            root_key,
            block_validity_bitmap,
        })
    }

    /// Create the device on a disk, given the root cryption key.
    pub fn create(&self, disk: D, root_key: Key) -> Result<Self> {
        let data_disk = disk.subset(0..disk.nblocks() / 10 * 9)?;
        let index_disk = disk.subset(disk.nblocks() / 10 * 9..)?;

        let tx_log_store = Arc::new(TxLogStore::format(index_disk)?);
        let block_validity_bitmap = Arc::new(Mutex::new(BitMap::repeat(true, data_disk.nblocks())));
        let listener_factory = Arc::new(TxLsmTreeListenerFactory::<RecordKey, RecordValue>::new(
            tx_log_store.clone(),
            block_validity_bitmap.clone(),
        ));
        let on_drop_record_in_memtable = |record: &Record| self.dealloc_block(record.value.hba);
        let tx_lsm_tree = TxLsmTree::<RecordKey, RecordValue>::format(
            tx_log_store,
            listener_factory,
            on_drop_record_in_memtable,
        );

        Ok(Self {
            tx_lsm_tree,
            user_data: data_disk,
            root_key,
            block_validity_bitmap,
        })
    }

    fn alloc_block(&self) -> Option<BlockId> {
        let bitmap = self.block_validity_bitmap.lock();
        bitmap.find_first_avail_block().ok()?
    }

    fn dealloc_block(&self, block_id: BlockId) {
        let mut bitmap = self.block_validity_bitmap.lock();
        bitmap.set(block_id, true);
    }

    fn data_disk(disk: &D) -> D {
        disk.subset(0..disk.nblocks() / 10 * 9) // TBD
    }

    fn index_disk(disk: &D) -> D {
        disk.subset(disk.nblocks() / 10 * 9..disk.nblocks()) // TBD
    }
}

struct TxLsmTreeListenerFactory<D> {
    store: Arc<TxLogStore<D>>,
    alloc_bitmap: Arc<AllocBitmap>,
}

impl<D> TxEventListenerFactory<D> {
    fn new(store: Arc<TxLogStore<D>>, alloc_bitmap: Arc<AllocBitmap>) -> Self {
        Self {
            store,
            alloc_bitmap,
        }
    }
}

impl<K, V> TxEventListenerFactory for TxLsmTreeListenerFactory<K, V> {
    fn new_event_listerner(&self, tx_type: TxType) -> Arc<dyn TxEventListener<K, V>> {
        Arc::new(TxLsmTreeListener::new(
            tx_type,
            Arc::new(BlockAlloc::new(
                self.alloc_bitmap.clone(),
                self.store.clone(),
            )),
        ))
    }
}

/// Event listener for `TxLsmTree`.
struct TxLsmTreeListener<D> {
    tx_type: TxType,
    block_alloc: Arc<BlockAlloc<D>>,
}

impl<D> TxLsmTreeListener<D> {
    fn new(tx_type: TxType, block_alloc: Arc<BlockAlloc<D>>) -> Self {
        Self {
            tx_type,
            block_alloc,
        }
    }
}

/// Register callbacks for different txs in `TxLsmTree`.
impl<K, V> TxEventListener for TxLsmTreeListener<K, V> {
    fn on_add_record(&self, record: &Record) -> Result<()> {
        match self.tx_type {
            TxType::Compaction { to_level } => {
                if to_level != LsmLevel::L0 {
                    return Ok(());
                }
                self.block_alloc.alloc_block(record.value.hba)
            }
            TxType::Migration => return Ok(()),
        }
    }

    fn on_drop_record(&self, record: &Record) -> Result<()> {
        match self.tx_type {
            TxType::Compaction | TxType::Migration => {
                self.block_alloc.dealloc_block(record.value.hba)
            }
        }
    }

    fn on_tx_at_beginning(&self, tx: &Tx) -> Result<()> {
        match self.tx_type {
            TxType::Compaction | TxType::Migration => {
                tx.context(|| self.block_alloc.open_diff_log())
            }
        }
    }

    fn on_tx_near_end(&self, tx: &Tx) -> Result<()> {
        match self.tx_type {
            TxType::Compaction | TxType::Migration => {
                let diff_log = self.block_alloc.diff_log.lock().unwrap();
                self.block_alloc.update_diff_log(diff_log)
            }
        }
    }

    fn on_tx_commit(&self) {
        match self.tx_type {
            TxType::Compaction | TxType::Migration => self.block_alloc.update_bitmap(),
        }
    }
}

const BUCKET_BLOCK_VALIDITY_BITMAP: &str = "BVB";
const BUCKET_BLOCK_VALIDITY_DIFF: &str = "BVD";

/// Block allocator, manages user-data blocks validity.
// TODO: Distinguish snapshot diff log (during compaction) and regular diff log
struct BlockAlloc<D> {
    bitmap: Arc<AllocBitmap>,               // In memory
    diff_table: AllocDiffTable,             // In memory
    store: Arc<TxLogStore<D>>,              // On disk
    diff_log: Mutex<Option<Arc<TxLog<D>>>>, // Cache opened diff log // TODO: Support multiple diff logs
}

/// Block validity bitmap.
type AllocBitmap = Mutex<BitMap>;
/// Incremental changes of block validity bitmap.
type AllocDiffTable = Mutex<BTreeMap<BlockId, AllocDiff>>;

enum AllocDiff {
    Alloc,
    Dealloc,
}

#[derive(Serialize, Deserialize)]
enum AllocDiffRecord {
    Diff(BlockId, AllocDiff),
    Checkpoint,
}

impl BlockBuf for AllocDiffRecord {}

impl<D> BlockAlloc<D> {
    fn new(bitmap: Arc<AllocBitmap>, store: TxLogStore<D>) -> Self {
        Self {
            bitmap,
            diff_table: BTreeMap::new(),
            store,
            diff_log: Mutex::new(None),
        }
    }

    /// Allocate a specifiied block, means update in-memory metadata.
    fn alloc_block(&self, block_id: BlockId) -> Result<()> {
        let mut diff_table = self.diff_table.lock();
        let replaced = diff_table.insert(block_id, AllocDiff::Alloc);
        if replaced == Some(AllocDiff::Alloc) {
            panic!("cannot allocate a block twice");
        }
        Ok(())
    }

    /// Deallocate a specifiied block, means update in-memory metadata.
    fn dealloc_block(&self, block_id: BlockId) -> Result<()> {
        let mut diff_table = self.diff_table.lock();
        let replaced = diff_table.insert(block_id, AllocDiff::Dealloc);
        if replaced == Some(AllocDiff::Dealloc) {
            panic!("cannot deallocate a block twice");
        }
        Ok(())
    }

    /// Open the block validity diff log.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    fn open_diff_log(&self) -> Result<Arc<TxLog<D>>> {
        let diff_log = self.store.open_log_in(BUCKET_BLOCK_VALIDITY_DIFF)?;
        self.diff_log.lock().insert(diff_log.clone());
        Ok(diff_log)
    }

    /// Update cached diff table to the block validity diff log.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    fn update_diff_log(&self, diff_log: Arc<TxLog<D>>) -> Result<()> {
        let mut diff_table = self.diff_table.lock();
        for (block_id, block_diff) in diff_table {
            diff_log.append(AllocDiffRecord::Diff(block_id, block_diff))?;
        }
        Ok(())
    }

    fn update_bitmap(&self) {
        let mut diff_table = self.diff_table.lock();
        let mut bitmap = self.bitmap.lock();
        for (block_id, block_diff) in diff_table {
            let validity = match block_diff {
                AllocDiff::Alloc => false,
                AllocDiff::Dealloc => true,
            };
            bitmap.set(block_id, validity)?;
        }
        drop(bitmap);
    }

    /// Checkpoint to seal a bitmap log snapshot, diff logs before
    /// checkpoint can be deleted.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    // TODO: Use snapshot diff log instead bitmap log
    fn checkpoint(&self, diff_log: Arc<TxLog>, bitmap_log: Arc<TxLog<D>>) -> Result<()> {
        let inner = self.inner.lock();
        bitmap_log.append(inner.validity_bitmap.to_bytes())?;
        diff_log.append(AllocDiffRecord::Checkpoint)
    }

    fn recover(store: Arc<TxLogStore<D>>) -> Result<Self> {
        // Open the latest bitmap log, apply each `AllocDiffRecord::Diff` after the newest `AllocDiffRecord::Checkpoint` to the bitmap
        todo!()
    }

    fn do_compaction(&self) {
        // Open the diff log, Migrate `AllocDiffRecord::Diff`s after newest `AllocDiffRecord::Checkpoint` to a new log, delete the old one
        todo!()
    }
}

/// K-V record for `TxLsmTree`.
struct Record {
    key: RecordKey,
    value: RecordValue,
}

struct RecordKey {
    pub lba: Lba,
}

type Key = [u8; 16];
type Mac = [u8; 12];

struct RecordValue {
    pub hba: Hba,
    pub key: Key,
    pub mac: Mac,
}

impl<K, V> AsKv<K, V> for Record {
    fn key(&self) -> &K {
        self.key
    }

    fn value(&self) -> &V {
        self.value
    }
}
