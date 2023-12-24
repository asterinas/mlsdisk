//! The layer of transactional Lsm-Tree.
//!
//! This module provides the implementation for `TxLsmTree` and its submodules.
//! `TxLsmTree` is similar to general-purpose LSM-Tree, supporting `put()`, `get()`, `get_range()`
//! key-value records, which are managed in `MemTable`s and `SSTable`s.
//!
//! `TxLsmTree` is transactional in the sense that
//! 1) it supports `sync()` that guarantees changes are persisted atomically and irreversibly,
//! synchronized records and unsynchronized records can co-existed.
//! 2) its internal data is securely stored in `TxLogStore` (L3) and updated in transactions for consistency,
//! WALs and SSTables are stored and managed in `TxLogStore`.
//!
//! `TxLsmTree` supports piggybacking callbacks during compaction and recovery.
//!
//! # Usage Example
//!
//! Create a `TxLsmTree` then put some records into it.
//!
//! ```
//! struct YourFactory;
//! struct YourListener;
//!
//! impl<K, V> TxEventListenerFactory<K, V> for YourFactory {
//!     fn new_event_listener(&self, tx_type: TxType
//!     ) -> Arc<dyn TxEventListener<K, V>> {
//!         Arc::new(YourListener::new(tx_type))
//!     }
//! }
//! impl<K, V> TxEventListener<K, V> for YourListener {
//!     /* omitted */
//! }
//!
//! let nblocks = 1024;
//! let mem_disk = MemDisk::create(nblocks)?;
//! let tx_log_store = Arc::new(TxLogStore::format(mem_disk)?);
//! let tx_lsm_tree: TxLsmTree<BlockId, String, MemDisk> =
//!     TxLsmTree::format(tx_log_store, Arc::new(YourFactory), None)?;
//!
//! for i in 0..10 {
//!     let (k, v) = (
//!         i as BlockId,
//!         i.to_string(),
//!     );
//!     tx_lsm_tree.put(k, v)?;
//! }
//! tx_lsm_tree.sync()?;
//!
//! let target_value = tx_lsm_tree.get(&5).unwrap();
//! assert_eq!(target_value, "5");
//! ```

mod compaction;
mod mem_table;
mod range_query_ctx;
mod sstable;
mod tx_lsm_tree;
mod wal;

pub use self::range_query_ctx::RangeQueryCtx;
pub use self::tx_lsm_tree::{
    AsKV, LsmLevel, RecordKey, RecordValue, SyncID, TxEventListener, TxEventListenerFactory,
    TxLsmTree, TxType,
};
