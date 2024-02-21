//! Data buffering.
use super::sworndisk::RecordKey;
use crate::layers::bio::{BufMut, BufRef};
use crate::os::{BTreeMap, Mutex};
use crate::prelude::*;

use core::ops::RangeInclusive;

// TODO: Put them into os module
// use std::sync::{Condvar, Mutex as StdMutex};

/// A buffer to cache data blocks before they are written to disk.
#[derive(Debug)]
pub(super) struct DataBuf {
    buf: Mutex<BTreeMap<RecordKey, Arc<DataBlock>>>,
    cap: usize,
    //    cvar: Condvar,
    //    state: StdMutex<BufState>,
}

/// User data block.
pub(super) struct DataBlock([u8; BLOCK_SIZE]);

/// State of the `DataBuf`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BufState {
    /// `Vacant` indicates the buffer has available space,
    /// the buffer is ready to read or write.
    Vacant,
    /// `Full` indicates the buffer capacity is run out,
    /// the buffer cannot write, can read.
    Full,
}

impl DataBuf {
    /// Create a new empty data buffer with a given capacity.
    pub fn new(cap: usize) -> Self {
        Self {
            buf: Mutex::new(BTreeMap::new()),
            cap,
            // cvar: Condvar::new(),
            // state: StdMutex::new(BufState::Vacant),
        }
    }

    /// Get the buffered data block with the key and copy
    /// the content into `buf`.
    pub fn get(&self, key: RecordKey, buf: &mut BufMut) -> Option<()> {
        debug_assert_eq!(buf.nblocks(), 1);
        if let Some(block) = self.buf.lock().get(&key) {
            buf.as_mut_slice().copy_from_slice(block.as_slice());
            Some(())
        } else {
            None
        }
    }

    /// Get the buffered data blocks which keys are within the given range.
    pub fn get_range(&self, range: RangeInclusive<RecordKey>) -> Vec<(RecordKey, Arc<DataBlock>)> {
        self.buf
            .lock()
            .iter()
            .filter_map(|(k, v)| {
                if range.contains(k) {
                    Some((*k, v.clone()))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Put the data block in `buf` into the buffer. Return
    /// whether the buffer is full after insertion.
    pub fn put(&self, key: RecordKey, buf: BufRef) -> bool {
        debug_assert_eq!(buf.nblocks(), 1);

        // let mut state = self.state.lock().unwrap();
        // while *state != BufState::Vacant {
        //     state = self.cvar.wait(state).unwrap();
        // }
        // debug_assert_eq!(*state, BufState::Vacant);

        let mut data_buf = self.buf.lock();
        let _ = data_buf.insert(key, DataBlock::from_buf(buf));

        let is_full = data_buf.len() >= self.cap;
        // if is_full {
        //     *state = BufState::Full;
        // }
        is_full
    }

    /// Return the number of data blocks of the buffer.
    pub fn nblocks(&self) -> usize {
        self.buf.lock().len()
    }

    /// Return whether the buffer is full.
    pub fn at_capacity(&self) -> bool {
        self.nblocks() >= self.cap
    }

    /// Return whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.nblocks() == 0
    }

    /// Empty the buffer.
    pub fn clear(&self) {
        // let mut state = self.state.lock().unwrap();
        // debug_assert_eq!(*state, BufState::Full);

        self.buf.lock().clear();

        // *state = BufState::Vacant;
        // self.cvar.notify_all();
    }

    /// Return all the buffered data blocks.
    pub fn all_blocks(&self) -> Vec<(RecordKey, Arc<DataBlock>)> {
        self.buf
            .lock()
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }
}

impl DataBlock {
    /// Create a new data block from the given `buf`.
    pub fn from_buf(buf: BufRef) -> Arc<Self> {
        debug_assert_eq!(buf.nblocks(), 1);
        Arc::new(DataBlock(buf.as_slice().try_into().unwrap()))
    }

    /// Return the immutable slice of the data block.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl Debug for DataBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataBlock")
            .field("first 16 bytes", &&self.0[..16])
            .finish()
    }
}
