use super::{BlockBuf, BlockLog, BlockSet};
use crate::prelude::*;

/// `BlockRing<S>` emulates a blocks log (`BlockLog`) with infinite
/// storage capacity by using a block set (`S: BlockSet`) of finite storage
/// capacity.
///
/// `BlockRing<S>` uses the entire storage space provided by the underlying
/// block set (`S`) for user data, maintaining no extra metadata.
/// Having no metadata, `BlockRing<S>` has to put three responsibilities to
/// its user:
///
/// 1. Tracking the valid block range for read.
/// `BlockRing<S>` accepts reads at any position regardless of whether the
/// position refers to a valid block. It blindly redirects the read request to
/// the underlying block set after moduloing the target position by the
/// size of the block set.
///
/// 2. Setting the cursor for appending new blocks.
/// `BlockRing<S>` won't remember the progress of writing blocks after reboot.
/// Thus, after a `BlockRing<S>` is instantiated, the user must specify the
/// append cursor (using the `set_cursor` method) before appending new blocks.
///
/// 3. Avoiding overriding valid data blocks mistakenly.
/// As the underlying storage is used in a ring buffer style, old
/// blocks must be overriden to accommodate new blocks. The user must ensure
/// that the underlying storage is big enough to avoid overriding any useful
/// data.
pub struct BlockRing<S> {
    storage: S,
    // The cursor for appending new blocks
    cursor: Option<BlockId>,
}

impl<S: BlockSet> BlockRing<S> {
    /// Creates a new instance.
    pub fn new(storage: S) -> Self {
        Self {
            storage,
            cursor: None,
        }
    }

    /// Set the cursor for appending new blocks.
    ///
    /// # Panics
    ///
    /// Calling the `append` method without setting the append cursor first
    /// via this method `set_cursor` causes panic.
    pub fn set_cursor(&mut self, new_cursor: BlockId) {
        self.cursor = Some(new_cursor);
    }
}

impl<S: BlockSet> BlockLog for BlockRing<S> {
    fn read(&self, pos: BlockId, buf: &mut impl BlockBuf) -> Result<()> {
        let pos = pos % self.storage.nblocks();
        self.storage.read(pos, buf)
    }

    fn append(&self, buf: &impl BlockBuf) -> Result<BlockId> {
        let cursor = self
            .cursor
            .expect("cursor must be set before appending new blocks");
        let pos = cursor % self.storage.nblocks();
        let cursor = cursor + buf.nblocks();
        // self.cursor.insert(cursor);
        self.storage.write(pos, buf).map(|_| cursor)
    }

    fn flush(&self) -> Result<()> {
        self.storage.flush()
    }

    fn nblocks(&self) -> usize {
        self.cursor.unwrap_or(0)
    }
}
