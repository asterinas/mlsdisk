use super::BlockBuf;
use crate::prelude::*;

use inherit_methods_macro::inherit_methods;

/// A log of data blocks that can support random reads and append-only
/// writes.
///
/// # Thread safety
///
/// `BlockLog` is a data structure of interior mutability.
/// It is ok to perform I/O on a `BlockLog` concurrently in multiple threads.
/// `BlockLog` promises the serialization of the append operations, i.e.,
/// concurrent appends are carried out as if they are done one by one.
pub trait BlockLog: Sync + Send {
    /// Read one or multiple blocks at a specified position.
    fn read(&self, pos: BlockId, buf: &mut impl BlockBuf) -> Result<()>;

    /// Append one or multiple blocks at the end,
    /// returning the ID of the first newly-appended block.
    fn append(&self, buf: &impl BlockBuf) -> Result<BlockId>;

    /// Ensure that blocks are persisted to the disk.
    fn flush(&self) -> Result<()>;

    /// Returns the number of blocks.
    fn nblocks(&self) -> usize;
}

macro_rules! impl_blocklog_pointer {
    ($typ:ty,$from:tt) => {
        #[inherit_methods(from = $from)]
        impl<T: BlockLog> BlockLog for $typ {
            fn read(&self, pos: BlockId, buf: &mut impl BlockBuf) -> Result<()>;
            fn append(&self, buf: &impl BlockBuf) -> Result<BlockId>;
            fn flush(&self) -> Result<()>;
            fn nblocks(&self) -> usize;
        }
    };
}

impl_blocklog_pointer!(&T, "(**self)");
// impl_blocklog_pointer!(&mut T, "(**self)");
impl_blocklog_pointer!(Box<T>, "(**self)");
impl_blocklog_pointer!(Arc<T>, "(**self)");
