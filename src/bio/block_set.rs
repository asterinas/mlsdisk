use core::ops::Range;
use pod::Pod;

use super::BlockId;

/// A fixed set of data blocks that can support random reads and writes.
/// 
/// # Thread safety
/// 
/// `BlockSet` is a data structure of interior mutability.
/// It is ok to perform I/O on a `BlockSet` concurrently in multiple threads.
/// `BlockSet` promises the atomicity of reading and writing individual blocks.
pub trait BlockSet: Sync + Send {
    /// Read one or multiple blocks at a specified position.
    fn read(&self, pos: BlockId, buf: &mut impl BlockBuf) -> Result<()>;

    /// Read a slice of bytes at a specified byte offset.
    fn read_slice(&self, offset: usize, buf: &mut [u8]) -> Result<()> {
        todo!("provide the default impl using read")
    }

    /// Write one or multiple blocks at a specified position.
    fn write(&self, pos: BlockId, buf: &impl BlockBuf) -> Result<()>;

    /// Write a slice of bytes at a specified byte offset.
    fn write_slice(&self, offset: usize, buf: &[u8]) -> Result<()> {
        todo!("provide the default impl using write")
    }

    /// Get a subset of the blocks in the block set.
    fn subset(&self, range: Range<BlockId>) -> Result<Self>; 

    /// Ensure that blocks are persisted to the disk.
    fn flush(&self) -> Result<()>;

    /// Returns the number of blocks.
    fn nblocks(&self) -> usize;
}

#[inherit_methods(from = "(**self)", inline = true)]
impl<T: BlockSet> &T for BlockSet {
    fn read(&self, pos: BlockId, buf: &mut impl BlockBuf) -> Result<()>;
    fn read_slice(&self, offset: usize, buf: &mut [u8]) -> Result<()>;
    fn write(&self, pos: BlockId, buf: &impl BlockBuf) -> Result<()>;
    fn write_slice(&self, offset: usize, buf: &[u8]) -> Result<()>;
    fn flush(&self) -> Result<()>;
    fn nblocks(&self) -> usize;
}

#[inherit_methods(from = "(**self)", inline = true)]
impl<T: BlockSet> Box<T> for BlockSet {
    fn read(&self, pos: BlockId, buf: &mut impl BlockBuf) -> Result<()>;
    fn read_slice(&self, offset: usize, buf: &mut [u8]) -> Result<()>;
    fn write(&self, pos: BlockId, buf: &impl BlockBuf) -> Result<()>;
    fn write_slice(&self, offset: usize, buf: &[u8]) -> Result<()>;
    fn flush(&self) -> Result<()>;
    fn nblocks(&self) -> usize;
}

#[inherit_methods(from = "(**self)", inline = true)]
impl<T: BlockSet> Arc<T> for BlockSet {
    fn read(&self, pos: BlockId, buf: &mut impl BlockBuf) -> Result<()>;
    fn read_slice(&self, offset: usize, buf: &mut [u8]) -> Result<()>;
    fn write(&self, pos: BlockId, buf: &impl BlockBuf) -> Result<()>;
    fn write_slice(&self, offset: usize, buf: &[u8]) -> Result<()>;
    fn flush(&self) -> Result<()>;
    fn nblocks(&self) -> usize;
}