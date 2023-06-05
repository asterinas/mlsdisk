/// A log of data blocks that can support random reads and append-only
/// writes.
pub trait BlockLog {
    /// Read one or multiple blocks at a specified position.
    fn read(&self, pos: BlockId, buf: &mut impl BlockBuf) -> Result<()>;

    /// Append one or multiple blocks at the end, 
    /// returning the ID of the newly-appended block.
    fn append(&self, buf: &impl BlockBuf) -> Result<BlockId>;

    /// Ensure that blocks are persisted to the disk.
    fn flush(&self) -> Result<()>;

    /// Returns the number of blocks.
    fn num_blocks(&self) -> usize;
}

#[inherit_methods(from = "(**self)", inline = true)]
impl<T: BlockLog> &T for BlockLog {
    fn read(&self, pos: BlockId, buf: &mut impl BlockBuf) -> Result<()>;
    fn append(&self, buf: &impl BlockBuf) -> Result<BlockId>;
    fn flush(&self) -> Result<()>;
    fn num_blocks(&self) -> usize;
}

#[inherit_methods(from = "(**self)", inline = true)]
impl<T: BlockLog> Box<T> for BlockLog {
    fn read(&self, pos: BlockId, buf: &mut impl BlockBuf) -> Result<()>;
    fn append(&self, buf: &impl BlockBuf) -> Result<BlockId>;
    fn flush(&self) -> Result<()>;
    fn num_blocks(&self) -> usize;
}

#[inherit_methods(from = "(**self)", inline = true)]
impl<T: BlockLog> Arc<T> for BlockLog {
    fn read(&self, pos: BlockId, buf: &mut impl BlockBuf) -> Result<()>;
    fn append(&self, buf: &impl BlockBuf) -> Result<BlockId>;
    fn flush(&self) -> Result<()>;
    fn num_blocks(&self) -> usize;
}