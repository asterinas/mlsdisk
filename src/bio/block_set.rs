use pod::Pod;

/// A fixed set of data blocks that can support random reads and writes.
pub trait BlockSet {
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

    /// Ensure that blocks are persisted to the disk.
    fn flush(&self) -> Result<()>;

    /// Returns the number of blocks.
    fn num_blocks(&self) -> usize;
}

#[inherit_methods(from = "(**self)", inline = true)]
impl<T: BlockSet> &T for BlockSet {
    fn read(&self, pos: BlockId, buf: &mut impl BlockBuf) -> Result<()>;
    fn read_slice(&self, offset: usize, buf: &mut [u8]) -> Result<()>;
    fn write(&self, pos: BlockId, buf: &impl BlockBuf) -> Result<()>;
    fn write_slice(&self, offset: usize, buf: &[u8]) -> Result<()>;
    fn flush(&self) -> Result<()>;
    fn num_blocks(&self) -> usize;
}

#[inherit_methods(from = "(**self)", inline = true)]
impl<T: BlockSet> Box<T> for BlockSet {
    fn read(&self, pos: BlockId, buf: &mut impl BlockBuf) -> Result<()>;
    fn read_slice(&self, offset: usize, buf: &mut [u8]) -> Result<()>;
    fn write(&self, pos: BlockId, buf: &impl BlockBuf) -> Result<()>;
    fn write_slice(&self, offset: usize, buf: &[u8]) -> Result<()>;
    fn flush(&self) -> Result<()>;
    fn num_blocks(&self) -> usize;
}

#[inherit_methods(from = "(**self)", inline = true)]
impl<T: BlockSet> Arc<T> for BlockSet {
    fn read(&self, pos: BlockId, buf: &mut impl BlockBuf) -> Result<()>;
    fn read_slice(&self, offset: usize, buf: &mut [u8]) -> Result<()>;
    fn write(&self, pos: BlockId, buf: &impl BlockBuf) -> Result<()>;
    fn write_slice(&self, offset: usize, buf: &[u8]) -> Result<()>;
    fn flush(&self) -> Result<()>;
    fn num_blocks(&self) -> usize;
}