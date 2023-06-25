/// A buffer that contains one or multiple buffers.
/// 
/// The advantage of using `BlockBuf` instead of `[u8]` is to enforce
/// the invariance that the length of a buffer is a multiple of block size
/// at the type level.
pub trait BlockBuf {
    fn as_slice(&self) -> &[u8];
    
    fn as_slice_mut(&mut self) -> &mut [u8];

    fn nblocks(&self) -> usize;
}

pub struct BlockBuf<T>(T);

impl<T> BlockBuf<T>
where
    T: Deref<[u8]> + DerefMut<[u8]>,
{
    fn as_slice(&self) -> &[u8] {
        self.0.deref()
    }
    
    fn as_slice_mut(&mut self) -> &mut [u8] {
        self.0.deref_mut()
    }

    fn nblocks(&self) -> usize {
        self.as_slice() / BLOCK_SIZE
    }
}

impl BlockBuf<Box<[u8]>> {
    pub fn new(num_blocks: usize) -> Self {
        
    }
}
