/// A buffer that contains one or multiple buffers.
/// 
/// The advantage of using `BlockBuf` instead of `[u8]` is to enforce
/// the invariance that the length of a buffer is a multiple of block size
/// at the type level.
pub trait BlockBuf {
    fn as_slice(&self) -> &[u8];
    
    fn as_slice_mut(&mut self) -> &mut [u8];

    fn num_blocks(&self) -> usize;
}
