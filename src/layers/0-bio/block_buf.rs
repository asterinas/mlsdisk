use crate::prelude::*;

use core::ops::{Deref, DerefMut};

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

pub struct BoxedBlockBuf<T>(T);

impl<T> BlockBuf for BoxedBlockBuf<T>
where
    T: Deref<Target = [u8]> + DerefMut,
{
    fn as_slice(&self) -> &[u8] {
        self.0.deref()
    }

    fn as_slice_mut(&mut self) -> &mut [u8] {
        self.0.deref_mut()
    }

    fn nblocks(&self) -> usize {
        self.as_slice().len() / BLOCK_SIZE
    }
}

impl BoxedBlockBuf<Box<[u8]>> {
    pub fn new(num_blocks: usize) -> Self {
        let boxed_slice = unsafe { Box::new_uninit_slice(num_blocks * BLOCK_SIZE).assume_init() };
        Self(boxed_slice)
    }
}
