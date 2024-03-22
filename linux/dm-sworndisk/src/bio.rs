// SPDX-License-Identifier: GPL-2.0
// Copyright (c) 2023 Ant Group CO., Ltd.

//! Block I/O.

use core::{
    fmt,
    ops::{Deref, DerefMut, Range},
    ptr::NonNull,
};
use kernel::{prelude::*, types::Opaque};

use super::block_device::{HostBlockDevice, BLOCK_SECTORS, BLOCK_SIZE};

// See definition at `include/linux/gfp_types.h`.
const GFP_NOIO: bindings::gfp_t = bindings::___GFP_DIRECT_RECLAIM | bindings::___GFP_KSWAPD_RECLAIM;

/// A handle to kernel's struct `bio`.
pub(crate) struct Bio {
    inner: NonNull<bindings::bio>,
}

// SAFETY: we get a reference to kernel's `bio` by `bio_get`, so it won't disappear
// and can be safely transferred across threads.
unsafe impl Send for Bio {}

impl Bio {
    /// Constructs a `Bio` from raw pointer.
    ///
    /// # Safety
    ///
    /// User must provide a valid pointer to the kernel's `bio`.
    pub unsafe fn from_raw(ptr: *mut bindings::bio) -> Self {
        // Get a reference to `bio`, so it won't disappear. And `bio_put` is
        // called when it's been dropped.
        bindings::bio_get(ptr);

        Self {
            inner: NonNull::new_unchecked(ptr),
        }
    }

    /// Allocates a `Bio` for the target `HostBlockDevice`.
    ///
    /// The bio operation is specified by `op`, targeting a given block region,
    /// while its I/O buffer is configured by `BioVec`.
    pub fn alloc(
        bdev: &HostBlockDevice,
        bio_op: BioOp,
        region: Range<usize>,
        io_buffer: &[BioVec],
    ) -> Result<Self> {
        let buf_len: usize = io_buffer.iter().map(|bio_vec| bio_vec.len()).sum();
        if buf_len != region.len() * BLOCK_SIZE {
            return Err(EINVAL);
        }

        // SAFETY: `bdev.as_ptr()` is valid pointer to `block_device`, `bio_alloc_bioset`
        // returns a pointer to new `bio`, or `NULL` on failure.
        let bio_ptr = unsafe {
            bindings::bio_alloc_bioset(
                bdev.as_ptr(),
                io_buffer.len() as _,
                bio_op as _,
                GFP_NOIO,
                &mut bindings::fs_bio_set,
            )
        };
        if bio_ptr.is_null() {
            return Err(ENOMEM);
        }

        for bio_vec in io_buffer {
            // SAFETY: `bio_vec` contains valid pointer to `page`, `bio_ptr` is
            // non-null and valid.
            unsafe {
                let page = bindings::virt_to_page(bio_vec.virt_addr as _);
                bindings::__bio_add_page(bio_ptr, page, bio_vec.len as _, bio_vec.offset as _);
            }
        }

        // SAFETY: the `bio_ptr` is non-null and valid.
        let inner = unsafe {
            (*bio_ptr).bi_iter.bi_sector = (region.start * BLOCK_SECTORS) as _;
            (*bio_ptr).bi_iter.bi_size = (region.len() * BLOCK_SIZE) as _;
            NonNull::new_unchecked(bio_ptr)
        };
        Ok(Self { inner })
    }

    /// Gets the value of operation and flags.
    ///
    /// The bottom 8 bits are encoding the operation, and the remaining 24 for flags.
    pub fn opf(&self) -> u32 {
        // SAFETY: `self.inner` is a valid `bio`.
        unsafe { (*self.inner.as_ptr()).bi_opf }
    }

    /// Sets the value of operation and flags.
    pub fn set_opf(&mut self, opf: u32) {
        // SAFETY: `self.inner` is a valid `bio`.
        unsafe { (*self.inner.as_ptr()).bi_opf = opf }
    }

    /// Returns the operation of `Bio`.
    pub fn op(&self) -> BioOp {
        BioOp::from(self.opf() as u8)
    }

    /// Returns true if it carries any data.
    pub fn has_data(&self) -> bool {
        // SAFETY: `self.inner` is a valid `bio`.
        unsafe { bindings::bio_has_data(self.inner.as_ptr()) }
    }

    /// Sets the target `HostBlockDevice` (remap bio request).
    pub fn set_dev(&mut self, bdev: &HostBlockDevice) {
        // SAFETY: `self.inner` is a valid `bio`.
        unsafe { bindings::bio_set_dev(self.inner.as_ptr(), bdev.as_ptr()) }
    }

    /// Returns the start sector.
    pub fn start_sector(&self) -> usize {
        // SAFETY: `self.inner` is a valid `bio`.
        unsafe { (*self.inner.as_ptr()).bi_iter.bi_sector as _ }
    }

    /// Sets the start sector (remap the request).
    pub fn set_start_sector(&mut self, sector: usize) {
        // SAFETY: `self.inner` is a valid `bio`.
        unsafe { (*self.inner.as_ptr()).bi_iter.bi_sector = sector as _ }
    }

    /// Returns the length in bytes.
    pub fn len(&self) -> usize {
        // SAFETY: `self.inner` is a valid `bio`.
        unsafe { (*self.inner.as_ptr()).bi_iter.bi_size as _ }
    }

    /// Submits the underlying `bio` (synchronously).
    ///
    /// # Panics
    ///
    /// User must ensure that this is an owned `Bio` created by `alloc`. An
    /// instance created by `from_raw` should not call this method, since we
    /// don't know if the raw `bio` has its own `bi_end_io` function.
    pub fn submit_sync(&self) -> Result<()> {
        // SAFETY: `self.inner` is a valid `bio`.
        let err = unsafe { bindings::submit_bio_wait(self.inner.as_ptr()) };
        kernel::error::to_result(err)
    }

    /// Ends the underlying `bio`.
    pub fn end(&self) {
        // SAFETY: `self.inner` is a valid `bio`.
        unsafe { bindings::bio_endio(self.inner.as_ptr()) }
    }

    /// Returns an iterator on the I/O buffer of `Bio`.
    pub fn iter(&self) -> BioVecIter {
        // SAFETY: `self.inner` is a valid `bio`.
        unsafe {
            let bio = self.inner.as_ptr();
            let vec = (*bio).bi_io_vec;
            let iter = core::ptr::addr_of_mut!((*bio).bi_iter);
            BioVecIter::from_raw(vec, iter)
        }
    }
}

impl fmt::Debug for Bio {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bio")
            .field("operation", &self.op())
            .field("start_sector", &self.start_sector())
            .field("length", &self.len())
            .finish()
    }
}

impl Drop for Bio {
    fn drop(&mut self) {
        // SAFETY: `self.inner` is a valid `bio`, we get a reference when constructs
        // an `Bio` instance, so put it here. The last put of a `bio` will free it.
        unsafe { bindings::bio_put(self.inner.as_ptr()) };
    }
}

/// An iterator on `BioVec`.
pub(crate) struct BioVecIter {
    iter: Opaque<bindings::bvec_iter>,
    vec: NonNull<bindings::bio_vec>,
}

impl BioVecIter {
    /// Constructs a `BioVecIter` from raw pointers.
    ///
    /// # Safety
    ///
    /// Caller must provide valid pointers to the `bio_vec` and `bvec_iter`
    /// of a valid `bio`.
    unsafe fn from_raw(vec: *mut bindings::bio_vec, iter: *mut bindings::bvec_iter) -> Self {
        let new_iter = Opaque::new(bindings::bvec_iter::default());
        *new_iter.get() = *iter;
        Self {
            iter: new_iter,
            vec: NonNull::new_unchecked(vec),
        }
    }

    /// Returns true if the iterator has next `BioVec`.
    fn has_next(&self) -> bool {
        unsafe { (*self.iter.get()).bi_size > 0 }
    }

    /// Returns the current `BioVec` of the iterator.
    fn current_bio_vec(&self) -> BioVec {
        // SAFETY: `self.iter` and `self.vec` is initialized by `from_raw`, should
        // be valid pointers.
        unsafe {
            let expect_len = (*self.iter.get()).bi_size as usize;
            let idx = (*self.iter.get()).bi_idx as usize;
            let next = self.vec.as_ptr().add(idx);
            BioVec::from_bio_vec(next, expect_len)
        }
    }

    /// Advances the iterator with current `BioVec`.
    fn advance_by(&mut self, vec: &BioVec) {
        // SAFETY: `self.iter` is initialized by `from_raw`, should be valid.
        // This function can only be called in `next`, so the `vec` is from
        // `current_bio_vec`, whose length would not larger than `bi_size`.
        unsafe {
            (*self.iter.get()).bi_size -= vec.len() as u32;
            (*self.iter.get()).bi_idx += 1;
        }
    }
}

impl Iterator for BioVecIter {
    type Item = BioVec;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.has_next() {
            return None;
        }

        let bio_vec = self.current_bio_vec();
        self.advance_by(&bio_vec);
        Some(bio_vec)
    }
}

/// A contiguous range of physical memory.
///
/// The `virt_addr` is virtual address to a valid and directly mapped `page`.
/// This struct is used to iterate the I/O buffer in `Bio`.
pub(crate) struct BioVec {
    virt_addr: usize,
    offset: usize,
    len: usize,
}

impl BioVec {
    /// Constructs a `BioVec` from kernel's `bio_vec`.
    ///
    /// # Safety
    ///
    /// User must provide a valid pointer to kernel's `bio_vec`.
    unsafe fn from_bio_vec(vec: *mut bindings::bio_vec, expect_len: usize) -> Self {
        Self {
            virt_addr: bindings::page_to_virt((*vec).bv_page) as _,
            offset: (*vec).bv_offset as _,
            len: expect_len.min((*vec).bv_len as _),
        }
    }

    /// Constructs a `BioVec` from raw pointer.
    ///
    /// # Safety
    ///
    /// User must ensure that `virt_addr` points to a valid and directly mapped `page`.
    pub unsafe fn from_raw(virt_addr: usize, offset: usize, len: usize) -> Self {
        Self {
            virt_addr,
            offset,
            len,
        }
    }
}

impl Deref for BioVec {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        // SAFETY: `self.virt_addr` is a pointer to valid memory.
        unsafe {
            let ptr = (self.virt_addr as *mut u8).add(self.offset);
            core::slice::from_raw_parts(ptr, self.len)
        }
    }
}

impl DerefMut for BioVec {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: `self.virt_addr` is a pointer to valid memory.
        unsafe {
            let ptr = (self.virt_addr as *mut u8).add(self.offset);
            core::slice::from_raw_parts_mut(ptr, self.len)
        }
    }
}

impl fmt::Debug for BioVec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BioVec")
            .field("virt_addr", &self.virt_addr)
            .field("offset", &self.offset)
            .field("len", &self.len)
            .finish()
    }
}

/// Wrap the bio operations (see `include/linux/blk_types.h`).
#[repr(u8)]
#[derive(Clone, Copy, Debug)]
pub(crate) enum BioOp {
    Read,
    Write,
    Flush,
    Discard,
    Undefined,
}

impl From<u8> for BioOp {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Read,
            1 => Self::Write,
            2 => Self::Flush,
            3 => Self::Discard,
            _ => Self::Undefined,
        }
    }
}
