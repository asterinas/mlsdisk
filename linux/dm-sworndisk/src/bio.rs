// SPDX-License-Identifier: GPL-2.0
// Copyright (c) 2023 Ant Group CO., Ltd.

//! Block I/O.

use core::{
    ops::{Deref, DerefMut, Range},
    ptr::NonNull,
};
use kernel::{prelude::*, types::Opaque};

use super::block_device::{BlockDevice, BLOCK_SECTORS, BLOCK_SIZE};

const GFP_NOIO: bindings::gfp_t = bindings::___GFP_DIRECT_RECLAIM | bindings::___GFP_KSWAPD_RECLAIM;

/// A wrapper for kernel's struct `bio`.
pub struct Bio {
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
        // Get a reference to a `bio`, so it won't disappear. And `bio_put` is
        // called when it's been dropped.
        bindings::bio_get(ptr);
        Self {
            inner: NonNull::new_unchecked(ptr),
        }
    }

    /// Allocates a bio for the target `BlockDevice`.
    ///
    /// The bio operation is specified by `op`, targeting a given block region,
    /// while its I/O buffer is configured by `BioVec`.
    pub fn alloc(
        bdev: &BlockDevice,
        op: BioOp,
        region: Range<usize>,
        io_buffer: &[BioVec],
    ) -> Result<Self> {
        // SAFETY: no safety requirements on this FFI call.
        let bio = unsafe {
            bindings::bio_alloc_bioset(
                bdev.as_ptr(),
                io_buffer.len() as _,
                op as _,
                GFP_NOIO,
                &mut bindings::fs_bio_set,
            )
        };
        if bio.is_null() {
            return Err(ENOMEM);
        }

        let mut buf_len = 0usize;
        for bio_vec in io_buffer {
            // SAFETY: the `bio` is non-null and valid (checked above), and `bio_vecs`
            // contains valid pages.
            unsafe {
                let page = bindings::virt_to_page(bio_vec.ptr as _);
                bindings::__bio_add_page(bio, page, bio_vec.len as _, bio_vec.offset as _);
                buf_len += bio_vec.len;
            }
        }

        let start_sector = region.start * BLOCK_SECTORS;
        let nr_bytes = region.len() * BLOCK_SIZE;
        if nr_bytes != buf_len {
            // SAFETY: the `bio` is non-null and valid. And the 'bio_alloc_bioset'
            // method sets the reference count, so we call `bio_put` to drop it here.
            unsafe { bindings::bio_put(bio) };
            return Err(EINVAL);
        }
        // SAFETY: the `bio` is non-null and valid.
        unsafe {
            (*bio).bi_iter.bi_sector = start_sector as _;
            (*bio).bi_iter.bi_size = nr_bytes as _;
        }

        // SAFETY: the `bio` is non-null and valid, and the 'bio_alloc_bioset' method
        // sets the reference count, so we don't need to call `bio_get` here.
        Ok(Self {
            inner: unsafe { NonNull::new_unchecked(bio) },
        })
    }

    // Clones a bio that shares the original bio's I/O buffer.
    pub fn alloc_clone(&self) -> Result<Self> {
        let src = self.inner.as_ptr();
        // SAFETY: the `src.bi_bdev` is a valid `block_device`.
        let cloned = unsafe {
            bindings::bio_alloc_clone((*src).bi_bdev, src, GFP_NOIO, &mut bindings::fs_bio_set)
        };
        if cloned.is_null() {
            return Err(ENOMEM);
        }

        // SAFETY: the `cloned` is non-null and valid, and the 'bio_alloc_clone' method
        // sets the reference count, so we don't need to call `bio_get` here.
        Ok(Self {
            inner: unsafe { NonNull::new_unchecked(cloned) },
        })
    }

    /// Gets the operation and flags. The bottom 8 bits are encoding the operation,
    /// and the remaining 24 for flags.
    pub fn opf(&self) -> u32 {
        // SAFETY: `self.inner` is a valid bio.
        unsafe { (*self.inner.as_ptr()).bi_opf }
    }

    /// Sets the operation and flags.
    pub fn set_opf(&mut self, opf: u32) {
        // SAFETY: `self.inner` is a valid bio.
        unsafe { (*self.inner.as_ptr()).bi_opf = opf }
    }

    /// Returns the operation of the `Bio`.
    pub fn op(&self) -> BioOp {
        BioOp::from(self.opf() as u8)
    }

    /// Returns true if this bio carries any data.
    pub fn has_data(&self) -> bool {
        // SAFETY: `self.inner` is a valid bio.
        unsafe { bindings::bio_has_data(self.inner.as_ptr()) }
    }

    /// Sets the block device of bio request (remap the request).
    pub fn set_dev(&mut self, bdev: &BlockDevice) {
        // SAFETY: `self.inner` is a valid bio.
        unsafe { bindings::bio_set_dev(self.inner.as_ptr(), bdev.as_ptr()) }
    }

    /// Returns the start sector of the bio.
    pub fn start_sector(&self) -> usize {
        // SAFETY: `self.inner` is a valid bio.
        unsafe { (*self.inner.as_ptr()).bi_iter.bi_sector as _ }
    }

    /// Sets the start sector of the bio (remap the request).
    pub fn set_start_sector(&mut self, sector: usize) {
        // SAFETY: `self.inner` is a valid bio.
        unsafe { (*self.inner.as_ptr()).bi_iter.bi_sector = sector as _ }
    }

    /// Returns the length in bytes of the bio.
    pub fn len(&self) -> usize {
        // SAFETY: `self.inner` is a valid bio.
        unsafe { (*self.inner.as_ptr()).bi_iter.bi_size as _ }
    }

    unsafe extern "C" fn bi_end_io<T, F>(bio: *mut bindings::bio)
    where
        F: FnOnce(&mut Bio, T),
        F: Send,
        T: Send,
    {
        let mut item = Box::from_raw((*bio).bi_private as *mut CallbackItem<T>);
        (item.func)(&mut item.base, item.data);
        item.base.end();
    }

    /// Set the callback that will be called through the `bi_end_io` method.
    pub fn set_callback<T, F>(self, data: T, func: F) -> Result<Self>
    where
        F: FnOnce(&mut Bio, T) + 'static,
        F: Send,
        T: Send,
    {
        let cloned = self.alloc_clone()?;
        let item = CallbackItem::try_new(self, data, func)?;
        let bio = cloned.inner.as_ptr();

        // SAFETY: `cloned.inner` is a valid bio.
        unsafe {
            (*bio).bi_private = Box::into_raw(item) as _;
            (*bio).bi_end_io = Some(Bio::bi_end_io::<T, F>);
        }
        Ok(cloned)
    }

    /// Submits the bio.
    ///
    /// # Panics
    ///
    /// User should ensure that this is an unaltered `from_raw` bio,
    /// or an owned bio returned by `set_callback`.
    ///
    /// The success/failure status of the request, along with notification of
    /// completion, is delivered asynchronously through the `bi_end_io` callback
    /// in bio. The bio must NOT be touched by the caller until ->bi_end_io()
    /// has been called.
    pub fn submit(&self) {
        // SAFETY: `self.inner` is a valid bio.
        unsafe { bindings::submit_bio(self.inner.as_ptr()) }
    }

    /// Submits a bio (synchronously).
    ///
    /// # Panics
    ///
    /// User must ensure that this is an owned bio without a `bi_end_io` callback,
    /// either one you have gotten with `alloc`, or `alloc_clone`.
    pub fn submit_sync(&self) -> Result<()> {
        // SAFETY: `self.inner` is a valid bio.
        let err = unsafe { bindings::submit_bio_wait(self.inner.as_ptr()) };
        kernel::error::to_result(err)
    }

    /// Ends the bio.
    ///
    /// This will end I/O on the whole bio. No one should call bi_end_io()
    /// directly on a kernel's `bio` unless they own it and thus know that
    /// it has an end_io function.
    pub fn end(&self) {
        // SAFETY: `self.inner` is a valid bio.
        unsafe { bindings::bio_endio(self.inner.as_ptr()) }
    }

    /// Returns an iterator on the bio_vec.
    pub fn iter(&self) -> BioVecIter {
        let bio = self.inner.as_ptr();
        // SAFETY: `self.inner` is a valid bio.
        unsafe {
            let vec = (*bio).bi_io_vec;
            let iter = core::ptr::addr_of_mut!((*bio).bi_iter);
            BioVecIter::from_raw(vec, iter)
        }
    }
}

impl Drop for Bio {
    fn drop(&mut self) {
        // SAFETY: `self.inner` is a valid bio.
        unsafe {
            // Put a reference to a kernel's `bio`, either one you have gotten
            // with `alloc`, `bio_get` or `alloc_clone`. The last put of a bio
            // will free it.
            bindings::bio_put(self.inner.as_ptr());
        }
    }
}

/// An iterator on `BioVec`.
pub struct BioVecIter {
    iter: Opaque<bindings::bvec_iter>,
    vec: NonNull<bindings::bio_vec>,
}

impl BioVecIter {
    /// Constructs a `BioVecIter` from raw pointers.
    ///
    /// # Safety
    ///
    /// Caller must provide valid pointers to the `bvec_iter` and `bio_vec`
    /// of a valid `bio`.
    pub unsafe fn from_raw(vec: *mut bindings::bio_vec, iter: *mut bindings::bvec_iter) -> Self {
        let opaque = Opaque::new(bindings::bvec_iter::default());
        *opaque.get() = *iter;
        Self {
            iter: opaque,
            vec: NonNull::new_unchecked(vec),
        }
    }

    /// Returns true if the iterator has next `BioVec`.
    fn has_next(&self) -> bool {
        unsafe { (*self.iter.get()).bi_size > 0 }
    }

    /// Returns the current item of the iterator.
    fn item(&self) -> BioVec {
        unsafe {
            let expect_len = (*self.iter.get()).bi_size as usize;
            let idx = (*self.iter.get()).bi_idx as usize;
            let next = self.vec.as_ptr().add(idx);
            BioVec::from_bio_vec(next, expect_len)
        }
    }

    /// Advances the iterator with current `BioVec`.
    fn advance_by(&mut self, vec: &BioVec) {
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
        let item = self.item();
        self.advance_by(&item);
        Some(item)
    }
}

/// A contiguous range of physical memory addresses.
///
/// Used to iterate the I/O buffer in `Bio`.
pub struct BioVec {
    ptr: usize,
    offset: usize,
    len: usize,
}

impl BioVec {
    /// Constructs a `BioVec` from kernel's `bio_vec`.
    ///
    /// # Safety
    ///
    /// User must provide a valid pointer to kernel's `bio_vec`.
    pub unsafe fn from_bio_vec(vec: *mut bindings::bio_vec, expect_len: usize) -> Self {
        Self {
            ptr: bindings::page_to_virt((*vec).bv_page) as _,
            offset: (*vec).bv_offset as _,
            len: expect_len.min((*vec).bv_len as _),
        }
    }

    /// Constructs a `BioVec` from raw pointer.
    ///
    /// # Safety
    ///
    /// User must ensure that `ptr` points to a valid and directly mapped page.
    pub unsafe fn from_ptr(ptr: usize, offset: usize, len: usize) -> Self {
        Self { ptr, offset, len }
    }

    /// Returns the length of `BioVec`.
    pub fn len(&self) -> usize {
        self.len
    }
}

impl Deref for BioVec {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        unsafe {
            let ptr = (self.ptr as *mut u8).add(self.offset);
            core::slice::from_raw_parts(ptr, self.len)
        }
    }
}

impl DerefMut for BioVec {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            let ptr = (self.ptr as *mut u8).add(self.offset);
            core::slice::from_raw_parts_mut(ptr, self.len)
        }
    }
}

/// Wrap the bio operations (see [req_op]).
///
/// [`req_op`]: include/linux/blk_types.h
#[allow(missing_docs)]
#[repr(u8)]
#[derive(Clone, Copy, Debug)]
pub enum BioOp {
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

/// A struct to carry the data for `bi_end_io`.
struct CallbackItem<T> {
    base: Bio,
    data: T,
    func: Box<dyn FnOnce(&mut Bio, T)>,
}

impl<T> CallbackItem<T> {
    pub fn try_new<F>(base: Bio, data: T, func: F) -> Result<Box<Self>>
    where
        F: FnOnce(&mut Bio, T) + 'static,
        F: Send,
        T: Send,
    {
        let func = Box::try_new(func)?;
        Ok(Box::try_new(Self { base, data, func })?)
    }
}
