// SPDX-License-Identifier: GPL-2.0
// Copyright (c) 2023 Ant Group CO., Ltd.

//! Block device.

use core::{fmt, ops::Range, ptr::NonNull};
use kernel::prelude::*;

pub(crate) const BLOCK_SIZE: usize = 1 << bindings::PAGE_SHIFT;
pub(crate) const BLOCK_SECTORS: usize = bindings::PAGE_SECTORS as usize;

/// A handle to kernel's struct `block_device`.
pub(crate) struct HostBlockDevice {
    inner: NonNull<bindings::block_device>,
}

// SAFETY: we get a reference to kernel's `block_device` when `open`, so it won't
// disappear and could be safely transferred across threads.
unsafe impl Send for HostBlockDevice {}

// SAFETY: it's only used to read available block range of the host `block_device`,
// so it could be accessed concurrently from multiple threads.
unsafe impl Sync for HostBlockDevice {}

// See definitions at `include/linux/blkdev.h`.
const BLK_OPEN_READ: bindings::blk_mode_t = 1 << 0;
const BLK_OPEN_WRITE: bindings::blk_mode_t = 1 << 1;

impl HostBlockDevice {
    /// Constructs a `HostBlockDevice` by path.
    pub fn open(path: &CStr) -> Result<Self> {
        // SAFETY: `path` is valid C string, `blkdev_get_by_path` would return
        // a pointer to `block_device`, or an `ERR_PTR` (non-null) on failure .
        let dev_ptr = unsafe {
            bindings::blkdev_get_by_path(
                path.as_char_ptr(),
                BLK_OPEN_READ | BLK_OPEN_WRITE,
                core::ptr::null_mut(),
                core::ptr::null(),
            )
        };

        // SAFETY: no safety requirement on this FFI call.
        if unsafe { kernel::bindings::IS_ERR(dev_ptr as _) } {
            return Err(ENODEV);
        }

        // SAFETY: `dev_ptr` is checked above, it is valid.
        Ok(Self {
            inner: unsafe { NonNull::new_unchecked(dev_ptr) },
        })
    }

    /// Returns the raw pointer to kernel's struct `block_device`.
    pub fn as_ptr(&self) -> *mut bindings::block_device {
        self.inner.as_ptr()
    }

    /// Returns the block range of the host `block_device`.
    pub fn region(&self) -> Range<usize> {
        // SAFETY: `self.inner` is valid `block_device`.
        let start_sector = unsafe { (*self.inner.as_ptr()).bd_start_sect as usize };
        let nr_sectors = unsafe { (*self.inner.as_ptr()).bd_nr_sectors as usize };
        let end_sector = start_sector + nr_sectors;

        // Align up the `start_sector`, and align down the `end_sector`.
        Range {
            start: (start_sector + BLOCK_SECTORS - 1) / BLOCK_SECTORS,
            end: end_sector / BLOCK_SECTORS,
        }
    }
}

impl fmt::Debug for HostBlockDevice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostBlockDevice")
            .field("region", &self.region())
            .finish()
    }
}

impl Drop for HostBlockDevice {
    fn drop(&mut self) {
        // SAFETY: `self.inner` is a valid `block_device`, we get a reference
        // to it when `open`, now put it here.
        unsafe { bindings::blkdev_put(self.inner.as_ptr(), core::ptr::null_mut()) }
    }
}
