// SPDX-License-Identifier: GPL-2.0
// Copyright (c) 2023 Ant Group CO., Ltd.

//! Block device.

use core::{ops::Range, ptr::NonNull};
use kernel::{prelude::*, types::Opaque};

pub const BLOCK_SIZE: usize = 1 << bindings::PAGE_SHIFT;
pub const BLOCK_SECTORS: usize = bindings::PAGE_SECTORS as usize;

/// A wrapper for kernel's struct `block_device`.
pub struct BlockDevice {
    inner: NonNull<bindings::block_device>,
}

// See definitions at `include/linux/blkdev.h`.
const BLK_OPEN_READ: bindings::blk_mode_t = 1 << 0;
const BLK_OPEN_WRITE: bindings::blk_mode_t = 1 << 1;

impl BlockDevice {
    /// Constructs a `BlockDevice` by path.
    pub fn open(path: &CStr) -> Result<Self> {
        let dev_ptr = unsafe {
            bindings::blkdev_get_by_path(
                path.as_char_ptr(),
                BLK_OPEN_READ | BLK_OPEN_WRITE,
                core::ptr::null_mut(),
                core::ptr::null(),
            )
        };
        if unsafe { kernel::bindings::IS_ERR(dev_ptr as _) } {
            return Err(ENODEV);
        }
        Ok(Self {
            inner: unsafe { NonNull::new_unchecked(dev_ptr) },
        })
    }

    /// Returns the raw pointer to kernel's struct `block_device`.
    pub fn as_ptr(&self) -> *mut bindings::block_device {
        self.inner.as_ptr()
    }

    /// Returns the block range of the `block_device`.
    pub fn region(&self) -> Range<usize> {
        // SAFETY: `self.0` is borrowed from foreign pointer, should be valid.
        let start_sector = unsafe { (*self.as_ptr()).bd_start_sect as usize };
        let nr_sectors = unsafe { (*self.as_ptr()).bd_nr_sectors as usize };
        let end_sector = start_sector + nr_sectors;
        let sectors_per_block = BLOCK_SIZE / (bindings::SECTOR_SIZE as usize);

        Range {
            start: (start_sector + sectors_per_block - 1) / sectors_per_block,
            end: end_sector / sectors_per_block,
        }
    }
}

impl Drop for BlockDevice {
    fn drop(&mut self) {
        unsafe { bindings::blkdev_put(self.as_ptr(), core::ptr::null_mut()) }
    }
}

/// Wrap the block error status values (see [blk_status_t]).
///
/// [`blk_status_t`]: include/linux/blk_types.h
#[allow(missing_docs)]
#[repr(u32)]
#[derive(Clone, Copy, Debug)]
pub enum BlkStatus {
    Ok,
    NotSupp,
    TimeOut,
    NoSpc,
    Transport,
    Target,
    Nexus,
    Medium,
    Protection,
    Resource,
    IoErr,
    DmRequeue,
    Again,
    DevResource,
    ZoneResource,
    ZoneOpenResource,
    ZoneActiveResource,
    Offline,
    Undefined,
}

impl From<u32> for BlkStatus {
    fn from(value: u32) -> Self {
        match value {
            0 => Self::Ok,
            1 => Self::NotSupp,
            2 => Self::TimeOut,
            3 => Self::NoSpc,
            4 => Self::Transport,
            5 => Self::Target,
            6 => Self::Nexus,
            7 => Self::Medium,
            8 => Self::Protection,
            9 => Self::Resource,
            10 => Self::IoErr,
            11 => Self::DmRequeue,
            12 => Self::Again,
            13 => Self::DevResource,
            14 => Self::ZoneResource,
            15 => Self::ZoneOpenResource,
            16 => Self::ZoneActiveResource,
            17 => Self::Offline,
            _ => Self::Undefined,
        }
    }
}
