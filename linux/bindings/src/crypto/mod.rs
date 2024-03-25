// SPDX-License-Identifier: GPL-2.0
// Copyright (c) 2023 Ant Group CO., Ltd.

//! Crypto API.
//!
//! C header: [`include/linux/crypto.h`](include/linux/crypto.h)
//!
//! Reference: <https://www.kernel.org/doc/html/latest/crypto/index.html>

use kernel::{bindings::GFP_KERNEL, prelude::*, types::Opaque};

mod aead;
mod skcipher;

pub use aead::{Aead, AeadReq};
pub use skcipher::{Skcipher, SkcipherReq};

/// A wrapper for kernel's struct `sg_table`.
#[repr(transparent)]
struct SgTable(Opaque<crate::sg_table>);

impl SgTable {
    /// Allocates a scatterlist table with `nents` entries.
    pub fn alloc(nents: usize) -> Result<Self> {
        let mut table = crate::sg_table::default();
        // SAFETY: `table` is non-null and valid.
        unsafe {
            kernel::error::to_result(crate::sg_alloc_table(
                core::ptr::addr_of_mut!(table),
                nents as u32,
                GFP_KERNEL,
            ))?;
        }
        Ok(SgTable(Opaque::new(table)))
    }

    /// Sets the memory buffer of `index` in the table.
    ///
    /// # Safety
    ///
    /// The `index` should not exceed the number of the table entries.
    /// And `buf` must be valid until the `SgTable` is destroyed.
    pub unsafe fn set_buf(&mut self, index: usize, buf: &[u8]) {
        let sgl_ptr = (*self.0.get()).sgl.add(index);
        crate::sg_set_buf(sgl_ptr, buf.as_ptr() as _, buf.len() as _);
    }
}

impl Drop for SgTable {
    fn drop(&mut self) {
        // SAFETY: `self.0` is non-null and valid.
        unsafe { crate::sg_free_table(self.0.get()) }
    }
}
