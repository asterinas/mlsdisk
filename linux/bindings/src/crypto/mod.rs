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

pub use aead::{Cipher as Aead, Request as AeadReq};
pub use skcipher::{Cipher as Skcipher, Request as SkcipherReq};

/// Produces a pointer to an object from a pointer to one of its fields.
///
/// # Safety
///
/// Callers must ensure that the pointer to the field is in fact a pointer to the specified field,
/// as opposed to a pointer to another object of the same type. If this condition is not met,
/// any dereference of the resulting pointer is UB.
///
/// # Examples
///
/// ```
/// # use crate::container_of;
/// struct Test {
///     a: u64,
///     b: u32,
/// }
///
/// let test = Test { a: 10, b: 20 };
/// let b_ptr = &test.b;
/// let test_alias = container_of!(b_ptr, Test, b);
/// assert!(core::ptr::eq(&test, test_alias));
/// ```
#[macro_export]
macro_rules! container_of {
    ($ptr:expr, $type:ty, $($f:tt)*) => {{
        let ptr = $ptr as *const _ as *const u8;
        let offset = core::mem::offset_of!($type, $($f)*);
        ptr.wrapping_sub(offset) as *const $type
    }}
}

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
