// SPDX-License-Identifier: GPL-2.0
// Copyright (c) 2023 Rust-for-Linux
// Copyright (c) 2023 Ant Group CO., Ltd.

//! Extra kernel Bindings.
//!
//! Contains the generated bindings by `bindgen`.
//!
//! If you need a kernel C API that is not ported or wrapped in the `kernel` crate,
//! then you could use this crate to add bindings for it.

#![no_std]
#![cfg_attr(test, allow(deref_nullptr))]
#![cfg_attr(test, allow(unaligned_references))]
#![cfg_attr(test, allow(unsafe_op_in_unsafe_fn))]
#![allow(
    clippy::all,
    missing_docs,
    non_camel_case_types,
    non_upper_case_globals,
    non_snake_case,
    improper_ctypes,
    unreachable_pub,
    unsafe_op_in_unsafe_fn,
    unused
)]
#![feature(allocator_api)]
#![feature(coerce_unsized)]
#![feature(dispatch_from_dyn)]
#![feature(layout_for_ptr)]
#![feature(offset_of)]
#![feature(receiver_trait)]
#![feature(unsize)]

mod bindings_generated {
    include!("./bindings_generated.rs");
    include!("./bindings_helpers_generated.rs");
}

pub mod crypto;
pub mod sync;
pub mod thread;

// Use glob import here to expose all generated bindings for types.
// Symbols defined within the module will take precedence to the glob import.
pub use bindings_generated::*;

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
