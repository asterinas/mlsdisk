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

mod bindings_raw {
    // Use glob import here to expose all helpers.
    // Symbols defined within the module will take precedence to the glob import.
    pub use super::bindings_helper::*;
    include!("./bindings_generated.rs");
}

// When both a directly exposed symbol and a helper exists for the same function,
// the directly exposed symbol is preferred and the helper becomes dead code, so
// ignore the warning here.
#[allow(dead_code)]
mod bindings_helper {
    // Import the generated bindings for types.
    use super::bindings_raw::*;
    include!("./bindings_helpers_generated.rs");
}

pub mod crypto;
pub mod sync;
pub mod thread;

pub use bindings_raw::*;
