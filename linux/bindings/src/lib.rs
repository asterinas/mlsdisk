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
#![feature(receiver_trait)]
#![feature(unsize)]

mod bindings_generated {
    include!("./bindings_generated.rs");
    include!("./bindings_helpers_generated.rs");
}

pub mod sync;
pub mod thread;

// Use glob import here to expose all generated bindings for types.
// Symbols defined within the module will take precedence to the glob import.
pub use bindings_generated::*;
