#![cfg_attr(not(feature = "std"), no_std)]
#![feature(allocator_api)]
#![feature(anonymous_lifetime_in_impl_trait)]
#![feature(coerce_unsized)]
#![feature(dispatch_from_dyn)]
#![feature(fn_traits)]
#![feature(int_log)]
#![feature(is_some_and)]
#![feature(is_sorted)]
#![feature(let_chains)]
#![feature(negative_impls)]
#![feature(new_uninit)]
#![feature(receiver_trait)]
#![feature(sized_type_properties)]
#![feature(slice_concat_trait)]
#![feature(slice_group_by)]
#![feature(slice_internals)]
#![feature(tuple_trait)]
#![feature(unboxed_closures)]
#![feature(unsize)]
#![allow(
    dead_code,
    internal_features,
    stable_features,
    unknown_lints,
    unused_imports
)]

mod error;
mod layers;
mod os;
mod prelude;
mod tx;
mod util;

#[cfg(not(feature = "occlum"))]
extern crate alloc;

#[cfg(feature = "linux")]
pub use self::os::{Arc, Mutex, Vec};

#[cfg(feature = "occlum")]
#[macro_use]
extern crate sgx_tstd;

pub use self::error::{Errno, Error};
pub use self::layers::bio::{BlockId, BlockSet, Buf, BufMut, BufRef, BLOCK_SIZE};
pub use self::layers::disk::SwornDisk;
pub use self::os::{Aead, AeadKey, Rng};
pub use self::util::{Aead as _, RandomInit, Rng as _};
