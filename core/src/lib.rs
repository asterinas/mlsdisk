// #![no_std]
#![feature(allocator_api)]
#![feature(coerce_unsized)]
#![feature(dispatch_from_dyn)]
#![feature(fn_traits)]
#![feature(is_sorted)]
#![feature(let_chains)]
#![feature(negative_impls)]
#![feature(new_uninit)]
#![feature(receiver_trait)]
#![feature(slice_concat_trait)]
#![feature(slice_group_by)]
#![feature(tuple_trait)]
#![feature(unboxed_closures)]
#![feature(unsize)]

mod error;
mod layers;
mod os;
mod prelude;
mod tx;
mod util;

extern crate alloc;
