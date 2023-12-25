// #![no_std]
#![feature(is_sorted)]
#![feature(let_chains)]
#![feature(negative_impls)]
#![feature(new_uninit)]
#![feature(slice_concat_trait)]
#![feature(slice_group_by)]

mod error;
mod layers;
mod os;
mod prelude;
mod tx;
mod util;

extern crate alloc;
