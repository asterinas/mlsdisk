// SPDX-License-Identifier: GPL-2.0

//! Dummy dm-sworndisk module.

use kernel::prelude::*;

module! {
    type: Dummy,
    name: "dm_sworndisk",
    author: "Rust for Linux Contributors",
    description: "Rust dm_sworndisk module",
    license: "GPL",
}

struct Dummy;

impl kernel::Module for Dummy {
    fn init(_module: &'static ThisModule) -> Result<Self> {
        pr_info!("Rust dm_sworndisk module (init)\n");
        Ok(Dummy)
    }
}

impl Drop for Dummy {
    fn drop(&mut self) {
        pr_info!("Rust dm_sworndisk module (exit)\n");
    }
}
