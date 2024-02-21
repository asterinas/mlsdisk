// SPDX-License-Identifier: GPL-2.0
// Copyright (c) 2023 Ant Group CO., Ltd.

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

        test_rwlock();
        test_weak();
        test_thread();

        Ok(Dummy)
    }
}

impl Drop for Dummy {
    fn drop(&mut self) {
        pr_info!("Rust dm_sworndisk module (exit)\n");
    }
}

fn test_rwlock() {
    use bindings::new_rwlock;

    let lock = Box::pin_init(new_rwlock!(5)).unwrap();
    // Many reader locks can be held at once.
    {
        let r1 = lock.read();
        let r2 = lock.read();
        assert_eq!(*r1, 5);
        assert_eq!(*r2, 5);
    } // read locks are dropped at this point

    // Only one write lock may be held.
    {
        let mut w = lock.write();
        *w += 1;
        assert_eq!(*w, 6);
        let r = lock.try_read();
        assert_eq!(r.is_ok(), false);
    } // write lock is dropped here

    // Try to get a read lock.
    let r = lock.try_read();
    assert_eq!(r.is_ok(), true);
    assert_eq!(*r.unwrap(), 6);
}

fn test_weak() {
    use bindings::sync::{Arc, Weak};

    struct Example {
        a: u32,
        b: u32,
    }

    // Create a `Arc` instance of `Example`.
    let obj = Arc::try_new(Example { a: 10, b: 20 }).unwrap();

    // Get a weak reference to `obj` and increment the weak refcount.
    let weak = Arc::downgrade(&obj);
    assert_eq!(Weak::strong_count(&weak), 1);
    assert_eq!(Weak::weak_count(&weak), 1);

    // Attempts to upgrade the `Weak` pointer.
    let upgrade = weak.upgrade();
    assert_eq!(upgrade.is_some(), true);
    let upgrade = upgrade.unwrap();
    assert_eq!(Weak::strong_count(&weak), 2);
    assert_eq!(Weak::weak_count(&weak), 1);

    // Drop `obj` and decrement its refcount. The values are still accessible
    // through `upgrade`.
    drop(obj);
    assert_eq!(upgrade.a, 10);
    assert_eq!(upgrade.b, 20);

    drop(upgrade);
    let upgrade = weak.upgrade();
    assert_eq!(upgrade.is_some(), false);
}

fn test_thread() {
    use bindings::thread::{spawn, JoinHandle, Thread};
    use core::time::Duration;

    let t = spawn(|| {
        for i in 0..5 {
            Thread::sleep(Duration::from_secs(1));
            pr_info!("never running: {i}");
        }
        21
    });
    drop(t);

    let t = spawn(|| {
        for i in 0..5 {
            Thread::sleep(Duration::from_secs(1));
            pr_info!("should not running: {i}");
        }
        21
    });
    let r = t.join();
    assert_eq!(r, Err(ESRCH));

    let t = spawn(|| {
        for i in 0..5 {
            Thread::sleep(Duration::from_secs(1));
            pr_info!("spawn running: {i}");
        }
        21
    });
    Thread::sleep(Duration::from_secs(10));
    let r = t.join();
    assert_eq!(r, Ok(21));
}
