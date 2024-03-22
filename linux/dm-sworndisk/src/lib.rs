// SPDX-License-Identifier: GPL-2.0
// Copyright (c) 2023 Ant Group CO., Ltd.

//! Dummy dm-sworndisk module.

#![feature(allocator_api)]

mod bio;
mod block_device;
mod device_mapper;

use bindings::thread::{spawn, JoinHandle};
use core::{
    ops::{Deref, DerefMut, Range},
    sync::atomic::{AtomicBool, Ordering},
};
use crossbeam_queue::SegQueue;
use kernel::{c_str, new_condvar, prelude::*, sync::CondVar};
use sworndisk::{Arc, BlockId, BlockSet, BufMut, BufRef, Errno, Error, Mutex};

use crate::bio::{Bio, BioOp, BioVec};
use crate::block_device::{HostBlockDevice, BLOCK_SECTORS, BLOCK_SIZE};
use crate::device_mapper::{Args, DmTargetOps, MapState, Target, TargetType};

module! {
    type: TargetManager,
    name: "dm_sworndisk",
    author: "Rust for Linux Contributors",
    description: "Rust dm_sworndisk module",
    license: "GPL",
}

/// A struct used to manage a `TargetType`.
///
/// Register the type to kernel when `init` the module, and `unregister` it
/// when `drop` (automatically).
struct TargetManager(Pin<Box<TargetType>>);

impl kernel::Module for TargetManager {
    fn init(_module: &'static ThisModule) -> Result<Self> {
        pr_info!("Rust dm_sworndisk module (init)\n");

        // TODO: add a switch to enable those tests.
        // test_rwlock();
        // test_weak();
        // test_thread();

        let dm_sworndisk = Box::pin_init(TargetType::register::<DmSwornDisk>(
            c_str!("sworndisk"),
            [0, 0, 1],
            0,
        ))?;

        Ok(TargetManager(dm_sworndisk))
    }
}

impl Drop for TargetManager {
    fn drop(&mut self) {
        pr_info!("Rust dm_sworndisk module (exit)\n");
    }
}

/// A request queue, dispatching bios from device mapper to `RawDisk`.
struct ReqQueue {
    bios: Mutex<SegQueue<Bio>>,
    // TODO: replace `RawDisk` with `SwornDisk`.
    disk: RawDisk,
    should_stop: AtomicBool,
    new_bio_condvar: Pin<Box<CondVar>>,
}

impl ReqQueue {
    /// Constructs a `ReqQueue`.
    pub fn new(disk: RawDisk) -> Self {
        Self {
            bios: Mutex::new(SegQueue::new()),
            disk,
            should_stop: AtomicBool::new(false),
            new_bio_condvar: Box::pin_init(new_condvar!()).unwrap(),
        }
    }

    /// Returns true if the device mapper is going to exit.
    fn should_stop(&self) -> bool {
        self.should_stop.load(Ordering::Acquire)
    }

    /// Set the `should_stop` flag.
    ///
    /// If the device mapper target is going to exit, user should call this
    /// method, in order to tell the `ReqQueue` worker thread to clear all
    /// the pending bios.
    pub fn set_stopped(&self) {
        self.should_stop.store(true, Ordering::Release);
        self.new_bio_condvar.notify_all();
    }

    /// Enqueues a `Bio`.
    pub fn enqueue(&self, bio: Bio) {
        self.bios.lock().push(bio);
        self.new_bio_condvar.notify_one();
    }

    /// Spawns a `JoinHandle` to deal with bio requests in the queue.
    pub fn spawn_req_worker(queue: Arc<Self>) -> JoinHandle<()> {
        spawn(move || {
            while !queue.should_stop() {
                let mut bios = queue.bios.lock();
                let Some(bio) = bios.pop() else {
                    let _ = queue.new_bio_condvar.wait(&mut bios);
                    continue;
                };
                drop(bios);

                queue.process(bio);
            }
            queue.clear();
        })
    }

    /// Dispatches the `Bio` from device mapper to `RawDisk`.
    fn process(&self, bio: Bio) {
        if bio.start_sector() % BLOCK_SECTORS != 0 || bio.len() % BLOCK_SIZE != 0 {
            pr_warn!(
                "bio not aligned to BLOCK_SIZE, start_sector: {}, len: {}",
                bio.start_sector(),
                bio.len(),
            );
            bio.end();
            return;
        }

        match bio.op() {
            BioOp::Read => {
                let mut pos = bio.start_sector() / BLOCK_SECTORS;
                for mut bio_vec in bio.iter() {
                    let buf = BufMut::try_from(bio_vec.deref_mut()).unwrap();
                    let nblocks = buf.nblocks();
                    if let Err(err) = self.disk.read(pos, buf) {
                        pr_info!(
                            "read sworndisk failed, block_id: {}, nblocks: {}, err: {:?}",
                            pos,
                            nblocks,
                            err,
                        );
                    }
                    pos += nblocks;
                }
            }
            BioOp::Write => {
                let mut pos = bio.start_sector() / BLOCK_SECTORS;
                for bio_vec in bio.iter() {
                    let buf = BufRef::try_from(bio_vec.deref()).unwrap();
                    let nblocks = buf.nblocks();
                    if let Err(err) = self.disk.write(pos, buf) {
                        pr_info!(
                            "write sworndisk failed, block_id: {}, nblocks: {}, err: {:?}",
                            pos,
                            nblocks,
                            err,
                        );
                    }
                    pos += nblocks;
                }
            }
            _ => unreachable!(),
        }
        bio.end();
    }

    /// Processes all the pending `Bio`s in the queue.
    fn clear(&self) {
        while let Some(bio) = self.bios.lock().pop() {
            self.process(bio);
        }
    }
}

/// A struct represent a `dm_target` type, which should impl `DmTargetOps`.
struct DmSwornDisk {
    queue: Arc<ReqQueue>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl DmSwornDisk {
    /// Returns an in-place initializer.
    fn new(queue: Arc<ReqQueue>, worker: JoinHandle<()>) -> impl Init<Self> {
        init!(Self {
            queue,
            worker: Mutex::new(Some(worker))
        })
    }
}

/// A struct to deal with host I/O, which should impl `BlockSet`.
struct RawDisk {
    device: Arc<Mutex<HostBlockDevice>>,
    region: Range<BlockId>,
}

impl RawDisk {
    /// Constructs a `RawDisk`.
    fn open(path: &CStr) -> Result<Self> {
        let block_device = HostBlockDevice::open(path)?;
        let region = block_device.region();
        Ok(Self {
            device: Arc::new(Mutex::new(block_device)),
            region,
        })
    }
}

impl BlockSet for RawDisk {
    fn read(&self, pos: BlockId, buf: BufMut) -> Result<(), Error> {
        if pos + buf.nblocks() > self.region.end {
            return Err(Error::with_msg(
                Errno::InvalidArgs,
                "read position is out of range",
            ));
        }

        let region = Range {
            start: self.region.start + pos,
            end: self.region.start + pos + buf.nblocks(),
        };

        // SAFETY: `buf` contains valid memory buffer.
        let bio_vec = unsafe {
            let slice = buf.as_slice();
            BioVec::from_raw(slice.as_ptr() as _, 0, slice.len())
        };

        let device = self.device.lock();
        let bio = Bio::alloc(&device, BioOp::Read, region, &[bio_vec])
            .map_err(|_| Error::with_msg(Errno::OutOfMemory, "alloc read bio failed"))?;
        bio.submit_sync()
            .map_err(|_| Error::with_msg(Errno::IoFailed, "read raw disk failed"))
    }

    fn write(&self, pos: BlockId, buf: BufRef) -> Result<(), Error> {
        if pos + buf.nblocks() > self.region.end {
            return Err(Error::with_msg(
                Errno::InvalidArgs,
                "write position is out of range",
            ));
        }

        let region = Range {
            start: self.region.start + pos,
            end: self.region.start + pos + buf.nblocks(),
        };

        // SAFETY: `buf` contains valid memory buffer.
        let bio_vec = unsafe {
            let slice = buf.as_slice();
            BioVec::from_raw(slice.as_ptr() as _, 0, slice.len())
        };

        let device = self.device.lock();
        let bio = Bio::alloc(&device, BioOp::Write, region, &[bio_vec])
            .map_err(|_| Error::with_msg(Errno::OutOfMemory, "alloc write bio failed"))?;
        bio.submit_sync()
            .map_err(|_| Error::with_msg(Errno::IoFailed, "write raw disk failed"))
    }

    fn flush(&self) -> Result<(), Error> {
        // TODO: issue a flush bio to host device.
        Ok(())
    }

    fn subset(&self, range: Range<BlockId>) -> Result<Self, Error> {
        if self.region.start + range.end > self.region.end {
            return Err(Error::with_msg(
                Errno::InvalidArgs,
                "subset is out of range",
            ));
        }

        Ok(RawDisk {
            device: self.device.clone(),
            region: Range {
                start: self.region.start + range.start,
                end: self.region.start + range.end,
            },
        })
    }

    fn nblocks(&self) -> usize {
        self.region.len()
    }
}

#[vtable]
impl DmTargetOps for DmSwornDisk {
    type Private = DmSwornDisk;

    fn ctr(target: &mut Target<Self>, args: Args) -> Result<Box<Self>> {
        // TODO: accept more arguments, e.g., root key.
        if args.len() != 1 {
            target.set_error(c_str!("Invalid argument count"));
            return Err(EINVAL);
        }

        let Ok(raw_disk) = RawDisk::open(&args[0]) else {
            target.set_error(c_str!("Device lookup failed"));
            return Err(ENODEV);
        };

        // TODO: use raw_disk to construct a sworndisk instance.
        let queue = Arc::new(ReqQueue::new(raw_disk));
        let worker = ReqQueue::spawn_req_worker(queue.clone());

        Box::init(DmSwornDisk::new(queue, worker))
    }

    fn dtr(target: &mut Target<Self>) {
        let Some(dm_sworndisk) = target.private() else {
            pr_warn!("Error, found no dm_sworndisk\n");
            return;
        };

        dm_sworndisk.queue.set_stopped();
        let worker = dm_sworndisk.worker.lock().take().unwrap();
        worker.join().unwrap();
    }

    fn map(target: &Target<Self>, bio: Bio) -> MapState {
        let Some(dm_sworndisk) = target.private() else {
            pr_warn!("Error, found no dm_sworndisk\n");
            return MapState::Kill;
        };

        match bio.op() {
            BioOp::Read | BioOp::Write => {
                dm_sworndisk.queue.enqueue(bio);
                return MapState::Submitted;
            }
            BioOp::Flush => {
                pr_info!("flush unsupported");
            }
            BioOp::Discard => {
                pr_info!("discard unsupported");
            }
            BioOp::Undefined => {
                pr_info!("undefined operations");
            }
        }
        MapState::Kill
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
    use bindings::thread::{sleep, spawn};
    use core::time::Duration;

    let t = spawn(|| {
        for i in 0..5 {
            sleep(Duration::from_secs(1));
            pr_info!("spawn running: {i}");
        }
        21
    });
    let r = t.join();
    assert_eq!(r, Ok(21));
}
