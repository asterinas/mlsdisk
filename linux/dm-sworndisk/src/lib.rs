// SPDX-License-Identifier: GPL-2.0
// Copyright (c) 2023 Ant Group CO., Ltd.

//! A device mapper target based on `SwornDisk`.

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
use sworndisk::{
    AeadKey, Arc, BlockId, BlockSet, Buf, BufMut, BufRef, Errno, Error, Mutex, SwornDisk, Vec,
};

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
        // test_aead();
        // test_skcipher();
        // test_rawdisk(c_str!("/dev/sdb"), 1 * 1024 * 1024 * 1024);
        // test_sworndisk(c_str!("/dev/sdb"), 10 * 1024 * 1024 * 1024);

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
struct ReqQueue<D: BlockSet> {
    bios: Mutex<SegQueue<Bio>>,
    disk: SwornDisk<D>,
    should_stop: AtomicBool,
    new_bio_condvar: Pin<Box<CondVar>>,
}

impl<D: BlockSet + 'static> ReqQueue<D> {
    /// Constructs a `ReqQueue`.
    pub fn new(disk: SwornDisk<D>) -> Self {
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
                "bio not aligned to BLOCK_SIZE, {:?}, start_sector: {}, len: {}",
                bio.op(),
                bio.start_sector(),
                bio.len(),
            );

            match bio.op() {
                BioOp::Read => self.read_unaligned(&bio),
                BioOp::Write => self.write_unaligned(&bio),
                _ => unreachable!(),
            }
            bio.end();
            return;
        }

        // TODO: we may need a block range iterator abstraction to deal with
        // blocks, including the unaligned ones.

        match bio.op() {
            BioOp::Read => {
                let mut bio_vecs = Vec::new();
                bio.iter().for_each(|bio_vec| bio_vecs.push(bio_vec));
                let mut bufs = Vec::new();
                bio_vecs.iter_mut().for_each(|bio_vec| {
                    bufs.push(BufMut::try_from(bio_vec.deref_mut()).unwrap());
                });

                let start_block = bio.start_sector() / BLOCK_SECTORS;
                if let Err(err) = self.disk.readv(start_block, &mut bufs) {
                    pr_info!(
                        "read sworndisk failed, block_id: {}, nblocks: {}, err: {:?}",
                        start_block,
                        bio.len() / BLOCK_SIZE,
                        err,
                    );
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

    fn read_unaligned(&self, bio: &Bio) {
        let start_bytes = bio.start_sector() * (BLOCK_SIZE / BLOCK_SECTORS);
        let end_bytes = start_bytes + bio.len();

        let start_block = start_bytes / BLOCK_SIZE;
        let end_block = (end_bytes + BLOCK_SIZE - 1) / BLOCK_SIZE;
        let nblocks = end_block - start_block;
        let mut buf = Buf::alloc(nblocks).expect("alloc read buffer failed");

        if let Err(err) = self.disk.read(start_block, buf.as_mut()) {
            pr_info!(
                "read sworndisk failed, block_id: {}, nblocks: {}, err: {:?}",
                start_block,
                nblocks,
                err,
            );
        }

        let mut offset = start_bytes % BLOCK_SIZE;
        for mut bio_vec in bio.iter() {
            let vec_len = bio_vec.len();
            bio_vec.copy_from_slice(&buf.as_slice()[offset..offset + vec_len]);
            offset += vec_len;
        }
    }

    fn write_unaligned(&self, bio: &Bio) {
        let start_bytes = bio.start_sector() * (BLOCK_SIZE / BLOCK_SECTORS);
        let end_bytes = start_bytes + bio.len();

        let start_block = start_bytes / BLOCK_SIZE;
        let end_block = (end_bytes + BLOCK_SIZE - 1) / BLOCK_SIZE;
        let nblocks = end_block - start_block;
        let mut buf = Buf::alloc(nblocks).expect("alloc read buffer failed");

        // TODO: we could only read the first and last blocks.
        if let Err(err) = self.disk.read(start_block, buf.as_mut()) {
            pr_info!(
                "read sworndisk failed, block_id: {}, nblocks: {}, err: {:?}",
                start_block,
                nblocks,
                err,
            );
        }

        let mut offset = start_bytes % BLOCK_SIZE;
        for mut bio_vec in bio.iter() {
            let vec_len = bio_vec.len();
            buf.as_mut_slice()[offset..offset + vec_len].copy_from_slice(bio_vec.deref());
            offset += vec_len;
        }

        if let Err(err) = self.disk.write(start_block, buf.as_ref()) {
            pr_info!(
                "write sworndisk failed, block_id: {}, nblocks: {}, err: {:?}",
                start_block,
                nblocks,
                err,
            );
        }
    }

    /// Processes all the pending `Bio`s in the queue.
    fn clear(&self) {
        while let Some(bio) = self.bios.lock().pop() {
            self.process(bio);
        }
    }

    /// Sync the disk before handle next bio.
    pub fn sync(&self) -> Result<(), Error> {
        let _ = self.bios.lock();
        self.disk.sync()
    }
}

/// A struct represent a `dm_target` type, which should impl `DmTargetOps`.
struct DmSwornDisk {
    queue: Arc<ReqQueue<RawDisk>>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl DmSwornDisk {
    /// Returns an in-place initializer.
    fn new(queue: Arc<ReqQueue<RawDisk>>, worker: JoinHandle<()>) -> impl Init<Self> {
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
        if args.len() != 3 {
            target.set_error(c_str!("Invalid argument count"));
            return Err(EINVAL);
        }

        let should_format = match args[0].to_str() {
            Ok("create") => true,
            Ok("open") => false,
            _ => {
                target.set_error(c_str!("Unsupported operation"));
                return Err(EINVAL);
            }
        };

        let Ok(raw_disk) = RawDisk::open(&args[1]) else {
            target.set_error(c_str!("Device lookup failed"));
            return Err(ENODEV);
        };

        let root_key = {
            let mut key = AeadKey::default();
            // SAFETY: `key` and `args` contain valid pointers.
            let err =
                unsafe { bindings::hex2bin(key.as_mut_ptr(), args[2].as_char_ptr(), key.len()) };
            if err != 0 {
                target.set_error(c_str!("Invalid root key"));
                return Err(EINVAL);
            };
            key
        };

        let sworndisk = match should_format {
            true => SwornDisk::create(raw_disk, root_key, None).map_err(|_| {
                target.set_error(c_str!("Create sworndisk failed"));
                ENODEV
            })?,
            // TODO: open with a `SyncIdStore`.
            false => SwornDisk::open(raw_disk, root_key, None).map_err(|_| {
                target.set_error(c_str!("Open sworndisk failed"));
                ENODEV
            })?,
        };
        target.set_region(0..sworndisk.total_blocks());

        let queue = Arc::new(ReqQueue::new(sworndisk));
        let worker = ReqQueue::spawn_req_worker(queue.clone());

        Box::init(DmSwornDisk::new(queue, worker))
    }

    fn dtr(target: &mut Target<Self>) {
        let Some(dm_sworndisk) = target.private() else {
            pr_err!("Error, found no dm_sworndisk\n");
            return;
        };

        dm_sworndisk.queue.set_stopped();
        let worker = dm_sworndisk.worker.lock().take().unwrap();
        worker.join().unwrap();

        dm_sworndisk.queue.sync().unwrap();
    }

    fn map(target: &Target<Self>, bio: Bio) -> MapState {
        let Some(dm_sworndisk) = target.private() else {
            pr_err!("Error, found no dm_sworndisk\n");
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

fn test_aead() {
    use bindings::crypto::{Aead, AeadReq};
    use kernel::stack_try_pin_init;

    stack_try_pin_init!(let aes_gcm = Aead::new(c_str!("gcm(aes)"), 0, 0));
    if let Err(e) = aes_gcm {
        pr_info!("alloc gcm(aes) cipher failed, errno: {}\n", e.to_errno());
        return;
    }
    let aes_gcm = aes_gcm.ok().unwrap();
    pr_info!("gcm(aes):\n");
    pr_info!("ivsize: {}\n", aes_gcm.ivsize());
    pr_info!("authsize: {}\n", aes_gcm.authsize());
    pr_info!("blocksize: {}\n", aes_gcm.blocksize());
    pr_info!("reqsize: {}\n", aes_gcm.reqsize());

    // From McGrew & Viega - http://citeseer.ist.psu.edu/656989.html
    let key: [u8; 16] = [
        0xfe, 0xff, 0xe9, 0x92, 0x86, 0x65, 0x73, 0x1c, 0x6d, 0x6a, 0x8f, 0x94, 0x67, 0x30, 0x83,
        0x08,
    ];
    let iv: [u8; 12] = [
        0xca, 0xfe, 0xba, 0xbe, 0xfa, 0xce, 0xdb, 0xad, 0xde, 0xca, 0xf8, 0x88,
    ];
    let plaintext: [u8; 64] = [
        0xd9, 0x31, 0x32, 0x25, 0xf8, 0x84, 0x06, 0xe5, 0xa5, 0x59, 0x09, 0xc5, 0xaf, 0xf5, 0x26,
        0x9a, 0x86, 0xa7, 0xa9, 0x53, 0x15, 0x34, 0xf7, 0xda, 0x2e, 0x4c, 0x30, 0x3d, 0x8a, 0x31,
        0x8a, 0x72, 0x1c, 0x3c, 0x0c, 0x95, 0x95, 0x68, 0x09, 0x53, 0x2f, 0xcf, 0x0e, 0x24, 0x49,
        0xa6, 0xb5, 0x25, 0xb1, 0x6a, 0xed, 0xf5, 0xaa, 0x0d, 0xe6, 0x57, 0xba, 0x63, 0x7b, 0x39,
        0x1a, 0xaf, 0xd2, 0x55,
    ];
    let expected_data: [u8; 64] = [
        0x42, 0x83, 0x1e, 0xc2, 0x21, 0x77, 0x74, 0x24, 0x4b, 0x72, 0x21, 0xb7, 0x84, 0xd0, 0xd4,
        0x9c, 0xe3, 0xaa, 0x21, 0x2f, 0x2c, 0x02, 0xa4, 0xe0, 0x35, 0xc1, 0x7e, 0x23, 0x29, 0xac,
        0xa1, 0x2e, 0x21, 0xd5, 0x14, 0xb2, 0x54, 0x66, 0x93, 0x1c, 0x7d, 0x8f, 0x6a, 0x5a, 0xac,
        0x84, 0xaa, 0x05, 0x1b, 0xa3, 0x0b, 0x39, 0x6a, 0x0a, 0xac, 0x97, 0x3d, 0x58, 0xe0, 0x91,
        0x47, 0x3f, 0x59, 0x85,
    ];
    let expected_mac: [u8; 16] = [
        0x4d, 0x5c, 0x2a, 0xf3, 0x27, 0xcd, 0x64, 0xa6, 0x2c, 0xf3, 0x5a, 0xbd, 0x2b, 0xa6, 0xfa,
        0xb4,
    ];

    let _ = aes_gcm.set_key(&key);
    let req = aes_gcm.alloc_request().unwrap();
    let mut ciphertext = [0u8; 64];
    let mut mac = [0u8; 16];
    if let Err(e) = req.encrypt(&[], &plaintext, &iv, &mut ciphertext, &mut mac) {
        pr_info!("aead encrypt failed, errno: {}\n", e.to_errno());
        return;
    }
    pr_info!("ciphertext: {:02x?}\n", ciphertext);
    pr_info!("mac: {:02x?}\n", mac);
    assert_eq!(ciphertext, expected_data);
    assert_eq!(mac, expected_mac);

    let req = aes_gcm.alloc_request().unwrap();
    let mut decrypted = [0u8; 64];
    if let Err(e) = req.decrypt(&[], &ciphertext, &mac, &iv, &mut decrypted) {
        pr_info!("aead decrypt failed, errno: {}\n", e.to_errno());
        return;
    }
    pr_info!("decrypted: {:0x?}\n", decrypted);
    assert_eq!(decrypted, plaintext);
}

fn test_skcipher() {
    use bindings::crypto::{Skcipher, SkcipherReq};

    let aes_ctr = Box::pin_init(Skcipher::new(c_str!("ctr(aes)"), 0, 0));
    if let Err(e) = aes_ctr {
        pr_info!("alloc ctr(aes) cipher failed, errno: {}\n", e.to_errno());
        return;
    }
    let aes_ctr = aes_ctr.ok().unwrap();
    pr_info!("ctr(aes):\n");
    pr_info!("ivsize: {}\n", aes_ctr.ivsize());
    pr_info!("blocksize: {}\n", aes_ctr.blocksize());
    pr_info!("reqsize: {}\n", aes_ctr.reqsize());

    // From NIST Special Publication 800-38A, Appendix F.5
    let key: [u8; 16] = [
        0x2b, 0x7e, 0x15, 0x16, 0x28, 0xae, 0xd2, 0xa6, 0xab, 0xf7, 0x15, 0x88, 0x09, 0xcf, 0x4f,
        0x3c,
    ];
    let mut iv: [u8; 16] = [
        0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8, 0xf9, 0xfa, 0xfb, 0xfc, 0xfd, 0xfe,
        0xff,
    ];
    let iv_out: [u8; 16] = [
        0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8, 0xf9, 0xfa, 0xfb, 0xfc, 0xfd, 0xff,
        0x03,
    ];
    let plaintext: [u8; 64] = [
        0x6b, 0xc1, 0xbe, 0xe2, 0x2e, 0x40, 0x9f, 0x96, 0xe9, 0x3d, 0x7e, 0x11, 0x73, 0x93, 0x17,
        0x2a, 0xae, 0x2d, 0x8a, 0x57, 0x1e, 0x03, 0xac, 0x9c, 0x9e, 0xb7, 0x6f, 0xac, 0x45, 0xaf,
        0x8e, 0x51, 0x30, 0xc8, 0x1c, 0x46, 0xa3, 0x5c, 0xe4, 0x11, 0xe5, 0xfb, 0xc1, 0x19, 0x1a,
        0x0a, 0x52, 0xef, 0xf6, 0x9f, 0x24, 0x45, 0xdf, 0x4f, 0x9b, 0x17, 0xad, 0x2b, 0x41, 0x7b,
        0xe6, 0x6c, 0x37, 0x10,
    ];
    let expected: [u8; 64] = [
        0x87, 0x4d, 0x61, 0x91, 0xb6, 0x20, 0xe3, 0x26, 0x1b, 0xef, 0x68, 0x64, 0x99, 0x0d, 0xb6,
        0xce, 0x98, 0x06, 0xf6, 0x6b, 0x79, 0x70, 0xfd, 0xff, 0x86, 0x17, 0x18, 0x7b, 0xb9, 0xff,
        0xfd, 0xff, 0x5a, 0xe4, 0xdf, 0x3e, 0xdb, 0xd5, 0xd3, 0x5e, 0x5b, 0x4f, 0x09, 0x02, 0x0d,
        0xb0, 0x3e, 0xab, 0x1e, 0x03, 0x1d, 0xda, 0x2f, 0xbe, 0x03, 0xd1, 0x79, 0x21, 0x70, 0xa0,
        0xf3, 0x00, 0x9c, 0xee,
    ];

    let _ = aes_ctr.set_key(&key);
    let req = aes_ctr.alloc_request().unwrap();
    let mut ciphertext = [0u8; 64];
    if let Err(e) = req.encrypt(&plaintext, &iv, &mut ciphertext) {
        pr_info!("skcipher encrypt failed, errno: {}\n", e.to_errno());
        return;
    }
    pr_info!("ciphertext: {:02x?}\n", ciphertext);
    assert_eq!(ciphertext, expected);
    assert_eq!(iv, iv_out);

    let req = aes_ctr.alloc_request().unwrap();
    let mut iv: [u8; 16] = [
        0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8, 0xf9, 0xfa, 0xfb, 0xfc, 0xfd, 0xfe,
        0xff,
    ];
    let mut decrypted = [0u8; 64];
    if let Err(e) = req.decrypt(&ciphertext, &iv, &mut decrypted) {
        pr_info!("skcipher decrypt failed, errno: {}\n", e.to_errno());
        return;
    }
    assert_eq!(decrypted, plaintext);
}

fn test_rawdisk(dev_path: &'static CStr, test_size: usize) {
    spawn(move || {
        pr_info!("----test_rawdisk begin");
        let rawdisk = RawDisk::open(dev_path).unwrap();
        let nblocks = test_size / BLOCK_SIZE;
        if rawdisk.region.end < nblocks {
            pr_err!("raw disk is too small for test_size: {test_size}");
            return;
        }

        let mut buf = Buf::alloc(1).unwrap();
        pr_info!("----write begin");
        for i in 0..nblocks {
            buf.as_mut_slice().fill(i as u8);
            match rawdisk.write(i, buf.as_ref()) {
                Err(err) => pr_info!("write sworndisk failed, block_id: {}, err: {:?}", i, err),
                Ok(_) => continue,
            }
        }
        pr_info!("----write end");

        rawdisk.flush().unwrap();

        pr_info!("----read begin");
        for i in 0..nblocks {
            match rawdisk.read(i, buf.as_mut()) {
                Err(err) => pr_info!("read sworndisk failed, block_id: {}, err: {:?}", i, err),
                Ok(_) => continue,
            }
            assert_eq!(buf.as_slice()[0], i as u8);
        }
        pr_info!("----read end");
        pr_info!("----test_rawdisk end");
    });
}

fn test_sworndisk(dev_path: &'static CStr, test_size: usize) {
    spawn(move || {
        pr_info!("----test_sworndisk begin");
        let root_key = AeadKey::default();
        let raw_disk = RawDisk::open(dev_path).unwrap();
        let sworndisk = SwornDisk::create(raw_disk, root_key, None).unwrap();
        let nblocks = test_size / BLOCK_SIZE;
        if sworndisk.total_blocks() < nblocks {
            pr_err!("sworndisk is too small for test_size: {test_size}");
            return;
        }

        let mut buf = Buf::alloc(1).unwrap();
        pr_info!("----write begin");
        for i in 0..nblocks {
            buf.as_mut_slice().fill(i as u8);
            match sworndisk.write(i, buf.as_ref()) {
                Err(err) => pr_info!("write sworndisk failed, block_id: {}, err: {:?}", i, err),
                Ok(_) => continue,
            }
        }
        pr_info!("----write end");

        sworndisk.sync().unwrap();

        pr_info!("----read begin");
        for i in 0..nblocks {
            match sworndisk.read(i, buf.as_mut()) {
                Err(err) => pr_info!("read sworndisk failed, block_id: {}, err: {:?}", i, err),
                Ok(_) => continue,
            }
            assert_eq!(buf.as_slice()[0], i as u8);
        }
        pr_info!("----read end");
        pr_info!("----test_sworndisk end");
    });
}
