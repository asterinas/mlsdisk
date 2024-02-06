//! Benchmarks of the system.
//!
//! Supports sequential/random write/read workloads.
//! Write/read amount, concurrency and I/O buffer size are configurable.
//! Provides a baseline named `EncDisk`, which simply protects data using authenticated encryption.
//! Results are displayed as throughput in MiB/sec.
use sworndisk_v2::*;

use self::benches::{Bench, BenchBuilder, IoPattern, IoType};
use self::consts::*;
use self::disks::{DiskType, FileAsDisk};
use self::util::{DisplayData, DisplayThroughput};

use libc::{fdatasync, ftruncate, open, pread, pwrite, unlink, O_CREAT, O_DIRECT, O_RDWR, O_TRUNC};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;

pub(crate) type Result<T> = core::result::Result<T, Error>;

fn main() {
    let total_bytes = 2 * GiB;
    // Specify all benchmarks
    let benches = vec![
        BenchBuilder::new("SwornDisk::write_seq")
            .disk_type(DiskType::SwornDisk)
            .io_type(IoType::Write)
            .io_pattern(IoPattern::Seq)
            .total_bytes(total_bytes)
            .buf_size(256 * KiB)
            .concurrency(1)
            .build()
            .unwrap(),
        BenchBuilder::new("SwornDisk::write_rnd")
            .disk_type(DiskType::SwornDisk)
            .io_type(IoType::Write)
            .io_pattern(IoPattern::Rnd)
            .total_bytes(total_bytes)
            .buf_size(4 * KiB)
            .concurrency(1)
            .build()
            .unwrap(),
        BenchBuilder::new("SwornDisk::read_seq")
            .disk_type(DiskType::SwornDisk)
            .io_type(IoType::Read)
            .io_pattern(IoPattern::Seq)
            .total_bytes(total_bytes)
            .buf_size(1 * MiB)
            .concurrency(1)
            .build()
            .unwrap(),
        BenchBuilder::new("SwornDisk::read_rnd")
            .disk_type(DiskType::SwornDisk)
            .io_type(IoType::Read)
            .io_pattern(IoPattern::Rnd)
            .total_bytes(total_bytes)
            .buf_size(4 * KiB)
            .concurrency(1)
            .build()
            .unwrap(),
        BenchBuilder::new("EncDisk::write_seq")
            .disk_type(DiskType::EncDisk)
            .io_type(IoType::Write)
            .io_pattern(IoPattern::Seq)
            .total_bytes(total_bytes)
            .buf_size(256 * KiB)
            .concurrency(1)
            .build()
            .unwrap(),
        BenchBuilder::new("EncDisk::write_rnd")
            .disk_type(DiskType::EncDisk)
            .io_type(IoType::Write)
            .io_pattern(IoPattern::Rnd)
            .total_bytes(total_bytes)
            .buf_size(4 * KiB)
            .concurrency(1)
            .build()
            .unwrap(),
        BenchBuilder::new("EncDisk::read_seq")
            .disk_type(DiskType::EncDisk)
            .io_type(IoType::Read)
            .io_pattern(IoPattern::Seq)
            .total_bytes(total_bytes)
            .buf_size(1 * MiB)
            .concurrency(1)
            .build()
            .unwrap(),
        BenchBuilder::new("EncDisk::read_rnd")
            .disk_type(DiskType::EncDisk)
            .io_type(IoType::Read)
            .io_pattern(IoPattern::Rnd)
            .total_bytes(total_bytes)
            .buf_size(4 * KiB)
            .concurrency(1)
            .build()
            .unwrap(),
    ];

    // Run all benchmarks and output the results
    run_benches(benches);
}

fn run_benches(benches: Vec<Box<dyn Bench>>) {
    println!("");

    let mut benched_count = 0;
    let mut failed_count = 0;
    for mut b in benches {
        print!("bench {} ... ", &b);
        b.prepare();

        let start = Instant::now();
        let res = b.run();
        if let Err(e) = res {
            failed_count += 1;
            println!("failed due to error {:?}", e);
            continue;
        }
        let elapsed = start.elapsed();

        let throughput = DisplayThroughput::new(b.total_bytes(), elapsed);
        println!("{}", throughput);

        b.display_ext();
        benched_count += 1;
    }

    let bench_res = if failed_count == 0 { "ok" } else { "failed" };
    println!(
        "\nbench result: {}. {} benched; {} failed.",
        bench_res, benched_count, failed_count
    );
}

mod benches {
    use super::disks::{BenchDisk, EncDisk};
    use super::*;

    use std::fmt::{self};
    use std::thread::{self, JoinHandle};

    pub trait Bench: fmt::Display {
        /// Returns the name of the benchmark.
        fn name(&self) -> &str;

        /// Returns the total number of bytes read or written.
        fn total_bytes(&self) -> usize;

        /// Do some preparatory work before running.
        fn prepare(&mut self) {}

        /// Run the benchmark.
        fn run(&self) -> Result<()>;

        /// Display extra information.
        fn display_ext(&self) {}
    }

    pub struct BenchBuilder {
        name: String,
        disk_type: Option<DiskType>,
        io_type: Option<IoType>,
        io_pattern: Option<IoPattern>,
        buf_size: usize,
        total_bytes: usize,
        concurrency: u32,
    }

    impl BenchBuilder {
        pub fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                disk_type: None,
                io_type: None,
                io_pattern: None,
                buf_size: 4 * KiB,
                total_bytes: 1 * MiB,
                concurrency: 1,
            }
        }

        pub fn disk_type(mut self, disk_type: DiskType) -> Self {
            self.disk_type = Some(disk_type);
            self
        }

        pub fn io_type(mut self, io_type: IoType) -> Self {
            self.io_type = Some(io_type);
            self
        }

        pub fn io_pattern(mut self, io_pattern: IoPattern) -> Self {
            self.io_pattern = Some(io_pattern);
            self
        }

        pub fn buf_size(mut self, buf_size: usize) -> Self {
            self.buf_size = buf_size;
            self
        }

        pub fn total_bytes(mut self, total_bytes: usize) -> Self {
            self.total_bytes = total_bytes;
            self
        }

        pub fn concurrency(mut self, concurrency: u32) -> Self {
            self.concurrency = concurrency;
            self
        }

        pub fn build(self) -> Result<Box<dyn Bench>> {
            let Self {
                name,
                disk_type,
                io_type,
                io_pattern,
                buf_size,
                total_bytes,
                concurrency,
            } = self;

            let disk_type = match disk_type {
                Some(disk_type) => disk_type,
                None => return_errno_with_msg!(Errno::InvalidArgs, "disk_type is not given"),
            };
            let io_type = match io_type {
                Some(io_type) => io_type,
                None => return_errno_with_msg!(Errno::InvalidArgs, "io_type is not given"),
            };
            let io_pattern = match io_pattern {
                Some(io_pattern) => io_pattern,
                None => return_errno_with_msg!(Errno::InvalidArgs, "io_pattern is not given"),
            };
            if total_bytes == 0 || total_bytes % BLOCK_SIZE != 0 {
                return_errno_with_msg!(
                    Errno::InvalidArgs,
                    "total_bytes must be greater than 0 and a multiple of block size"
                );
            }
            if buf_size == 0 || buf_size % BLOCK_SIZE != 0 {
                return_errno_with_msg!(
                    Errno::InvalidArgs,
                    "buf_size must be greater than 0 and a multiple of block size"
                );
            }
            if concurrency == 0 {
                return_errno_with_msg!(Errno::InvalidArgs, "concurrency must be greater than 0");
            }

            Ok(Box::new(SimpleDiskBench {
                name,
                disk_type,
                disk: None,
                io_type,
                io_pattern,
                buf_size,
                total_bytes,
                concurrency,
            }))
        }
    }

    pub struct SimpleDiskBench {
        name: String,
        disk_type: DiskType,
        disk: Option<Arc<dyn BenchDisk>>,
        io_type: IoType,
        io_pattern: IoPattern,
        buf_size: usize,
        total_bytes: usize,
        concurrency: u32,
    }

    impl Bench for SimpleDiskBench {
        fn name(&self) -> &str {
            &self.name
        }

        fn total_bytes(&self) -> usize {
            self.total_bytes
        }

        fn run(&self) -> Result<()> {
            let io_type = self.io_type;
            let io_pattern = self.io_pattern;
            let buf_nblocks = self.buf_size / BLOCK_SIZE;
            let total_nblock = self.total_bytes / BLOCK_SIZE;
            let concurrency = self.concurrency;

            let disk = self.disk.as_ref().unwrap();

            let local_nblocks = total_nblock / (concurrency as usize);
            let join_handles: Vec<JoinHandle<Result<()>>> = (0..concurrency)
                .map(|i| {
                    let disk = disk.clone();
                    let local_pos = (i as BlockId) * local_nblocks;
                    thread::spawn(move || match (io_type, io_pattern) {
                        (IoType::Read, IoPattern::Seq) => {
                            disk.read_seq(local_pos, local_nblocks, buf_nblocks)
                        }
                        (IoType::Write, IoPattern::Seq) => {
                            disk.write_seq(local_pos, local_nblocks, buf_nblocks)
                        }

                        (IoType::Read, IoPattern::Rnd) => {
                            disk.read_rnd(local_pos, local_nblocks, buf_nblocks)
                        }
                        (IoType::Write, IoPattern::Rnd) => {
                            disk.write_rnd(local_pos, local_nblocks, buf_nblocks)
                        }
                    })
                })
                .collect();

            let mut any_error = None;
            for join_handle in join_handles {
                let res = join_handle
                    .join()
                    .expect("couldn't join on the associated thread");
                if let Err(e) = res {
                    println!("benchmark task error: {:?}", &e);
                    any_error = Some(e);
                }
            }
            match any_error {
                None => Ok(()),
                Some(e) => Err(e),
            }
        }

        fn prepare(&mut self) {
            let disk = self.prepare_disk().unwrap();
            let _ = self.disk.insert(disk);
        }
    }

    impl SimpleDiskBench {
        fn prepare_disk(&self) -> Result<Arc<dyn BenchDisk>> {
            static DISK_ID: AtomicU32 = AtomicU32::new(0);

            let total_nblocks = self.total_bytes / BLOCK_SIZE;
            let disk: Arc<dyn BenchDisk> = match self.disk_type {
                DiskType::SwornDisk => Arc::new(SwornDisk::create(
                    FileAsDisk::create(
                        total_nblocks * 5 / 4, // TBD
                        &format!(
                            "sworndisk-{}.image",
                            DISK_ID.fetch_add(1, Ordering::Release)
                        ),
                    ),
                    AeadKey::default(),
                )?),

                DiskType::EncDisk => Arc::new(EncDisk::create(
                    total_nblocks,
                    &format!("encdisk-{}.image", DISK_ID.fetch_add(1, Ordering::Release)),
                )),
            };

            if self.io_type == IoType::Read {
                let disk = disk.clone();
                let _ = thread::spawn(move || {
                    disk.write_seq(0 as BlockId, total_nblocks, 1024).unwrap()
                })
                .join();
            }

            Ok(disk)
        }
    }

    impl fmt::Display for SimpleDiskBench {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "{} (total = {}, buf = {}, tasks = {})",
                self.name(),
                DisplayData::new(self.total_bytes),
                DisplayData::new(self.buf_size),
                self.concurrency
            )
        }
    }

    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub enum IoType {
        Read,
        Write,
    }

    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub enum IoPattern {
        Seq,
        Rnd,
    }
}

mod consts {
    pub const B: usize = 1;

    pub const KiB: usize = 1024 * B;
    pub const MiB: usize = 1024 * KiB;
    pub const GiB: usize = 1024 * MiB;

    pub const KB: usize = 1000 * B;
    pub const MB: usize = 1000 * KB;
    pub const GB: usize = 1000 * MB;
}

mod disks {
    use super::*;
    use std::{ffi::CString, ops::Range};

    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub enum DiskType {
        SwornDisk,
        EncDisk,
    }

    pub trait BenchDisk: Send + Sync {
        fn read_seq(&self, pos: BlockId, total_nblocks: usize, buf_nblocks: usize) -> Result<()>;
        fn write_seq(&self, pos: BlockId, total_nblocks: usize, buf_nblocks: usize) -> Result<()>;

        fn read_rnd(&self, pos: BlockId, total_nblocks: usize, buf_nblocks: usize) -> Result<()>;
        fn write_rnd(&self, pos: BlockId, total_nblocks: usize, buf_nblocks: usize) -> Result<()>;
    }

    #[derive(Clone)]
    pub struct FileAsDisk {
        fd: i32,
        path: String,
        range: Range<BlockId>,
    }

    impl FileAsDisk {
        pub fn create(nblocks: usize, path: &str) -> Self {
            unsafe {
                // let oflag = O_RDWR | O_CREAT | O_TRUNC;
                let oflag = O_RDWR | O_CREAT | O_TRUNC | O_DIRECT;
                let fd = open(CString::new(path).unwrap().as_ptr() as _, oflag, 0o666);
                if fd == -1 {
                    println!("open error: {}", std::io::Error::last_os_error());
                }
                assert!(fd > 0);

                let res = ftruncate(fd, (nblocks * BLOCK_SIZE) as _);
                if res == -1 {
                    println!("ftruncate error: {}", std::io::Error::last_os_error());
                }
                assert!(res >= 0);

                Self {
                    fd,
                    path: path.to_string(),
                    range: 0..nblocks,
                }
            }
        }
    }

    impl BlockSet for FileAsDisk {
        fn read(&self, mut pos: BlockId, mut buf: BufMut) -> Result<()> {
            pos += self.range.start;
            debug_assert!(pos + buf.nblocks() <= self.range.end);

            let buf_mut_slice = buf.as_mut_slice();
            unsafe {
                let res = pread(
                    self.fd,
                    buf_mut_slice.as_ptr() as _,
                    buf_mut_slice.len(),
                    (pos * BLOCK_SIZE) as _,
                );
                if res == -1 {
                    return_errno_with_msg!(Errno::IoFailed, "file read failed");
                }
            }

            Ok(())
        }

        fn write(&self, mut pos: BlockId, buf: BufRef) -> Result<()> {
            pos += self.range.start;
            debug_assert!(pos + buf.nblocks() <= self.range.end);

            let buf_slice = buf.as_slice();
            unsafe {
                let res = pwrite(
                    self.fd,
                    buf_slice.as_ptr() as _,
                    buf_slice.len(),
                    (pos * BLOCK_SIZE) as _,
                );
                if res == -1 {
                    return_errno_with_msg!(Errno::IoFailed, "file write failed");
                }
            }

            Ok(())
        }

        fn subset(&self, range: Range<BlockId>) -> Result<Self>
        where
            Self: Sized,
        {
            debug_assert!(self.range.start + range.end <= self.range.end);
            Ok(Self {
                fd: self.fd,
                path: self.path.clone(),
                range: Range {
                    start: self.range.start + range.start,
                    end: self.range.start + range.end,
                },
            })
        }

        fn flush(&self) -> Result<()> {
            unsafe {
                let res = fdatasync(self.fd);
                if res == -1 {
                    return_errno_with_msg!(Errno::IoFailed, "file sync failed");
                }
            }
            Ok(())
        }

        fn nblocks(&self) -> usize {
            self.range.len()
        }
    }

    impl Drop for FileAsDisk {
        fn drop(&mut self) {
            unsafe {
                unlink(self.path.as_ptr() as _);
            }
        }
    }

    impl BenchDisk for SwornDisk<FileAsDisk> {
        fn read_seq(&self, pos: BlockId, total_nblocks: usize, buf_nblocks: usize) -> Result<()> {
            let mut buf = Buf::alloc(buf_nblocks)?;

            for i in 0..total_nblocks / buf_nblocks {
                self.read(pos + i * buf_nblocks, buf.as_mut())?;
            }

            Ok(())
        }

        fn write_seq(&self, pos: BlockId, total_nblocks: usize, buf_nblocks: usize) -> Result<()> {
            let buf = Buf::alloc(buf_nblocks)?;

            for i in 0..total_nblocks / buf_nblocks {
                self.write(pos + i * buf_nblocks, buf.as_ref())?;
            }

            self.sync()
        }

        fn read_rnd(&self, pos: BlockId, total_nblocks: usize, buf_nblocks: usize) -> Result<()> {
            let mut buf = Buf::alloc(buf_nblocks)?;

            for _ in 0..total_nblocks / buf_nblocks {
                let rnd_pos = gen_rnd_pos(total_nblocks, buf_nblocks);
                self.read(pos + rnd_pos, buf.as_mut())?;
            }

            Ok(())
        }

        fn write_rnd(&self, pos: BlockId, total_nblocks: usize, buf_nblocks: usize) -> Result<()> {
            let buf = Buf::alloc(buf_nblocks)?;

            for _ in 0..total_nblocks / buf_nblocks {
                let rnd_pos = gen_rnd_pos(total_nblocks, buf_nblocks);
                self.write(pos + rnd_pos, buf.as_ref())?;
            }

            self.sync()
        }
    }

    fn gen_rnd_pos(total_nblocks: usize, buf_nblocks: usize) -> BlockId {
        let mut rnd_pos_bytes = [0u8; 8];
        Rng::new(&[]).fill_bytes(&mut rnd_pos_bytes).unwrap();
        BlockId::from_le_bytes(rnd_pos_bytes) % (total_nblocks - buf_nblocks)
    }

    #[derive(Clone)]
    pub struct EncDisk {
        file_disk: FileAsDisk,
    }

    impl EncDisk {
        pub fn create(nblocks: usize, path: &str) -> Self {
            Self {
                file_disk: FileAsDisk::create(nblocks, path),
            }
        }

        fn dummy_encrypt() -> Result<()> {
            let key = AeadKey::random();
            let plain = Buf::alloc(1)?;
            let mut cipher = Buf::alloc(1)?;
            let _ = Aead::new().encrypt(
                plain.as_slice(),
                &key,
                &AeadIv::default(),
                &[],
                cipher.as_mut_slice(),
            )?;
            Ok(())
        }

        fn dummy_decrypt() -> Result<()> {
            let cipher = Buf::alloc(1)?;
            let mut plain = Buf::alloc(1)?;
            let _ = Aead::new().decrypt(
                cipher.as_slice(),
                &AeadKey::default(),
                &AeadIv::default(),
                &[],
                &AeadMac::default(),
                plain.as_mut_slice(),
            );
            Ok(())
        }
    }

    impl BenchDisk for EncDisk {
        fn read_seq(&self, pos: BlockId, total_nblocks: usize, buf_nblocks: usize) -> Result<()> {
            let mut buf = Buf::alloc(buf_nblocks)?;

            for i in 0..total_nblocks / buf_nblocks {
                for _ in 0..buf_nblocks {
                    Self::dummy_decrypt().unwrap();
                }
                self.file_disk.read(pos + i * buf_nblocks, buf.as_mut())?;
            }

            Ok(())
        }

        fn write_seq(&self, pos: BlockId, total_nblocks: usize, buf_nblocks: usize) -> Result<()> {
            let buf = Buf::alloc(buf_nblocks)?;

            for i in 0..total_nblocks / buf_nblocks {
                for _ in 0..buf_nblocks {
                    Self::dummy_encrypt().unwrap();
                }
                self.file_disk.write(pos + i * buf_nblocks, buf.as_ref())?;
            }

            self.file_disk.flush()
        }

        fn read_rnd(&self, pos: BlockId, total_nblocks: usize, buf_nblocks: usize) -> Result<()> {
            let mut buf = Buf::alloc(buf_nblocks)?;

            for _ in 0..total_nblocks / buf_nblocks {
                for _ in 0..buf_nblocks {
                    Self::dummy_decrypt().unwrap();
                }
                let rnd_pos = gen_rnd_pos(total_nblocks, buf_nblocks);
                self.file_disk.read(pos + rnd_pos, buf.as_mut())?;
            }

            Ok(())
        }

        fn write_rnd(&self, pos: BlockId, total_nblocks: usize, buf_nblocks: usize) -> Result<()> {
            let buf = Buf::alloc(buf_nblocks)?;

            for _ in 0..total_nblocks / buf_nblocks {
                for _ in 0..buf_nblocks {
                    Self::dummy_encrypt().unwrap();
                }
                let rnd_pos = gen_rnd_pos(total_nblocks, buf_nblocks);
                self.file_disk.write(pos + rnd_pos, buf.as_ref())?;
            }

            self.file_disk.flush()
        }
    }
}

mod util {
    use super::*;
    use std::fmt::{self};
    use std::time::Duration;

    pub fn align_up(n: usize, a: usize) -> usize {
        debug_assert!(a >= 2 && a.is_power_of_two());
        (n + a - 1) & !(a - 1)
    }

    /// Display the amount of data in the unit of GB, MB, KB, or bytes.
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub struct DisplayData(usize);

    impl DisplayData {
        pub fn new(nbytes: usize) -> Self {
            Self(nbytes)
        }
    }

    impl fmt::Display for DisplayData {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            const UNIT_TABLE: [(&str, usize); 4] =
                [("GiB", GiB), ("MiB", MiB), ("KiB", KiB), ("bytes", 0)];
            let (unit_str, unit_val) = {
                let (unit_str, mut unit_val) = UNIT_TABLE
                    .iter()
                    .find(|(_, unit_val)| self.0 >= *unit_val)
                    .unwrap();
                if unit_val == 0 {
                    unit_val = 1;
                }
                (unit_str, unit_val)
            };
            let data_val_in_unit = (self.0 as f64) / (unit_val as f64);
            write!(f, "{:.1} {}", data_val_in_unit, unit_str)
        }
    }

    /// Display throughput in the unit of bytes/s, KB/s, MB/s, or GB/s.
    #[derive(Copy, Clone, Debug, PartialEq)]
    pub struct DisplayThroughput(f64);

    impl DisplayThroughput {
        pub fn new(total_bytes: usize, elapsed: Duration) -> Self {
            let total_bytes = total_bytes as f64;
            let elapsed_secs = elapsed.as_secs_f64();
            let throughput = total_bytes / elapsed_secs;
            Self(throughput)
        }
    }

    impl fmt::Display for DisplayThroughput {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            const UNIT_TABLE: [(&str, usize); 4] =
                [("GB/s", GB), ("MB/s", MB), ("KB/s", KB), ("bytes/s", 0)];
            let (unit_str, unit_val) = {
                let (unit_str, mut unit_val) = UNIT_TABLE
                    .iter()
                    .find(|(_, unit_val)| self.0 >= (*unit_val as f64))
                    .unwrap();
                if unit_val == 0 {
                    unit_val = 1;
                }
                (unit_str, unit_val)
            };
            let throughput_in_unit = self.0 / (unit_val as f64);
            write!(f, "{:.2} {}", throughput_in_unit, unit_str)
        }
    }
}
