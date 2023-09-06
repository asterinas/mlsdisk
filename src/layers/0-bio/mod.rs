use static_assertions::assert_eq_size;

mod block_buf;
mod block_log;
mod block_ring;
mod block_set;

pub use self::block_buf::{Buf, BufMut, BufRef};
pub use self::block_log::BlockLog;
pub use self::block_ring::BlockRing;
pub use self::block_set::{BlockSet, MemDisk};

pub type BlockId = usize;
pub const BLOCK_SIZE: usize = 0x1000;

// This definition of BlockId assumes the target architecture is 64-bit
assert_eq_size!(usize, u64);
