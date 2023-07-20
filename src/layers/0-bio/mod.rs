use static_assertions::assert_eq_size;

mod block_buf;
mod block_log;
mod block_set;
mod block_ring;

pub use self::block_log::BlockLog;
pub use self::block_set::BlockSet;
pub use self::block_ring::BlockRing;
pub use self::block_buf::BlockBuf;

pub type BlockId = usize;
// This definition of BlockId assumes the target architecture is 64-bit
assert_eq_size!(usize, u64); 