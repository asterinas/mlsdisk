mod bitmap;
mod crypto;
mod lazy_delete;

pub use self::bitmap::BitMap;
pub use self::crypto::{Aead, RandomInit, Rng, Skcipher};
pub use self::lazy_delete::LazyDelete;

pub(crate) const fn align_up(x: usize, align: usize) -> usize {
    ((x + align - 1) / align) * align
}

pub(crate) const fn align_down(x: usize, align: usize) -> usize {
    (x / align) * align
}
