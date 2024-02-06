mod bitmap;
mod crypto;
mod lazy_delete;
mod metrics;

pub use self::bitmap::BitMap;
pub use self::crypto::{Aead, RandomInit, Rng, Skcipher};
pub use self::lazy_delete::LazyDelete;
pub use self::metrics::{AmpType, AmplificationMetrics, LatencyMetrics, Metrics, ReqType};

pub(crate) const fn align_up(x: usize, align: usize) -> usize {
    ((x + align - 1) / align) * align
}

pub(crate) const fn align_down(x: usize, align: usize) -> usize {
    (x / align) * align
}
