//! The layer of transactional logging.

pub mod chunk;
pub mod raw_log;
mod tx_log;

pub use self::tx_log::{TxLog, TxLogId, TxLogStore};

// TODO: Use D as the parameter for BlockSet
