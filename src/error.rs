/// The error type which is returned from the APIs of this crate.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Errno {
    /// Transaction aborted.
    TxAborted,
    /// Not found.
    NotFound,
    InvalidArgs,
    NoMemory,
    IoError,
    NotEnoughSpace,
    /// OS-specific unknown error.
    OsSpecUnknown,
    /// Encryption operation failed.
    EncryptFault,
    /// Decryption operation failed.
    DecryptFault,
    /// Not aligned to `BLOCK_SIZE`.
    NonBlockAlignedSizeError,
}

/// error used in this crate
#[derive(Debug, Clone)]
pub struct Error {
    errno: Errno,
    msg: Option<&'static str>,
}

impl Error {
    pub const fn new(errno: Errno) -> Self {
        Error { errno, msg: None }
    }

    pub const fn with_msg(errno: Errno, msg: &'static str) -> Self {
        Error {
            errno,
            msg: Some(msg),
        }
    }

    pub fn errno(&self) -> Errno {
        self.errno
    }
}

impl From<Errno> for Error {
    fn from(errno: Errno) -> Self {
        Error::new(errno)
    }
}

#[macro_export]
macro_rules! return_errno {
    ($errno: expr) => {
        return core::result::Result::Err(crate::error::Error::new($errno))
    };
}

#[macro_export]
macro_rules! return_errno_with_msg {
    ($errno: expr, $msg: expr) => {
        return core::result::Result::Err(crate::error::Error::with_msg($errno, $msg))
    };
}
