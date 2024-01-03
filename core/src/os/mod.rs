//! OS-specific or OS-dependent APIs.

#[cfg(feature = "jinux")]
mod jinux;
#[cfg(feature = "jinux")]
pub use self::jinux::{
    Aead, AeadIv, AeadKey, AeadMac, Arc, Box, CurrentThread, HashMap, HashSet, Mutex, MutexGuard,
    Pages, Rng, RwLock, RwLockReadGuard, RwLockWriteGuard, Skcipher, SkcipherIv, SkcipherKey,
    String, Tid, ToString, Vec, Weak, PAGE_SIZE,
};

#[cfg(feature = "linux")]
mod linux;
#[cfg(feature = "linux")]
pub use self::linux::{
    Aead, AeadIv, AeadKey, AeadMac, Arc, Box, CurrentThread, HashMap, HashSet, Mutex, MutexGuard,
    Pages, Rng, RwLock, RwLockReadGuard, RwLockWriteGuard, Skcipher, SkcipherIv, SkcipherKey,
    String, Tid, ToString, Vec, Weak, PAGE_SIZE,
};

#[cfg(all(feature = "occlum", target_env = "sgx"))]
mod occlum;
#[cfg(all(feature = "occlum", target_env = "sgx"))]
pub use self::occlum::{
    Aead, AeadIv, AeadKey, AeadMac, Arc, Box, CurrentThread, HashMap, HashSet, Mutex, MutexGuard,
    Pages, Rng, RwLock, RwLockReadGuard, RwLockWriteGuard, Skcipher, SkcipherIv, SkcipherKey,
    String, Tid, ToString, Vec, Weak, PAGE_SIZE,
};

#[cfg(feature = "std")]
mod std;
#[cfg(feature = "std")]
pub use self::std::{
    Aead, AeadIv, AeadKey, AeadMac, Arc, Box, CurrentThread, HashMap, HashSet, Mutex, MutexGuard,
    Pages, Rng, RwLock, RwLockReadGuard, RwLockWriteGuard, Skcipher, SkcipherIv, SkcipherKey,
    String, Tid, ToString, Vec, Weak, PAGE_SIZE,
};
