//! OS-specific or OS-dependent APIs.

#[cfg(feature = "jinux")]
mod jinux;
#[cfg(feature = "jinux")]
pub use self::jinux::{
    spawn, Aead, AeadIv, AeadKey, AeadMac, Arc, Box, CurrentThread, HashMap, HashSet, JoinHandle,
    Mutex, MutexGuard, Pages, Rng, RwLock, RwLockReadGuard, RwLockWriteGuard, Skcipher, SkcipherIv,
    SkcipherKey, String, Tid, ToString, Vec, Weak, PAGE_SIZE,
};

#[cfg(feature = "linux")]
mod linux;
#[cfg(feature = "linux")]
pub use self::linux::{
    spawn, Aead, AeadIv, AeadKey, AeadMac, Arc, Box, CurrentThread, HashMap, HashSet, JoinHandle,
    Mutex, MutexGuard, Pages, Rng, RwLock, RwLockReadGuard, RwLockWriteGuard, Skcipher, SkcipherIv,
    SkcipherKey, String, Tid, ToString, Vec, Weak, PAGE_SIZE,
};

#[cfg(feature = "occlum")]
mod occlum;
#[cfg(feature = "occlum")]
pub use self::occlum::{
    spawn, Aead, AeadIv, AeadKey, AeadMac, Arc, BTreeMap, Box, CurrentThread, HashMap, HashSet,
    JoinHandle, Mutex, MutexGuard, Pages, Rng, RwLock, RwLockReadGuard, RwLockWriteGuard, Skcipher,
    SkcipherIv, SkcipherKey, String, Tid, ToString, Vec, Weak, PAGE_SIZE,
};

#[cfg(feature = "std")]
mod std;
#[cfg(feature = "std")]
pub use self::std::{
    spawn, Aead, AeadIv, AeadKey, AeadMac, Arc, BTreeMap, Box, CurrentThread, HashMap, HashSet,
    JoinHandle, Mutex, MutexGuard, Pages, Rng, RwLock, RwLockReadGuard, RwLockWriteGuard, Skcipher,
    SkcipherIv, SkcipherKey, String, Tid, ToString, Vec, Weak, PAGE_SIZE,
};
