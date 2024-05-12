//! Occlum specific implementations.

use crate::error::Errno;
use crate::prelude::{Error, Result};

use core::marker::PhantomData;
use core::ptr::NonNull;
use pod::Pod;
use serde::{Deserialize, Serialize};
use sgx_rand::{thread_rng, Rng as _};
use sgx_tcrypto::{rsgx_aes_ctr_decrypt, rsgx_aes_ctr_encrypt};
use sgx_tcrypto::{rsgx_rijndael128GCM_decrypt, rsgx_rijndael128GCM_encrypt};
use sgx_tstd::alloc::{alloc, dealloc, Layout};
use sgx_tstd::thread;
use sgx_types::sgx_status_t;

pub use hashbrown::{HashMap, HashSet};
/// Reuse lock implementation of crate spin.
pub use spin::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub use sgx_tstd::boxed::Box;
pub use sgx_tstd::collections::BTreeMap;
pub use sgx_tstd::string::{String, ToString};
pub use sgx_tstd::sync::{Arc, Weak};
pub use sgx_tstd::thread::{spawn, JoinHandle};
#[macro_use]
pub use sgx_tstd::vec::Vec;

/// Unique ID for the OS thread.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[repr(transparent)]
pub struct Tid(thread::ThreadId);

/// A struct to get the current thread id.
pub struct CurrentThread;

impl CurrentThread {
    pub fn id() -> Tid {
        // SAFETY: calling extern "C" to get the thread_id is safe here.
        Tid(thread::current().id())
    }
}

pub const PAGE_SIZE: usize = 0x1000;

struct PageAllocator;

impl PageAllocator {
    /// Allocate memory buffer with specific size.
    ///
    /// The `len` indicates the number of pages.
    fn alloc(len: usize) -> Option<NonNull<u8>> {
        if len == 0 {
            return None;
        }

        // SAFETY: the `count` is non-zero, then the `Layout` has
        // non-zero size, so it's safe.
        unsafe {
            let layout = Layout::from_size_align_unchecked(len * PAGE_SIZE, PAGE_SIZE);
            let ptr = alloc(layout);
            NonNull::new(ptr)
        }
    }

    /// Deallocate memory buffer at the given `ptr` and `len`.
    ///
    /// # Safety
    ///
    /// The caller should make sure that:
    /// * `ptr` must denote the memory buffer currently allocated via
    ///   `PageAllocator::alloc`,
    ///
    /// * `len` must be the same size that was used to allocate the
    ///   memory buffer.
    unsafe fn dealloc(ptr: *mut u8, len: usize) {
        // SAFETY: the caller should pass valid `ptr` and `len`.
        unsafe {
            let layout = Layout::from_size_align_unchecked(len * PAGE_SIZE, PAGE_SIZE);
            dealloc(ptr, layout)
        }
    }
}

/// A struct for `PAGE_SIZE` aligned memory buffer.
pub struct Pages {
    ptr: NonNull<u8>,
    len: usize,
    _p: PhantomData<[u8]>,
}

// SAFETY: `Pages` owns the memory buffer, so it can be safely
// transferred across threads.
unsafe impl Send for Pages {}

impl Pages {
    /// Allocate specific number of pages.
    pub fn alloc(len: usize) -> Result<Self> {
        let ptr = PageAllocator::alloc(len).ok_or(Error::with_msg(
            Errno::OutOfMemory,
            "page allocation failed",
        ))?;
        Ok(Self {
            ptr,
            len,
            _p: PhantomData,
        })
    }

    /// Return the number of pages.
    pub fn len(&self) -> usize {
        self.len
    }
}

impl Drop for Pages {
    fn drop(&mut self) {
        // SAFETY: `ptr` is `NonNull` and allocated by `PageAllocator::alloc`
        // with the same size of `len`, so it's valid and safe.
        unsafe { PageAllocator::dealloc(self.ptr.as_mut(), self.len) }
    }
}

impl core::ops::Deref for Pages {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        // SAFETY: `ptr` is `NonNull` and points valid memory with proper length.
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.len * PAGE_SIZE) }
    }
}

impl core::ops::DerefMut for Pages {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: `ptr` is `NonNull` and points valid memory with proper length.
        unsafe { core::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len * PAGE_SIZE) }
    }
}

/// A random number generator.
pub struct Rng;

impl crate::util::Rng for Rng {
    fn new(_seed: &[u8]) -> Self {
        Self
    }

    fn fill_bytes(&self, dest: &mut [u8]) -> Result<()> {
        thread_rng().fill_bytes(dest);
        Ok(())
    }
}

/// A macro to define byte_array_types used by `Aead` or `Skcipher`.
macro_rules! new_byte_array_type {
    ($name:ident, $n:expr) => {
        #[repr(C)]
        #[derive(Copy, Clone, Pod, Debug, Default, Deserialize, Serialize)]
        pub struct $name([u8; $n]);

        impl core::ops::Deref for $name {
            type Target = [u8];

            fn deref(&self) -> &Self::Target {
                self.0.as_slice()
            }
        }

        impl core::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                self.0.as_mut_slice()
            }
        }

        impl crate::util::RandomInit for $name {
            fn random() -> Self {
                use crate::util::Rng;

                let mut result = Self::default();
                let rng = self::Rng::new(&[]);
                rng.fill_bytes(&mut result).unwrap_or_default();
                result
            }
        }
    };
}

const AES_GCM_KEY_SIZE: usize = 16;
const AES_GCM_IV_SIZE: usize = 12;
const AES_GCM_MAC_SIZE: usize = 16;

new_byte_array_type!(AeadKey, AES_GCM_KEY_SIZE);
new_byte_array_type!(AeadIv, AES_GCM_IV_SIZE);
new_byte_array_type!(AeadMac, AES_GCM_MAC_SIZE);

/// An `AEAD` cipher.
pub struct Aead;

impl Aead {
    /// Construct an `Aead` instance.
    pub fn new() -> Self {
        Self
    }
}

impl crate::util::Aead for Aead {
    type Key = AeadKey;
    type Iv = AeadIv;
    type Mac = AeadMac;

    fn encrypt(
        &self,
        input: &[u8],
        key: &Self::Key,
        iv: &Self::Iv,
        aad: &[u8],
        output: &mut [u8],
    ) -> Result<Self::Mac> {
        let mut mac = AeadMac::default();

        rsgx_rijndael128GCM_encrypt(&key.0, input, &iv.0, aad, output, &mut mac.0)
            .map_err(|_| Error::with_msg(Errno::EncryptFailed, "aead encrypt failed"))?;

        Ok(mac)
    }

    fn decrypt(
        &self,
        input: &[u8],
        key: &Self::Key,
        iv: &Self::Iv,
        aad: &[u8],
        mac: &Self::Mac,
        output: &mut [u8],
    ) -> Result<()> {
        rsgx_rijndael128GCM_decrypt(&key.0, input, &iv.0, aad, &mac.0, output).map_err(|e| {
            let errno = if e == sgx_status_t::SGX_ERROR_MAC_MISMATCH {
                Errno::MacMismatched
            } else {
                Errno::DecryptFailed
            };
            Error::with_msg(errno, "aead decrypt failed")
        })
    }
}

const AES_CTR_KEY_SIZE: usize = 16;
const AES_CTR_IV_SIZE: usize = 16;

new_byte_array_type!(SkcipherKey, AES_CTR_KEY_SIZE);
new_byte_array_type!(SkcipherIv, AES_CTR_IV_SIZE);

/// A symmetric key cipher.
pub struct Skcipher;

impl Skcipher {
    /// Construct a `Skcipher` instance.
    pub fn new() -> Self {
        Self
    }
}

impl crate::util::Skcipher for Skcipher {
    type Key = SkcipherKey;
    type Iv = SkcipherIv;

    fn encrypt(
        &self,
        input: &[u8],
        key: &Self::Key,
        iv: &Self::Iv,
        output: &mut [u8],
    ) -> Result<()> {
        let ctr_inc_bits = 128_u32;
        let mut ctr = *iv;

        rsgx_aes_ctr_encrypt(&key.0, input, &mut ctr.0, ctr_inc_bits, output)
            .map_err(|_| Error::with_msg(Errno::EncryptFailed, "skcipher encrypt failed"))
    }

    fn decrypt(
        &self,
        input: &[u8],
        key: &Self::Key,
        iv: &Self::Iv,
        output: &mut [u8],
    ) -> Result<()> {
        let ctr_inc_bits = 128_u32;
        let mut ctr = *iv;

        rsgx_aes_ctr_decrypt(&key.0, input, &mut ctr.0, ctr_inc_bits, output)
            .map_err(|_| Error::with_msg(Errno::DecryptFailed, "skcipher decrypt failed"))
    }
}
