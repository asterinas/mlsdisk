//! Std user space implementations.

use crate::error::Errno;
use crate::prelude::{Error, Result};
use core::marker::PhantomData;
use core::ptr::NonNull;
use libc;
use openssl::rand::rand_bytes;
use openssl::symm::{decrypt, decrypt_aead, encrypt, encrypt_aead, Cipher};
use pod::Pod;
use serde::{Deserialize, Serialize};

/// Reuse the `Mutex` and `MutexGuard` implementation.
pub use spin::{
    Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockUpgradableGuard, RwLockWriteGuard,
};

/// Unique ID for the OS thread.
#[derive(PartialEq, Eq, Debug)]
#[repr(transparent)]
pub struct Tid(libc::pthread_t);

/// A struct to get the current thread id.
pub struct CurrentThread;

impl CurrentThread {
    pub fn id() -> Tid {
        // SAFETY: calling extern "C" to get the thread_id is safe here.
        Tid(unsafe { libc::pthread_self() })
    }
}

pub const PAGE_SIZE: usize = 4096;

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
            let layout =
                alloc::alloc::Layout::from_size_align_unchecked(len * PAGE_SIZE, PAGE_SIZE);
            let ptr = alloc::alloc::alloc(layout);
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
            let layout =
                alloc::alloc::Layout::from_size_align_unchecked(len * PAGE_SIZE, PAGE_SIZE);
            alloc::alloc::dealloc(ptr, layout)
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
        let ptr = PageAllocator::alloc(len).ok_or(Error::new(Errno::NoMemory))?;
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
        rand_bytes(dest).map_err(|_| Error::new(Errno::OsSpecUnknown))
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

        let result = encrypt_aead(Cipher::aes_128_gcm(), key, Some(iv), aad, input, &mut mac)
            .map_err(|_| Error::new(Errno::EncryptFault))?;
        output.copy_from_slice(result.as_slice());
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
        let result = decrypt_aead(Cipher::aes_128_gcm(), key, Some(iv), aad, input, mac)
            .map_err(|_| Error::new(Errno::DecryptFault))?;
        output.copy_from_slice(result.as_slice());
        Ok(())
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
        let result = encrypt(Cipher::aes_128_ctr(), key, Some(iv), input)
            .map_err(|_| Error::new(Errno::EncryptFault))?;
        output.copy_from_slice(result.as_slice());
        Ok(())
    }

    fn decrypt(
        &self,
        input: &[u8],
        key: &Self::Key,
        iv: &Self::Iv,
        output: &mut [u8],
    ) -> Result<()> {
        let result = decrypt(Cipher::aes_128_ctr(), key, Some(iv), input)
            .map_err(|_| Error::new(Errno::DecryptFault))?;
        output.copy_from_slice(result.as_slice());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn thread_id() {
        use super::CurrentThread;

        let tid0 = CurrentThread::id();
        let tid1 = CurrentThread::id();
        assert_eq!(tid0, tid1);
        println!("current thread_id: {:?}", tid0);
    }

    #[test]
    fn page() {
        use super::{Pages, PAGE_SIZE};

        let page = Pages::alloc(3).unwrap();
        let align = (page.ptr.as_ptr() as usize) & (PAGE_SIZE - 1);
        assert_eq!(align, 0);
        assert_eq!(page.len(), 3);
        assert_eq!((*page).len(), 3 * PAGE_SIZE);
    }

    #[test]
    fn mutex() {
        use super::Mutex;

        let x: Mutex<i32> = Mutex::new(0);
        let mut y = x.lock();
        *y += 1;
        assert_eq!(*y, 1);
    }

    #[test]
    fn rng() {
        use super::Rng as OsRng;
        use crate::util::Rng;

        let rng = OsRng::new(&[]);
        let mut buf = [0u8; 16];
        rng.fill_bytes(&mut buf).unwrap();
        println!("random bytes: {:?}", buf);
    }

    #[test]
    fn aead() {
        use super::{Aead as OsAead, AeadIv, AeadKey};
        use crate::util::{Aead, RandomInit};

        let data = b"Some Crypto Text";
        let key = AeadKey::random();
        let iv = AeadIv::random();

        let aead = OsAead::new();
        let mut ciphertext = [0u8; 16];
        let mac = aead.encrypt(data, &key, &iv, &[], &mut ciphertext).unwrap();
        println!("aead data: {:?}", data);
        println!("aead key: {:?}", &key);
        println!("aead iv: {:?}", &iv);
        println!("aead ciphertext: {:?}", &ciphertext);
        println!("aead mac: {:?}", &mac);

        let mut plaintext = [0u8; 16];
        aead.decrypt(&ciphertext, &key, &iv, &[], &mac, &mut plaintext)
            .unwrap();
        println!("aead plaintext: {:?}", &plaintext);
        assert_eq!(data, &plaintext);
    }

    #[test]
    fn skcipher() {
        use super::{Skcipher as OsSkcipher, SkcipherIv, SkcipherKey};
        use crate::util::{RandomInit, Skcipher};

        let data = b"Some Crypto Text";
        let key = SkcipherKey::random();
        let iv = SkcipherIv::random();

        let skcipher = OsSkcipher::new();
        let mut ciphertext = [0u8; 16];
        skcipher.encrypt(data, &key, &iv, &mut ciphertext).unwrap();
        println!("skcipher data: {:?}", data);
        println!("skcipher key: {:?}", &key);
        println!("skcipher iv: {:?}", &iv);
        println!("skcipher ciphertext: {:?}", &ciphertext);

        let mut plaintext = [0u8; 16];
        skcipher
            .decrypt(&ciphertext, &key, &iv, &mut plaintext)
            .unwrap();
        println!("skcipher plaintext: {:?}", &plaintext);
        assert_eq!(data, &plaintext);
    }
}
