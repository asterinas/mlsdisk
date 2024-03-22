// SPDX-License-Identifier: GPL-2.0
// Copyright (c) 2023 Ant Group CO., Ltd.

//! Authenticated Encryption with Associated Data (AEAD).
//!
//! # Examples
//!
//! ```rust
//! # use bindings::crypto::{Aead, AeadReq};
//! use alloc::boxed::Box;
//!
//! let aes_gcm = Box::pin_init(Aead::new(c_str!("gcm(aes)"), 0, 0)).unwrap();
//!
//! let key: [u8; 16] = [0; 16];
//! let iv: [u8; 12] = [0; 12];
//! let plaintext: [u8; 64] = [0; 64];
//!
//! let _ = aes_gcm.set_key(&key);
//! let req = aes_gcm.alloc_request().unwrap();
//! let mut ciphertext = [0u8; 64];
//! let mut mac = [0u8; 16];
//! if let Err(e) = req.encrypt(&[], &plaintext, &iv, &mut ciphertext, &mut mac) {
//!     pr_info!("aead encrypt failed, errno: {}\n", e.to_errno());
//! }
//!
//! let req = aes_gcm.alloc_request().unwrap();
//! let mut decrypted = [0u8; 64];
//! if let Err(e) = req.decrypt(&[], &ciphertext, &mac, &iv, &mut decrypted) {
//!     pr_info!("aead decrypt failed, errno: {}\n", e.to_errno());
//! }
//! assert_eq!(decrypted, plaintext);
//! ```
//!
//! C header: [`include/crypto/aead.h`](include/crypto/aead.h)

use core::ptr::{addr_of_mut, NonNull};
use kernel::{
    bindings::{GFP_KERNEL, IS_ERR, PTR_ERR},
    error::to_result,
    new_mutex,
    prelude::*,
    str::CStr,
    sync::Mutex,
};

use super::SgTable;

/// An Authenticated Encryption with Associated Data (AEAD) wrapper.
///
/// The `crypto_aead` pointer is constructed by the `new` initializer.
/// The recommended way to create such instances is with the [`pin_init`].
///
/// # Examples
///
/// The following is examples of creating [`Aead`] instances.
///
/// ```rust
/// use kernel::{c_str, stack_try_pin_init};
/// # use core::pin::Pin;
/// # use crate::crypto::Aead;
///
/// // Allocates an instance on stack.
/// stack_try_pin_init!(let foo = Aead::new(c_str!("gcm(aes)"), 0, 0));
/// let foo: Result<Pin<&mut Aead>> = foo;
///
/// // Allocate an instance by Box::pin_init.
/// let bar: Result<Pin<Box<Aead>>> = Box::pin_init(Aead::new(c_str!("gcm(aes)"), 0, 0));
/// ```
#[pin_data(PinnedDrop)]
pub struct Aead {
    #[pin]
    write_access: Mutex<()>,
    aead: NonNull<crate::crypto_aead>,
}

// SAFETY: `aead` is pointer to `crypto_aead`, which could be safely
// transferred across threads.
unsafe impl Send for Aead {}

// SAFETY: `write_access` ensure the synchronization, so it could be
// used concurrently from multiple threads.
unsafe impl Sync for Aead {}

impl Aead {
    /// Constructs a new initializer.
    ///
    /// Try to allocate an AEAD cipher handler, `alg_name` specifies the driver
    /// name of the cipher, `type_` specifies the type of the cipher, `mask`
    /// specifies the mask for the cipher.
    ///
    /// Returns a type impl `PinInit<Aead>` in case of success, or an error
    /// code in `Error`.
    pub fn new(alg_name: &'static CStr, type_: u32, mask: u32) -> impl PinInit<Self, Error> {
        try_pin_init!(Self {
            write_access <- new_mutex!(()),
            // SAFETY: `alg_name` has static lifetimes and live indefinitely.
            // Any error will be checked by `IS_ERR`.
            aead: unsafe {
                let aead = crate::crypto_alloc_aead(alg_name.as_char_ptr(), type_, mask);
                let const_ptr: *const core::ffi::c_void = aead.cast();
                if IS_ERR(const_ptr) {
                    let err = PTR_ERR(const_ptr) as i32;
                    return Err(to_result(err).err().unwrap());
                }
                NonNull::new_unchecked(aead)
            },
        })
    }

    /// Allocates a cipher request, which could be used to handle the cipher
    /// operations later.
    pub fn alloc_request(&self) -> Result<AeadReq> {
        // SAFETY: `self.aead` is a valid cipher.
        let req = unsafe { crate::aead_request_alloc(self.aead.as_ptr(), GFP_KERNEL) };
        if req.is_null() {
            return Err(ENOMEM);
        }

        Ok(AeadReq {
            req: NonNull::new(req).unwrap(),
        })
    }

    /// Returns the size (in bytes) of the IV for the aead referenced by the
    /// cipher handle. This IV size may be zero if the cipher does not need an IV.
    pub fn ivsize(&self) -> usize {
        // SAFETY: `self.aead` is constructed by `new`, it is non-null and valid.
        unsafe {
            let crypto_alg = (*self.aead.as_ptr()).base.__crt_alg;
            let aead_alg = crate::container_of!(crypto_alg, crate::aead_alg, base);
            (*aead_alg).ivsize as _
        }
    }

    /// Returns the maximum size (in bytes) of the authentication data for the
    /// AEAD cipher referenced by the AEAD cipher handle. The authentication data
    /// size may be zero if the cipher implements a hard-coded maximum.
    pub fn authsize(&self) -> usize {
        // SAFETY: `self.aead` is constructed by `new`, it is non-null and valid.
        unsafe { (*self.aead.as_ptr()).authsize as _ }
    }

    /// Returns the size (in bytes) of the request data structure. The `aead_request`
    /// data structure contains all pointers to data required for the AEAD cipher
    /// operation.
    pub fn reqsize(&self) -> usize {
        // SAFETY: `self.aead` is constructed by `new`, it is non-null and valid.
        unsafe { (*self.aead.as_ptr()).reqsize as _ }
    }

    /// Returns the block size (in bytes) for the AEAD referenced with the cipher
    /// handle. The caller may use that information to allocate appropriate memory
    /// for the data returned by the encryption or decryption operation.
    pub fn blocksize(&self) -> usize {
        // SAFETY: `self.aead` is constructed by `new`, it is non-null and valid.
        unsafe {
            let crypto_alg = (*self.aead.as_ptr()).base.__crt_alg;
            (*crypto_alg).cra_blocksize as _
        }
    }

    /// Sets authentication data size, should be called before `alloc_request`.
    ///
    /// AEAD requires an authentication tag (or MAC) in addition to the associated data.
    ///
    /// Returns `Ok(())` if the setting of the `authsize` was successful, or an error
    /// code in [`Error`].
    pub fn set_authsize(&self, authsize: usize) -> Result {
        let write_access = self.write_access.lock();
        // SAFETY: `self.aead` is constructed by `new`, it is non-null and valid.
        unsafe {
            let err = crate::crypto_aead_setauthsize(self.aead.as_ptr(), authsize as u32);
            to_result(err)
        }
    }

    /// Sets key for AEAD cipher, should be called before `alloc_request`.
    ///
    /// The key length determines the cipher type. Many block ciphers implement
    /// different cipher modes depending on the key size, such as AES-128 vs AES-192
    /// vs. AES-256. When providing a 16 byte key for an AES cipher handle, AES-128
    /// is performed.
    ///
    /// Returns `Ok(())` if the setting of the key was successful, or an error
    /// code in [`Error`].
    pub fn set_key(&self, key: &[u8]) -> Result {
        let write_access = self.write_access.lock();
        // SAFETY: `self.aead` is constructed by `new`, it is non-null and valid.
        unsafe {
            to_result(crate::crypto_aead_setkey(
                self.aead.as_ptr(),
                key.as_ptr(),
                key.len() as u32,
            ))
        }
    }
}

#[pinned_drop]
impl PinnedDrop for Aead {
    fn drop(self: Pin<&mut Self>) {
        // SAFETY: `self.aead` is constructed by `new`, it is non-null and valid.
        unsafe {
            let aead = self.aead.as_ptr();
            let base = addr_of_mut!((*aead).base);
            crate::crypto_destroy_tfm(aead as _, base);
        }
    }
}

/// An `Aead` request.
///
/// This is constructed by the `Aead`, and is used to construct a
/// cipher operation then handle it.
pub struct AeadReq {
    req: NonNull<crate::aead_request>,
}

impl AeadReq {
    /// The kernel crypto APIs use `scatterlist` to transfer memory buffer.
    ///
    /// The memory structure for AEAD cipher operation has the following structure:
    ///
    /// - AEAD encryption input:  assoc data || plaintext
    /// - AEAD encryption output: assoc data || ciphertext || auth tag
    /// - AEAD decryption input:  assoc data || ciphertext || auth tag
    /// - AEAD decryption output: assoc data || plaintext
    const SCATTERLIST_SIZE: usize = 3;

    /// Sets information needed to perform the cipher operation.
    ///
    /// The `src` and `dst` hold the associated data concatenated with the
    /// plaintext or ciphertext. And `assoclen` specifies the length of
    /// associated data in `src`, `cryptlen` specifies the number
    /// of bytes to process from `src`, `iv` specifies the IV for the cipher
    /// operation which must comply with the IV size returned by `Aead::ivsize`.
    fn set_crypt(&self, src: &SgTable, dst: &SgTable, assoclen: usize, cryptlen: usize, iv: &[u8]) {
        // SAFETY: `self.req` is non-null and valid.
        unsafe {
            let req_src = addr_of_mut!((*self.req.as_ptr()).src);
            *req_src = (*src.0.get()).sgl;
            let req_dst = addr_of_mut!((*self.req.as_ptr()).dst);
            *req_dst = (*dst.0.get()).sgl;
            let req_assoclen = addr_of_mut!((*self.req.as_ptr()).assoclen);
            *req_assoclen = assoclen as _;
            let req_cryptlen = addr_of_mut!((*self.req.as_ptr()).cryptlen);
            *req_cryptlen = cryptlen as _;
            let req_iv = addr_of_mut!((*self.req.as_ptr()).iv);
            *req_iv = iv.as_ptr() as _;
        }
    }

    /// Handles encryption request.
    ///
    /// Returns `Ok(())` if the cipher operation was successful, or an error
    /// code in [`Error`].
    pub fn encrypt(
        self,
        aad: &[u8],
        input: &[u8],
        iv: &[u8],
        output: &mut [u8],
        mac: &mut [u8],
    ) -> Result {
        let mut src = SgTable::alloc(Self::SCATTERLIST_SIZE)?;
        let mut dst = SgTable::alloc(Self::SCATTERLIST_SIZE)?;

        // SAFETY: the index does not exceed `SCATTERLIST_SIZE`.
        unsafe {
            src.set_buf(0, aad);
            src.set_buf(1, input);
            dst.set_buf(0, aad);
            dst.set_buf(1, output);
            dst.set_buf(2, mac);
        }
        self.set_crypt(&src, &dst, aad.len(), input.len(), iv);

        // SAFETY: `self.req` is non-null and valid.
        unsafe { to_result(crate::crypto_aead_encrypt(self.req.as_ptr())) }
    }

    /// Handles decryption request.
    ///
    /// Returns `Ok(())` if the cipher operation was successful, or an error
    /// code in [`Error`].
    pub fn decrypt(
        self,
        aad: &[u8],
        input: &[u8],
        mac: &[u8],
        iv: &[u8],
        output: &mut [u8],
    ) -> Result {
        let mut src = SgTable::alloc(Self::SCATTERLIST_SIZE)?;
        let mut dst = SgTable::alloc(Self::SCATTERLIST_SIZE)?;

        // SAFETY: the index does not exceed `SCATTERLIST_SIZE`.
        unsafe {
            src.set_buf(0, aad);
            src.set_buf(1, input);
            src.set_buf(2, mac);
            dst.set_buf(0, aad);
            dst.set_buf(1, output);
        }
        self.set_crypt(&src, &dst, aad.len(), input.len() + mac.len(), iv);

        // SAFETY: `self.req` is non-null and valid.
        unsafe { to_result(crate::crypto_aead_decrypt(self.req.as_ptr())) }
    }
}

impl Drop for AeadReq {
    fn drop(&mut self) {
        // SAFETY: `self.req` is non-null and valid.
        unsafe { crate::kfree_sensitive(self.req.as_ptr() as _) };
    }
}
