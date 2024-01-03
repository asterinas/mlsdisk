// SPDX-License-Identifier: GPL-2.0
// Copyright (c) 2023 Ant Group CO., Ltd.

//! A kernel rwlock.
//!
//! This module allows Rust code to use the kernel's `struct rw_semaphore`.

use core::{marker::PhantomData, mem::transmute};
use kernel::prelude::*;

use super::{Backend, Guard, Lock};
use crate::sync::LockClassKey;

/// Creates a [`RwLock`] initialiser with the given name and a newly-created lock class.
///
/// It uses the name if one is given, otherwise it generates one based on the file name and line
/// number.
#[macro_export]
macro_rules! new_rwlock {
    ($inner:expr $(, $name:literal)? $(,)?) => {
        $crate::sync::RwLock::new(
            $inner, $crate::optional_name!($($name)?), $crate::static_lock_class!())
    };
}

/// A read/write lock.
///
/// Exposes the kernel's `struct rw_semaphore`. It allows multiple readers to acquire it
/// concurrently, but only one writer at a time. On contention, waiters sleep.
///
/// Instances of [`RwLock`] need a lock class and to be pinned. The recommended way to
/// create such instances is with the [`pin_init`](crate::pin_init) and [`new_rwlock`] macros.
///
/// # Examples
///
/// The following example shows how to declare, allocate and initialize a `RwLock`.
/// ```
/// use crate::sync::RwLock;
///
/// // Allocate a boxed `RwLock`.
/// let lock = Box::pin_init(new_rwlock!(5))?;
///
/// // Many reader locks can be held at once.
/// {
///     let r1 = lock.read();
///     let r2 = lock.read();
///     assert_eq!(*r1, 5);
///     assert_eq!(*r2, 5);
/// } // read locks are dropped at this point
///
/// // Only one write lock may be held.
/// {
///     let mut w = lock.write();
///     *w += 1;
///     assert_eq!(*w, 6);
///     let r = lock.try_read();
///     assert_eq!(r.is_ok(), false);
/// } // write lock is dropped here
///
/// // Try to get a read lock.
/// let r = lock.try_read();
/// assert_eq!(r.is_ok(), true);
/// assert_eq!(*r.unwrap(), 6);
/// ```
#[repr(transparent)]
#[pin_data]
pub struct RwLock<T> {
    #[pin]
    inner: Lock<T, RwLockBackend<Read>>,
}

/// An enumeration of possible errors associated with `RwLock`.
#[derive(Debug)]
pub enum LockError {
    /// Failed on try_read.
    TryRead,
    /// Failed on read_interruptible.
    ReadInterruptible,
    /// Failed on read_killable,
    ReadKillable,
    /// Failed on try_write.
    TryWrite,
    /// Failed on write_killable.
    WriteKillable,
}

impl core::fmt::Display for LockError {
    fn fmt(
        &self,
        fmt: &mut core::fmt::Formatter<'_>,
    ) -> core::result::Result<(), core::fmt::Error> {
        fmt.write_str("lock failed on ")?;
        let reason = match self {
            LockError::TryRead => "down_read_trylock",
            LockError::ReadInterruptible => "down_read_interruptible",
            LockError::ReadKillable => "down_read_killable",
            LockError::TryWrite => "down_write_trylock",
            LockError::WriteKillable => "down_write_killable",
        };
        fmt.write_str(reason)
    }
}

impl<T> RwLock<T> {
    /// Constructs a new `RwLock` initializer.
    pub fn new(t: T, name: &'static CStr, key: &'static LockClassKey) -> impl PinInit<Self> {
        pin_init!(Self {
            inner <- Lock::new(t, name, key)
        })
    }

    /// Locks this `RwLock` with shared read access, blocking the current thread
    /// until it can be acquired (set the `TASK_UNINTERRUPTIBLE` flag).
    pub fn read(&self) -> Guard<'_, T, RwLockBackend<Read>> {
        // SAFETY: the markers are ZST, so it's safe to transmute them.
        let inner: &Lock<T, RwLockBackend<Read>> = unsafe { transmute(&self.inner) };
        inner.lock()
    }

    /// Attempts to acquire this `RwLock` with shared read access.
    ///
    /// If the access could not be granted at this time, then `Err` is returned.
    /// Otherwise, an RAII guard is returned which will release the shared access
    /// when it is dropped.
    ///
    /// This function does not block.
    pub fn try_read(&self) -> Result<Guard<'_, T, RwLockBackend<Read>>, LockError> {
        // SAFETY: the markers are ZST, so it's safe to transmute them.
        let inner: &Lock<T, RwLockBackend<TryRead>> = unsafe { transmute(&self.inner) };
        // SAFETY: instances are created by `pin_init`, so it's state is valid.
        let state = unsafe { RwLockBackend::<TryRead>::lock(inner.state.get()) };
        match state {
            // SAFETY: `try_read` successes, now we owns the lock.
            Some(val) if val == 1 => Ok(unsafe { Guard::new(inner, state).into() }),
            _ => Err(LockError::TryRead),
        }
    }

    /// Locks this `RwLock` with shared read access, blocking the current thread
    /// until it can be acquired (set the `TASK_INTERRUPTIBLE` flag).
    ///
    /// Return `Err` if it's been interrupted by a signal (acquire the lock failed).
    pub fn read_interruptible(&self) -> Result<Guard<'_, T, RwLockBackend<Read>>, LockError> {
        // SAFETY: the markers are ZST, so it's safe to transmute them.
        let inner: &Lock<T, RwLockBackend<ReadInterruptible>> = unsafe { transmute(&self.inner) };
        // SAFETY: instances are created by `pin_init`, so it's state is valid.
        let state = unsafe { RwLockBackend::<ReadInterruptible>::lock(inner.state.get()) };
        match state {
            // SAFETY: `read_interruptible` successes, now we owns the lock.
            Some(val) if val == 0 => Ok(unsafe { Guard::new(inner, state).into() }),
            _ => Err(LockError::ReadInterruptible),
        }
    }

    /// Locks this `RwLock` with shared read access, blocking the current thread
    /// until it can be acquired (set the `TASK_KILLABLE` flag).
    ///
    /// Return `Err` if it's been interrupted by a signal (acquire the lock failed).
    pub fn read_killable(&self) -> Result<Guard<'_, T, RwLockBackend<Read>>, LockError> {
        // SAFETY: the markers are ZST, so it's safe to transmute them.
        let inner: &Lock<T, RwLockBackend<ReadKillable>> = unsafe { transmute(&self.inner) };
        // SAFETY: instances are created by `pin_init`, so it's state is valid.
        let state = unsafe { RwLockBackend::<ReadKillable>::lock(inner.state.get()) };
        match state {
            // SAFETY: `read_killable` successes, now we owns the lock.
            Some(val) if val == 0 => Ok(unsafe { Guard::new(inner, state).into() }),
            _ => Err(LockError::ReadKillable),
        }
    }

    /// Locks this `RwLock` with exclusive write access, blocking the current
    /// thread until it can be acquired (set the `TASK_UNINTERRUPTIBLE` flag).
    pub fn write(&self) -> Guard<'_, T, RwLockBackend<Write>> {
        // SAFETY: the markers is ZST, so it's safe to transmute them.
        let inner: &Lock<T, RwLockBackend<Write>> = unsafe { transmute(&self.inner) };
        inner.lock()
    }

    /// Attempts to lock this `RwLock` with exclusive write access.
    ///
    /// If the lock could not be acquired at this time, then `Err` is returned.
    /// Otherwise, an RAII guard is returned which will release the lock when
    /// it is dropped.
    ///
    /// This function does not block.
    pub fn try_write(&self) -> Result<Guard<'_, T, RwLockBackend<Write>>, LockError> {
        // SAFETY: the markers are ZST, so it's safe to transmute them.
        let inner: &Lock<T, RwLockBackend<TryWrite>> = unsafe { transmute(&self.inner) };
        // SAFETY: instances are created by `pin_init`, so it's state is valid.
        let state = unsafe { RwLockBackend::<TryWrite>::lock(inner.state.get()) };
        match state {
            // SAFETY: `try_write` successes, now we owns the lock.
            Some(val) if val == 1 => Ok(unsafe { Guard::new(inner, state).into() }),
            _ => Err(LockError::TryWrite),
        }
    }

    /// Locks this `RwLock` with exclusive write access, blocking the current
    /// thread until it can be acquired (set the `TASK_KILLABLE` flag).
    ///
    /// Return `Err` if it's been interrupted by a signal (acquire the lock failed).
    pub fn write_killable(&self) -> Result<Guard<'_, T, RwLockBackend<Write>>, LockError> {
        // SAFETY: the markers are ZST, so it's safe to transmute them.
        let inner: &Lock<T, RwLockBackend<WriteKillable>> = unsafe { transmute(&self.inner) };
        // SAFETY: instances are created by `pin_init`, so it's state is valid.
        let state = unsafe { RwLockBackend::<WriteKillable>::lock(inner.state.get()) };
        match state {
            // SAFETY: `write_killable` successes, now we owns the lock.
            Some(val) if val == 0 => Ok(unsafe { Guard::new(inner, state).into() }),
            _ => Err(LockError::WriteKillable),
        }
    }

    /// Downgrade write lock to read lock.
    pub fn downgrade<'a>(
        this: Guard<'a, T, RwLockBackend<Write>>,
    ) -> Guard<'a, T, RwLockBackend<Read>> {
        // SAFETY: instances are created by `pin_init`, so it's state is valid.
        // The markers are ZST, so it's safe to transmute them.
        unsafe {
            crate::downgrade_write(this.lock.state.get());
            transmute(this)
        }
    }
}

/// A kernel `struct rw_semaphore` lock backend with a ZST marker `M`.
///
/// The markers are used to bind different locking functions.
pub struct RwLockBackend<M> {
    _m: PhantomData<M>,
}

/// The `read` marker.
pub struct Read;

/// The `try_read` marker.
pub struct TryRead;

/// The `read_interruptible` marker.
pub struct ReadInterruptible;

/// The `read_killable` marker.
pub struct ReadKillable;

/// The `write` marker.
pub struct Write;

/// The `try_write` marker.
pub struct TryWrite;

/// The `write_killable` marker.
pub struct WriteKillable;

/// Implement `Into` for markers.
macro_rules! impl_into {
    ($(($($src:ident$(,)?)+) -> $dst:ident,)+) => {
        $($(
            impl<'a, T> Into<Guard<'a, T, RwLockBackend<$dst>>> for Guard<'a, T, RwLockBackend<$src>> {
                fn into(self) -> Guard<'a, T, RwLockBackend<$dst>> {
                    // SAFETY: the markers are ZST, so it's safe to transmute them.
                    unsafe { transmute(self) }
                }
            }
        )+)+
    };
}

/// A trait to bind the lock/unlock functions of `Backend` for markers.
trait RwLockFn {
    /// The real lock function of the marker.
    unsafe fn lock(ptr: *mut crate::rw_semaphore) -> Option<core::ffi::c_int>;
    /// The real unlock function of the marker.
    unsafe fn unlock(ptr: *mut crate::rw_semaphore);
}

/// Bind lock/unlock functions for markers.
macro_rules! bind_rwlock_fn {
    (@lock $fn:ident) => {
        unsafe fn lock(ptr: *mut crate::rw_semaphore) -> Option<core::ffi::c_int> {
            unsafe { crate::$fn(ptr) };
            None
        }
    };
    (@lock $fn:ident, fallible) => {
        unsafe fn lock(ptr: *mut crate::rw_semaphore) -> Option<core::ffi::c_int> {
            Some(unsafe { crate::$fn(ptr) })
        }
    };
    (@unlock $fn:ident) => {
        unsafe fn unlock(ptr: *mut crate::rw_semaphore) {
            unsafe { crate::$fn(ptr) }
        }
    };
    ($(($type:ident, $lock_fn:ident, $unlock_fn:ident $(, $fallible:tt)?),)+) => {
        $(impl RwLockFn for $type {
            bind_rwlock_fn!(@lock $lock_fn $(, $fallible)?);
            bind_rwlock_fn!(@unlock $unlock_fn);
        })+
    };
}

impl_into! {
    (TryRead, ReadInterruptible, ReadKillable) -> Read,
    (TryWrite, WriteKillable) -> Write,
}

bind_rwlock_fn! {
    (Read, down_read, up_read),
    (TryRead, down_read_trylock, up_read, fallible),
    (ReadInterruptible, down_read_interruptible, up_read, fallible),
    (ReadKillable, down_read_killable, up_read, fallible),
    (Write, down_write, up_write),
    (TryWrite, down_write_trylock, up_write, fallible),
    (WriteKillable, down_write_killable, up_write, fallible),
}

// SAFETY: The underlying kernel `struct rw_semaphore` object ensures mutual exclusion.
unsafe impl<S: RwLockFn> Backend for RwLockBackend<S> {
    type State = crate::rw_semaphore;
    type GuardState = Option<core::ffi::c_int>;

    unsafe fn init(
        ptr: *mut Self::State,
        name: *const core::ffi::c_char,
        key: *mut crate::lock_class_key,
    ) {
        // SAFETY: The safety requirements ensure that `ptr` is valid for writes, and `name` and
        // `key` are valid for read indefinitely.
        unsafe { crate::__init_rwsem(ptr, name, key) }
    }

    unsafe fn lock(ptr: *mut Self::State) -> Self::GuardState {
        // SAFETY: The safety requirements of this function ensure that `ptr` points to valid
        // memory, and that it has been initialized before.
        unsafe { S::lock(ptr) }
    }

    unsafe fn unlock(ptr: *mut Self::State, _guard_state: &Self::GuardState) {
        // SAFETY: The safety requirements of this function ensure that `ptr` is valid.
        unsafe { S::unlock(ptr) }
    }
}
