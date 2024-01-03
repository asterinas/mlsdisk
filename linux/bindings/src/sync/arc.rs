// SPDX-License-Identifier: GPL-2.0
// Copyright (c) 2023 Rust-for-Linux
// Copyright (c) 2023 Ant Group CO., Ltd.

//! A reference-counted pointer.

use alloc::boxed::Box;
use core::{
    alloc::AllocError,
    fmt,
    marker::{PhantomData, Unsize},
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
    ptr::{addr_of_mut, NonNull},
};
use kernel::{prelude::*, types::Opaque};

mod std_vendor;

/// A reference-counted pointer to an instance of `T`.
///
/// The reference count is incremented when new instances of [`Arc`] are created, and decremented
/// when they are dropped. When the count reaches zero, the underlying `T` is also dropped.
///
/// # Invariants
///
/// The reference count on an instance of [`Arc`] is always non-zero.
/// The object pointed to by [`Arc`] is always pinned.
///
/// # Examples
///
/// ```
/// use kernel::sync::Arc;
///
/// struct Example {
///     a: u32,
///     b: u32,
/// }
///
/// // Create a ref-counted instance of `Example`.
/// let obj = Arc::try_new(Example { a: 10, b: 20 })?;
///
/// // Get a new pointer to `obj` and increment the refcount.
/// let cloned = obj.clone();
///
/// // Assert that both `obj` and `cloned` point to the same underlying object.
/// assert!(core::ptr::eq(&*obj, &*cloned));
///
/// // Destroy `obj` and decrement its refcount.
/// drop(obj);
///
/// // Check that the values are still accessible through `cloned`.
/// assert_eq!(cloned.a, 10);
/// assert_eq!(cloned.b, 20);
///
/// // The refcount drops to zero when `cloned` goes out of scope, and the memory is freed.
/// # Ok::<(), Error>(())
/// ```
///
/// Using `Arc<T>` as the type of `self`:
///
/// ```
/// use kernel::sync::Arc;
///
/// struct Example {
///     a: u32,
///     b: u32,
/// }
///
/// impl Example {
///     fn take_over(self: Arc<Self>) {
///         // ...
///     }
///
///     fn use_reference(self: &Arc<Self>) {
///         // ...
///     }
/// }
///
/// let obj = Arc::try_new(Example { a: 10, b: 20 })?;
/// obj.use_reference();
/// obj.take_over();
/// # Ok::<(), Error>(())
/// ```
///
/// Coercion from `Arc<Example>` to `Arc<dyn MyTrait>`:
///
/// ```
/// use kernel::sync::Arc;
///
/// trait MyTrait {
///     // Trait has a function whose `self` type is `Arc<Self>`.
///     fn example1(self: Arc<Self>) {}
/// }
///
/// struct Example;
/// impl MyTrait for Example {}
///
/// // `obj` has type `Arc<Example>`.
/// let obj: Arc<Example> = Arc::try_new(Example)?;
///
/// // `coerced` has type `Arc<dyn MyTrait>`.
/// let coerced: Arc<dyn MyTrait> = obj;
/// # Ok::<(), Error>(())
/// ```
pub struct Arc<T: ?Sized> {
    ptr: NonNull<ArcInner<T>>,
    _p: PhantomData<ArcInner<T>>,
}

/// `Weak` is a version of [`Arc`] that holds a non-owning reference to the
/// managed allocation. The allocation is accessed by calling [`upgrade`] on the `Weak`
/// pointer, which returns an [`Option<Arc<T>>`].
///
/// The typical way to obtain a `Weak` pointer is to call [`Arc::downgrade`].
///
/// # Examples
///
/// ```
/// use kernel::sync::{Arc, Weak};
///
/// struct Example {
///     a: u32,
///     b: u32,
/// }
///
/// // Create a `Arc` instance of `Example`.
/// let obj = Arc::try_new(Example { a: 10, b: 20 })?;
///
/// // Get a weak reference to `obj` and increment the weak refcount.
/// let weak = Arc::downgrade(&obj);
/// assert_eq!(Weak::strong_count(&weak), 1);
/// assert_eq!(Weak::weak_count(&weak), 1);
///
/// // Attempts to upgrade the `Weak` pointer.
/// let upgrade = weak.upgrade();
/// assert_eq!(upgrade.is_some(), true);
/// let upgrade = upgrade.unwrap();
/// assert_eq!(Weak::strong_count(&weak), 2);
/// assert_eq!(Weak::weak_count(&weak), 1);
///
/// // Drop `obj` and decrement its refcount. The values are still accessible
/// // through `upgrade`.
/// drop(obj);
/// assert_eq!(upgrade.a, 10);
/// assert_eq!(upgrade.b, 20);
///
/// drop(upgrade);
/// let upgrade = weak.upgrade();
/// assert_eq!(upgrade.is_some(), false);
/// ```
///
/// [`upgrade`]: Weak::upgrade
pub struct Weak<T: ?Sized> {
    ptr: NonNull<ArcInner<T>>,
}

#[pin_data]
#[repr(C)]
struct ArcInner<T: ?Sized> {
    strong: Opaque<crate::refcount_t>,
    weak: Opaque<crate::refcount_t>,
    data: T,
}

// This is to allow [`Arc`] (and variants) to be used as the type of `self`.
impl<T: ?Sized> core::ops::Receiver for Arc<T> {}

// This is to allow coercion from `Arc<T>` to `Arc<U>` if `T` can be converted to the
// dynamically-sized type (DST) `U`.
impl<T: ?Sized + Unsize<U>, U: ?Sized> core::ops::CoerceUnsized<Arc<U>> for Arc<T> {}
impl<T: ?Sized + Unsize<U>, U: ?Sized> core::ops::CoerceUnsized<Weak<U>> for Weak<T> {}

// This is to allow `Arc<U>` to be dispatched on when `Arc<T>` can be coerced into `Arc<U>`.
impl<T: ?Sized + Unsize<U>, U: ?Sized> core::ops::DispatchFromDyn<Arc<U>> for Arc<T> {}
impl<T: ?Sized + Unsize<U>, U: ?Sized> core::ops::DispatchFromDyn<Weak<U>> for Weak<T> {}

// SAFETY: It is safe to send `Arc<T>` to another thread when the underlying `T` is `Sync` because
// it effectively means sharing `&T` (which is safe because `T` is `Sync`); additionally, it needs
// `T` to be `Send` because any thread that has an `Arc<T>` may ultimately access `T` using a
// mutable reference when the reference count reaches zero and `T` is dropped.
unsafe impl<T: ?Sized + Sync + Send> Send for Arc<T> {}
unsafe impl<T: ?Sized + Sync + Send> Send for Weak<T> {}

// SAFETY: It is safe to send `&Arc<T>` to another thread when the underlying `T` is `Sync`
// because it effectively means sharing `&T` (which is safe because `T` is `Sync`); additionally,
// it needs `T` to be `Send` because any thread that has a `&Arc<T>` may clone it and get an
// `Arc<T>` on that thread, so the thread may ultimately access `T` using a mutable reference when
// the reference count reaches zero and `T` is dropped.
unsafe impl<T: ?Sized + Sync + Send> Sync for Arc<T> {}
unsafe impl<T: ?Sized + Sync + Send> Sync for Weak<T> {}

impl<T> Arc<T> {
    /// Constructs a new reference counted instance of `T`.
    pub fn try_new(contents: T) -> Result<Self, AllocError> {
        // INVARIANT: The refcount is initialised to a non-zero value.
        let value = ArcInner {
            // SAFETY: There are no safety requirements for this FFI call.
            strong: Opaque::new(unsafe { crate::REFCOUNT_INIT(1) }),
            // Start the weak pointer count as 1 which is the weak pointer that's
            // held by all the strong pointers (kinda), see std/rc.rs for more info.
            weak: Opaque::new(unsafe { crate::REFCOUNT_INIT(1) }),
            data: contents,
        };

        let inner = Box::try_new(value)?;

        // SAFETY: We just created `inner` with a reference count of 1, which is owned by the new
        // `Arc` object.
        Ok(unsafe { Self::from_inner(Box::leak(inner).into()) })
    }

    /// Constructs a new `Arc<T>` while giving you a `Weak<T>` to the allocation,
    /// to allow you to construct a `T` which holds a weak pointer to itself.
    pub fn try_new_cyclic<F>(data_fn: F) -> Result<Self, AllocError>
    where
        F: FnOnce(&Weak<T>) -> T,
    {
        // Construct the inner in the "uninitialized" state with a single
        // weak reference.
        let inner = Box::try_new(ArcInner {
            // SAFETY: There are no safety requirements for this FFI call.
            strong: Opaque::new(unsafe { crate::REFCOUNT_INIT(0) }),
            weak: Opaque::new(unsafe { crate::REFCOUNT_INIT(1) }),
            data: MaybeUninit::<T>::uninit(),
        })?;
        let uninit_ptr: NonNull<_> = Box::leak(inner).into();
        let init_ptr: NonNull<ArcInner<T>> = uninit_ptr.cast();

        let weak = Weak { ptr: init_ptr };

        // It's important we don't give up ownership of the weak pointer, or
        // else the memory might be freed by the time `data_fn` returns. If
        // we really wanted to pass ownership, we could create an additional
        // weak pointer for ourselves, but this would result in additional
        // updates to the weak reference count which might not be necessary
        // otherwise.
        let data = data_fn(&weak);

        // Now we can properly initialize the inner value and turn our weak
        // reference into a strong reference.
        let strong = unsafe {
            let inner = init_ptr.as_ptr();
            core::ptr::write(addr_of_mut!((*inner).data), data);

            let refs = addr_of_mut!((*(*inner).strong.get()).refs);
            let prev_value = crate::atomic_fetch_add_release(1, refs);
            debug_assert_eq!(prev_value, 0, "No prior strong references should exist");
            Arc::from_inner(init_ptr)
        };

        // Strong references should collectively own a shared weak reference,
        // so don't run the destructor for our old weak reference.
        core::mem::forget(weak);
        Ok(strong)
    }
}

impl<T: ?Sized> Arc<T> {
    /// Constructs a new [`Arc`] from an existing [`ArcInner`].
    ///
    /// # Safety
    ///
    /// The caller must ensure that `inner` points to a valid location and has a non-zero reference
    /// count, one of which will be owned by the new [`Arc`] instance.
    unsafe fn from_inner(inner: NonNull<ArcInner<T>>) -> Self {
        // INVARIANT: By the safety requirements, the invariants hold.
        Arc {
            ptr: inner,
            _p: PhantomData,
        }
    }

    /// Compare whether two [`Arc`] pointers reference the same underlying object.
    pub fn ptr_eq(this: &Self, other: &Self) -> bool {
        core::ptr::eq(this.ptr.as_ptr(), other.ptr.as_ptr())
    }

    /// Creates a new [`Weak`] pointer.
    pub fn downgrade(this: &Self) -> Weak<T> {
        // INVARIANT: C `refcount_inc` saturates the refcount, so it cannot overflow to zero.
        // SAFETY: By the type invariant, there is necessarily a reference to the object, so it is
        // safe to increment the refcount.
        unsafe { crate::refcount_inc(this.ptr.as_ref().weak.get()) };
        Weak { ptr: this.ptr }
    }

    /// Gets the number of strong (`Arc`) pointers.
    pub fn strong_count(this: &Self) -> usize {
        // SAFETY: By the type invariant, there is necessarily a reference to the object, so it is
        // safe to read the strong refcount.
        unsafe {
            let refs = addr_of_mut!((*(this.ptr.as_ref().strong.get())).refs);
            crate::atomic_read_acquire(refs) as _
        }
    }

    /// Gets the number of [`Weak`] pointers.
    pub fn weak_count(this: &Self) -> usize {
        // SAFETY: By the type invariant, there is necessarily a reference to the object, so it is
        // safe to read the weak refcount.
        unsafe {
            let refs = addr_of_mut!((*(this.ptr.as_ref().weak.get())).refs);
            let cnt = crate::atomic_read_acquire(refs) as usize;
            debug_assert_ne!(
                cnt, 0,
                "Strong references should own a shared weak reference"
            );
            cnt - 1
        }
    }

    /// Returns a mutable reference into the given `Arc`,
    /// without any check.
    unsafe fn get_mut_unchecked(this: &mut Self) -> &mut T {
        // We are careful to *not* create a reference covering the "count" fields, as
        // this would alias with concurrent access to the reference counts (e.g. by `Weak`).
        unsafe { &mut (*this.ptr.as_ptr()).data }
    }
}

impl<T: ?Sized> Deref for Arc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: By the type invariant, there is necessarily a reference to the object, so it is
        // safe to dereference it.
        unsafe { &self.ptr.as_ref().data }
    }
}

impl<T: ?Sized> AsRef<T> for Arc<T> {
    fn as_ref(&self) -> &T {
        self.deref()
    }
}

impl<T: ?Sized> Clone for Arc<T> {
    fn clone(&self) -> Self {
        // INVARIANT: C `refcount_inc` saturates the refcount, so it cannot overflow to zero.
        // SAFETY: By the type invariant, there is necessarily a reference to the object, so it is
        // safe to increment the refcount.
        unsafe { crate::refcount_inc(self.ptr.as_ref().strong.get()) };

        // SAFETY: We just incremented the refcount. This increment is now owned by the new `Arc`.
        unsafe { Self::from_inner(self.ptr) }
    }
}

impl<T: ?Sized> Drop for Arc<T> {
    fn drop(&mut self) {
        // INVARIANT: If the refcount reaches zero, there are no other instances of `Arc`, and
        // this instance is being dropped, so the broken invariant is not observable.
        // SAFETY: Also by the type invariant, we are allowed to decrement the refcount.
        // Because `fetch_dec` is already atomic, we do not need to synchronize with other threads.
        let old = unsafe {
            let refs = addr_of_mut!((*(*self.ptr.as_ptr()).strong.get()).refs);
            crate::atomic_fetch_dec_acquire(refs)
        };
        if old != 1 {
            return;
        }

        // Destroy the data at this time, even though we must not free the box
        // allocation itself (there might still be weak pointers lying around).
        unsafe { core::ptr::drop_in_place(Self::get_mut_unchecked(self)) };

        // Decrement the weak ref collectively held by all strong references.
        drop(Weak { ptr: self.ptr })
    }
}

impl<T: ?Sized> Weak<T> {
    /// Attempts to upgrade the `Weak` pointer to an [`Arc`], delaying
    /// dropping of the inner value if successful.
    ///
    /// Returns [`None`] if the inner value has since been dropped.
    pub fn upgrade(&self) -> Option<Arc<T>> {
        // SAFETY: `Weak<T>` is constructed by `Arc::downgrade`, by the type invariant of `Arc`,
        // there is necessarily a reference (maybe weak) to the object, so it is safe to
        // increment strong refcount unless it is 0.
        let success = unsafe {
            let strong_ptr = self.ptr.as_ref().strong.get();
            let refs = addr_of_mut!((*strong_ptr).refs);
            let prev_value = crate::atomic_fetch_add_release(1, refs);
            prev_value != 0
        };
        if success {
            // SAFETY: By the type invariant of `Arc`, there is necessarily a reference (maybe weak)
            // to the object, so `self.ptr` is valid.
            Some(unsafe { Arc::from_inner(self.ptr) })
        } else {
            None
        }
    }

    /// Gets the number of strong (`Arc`) pointers.
    pub fn strong_count(&self) -> usize {
        // SAFETY: `Weak<T>` is constructed by `Arc::downgrade`, by the type invariant of `Arc`,
        // there is necessarily a reference (maybe weak) to the object, so it is safe to read
        // the refcount.
        unsafe {
            let refs = addr_of_mut!((*(self.ptr.as_ref().strong.get())).refs);
            crate::atomic_read_acquire(refs) as _
        }
    }

    /// Gets an approximation of the number of `Weak` pointers.
    ///
    /// Due to implementation details, the returned value can be off by 1 in
    /// either direction when other threads are manipulating any `Arc`s or
    /// `Weak`s pointing to the same allocation.
    pub fn weak_count(&self) -> usize {
        // SAFETY: `Weak<T>` is constructed by `Arc::downgrade`, by the type invariant of `Arc`,
        // there is necessarily a reference (maybe weak) to the object, so it is safe to read
        // the refcount.
        let weak = unsafe {
            let refs = addr_of_mut!((*(self.ptr.as_ref().weak.get())).refs);
            crate::atomic_read_acquire(refs) as usize
        };
        let strong = self.strong_count();
        if strong == 0 {
            0
        } else {
            // Since we observed that there was at least one strong pointer
            // after reading the weak count, we know that the implicit weak
            // reference (present whenever any strong references are alive)
            // was still around when we observed the weak count, and can
            // therefore safely subtract it.
            weak - 1
        }
    }

    /// Compare whether two [`Weak`] pointers reference the same underlying object.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        core::ptr::eq(self.ptr.as_ptr(), other.ptr.as_ptr())
    }
}

impl<T: ?Sized> Clone for Weak<T> {
    fn clone(&self) -> Self {
        // INVARIANT: C `refcount_inc` saturates the refcount, so it cannot overflow to zero.
        // SAFETY: By the type invariant, there is necessarily a reference to the object, so it is
        // safe to increment the refcount.
        unsafe { crate::refcount_inc(self.ptr.as_ref().weak.get()) };

        // SAFETY: We just incremented the refcount. This increment is now owned by the new `Weak`.
        Weak { ptr: self.ptr }
    }
}

impl<T: ?Sized> Drop for Weak<T> {
    fn drop(&mut self) {
        // INVARIANT: If the refcount reaches zero, there are no other instances of `Weak`, and
        // this instance is being dropped, so the broken invariant is not observable.
        // SAFETY: Also by the type invariant, we are allowed to decrement the refcount.
        // Because `fetch_dec` is already atomic, we do not need to synchronize with other threads.
        let old = unsafe {
            let refs = addr_of_mut!((*(*self.ptr.as_ptr()).weak.get()).refs);
            crate::atomic_fetch_dec_acquire(refs)
        };
        if old != 1 {
            return;
        }

        // Weak count reached zero, we must free the memory.
        //
        // The pointer was initialized from the result of `Box::leak`.
        unsafe { drop(Box::from_raw(self.ptr.as_ptr())) };
    }
}

impl<T: fmt::Display + ?Sized> fmt::Display for Arc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.deref(), f)
    }
}

impl<T: fmt::Debug + ?Sized> fmt::Debug for Arc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.deref(), f)
    }
}

impl<T: ?Sized> fmt::Debug for Weak<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(Weak)")
    }
}
