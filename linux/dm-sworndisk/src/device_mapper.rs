// SPDX-License-Identifier: GPL-2.0
// Copyright (c) 2023 Ant Group CO., Ltd.

//! Device mapper.

use core::{
    marker::PhantomData,
    ops::{Index, Range},
    ptr::NonNull,
};
use kernel::{error::to_result, prelude::*, str::CStr, types::Opaque};

use super::bio::Bio;
use super::block_device::{BlkStatus, BlockDevice, BLOCK_SECTORS, BLOCK_SIZE};

/// A trait declares operations that a device mapper target can do.
#[vtable]
pub trait TargetOperations: Sized {
    /// Persist user data.
    type Private: Sync;

    /// Constructor. The target will already have the table, type, begin and
    /// len fields filled in. A `Private` struct can be returned to persist
    /// its own context.
    fn ctr(t: &mut Target<Self>, args: Args) -> Result<Box<Self::Private>>;

    /// Destructor. The target could clean up anything hidden in `Private`,
    /// and `Private` itself can be dropped automatically.
    fn dtr(t: &mut Target<Self>);

    /// Map block IOs. Return [`MapState`] to indicate how to handle the `bio`
    /// later (end or resubmit).
    fn map(t: &Target<Self>, bio: Bio) -> MapState;

    /// End the `bio`. Return [`EndState`] and [`BlkStatus`].
    #[allow(unused)]
    fn end_io(t: &Target<Self>, bio: Bio) -> (EndState, BlkStatus) {
        unreachable!()
    }
}

/// Wrap the kernel struct `target_type`.
///
/// It contains a struct `list_head` for internal device-mapper use, so it
/// should be pinned. Users can use this struct to register/unregister their
/// own device mapper target.
#[pin_data(PinnedDrop)]
pub struct TargetType {
    #[pin]
    opaque: Opaque<bindings::target_type>,
}

/// Define target feature type, see `include/linux/device-mapper.h`.
pub type Features = u64;

// SAFETY: It's OK to access `TargetType` from multiple threads. The
// `dm_register_target` and `dm_unregister_target` provides its own
// synchronization.
unsafe impl Sync for TargetType {}

macro_rules! binding_target_operations {
    ($target:expr, $(($op:ident, $method:ident, $func:ident),)+) => {$(
        if <T as TargetOperations>::$op {
            (*$target).$method = Some(TargetType::$func::<T>);
        }
    )+};
}

impl TargetType {
    /// Provide an in-place constructor to register a new device mapper target.
    pub fn register<T: TargetOperations>(
        name: &'static CStr,
        version: [u32; 3],
        features: Features,
    ) -> impl PinInit<Self, Error> {
        // SAFETY: `slot` is valid while the closure is called.
        unsafe {
            init::pin_init_from_closure(move |slot: *mut Self| {
                // The `slot` contains uninit memory. Avoid creating a reference.
                let opaque = core::ptr::addr_of!((*slot).opaque);
                let target = Opaque::raw_get(opaque);

                (*target).module = &mut bindings::__this_module as _;
                (*target).name = name.as_char_ptr();
                (*target).version = version;
                (*target).features = features;

                binding_target_operations!(
                    target,
                    (HAS_CTR, ctr, dm_ctr_fn),
                    (HAS_DTR, dtr, dm_dtr_fn),
                    (HAS_MAP, map, dm_map_fn),
                    (HAS_END_IO, end_io, dm_endio_fn),
                );

                to_result(bindings::dm_register_target(target))
            })
        }
    }
}

#[pinned_drop]
impl PinnedDrop for TargetType {
    fn drop(self: Pin<&mut Self>) {
        // SAFETY: `self.opaque` are initialized by the `register` constructor,
        // so it's valid.
        unsafe { bindings::dm_unregister_target(self.opaque.get()) };
    }
}

impl TargetType {
    unsafe extern "C" fn dm_ctr_fn<T: TargetOperations>(
        ti: *mut bindings::dm_target,
        argc: core::ffi::c_uint,
        argv: *mut *mut core::ffi::c_char,
    ) -> core::ffi::c_int {
        // SAFETY: the kernel splits arguments by `dm_split_args`, then pass
        // suitable `argc` and `argv` to `dm_ctr_fn`. If `argc` is not zero,
        // `argv` is non-null and valid.
        let args = unsafe { Args::new(argc, argv) };

        // SAFETY: the kernel should pass a valid `dm_target`.
        let target = unsafe { Target::borrow_mut(ti) };
        T::ctr(target, args).map_or_else(
            |e| e.to_errno(),
            // SAFETY: the kernel should pass a valid `dm_target`.
            |p| unsafe {
                (*ti).private = Box::into_raw(p) as _;
                0
            },
        )
    }

    unsafe extern "C" fn dm_dtr_fn<T: TargetOperations>(ti: *mut bindings::dm_target) {
        // SAFETY: the kernel should pass a valid `dm_target`.
        let target = unsafe { Target::borrow_mut(ti) };
        T::dtr(target);
        // SAFETY: `private` is constructed in `dm_ctr_fn`, and we drop it here.
        unsafe {
            let private = (*ti).private as *mut T::Private;
            drop(Box::from_raw(private));
            (*ti).private = core::ptr::null_mut();
        }
    }

    unsafe extern "C" fn dm_map_fn<T: TargetOperations>(
        ti: *mut bindings::dm_target,
        bio: *mut bindings::bio,
    ) -> core::ffi::c_int {
        // SAFETY: the kernel should pass a valid `dm_target` and `bio`.
        unsafe {
            let target = Target::borrow(ti);
            let bio = Bio::from_raw(bio);
            T::map(target, bio) as _
        }
    }

    unsafe extern "C" fn dm_endio_fn<T: TargetOperations>(
        ti: *mut bindings::dm_target,
        bio: *mut bindings::bio,
        error: *mut bindings::blk_status_t,
    ) -> core::ffi::c_int {
        // SAFETY: the kernel should pass valid `dm_target`, `bio` and
        // `error` pointers.
        unsafe {
            let target = Target::borrow(ti);
            let bio = Bio::from_raw(bio);
            let (end_state, blk_status) = T::end_io(target, bio);
            *error = blk_status as _;
            end_state as _
        }
    }
}

/// Wrap the kernel struct `dm_target`.
///
/// This struct represents a device mapper target. And the device mapper
/// core will alloc/free `dm_target` instances, so we just `borrow` it.
/// It also holds a `Private` struct, which is used to persist user's data,
/// and can be accessed by the `private` method.
pub struct Target<T: TargetOperations + Sized> {
    opaque: Opaque<bindings::dm_target>,
    _p: PhantomData<*mut T::Private>,
}

impl<T: TargetOperations> Target<T> {
    /// Borrows the instance from a foreign pointer immutably.
    ///
    /// # Safety
    ///
    /// User must provide a valid pointer to the kernel's `dm_target`.
    unsafe fn borrow<'a>(ptr: *const bindings::dm_target) -> &'a Self {
        &*(ptr as *const Self)
    }

    /// Borrows the instance from a foreign pointer mutably.
    ///
    /// # Safety
    ///
    /// User must provide a valid pointer to the kernel's `dm_target`.
    unsafe fn borrow_mut<'a>(ptr: *mut bindings::dm_target) -> &'a mut Self {
        &mut *(ptr as *mut Self)
    }

    /// Access user's private data.
    pub fn private(&self) -> Option<&T::Private> {
        let target = self.opaque.get();
        // SAFETY: `self.opaque` is borrowed from foreign pointer, should be valid.
        // And '(*target).private' is assigned in `dm_ctr_fn`, it's also valid.
        unsafe { ((*target).private as *const T::Private).as_ref() }
    }

    /// Return the target name.
    pub fn name(&self) -> &CStr {
        // SAFETY: `self.opaque` is borrowed from foreign pointer, should be valid.
        unsafe {
            let name = (*(*self.opaque.get()).type_).name;
            CStr::from_char_ptr(name)
        }
    }

    /// Return the target version.
    pub fn version(&self) -> [u32; 3] {
        // SAFETY: `self.opaque` is borrowed from foreign pointer, should be valid.
        unsafe { (*(*self.opaque.get()).type_).version }
    }

    /// Return the block range of the device mapper target.
    pub fn region(&self) -> Range<usize> {
        // SAFETY: `self.opaque` is borrowed from foreign pointer, should be valid.
        let start_sector = unsafe { (*self.opaque.get()).begin as usize };
        let nr_sectors = unsafe { (*self.opaque.get()).len as usize };
        let end_sector = start_sector + nr_sectors;
        let sectors_per_block = BLOCK_SIZE / (bindings::SECTOR_SIZE as usize);

        Range {
            start: (start_sector + sectors_per_block - 1) / sectors_per_block,
            end: end_sector / sectors_per_block,
        }
    }

    /// Set the block range of the device mapper target.
    pub fn set_region(&mut self, blocks: Range<usize>) {
        let start_sector = blocks.start * BLOCK_SECTORS;
        let nr_sectors = blocks.len() * BLOCK_SECTORS;
        // SAFETY: `self.opaque` is borrowed from foreign pointer, should be valid.
        unsafe {
            (*self.opaque.get()).begin = start_sector as _;
            (*self.opaque.get()).len = nr_sectors as _;
        }
    }

    /// Return the number of zero-length barrier bios that will be submitted
    /// to the target for the purpose of flushing cache.
    pub fn num_flush_bios(&self) -> usize {
        // SAFETY: `self.opaque` is borrowed from foreign pointer, should be valid.
        unsafe { (*self.opaque.get()).num_flush_bios as _ }
    }

    /// Set the number of zero-length barrier bios that will be submitted
    /// to the target for the purpose of flushing cache.
    pub fn set_num_flush_bios(&mut self, num: usize) {
        // SAFETY: `self.opaque` is borrowed from foreign pointer, should be valid.
        unsafe { (*self.opaque.get()).num_flush_bios = num as _ };
    }

    /// Return the number of discard bios.
    pub fn num_discard_bios(&self) -> usize {
        // SAFETY: `self.opaque` is borrowed from foreign pointer, should be valid.
        unsafe { (*self.opaque.get()).num_discard_bios as _ }
    }

    /// Set the number of discard bios.
    pub fn set_num_discard_bios(&mut self, num: usize) {
        // SAFETY: `self.opaque` is borrowed from foreign pointer, should be valid.
        unsafe { (*self.opaque.get()).num_discard_bios = num as _ };
    }

    /// Set an error string for the target, could be used
    /// by [`TargetOperations::ctr`].
    pub fn set_error(&mut self, err: &CStr) {
        // SAFETY: `self.opaque` is borrowed from foreign pointer, should be valid.
        unsafe { (*self.opaque.get()).error = err.as_char_ptr() as _ };
    }
}

/// The return values of target map function, i.e., [`TargetOperations::map`].
#[repr(u32)]
pub enum MapState {
    /// The target will handle the io by resubmitting it later.
    Submitted = bindings::DM_MAPIO_SUBMITTED,

    /// Simple remap complete.
    Remapped = bindings::DM_MAPIO_REMAPPED,

    /// The target wants to requeue the io.
    Requeue = bindings::DM_MAPIO_REQUEUE,

    /// The target wants to requeue the io after a delay.
    DelayRequeue = bindings::DM_MAPIO_DELAY_REQUEUE,

    /// The target wants to complete the io.
    Kill = bindings::DM_MAPIO_KILL,
}

/// The return values of target end_io function.
#[repr(u32)]
pub enum EndState {
    /// Ended successfully.
    Done = bindings::DM_ENDIO_DONE,

    /// The io has still not completed (eg, multipath target might
    /// want to requeue a failed io).
    Incomplete = bindings::DM_ENDIO_INCOMPLETE,

    /// The target wants to requeue the io.
    Requeue = bindings::DM_ENDIO_REQUEUE,

    /// The target wants to requeue the io after a delay.
    DelayRequeue = bindings::DM_ENDIO_DELAY_REQUEUE,
}

/// A struct wraps `c_char` arguments, and yields `CStr`.
pub struct Args {
    argc: core::ffi::c_uint,
    argv: *mut *mut core::ffi::c_char,
}

impl Args {
    /// The caller should ensure that the number of valid `argv` pointers
    /// should be `argc` exactly.
    pub unsafe fn new(argc: core::ffi::c_uint, argv: *mut *mut core::ffi::c_char) -> Self {
        Self { argc, argv }
    }

    /// Returns the number of arguments.
    pub fn len(&self) -> usize {
        self.argc as _
    }

    /// Returns the `nth` (from zero) argument.
    ///
    /// If the index is out of bounds, return `None`.
    pub fn get(&self, index: usize) -> Option<&CStr> {
        if self.argc == 0 || index >= self.argc as _ {
            None
        } else {
            // SAFETY: the `new` caller should ensure the number of valid `argv`.
            unsafe { Some(CStr::from_char_ptr(*self.argv.add(index))) }
        }
    }
}

impl Index<usize> for Args {
    type Output = CStr;

    /// When using the indexing operator(`[]`), the caller should check the
    /// length of [`Args`]. If the index is out of bounds, this will [`panic`].
    fn index(&self, index: usize) -> &Self::Output {
        if self.argc == 0 || index >= self.argc as _ {
            panic!(
                "Index out of bounds: the actual length is {} but the index is {}.",
                self.argc, index
            )
        } else {
            // SAFETY: the `new` caller should ensure the number of valid `argv`.
            unsafe { CStr::from_char_ptr(*self.argv.add(index)) }
        }
    }
}
