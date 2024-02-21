// SPDX-License-Identifier: GPL-2.0
// Copyright (c) 2023 Ant Group CO., Ltd.

//! Kernel thread.

use core::{
    marker::PhantomData,
    ptr::NonNull,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use kernel::{prelude::*, types::Opaque};

/// A wrapper for the kernel's `struct task_struct`.
#[repr(transparent)]
pub struct Thread(Opaque<crate::task_struct>);

/// A struct to carry the user closure and data for `Thread`.
struct ThreadItem<T> {
    func: Option<Box<dyn FnOnce() -> T>>,
    data: Option<T>,
    is_finished: AtomicBool,
}

impl<T> ThreadItem<T> {
    /// Constructs a `ThreadItem`.
    pub fn try_new<F>(func: F) -> Result<Box<Self>>
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static,
    {
        let func = Box::try_new(func)?;
        Ok(Box::try_new(Self {
            func: Some(func),
            data: None,
            is_finished: AtomicBool::new(false),
        })?)
    }

    /// Executes the user closure.
    ///
    /// # Panics
    ///
    /// This method must only be called once.
    pub fn execute(&mut self) {
        let closure = self.func.take().unwrap();
        self.data = Some(closure());
        self.is_finished.store(true, Ordering::Release);
    }

    /// Retrieves the data, produced by the closure.
    pub fn data(&mut self) -> Option<T> {
        self.data.take()
    }
}

impl Thread {
    /// Gets a reference from a raw pointer.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the provided raw pointer is not dangling, and
    /// points at a valid task_struct.
    pub unsafe fn from_raw<'a>(ptr: *mut crate::task_struct) -> &'a Thread {
        &*(ptr as *const Thread)
    }

    /// Refines the user specified `name`.
    ///
    /// Add a prefix "krfl-" and ensure that total length should not be greater
    /// than `TASK_COMM_LEN`, defined in `include/linux/sched.h`.
    fn refine_name(name: &str) -> [u8; crate::TASK_COMM_LEN as _] {
        const PREFIX: &str = "krfl-";
        let pre_len = PREFIX.as_bytes().len();
        let mut name_buf = [0u8; crate::TASK_COMM_LEN as _];
        name_buf[..pre_len].copy_from_slice(PREFIX.as_bytes());

        // Copy the user specified `name` to buffer, may be truncated.
        let avail_len = name_buf.len() - pre_len - 1; // The last byte should always be `\0`.
        let copy_len = name.as_bytes().len().min(avail_len);
        name_buf[pre_len..pre_len + copy_len].copy_from_slice(&name.as_bytes()[..copy_len]);
        name_buf
    }

    /// Creates an instance with a `name`.
    ///
    /// It will run the `func` closure when it's started by `wake_up`.
    pub fn create<F, T>(name: &str, func: F) -> Result<NonNull<crate::task_struct>>
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static,
    {
        // Constructs a boxed `ThreadItem`, which will not be drop until the kthread stops.
        let item = ThreadItem::try_new(func)?;
        // SAFETY: no safety requirements on this FFI call.
        let task_ptr = unsafe {
            crate::kthread_create_on_node(
                Some(Self::threadfn::<F, T>),
                Box::into_raw(item) as _,
                crate::NUMA_NO_NODE,
                Self::refine_name(name).as_ptr() as _,
            )
        };
        // SAFETY: FFI call.
        if unsafe { kernel::bindings::IS_ERR(task_ptr as _) } {
            return Err(ENOMEM);
        }
        // SAFETY: it is checked above that the `task_ptr` is valid.
        Ok(unsafe { NonNull::new_unchecked(task_ptr) })
    }

    unsafe extern "C" fn threadfn<F, T>(data: *mut core::ffi::c_void) -> core::ffi::c_int
    where
        F: FnOnce() -> T,
        F: Send,
        T: Send,
    {
        // Execute the user closure.
        let mut item = Box::from_raw(data as *mut ThreadItem<T>);
        item.execute();
        // Do not drop the item, until the kthread stops.
        Box::into_raw(item);

        return 0;
    }

    /// Returns true if the user specified `func` has been finished.
    pub fn is_finished<T>(&self) -> bool {
        // SAFETY: the `self.0` is a valid `task_struct`, so the `kthread_data`
        // is also valid.
        unsafe {
            let item_ptr = crate::kthread_data(self.0.get()) as *mut ThreadItem<T>;
            (*item_ptr).is_finished.load(Ordering::Acquire)
        }
    }

    /// Blocks unless or until the current thread's token is made available.
    pub fn park() {
        // SAFETY: no safety requirements on this FFI call.
        unsafe { crate::kthread_parkme() };
    }

    /// Makes the handle's token available if it is not already.
    pub fn unpark(&self) {
        // SAFETY: the `self.0` is a valid `task_struct`.
        unsafe { crate::kthread_unpark(self.0.get()) };
    }

    // Cooperatively gives up a timeslice to scheduler.
    pub fn yeild_now() {
        // SAFETY: no safety requirements on this FFI call.
        unsafe { crate::schedule() };
    }

    /// Puts the current thread to sleep for at least the specified amount of time.
    pub fn sleep(dur: Duration) {
        // SAFETY: no safety requirements on this FFI call.
        unsafe { crate::msleep(dur.as_millis() as _) };
    }

    /// Attempts to wake up the kthread.
    pub fn wake_up(&self) {
        // SAFETY: the `self.0` is a valid `task_struct`.
        unsafe { crate::wake_up_process(self.0.get()) };
    }

    /// Stops the kthread and retrieves the data of `ThreadItem`.
    ///
    /// If the `threadfn` is never executed, the data is `None`, else it should
    /// be some item produced by user specified closure.
    ///
    /// # Safety
    ///
    /// This method must only be called once, since we will drop `ThreadItem` here.
    /// And after this, the `thread` should not be used ever.
    pub unsafe fn stop<T>(&self) -> Option<T> {
        // SAFETY: the `self.0` is a valid `task_struct`, so the `kthread_data`
        // is also valid.
        let item_ptr = unsafe { crate::kthread_data(self.0.get()) as *mut ThreadItem<T> };

        // SAFETY: the `self.0` is a valid `task_struct`. Now we stop the kthread
        // and wait for its user closure to finish.
        let err = unsafe { crate::kthread_stop(self.0.get()) };
        if err != 0 {
            return None;
        }
        Box::from_raw(item_ptr).data()
    }
}

/// An owned permission to join on a `Thread` (block on its termination).
///
/// This `struct` is created by the [`spawn`] function.
pub struct JoinHandle<T> {
    thread: NonNull<crate::task_struct>,
    should_stop: AtomicBool,
    _p: PhantomData<T>,
}

impl<T> JoinHandle<T> {
    /// Creates a named JoinHandle.
    pub fn create<F>(name: &str, func: F) -> Result<Self>
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static,
    {
        let thread = Thread::create(name, func)?;
        // SAFETY: `thread` is a valid `task_struct`, we get a reference
        // to it, so that it can't go away when we try to join the thread.
        unsafe { crate::get_task_struct(thread.as_ptr()) };
        let join_handle = JoinHandle {
            thread,
            should_stop: AtomicBool::new(true),
            _p: PhantomData,
        };
        join_handle.thread().wake_up();
        Ok(join_handle)
    }

    /// Extracts a handle to the underlying thread.
    pub fn thread(&self) -> &Thread {
        // SAFETY: the `self.thread` is non-null and valid.
        unsafe { Thread::from_raw(self.thread.as_ptr()) }
    }

    /// Waits for the associated thread to finish.
    pub fn join(self) -> Result<T> {
        // SAFETY: we use the `should_stop` to ensure that this method should
        // only be called once.
        let data = unsafe { self.thread().stop() };
        self.should_stop.store(false, Ordering::Relaxed);
        data.ok_or(ESRCH)
    }

    /// Checks if the associated thread has finished running its main function.
    pub fn is_finished(&self) -> bool {
        self.thread().is_finished::<T>()
    }
}

impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        if self.should_stop.load(Ordering::Relaxed) {
            // SAFETY: we use the `should_stop` to ensure that this method
            // should only be called once.
            let _ = unsafe { self.thread().stop::<T>() };
        }
        // SAFETY: `self.thread` is a valid `task_struct`, we get a reference
        // to it when `create` the JoinHandle, now put it here.
        unsafe { crate::put_task_struct(self.thread.as_ptr()) };
    }
}

/// Spawns a new thread, returning a `JoinHandle` for it.
pub fn spawn<F, T>(func: F) -> JoinHandle<T>
where
    F: FnOnce() -> T,
    F: Send + 'static,
    T: Send + 'static,
{
    JoinHandle::create("default", func).expect("spawn kthread failed")
}
