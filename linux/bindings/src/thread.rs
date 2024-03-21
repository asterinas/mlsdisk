// SPDX-License-Identifier: GPL-2.0
// Copyright (c) 2023 Ant Group CO., Ltd.

//! Kernel thread.

use core::{
    cell::Cell,
    fmt,
    marker::PhantomData,
    ptr::NonNull,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use kernel::{
    new_condvar, new_mutex,
    prelude::*,
    sync::{CondVar, Mutex},
};

/// A handle to kernel's `struct task_struct`.
pub struct Thread {
    inner: NonNull<crate::task_struct>,
}

/// An owned permission to join on a `Thread` (block on its termination).
///
/// This struct is created by the `spawn` function.
pub struct JoinHandle<T> {
    thread: Thread,
    should_stop: AtomicBool,
    _p: PhantomData<T>,
}

/// An item to hold the user closure and its return value for `Thread`.
///
/// The `closure` can only be accessed in the `threadfn` of one kthread,
/// so it's safe to use `Cell`.
#[pin_data]
struct ThreadItem<T> {
    closure: Cell<Option<Box<dyn FnOnce() -> T>>>,
    #[pin]
    value: Mutex<Option<T>>,
    #[pin]
    finished: CondVar,
}

impl<T> ThreadItem<T> {
    /// Constructs an in-place fallible initializer.
    fn try_new<F>(f: F) -> impl PinInit<Self, Error>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        try_pin_init!(Self {
            closure: {
                let closure: Box<dyn FnOnce() -> T> = Box::try_new(f)?;
                Cell::new(Some(closure))
            },
            value <- new_mutex!(None),
            finished <- new_condvar!(),
        })
    }

    /// Executes the user closure.
    ///
    /// # Panics
    ///
    /// This method must only be called once.
    fn execute(&self) {
        let closure = self.closure.take().expect("take closure more than once");
        let value = closure();
        let old_value = self.value.lock().replace(value);
        debug_assert!(old_value.is_none());
        self.finished.notify_one();
    }

    /// Returns true if the closure has finished.
    fn is_finished(&self) -> bool {
        self.value.lock().is_some()
    }

    /// Returns the value, produced by the closure.
    fn wait_value(&self) -> T {
        let mut value = self.value.lock();
        while value.is_none() {
            // It may wake up spuriously, when the thread receives a signal.
            let _ = self.finished.wait(&mut value);
        }
        value.take().unwrap()
    }
}

impl Thread {
    unsafe extern "C" fn threadfn<T>(item: *mut core::ffi::c_void) -> core::ffi::c_int {
        // Execute the user closure, only once.
        let mut item = Box::from_raw(item as *mut ThreadItem<T>);
        item.execute();
        // Do not drop the item, until the kthread stops.
        Box::into_raw(item);

        return 0;
    }

    /// Refines the user specified `name`.
    ///
    /// Add a prefix "krfl-", and ensure that total length should not be greater
    /// than `TASK_COMM_LEN`, defined in `include/linux/sched.h`.
    fn format_name(name: &str) -> [u8; crate::TASK_COMM_LEN as _] {
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
    /// It will run the `closure` when it's actually scheduled by kernel.
    fn create<F, T>(name: &str, closure: F) -> Result<Self>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        // SAFETY: `ThreadItem` is initialized by `pin_init`, it is valid, and it
        // should not be dropped until the kthread stops.
        let item_ptr = unsafe {
            let pinned_item = Box::pin_init(ThreadItem::try_new(closure))?;
            Box::into_raw(Pin::into_inner_unchecked(pinned_item))
        };

        // SAFETY: both `threadfn` and `item_ptr` are valid, and `name` will be
        // checked by `format_name`.
        let task_ptr = unsafe {
            crate::kthread_create_on_node(
                Some(Self::threadfn::<T>),
                item_ptr as _,
                crate::NUMA_NO_NODE,
                Self::format_name(name).as_ptr() as _,
            )
        };

        // SAFETY: no safety requirement on this FFI call.
        if unsafe { kernel::bindings::IS_ERR(task_ptr as _) } {
            // SAFETY: `item_ptr` is initialized above, and it should be dropped
            // here due to the kthread creation failure.
            unsafe { Box::from_raw(item_ptr) };
            return Err(ENOMEM);
        }

        // SAFETY: `task_ptr` is a valid `task_struct`, we get a reference
        // to it, so that it can't go away util the kthread stops.
        unsafe {
            crate::get_task_struct(task_ptr);
            crate::wake_up_process(task_ptr);
        }

        // SAFETY: it is checked above that `task_ptr` is valid.
        Ok(Self {
            inner: unsafe { NonNull::new_unchecked(task_ptr) },
        })
    }

    /// Returns true if the user `closure` has been finished.
    fn is_finished<T>(&self) -> bool {
        // SAFETY: the `self.inner` is a valid `task_struct`, `kthread_data`
        // returns value specified on kthread creation, which is a valid
        // `ThreadItem`.
        unsafe {
            let item_ptr = crate::kthread_data(self.inner.as_ptr()) as *mut ThreadItem<T>;
            (*item_ptr).is_finished()
        }
    }

    /// Waits the kthread to finish and retrieves the value of `ThreadItem`.
    ///
    /// # Safety
    ///
    /// This method must be called, and only be called once, since we should drop
    /// `ThreadItem` here. And after this, it should not be used ever.
    unsafe fn stop<T>(&self) -> T {
        // SAFETY: the `self.inner` is a valid `task_struct`, so the `kthread_data`
        // is also a valid `ThreadItem`.
        let item = unsafe {
            let item_ptr = crate::kthread_data(self.inner.as_ptr()) as *mut ThreadItem<T>;
            Box::from_raw(item_ptr)
        };

        // Wait `ThreadItem` to be executed and take the value.
        item.wait_value()
    }
}

impl Drop for Thread {
    fn drop(&mut self) {
        // SAFETY: `self.inner` is a valid `task_struct`, we get a reference
        // to it on kthread creation, now put it here.
        unsafe { crate::put_task_struct(self.inner.as_ptr()) };
    }
}

// SAFETY: `Thread` just holds a pointer to kernel's `task_struct`, which provides
// its own synchronization, and could be safely transferred across threads.
unsafe impl Send for Thread {}

// SAFETY: `Thread` just holds a pointer to kernel's `task_struct`, which provides
// its own synchronization, and could be used concurrently from multiple threads.
unsafe impl Sync for Thread {}

impl<T> JoinHandle<T> {
    /// Checks if the associated thread has finished running its main function.
    pub fn is_finished(&self) -> bool {
        self.thread.is_finished::<T>()
    }

    /// Waits for the associated thread to finish.
    pub fn join(self) -> Result<T> {
        // SAFETY: `self.should_stop` could ensure that `stop` method
        // should only be called once.
        let value = unsafe { self.thread.stop() };
        self.should_stop.store(false, Ordering::Relaxed);
        Ok(value)
    }

    /// Extracts a handle to the underlying thread.
    pub fn thread(&self) -> &Thread {
        &self.thread
    }
}

impl<T> fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinHandle").finish_non_exhaustive()
    }
}

impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        if self.should_stop.load(Ordering::Relaxed) {
            // SAFETY: `self.should_stop` could ensure that `stop` method
            // should only be called once.
            let _ = unsafe { self.thread.stop::<T>() };
        }
    }
}

/// Puts the current thread to sleep for at least the specified amount of time.
pub fn sleep(dur: Duration) {
    // SAFETY: no safety requirements on this FFI call.
    unsafe { crate::msleep(dur.as_millis() as _) };
}

/// Spawns a new thread, returning a `JoinHandle` for it.
pub fn spawn<F, T>(f: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    JoinHandle {
        thread: Thread::create("default", f).expect("create kthread failed"),
        should_stop: AtomicBool::new(true),
        _p: PhantomData,
    }
}
