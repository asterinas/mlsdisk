//! Get and set the current transaction of the current thread.

use core::cell::Cell;
use core::ptr;

use super::{Tx, TxManager};

/// The current transaction on a thread.
#[derive(Clone)]
pub struct CurrentTx<'a> {
    manager: &'a TxManager,
}

// CurrentTx is only useful and valid for the current thread
impl<'a> !Send for CurrentTx<'a> {}
impl<'a> !Sync for CurrentTx<'a> {}

impl<'a> CurrentTx<'a> {
    pub(super) fn new(manager: &TxManager) -> Self {
        assert!(
            get_current_manager_id() == manager.id,
            "a current transation must be accessed with its own manager, not others"
        );

        Self {
            manager,
        }
    }

    /// The ID of the transaction.
    pub fn id(&self) -> TxId {
        get_current_mut_with(|tx| tx.id() ) 
    }

    /// Get immutable access to some type of the per-transaction data within a closure.
    pub fn data_with<T: TxData, F, R>(&mut self, f: F)
    where
        F: FnOnce(&T) -> R,
    {
        get_current_mut_with(|tx| {
            let data = tx.data();
            f(data)
        })
    }

    /// Get mutable access to some type of the per-transaction data within a closure.
    pub fn data_mut_with<T: TxData>(&mut self, f: F)
    where
        F: FnOnce(&mut T) -> R,
    {
        get_current_mut_with(|tx| {
            let data = tx.data_mut();
            f(data)
        })
    }
}

/// Set the current transaction of the current thread, 
/// whose scope is limited to the scope of a given closure.
/// 
/// # Panics
///
/// The `set_with` method must _not_ be called recursively. 
fn set_and_exec_with<F, R>(tx: &mut Tx, f: F) -> R
where
    F: FnOnce() -> R, 
{
    CURRENT.with(|cell| {
        // This method is not allowed to be invoked recursively.
        assert!(cell.get() != ptr::null_mut());
        cell.set(tx);
    });

    let retval = {
        // Drop the mutable reference first to because the f() function may 
        // invoke `get_with` method to re-construct the mutable reference.
        // This avoids violating Rust's non-aliasing requirement for mutable 
        // references.
        drop(tx);

        f()
    };

    CURRENT.with(|cell| {
        cell.set(ptr::null_mut());
    });

    retval
}

/// Get a _mutable_ reference to the current transaction of the current thread,
/// passing it to a given closure.
/// 
/// # Panics
/// 
/// The `get_with` (or `get_mut_with`) method must be called within the closure
/// of `set_with`.
/// 
/// In addition, the `get_with` (or `get_mut_with`) method must _not_ be called
/// recursively.
fn get_current_mut_with<F, R>(f: F) -> R
where
    F: FnOnce(&mut Tx) -> R 
{
    CURRENT.with(|cell| {
        // Take the current pointer so that it 
        let current_ptr = cell.take();
        assert!(current_ptr != ptr::null_mut(),
            "get_mut_with must not
            1) be called without calling set_mut_with first, or 
            2) be called recursively");

        // SAFETY. It is safe to construct a mutable reference from a pointer because 
        // 1) The pointer is originally converted from a _mutable_ reference 
        // taken as input by the `set_with` method;
        // 2) The pointer is set within the lifetime of the input reference;
        // 3) The original mutable reference is dropped within `set_with`;
        // 4) At any given time, at most one mutable reference will be 
        // constructed from the pointer.
        let current_mut = unsafe {
            &mut *current_ptr
        };

        let retval: R = f(current_mut);

        drop(current_mut);
        cell.set(current_ptr);

        retval
    })
}

fn get_current_manager_id() -> u64 {
    CURRENT_MANAGER_ID.with(|cell| cell.get())
}

// The information about the per-thread current transaction
thread_local! {
    static CURRENT: Cell<*mut Tx> = Cell::new(core::ptr::null_mut());
    static CURRENT_MANAGER_ID: Cell<u64> = Cell::new(0);
}
