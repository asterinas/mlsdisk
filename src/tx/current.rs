//! Get and set the current transaction of the current thread.
use super::{Tx, TxData, TxId, TxProvider};

use core::cell::Cell;
use core::ptr;

/// The current transaction on a thread.
#[derive(Clone)]
pub struct CurrentTx<'a> {
    provider: &'a TxProvider,
}

// CurrentTx is only useful and valid for the current thread
impl<'a> !Send for CurrentTx<'a> {}
impl<'a> !Sync for CurrentTx<'a> {}

impl<'a> CurrentTx<'a> {
    pub(super) fn new(provider: &'a TxProvider) -> Self {
        if is_current_provider_id_uninit() {
            set_current_provider_id(provider.id);
        }
        assert!(
            get_current_provider_id() == provider.id,
            "a current transaction must be accessed with its own provider, not others"
        );

        Self { provider }
    }

    /// The ID of the transaction.
    pub fn id(&self) -> TxId {
        get_current_mut_with(|tx| tx.id())
    }

    /// Get immutable access to some type of the per-transaction data within a closure.
    pub fn data_with<T: TxData, F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        get_current_mut_with(|tx| {
            let data = tx.data::<T>();
            f(data)
        })
    }

    /// Get mutable access to some type of the per-transaction data within a closure.
    pub fn data_mut_with<T: TxData, F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        get_current_mut_with(|tx| {
            let data = tx.data_mut::<T>();
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
pub(super) fn set_and_exec_with<F, R>(tx: &mut Tx, f: F) -> R
where
    F: FnOnce() -> R,
{
    CURRENT.with(|cell| {
        // This method is not allowed to be invoked recursively.
        assert!(cell.get() == ptr::null_mut());
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
    F: FnOnce(&mut Tx) -> R,
{
    CURRENT.with(|cell| {
        // Take the current pointer so that it
        let current_ptr = cell.replace(core::ptr::null_mut());
        assert!(
            current_ptr != ptr::null_mut(),
            "get_mut_with must not
            1) be called without calling set_mut_with first, or 
            2) be called recursively"
        );

        // SAFETY. It is safe to construct a mutable reference from a pointer because
        // 1) The pointer is originally converted from a _mutable_ reference
        // taken as input by the `set_with` method;
        // 2) The pointer is set within the lifetime of the input reference;
        // 3) The original mutable reference is dropped within `set_with`;
        // 4) At any given time, at most one mutable reference will be
        // constructed from the pointer.
        let current_mut = unsafe { &mut *current_ptr };

        let retval: R = f(current_mut);

        drop(current_mut);
        cell.set(current_ptr);

        retval
    })
}

fn get_current_provider_id() -> u64 {
    CURRENT_PROVIDER_ID.with(|cell| cell.get())
}

fn set_current_provider_id(id: u64) {
    CURRENT_PROVIDER_ID.with(|cell| cell.set(id));
}

fn is_current_provider_id_uninit() -> bool {
    CURRENT_PROVIDER_ID.with(|cell| cell.get() == u64::MAX)
}

// The information about the per-thread current transaction
thread_local! {
    static CURRENT: Cell<*mut Tx> = Cell::new(core::ptr::null_mut());
    static CURRENT_PROVIDER_ID: Cell<u64> = Cell::new(u64::MAX);
}
