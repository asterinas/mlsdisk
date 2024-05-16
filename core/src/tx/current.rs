//! Get and set the current transaction of the current thread.
use core::sync::atomic::Ordering::{Acquire, Release};

use super::{Tx, TxData, TxId, TxProvider};
use crate::os::CurrentThread;

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
        Self { provider }
    }

    /// The ID of the transaction.
    pub fn id(&self) -> TxId {
        self.get_current_mut_with(|tx| tx.id())
    }

    /// Get immutable access to some type of the per-transaction data within a closure.
    ///
    /// # Panics
    ///
    /// The `data_with` method must _not_ be called recursively.
    pub fn data_with<T: TxData, F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        self.get_current_mut_with(|tx| {
            let data = tx.data::<T>();
            f(data)
        })
    }

    /// Get mutable access to some type of the per-transaction data within a closure.
    pub fn data_mut_with<T: TxData, F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        self.get_current_mut_with(|tx| {
            let data = tx.data_mut::<T>();
            f(data)
        })
    }

    /// Get a _mutable_ reference to the current transaction of the current thread,
    /// passing it to a given closure.
    ///
    /// # Panics
    ///
    /// The `get_current_mut_with` method must be called within the closure
    /// of `set_and_exec_with`.
    ///
    /// In addition, the `get_current_mut_with` method must _not_ be called
    /// recursively.
    #[allow(dropping_references)]
    fn get_current_mut_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut Tx) -> R,
    {
        let tx_table = self.provider.tx_table.lock();
        let tx_ptr = tx_table.get(&CurrentThread::id()).expect(
            "get_current_mut_with must not be called without calling set_and_exec_with first",
        );

        // SAFETY. It is safe to construct a mutable reference from a pointer because
        // 1) The pointer is originally converted from a _mutable_ reference
        // taken as input by the `set_and_exec_with` method;
        // 2) The pointer is set within the lifetime of the input reference;
        // 3) The original mutable reference is dropped within `set_and_exec_with`;
        // 4) At any given time, at most one mutable reference will be
        // constructed from the pointer.
        let tx = unsafe {
            if (**tx_ptr).is_accessing_data.swap(true, Acquire) {
                panic!("get_current_mut_with must not be called recursively");
            }
            &mut **tx_ptr
        };
        drop(tx_table);

        let retval: R = f(tx);

        // SAFETY. At any given time, at most one mutable reference will be constructed
        // between the Acquire-Release section. And it is safe to drop `&mut Tx` after
        // `Release`, since drop the reference does nothing to the `Tx` itself.
        tx.is_accessing_data.store(false, Release);
        drop(tx);

        retval
    }
}

/// Set the current transaction of the current thread,
/// whose scope is limited to the scope of a given closure.
///
/// # Panics
///
/// The `set_and_exec_with` method must _not_ be called recursively.
#[allow(dropping_references)]
pub(super) fn set_and_exec_with<F, R>(tx: &mut Tx, f: F) -> R
where
    F: FnOnce() -> R,
{
    let tid = CurrentThread::id();
    let provider = tx.provider();
    let old = provider.tx_table.lock().insert(tid, tx as *mut Tx);
    assert_eq!(
        old, None,
        "set_and_exec_with method must not be called recursively"
    );

    let retval = {
        // Drop the mutable reference first to because the f() function may
        // invoke `get_with` method to re-construct the mutable reference.
        // This avoids violating Rust's non-aliasing requirement for mutable
        // references.
        drop(tx);

        f()
    };

    provider.tx_table.lock().remove(&tid);

    retval
}
