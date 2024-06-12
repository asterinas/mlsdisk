//! Linux specific implementations.

use alloc::{
    alloc::{alloc, dealloc, AllocError, Layout},
    boxed::Box as KBox,
    vec::Vec as KVec,
};
use bindings::{
    new_rwlock,
    sync::lock::rwlock::{Read, RwLockBackend, Write},
    sync::Arc as KArc,
    sync::Weak as KWeak,
};
use core::{
    any::Any,
    borrow::Borrow,
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    marker::{PhantomData, Tuple, Unsize},
    mem::SizedTypeProperties,
    ops::{CoerceUnsized, Deref, DerefMut, DispatchFromDyn, Receiver},
    pin::Pin,
    ptr::NonNull,
    result, slice,
    slice::sort::{merge_sort, TimSortRun},
};
use kernel::{
    c_str, current,
    init::{InPlaceInit, Init, PinInit},
    new_condvar, new_mutex,
    sync::lock::{mutex::MutexBackend, Guard},
};
use pod::Pod;
use serde::{Deserialize, Serialize};

use crate::{
    error::Errno,
    prelude::{Error, Result},
};

/// Reuse `BTreeMap` in `btree` crate.
pub use btree::BTreeMap;

/// Reuse `spawn` and `JoinHandle` in `bindings::thread`.
pub use bindings::thread::{spawn, JoinHandle};

/// Wrap `alloc::boxed::Box` provided by kernel.
#[repr(transparent)]
pub struct Box<T: ?Sized> {
    inner: KBox<T>,
}

impl<T> Box<T> {
    /// Allocates memory and then places `t` into it.
    pub fn new(t: T) -> Self {
        Self {
            inner: KBox::try_new(t).expect("alloc::boxed::Box try_new failed"),
        }
    }

    /// Consumes the `Box`, returning a wrapped raw pointer.
    pub fn into_raw(b: Self) -> *mut T {
        KBox::into_raw(b.inner)
    }

    /// Consumes and leaks the `Box`, returning a mutable reference,
    /// `&'a mut T`.
    pub fn leak<'a>(b: Self) -> &'a mut T {
        KBox::leak(b.inner)
    }

    /// Constructs a box from a raw pointer.
    ///
    /// After calling this function, the raw pointer is owned by the
    /// resulting `Box`. Specifically, the `Box` destructor will call
    /// the destructor of `T` and free the allocated memory.
    ///
    /// # Safety
    ///
    /// This function is unsafe because improper use may lead to
    /// memory problems. For example, a double-free may occur if the
    /// function is called twice on the same raw pointer.
    pub unsafe fn from_raw(ptr: *mut T) -> Self {
        // SAFETY: the caller should obey the safety requirements.
        unsafe { KBox::from_raw(ptr).into() }
    }
}

impl<T: Clone> Clone for Box<T> {
    fn clone(&self) -> Self {
        let mut dst = KBox::<T>::try_new_zeroed().expect("alloc::box::Box try_new_zeroed failed");
        // SAFETY: use `self.ptr` to initialize `dst`, both ptr are valid.
        unsafe {
            let src_ptr = self.as_ref() as *const _ as *mut T;
            core::ptr::copy(src_ptr, dst.as_mut_ptr(), 1);
            dst.assume_init().into()
        }
    }
}

impl<T: ?Sized> Deref for Box<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &*(*self).inner
    }
}

impl<T: ?Sized> DerefMut for Box<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut *(*self).inner
    }
}

impl<T: ?Sized> AsRef<T> for Box<T> {
    fn as_ref(&self) -> &T {
        self
    }
}

impl<T: ?Sized> AsMut<T> for Box<T> {
    fn as_mut(&mut self) -> &mut T {
        self
    }
}

impl<I: Iterator + ?Sized> Iterator for Box<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<I::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<T: fmt::Debug + ?Sized> fmt::Debug for Box<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized> Receiver for Box<T> {}

impl<T: ?Sized + Unsize<U>, U: ?Sized> CoerceUnsized<Box<U>> for Box<T> {}

impl<T: ?Sized + Unsize<U>, U: ?Sized> DispatchFromDyn<Box<U>> for Box<T> {}

impl<Args: Tuple, F: FnOnce<Args> + ?Sized> FnOnce<Args> for Box<F> {
    type Output = <F as FnOnce<Args>>::Output;

    extern "rust-call" fn call_once(self, args: Args) -> Self::Output {
        self.inner.call_once(args)
    }
}

impl<Args: Tuple, F: FnMut<Args> + ?Sized> FnMut<Args> for Box<F> {
    extern "rust-call" fn call_mut(&mut self, args: Args) -> Self::Output {
        self.inner.as_mut().call_mut(args)
    }
}

impl<Args: Tuple, F: Fn<Args> + ?Sized> Fn<Args> for Box<F> {
    extern "rust-call" fn call(&self, args: Args) -> Self::Output {
        self.inner.as_ref().call(args)
    }
}

impl<T> From<KBox<T>> for Box<T> {
    fn from(value: KBox<T>) -> Self {
        Self { inner: value }
    }
}

impl<T> From<Box<T>> for Pin<Box<T>> {
    fn from(value: Box<T>) -> Self {
        // SAFETY: alloc::boxed::Box is constructed by `pin_init`.
        unsafe { Pin::new_unchecked(value) }
    }
}

impl<T> InPlaceInit<T> for Box<T> {
    #[inline]
    fn try_pin_init<E>(init: impl PinInit<T, E>) -> result::Result<Pin<Self>, E>
    where
        E: From<AllocError>,
    {
        let mut inner = KBox::try_new_uninit()?;
        let slot = inner.as_mut_ptr();
        // SAFETY: When init errors/panics, slot will get deallocated but not dropped,
        // slot is valid and will not be moved, because we pin it later.
        unsafe { init.__pinned_init(slot)? };
        // SAFETY: All fields have been initialized.
        let boxed = Self {
            inner: unsafe { inner.assume_init() },
        };
        Ok(boxed.into())
    }

    #[inline]
    fn try_init<E>(init: impl Init<T, E>) -> result::Result<Self, E>
    where
        E: From<AllocError>,
    {
        let mut inner = KBox::try_new_uninit()?;
        let slot = inner.as_mut_ptr();
        // SAFETY: When init errors/panics, slot will get deallocated but not dropped,
        // slot is valid.
        unsafe { init.__init(slot)? };
        // SAFETY: All fields have been initialized.
        Ok(Self {
            inner: unsafe { inner.assume_init() },
        })
    }
}

#[macro_export]
macro_rules! vec {
    () => {
        $crate::os::Vec::new()
    };
    ($elem:expr; $n:expr) => {{
        let mut vec = $crate::os::Vec::with_capacity($n);
        vec.resize($n, $elem);
        vec
    }};
    ($($x:expr),+ $(,)?) => {{
        let mut vec = $crate::os::Vec::new();
        $(vec.push($x);)+
        vec
    }};
}

/// Wrap `alloc::vec::Vec` provided by kernel.
#[repr(transparent)]
#[derive(PartialEq, Eq, Hash)]
pub struct Vec<T> {
    inner: KVec<T>,
}

impl<T> Vec<T> {
    /// Constructs a new, empty `Vec<T>`.
    pub fn new() -> Self {
        Self { inner: KVec::new() }
    }

    /// Constructs a new, empty `Vec<T>` with at least the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: KVec::try_with_capacity(capacity)
                .expect("alloc::vec::Vec try_with_capacity failed"),
        }
    }

    /// Appends an element to the back of a collection.
    pub fn push(&mut self, value: T) {
        self.inner
            .try_push(value)
            .expect("alloc::vec::Vec try_push failed")
    }

    /// Reserves capacity for at least `additional` more elements to be inserted.
    pub fn reserve(&mut self, additional: usize) {
        self.inner
            .try_reserve(additional)
            .expect("alloc::vec::Vec try_reserve failed")
    }
}

impl<T: Clone> Vec<T> {
    /// Clones and appends all elements in a slice to the `Vec`.
    pub fn extend_from_slice(&mut self, other: &[T]) {
        self.inner
            .try_extend_from_slice(other)
            .expect("alloc::vec::Vec try_extend_from_slice failed")
    }

    /// Resizes the `Vec` in-place so that `len` is equal to `new_len`.
    pub fn resize(&mut self, new_len: usize, value: T) {
        self.inner
            .try_resize(new_len, value)
            .expect("alloc::vec::Vec try_resize failed")
    }
}

impl<T> Extend<T> for Vec<T> {
    #[inline]
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        let iter = iter.into_iter();
        let (size, _) = iter.size_hint();
        self.reserve(size);
        for item in iter {
            self.push(item);
        }
    }
}

impl<'a, T: Copy + 'a> Extend<&'a T> for Vec<T> {
    fn extend<I: IntoIterator<Item = &'a T>>(&mut self, iter: I) {
        let iter = iter.into_iter();
        let (size, _) = iter.size_hint();
        self.reserve(size);
        for &item in iter {
            self.push(item);
        }
    }
}

impl<T> FromIterator<T> for Vec<T> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Vec<T> {
        let iter = iter.into_iter();
        let (size, _) = iter.size_hint();
        let mut vec = Vec::with_capacity(size);
        for item in iter {
            vec.push(item);
        }
        vec
    }
}

impl<T> Deref for Vec<T> {
    type Target = KVec<T>;

    fn deref(&self) -> &KVec<T> {
        &self.inner
    }
}

impl<T> DerefMut for Vec<T> {
    fn deref_mut(&mut self) -> &mut KVec<T> {
        &mut self.inner
    }
}

impl<T> IntoIterator for Vec<T> {
    type Item = T;
    type IntoIter = alloc::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a Vec<T> {
    type Item = &'a T;
    type IntoIter = slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut Vec<T> {
    type Item = &'a mut T;
    type IntoIter = slice::IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter_mut()
    }
}

impl<T: Clone> Clone for Vec<T> {
    fn clone(&self) -> Self {
        let mut dst = Vec::with_capacity(self.capacity());
        // SAFETY: both `self.ptr` and `dst.ptr` are valid. The `new_len` is less than
        // or equal to the `capacity()`. And the elements at `old_len..new_len` have
        // been initialized by `ptr::copy`.
        unsafe {
            core::ptr::copy(self.as_ptr(), dst.as_mut_ptr(), self.len());
            dst.set_len(self.len());
        }
        dst
    }
}

impl<T> From<KVec<T>> for Vec<T> {
    fn from(value: KVec<T>) -> Self {
        Self { inner: value }
    }
}

impl<T: fmt::Debug> fmt::Debug for Vec<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T> AsRef<Vec<T>> for Vec<T> {
    fn as_ref(&self) -> &Vec<T> {
        self
    }
}

impl<T> AsMut<Vec<T>> for Vec<T> {
    fn as_mut(&mut self) -> &mut Vec<T> {
        self
    }
}

impl<T> Serialize for Vec<T>
where
    T: Serialize,
{
    #[inline]
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_seq(self)
    }
}

impl<'de, T> Deserialize<'de> for Vec<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct VecVisitor<T> {
            marker: PhantomData<T>,
        }

        impl<'de, T> serde::de::Visitor<'de> for VecVisitor<T>
        where
            T: Deserialize<'de>,
        {
            type Value = Vec<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a custom sequence")
            }

            fn visit_seq<A>(self, mut seq: A) -> result::Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let capacity = seq.size_hint().unwrap_or(0);
                let mut values = Vec::<T>::with_capacity(capacity);

                while let Some(value) = seq.next_element()? {
                    values.push(value);
                }

                Ok(values)
            }
        }

        let visitor = VecVisitor {
            marker: PhantomData,
        };
        deserializer.deserialize_seq(visitor)
    }
}

/// Revised standard implementation from `alloc/slice.rs`.
fn stable_sort<T, F>(v: &mut [T], mut is_less: F)
where
    F: FnMut(&T, &T) -> bool,
{
    if T::IS_ZST {
        // Sorting has no meaningful behavior on zero-sized types. Do nothing.
        return;
    }

    let elem_alloc_fn = |len: usize| -> *mut T {
        // SAFETY: Creating the layout is safe as long as merge_sort never calls this with len >
        // v.len(). Alloc in general will only be used as 'shadow-region' to store temporary swap
        // elements.
        unsafe { alloc(Layout::array::<T>(len).unwrap_unchecked()) as *mut T }
    };

    let elem_dealloc_fn = |buf_ptr: *mut T, len: usize| {
        // SAFETY: Creating the layout is safe as long as merge_sort never calls this with len >
        // v.len(). The caller must ensure that buf_ptr was created by elem_alloc_fn with the same
        // len.
        unsafe {
            dealloc(
                buf_ptr as *mut u8,
                Layout::array::<T>(len).unwrap_unchecked(),
            );
        }
    };

    let run_alloc_fn = |len: usize| -> *mut TimSortRun {
        // SAFETY: Creating the layout is safe as long as merge_sort never calls this with an
        // obscene length or 0.
        unsafe { alloc(Layout::array::<TimSortRun>(len).unwrap_unchecked()) as *mut TimSortRun }
    };

    let run_dealloc_fn = |buf_ptr: *mut TimSortRun, len: usize| {
        // SAFETY: The caller must ensure that buf_ptr was created by elem_alloc_fn with the same
        // len.
        unsafe {
            dealloc(
                buf_ptr as *mut u8,
                Layout::array::<TimSortRun>(len).unwrap_unchecked(),
            );
        }
    };

    merge_sort(
        v,
        &mut is_less,
        elem_alloc_fn,
        elem_dealloc_fn,
        run_alloc_fn,
        run_dealloc_fn,
    );
}

impl<T: Ord> Vec<T> {
    /// Sorts the slice.
    pub fn sort(&mut self) {
        stable_sort(self.as_mut_slice(), T::lt);
    }
}

impl<T> Vec<T> {
    /// Sorts the slice with a comparator function.
    pub fn sort_by<F>(&mut self, mut compare: F)
    where
        F: FnMut(&T, &T) -> Ordering,
    {
        stable_sort(self.as_mut_slice(), |a, b| compare(a, b) == Ordering::Less);
    }

    /// Sorts the slice with a key extraction function.
    pub fn sort_by_key<K, F>(&mut self, mut f: F)
    where
        F: FnMut(&T) -> K,
        K: Ord,
    {
        stable_sort(self, |a, b| f(a).lt(&f(b)));
    }
}

/// Wrap `hashbrown::HashMap` to impl serde trait.
#[derive(Clone, Debug)]
pub struct HashMap<K, V> {
    inner: hashbrown::HashMap<K, V>,
}

impl<K, V> HashMap<K, V> {
    /// Creates an empty `HashMap`.
    pub fn new() -> Self {
        Self {
            inner: hashbrown::HashMap::new(),
        }
    }

    /// Creates an empty `HashMap` with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: hashbrown::HashMap::with_capacity(capacity),
        }
    }
}

impl<K, V> Deref for HashMap<K, V> {
    type Target = hashbrown::HashMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K, V> DerefMut for HashMap<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<K, V> PartialEq for HashMap<K, V>
where
    K: Eq + Hash,
    V: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl<K, V> Eq for HashMap<K, V>
where
    K: Eq + Hash,
    V: Eq,
{
}

impl<K, V, const N: usize> From<[(K, V); N]> for HashMap<K, V>
where
    K: Eq + Hash,
{
    fn from(arr: [(K, V); N]) -> Self {
        arr.into_iter().collect()
    }
}

impl<K, V> IntoIterator for HashMap<K, V> {
    type Item = (K, V);
    type IntoIter = hashbrown::hash_map::IntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<'a, K, V> IntoIterator for &'a HashMap<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = hashbrown::hash_map::Iter<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter()
    }
}

impl<'a, K, V> IntoIterator for &'a mut HashMap<K, V> {
    type Item = (&'a K, &'a mut V);
    type IntoIter = hashbrown::hash_map::IterMut<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter_mut()
    }
}

impl<K, V> FromIterator<(K, V)> for HashMap<K, V>
where
    K: Eq + Hash,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let mut map = Self::with_capacity(iter.size_hint().0);
        iter.for_each(|(k, v)| {
            map.insert(k, v);
        });
        map
    }
}

impl<K, V> Extend<(K, V)> for HashMap<K, V>
where
    K: Eq + Hash,
{
    fn extend<T: IntoIterator<Item = (K, V)>>(&mut self, iter: T) {
        self.inner.extend(iter);
    }
}

impl<'a, K, V> Extend<(&'a K, &'a V)> for HashMap<K, V>
where
    K: Eq + Hash + Copy,
    V: Copy,
{
    fn extend<T: IntoIterator<Item = (&'a K, &'a V)>>(&mut self, iter: T) {
        self.inner.extend(iter);
    }
}

impl<'a, K, V> Extend<&'a (K, V)> for HashMap<K, V>
where
    K: Eq + Hash + Copy,
    V: Copy,
{
    fn extend<T: IntoIterator<Item = &'a (K, V)>>(&mut self, iter: T) {
        self.inner.extend(iter);
    }
}

impl<K, V> Serialize for HashMap<K, V>
where
    K: Serialize + Eq + Hash,
    V: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_map(self)
    }
}

impl<'de, K, V> Deserialize<'de> for HashMap<K, V>
where
    K: Deserialize<'de> + Eq + Hash,
    V: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct MapVisitor<K, V> {
            marker: PhantomData<HashMap<K, V>>,
        }

        impl<'de, K, V> serde::de::Visitor<'de> for MapVisitor<K, V>
        where
            K: Deserialize<'de> + Eq + Hash,
            V: Deserialize<'de>,
        {
            type Value = HashMap<K, V>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a HashMap")
            }

            fn visit_map<A>(self, mut map: A) -> result::Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut values = HashMap::with_capacity(map.size_hint().unwrap_or(0));

                while let Some((key, value)) = map.next_entry()? {
                    values.insert(key, value);
                }

                Ok(values)
            }
        }

        let visitor = MapVisitor {
            marker: PhantomData,
        };
        deserializer.deserialize_map(visitor)
    }
}

/// Wrap `hashbrown::HashMap` to impl serde trait.
#[derive(Clone, Debug)]
pub struct HashSet<T> {
    inner: hashbrown::HashSet<T>,
}

impl<T> HashSet<T> {
    /// Creates an empty `HashSet`.
    pub fn new() -> Self {
        Self {
            inner: hashbrown::HashSet::new(),
        }
    }

    /// Creates an empty `HashSet` with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: hashbrown::HashSet::with_capacity(capacity),
        }
    }
}

impl<T> Deref for HashSet<T> {
    type Target = hashbrown::HashSet<T>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for HashSet<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T> PartialEq for HashSet<T>
where
    T: Eq + Hash,
{
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl<T> Eq for HashSet<T> where T: Eq + Hash {}

impl<T> FromIterator<T> for HashSet<T>
where
    T: Eq + Hash,
{
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut set = Self::with_capacity(iter.size_hint().0);
        iter.for_each(|t| {
            set.insert(t);
        });
        set
    }
}

impl<T, const N: usize> From<[T; N]> for HashSet<T>
where
    T: Eq + Hash,
{
    fn from(arr: [T; N]) -> Self {
        arr.into_iter().collect()
    }
}

impl<T> Extend<T> for HashSet<T>
where
    T: Eq + Hash,
{
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        self.inner.extend(iter);
    }
}

impl<'a, T> Extend<&'a T> for HashSet<T>
where
    T: 'a + Eq + Hash + Copy,
{
    fn extend<I: IntoIterator<Item = &'a T>>(&mut self, iter: I) {
        self.inner.extend(iter);
    }
}

impl<'a, T> IntoIterator for &'a HashSet<T> {
    type Item = &'a T;
    type IntoIter = hashbrown::hash_set::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter()
    }
}

impl<T> IntoIterator for HashSet<T> {
    type Item = T;
    type IntoIter = hashbrown::hash_set::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<T> Serialize for HashSet<T>
where
    T: Serialize + Eq + Hash,
{
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_seq(self)
    }
}

impl<'de, T> Deserialize<'de> for HashSet<T>
where
    T: Deserialize<'de> + Eq + Hash,
{
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct SeqVisitor<T> {
            marker: PhantomData<HashSet<T>>,
        }

        impl<'de, T> serde::de::Visitor<'de> for SeqVisitor<T>
        where
            T: Deserialize<'de> + Eq + Hash,
        {
            type Value = HashSet<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a HashSet")
            }

            fn visit_seq<A>(self, mut seq: A) -> result::Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut values = HashSet::with_capacity(seq.size_hint().unwrap_or(0));

                while let Some(value) = seq.next_element()? {
                    values.insert(value);
                }

                Ok(values)
            }
        }

        let visitor = SeqVisitor {
            marker: PhantomData,
        };
        deserializer.deserialize_seq(visitor)
    }
}

/// An owned string that is guaranteed to have exactly one `NUL` byte, which is at the end.
///
/// Used for interoperability with kernel APIs that take C strings.
///
/// # Invariants
///
/// The string is always `NUL`-terminated and contains no other `NUL` bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CString {
    buf: Vec<u8>,
}

pub type String = CString;

/// A trait for converting a value to a `String`.
pub trait ToString {
    /// Converts the given value to a `String`.
    fn to_string(&self) -> String;
}

impl CString {
    /// Returns the length of this string excluding `NUL`.
    pub fn len(&self) -> usize {
        self.len_with_nul() - 1
    }

    /// Returns the length of this string with `NUL`.
    pub fn len_with_nul(&self) -> usize {
        // SAFETY: This is one of the invariant of `CStr`.
        // We add a `unreachable_unchecked` here to hint the optimizer that
        // the value returned from this function is non-zero.
        if self.buf.is_empty() {
            unsafe { core::hint::unreachable_unchecked() };
        }
        self.buf.len()
    }

    /// Returns `true` if the string only includes `NUL`.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Convert the string to a byte slice without the trailing 0 byte.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf[..self.len()]
    }

    /// Convert the string to a byte slice containing the trailing 0 byte.
    pub fn as_bytes_with_nul(&self) -> &[u8] {
        &self.buf
    }

    /// Yields a `&str` slice if `CString` contains valid UTF-8.
    ///
    /// If the contents of the `CString` are valid UTF-8 data, this
    /// function will return the corresponding [`&str`] slice. Otherwise,
    /// it will return an error with details of where UTF-8 validation failed.
    pub fn to_str(&self) -> result::Result<&str, core::str::Utf8Error> {
        core::str::from_utf8(self.as_bytes())
    }
}

impl Deref for CString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.to_str().expect("it contains invalid UTF-8 bytes")
    }
}

impl From<&str> for CString {
    /// Creates a `CString` from a `str`.
    ///
    /// The str must NOT contain any interior `NUL` bytes.
    fn from(value: &str) -> Self {
        let bytes = value.as_bytes();
        let mut buf = Vec::with_capacity(bytes.len() + 1);
        let mut i = 0;

        // Check if the `str` has interior `NUL` bytes.
        while i < bytes.len() {
            if bytes[i] == 0 {
                panic!("the str has interior `NUL` bytes");
            }
            buf.push(bytes[i]);
            i += 1;
        }
        buf.push(0);

        Self { buf }
    }
}

impl ToString for &str {
    fn to_string(&self) -> String {
        String::from(*self)
    }
}

impl Borrow<str> for CString {
    fn borrow(&self) -> &str {
        self.to_str().unwrap()
    }
}

impl Hash for String {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        (**self).hash(hasher)
    }
}

impl<'a> PartialEq<&'a str> for CString {
    fn eq(&self, other: &&'a str) -> bool {
        PartialEq::eq(&self.to_str().unwrap(), other)
    }

    fn ne(&self, other: &&'a str) -> bool {
        PartialEq::ne(&self.to_str().unwrap(), other)
    }
}

impl<'a> PartialEq<CString> for &'a str {
    fn eq(&self, other: &CString) -> bool {
        PartialEq::eq(self, &other.to_str().unwrap())
    }

    fn ne(&self, other: &CString) -> bool {
        PartialEq::ne(self, &other.to_str().unwrap())
    }
}

/// Wrap `kernel::sync::Arc` provided by kernel.
#[repr(transparent)]
pub struct Arc<T: ?Sized> {
    inner: KArc<T>,
}

impl<T> Arc<T> {
    /// Constructs a new `Arc<T>`.
    pub fn new(t: T) -> Self {
        Self {
            inner: KArc::try_new(t).expect("kernel::sync::Arc try_new failed"),
        }
    }

    /// Constructs a new `Arc<T>` while giving you a `Weak<T>` to the allocation,
    /// to allow you to construct a `T` which holds a weak pointer to itself.
    pub fn new_cyclic<F>(data_fn: F) -> Self
    where
        F: FnOnce(&KWeak<T>) -> T,
    {
        Self {
            inner: KArc::try_new_cyclic(data_fn).expect("kernel::sync::Arc try_new_cyclic failed"),
        }
    }

    /// Creates a new `Weak` pointer to this allocation.
    pub fn downgrade(this: &Self) -> Weak<T> {
        KArc::downgrade(&this.inner).into()
    }

    /// Gets the number of strong (`Arc`) pointers to this allocation.
    pub fn strong_count(this: &Self) -> usize {
        KArc::strong_count(&this.inner)
    }

    /// Gets the number of `Weak` pointers to this allocation.
    pub fn weak_count(this: &Self) -> usize {
        KArc::weak_count(&this.inner)
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for Arc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + Unsize<U>, U: ?Sized> CoerceUnsized<Arc<U>> for Arc<T> {}

impl<T: ?Sized + Unsize<U>, U: ?Sized> DispatchFromDyn<Arc<U>> for Arc<T> {}

impl<T: ?Sized> Deref for Arc<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &*self.inner
    }
}

impl<T: ?Sized> AsRef<T> for Arc<T> {
    fn as_ref(&self) -> &T {
        &*self.inner
    }
}

impl<T: ?Sized> Clone for Arc<T> {
    fn clone(&self) -> Self {
        self.inner.clone().into()
    }
}

impl<T: ?Sized> From<KArc<T>> for Arc<T> {
    fn from(value: KArc<T>) -> Self {
        Self { inner: value }
    }
}

impl Arc<dyn Any + Send + Sync> {
    #[inline]
    pub fn downcast<T>(self) -> Result<Arc<T>>
    where
        T: Any + Send + Sync,
    {
        let inner: KArc<T> = self.inner.downcast::<T>().map_err(|_| {
            Error::with_msg(Errno::InvalidArgs, "kernel::sync::Arc downcast failed")
        })?;
        Ok(inner.into())
    }
}

/// Wrap `kernel::sync::Weak` provided by kernel.
#[repr(transparent)]
pub struct Weak<T: ?Sized> {
    inner: KWeak<T>,
}

impl<T: ?Sized> Weak<T> {
    /// Attempts to upgrade the `Weak` pointer to an `Arc`, delaying
    /// dropping of the inner value if successful.
    ///
    /// Returns [`None`] if the inner value has since been dropped.
    pub fn upgrade(&self) -> Option<Arc<T>> {
        self.inner.upgrade().map(|arc| arc.into())
    }

    /// Gets the number of strong (`Arc`) pointers pointing to this allocation.
    pub fn strong_count(&self) -> usize {
        KWeak::strong_count(&self.inner)
    }

    /// Gets the number of `Weak` pointers to this allocation.
    pub fn weak_count(&self) -> usize {
        KWeak::weak_count(&self.inner)
    }
}

impl<T: ?Sized> Clone for Weak<T> {
    fn clone(&self) -> Self {
        self.inner.clone().into()
    }
}

impl<T: ?Sized> From<KWeak<T>> for Weak<T> {
    fn from(value: KWeak<T>) -> Self {
        Self { inner: value }
    }
}

/// Wrap the `Mutex` provided by kernel.
///
/// Instances of `kernel::sync::Mutex` need a lock class and to
/// be pinned.
#[repr(transparent)]
pub struct Mutex<T> {
    inner: Pin<Box<kernel::sync::Mutex<T>>>,
}

/// Reuse the lock guard of `kernel::sync::Mutex`.
pub type MutexGuard<'a, T> = Guard<'a, T, MutexBackend>;

impl<T> Mutex<T> {
    /// Constructs a new `Mutex` lock, using the kernel's `struct mutex`.
    pub fn new(t: T) -> Self {
        let inner = Box::pin_init(new_mutex!(t)).unwrap();
        Self { inner }
    }

    /// Acquires the lock and gives the caller access to the data protected by it.
    pub fn lock(&self) -> MutexGuard<'_, T> {
        self.inner.lock()
    }
}

impl<T: fmt::Debug> fmt::Debug for Mutex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("No data, since `Mutex` does't support `try_lock` now")
    }
}

/// Wrap the `CondVar` provided by kernel.
///
/// Instances of `kernel::sync::CondVar` need a lock class and to
/// be pinned.
#[repr(transparent)]
pub struct Condvar {
    inner: Pin<Box<kernel::sync::CondVar>>,
}

impl Condvar {
    /// Constructs a new `Condvar`.
    pub fn new() -> Self {
        let inner = Box::pin_init(new_condvar!()).unwrap();
        Self { inner }
    }

    /// Blocks the current thread until this condition variable receives a notification.
    ///
    /// It may also wake up spuriously. Return `Error`, if there is a signal pending.
    pub fn wait<'a, T>(&self, mut guard: MutexGuard<'a, T>) -> Result<MutexGuard<'a, T>> {
        let signal_pending = self.inner.wait(&mut guard);
        if signal_pending {
            return Err(Error::with_msg(
                Errno::NotFound,
                "condvar spuriously wake up",
            ));
        }
        Ok(guard)
    }

    /// Wakes a single waiter up, if any.
    pub fn notify_one(&self) {
        self.inner.notify_one()
    }

    /// Wakes all waiters up, if any.
    pub fn notify_all(&self) {
        self.inner.notify_all()
    }
}

impl fmt::Debug for Condvar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Condvar").finish_non_exhaustive()
    }
}

/// Wrap the `Mutex` provided by kernel, used for `Condvar`.
#[repr(transparent)]
pub struct CvarMutex<T> {
    inner: Pin<Box<kernel::sync::Mutex<T>>>,
}

// TODO: add distinguish guard type for `CvarMutex` if needed.

impl<T> CvarMutex<T> {
    /// Constructs a new `Mutex` lock, using the kernel's `struct mutex`.
    pub fn new(t: T) -> Self {
        let inner = Box::pin_init(new_mutex!(t)).unwrap();
        Self { inner }
    }

    /// Acquires the lock and gives the caller access to the data protected by it.
    pub fn lock(&self) -> Result<MutexGuard<'_, T>> {
        Ok(self.inner.lock())
    }
}

impl<T: fmt::Debug> fmt::Debug for CvarMutex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("No data, since `CvarMutex` does't support `try_lock` now")
    }
}

/// Wrap the `RwLock` provided by kernel.
///
/// Instances of `kernel::sync::RwLock` need a lock class and to
/// be pinned.
#[repr(transparent)]
pub struct RwLock<T> {
    inner: Pin<Box<bindings::sync::RwLock<T>>>,
}

impl<T> RwLock<T> {
    /// Constructs a new `RwLock`, using the kernel's `struct rw_semaphore`.
    pub fn new(t: T) -> Self {
        let inner = Box::pin_init(new_rwlock!(t)).unwrap();
        Self { inner }
    }

    /// Locks this `RwLock` with shared read access, blocking the current thread
    /// until it can be acquired
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        self.inner.read()
    }

    /// Attempts to acquire this `RwLock` with shared read access.
    ///
    /// If the access could not be granted at this time, then `Err` is returned.
    /// This function does not block.
    pub fn try_read(&self) -> Result<RwLockReadGuard<'_, T>> {
        self.inner
            .try_read()
            .map_err(|_| Error::new(Errno::TryLockFailed))
    }

    /// Locks this `RwLock` with exclusive write access, blocking the current
    /// thread until it can be acquired
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.inner.write()
    }

    /// Attempts to lock this `RwLock` with exclusive write access.
    ///
    /// If the lock could not be acquired at this time, then `Err` is returned.
    /// This function does not block.
    pub fn try_write(&self) -> Result<RwLockWriteGuard<'_, T>> {
        self.inner
            .try_write()
            .map_err(|_| Error::new(Errno::TryLockFailed))
    }
}

impl<T: fmt::Debug> fmt::Debug for RwLock<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("RwLock");
        match self.try_read() {
            Ok(guard) => {
                d.field("data", &&*guard);
            }
            Err(_) => {
                d.field("data", &format_args!("<locked>"));
            }
        }
        d.finish_non_exhaustive()
    }
}

/// Reuse `RwLockReadGuard` provided by kernel.
pub type RwLockReadGuard<'a, T> = bindings::sync::lock::Guard<'a, T, RwLockBackend<Read>>;

/// Reuse `RwLockWriteGuard` provided by kernel.
pub type RwLockWriteGuard<'a, T> = bindings::sync::lock::Guard<'a, T, RwLockBackend<Write>>;

/// Unique ID for the OS thread.
///
/// Linux implements all threads as standard processes. The `pid` field in
/// the task_struct is used to identify the thread of the process, and `tgid`
/// is used to identify the pid of the thread which started the process.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Tid(kernel::bindings::pid_t);

/// A struct to get the current thread id.
pub struct CurrentThread;

impl CurrentThread {
    /// Returns the Tid of current kernel thread.
    pub fn id() -> Tid {
        Tid(current!().pid())
    }
}

/// Page size defined in terms of the `PAGE_SHIFT` macro from C.
pub const PAGE_SIZE: usize = 1 << kernel::bindings::PAGE_SHIFT;

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
            let layout = Layout::from_size_align_unchecked(len * PAGE_SIZE, PAGE_SIZE);
            let ptr = alloc(layout);
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
            let layout = Layout::from_size_align_unchecked(len * PAGE_SIZE, PAGE_SIZE);
            dealloc(ptr, layout)
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
        let ptr = PageAllocator::alloc(len).ok_or(Error::new(Errno::OutOfMemory))?;
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

impl Deref for Pages {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        // SAFETY: `ptr` is `NonNull` and points valid memory with proper length.
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len * PAGE_SIZE) }
    }
}

impl DerefMut for Pages {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: `ptr` is `NonNull` and points valid memory with proper length.
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len * PAGE_SIZE) }
    }
}

/// A random number generator.
pub struct Rng;

impl crate::util::Rng for Rng {
    fn new(_seed: &[u8]) -> Self {
        Self
    }

    fn fill_bytes(&self, dest: &mut [u8]) -> Result<()> {
        unsafe {
            kernel::bindings::get_random_bytes(
                dest.as_mut_ptr() as *mut core::ffi::c_void,
                dest.len(),
            );
        }
        Ok(())
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
pub struct Aead {
    inner: Pin<Box<bindings::crypto::Aead>>,
}

impl Aead {
    /// Construct an `Aead` instance.
    pub fn new() -> Self {
        let inner = Box::pin_init(bindings::crypto::Aead::new(
            kernel::c_str!("gcm(aes)"),
            0,
            0,
        ))
        .expect("alloc gcm(aes) cipher failed");
        Self { inner }
    }
}

impl crate::util::Aead for Aead {
    type Key = AeadKey;
    type Iv = AeadIv;
    type Mac = AeadMac;

    fn encrypt(
        &self,
        input: &[u8],
        key: &AeadKey,
        iv: &AeadIv,
        aad: &[u8],
        output: &mut [u8],
    ) -> Result<AeadMac> {
        self.inner.set_key(key);
        let req = self
            .inner
            .alloc_request()
            .map_err(|_| Error::with_msg(Errno::OutOfMemory, "alloc aead_request failed"))?;
        let mut mac = AeadMac::default();
        req.encrypt(aad, input, &iv, output, &mut mac)
            .map_err(|_| Error::with_msg(Errno::EncryptFailed, "gcm(aes) encryption failed"))?;
        Ok(mac)
    }

    fn decrypt(
        &self,
        input: &[u8],
        key: &AeadKey,
        iv: &AeadIv,
        aad: &[u8],
        mac: &AeadMac,
        output: &mut [u8],
    ) -> Result<()> {
        self.inner.set_key(key);
        let req = self
            .inner
            .alloc_request()
            .map_err(|_| Error::with_msg(Errno::OutOfMemory, "alloc aead_request failed"))?;
        req.decrypt(aad, input, &mac, &iv, output)
            .map_err(|_| Error::with_msg(Errno::DecryptFailed, "gcm(aes) decryption failed"))
    }
}

const AES_CTR_KEY_SIZE: usize = 16;
const AES_CTR_IV_SIZE: usize = 16;

new_byte_array_type!(SkcipherKey, AES_CTR_KEY_SIZE);
new_byte_array_type!(SkcipherIv, AES_CTR_IV_SIZE);

/// A symmetric key cipher.
pub struct Skcipher {
    inner: Pin<Box<bindings::crypto::Skcipher>>,
}

// TODO: impl `Skcipher` with linux kernel Crypto API.
impl Skcipher {
    /// Construct a `Skcipher` instance.
    pub fn new() -> Self {
        let inner = Box::pin_init(bindings::crypto::Skcipher::new(
            kernel::c_str!("ctr(aes)"),
            0,
            0,
        ))
        .expect("alloc ctr(aes) cipher failed");
        Self { inner }
    }
}

impl crate::util::Skcipher for Skcipher {
    type Key = SkcipherKey;
    type Iv = SkcipherIv;

    fn encrypt(
        &self,
        input: &[u8],
        key: &SkcipherKey,
        iv: &SkcipherIv,
        output: &mut [u8],
    ) -> Result<()> {
        self.inner.set_key(key);
        let req = self
            .inner
            .alloc_request()
            .map_err(|_| Error::with_msg(Errno::OutOfMemory, "alloc skcipher_request failed"))?;
        req.encrypt(input, &iv, output)
            .map_err(|_| Error::with_msg(Errno::EncryptFailed, "ctr(aes) encryption failed"))
    }

    fn decrypt(
        &self,
        input: &[u8],
        key: &SkcipherKey,
        iv: &SkcipherIv,
        output: &mut [u8],
    ) -> Result<()> {
        self.inner.set_key(key);
        let req = self
            .inner
            .alloc_request()
            .map_err(|_| Error::with_msg(Errno::OutOfMemory, "alloc skcipher_request failed"))?;
        req.decrypt(input, &iv, output)
            .map_err(|_| Error::with_msg(Errno::DecryptFailed, "ctr(aes) decryption failed"))
    }
}
