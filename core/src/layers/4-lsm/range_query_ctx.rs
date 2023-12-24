// Context for range query.
use super::{RecordKey, RecordValue};
use crate::prelude::*;
use crate::util::BitMap;

use core::ops::RangeInclusive;

/// Context for a range query request.
/// It tracks the completing process of each slot within the range.
#[derive(Debug)]
pub struct RangeQueryCtx<K, V> {
    start: K,
    count: usize,
    complete_table: BitMap,
    min_completed: usize,
    res: Vec<(K, V)>,
}

impl<K: RecordKey<K>, V: RecordValue> RangeQueryCtx<K, V> {
    /// Create a new context with the given start key, and the count for query.
    pub fn new(start: K, count: usize) -> Self {
        Self {
            start,
            count,
            complete_table: BitMap::repeat(false, count),
            min_completed: 0,
            res: Vec::with_capacity(count),
        }
    }

    /// Gets the uncompleted range within the whole, returns `None`
    /// if all slots are already completed.
    pub fn range_uncompleted(&self) -> Option<RangeInclusive<K>> {
        let first_uncompleted = self.start + self.complete_table.first_zero(self.min_completed)?;
        let last_uncompleted = self.start + self.complete_table.last_zero()?;
        Some(first_uncompleted..=last_uncompleted)
    }

    /// Whether the uncompleted range contains the target key.
    pub fn contains_uncompleted(&self, key: &K) -> bool {
        let nth = *key - self.start;
        nth < self.count && !self.complete_table[nth]
    }

    /// Whether the range query context is completed, means
    /// all slots are filled with the corresponding values.
    pub fn is_completed(&self) -> bool {
        self.complete_table.count_zeros() == 0
    }

    /// Complete one slot within the range, with the specific
    /// key and the queried value.
    pub fn complete(&mut self, key: K, value: V) {
        let nth = key - self.start;
        if self.complete_table[nth] {
            return;
        }

        self.res.push((key, value));
        self.complete_table.set(nth, true);
        self.min_completed = self.min_completed.min(nth);
    }

    /// Mark the specific slot as completed.
    pub fn mark_completed(&mut self, key: K) {
        let nth = key - self.start;
        self.complete_table.set(nth, true);
        self.min_completed = self.min_completed.min(nth);
    }

    /// Turn the context into final results.
    pub fn as_results(self) -> Vec<(K, V)> {
        debug_assert!(self.is_completed());
        self.res
    }
}
