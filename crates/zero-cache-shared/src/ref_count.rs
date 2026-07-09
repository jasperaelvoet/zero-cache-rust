//! Port of `packages/shared/src/ref-count.ts`.
//!
//! A basic reference-count map. The TS version keys by object identity via a
//! `WeakMap` (auto-evicting entries when the key is GC'd). Rust has no
//! ambient GC, so this port keys by an explicit, `Eq + Hash` key (typically an
//! id, not the value itself) and never auto-evicts — callers own eviction via
//! `dec`, matching the explicit-decrement usage pattern the TS class already
//! requires.

use std::collections::HashMap;
use std::hash::Hash;

/// Tracks a reference count per key. Port of `RefCount<T>`.
pub struct RefCount<K: Eq + Hash> {
    map: HashMap<K, u32>,
}

impl<K: Eq + Hash> Default for RefCount<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Hash> RefCount<K> {
    pub fn new() -> Self {
        RefCount {
            map: HashMap::new(),
        }
    }

    /// Increments the count for `key`. Returns `true` if this was the first
    /// reference (the key was just added). Port of `inc`.
    pub fn inc(&mut self, key: K) -> bool {
        let rc = self.map.entry(key).or_insert(0);
        *rc += 1;
        *rc == 1
    }

    /// Decrements the count for `key`. Returns `true` if the count reached
    /// zero and the key was removed. Panics if `key` is not present (matching
    /// the TS `must()` call). Port of `dec`.
    pub fn dec(&mut self, key: &K) -> bool
    where
        K: Clone,
    {
        let rc = *self.map.get(key).expect("RefCount.dec: key not present");
        if rc == 1 {
            self.map.remove(key);
            true
        } else {
            self.map.insert(key.clone(), rc - 1);
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inc_returns_true_on_first_add() {
        let mut rc: RefCount<u32> = RefCount::new();
        assert!(rc.inc(1));
        assert!(!rc.inc(1));
        assert!(rc.inc(2));
    }

    #[test]
    fn dec_decreases_reference_count() {
        let mut rc: RefCount<u32> = RefCount::new();
        rc.inc(1);
        rc.inc(1);
        assert!(!rc.dec(&1));
        assert!(rc.dec(&1));
    }

    #[test]
    #[should_panic]
    fn dec_panics_on_missing_key() {
        let mut rc: RefCount<u32> = RefCount::new();
        rc.dec(&1);
    }
}
