//! Port of `shared/src/cache.ts`'s `TimedCache` — a generic TTL cache that
//! stores each value's expiration at insertion time (not refreshed on
//! read). First slice of the previously entirely-unmapped
//! `zero-cache/src/custom-queries` gap (`transform-query.ts`'s
//! `CustomQueryTransformer` uses this to cache transformed custom-query
//! results for 5 seconds) — this module ports the cache primitive itself,
//! independent of that larger, `ConnectionContext`-coupled caller.
//!
//! Determinism convention: `now` is an explicit parameter on every method
//! rather than reading `Date.now()` ambiently. The periodic
//! `setInterval`-driven sweep upstream runs every `ttlMs * 2` to bound
//! memory is instead a caller-driven [`TimedCache::cleanup`] call — this
//! port has no ambient timer wheel to hook a background sweep into (and
//! `get`'s own lazy eviction-on-read already keeps individually-queried
//! stale entries from being returned, matching upstream's `get` exactly;
//! `cleanup` is only needed to reclaim memory for keys that are set but
//! never read again).

use std::collections::HashMap;
use std::hash::Hash;

struct Entry<T> {
    value: T,
    expires_at: i64,
}

/// Port of `TimedCache<T>`.
pub struct TimedCache<K, T> {
    ttl_ms: i64,
    entries: HashMap<K, Entry<T>>,
}

impl<K: Eq + Hash, T> TimedCache<K, T> {
    pub fn new(ttl_ms: i64) -> Self {
        TimedCache {
            ttl_ms,
            entries: HashMap::new(),
        }
    }

    /// Port of `set`.
    pub fn set(&mut self, key: K, value: T, now: i64) {
        self.entries.insert(
            key,
            Entry {
                value,
                expires_at: now + self.ttl_ms,
            },
        );
    }

    /// Port of `get`: lazily evicts (and returns `None` for) an expired
    /// entry on read, matching upstream's `entry.expiresAt < Date.now()`
    /// check plus delete.
    pub fn get(&mut self, key: &K, now: i64) -> Option<&T> {
        let expired = matches!(self.entries.get(key), Some(e) if e.expires_at < now);
        if expired {
            self.entries.remove(key);
            return None;
        }
        self.entries.get(key).map(|e| &e.value)
    }

    /// Port of the periodic sweep body inside the `setInterval` callback
    /// (`for (const [key, entry] of cache) if (entry.expiresAt < now)
    /// delete`) — see module doc for why this is caller-driven instead of
    /// an ambient timer.
    pub fn cleanup(&mut self, now: i64) {
        self.entries.retain(|_, e| e.expires_at >= now);
    }

    /// Port of `destroy`'s cache-clearing half (`clearInterval` has no
    /// analog here since there's no real timer to cancel).
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_returns_a_value_set_within_ttl() {
        let mut cache: TimedCache<String, i32> = TimedCache::new(1000);
        cache.set("a".to_string(), 42, 0);
        assert_eq!(cache.get(&"a".to_string(), 500), Some(&42));
    }

    #[test]
    fn get_returns_none_for_an_expired_value() {
        let mut cache: TimedCache<String, i32> = TimedCache::new(1000);
        cache.set("a".to_string(), 42, 0);
        assert_eq!(cache.get(&"a".to_string(), 1001), None);
    }

    #[test]
    fn get_does_not_refresh_expiration_on_read() {
        let mut cache: TimedCache<String, i32> = TimedCache::new(1000);
        cache.set("a".to_string(), 42, 0);
        assert_eq!(cache.get(&"a".to_string(), 500), Some(&42));
        // Still expires at the ORIGINAL insertion-time deadline, not
        // refreshed by the read at t=500.
        assert_eq!(cache.get(&"a".to_string(), 1001), None);
    }

    #[test]
    fn get_missing_key_returns_none() {
        let mut cache: TimedCache<String, i32> = TimedCache::new(1000);
        assert_eq!(cache.get(&"missing".to_string(), 0), None);
    }

    #[test]
    fn expired_entry_is_evicted_on_read() {
        let mut cache: TimedCache<String, i32> = TimedCache::new(1000);
        cache.set("a".to_string(), 42, 0);
        assert_eq!(cache.len(), 1);
        cache.get(&"a".to_string(), 2000);
        assert_eq!(
            cache.len(),
            0,
            "an expired entry should be removed on read, not just hidden"
        );
    }

    #[test]
    fn set_overwrites_an_existing_key() {
        let mut cache: TimedCache<String, i32> = TimedCache::new(1000);
        cache.set("a".to_string(), 1, 0);
        cache.set("a".to_string(), 2, 100);
        assert_eq!(cache.get(&"a".to_string(), 100), Some(&2));
    }

    #[test]
    fn cleanup_removes_only_expired_entries() {
        let mut cache: TimedCache<String, i32> = TimedCache::new(1000);
        cache.set("expired".to_string(), 1, 0);
        cache.set("fresh".to_string(), 2, 2000);
        cache.cleanup(2500);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(&"fresh".to_string(), 2500), Some(&2));
    }

    #[test]
    fn clear_removes_everything_regardless_of_expiration() {
        let mut cache: TimedCache<String, i32> = TimedCache::new(1000);
        cache.set("a".to_string(), 1, 0);
        cache.clear();
        assert!(cache.is_empty());
    }
}
