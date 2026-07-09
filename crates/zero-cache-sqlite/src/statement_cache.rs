//! Port of `zqlite/src/internal/statement-cache.ts`'s `StatementCache` — a
//! prepared-statement cache with checkout/return (not get/put) semantics:
//! statement preparation isn't cheap (SQLite evaluates query plans, not
//! just parses SQL), but a single prepared statement can't be iterated by
//! two callers concurrently, so `get` REMOVES a statement from the cache
//! (or prepares a fresh one if none is cached) and `return` adds it back —
//! concurrent callers for the same SQL each get their own statement rather
//! than contending for one.
//!
//! Scope deviation: generic over an opaque prepared-statement handle `T`
//! rather than tied to `rusqlite::Statement<'conn>` directly. A real
//! `rusqlite::Statement` borrows from its `Connection` with a lifetime,
//! which doesn't fit in a `HashMap` the way this cache's own state needs
//! to outlive individual borrows — the same self-referential-struct
//! problem `rusqlite` itself sidesteps with its own built-in
//! `Connection::prepare_cached` (a real, already-available alternative for
//! callers who just want connection-level caching without this module).
//! This module ports the actual DATA-STRUCTURE logic upstream has (the
//! checkout/return bookkeeping, LRU-free stack-per-SQL-string cache, size
//! tracking, `drop`'s partial-eviction algorithm) generically, so it's
//! usable once a caller has a concrete non-lifetime-bound statement handle
//! (e.g. an owned wrapper) to plug in — not attempted here, a real
//! follow-on if this cache ever needs to back live rusqlite statements
//! instead of `prepare_cached`.

use std::collections::HashMap;

/// Port of `StatementCache<T>`. `T` stands in for upstream's `Statement`
/// (see module doc on why this isn't `rusqlite::Statement` directly).
pub struct StatementCache<T> {
    cache: HashMap<String, Vec<T>>,
    size: usize,
}

impl<T> Default for StatementCache<T> {
    fn default() -> Self {
        StatementCache {
            cache: HashMap::new(),
            size: 0,
        }
    }
}

impl<T> StatementCache<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of statements currently cached (checked-out statements
    /// don't count). Port of the `size` getter.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Evicts up to `n` cached statements, oldest-map-entry-first (matching
    /// upstream's iteration-order-based eviction — `HashMap` iteration
    /// order isn't insertion order like JS `Map`, so this is a documented
    /// behavioral approximation: it removes exactly `n` statements total,
    /// just not necessarily the SAME `n` upstream's insertion-ordered
    /// `Map` would pick). Panics if `n` is negative... `n` is a `usize`
    /// here so that's not representable; panics if `n` exceeds `size()`,
    /// matching upstream's assert.
    pub fn drop_n(&mut self, n: usize) {
        assert!(
            n <= self.size,
            "Cannot drop more items than are in the cache"
        );
        let mut remaining = n;
        let keys: Vec<String> = self.cache.keys().cloned().collect();
        for key in keys {
            if remaining == 0 {
                break;
            }
            let statements = self.cache.get_mut(&key).unwrap();
            if remaining >= statements.len() {
                remaining -= statements.len();
                self.size -= statements.len();
                self.cache.remove(&key);
            } else {
                statements.drain(0..remaining);
                self.size -= remaining;
                remaining = 0;
            }
        }
    }

    /// Port of `get`: removes and returns a cached statement for `sql`
    /// (after whitespace normalization) if one exists, else prepares a
    /// fresh one via `prepare`. Returns the normalized SQL alongside the
    /// statement so the caller can pass it straight to [`Self::return_stmt`].
    pub fn get(&mut self, sql: &str, prepare: impl FnOnce(&str) -> T) -> (String, T) {
        let sql = normalize_whitespace(sql);
        if let Some(statements) = self.cache.get_mut(&sql) {
            if let Some(statement) = statements.pop() {
                self.size -= 1;
                if statements.is_empty() {
                    self.cache.remove(&sql);
                }
                return (sql, statement);
            }
        }
        let statement = prepare(&sql);
        (sql, statement)
    }

    /// Port of `use`: checks out a statement, runs `cb`, then returns it —
    /// even if `cb` panics is NOT guaranteed here (no `finally` without a
    /// guard type); ordinary success/early-`?`-return callers get the
    /// same "always returned" behavior as upstream's `try/finally`.
    pub fn use_stmt<R>(
        &mut self,
        sql: &str,
        prepare: impl FnOnce(&str) -> T,
        cb: impl FnOnce(&mut T) -> R,
    ) -> R {
        let (sql, mut statement) = self.get(sql, prepare);
        let result = cb(&mut statement);
        self.return_stmt(sql, statement);
        result
    }

    /// Port of `return`: adds a checked-out statement back to the cache.
    pub fn return_stmt(&mut self, sql: String, statement: T) {
        self.cache.entry(sql).or_default().push(statement);
        self.size += 1;
    }
}

/// Port of `normalizeWhitespace`: collapses every run of whitespace to a
/// single space (leading/trailing whitespace becomes a single space too,
/// NOT trimmed — matching `replaceAll(/\s+/g, ' ')` exactly, which
/// `split_whitespace().join(" ")` would get wrong by also trimming).
fn normalize_whitespace(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut in_whitespace = false;
    for c in sql.chars() {
        if c.is_whitespace() {
            if !in_whitespace {
                result.push(' ');
                in_whitespace = true;
            }
        } else {
            result.push(c);
            in_whitespace = false;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_whitespace_collapses_runs_without_trimming() {
        assert_eq!(
            normalize_whitespace("select  *   from t"),
            "select * from t"
        );
        assert_eq!(normalize_whitespace(" select *"), " select *");
        assert_eq!(
            normalize_whitespace("select *\n\tfrom t"),
            "select * from t"
        );
    }

    #[test]
    fn get_prepares_a_fresh_statement_when_nothing_is_cached() {
        let mut cache: StatementCache<String> = StatementCache::new();
        let (sql, stmt) = cache.get("select 1", |s| format!("prepared:{s}"));
        assert_eq!(sql, "select 1");
        assert_eq!(stmt, "prepared:select 1");
        assert_eq!(
            cache.size(),
            0,
            "a freshly-prepared (not yet returned) statement isn't counted as cached"
        );
    }

    #[test]
    fn returned_statements_are_reused_by_get() {
        let mut cache: StatementCache<String> = StatementCache::new();
        let (sql, stmt) = cache.get("select 1", |s| format!("prepared:{s}"));
        cache.return_stmt(sql, stmt);
        assert_eq!(cache.size(), 1);

        let mut prepared_again = false;
        let (_, stmt2) = cache.get("select 1", |s| {
            prepared_again = true;
            format!("prepared:{s}")
        });
        assert!(
            !prepared_again,
            "a returned statement should be reused, not re-prepared"
        );
        assert_eq!(stmt2, "prepared:select 1");
        assert_eq!(
            cache.size(),
            0,
            "get checks the statement back OUT of the cache"
        );
    }

    #[test]
    fn concurrent_gets_for_the_same_sql_each_get_their_own_statement() {
        let mut cache: StatementCache<u32> = StatementCache::new();
        let mut next = 0;
        let (sql1, s1) = cache.get("select 1", |_| {
            next += 1;
            next
        });
        let (sql2, s2) = cache.get("select 1", |_| {
            next += 1;
            next
        });
        assert_ne!(
            s1, s2,
            "two concurrent checkouts of the same SQL must not share one statement"
        );
        cache.return_stmt(sql1, s1);
        cache.return_stmt(sql2, s2);
        assert_eq!(
            cache.size(),
            2,
            "both copies are returned to the cache, serving future concurrent callers"
        );
    }

    #[test]
    fn use_stmt_always_returns_the_statement_after_the_callback() {
        let mut cache: StatementCache<String> = StatementCache::new();
        let result = cache.use_stmt(
            "select 1",
            |s| format!("prepared:{s}"),
            |stmt| format!("ran:{stmt}"),
        );
        assert_eq!(result, "ran:prepared:select 1");
        assert_eq!(cache.size(), 1);
    }

    #[test]
    fn drop_n_evicts_exactly_n_statements() {
        let mut cache: StatementCache<u32> = StatementCache::new();
        for sql in ["a", "b", "c"] {
            let (sql, stmt) = cache.get(sql, |_| 1);
            cache.return_stmt(sql, stmt);
        }
        assert_eq!(cache.size(), 3);
        cache.drop_n(2);
        assert_eq!(cache.size(), 1);
    }

    #[test]
    fn drop_n_zero_is_a_no_op() {
        let mut cache: StatementCache<u32> = StatementCache::new();
        let (sql, stmt) = cache.get("a", |_| 1);
        cache.return_stmt(sql, stmt);
        cache.drop_n(0);
        assert_eq!(cache.size(), 1);
    }

    #[test]
    #[should_panic(expected = "Cannot drop more items than are in the cache")]
    fn drop_n_more_than_size_panics() {
        let mut cache: StatementCache<u32> = StatementCache::new();
        cache.drop_n(1);
    }
}
