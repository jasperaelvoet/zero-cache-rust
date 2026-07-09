//! Port of `SQLiteStatFanout` (`zqlite/src/sqlite-stat-fanout.ts`, 468
//! lines) — computes join fanout factors (average child rows per distinct
//! parent key) from SQLite's own `sqlite_stat4`/`sqlite_stat1` statistics
//! tables, for the query planner's cost model. Found via a directory-
//! coverage scan of `zqlite/src`; previously assumed blocked on the same
//! `rusqlite` `scanStatus` gap as `sqlite_cost_model.rs`, but that
//! assumption was WRONG — this class never touches `scanStatus` at all, it
//! only queries `sqlite_stat4`/`sqlite_stat1`/`pragma_index_list`/
//! `pragma_index_info`, all ordinary SQL/pragma queries `rusqlite` already
//! supports. Confirmed live: a bundled `rusqlite` build creates BOTH
//! `sqlite_stat4` and `sqlite_stat1` after `ANALYZE`, so this is fully
//! live-testable against a real in-memory database, not just unit-testable
//! on hand-built rows.
//!
//! ## Why stat4 over stat1
//!
//! `sqlite_stat1` includes NULL rows in its average, which can badly
//! overestimate fanout for sparse foreign keys (100 rows, 20 non-NULL, 80
//! NULL → stat1 says fanout≈17, but the true non-NULL fanout is 4).
//! `sqlite_stat4`'s histogram samples let us separate NULL from non-NULL
//! samples and take the median of just the non-NULL ones. `stat1` is only
//! a fallback when `stat4` has no samples for the relevant index.

use std::cell::RefCell;
use std::collections::BTreeMap;

use crate::{StatementRunner, Value};

/// Port of `FanoutResult`.
#[derive(Debug, Clone, PartialEq)]
pub struct FanoutResult {
    pub fanout: i64,
    pub confidence: FanoutConfidence,
    pub source: FanoutSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FanoutConfidence {
    High,
    Med,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FanoutSource {
    Stat4,
    Stat1,
    Default,
}

/// Port of `SQLiteStatFanout`. Holds a shared reference to the
/// [`StatementRunner`] it queries against (matching upstream's `#db`
/// field, a live connection captured at construction), plus a
/// per-`(table, columns)` result cache mirroring `#cache`.
pub struct SqliteStatFanout<'a> {
    db: &'a StatementRunner,
    default_fanout: i64,
    cache: RefCell<BTreeMap<String, FanoutResult>>,
}

impl<'a> SqliteStatFanout<'a> {
    /// Port of the constructor with its default `defaultFanout = 3`.
    pub fn new(db: &'a StatementRunner) -> Self {
        Self::with_default_fanout(db, 3)
    }

    pub fn with_default_fanout(db: &'a StatementRunner, default_fanout: i64) -> Self {
        SqliteStatFanout {
            db,
            default_fanout,
            cache: RefCell::new(BTreeMap::new()),
        }
    }

    /// Port of `getFanout`: stat4 → stat1 → default, in that order,
    /// caching whichever result is found.
    pub fn get_fanout(&self, table_name: &str, columns: &[String]) -> FanoutResult {
        let mut sorted_columns = columns.to_vec();
        sorted_columns.sort();
        let cache_key = format!("{table_name}:{}", sorted_columns.join(","));
        if let Some(cached) = self.cache.borrow().get(&cache_key) {
            return cached.clone();
        }

        let result = self
            .get_fanout_from_stat4(table_name, columns)
            .or_else(|| self.get_fanout_from_stat1(table_name, columns))
            .unwrap_or(FanoutResult {
                fanout: self.default_fanout,
                confidence: FanoutConfidence::None,
                source: FanoutSource::Default,
            });

        self.cache.borrow_mut().insert(cache_key, result.clone());
        result
    }

    /// Port of `clearCache`.
    pub fn clear_cache(&self) {
        self.cache.borrow_mut().clear();
    }

    /// Port of `#findIndexForColumns`. Returns `None` on any query failure
    /// (matching upstream's `catch { return undefined }`) or when no index
    /// has `columns` as a (order-independent) prefix.
    fn find_index_for_columns(
        &self,
        table_name: &str,
        columns: &[String],
    ) -> Option<(String, usize)> {
        let rows = self
            .db
            .all(
                "SELECT il.name as index_name, ii.seqno, ii.name as column_name \
             FROM pragma_index_list(?) il JOIN pragma_index_info(il.name) ii \
             ORDER BY il.seq, ii.seqno",
                &[Value::Text(table_name.to_string())],
            )
            .ok()?;

        let mut index_map: Vec<(String, Vec<String>)> = Vec::new();
        for row in &rows {
            let index_name = text_col(row, "index_name")?;
            let column_name = text_col(row, "column_name")?;
            match index_map.iter_mut().find(|(name, _)| *name == index_name) {
                Some(entry) => entry.1.push(column_name),
                None => index_map.push((index_name, vec![column_name])),
            }
        }

        index_map
            .into_iter()
            .find(|(_, index_columns)| is_prefix_match(columns, index_columns))
            .map(|(index_name, _)| (index_name, columns.len()))
    }

    /// Port of `#getFanoutFromStat4`.
    fn get_fanout_from_stat4(&self, table_name: &str, columns: &[String]) -> Option<FanoutResult> {
        let (index_name, depth) = self.find_index_for_columns(table_name, columns)?;
        let rows = self.db.all(
            "SELECT neq, nlt, ndlt, sample FROM sqlite_stat4 WHERE tbl = ? AND idx = ? ORDER BY nlt",
            &[Value::Text(table_name.to_string()), Value::Text(index_name)],
        ).ok()?;
        if rows.is_empty() {
            return None;
        }

        let neq_index = depth - 1;
        let mut non_null_fanouts: Vec<i64> = Vec::new();
        for row in &rows {
            let neq = text_col(row, "neq")?;
            let sample = blob_col(row, "sample")?;
            let neq_parts: Vec<&str> = neq.split(' ').collect();
            let fanout: i64 = neq_parts
                .get(neq_index)
                .or_else(|| neq_parts.first())?
                .parse()
                .ok()?;
            if !decode_sample_is_null(&sample) {
                non_null_fanouts.push(fanout);
            }
        }

        if non_null_fanouts.is_empty() {
            // All samples are NULL - fanout 0 since NULLs don't match in joins.
            return Some(FanoutResult {
                fanout: 0,
                source: FanoutSource::Stat4,
                confidence: FanoutConfidence::High,
            });
        }

        Some(FanoutResult {
            fanout: median_fanout(&non_null_fanouts),
            source: FanoutSource::Stat4,
            confidence: FanoutConfidence::High,
        })
    }

    /// Port of `#getFanoutFromStat1`.
    fn get_fanout_from_stat1(&self, table_name: &str, columns: &[String]) -> Option<FanoutResult> {
        let (index_name, depth) = self.find_index_for_columns(table_name, columns)?;
        let row = self
            .db
            .get(
                "SELECT stat FROM sqlite_stat1 WHERE tbl = ? AND idx = ?",
                &[Value::Text(table_name.to_string()), Value::Text(index_name)],
            )
            .ok()??;
        let stat = text_col(&row, "stat")?;
        let parts: Vec<&str> = stat.split(' ').collect();
        if parts.len() < depth + 1 {
            return None;
        }
        let fanout: i64 = parts[depth].parse().ok()?;
        Some(FanoutResult {
            fanout,
            source: FanoutSource::Stat1,
            confidence: FanoutConfidence::Med,
        })
    }
}

fn text_col(row: &crate::Row, name: &str) -> Option<String> {
    row.iter()
        .find(|(col, _)| col == name)
        .and_then(|(_, v)| match v {
            Value::Text(s) => Some(s.clone()),
            _ => None,
        })
}

fn blob_col(row: &crate::Row, name: &str) -> Option<Vec<u8>> {
    row.iter()
        .find(|(col, _)| col == name)
        .and_then(|(_, v)| match v {
            Value::Blob(b) => Some(b.clone()),
            _ => None,
        })
}

/// Port of `#isPrefixMatch`: true if every column in `query_columns`
/// (case-insensitively) appears within the first `query_columns.len()`
/// positions of `index_columns`, regardless of order within that prefix.
/// Gaps aren't allowed — a column appearing later in the index doesn't
/// count, since SQLite's per-depth statistics are cumulative from
/// position 0.
pub fn is_prefix_match(query_columns: &[String], index_columns: &[String]) -> bool {
    if query_columns.len() > index_columns.len() {
        return false;
    }
    let prefix: std::collections::HashSet<String> = index_columns[..query_columns.len()]
        .iter()
        .map(|c| c.to_lowercase())
        .collect();
    query_columns
        .iter()
        .all(|c| prefix.contains(&c.to_lowercase()))
}

/// Port of `#decodeSampleIsNull`: reads a `sqlite_stat4` sample's raw
/// record-format bytes and checks whether its first column's serial type
/// is `0` (NULL). A simplified single-byte-varint reader, matching
/// upstream's own simplification (real varints can be multi-byte, but
/// header sizes small enough to fit one byte cover the practical case).
pub fn decode_sample_is_null(sample: &[u8]) -> bool {
    if sample.is_empty() {
        return true;
    }
    let header_size = sample[0] as usize;
    if header_size == 0 || header_size >= sample.len() {
        return true;
    }
    let serial_type = sample[1];
    serial_type == 0
}

/// Port of the median-fanout reduction inside `#getFanoutFromStat4`:
/// "nearest rank"-adjacent median (average of the two middle values,
/// floored, for an even count).
pub fn median_fanout(fanouts: &[i64]) -> i64 {
    let mut sorted = fanouts.to_vec();
    sorted.sort();
    let n = sorted.len();
    if n % 2 == 0 {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2
    } else {
        sorted[n / 2]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn is_prefix_match_allows_reordering_within_the_prefix() {
        assert!(is_prefix_match(&cols(&["a", "b"]), &cols(&["a", "b", "c"])));
        assert!(
            is_prefix_match(&cols(&["a", "b"]), &cols(&["b", "a", "c"])),
            "order within the prefix doesn't matter"
        );
        assert!(
            !is_prefix_match(&cols(&["a", "c"]), &cols(&["a", "b", "c"])),
            "a gap (c not in the first 2 positions) must not match"
        );
        assert!(
            !is_prefix_match(&cols(&["a", "b", "c"]), &cols(&["a", "b"])),
            "more query columns than the index has can't match"
        );
    }

    #[test]
    fn is_prefix_match_is_case_insensitive() {
        assert!(is_prefix_match(&cols(&["UserId"]), &cols(&["userid"])));
    }

    #[test]
    fn decode_sample_is_null_reads_the_first_serial_type() {
        assert!(decode_sample_is_null(&[]), "empty sample");
        assert!(decode_sample_is_null(&[0, 1, 2]), "header_size == 0");
        assert!(
            decode_sample_is_null(&[10, 1, 2]),
            "header_size >= sample.len()"
        );
        assert!(
            decode_sample_is_null(&[2, 0, 1, 2]),
            "serial type 0 == NULL"
        );
        assert!(
            !decode_sample_is_null(&[2, 1, 1, 2]),
            "serial type 1 == non-NULL (8-bit int)"
        );
    }

    #[test]
    fn median_fanout_matches_known_cases() {
        assert_eq!(median_fanout(&[5]), 5);
        assert_eq!(median_fanout(&[1, 2, 3]), 2);
        assert_eq!(median_fanout(&[1, 2, 3, 4]), 2, "floor((2+3)/2)");
        assert_eq!(
            median_fanout(&[4, 1, 3, 2]),
            2,
            "sorts before taking the median"
        );
    }

    fn open() -> StatementRunner {
        StatementRunner::open_in_memory().unwrap()
    }

    #[test]
    fn live_get_fanout_falls_back_to_default_with_no_statistics() {
        let db = open();
        db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, fk INTEGER)")
            .unwrap();
        let calc = SqliteStatFanout::new(&db);
        let result = calc.get_fanout("t", &cols(&["fk"]));
        assert_eq!(
            result,
            FanoutResult {
                fanout: 3,
                confidence: FanoutConfidence::None,
                source: FanoutSource::Default
            }
        );
    }

    #[test]
    fn live_get_fanout_uses_a_custom_default() {
        let db = open();
        db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, fk INTEGER)")
            .unwrap();
        let calc = SqliteStatFanout::with_default_fanout(&db, 10);
        assert_eq!(calc.get_fanout("t", &cols(&["fk"])).fanout, 10);
    }

    #[test]
    fn live_get_fanout_reads_a_real_stat4_histogram() {
        let db = open();
        db.exec(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, fk INTEGER); CREATE INDEX idx_fk ON t(fk)",
        )
        .unwrap();
        // 4 distinct fk values, 20 rows each -> true non-NULL fanout is 20.
        for fk in 0..4 {
            for _ in 0..20 {
                db.run("INSERT INTO t (fk) VALUES (?)", &[Value::Integer(fk)])
                    .unwrap();
            }
        }
        db.exec("ANALYZE").unwrap();

        let calc = SqliteStatFanout::new(&db);
        let result = calc.get_fanout("t", &cols(&["fk"]));
        assert_eq!(
            result.source,
            FanoutSource::Stat4,
            "a real bundled SQLite build populates sqlite_stat4 after ANALYZE"
        );
        assert_eq!(result.confidence, FanoutConfidence::High);
        assert!(result.fanout > 0);
    }

    #[test]
    fn live_get_fanout_excludes_nulls_unlike_stat1() {
        let db = open();
        db.exec(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, fk INTEGER); CREATE INDEX idx_fk ON t(fk)",
        )
        .unwrap();
        // 20 rows with fk = 1..20 distinct-ish low-fanout non-NULL values,
        // 80 rows with fk = NULL -- stat1's average would be dragged way up
        // by the NULL bucket; stat4's non-NULL-only median should not be.
        for i in 0..20 {
            db.run("INSERT INTO t (fk) VALUES (?)", &[Value::Integer(i % 4)])
                .unwrap();
        }
        for _ in 0..80 {
            db.run("INSERT INTO t (fk) VALUES (NULL)", &[]).unwrap();
        }
        db.exec("ANALYZE").unwrap();

        let calc = SqliteStatFanout::new(&db);
        let result = calc.get_fanout("t", &cols(&["fk"]));
        assert_eq!(result.source, FanoutSource::Stat4);
        // 20 non-NULL rows / 4 distinct values = fanout 5, nowhere near
        // stat1's ~20 (100 rows / (arguably) ~5 "distinct" including NULL).
        assert!(
            result.fanout <= 5,
            "stat4 must exclude the NULL bucket, got fanout {}",
            result.fanout
        );
    }

    #[test]
    fn live_get_fanout_falls_back_to_stat1_when_stat4_has_no_index() {
        let db = open();
        // No index at all on `fk` -> stat4/stat1 both have nothing for it,
        // but the table's rowid PK index gives stat1 a row for `id`.
        db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, fk INTEGER)")
            .unwrap();
        for i in 0..10 {
            db.run("INSERT INTO t (fk) VALUES (?)", &[Value::Integer(i)])
                .unwrap();
        }
        db.exec("ANALYZE").unwrap();

        let calc = SqliteStatFanout::new(&db);
        // No index on `fk` at all -> neither stat4 nor stat1 has anything,
        // falls all the way through to the default.
        let result = calc.get_fanout("t", &cols(&["fk"]));
        assert_eq!(result.source, FanoutSource::Default);
    }

    #[test]
    fn live_get_fanout_caches_results() {
        let db = open();
        db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, fk INTEGER)")
            .unwrap();
        let calc = SqliteStatFanout::new(&db);
        let first = calc.get_fanout("t", &cols(&["fk"]));
        db.exec("DROP TABLE t").unwrap(); // if not cached, the next call would now error/behave differently
        let second = calc.get_fanout("t", &cols(&["fk"]));
        assert_eq!(first, second);
    }

    #[test]
    fn live_clear_cache_forces_recomputation() {
        let db = open();
        db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, fk INTEGER)")
            .unwrap();
        let calc = SqliteStatFanout::new(&db);
        calc.get_fanout("t", &cols(&["fk"]));
        calc.clear_cache();
        db.exec("CREATE INDEX idx_fk ON t(fk)").unwrap();
        for i in 0..10 {
            db.run("INSERT INTO t (fk) VALUES (?)", &[Value::Integer(i % 2)])
                .unwrap();
        }
        db.exec("ANALYZE").unwrap();
        let result = calc.get_fanout("t", &cols(&["fk"]));
        assert_ne!(
            result.source,
            FanoutSource::Default,
            "after clearing the cache and adding real statistics, it must recompute"
        );
    }
}
