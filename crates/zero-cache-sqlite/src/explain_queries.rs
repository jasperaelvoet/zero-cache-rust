//! Port of `explainQueries` (`zqlite/src/explain-queries.ts`) — runs
//! `EXPLAIN QUERY PLAN` against every distinct query string found across a
//! `RowCountsBySource`-shaped map (source name -> query string -> row
//! count; only the query strings matter here, the counts are unused,
//! matching upstream which only reads `Object.keys`), substituting a fixed
//! placeholder for every `?` bind parameter first (upstream's own
//! simplification — different literal values can select a different plan,
//! e.g. `scan` vs `search` at an index boundary, but a representative plan
//! is good enough for introspection).

use std::collections::BTreeMap;

use crate::{DbError, StatementRunner, Value};

/// Port of `explainQueries`. `counts` only needs its query-string keys
/// (the row-count values are read by callers this port doesn't have yet,
/// so it's modeled generically as `BTreeMap<String, BTreeMap<String, T>>`
/// rather than pinning a specific count type).
pub fn explain_queries<T>(
    counts: &BTreeMap<String, BTreeMap<String, T>>,
    db: &StatementRunner,
) -> Result<BTreeMap<String, Vec<String>>, DbError> {
    let mut plans = BTreeMap::new();
    for query_set in counts.values() {
        for query in query_set.keys() {
            let sql = format!("EXPLAIN QUERY PLAN {}", query.replace('?', "'sdfse'"));
            let rows = db.query_uncached(&sql, &[])?;
            let detail: Vec<String> = rows
                .iter()
                .filter_map(|row| {
                    row.iter()
                        .find(|(col, _)| col == "detail")
                        .and_then(|(_, v)| match v {
                            Value::Text(s) => Some(s.clone()),
                            _ => None,
                        })
                })
                .collect();
            plans.insert(query.clone(), detail);
        }
    }
    Ok(plans)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explains_every_distinct_query_across_every_source() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();

        let mut counts: BTreeMap<String, BTreeMap<String, i64>> = BTreeMap::new();
        let mut source_a = BTreeMap::new();
        source_a.insert("SELECT * FROM t WHERE id = ?".to_string(), 5);
        counts.insert("source-a".to_string(), source_a);
        let mut source_b = BTreeMap::new();
        source_b.insert("SELECT * FROM t".to_string(), 10);
        counts.insert("source-b".to_string(), source_b);

        let plans = explain_queries(&counts, &db).unwrap();
        assert_eq!(plans.len(), 2);
        assert!(!plans["SELECT * FROM t WHERE id = ?"].is_empty());
        assert!(!plans["SELECT * FROM t"].is_empty());
    }

    #[test]
    fn substitutes_every_bind_parameter_placeholder() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE t (a TEXT, b TEXT)").unwrap();
        let mut counts: BTreeMap<String, BTreeMap<String, i64>> = BTreeMap::new();
        let mut source = BTreeMap::new();
        // Two placeholders -- must not error from an un-substituted `?`.
        source.insert("SELECT * FROM t WHERE a = ? AND b = ?".to_string(), 1);
        counts.insert("s".to_string(), source);

        let plans = explain_queries(&counts, &db).unwrap();
        assert!(!plans["SELECT * FROM t WHERE a = ? AND b = ?"].is_empty());
    }

    #[test]
    fn propagates_a_malformed_query_as_an_error() {
        let db = StatementRunner::open_in_memory().unwrap();
        let mut counts: BTreeMap<String, BTreeMap<String, i64>> = BTreeMap::new();
        let mut source = BTreeMap::new();
        source.insert("SELECT * FROM nonexistent_table".to_string(), 1);
        counts.insert("s".to_string(), source);

        assert!(explain_queries(&counts, &db).is_err());
    }
}
