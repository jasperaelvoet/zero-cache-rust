//! Port of the `mergeRefCounts` and `newQueryRecord` helpers from
//! `zero-cache/src/services/view-syncer/cvr.ts`.
//!
//! These are pure functions the CVR store uses when applying row/query patches;
//! they are not exported upstream (no dedicated test file), so the tests here
//! are written directly from the documented semantics and the call sites in
//! `cvr.ts`.

use std::collections::BTreeMap;

use zero_cache_protocol::ast::Ast;
use zero_cache_shared::bigint_json::JsonValue;

use crate::cvr_types::{ClientQueryRecord, CustomQueryRecord, ExternalQueryBase, QueryRecord};

/// A row's per-query reference counts: hash -> count.
pub type RefCounts = BTreeMap<String, i64>;

/// Merges `existing` and `received` reference counts, dropping any hash in
/// `remove_hashes` from `existing` and any hash whose merged count reaches
/// zero. Returns `None` if the merged result has no positive counts (i.e. the
/// row should be considered a tombstone). Port of `mergeRefCounts`.
pub fn merge_ref_counts(
    existing: Option<&RefCounts>,
    received: Option<&RefCounts>,
    remove_hashes: Option<&std::collections::HashSet<String>>,
) -> Option<RefCounts> {
    let mut merged: RefCounts = BTreeMap::new();

    match existing {
        None => {
            if let Some(received) = received {
                merged = received.clone();
            }
        }
        Some(existing) => {
            // i == 0 (existing), i == 1 (received), matching the TS
            // `[existing, received].forEach`.
            for (i, counts) in [Some(existing), received].into_iter().enumerate() {
                let Some(counts) = counts else { continue };
                for (hash, count) in counts {
                    if i == 0 {
                        if let Some(remove) = remove_hashes {
                            if remove.contains(hash) {
                                continue;
                            }
                        }
                    }
                    let entry = merged.entry(hash.clone()).or_insert(0);
                    *entry += count;
                    if *entry == 0 {
                        merged.remove(hash);
                    }
                }
            }
        }
    }

    if merged.values().any(|&v| v > 0) {
        Some(merged)
    } else {
        None
    }
}

/// Builds a fresh (empty client-state) query record from either an `ast` (a
/// client query) or a `name`+`args` pair (a custom query). Port of
/// `newQueryRecord`. Panics if both or neither are supplied, matching the TS
/// `assert`s.
pub fn new_query_record(
    id: &str,
    ast: Option<&Ast>,
    name: Option<&str>,
    args: Option<&[JsonValue]>,
) -> QueryRecord {
    if let Some(ast) = ast {
        assert!(
            name.is_none() && args.is_none(),
            "Cannot provide name or args with ast"
        );
        return QueryRecord::Client(ClientQueryRecord {
            base: ExternalQueryBase {
                id: id.to_string(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
                client_state: BTreeMap::new(),
                patch_version: None,
            },
            ast: ast.clone(),
        });
    }

    let (name, args) = match (name, args) {
        (Some(n), Some(a)) => (n, a),
        _ => panic!("Must provide name and args"),
    };
    QueryRecord::Custom(CustomQueryRecord {
        base: ExternalQueryBase {
            id: id.to_string(),
            transformation_hash: None,
            transformation_version: None,
            row_set_signature: None,
            client_state: BTreeMap::new(),
            patch_version: None,
        },
        name: name.to_string(),
        args: args.to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn rc(pairs: &[(&str, i64)]) -> RefCounts {
        pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    #[test]
    fn no_existing_uses_received_as_is() {
        let received = rc(&[("h1", 1), ("h2", 2)]);
        assert_eq!(
            merge_ref_counts(None, Some(&received), None),
            Some(received)
        );
        assert_eq!(merge_ref_counts(None, None, None), None);
    }

    #[test]
    fn sums_matching_hashes() {
        let existing = rc(&[("h1", 1)]);
        let received = rc(&[("h1", 2), ("h2", 1)]);
        let merged = merge_ref_counts(Some(&existing), Some(&received), None).unwrap();
        assert_eq!(merged.get("h1"), Some(&3));
        assert_eq!(merged.get("h2"), Some(&1));
    }

    #[test]
    fn drops_hashes_that_reach_zero() {
        let existing = rc(&[("h1", 1)]);
        let received = rc(&[("h1", -1)]);
        let merged = merge_ref_counts(Some(&existing), Some(&received), None);
        assert_eq!(merged, None);
    }

    #[test]
    fn remove_hashes_excludes_from_existing() {
        let existing = rc(&[("h1", 5), ("h2", 3)]);
        let remove: HashSet<String> = ["h1".to_string()].into_iter().collect();
        let merged = merge_ref_counts(Some(&existing), None, Some(&remove)).unwrap();
        assert!(!merged.contains_key("h1"));
        assert_eq!(merged.get("h2"), Some(&3));
    }

    #[test]
    fn partial_dereference_keeps_row_alive_via_other_query() {
        // A row referenced by two queries (h1, h2). One query stops referencing
        // it (h1: -1). The row must SURVIVE because h2 still references it — the
        // core multi-query ref-count invariant.
        let existing = rc(&[("h1", 1), ("h2", 1)]);
        let received = rc(&[("h1", -1)]);
        let merged = merge_ref_counts(Some(&existing), Some(&received), None).unwrap();
        assert!(
            !merged.contains_key("h1"),
            "the dropped query's ref is gone"
        );
        assert_eq!(merged.get("h2"), Some(&1), "the row survives via h2");
    }

    #[test]
    fn all_zero_or_negative_yields_tombstone() {
        let existing = rc(&[("h1", 1), ("h2", 1)]);
        let received = rc(&[("h1", -1), ("h2", -1)]);
        assert_eq!(
            merge_ref_counts(Some(&existing), Some(&received), None),
            None
        );
    }

    #[test]
    fn new_query_record_client() {
        let ast = Ast::table("issues");
        let record = new_query_record("q1", Some(&ast), None, None);
        match record {
            QueryRecord::Client(c) => {
                assert_eq!(c.base.id, "q1");
                assert_eq!(c.ast.table, "issues");
                assert!(c.base.client_state.is_empty());
            }
            _ => panic!("expected client query record"),
        }
    }

    #[test]
    fn new_query_record_custom() {
        let args = vec![JsonValue::Number(1.0)];
        let record = new_query_record("q2", None, Some("myQuery"), Some(&args));
        match record {
            QueryRecord::Custom(c) => {
                assert_eq!(c.base.id, "q2");
                assert_eq!(c.name, "myQuery");
                assert_eq!(c.args, args);
            }
            _ => panic!("expected custom query record"),
        }
    }

    #[test]
    #[should_panic(expected = "Cannot provide name or args with ast")]
    fn new_query_record_rejects_ast_with_name() {
        let ast = Ast::table("issues");
        new_query_record("q3", Some(&ast), Some("name"), None);
    }

    #[test]
    #[should_panic(expected = "Must provide name and args")]
    fn new_query_record_rejects_neither() {
        new_query_record("q4", None, None, None);
    }
}
