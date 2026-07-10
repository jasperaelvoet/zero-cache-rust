//! Port of the schema-drift-detection functions in
//! `zero-cache/src/services/change-source/pg/change-source.ts`:
//! `getSchemaDifference`, `getTableDifference`, `relationDifferent`.
//!
//! These compare a previously-published Postgres schema snapshot against a
//! freshly-queried one (`getSchemaDifference`/`getTableDifference`, used when
//! re-establishing a replication connection) or against a `Relation` message
//! from the logical replication stream itself (`relationDifferent`, used to
//! detect that upstream's schema has drifted since the last sync). Both
//! surface a human-readable reason rather than a boolean, matching upstream.

use std::collections::HashSet;

use zero_cache_types::specs::PublishedTableSpec;

use crate::data::{Relation, RowKey};
use crate::pgoutput::{PgoutputMessage, ReplicaIdentity};

/// Compares two full schema snapshots (lists of tables in the same order),
/// returning a description of the first difference found, or `None` if they
/// match. Indexes are deliberately ignored — index-only changes need not halt
/// replication. Port of `getSchemaDifference`.
pub fn get_schema_difference(a: &[PublishedTableSpec], b: &[PublishedTableSpec]) -> Option<String> {
    if a.len() != b.len() {
        return Some("tables created or dropped".to_string());
    }
    for (at, bt) in a.iter().zip(b.iter()) {
        if let Some(diff) = get_table_difference(at, bt) {
            return Some(diff);
        }
    }
    None
}

/// Compares two table specs, returning a description of the first difference
/// found (identity, primary key, or column set/order/type/nullability), or
/// `None` if they match. Port of `getTableDifference`.
pub fn get_table_difference(a: &PublishedTableSpec, b: &PublishedTableSpec) -> Option<String> {
    if a.oid != b.oid || a.schema != b.schema || a.name != b.name {
        return Some(format!(
            "Table \"{}\" differs from table \"{}\"",
            a.name, b.name
        ));
    }
    if a.primary_key != b.primary_key {
        return Some(format!("Primary key of table \"{}\" has changed", a.name));
    }

    let mut acols = a.columns.clone();
    let mut bcols = b.columns.clone();
    acols.sort_by_key(|(_, c)| c.column.pos);
    bcols.sort_by_key(|(_, c)| c.column.pos);

    let columns_match = acols.len() == bcols.len()
        && acols
            .iter()
            .zip(bcols.iter())
            .all(|((aname, acol), (bname, bcol))| {
                aname == bname
                    && acol.column.pos == bcol.column.pos
                    && acol.type_oid == bcol.type_oid
                    && acol.column.not_null == bcol.column.not_null
            });
    if !columns_match {
        return Some(format!("Columns of table \"{}\" have changed", a.name));
    }
    None
}

/// Compares a published table spec against a `Relation` message from the
/// logical replication stream, returning `true` if they differ in a way that
/// requires re-syncing. Full port of `relationDifferent`: identity
/// (oid/schema/name), key columns (for `replicaIdentity == default`), and the
/// full column list/type/order comparison.
///
/// For `replicaIdentity == default`, the primary key and the relation's key
/// columns are compared order-agnostically: the `Relation` message lists key
/// columns in column-declaration order, while `PublishedTableSpec.primaryKey`
/// lists them in index order, and key-order-only changes aren't detectable
/// from the `Relation` message alone.
///
/// The column comparison is positional: `a`'s columns sorted by `pos` are
/// zipped against `b`'s columns in wire order, and any length, name, or type
/// OID mismatch marks the relation as different (matching upstream's
/// `acols.some(([aname, acol], i) => aname !== bcol.name || acol.typeOID !==
/// bcol.typeOid)`).
pub fn relation_different(
    a: &PublishedTableSpec,
    b: &Relation,
    b_relation_oid: i64,
    b_replica_identity_default: bool,
) -> bool {
    if a.oid != b_relation_oid || a.schema != b.schema || a.name != b.name {
        return true;
    }
    if b_replica_identity_default {
        let a_pk: HashSet<&String> = a.primary_key.iter().flatten().collect();
        let b_key: HashSet<&String> = b.row_key.columns.iter().collect();
        if a_pk != b_key {
            return true;
        }
    }

    let mut acols = a.columns.clone();
    acols.sort_by_key(|(_, c)| c.column.pos);
    if acols.len() != b.columns.len() {
        return true;
    }
    acols
        .iter()
        .zip(b.columns.iter())
        .any(|((aname, acol), (bname, btype_oid))| {
            aname != bname || acol.type_oid != i64::from(*btype_oid)
        })
}

/// Given a pgoutput `Relation` message from the live stream and the currently
/// known published table specs, returns `Some(reason)` if the streamed
/// relation's shape has DRIFTED from its published spec — the signal a
/// replication consumer uses to trigger a re-sync (upstream's
/// schema-change-detected path). Returns `None` when the relation matches its
/// spec, when `msg` is not a `Relation` message, or when it names a table not
/// in `specs`.
///
/// This is the production consumer of [`relation_different`]: it builds a
/// [`Relation`] view from the pgoutput message (schema/name, key columns, and
/// the full `(name, type_oid)` column list) and diffs it against the matching
/// spec (matched by `schema`+`name`).
pub fn relation_message_drift(
    msg: &PgoutputMessage,
    specs: &[PublishedTableSpec],
) -> Option<String> {
    let PgoutputMessage::Relation {
        relation_id,
        namespace,
        name,
        replica_identity,
        columns,
    } = msg
    else {
        return None;
    };
    let spec = specs
        .iter()
        .find(|s| &s.schema == namespace && &s.name == name)?;
    let relation = Relation {
        schema: namespace.clone(),
        name: name.clone(),
        row_key: RowKey {
            columns: columns
                .iter()
                .filter(|c| c.is_key)
                .map(|c| c.name.clone())
                .collect(),
            kind: None,
        },
        columns: columns
            .iter()
            .map(|c| (c.name.clone(), c.type_oid))
            .collect(),
    };
    let replica_default = matches!(replica_identity, ReplicaIdentity::Default);
    if relation_different(spec, &relation, i64::from(*relation_id), replica_default) {
        Some(format!("schema of table \"{namespace}.{name}\" changed"))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use zero_cache_types::specs::{ColumnSpec, PublishedColumnSpec};

    fn col(name: &str, pos: i64, type_oid: i64, not_null: bool) -> (String, PublishedColumnSpec) {
        (
            name.to_string(),
            PublishedColumnSpec {
                column: ColumnSpec {
                    pos,
                    data_type: "text".into(),
                    pg_type_class: None,
                    elem_pg_type_class: None,
                    character_maximum_length: None,
                    not_null: Some(not_null),
                    dflt: None,
                },
                type_oid,
            },
        )
    }

    fn table(
        oid: i64,
        name: &str,
        pk: Vec<&str>,
        columns: Vec<(String, PublishedColumnSpec)>,
    ) -> PublishedTableSpec {
        PublishedTableSpec {
            name: name.into(),
            schema: "public".into(),
            oid,
            schema_oid: None,
            columns,
            primary_key: Some(pk.into_iter().map(String::from).collect()),
            replica_identity: None,
            publications: BTreeMap::new(),
        }
    }

    #[test]
    fn identical_tables_have_no_difference() {
        let t = table(1, "issues", vec!["id"], vec![col("id", 1, 25, true)]);
        assert_eq!(get_table_difference(&t, &t), None);
    }

    #[test]
    fn different_oid_is_a_difference() {
        let a = table(1, "issues", vec!["id"], vec![col("id", 1, 25, true)]);
        let b = table(2, "issues", vec!["id"], vec![col("id", 1, 25, true)]);
        assert!(get_table_difference(&a, &b)
            .unwrap()
            .contains("differs from"));
    }

    #[test]
    fn changed_primary_key_is_a_difference() {
        let a = table(
            1,
            "issues",
            vec!["id"],
            vec![col("id", 1, 25, true), col("org", 2, 25, true)],
        );
        let b = table(
            1,
            "issues",
            vec!["org"],
            vec![col("id", 1, 25, true), col("org", 2, 25, true)],
        );
        assert_eq!(
            get_table_difference(&a, &b),
            Some("Primary key of table \"issues\" has changed".to_string())
        );
    }

    #[test]
    fn changed_column_type_is_a_difference() {
        let a = table(1, "issues", vec!["id"], vec![col("id", 1, 25, true)]);
        let b = table(1, "issues", vec!["id"], vec![col("id", 1, 23, true)]); // different type_oid
        assert_eq!(
            get_table_difference(&a, &b),
            Some("Columns of table \"issues\" have changed".to_string())
        );
    }

    #[test]
    fn column_order_in_storage_does_not_matter_pos_does() {
        // Columns stored in a different order but with matching `pos` values
        // sort to the same effective order, so no difference is reported.
        let a = table(
            1,
            "issues",
            vec!["id"],
            vec![col("id", 1, 25, true), col("title", 2, 25, false)],
        );
        let b = table(
            1,
            "issues",
            vec!["id"],
            vec![col("title", 2, 25, false), col("id", 1, 25, true)],
        );
        assert_eq!(get_table_difference(&a, &b), None);
    }

    #[test]
    fn schema_difference_detects_table_count_change() {
        let t = table(1, "issues", vec!["id"], vec![col("id", 1, 25, true)]);
        assert_eq!(
            get_schema_difference(std::slice::from_ref(&t), &[t.clone(), t.clone()]),
            Some("tables created or dropped".to_string())
        );
    }

    #[test]
    fn schema_difference_none_when_all_tables_match() {
        let t1 = table(1, "issues", vec!["id"], vec![col("id", 1, 25, true)]);
        let t2 = table(2, "comments", vec!["id"], vec![col("id", 1, 25, true)]);
        assert_eq!(
            get_schema_difference(&[t1.clone(), t2.clone()], &[t1, t2]),
            None
        );
    }

    #[test]
    fn relation_different_detects_oid_change() {
        let a = table(1, "issues", vec!["id"], vec![col("id", 1, 25, true)]);
        let b = Relation {
            schema: "public".into(),
            name: "issues".into(),
            row_key: crate::data::RowKey {
                columns: vec!["id".into()],
                kind: None,
            },
            columns: vec![("id".into(), 25)],
        };
        assert!(relation_different(&a, &b, 999, true)); // OID mismatch
        assert!(!relation_different(&a, &b, 1, true)); // matches
    }

    #[test]
    fn relation_different_detects_key_column_change() {
        let a = table(1, "issues", vec!["id"], vec![col("id", 1, 25, true)]);
        let b = Relation {
            schema: "public".into(),
            name: "issues".into(),
            row_key: crate::data::RowKey {
                columns: vec!["other_col".into()],
                kind: None,
            },
            columns: vec![("id".into(), 25)],
        };
        assert!(relation_different(&a, &b, 1, true));
    }

    #[test]
    fn relation_different_detects_column_type_and_name_and_count_changes() {
        let a = table(1, "issues", vec!["id"], vec![col("id", 1, 25, true)]);
        let base = |columns: Vec<(String, i32)>| Relation {
            schema: "public".into(),
            name: "issues".into(),
            row_key: crate::data::RowKey {
                columns: vec!["id".into()],
                kind: None,
            },
            columns,
        };
        // Same identity + key, matching columns -> not different.
        assert!(!relation_different(
            &a,
            &base(vec![("id".into(), 25)]),
            1,
            true
        ));
        // A column TYPE OID change is detected even when oid/name/key match.
        assert!(relation_different(
            &a,
            &base(vec![("id".into(), 1043)]),
            1,
            true
        ));
        // A column NAME change is detected.
        assert!(relation_different(
            &a,
            &base(vec![("renamed".into(), 25)]),
            1,
            true
        ));
        // A column count change (added column) is detected.
        assert!(relation_different(
            &a,
            &base(vec![("id".into(), 25), ("extra".into(), 25)]),
            1,
            true
        ));
    }

    #[test]
    fn relation_message_drift_detects_streamed_schema_changes() {
        use crate::pgoutput::{PgoutputMessage, RelationColumn, ReplicaIdentity};

        let specs = vec![table(1, "issues", vec!["id"], vec![col("id", 1, 25, true)])];
        let rel = |cols: Vec<RelationColumn>| PgoutputMessage::Relation {
            relation_id: 1,
            namespace: "public".into(),
            name: "issues".into(),
            replica_identity: ReplicaIdentity::Default,
            columns: cols,
        };
        let idcol = |type_oid: i32, is_key: bool| RelationColumn {
            is_key,
            name: "id".into(),
            type_oid,
            atttypmod: -1,
        };

        // Matching relation -> no drift.
        assert_eq!(
            relation_message_drift(&rel(vec![idcol(25, true)]), &specs),
            None
        );
        // Column type change -> drift.
        assert!(relation_message_drift(&rel(vec![idcol(1043, true)]), &specs).is_some());
        // Added column -> drift.
        assert!(relation_message_drift(
            &rel(vec![
                idcol(25, true),
                RelationColumn {
                    is_key: false,
                    name: "title".into(),
                    type_oid: 25,
                    atttypmod: -1,
                },
            ]),
            &specs,
        )
        .is_some());
        // A non-Relation message is never drift.
        assert_eq!(
            relation_message_drift(
                &PgoutputMessage::Commit {
                    commit_lsn: 0,
                    end_lsn: 0,
                    commit_timestamp: 0,
                },
                &specs,
            ),
            None
        );
        // A relation naming a table not in the specs is ignored.
        assert_eq!(
            relation_message_drift(
                &PgoutputMessage::Relation {
                    relation_id: 2,
                    namespace: "public".into(),
                    name: "unknown".into(),
                    replica_identity: ReplicaIdentity::Default,
                    columns: vec![idcol(25, true)],
                },
                &specs,
            ),
            None
        );
    }
}
