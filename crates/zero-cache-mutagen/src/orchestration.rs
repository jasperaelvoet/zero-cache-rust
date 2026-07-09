//! Port of `processMutationWithTx`'s pure task-planning logic: given a
//! CRUD mutation's ops and whether it's running in "error mode" (see
//! `mutagen.ts`'s big comment on the retry-in-error-mode policy, quoted in
//! `crud_ops`'s module doc) and whether the write authorizer allowed the
//! mutation, decide which SQL statements should run and in what order.
//!
//! Deliberately excluded (still real gaps, tracked in `PORTING.md`): write
//! authorization itself is now ported (`zero-cache-auth::write_authorizer`'s
//! `can_pre_mutation`/`can_post_mutation`/`normalize_ops`/
//! `validate_table_names`, composed into `authorize_mutation`, and further
//! composed with this module's `plan_mutation_sql` via
//! `zero-cache-auth::write_authorizer::authorize_and_plan_mutation`) — this
//! module itself still takes the authorization *verdict* as a plain `bool`
//! input rather than depending on `zero-cache-auth` directly, since
//! `zero-cache-auth` already depends on `zero-cache-mutagen` (for
//! `CrudOp`/`NormalizedCrudOp`) and the reverse dependency would cycle;
//! actually executing the returned SQL against a live transaction, and the
//! outer serialization-failure retry loop (`processMutation`'s
//! `MAX_SERIALIZATION_ATTEMPTS` loop) remain unported.

use crate::crud_ops::CrudOp;
use crate::sql::{get_delete_sql, get_insert_sql, get_update_sql, get_upsert_sql};

/// The ordered list of SQL statements `processMutationWithTx` would run for
/// one mutation. Port of its `tasks` array, after the `Promise.all`
/// fan-out is replaced with plain ordering (statements are logically
/// independent upstream too — `Promise.all`, not a sequential await chain —
/// but this crate has no transaction executor yet to actually run them
/// concurrently against).
pub fn plan_mutation_sql(ops: &[CrudOp], error_mode: bool, authorized: bool) -> Vec<String> {
    // Port of `tasks.unshift(() => checkSchemaVersionAndIncrementLastMutationID(...))`:
    // the last-mutation-id upsert always runs first, regardless of
    // errorMode/authorization — "Confirm the mutation even though it may
    // have been blocked by the authorizer. Authorizer blocking a mutation
    // is not an error but the correct result of the mutation." Callers
    // prepend that statement themselves via
    // `last_mutation_id::get_upsert_last_mutation_id_sql`, since it needs
    // shard/client-group/client-id context this function doesn't have.
    if error_mode || !authorized {
        return Vec::new();
    }
    ops.iter()
        .map(|op| match op {
            CrudOp::Insert(o) => get_insert_sql(o),
            CrudOp::Upsert(o) => get_upsert_sql(o),
            CrudOp::Update(o) => get_update_sql(o),
            CrudOp::Delete(o) => get_delete_sql(o),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crud_ops::{DeleteOp, InsertOp};
    use std::collections::BTreeMap;
    use zero_cache_shared::bigint_json::JsonValue;

    fn insert(id: &str) -> CrudOp {
        CrudOp::Insert(InsertOp {
            table_name: "t".into(),
            primary_key: vec!["id".into()],
            value: vec![("id".into(), JsonValue::String(id.into()))],
        })
    }

    #[test]
    fn error_mode_produces_no_sql_regardless_of_authorization() {
        let ops = vec![insert("a")];
        assert_eq!(plan_mutation_sql(&ops, true, true), Vec::<String>::new());
        assert_eq!(plan_mutation_sql(&ops, true, false), Vec::<String>::new());
    }

    #[test]
    fn unauthorized_produces_no_sql() {
        let ops = vec![insert("a")];
        assert_eq!(plan_mutation_sql(&ops, false, false), Vec::<String>::new());
    }

    #[test]
    fn authorized_normal_mode_produces_sql_per_op_in_order() {
        let ops = vec![
            insert("a"),
            CrudOp::Delete(DeleteOp {
                table_name: "t".into(),
                primary_key: vec!["id".into()],
                value: BTreeMap::from([("id".to_string(), JsonValue::String("b".into()))]),
            }),
        ];
        let sql = plan_mutation_sql(&ops, false, true);
        assert_eq!(sql.len(), 2);
        assert!(sql[0].starts_with("INSERT INTO"));
        assert!(sql[1].starts_with("DELETE FROM"));
    }

    #[test]
    fn empty_ops_produces_empty_sql() {
        assert_eq!(plan_mutation_sql(&[], false, true), Vec::<String>::new());
    }
}
