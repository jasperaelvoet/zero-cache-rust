//! Joins the replicator side to the view-syncer side at the commit→poke
//! boundary — the connective step a top-level sync process performs each time
//! the change-streamer fans out a commit.
//!
//! The replicator half writes each committed transaction's row changes into
//! the durable change-log (`zero-cache-sqlite::change_log`) and fans out a
//! `CommitNotification`. A view-syncer connection, on receiving that
//! notification, must decide which of the client's currently-"got" queries the
//! commit invalidated, re-hydrate exactly those, and poke the resulting row
//! diffs to the client. This module is that decision, composed from the pieces
//! each half already provides:
//!
//! 1. `ChangeLog::read_since(since)` → the change-log rows for this commit;
//! 2. `subscriber_catchup::changed_tables` → the distinct tables it touched;
//! 3. `query_invalidation::invalidated_query_hashes` → which tracked queries
//!    read those tables;
//! 4. `query_invalidation::queries_to_reexecute` → of those, the ones the
//!    client currently holds as "got" (the only ones needing a re-run);
//! 5. the caller's `rehydrate` closure → the row/config patches from
//!    re-executing each such query (the real IVM pipeline in production);
//! 6. `poke_builder::build_poke` → the wire poke messages.
//!
//! Keeping the re-hydration behind a closure is deliberate: this crate owns the
//! change-log↔invalidation↔poke wiring, while the IVM re-execution that turns a
//! query hash into concrete patches stays with the view-syncer/query layers.
//! This is the seam a running process fills with its live pipelines.

use std::collections::BTreeSet;

use zero_cache_protocol::ast::Ast;
use zero_cache_sqlite::change_log::ChangeLog;
use zero_cache_sqlite::subscriber_catchup::changed_tables;
use zero_cache_sqlite::{DbError, StatementRunner};
use zero_cache_view_syncer::client_patch::PatchToVersion;
use zero_cache_view_syncer::cvr_version::NullableCvrVersion;
use zero_cache_view_syncer::poke_builder::{build_poke, PokeBuildError, PokeMessages};
use zero_cache_view_syncer::query_invalidation::{invalidated_query_hashes, queries_to_reexecute};

/// Errors from [`pokes_for_commit`].
#[derive(Debug, thiserror::Error)]
pub enum CommitDispatchError {
    #[error(transparent)]
    Db(#[from] DbError),
    #[error(transparent)]
    Poke(#[from] PokeBuildError),
}

/// A tracked query on a connection: its stable hash, its AST (for the read-set
/// diff), and whether the client currently holds its results ("got").
pub struct TrackedQuery {
    pub hash: String,
    pub ast: Ast,
    pub got: bool,
}

/// Computes the poke to send a client for the commit whose change-log rows lie
/// strictly after `since_watermark`, given the connection's `tracked` queries.
///
/// Returns `Ok(None)` when the commit invalidates none of the client's got
/// queries (nothing to poke) or when re-hydration yields no patches. `rehydrate`
/// is invoked once per query hash that needs re-execution and returns that
/// query's patches (row puts/deletes + any config patch); in production this is
/// the live IVM pipeline, in tests a stand-in.
pub fn pokes_for_commit<F>(
    db: &StatementRunner,
    since_watermark: &str,
    tracked: &[TrackedQuery],
    poke_id: &str,
    base_version: &NullableCvrVersion,
    timestamp: Option<f64>,
    mut rehydrate: F,
) -> Result<Option<PokeMessages>, CommitDispatchError>
where
    F: FnMut(&str) -> Vec<PatchToVersion>,
{
    let entries = ChangeLog::new(db).read_since(since_watermark)?;
    let changed = changed_tables(&entries);
    if changed.is_empty() {
        return Ok(None);
    }

    let invalidated =
        invalidated_query_hashes(&changed, tracked.iter().map(|q| (q.hash.as_str(), &q.ast)));
    let got: BTreeSet<String> = tracked
        .iter()
        .filter(|q| q.got)
        .map(|q| q.hash.clone())
        .collect();
    let to_reexecute = queries_to_reexecute(&invalidated, &got);

    let mut patches: Vec<PatchToVersion> = Vec::new();
    for hash in &to_reexecute {
        patches.extend(rehydrate(hash));
    }

    Ok(build_poke(poke_id, base_version, &patches, timestamp)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use zero_cache_protocol::ast::Ast;
    use zero_cache_shared::bigint_json::JsonValue;
    use zero_cache_sqlite::change_log::{RowKey, CREATE_CHANGELOG_SCHEMA};
    use zero_cache_view_syncer::client_patch::{ClientPutRowPatch, ClientRowPatch, Patch};
    use zero_cache_view_syncer::cvr_types::RowId;
    use zero_cache_view_syncer::cvr_version::CvrVersion;

    fn db_with_changelog() -> StatementRunner {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        db
    }

    fn rk(id: i64) -> RowKey {
        vec![("id".to_string(), JsonValue::Number(id as f64))]
    }

    fn version(v: &str) -> CvrVersion {
        CvrVersion {
            state_version: v.into(),
            config_version: None,
        }
    }

    fn put_patch(table: &str, id: &str) -> PatchToVersion {
        PatchToVersion {
            patch: Patch::Row(ClientRowPatch::Put(ClientPutRowPatch {
                id: RowId {
                    schema: "public".into(),
                    table: table.into(),
                    row_key: BTreeMap::from([("id".to_string(), JsonValue::String(id.into()))]),
                },
                contents: vec![("title".to_string(), JsonValue::String("filed".into()))],
            })),
            to_version: version("02"),
        }
    }

    #[test]
    fn commit_touching_a_got_query_table_produces_a_poke() {
        let db = db_with_changelog();
        // A commit after watermark "01" changed the `issues` table.
        ChangeLog::new(&db)
            .log_set_op("02", 0, "issues", &rk(1), None)
            .unwrap();

        let tracked = vec![TrackedQuery {
            hash: "h1".into(),
            ast: Ast::table("issues"),
            got: true,
        }];

        let mut rehydrated_for: Vec<String> = Vec::new();
        let poke = pokes_for_commit(&db, "01", &tracked, "p1", &None, Some(1.0), |hash| {
            rehydrated_for.push(hash.to_string());
            vec![put_patch("issues", "1")]
        })
        .unwrap()
        .expect("an invalidated got query produced a poke");

        // Only h1 was re-hydrated, and its row made it into the poke.
        assert_eq!(rehydrated_for, vec!["h1".to_string()]);
        let rows = poke.part.rows_patch.expect("rows patch present");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn commit_touching_no_tracked_table_produces_no_poke() {
        let db = db_with_changelog();
        ChangeLog::new(&db)
            .log_set_op("02", 0, "unrelated", &rk(1), None)
            .unwrap();

        let tracked = vec![TrackedQuery {
            hash: "h1".into(),
            ast: Ast::table("issues"),
            got: true,
        }];

        let mut called = false;
        let poke = pokes_for_commit(&db, "01", &tracked, "p1", &None, Some(1.0), |_| {
            called = true;
            vec![put_patch("issues", "1")]
        })
        .unwrap();
        assert!(poke.is_none(), "unrelated table change pokes nothing");
        assert!(
            !called,
            "re-hydration must not run when nothing is invalidated"
        );
    }

    #[test]
    fn invalidated_but_not_got_query_is_not_rehydrated() {
        let db = db_with_changelog();
        ChangeLog::new(&db)
            .log_set_op("02", 0, "issues", &rk(1), None)
            .unwrap();

        // The query reads `issues` (so it's invalidated) but the client does
        // NOT currently hold it ("got: false") — it hydrates on its own path,
        // not via this commit poke.
        let tracked = vec![TrackedQuery {
            hash: "h1".into(),
            ast: Ast::table("issues"),
            got: false,
        }];

        let mut called = false;
        let poke = pokes_for_commit(&db, "01", &tracked, "p1", &None, Some(1.0), |_| {
            called = true;
            vec![put_patch("issues", "1")]
        })
        .unwrap();
        assert!(poke.is_none());
        assert!(!called, "a not-got query must not be re-hydrated here");
    }
}
