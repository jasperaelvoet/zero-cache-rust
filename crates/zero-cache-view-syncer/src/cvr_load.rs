//! Partial port of `CVRStore.#load`'s in-memory reconstruction logic (the
//! part after the SQL rows come back — see `cvr-store.ts`'s `#load`, and
//! `asQuery`, its per-row `QueriesRow -> QueryRecord` converter).
//!
//! Scope: given flat rows already fetched from the `instances`/`clients`/
//! `queries`/`desires` tables (whose schema `cvr_schema_sql` generates),
//! reconstructs a [`Cvr`]. This is genuinely the "hard part" of `#load` —
//! merging four independent row sets into one nested structure — extracted
//! as pure logic so it's testable without a live Postgres connection.
//!
//! NOT ported (real gaps, deliberately deferred): the SQL queries
//! themselves (a live `tokio-postgres` transaction against the
//! `cvr_schema_sql` tables), task ownership/lease conflict handling
//! (`OwnershipError`, the `owner`/`grantedAt` compare-and-swap), and
//! `RowsVersionBehindError` (comparing `instances.version` against
//! `rowsVersion.version` to detect a lagging row cache) — all three need a
//! notion of "current task id" and live transaction semantics this port
//! doesn't model yet. This module assumes the simple case: the CVR
//! instance row it's given (if any) is already known to be owned and
//! caught up; the caller is responsible for those checks before calling
//! [`load_cvr_from_rows`].

use std::collections::BTreeMap;

use zero_cache_protocol::ast::Ast;
use zero_cache_shared::bigint_json::JsonValue;
use zero_cache_zql::ttl::{clamp_ttl, Ttl, DEFAULT_TTL_MS};

use crate::cvr_types::{
    ClientQueryRecord, ClientQueryState, ClientRecord, CustomQueryRecord, Cvr, ExternalQueryBase,
    InternalQueryRecord, QueryRecord, TtlClock,
};
use crate::cvr_version::{empty_cvr_version, version_from_string, CvrVersion, VersionError};

/// A loaded `clients` table row. Port of `Pick<ClientsRow, 'clientID'>`.
#[derive(Debug, Clone, PartialEq)]
pub struct LoadedClientRow {
    pub client_id: String,
}

/// A loaded `queries` table row. Port of `QueriesRow`.
#[derive(Debug, Clone, PartialEq)]
pub struct LoadedQueryRow {
    pub query_hash: String,
    pub client_ast: Option<Ast>,
    pub query_name: Option<String>,
    pub query_args: Option<Vec<JsonValue>>,
    pub patch_version: Option<String>,
    pub transformation_hash: Option<String>,
    pub transformation_version: Option<String>,
    pub internal: Option<bool>,
    pub row_set_signature: Option<String>,
}

/// A loaded `desires` table row (with the `ttlMs`/`inactivatedAtMs` SQL
/// column aliases already applied — see `cvr-store.ts`'s `AS "ttl"`/`AS
/// "inactivatedAt"`).
#[derive(Debug, Clone, PartialEq)]
pub struct LoadedDesireRow {
    pub client_id: String,
    pub query_hash: String,
    pub patch_version: String,
    pub deleted: Option<bool>,
    pub ttl: Option<f64>,
    pub inactivated_at: Option<f64>,
}

/// Error building a [`QueryRecord`] from a [`LoadedQueryRow`]. Port of the
/// `assert` in `asQuery` ("queryName and queryArgs must be set for custom
/// queries").
#[derive(Debug, PartialEq, thiserror::Error)]
pub enum AsQueryError {
    #[error("queryName and queryArgs must be set for custom queries (query {0:?})")]
    MissingCustomQueryFields(String),
    #[error(transparent)]
    Version(#[from] VersionError),
}

fn maybe_version(s: &Option<String>) -> Result<Option<CvrVersion>, VersionError> {
    s.as_deref().map(version_from_string).transpose()
}

/// Converts one loaded `queries` row into a [`QueryRecord`]. Port of
/// `asQuery`: no `clientAST` means a custom query (needs `queryName`+
/// `queryArgs`); otherwise `internal` selects between an `Internal` and a
/// `Client` record.
pub fn as_query(row: &LoadedQueryRow) -> Result<QueryRecord, AsQueryError> {
    let row_set_signature = row.row_set_signature.clone();

    let Some(ast) = row.client_ast.clone() else {
        let (Some(name), Some(args)) = (row.query_name.clone(), row.query_args.clone()) else {
            return Err(AsQueryError::MissingCustomQueryFields(
                row.query_hash.clone(),
            ));
        };
        return Ok(QueryRecord::Custom(CustomQueryRecord {
            base: ExternalQueryBase {
                id: row.query_hash.clone(),
                transformation_hash: row.transformation_hash.clone(),
                transformation_version: maybe_version(&row.transformation_version)?,
                row_set_signature,
                client_state: BTreeMap::new(),
                patch_version: maybe_version(&row.patch_version)?,
            },
            name,
            args,
        }));
    };

    if row.internal == Some(true) {
        Ok(QueryRecord::Internal(InternalQueryRecord {
            id: row.query_hash.clone(),
            transformation_hash: row.transformation_hash.clone(),
            transformation_version: maybe_version(&row.transformation_version)?,
            row_set_signature,
            ast,
        }))
    } else {
        Ok(QueryRecord::Client(ClientQueryRecord {
            base: ExternalQueryBase {
                id: row.query_hash.clone(),
                transformation_hash: row.transformation_hash.clone(),
                transformation_version: maybe_version(&row.transformation_version)?,
                row_set_signature,
                client_state: BTreeMap::new(),
                patch_version: maybe_version(&row.patch_version)?,
            },
            ast,
        }))
    }
}

fn client_state_mut(query: &mut QueryRecord) -> Option<&mut BTreeMap<String, ClientQueryState>> {
    match query {
        QueryRecord::Client(c) => Some(&mut c.base.client_state),
        QueryRecord::Custom(c) => Some(&mut c.base.client_state),
        QueryRecord::Internal(_) => None,
    }
}

/// Reconstructs a fresh [`Cvr`] (with `id`, empty version/clients/queries)
/// and merges `clients_rows`/`query_rows`/`desires_rows` into it. Port of
/// the row-merging tail of `CVRStore.#load` (the `for (const row of
/// clientsRows/queryRows/desiresRows)` loops), for the "new/empty CVR"
/// case — an already-populated `instances` row's fields
/// (version/lastActive/ttlClock/replicaVersion/profileID/clientSchema)
/// are the caller's responsibility to overlay afterward (see module doc:
/// this function doesn't do the ownership/version-catchup checks needed to
/// safely trust a loaded instance row).
pub fn load_cvr_from_rows(
    id: &str,
    clients_rows: &[LoadedClientRow],
    query_rows: &[LoadedQueryRow],
    desires_rows: &[LoadedDesireRow],
) -> Result<Cvr, AsQueryError> {
    let mut cvr = Cvr {
        id: id.to_string(),
        version: empty_cvr_version(),
        last_active: 0.0,
        ttl_clock: TtlClock::from_number(0.0),
        replica_version: None,
        clients: BTreeMap::new(),
        queries: BTreeMap::new(),
        client_schema: None,
        profile_id: None,
    };

    for row in clients_rows {
        cvr.clients.insert(
            row.client_id.clone(),
            ClientRecord {
                id: row.client_id.clone(),
                desired_query_ids: vec![],
            },
        );
    }

    for row in query_rows {
        cvr.queries.insert(row.query_hash.clone(), as_query(row)?);
    }

    for row in desires_rows {
        let deleted = row.deleted.unwrap_or(false);
        if !deleted && row.inactivated_at.is_none() {
            if let Some(client) = cvr.clients.get_mut(&row.client_id) {
                client.desired_query_ids.push(row.query_hash.clone());
            }
            // else: client was deleted but the query desire row is still
            // present — matches upstream's debug-log-and-skip.
        }

        if let Some(query) = cvr.queries.get_mut(&row.query_hash) {
            let visible = !deleted || row.inactivated_at.is_some();
            if visible {
                if let Some(client_state) = client_state_mut(query) {
                    let (clamped_ttl, _was_clamped) =
                        clamp_ttl(&Ttl::Millis(row.ttl.unwrap_or(DEFAULT_TTL_MS)));
                    client_state.insert(
                        row.client_id.clone(),
                        ClientQueryState {
                            inactivated_at: row.inactivated_at.map(TtlClock::from_number),
                            ttl: clamped_ttl,
                            deleted,
                            version: version_from_string(&row.patch_version)?,
                        },
                    );
                }
            }
        }
    }

    Ok(cvr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_protocol::ast::Ast;

    fn client_query_row(hash: &str) -> LoadedQueryRow {
        LoadedQueryRow {
            query_hash: hash.into(),
            client_ast: Some(Ast::default()),
            query_name: None,
            query_args: None,
            patch_version: Some("01".into()),
            transformation_hash: None,
            transformation_version: None,
            internal: Some(false),
            row_set_signature: None,
        }
    }

    #[test]
    fn as_query_custom_requires_name_and_args() {
        let row = LoadedQueryRow {
            query_hash: "h1".into(),
            client_ast: None,
            query_name: None,
            query_args: None,
            patch_version: None,
            transformation_hash: None,
            transformation_version: None,
            internal: None,
            row_set_signature: None,
        };
        assert_eq!(
            as_query(&row),
            Err(AsQueryError::MissingCustomQueryFields("h1".into()))
        );
    }

    #[test]
    fn as_query_custom_with_fields() {
        let row = LoadedQueryRow {
            query_hash: "h1".into(),
            client_ast: None,
            query_name: Some("myQuery".into()),
            query_args: Some(vec![JsonValue::Number(1.0)]),
            patch_version: None,
            transformation_hash: None,
            transformation_version: None,
            internal: None,
            row_set_signature: None,
        };
        let QueryRecord::Custom(c) = as_query(&row).unwrap() else {
            panic!("expected Custom")
        };
        assert_eq!(c.name, "myQuery");
        assert_eq!(c.args, vec![JsonValue::Number(1.0)]);
    }

    #[test]
    fn as_query_internal_vs_client() {
        let mut row = client_query_row("h1");
        row.internal = Some(true);
        assert!(matches!(as_query(&row).unwrap(), QueryRecord::Internal(_)));

        row.internal = Some(false);
        assert!(matches!(as_query(&row).unwrap(), QueryRecord::Client(_)));
    }

    #[test]
    fn load_cvr_from_rows_populates_clients_and_queries() {
        let cvr = load_cvr_from_rows(
            "cg1",
            &[LoadedClientRow {
                client_id: "c1".into(),
            }],
            &[client_query_row("h1")],
            &[],
        )
        .unwrap();
        assert_eq!(cvr.id, "cg1");
        assert!(cvr.clients.contains_key("c1"));
        assert!(cvr.queries.contains_key("h1"));
        assert_eq!(cvr.clients["c1"].desired_query_ids, Vec::<String>::new());
    }

    #[test]
    fn desire_row_adds_to_desired_query_ids_and_client_state() {
        let cvr = load_cvr_from_rows(
            "cg1",
            &[LoadedClientRow {
                client_id: "c1".into(),
            }],
            &[client_query_row("h1")],
            &[LoadedDesireRow {
                client_id: "c1".into(),
                query_hash: "h1".into(),
                patch_version: "02".into(),
                deleted: Some(false),
                ttl: Some(60_000.0),
                inactivated_at: None,
            }],
        )
        .unwrap();
        assert_eq!(cvr.clients["c1"].desired_query_ids, vec!["h1".to_string()]);
        let QueryRecord::Client(q) = &cvr.queries["h1"] else {
            panic!("expected Client")
        };
        assert_eq!(q.base.client_state["c1"].ttl, 60_000.0);
        assert!(!q.base.client_state["c1"].deleted);
    }

    #[test]
    fn deleted_desire_row_does_not_add_to_desired_query_ids() {
        let cvr = load_cvr_from_rows(
            "cg1",
            &[LoadedClientRow {
                client_id: "c1".into(),
            }],
            &[client_query_row("h1")],
            &[LoadedDesireRow {
                client_id: "c1".into(),
                query_hash: "h1".into(),
                patch_version: "02".into(),
                deleted: Some(true),
                ttl: None,
                inactivated_at: None,
            }],
        )
        .unwrap();
        assert!(cvr.clients["c1"].desired_query_ids.is_empty());
        // Deleted-without-inactivatedAt means "not visible" to the query's
        // client_state either.
        let QueryRecord::Client(q) = &cvr.queries["h1"] else {
            panic!("expected Client")
        };
        assert!(!q.base.client_state.contains_key("c1"));
    }

    #[test]
    fn deleted_desire_row_with_inactivated_at_retains_deleted_client_state() {
        let cvr = load_cvr_from_rows(
            "cg1",
            &[LoadedClientRow {
                client_id: "c1".into(),
            }],
            &[client_query_row("h1")],
            &[LoadedDesireRow {
                client_id: "c1".into(),
                query_hash: "h1".into(),
                patch_version: "02".into(),
                deleted: Some(true),
                ttl: Some(60_000.0),
                inactivated_at: Some(42.0),
            }],
        )
        .unwrap();

        assert!(cvr.clients["c1"].desired_query_ids.is_empty());
        let QueryRecord::Client(q) = &cvr.queries["h1"] else {
            panic!("expected Client")
        };
        let state = &q.base.client_state["c1"];
        assert!(state.deleted);
        assert_eq!(state.inactivated_at, Some(TtlClock::from_number(42.0)));
        assert_eq!(state.ttl, 60_000.0);
    }

    #[test]
    fn desire_row_for_deleted_client_is_skipped_not_errored() {
        // No client "c1" registered at all — matches upstream's
        // debug-log-and-skip rather than a KeyError/panic.
        let cvr = load_cvr_from_rows(
            "cg1",
            &[],
            &[client_query_row("h1")],
            &[LoadedDesireRow {
                client_id: "c1".into(),
                query_hash: "h1".into(),
                patch_version: "02".into(),
                deleted: Some(false),
                ttl: None,
                inactivated_at: None,
            }],
        )
        .unwrap();
        assert!(cvr.clients.is_empty());
    }

    #[test]
    fn desire_row_ttl_is_clamped_to_max() {
        let cvr = load_cvr_from_rows(
            "cg1",
            &[LoadedClientRow {
                client_id: "c1".into(),
            }],
            &[client_query_row("h1")],
            &[LoadedDesireRow {
                client_id: "c1".into(),
                query_hash: "h1".into(),
                patch_version: "02".into(),
                deleted: Some(false),
                ttl: Some(999_999_999.0),
                inactivated_at: None,
            }],
        )
        .unwrap();
        let QueryRecord::Client(q) = &cvr.queries["h1"] else {
            panic!("expected Client")
        };
        assert_eq!(
            q.base.client_state["c1"].ttl,
            zero_cache_zql::ttl::MAX_TTL_MS
        );
    }
}
