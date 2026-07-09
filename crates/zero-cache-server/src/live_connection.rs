//! The real per-connection handler `run_accept_loop`/`serve_connection`
//! actually drive — the literal closure passed to
//! [`crate::sync_server::run_accept_loop`]'s `make_handler` seam, closing the
//! last-named gap: `live_hydration`'s composition was previously proven as a
//! standalone call, not the handler a served connection invokes.
//!
//! [`DesiredQueriesHandler`] owns one connection's CVR state and its own
//! replica handle, and its [`DesiredQueriesHandler::on_action`] IS the
//! `FnMut(ConnectionAction) -> HandlerOutcome` [`crate::serve_connection::serve_connection`]
//! calls. On `Initialize`/`UpdateDesiredQueries` it applies the patch to the
//! CVR ([`CvrQueryHandler::apply_desired_queries_patch`]), hydrates any
//! newly-put query this connection recognizes against the real SQLite replica
//! ([`live_hydration::hydrate_patches_from_sqlite`]), merges both patch sets
//! into ONE wire poke (`build_poke`), and returns the poke's three frames
//! (`pokeStart`/`pokePart`/`pokeEnd`) JSON-encoded via `poke_message_json` —
//! ready for `serve_connection` to send.
//!
//! Scope: hydration can now serve desired-query puts that carry an AST by
//! introspecting the AST root table from SQLite, plus single/compound/nested
//! `related` child reads constrained by the fetched parent rows. Name-only/
//! custom query puts still use the small hardcoded query-hash -> table registry
//! unless a transform is available, and full planner-backed query execution is
//! still outside this slice. Live inspect `analyzeQuery` is broader: it
//! introspects table/column/primary-key metadata from SQLite for the requested
//! AST graph before analysis.

use std::collections::{BTreeMap, HashMap, HashSet};

use zero_cache_auth::policy::PermissionsConfig;
use zero_cache_auth::read_authorizer::transform_and_hash_query;
use zero_cache_mutagen::api_request::HeaderOptions;
use zero_cache_mutagen::crud_ops_json::crud_ops_from_json;
use zero_cache_mutagen::last_mutation_id::{check_mutation_id, MutationIdCheck};
use zero_cache_mutagen::orchestration::plan_mutation_sql;
use zero_cache_protocol::ast::{
    ColumnReference, Condition, CorrelatedSubquery, Direction, LiteralValue, Ordering,
    SimpleOperator, ValuePosition,
};
use zero_cache_protocol::client_schema::ValueType;
use zero_cache_protocol::custom_queries::TransformRequestQuery;
use zero_cache_protocol::inspect_down::InspectDownBody;
use zero_cache_protocol::inspect_down_json::inspect_down_message_json;
use zero_cache_protocol::inspect_up::InspectUpBody;
use zero_cache_protocol::mutation_result::{
    MutationError, MutationOk, MutationResponse, MutationResult, MutationZeroError, ZeroErrorKind,
};
use zero_cache_protocol::poke::PokeMessage;
use zero_cache_protocol::poke_json::poke_message_json;
use zero_cache_protocol::pull::{PullRequestBody, PullResponseBody};
use zero_cache_protocol::pull_json::pull_response_message_json;
use zero_cache_protocol::push::{Mutation, PushBody, PushOk};
use zero_cache_protocol::push_json::push_ok_message_json;
use zero_cache_protocol::queries_patch::{UpQueriesPatchOp, UpQueriesPutOp};
use zero_cache_protocol::query_hash::hash_of_name_and_args;
use zero_cache_shared::bigint_json::JsonValue;
use zero_cache_shared::timed_cache::TimedCache;
use zero_cache_sqlite::lite_tables::list_tables;
use zero_cache_sqlite::StatementRunner;
use zero_cache_view_syncer::connection_dispatch::ConnectionAction;
use zero_cache_view_syncer::cvr_query_handler::CvrQueryHandler;
use zero_cache_view_syncer::cvr_types::{RowId, RowRecord};
use zero_cache_view_syncer::cvr_version::version_to_cookie;
use zero_cache_view_syncer::transform_query_fetch::fetch_and_shape_transform_response;
use zero_cache_view_syncer::transform_query_response::{
    HashedTransformResponse, TransformedAndHashed, TransformedOrErrored,
};
use zero_cache_zql::ivm::constraint::PrimaryKey;
use zero_cache_zql::ivm::data::Row as ZqlRow;

use crate::analyze_query::{analyze_sqlite_ast_query, AnalyzeQueryColumn, AnalyzeQueryTable};
use crate::inspect_handler::handle_inspect;
use crate::inspector_delegate::InspectorDelegate;
use crate::live_hydration::{
    hydrate_patches_from_sqlite_with_row_updates, hydrate_rows_from_sqlite_with_row_updates,
    HydratePatchesResult, RowIdentity,
};
use crate::serve_connection::HandlerOutcome;

/// A recognized query hash this handler can hydrate: which table it reads,
/// under what name/primary-key/columns. Real deployments derive this from the
/// AST-to-SQL compiler; this is a static stand-in registry for the wiring
/// slice this module proves.
struct QueryDef {
    table_name: &'static str,
    primary_key: &'static [&'static str],
    columns: &'static [QueryDefColumn],
}

struct QueryDefColumn {
    name: &'static str,
    value_type: ValueType,
    optional: bool,
}

#[derive(Clone)]
struct HydrationPlan {
    table_name: String,
    primary_key: PrimaryKey,
    columns: Vec<String>,
}

/// The queries this handler recognizes. Extend by adding an entry; the
/// wiring itself (apply patch -> hydrate -> merge -> poke) is generic.
fn query_catalog() -> BTreeMap<&'static str, QueryDef> {
    BTreeMap::from([(
        "issue-all",
        QueryDef {
            table_name: "issue",
            primary_key: &["id"],
            columns: &[
                QueryDefColumn {
                    name: "id",
                    value_type: ValueType::Number,
                    optional: false,
                },
                QueryDefColumn {
                    name: "title",
                    value_type: ValueType::String,
                    optional: false,
                },
            ],
        },
    )])
}

fn analyze_catalog(catalog: &BTreeMap<&'static str, QueryDef>) -> Vec<AnalyzeQueryTable> {
    catalog
        .values()
        .map(|def| AnalyzeQueryTable {
            table_name: def.table_name.to_string(),
            primary_key: def.primary_key.iter().map(|key| key.to_string()).collect(),
            columns: def
                .columns
                .iter()
                .map(|column| AnalyzeQueryColumn {
                    name: column.name.to_string(),
                    value_type: column.value_type,
                    optional: column.optional,
                })
                .collect(),
        })
        .collect()
}

fn row_str(row: &ZqlRow, col: &str) -> String {
    match row.iter().find(|(k, _)| k == col) {
        Some((_, JsonValue::Number(n))) => (*n as i64).to_string(),
        Some((_, JsonValue::String(s))) => s.clone(),
        other => panic!("unexpected {col}: {other:?}"),
    }
}

fn identity_for(def: &QueryDef, query_id: &str) -> RowIdentity<String> {
    let table = def.table_name.to_string();
    let query_id = query_id.to_string();
    RowIdentity {
        row_key: Box::new(|row| format!("row-{}", row_str(row, "id"))),
        row_ref_counts: Box::new(move |_row| BTreeMap::from([(query_id.clone(), 1i64)])),
        row_version: Box::new(|row| format!("v{}", row_str(row, "id"))),
        wire_row_id: Box::new(move |key: &String| RowId {
            schema: "public".into(),
            table: table.clone(),
            row_key: BTreeMap::from([("id".to_string(), JsonValue::String(key.clone()))]),
        }),
    }
}

fn identity_for_plan(plan: &HydrationPlan, query_id: &str) -> RowIdentity<String> {
    let table = plan.table_name.clone();
    let primary_key = plan.primary_key.clone();
    let row_key_primary_key = primary_key.clone();
    let ref_counts_query_id = query_id.to_string();
    let version_primary_key = primary_key.clone();
    let wire_primary_key = primary_key;
    RowIdentity {
        row_key: Box::new(move |row| row_key_from_primary_key(row, &row_key_primary_key)),
        row_ref_counts: Box::new(move |_row| BTreeMap::from([(ref_counts_query_id.clone(), 1i64)])),
        row_version: Box::new(move |row| {
            format!("v{}", row_key_from_primary_key(row, &version_primary_key))
        }),
        wire_row_id: Box::new(move |key: &String| RowId {
            schema: "public".into(),
            table: table.clone(),
            row_key: row_id_from_key_string(key, &wire_primary_key),
        }),
    }
}

/// The SQL `ORDER BY` for a query's root/related hydration: the query's own
/// `orderBy` when present, otherwise plain primary-key order. When an explicit
/// `orderBy` is given, the primary key is appended (skipping columns the
/// `orderBy` already names) so the ordering is total and the top-N kept under a
/// `limit` is deterministic even for a non-unique sort column.
fn sort_for_hydration(plan: &HydrationPlan, order_by: Option<&Ordering>) -> Ordering {
    match order_by {
        Some(order_by) if !order_by.is_empty() => {
            let mut sort: Ordering = order_by.clone();
            for key in &plan.primary_key {
                if !sort.iter().any(|(col, _)| col == key) {
                    sort.push((key.clone(), Direction::Asc));
                }
            }
            sort
        }
        _ => plan
            .primary_key
            .iter()
            .map(|s| (s.to_string(), Direction::Asc))
            .collect(),
    }
}

fn row_key_from_primary_key(row: &ZqlRow, primary_key: &[String]) -> String {
    primary_key
        .iter()
        .map(|field| {
            let value = row
                .iter()
                .find(|(name, _)| name == field)
                .map(|(_, value)| value.clone())
                .unwrap_or(JsonValue::Null);
            format!("{}={}", field, value.stringify())
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn row_id_from_key_string(key: &str, primary_key: &[String]) -> BTreeMap<String, JsonValue> {
    let mut row_key = BTreeMap::new();
    for (index, field) in primary_key.iter().enumerate() {
        let prefix = format!("{field}=");
        let raw = key
            .split('|')
            .nth(index)
            .and_then(|part| part.strip_prefix(&prefix))
            .unwrap_or("null");
        let value = zero_cache_shared::bigint_json::parse(raw).unwrap_or(JsonValue::Null);
        row_key.insert(field.clone(), value);
    }
    row_key
}

fn hydration_plan_from_ast(
    db: &StatementRunner,
    ast: &zero_cache_protocol::ast::Ast,
) -> Result<HydrationPlan, zero_cache_sqlite::DbError> {
    let tables = list_tables(db)?;
    let Some(table) = tables.into_iter().find(|table| table.name == ast.table) else {
        return Err(zero_cache_sqlite::DbError(format!(
            "desired-query hydration table `{}` is not in SQLite replica",
            ast.table
        )));
    };
    let primary_key = table.primary_key.unwrap_or_default();
    Ok(HydrationPlan {
        table_name: table.name,
        primary_key,
        columns: table.columns.into_iter().map(|(name, _)| name).collect(),
    })
}

fn json_to_literal(value: &JsonValue) -> Option<LiteralValue> {
    match value {
        JsonValue::String(s) => Some(LiteralValue::String(s.clone())),
        JsonValue::Number(n) => Some(LiteralValue::Number(*n)),
        JsonValue::BigInt(n) => n.to_string().parse::<f64>().ok().map(LiteralValue::Number),
        JsonValue::Bool(b) => Some(LiteralValue::Bool(*b)),
        JsonValue::Null => Some(LiteralValue::Null),
        JsonValue::Array(_) | JsonValue::Object(_) => None,
    }
}

fn related_filter_from_parent_rows(
    parent_rows: &[(RowId, zero_cache_protocol::row_patch::Row)],
    related: &CorrelatedSubquery,
) -> Option<Condition> {
    if related.correlation.parent_field.is_empty()
        || related.correlation.parent_field.len() != related.correlation.child_field.len()
    {
        return None;
    }

    let correlation_filter = if parent_rows.is_empty() {
        Condition::Or { conditions: vec![] }
    } else if related.correlation.parent_field.len() == 1 {
        let parent_field = &related.correlation.parent_field[0];
        let child_field = &related.correlation.child_field[0];
        let mut values = Vec::new();
        for (_, row) in parent_rows {
            if let Some((_, value)) = row.iter().find(|(field, _)| field == parent_field) {
                if let Some(literal) = json_to_literal(value) {
                    values.push(literal);
                }
            }
        }
        if values.is_empty() {
            Condition::Or { conditions: vec![] }
        } else {
            Condition::Simple {
                op: SimpleOperator::In,
                left: ValuePosition::Column(ColumnReference {
                    name: child_field.clone(),
                }),
                right: ValuePosition::Literal(LiteralValue::Array(values)),
            }
        }
    } else {
        let mut tuple_conditions = Vec::new();
        for (_, row) in parent_rows {
            let mut field_conditions = Vec::new();
            for (parent_field, child_field) in related
                .correlation
                .parent_field
                .iter()
                .zip(&related.correlation.child_field)
            {
                let Some((_, value)) = row.iter().find(|(field, _)| field == parent_field) else {
                    field_conditions.clear();
                    break;
                };
                let Some(literal) = json_to_literal(value) else {
                    field_conditions.clear();
                    break;
                };
                field_conditions.push(Condition::Simple {
                    op: SimpleOperator::Eq,
                    left: ValuePosition::Column(ColumnReference {
                        name: child_field.clone(),
                    }),
                    right: ValuePosition::Literal(literal),
                });
            }
            if !field_conditions.is_empty() {
                tuple_conditions.push(Condition::And {
                    conditions: field_conditions,
                });
            }
        }
        Condition::Or {
            conditions: tuple_conditions,
        }
    };

    Some(match related.subquery.where_.as_ref() {
        Some(where_) => Condition::And {
            conditions: vec![where_.clone(), correlation_filter],
        },
        None => correlation_filter,
    })
}

/// Collects correlated subqueries from one condition scope. Subqueries nested
/// inside a collected relation's own AST are intentionally left for the
/// recursive hydration pass, after that relation's child rows are known.
fn correlated_subqueries_in_condition(condition: &Condition) -> Vec<CorrelatedSubquery> {
    fn collect(condition: &Condition, out: &mut Vec<CorrelatedSubquery>) {
        match condition {
            Condition::Simple { .. } => {}
            Condition::And { conditions } | Condition::Or { conditions } => {
                for condition in conditions {
                    collect(condition, out);
                }
            }
            Condition::CorrelatedSubquery { related, .. } => out.push(related.clone()),
        }
    }

    let mut related = Vec::new();
    collect(condition, &mut related);
    related
}

fn hydrate_related_rows_recursive(
    db: &StatementRunner,
    cvr: &mut zero_cache_view_syncer::cvr_types::Cvr,
    orig_version: &zero_cache_view_syncer::cvr_version::CvrVersion,
    query_hash: &str,
    parent_rows: &[(RowId, zero_cache_protocol::row_patch::Row)],
    related: &[CorrelatedSubquery],
) -> Result<HydratePatchesResult, zero_cache_sqlite::DbError> {
    let mut result = HydratePatchesResult {
        patches: Vec::new(),
        row_updates: Vec::new(),
        row_bodies: Vec::new(),
    };

    for relation in related {
        let child_plan = hydration_plan_from_ast(db, &relation.subquery)?;
        let child_identity = identity_for_plan(&child_plan, query_hash);
        let child_sort = sort_for_hydration(&child_plan, relation.subquery.order_by.as_ref());
        let child_limit = relation.subquery.limit.map(|n| n.max(0.0) as usize);
        let child_start = relation.subquery.start.as_ref();

        // A related subquery's `limit`/`start` are per-parent (upstream applies
        // them in `Take`/skip operators downstream of the correlation). Fetching
        // all children for all parents in one `IN (...)` read and then
        // truncating/seeking would be a wrong global cap/cursor, so when either
        // is present we fetch each parent's children separately (single-parent
        // correlation filter + child ordering + per-parent truncate/cursor = a
        // correct per-parent window). Without them the batched single-read path
        // stays, since ordering alone drops no rows.
        let parent_batches: Vec<Vec<(RowId, zero_cache_protocol::row_patch::Row)>> =
            if child_limit.is_some() || child_start.is_some() {
                parent_rows.iter().map(|row| vec![row.clone()]).collect()
            } else {
                vec![parent_rows.to_vec()]
            };

        let mut child_result = HydratePatchesResult {
            patches: Vec::new(),
            row_updates: Vec::new(),
            row_bodies: Vec::new(),
        };
        for batch in &parent_batches {
            let Some(child_filter) = related_filter_from_parent_rows(batch, relation) else {
                continue;
            };
            let batch_result = hydrate_rows_from_sqlite_with_row_updates(
                db,
                child_plan.table_name.clone(),
                child_plan.primary_key.clone(),
                child_sort.clone(),
                child_plan.columns.clone(),
                cvr,
                orig_version,
                &child_identity,
                &HashMap::new(),
                Some(&child_filter),
                child_limit,
                child_start,
            )?;
            child_result.patches.extend(batch_result.patches);
            child_result.row_updates.extend(batch_result.row_updates);
            child_result.row_bodies.extend(batch_result.row_bodies);
        }

        // A related row is evaluated by the client with the subquery's full
        // local pipeline. Hydrate correlated-subquery rows referenced by its
        // `where_` as well as rows requested for result shaping via
        // `related`, otherwise a nested `whereExists` sees an empty local
        // child table and incorrectly removes this row on the client.
        if let Some(where_) = &relation.subquery.where_ {
            let exists_related = correlated_subqueries_in_condition(where_);
            let nested_result = hydrate_related_rows_recursive(
                db,
                cvr,
                orig_version,
                query_hash,
                &child_result.row_bodies,
                &exists_related,
            )?;
            child_result.row_updates.extend(nested_result.row_updates);
            child_result.row_bodies.extend(nested_result.row_bodies);
            child_result.patches.extend(nested_result.patches);
        }

        if let Some(nested) = &relation.subquery.related {
            let nested_result = hydrate_related_rows_recursive(
                db,
                cvr,
                orig_version,
                query_hash,
                &child_result.row_bodies,
                nested,
            )?;
            child_result.row_updates.extend(nested_result.row_updates);
            child_result.row_bodies.extend(nested_result.row_bodies);
            child_result.patches.extend(nested_result.patches);
        }

        result.row_updates.extend(child_result.row_updates);
        result.row_bodies.extend(child_result.row_bodies);
        result.patches.extend(child_result.patches);
    }

    Ok(result)
}

pub struct CustomQueryTransformHttpConfig {
    pub client: reqwest::Client,
    pub url: String,
    pub schema: String,
    pub app_id: String,
    pub api_key: Option<String>,
    pub custom_headers: Vec<(String, String)>,
    pub request_headers: Vec<(String, String)>,
    pub auth_raw: Option<String>,
    pub cookie: Option<String>,
    pub origin: Option<String>,
    pub now_ms: i64,
    cache: TimedCache<String, TransformedAndHashed>,
}

impl CustomQueryTransformHttpConfig {
    pub fn new(
        url: impl Into<String>,
        schema: impl Into<String>,
        app_id: impl Into<String>,
    ) -> Self {
        CustomQueryTransformHttpConfig {
            client: reqwest::Client::new(),
            url: url.into(),
            schema: schema.into(),
            app_id: app_id.into(),
            api_key: None,
            custom_headers: Vec::new(),
            request_headers: Vec::new(),
            auth_raw: None,
            cookie: None,
            origin: None,
            now_ms: 0,
            cache: TimedCache::new(5000),
        }
    }
}

/// One served connection's stateful handler: owns its CVR and a replica
/// handle. This is literally the closure a connection's factory hands to
/// `serve_connection` — see the live test for the real wiring.
pub struct DesiredQueriesHandler {
    db: StatementRunner,
    cvr_handler: CvrQueryHandler,
    inspector_delegate: InspectorDelegate,
    client_group_id: String,
    row_records: Vec<RowRecord>,
    row_bodies: Vec<(RowId, zero_cache_protocol::row_patch::Row)>,
    tracked: HashSet<String>,
    poke_seq: u64,
    inspect_protocol_version: u32,
    inspect_server_version: String,
    inspect_development_mode: bool,
    inspect_admin_password: Option<String>,
    read_permissions: Option<PermissionsConfig>,
    auth_raw: Option<String>,
    custom_query_transform_http: Option<CustomQueryTransformHttpConfig>,
    /// Per-client last-mutation-id counters, standing in for the real
    /// upstream-Postgres `clients` table upsert
    /// (`zero_cache_mutagen::apply_mutation::apply_crud_mutation`, built and
    /// live-tested separately against real Postgres). See
    /// [`Self::apply_push`]'s doc for why push mutations are applied against
    /// the local replica here instead of calling that async executor.
    last_mutation_ids: BTreeMap<String, i64>,
    /// Mutation responses the client has not acknowledged yet. Production
    /// zero-cache persists/cleans this through the pusher; this demo handler
    /// keeps the same lifecycle in memory so `ackMutationResponses` is no
    /// longer a no-op.
    pending_mutation_responses: Vec<MutationResponse>,
    /// The connection's currently-desired PUT ops, kept so the queries can be
    /// RE-hydrated against the replica on each upstream commit (live sync).
    /// Keyed by query hash; a `del`/`clear` removes them.
    desired_puts: std::collections::BTreeMap<String, UpQueriesPutOp>,
    /// When set (synced mode), `push` mutations are applied to UPSTREAM Postgres
    /// via `apply_crud_mutation` (and flow back through replication), instead of
    /// the read-only local replica. `(libpq conn string, mutation schema)`.
    upstream_push: Option<(String, String)>,
    /// A lazily-opened upstream Postgres client for the push path (one per
    /// connection). `apply_crud_mutation` needs `&mut Client`.
    upstream_client: Option<tokio_postgres::Client>,
    /// When set, `push` mutations are forwarded to the app's custom-mutator API
    /// server (`ZERO_MUTATE_URL`) instead of applied locally/upstream.
    mutate_api: Option<crate::custom_mutation::MutateApi>,
}

impl DesiredQueriesHandler {
    pub fn new(db: StatementRunner, client_group_id: &str, client_id: &str) -> Self {
        Self::with_inspect_options(
            db,
            client_group_id,
            client_id,
            51,
            env!("CARGO_PKG_VERSION").to_string(),
            true,
            None,
        )
    }

    /// Seeds the connection's initial bearer token (the `authToken` from the
    /// connect handshake), so the first forwarded mutation/query carries it as
    /// `Authorization: Bearer …` — matching upstream, where the connection
    /// context holds `auth.raw` from connect. A later `updateAuth` overrides it.
    pub fn with_auth(mut self, token: Option<String>) -> Self {
        if token.is_some() {
            self.auth_raw = token;
        }
        self
    }

    /// Seeds the CVR from the client's connect `baseCookie` so a RECONNECTING
    /// client's first poke bases at the cookie it holds (not `null`). Without
    /// this, every reconnect fails with "unexpected base cookie during sync"
    /// because the fresh per-connection CVR would send `baseCookie:null` while
    /// the client is at e.g. `"00:01"`. Empty/absent cookie = a fresh client
    /// (leave the CVR empty → first poke bases at `null`).
    pub fn with_base_cookie(mut self, cookie: Option<String>) -> Self {
        let cookie = cookie.filter(|c| !c.is_empty());
        if let Ok(Some(version)) =
            zero_cache_view_syncer::cvr_version::cookie_to_version(cookie.as_deref())
        {
            self.cvr_handler.seed_version(version);
        }
        self
    }

    pub fn with_inspect_options(
        db: StatementRunner,
        client_group_id: &str,
        client_id: &str,
        inspect_protocol_version: u32,
        inspect_server_version: String,
        inspect_development_mode: bool,
        inspect_admin_password: Option<String>,
    ) -> Self {
        DesiredQueriesHandler {
            db,
            cvr_handler: CvrQueryHandler::new(client_group_id, client_id, None),
            inspector_delegate: InspectorDelegate::new(),
            client_group_id: client_group_id.to_string(),
            row_records: Vec::new(),
            row_bodies: Vec::new(),
            tracked: HashSet::new(),
            poke_seq: 0,
            inspect_protocol_version,
            inspect_server_version,
            inspect_development_mode,
            inspect_admin_password,
            read_permissions: None,
            auth_raw: None,
            custom_query_transform_http: None,
            last_mutation_ids: BTreeMap::new(),
            pending_mutation_responses: Vec::new(),
            desired_puts: std::collections::BTreeMap::new(),
            upstream_push: None,
            upstream_client: None,
            mutate_api: None,
        }
    }

    /// Forwards `push` mutations to the app's custom-mutator API server
    /// (`ZERO_MUTATE_URL`). Takes priority over the CRUD/upstream path.
    pub fn with_mutate_api(
        mut self,
        url: String,
        api_key: Option<String>,
        schema: String,
        app_id: String,
    ) -> Self {
        self.mutate_api = Some(crate::custom_mutation::MutateApi::new(url, api_key, schema, app_id));
        self
    }

    /// Like [`Self::with_mutate_api`] but also forwarding the client's session
    /// cookie + allowed client headers to the mutate server (cookie-auth apps).
    pub fn with_mutate_api_forwarding(
        mut self,
        url: String,
        api_key: Option<String>,
        schema: String,
        app_id: String,
        cookie: Option<String>,
        custom_headers: Vec<(String, String)>,
    ) -> Self {
        self.mutate_api = Some(
            crate::custom_mutation::MutateApi::new(url, api_key, schema, app_id)
                .with_forwarding(cookie, custom_headers),
        );
        self
    }

    /// Routes `push` mutations to UPSTREAM Postgres (schema `schema`, connected
    /// via libpq `conn_str`) instead of the local replica — the production write
    /// path. The mutation flows to Postgres and replicates back into the replica.
    pub fn with_upstream_push(mut self, conn_str: String, schema: String) -> Self {
        self.upstream_push = Some((conn_str, schema));
        self
    }

    pub fn with_custom_query_transform_http(
        mut self,
        config: CustomQueryTransformHttpConfig,
    ) -> Self {
        self.custom_query_transform_http = Some(config);
        self
    }

    pub fn with_read_permissions(mut self, permissions: PermissionsConfig) -> Self {
        self.read_permissions = Some(permissions);
        self
    }

    pub fn add_custom_query_transform(
        &mut self,
        name: &str,
        args: &[JsonValue],
        ast: zero_cache_protocol::ast::Ast,
    ) {
        self.inspector_delegate
            .add_custom_query_transform(name, args, ast);
    }

    pub fn pending_mutation_response_count(&self) -> usize {
        self.pending_mutation_responses.len()
    }

    pub fn auth_raw(&self) -> Option<&str> {
        self.auth_raw.as_deref()
    }

    /// The real handler: `serve_connection`/`run_accept_loop` call this for
    /// every routed [`ConnectionAction`].
    pub fn on_action(&mut self, action: ConnectionAction) -> HandlerOutcome {
        match action {
            ConnectionAction::Initialize(body) => {
                self.store_client_schema(&body);
                self.apply_and_poke(&body.desired_queries_patch)
            }
            ConnectionAction::UpdateDesiredQueries(body) => {
                self.apply_and_poke(&body.desired_queries_patch)
            }
            ConnectionAction::Push(body) => self.apply_push(&body),
            ConnectionAction::Inspect(body) => self.apply_inspect(body),
            ConnectionAction::AckMutationResponses(body) => self.apply_ack_mutation_response(&body),
            ConnectionAction::UpdateAuth(body) => self.apply_update_auth(&body),
            ConnectionAction::Pull(body) => self.apply_pull(&body),
            // Same boundary: the wire/router layer is ported, while
            // resolving auth and pusher cleanup need the full view-syncer /
            // pusher service wrappers.
            ConnectionAction::DeleteClients(_)
            | ConnectionAction::Close
            | ConnectionAction::Pong => HandlerOutcome::empty(),
        }
    }

    pub async fn on_action_async(&mut self, action: ConnectionAction) -> HandlerOutcome {
        match action {
            ConnectionAction::Initialize(body) => {
                self.store_client_schema(&body);
                self.fetch_missing_custom_query_transforms_for_patch(&body.desired_queries_patch)
                    .await;
                self.apply_and_poke(&body.desired_queries_patch)
            }
            ConnectionAction::UpdateDesiredQueries(body) => {
                self.fetch_missing_custom_query_transforms_for_patch(&body.desired_queries_patch)
                    .await;
                self.apply_and_poke(&body.desired_queries_patch)
            }
            ConnectionAction::Inspect(body) => self.apply_inspect_async(body).await,
            // Custom mutators take priority: forward the push to the app's
            // mutate API server (writes land upstream + replicate back).
            ConnectionAction::Push(body) if self.mutate_api.is_some() => {
                let api = self.mutate_api.clone().expect("checked");
                let responses = crate::custom_mutation::forward_push(
                    &api,
                    &body,
                    self.auth_raw.as_deref(),
                )
                .await;
                self.mutation_responses_outcome(responses)
            }
            ConnectionAction::Push(body) if self.upstream_push.is_some() => {
                self.apply_push_upstream(&body).await
            }
            other => self.on_action(other),
        }
    }

    /// Applies a `push`'s CRUD mutations to UPSTREAM Postgres via
    /// `apply_crud_mutation` (the live-tested executor), returning per-mutation
    /// responses. Used in synced mode where the local replica is read-only.
    async fn apply_push_upstream(&mut self, push: &PushBody) -> HandlerOutcome {
        let (conn_str, schema) = self.upstream_push.clone().expect("upstream configured");

        // Lazily open (once per connection) the upstream client.
        if self.upstream_client.is_none() {
            match zero_cache_change_source::pg_connection::connect(&conn_str).await {
                Ok(c) => self.upstream_client = Some(c),
                Err(e) => {
                    // Report every mutation as failed if we can't reach upstream.
                    let responses = push
                        .mutations
                        .iter()
                        .map(|m| MutationResponse {
                            id: m.id(),
                            result: MutationResult::Error(MutationError::App(
                                zero_cache_protocol::mutation_result::MutationAppError {
                                    message: Some(format!("upstream connect failed: {e}")),
                                    details: None,
                                },
                            )),
                        })
                        .collect();
                    return self.mutation_responses_outcome(responses);
                }
            }
        }
        let client = self.upstream_client.as_mut().expect("connected above");

        let mut responses = Vec::with_capacity(push.mutations.len());
        for mutation in &push.mutations {
            let id = mutation.id();
            let Mutation::Crud(crud) = mutation else {
                responses.push(MutationResponse {
                    id,
                    result: MutationResult::Error(MutationError::App(
                        zero_cache_protocol::mutation_result::MutationAppError {
                            message: Some("custom mutations are not supported".into()),
                            details: None,
                        },
                    )),
                });
                continue;
            };
            let ops = match crud_ops_from_json(&crud.ops_json) {
                Ok(ops) => ops,
                Err(e) => {
                    responses.push(MutationResponse {
                        id,
                        result: MutationResult::Error(MutationError::App(
                            zero_cache_protocol::mutation_result::MutationAppError {
                                message: Some(e.to_string()),
                                details: None,
                            },
                        )),
                    });
                    continue;
                }
            };
            let result = zero_cache_mutagen::apply_mutation::apply_crud_mutation(
                client,
                &schema,
                &push.client_group_id,
                &crud.client_id,
                crud.id as i64,
                &ops,
                true,  // authorized (read/write authorizers compose above this)
                false, // not error mode
            )
            .await;
            responses.push(MutationResponse {
                id,
                result: match result {
                    Ok(_) => MutationResult::Ok(MutationOk { data: None }),
                    Err(e) => MutationResult::Error(MutationError::App(
                        zero_cache_protocol::mutation_result::MutationAppError {
                            message: Some(e.to_string()),
                            details: None,
                        },
                    )),
                },
            });
        }
        self.mutation_responses_outcome(responses)
    }

    async fn fetch_missing_custom_query_transforms_for_patch(
        &mut self,
        patch: &[UpQueriesPatchOp],
    ) {
        for op in patch {
            let UpQueriesPatchOp::Put(p) = op else {
                continue;
            };
            if p.ast.is_some() {
                continue;
            }
            let Some(name) = p.name.as_deref() else {
                continue;
            };
            let args = p.args.clone().unwrap_or_default();
            if self
                .inspector_delegate
                .transform_custom_query(name, &args)
                .is_some()
            {
                continue;
            }
            crate::debug!("fetching custom-query transform for '{name}' ({} arg(s))", args.len());
            match self.fetch_and_register_custom_query_transform(name, &args).await {
                Ok(()) => crate::debug!("registered transform for query '{name}'"),
                Err(e) => crate::warn!("custom-query transform for '{name}' FAILED: {e}"),
            }
        }
    }

    async fn apply_inspect_async(&mut self, body: InspectUpBody) -> HandlerOutcome {
        let InspectUpBody::AnalyzeQuery {
            value,
            ast,
            name,
            args,
            ..
        } = &body
        else {
            return self.apply_inspect(body);
        };

        let args = args.clone().unwrap_or_default();
        if ast.is_none()
            && value.is_none()
            && name.as_deref().is_some_and(|name| {
                self.inspector_delegate
                    .transform_custom_query(name, &args)
                    .is_none()
            })
        {
            if let Some(name) = name.as_deref() {
                if let Err(e) = self
                    .fetch_and_register_custom_query_transform(name, &args)
                    .await
                {
                    return HandlerOutcome::send(vec![inspect_down_message_json(
                        &InspectDownBody::Error {
                            id: body.id().to_string(),
                            value: e,
                        },
                    )]);
                }
            }
        }

        self.apply_inspect(body)
    }

    async fn fetch_and_register_custom_query_transform(
        &mut self,
        name: &str,
        args: &[JsonValue],
    ) -> Result<(), String> {
        let Some(config) = self.custom_query_transform_http.as_mut() else {
            return Ok(());
        };
        let id = hash_of_name_and_args(name, args);
        let request = vec![TransformRequestQuery {
            id: id.clone(),
            name: name.to_string(),
            args: args.to_vec(),
        }];
        let headers = HeaderOptions {
            api_key: config.api_key.as_deref(),
            custom_headers: &config.custom_headers,
            request_headers: &config.request_headers,
            auth_raw: config.auth_raw.as_deref().or(self.auth_raw.as_deref()),
            cookie: config.cookie.as_deref(),
            origin: config.origin.as_deref(),
        };

        let response = fetch_and_shape_transform_response(
            &config.client,
            &config.url,
            &config.schema,
            &config.app_id,
            &headers,
            &request,
            vec![],
            &mut config.cache,
            |cache_id| cache_id.to_string(),
            config.now_ms,
        )
        .await
        .map_err(|e| e.to_string())?;

        match response {
            HashedTransformResponse::Failed(body) => Err(body.message),
            HashedTransformResponse::Success { result, .. } => {
                for item in result {
                    match item {
                        TransformedOrErrored::Ok(t) if t.id == id => {
                            self.inspector_delegate.add_custom_query_transform(
                                name,
                                args,
                                t.transformed_ast,
                            );
                            return Ok(());
                        }
                        TransformedOrErrored::Errored(e) if e.id == id => {
                            return Err(e
                                .message
                                .unwrap_or_else(|| "custom query transform failed".to_string()));
                        }
                        _ => {}
                    }
                }
                Err("custom query transform response did not include requested query".to_string())
            }
        }
    }

    fn apply_inspect(&mut self, body: InspectUpBody) -> HandlerOutcome {
        if let InspectUpBody::AnalyzeQuery {
            id,
            value,
            options,
            ast,
            name,
            args,
        } = body
        {
            let demo_catalog = query_catalog();
            let args = args.unwrap_or_default();
            let synced_query_id = name
                .as_deref()
                .map(|name| hash_of_name_and_args(name, &args));
            let transformed = match (ast.as_ref().or(value.as_ref()), name.as_deref()) {
                (Some(ast), _) => Some(ast.clone()),
                (None, Some(name)) => self
                    .inspector_delegate
                    .transform_custom_query(name, &args)
                    .cloned(),
                (None, None) => None,
            };
            let transformed = transformed.map(|ast| self.apply_read_permissions(&id, ast));
            let unresolved_custom_name = if transformed.is_none() {
                name.as_deref()
            } else {
                None
            };
            let catalog = match transformed.as_ref() {
                Some(ast) => crate::analyze_query::analyze_catalog_from_sqlite_ast(&self.db, ast),
                None => Ok(analyze_catalog(&demo_catalog)),
            };
            let response = match catalog.and_then(|catalog| {
                analyze_sqlite_ast_query(
                    &self.db,
                    &catalog,
                    transformed.as_ref(),
                    unresolved_custom_name,
                    options.as_ref(),
                    synced_query_id.as_deref(),
                    &self.row_records,
                    &self.row_bodies,
                )
            }) {
                Ok(value) => InspectDownBody::AnalyzeQuery { id, value },
                Err(e) => InspectDownBody::Error {
                    id,
                    value: e.to_string(),
                },
            };
            return HandlerOutcome::send(vec![inspect_down_message_json(&response)]);
        }

        let admin_password = self.inspect_admin_password.as_deref();
        let response = handle_inspect(
            body,
            &self.cvr_handler.cvr,
            &self.row_records,
            &mut self.inspector_delegate,
            &self.client_group_id,
            self.inspect_protocol_version,
            &self.inspect_server_version,
            self.inspect_development_mode,
            |password| admin_password.is_some_and(|expected| expected == password),
        );
        HandlerOutcome::send(vec![inspect_down_message_json(&response)])
    }

    fn apply_read_permissions(
        &self,
        query_id: &str,
        ast: zero_cache_protocol::ast::Ast,
    ) -> zero_cache_protocol::ast::Ast {
        let Some(permissions) = &self.read_permissions else {
            return ast;
        };
        transform_and_hash_query(query_id, &ast, permissions, false).transformed_ast
    }

    /// Applies a `push`'s CRUD mutations and returns a `pushResponse` frame.
    ///
    /// Scope decision: `serve_connection`'s handler contract
    /// (`FnMut(ConnectionAction) -> HandlerOutcome`) is synchronous, but the
    /// real upstream-Postgres executor
    /// (`zero_cache_mutagen::apply_mutation::apply_crud_mutation`, live-tested
    /// separately this round) is async `tokio-postgres` I/O — calling it here
    /// would require blocking the connection's task or a broader async-handler
    /// refactor across `serve_connection`/`sync_server`. Instead this applies
    /// each mutation's ops directly against the connection's own (synchronous)
    /// SQLite replica handle via the SAME `plan_mutation_sql` statement
    /// planner and the SAME `crud_ops_json`/`check_mutation_id` decode/check
    /// logic the real executor uses — proving the full decode -> plan -> apply
    /// -> respond pipeline for real, with the one substitution being which
    /// database receives the writes. A production deployment wires
    /// `apply_crud_mutation` into an async-capable handler instead.
    fn apply_push(&mut self, push: &PushBody) -> HandlerOutcome {
        let mut responses = Vec::with_capacity(push.mutations.len());
        for mutation in &push.mutations {
            let id = mutation.id();
            let Mutation::Crud(crud) = mutation else {
                // Custom mutators are a separate, unported subsystem (they
                // call out to a user-supplied API server, not local SQL).
                responses.push(MutationResponse {
                    id,
                    result: MutationResult::Error(MutationError::App(
                        zero_cache_protocol::mutation_result::MutationAppError {
                            message: Some(
                                "custom mutations are not supported by this handler".into(),
                            ),
                            details: None,
                        },
                    )),
                });
                continue;
            };

            let last = *self.last_mutation_ids.get(&crud.client_id).unwrap_or(&0);
            let received = crud.id as i64;
            match check_mutation_id(&crud.client_id, received, last + 1) {
                MutationIdCheck::Unexpected(e) => {
                    responses.push(MutationResponse {
                        id,
                        result: MutationResult::Error(MutationError::Zero(MutationZeroError {
                            error: ZeroErrorKind::OooMutation,
                            details: Some(JsonValue::String(e.error_body.message.clone())),
                        })),
                    });
                    continue;
                }
                MutationIdCheck::AlreadyProcessed(_) => {
                    responses.push(MutationResponse {
                        id,
                        result: MutationResult::Error(MutationError::Zero(MutationZeroError {
                            error: ZeroErrorKind::AlreadyProcessed,
                            details: None,
                        })),
                    });
                    continue;
                }
                MutationIdCheck::Ok => {}
            }
            self.last_mutation_ids
                .insert(crud.client_id.clone(), received);

            match crud_ops_from_json(&crud.ops_json) {
                Ok(ops) => {
                    let mut apply_err = None;
                    for stmt in plan_mutation_sql(&ops, false, true) {
                        if let Err(e) = self.db.exec(&stmt) {
                            apply_err = Some(e.to_string());
                            break;
                        }
                    }
                    responses.push(MutationResponse {
                        id,
                        result: match apply_err {
                            None => MutationResult::Ok(MutationOk { data: None }),
                            Some(msg) => MutationResult::Error(MutationError::App(
                                zero_cache_protocol::mutation_result::MutationAppError {
                                    message: Some(msg),
                                    details: None,
                                },
                            )),
                        },
                    });
                }
                Err(e) => {
                    responses.push(MutationResponse {
                        id,
                        result: MutationResult::Error(MutationError::App(
                            zero_cache_protocol::mutation_result::MutationAppError {
                                message: Some(e.to_string()),
                                details: None,
                            },
                        )),
                    });
                }
            }
        }

        self.mutation_responses_outcome(responses)
    }

    /// Builds the `pushResponse` frame for a set of mutation responses and
    /// records them as pending (for `ackMutationResponses`).
    fn mutation_responses_outcome(&mut self, responses: Vec<MutationResponse>) -> HandlerOutcome {
        // Only relay results for THIS connection's client. A push can carry
        // mutations for OTHER clients in the group (Replicache re-pushes dead
        // clients' unconfirmed mutations through whichever client is connected);
        // upstream's pusher fans each result out to its own client's connection.
        // Sending another client's result here is FATAL on the client side
        // ("received mutation for the wrong client") and closes the socket.
        let own = self.cvr_handler.client_id().to_string();
        let (mine, others): (Vec<_>, Vec<_>) = responses
            .into_iter()
            .partition(|r| r.id.client_id == own);
        if !others.is_empty() {
            crate::debug!(
                "dropping {} mutation result(s) for other clients in the group (not {own})",
                others.len()
            );
        }
        if mine.is_empty() {
            return HandlerOutcome::empty();
        }
        self.pending_mutation_responses.extend(mine.clone());
        HandlerOutcome::send(vec![push_ok_message_json(&PushOk { mutations: mine })])
    }

    fn apply_ack_mutation_response(
        &mut self,
        ack: &zero_cache_protocol::push::AckMutationResponsesBody,
    ) -> HandlerOutcome {
        self.pending_mutation_responses
            .retain(|response| response.id != ack.mutation_id);
        HandlerOutcome::empty()
    }

    fn apply_update_auth(
        &mut self,
        body: &zero_cache_protocol::update_auth::UpdateAuthBody,
    ) -> HandlerOutcome {
        self.auth_raw = Some(body.auth.clone());
        HandlerOutcome::empty()
    }

    fn apply_pull(&self, body: &PullRequestBody) -> HandlerOutcome {
        let last_mutation_id_changes = self
            .last_mutation_ids
            .iter()
            .map(|(client_id, last_mutation_id)| (client_id.clone(), *last_mutation_id as f64))
            .collect();
        HandlerOutcome::send(vec![pull_response_message_json(&PullResponseBody {
            cookie: version_to_cookie(self.cvr_handler.version())
                .expect("live CVR versions always encode as pull cookies"),
            request_id: body.request_id.clone(),
            last_mutation_id_changes,
        })])
    }

    /// Retains the client's declared schema (from `initConnection`) on the CVR
    /// via the validated `set_client_schema` transition: set-on-first-use, and
    /// on a later connection defensively reject (rather than overwrite) a schema
    /// that differs from the stored one — matching upstream's `setClientSchema`.
    /// Previously the received schema was dropped.
    ///
    /// A mismatch `Err` is currently swallowed (the stored schema is left
    /// intact, which is the invariant that matters); surfacing it as a
    /// downstream `error` frame that closes the connection is deferred with the
    /// rest of this demo handler's error-frame path. No-op when the client sent
    /// no schema.
    fn store_client_schema(&mut self, body: &zero_cache_protocol::connect::InitConnectionBody) {
        if let Some(schema) = &body.client_schema {
            // Canonicalize (sort tables/columns) before storing so the CVR's
            // order-sensitive JSON equality check matches upstream's
            // order-insensitive `deepEqual` on `setClientSchema`: two clients
            // sending the same schema in different key orders compare equal.
            let normalized = zero_cache_protocol::client_schema::normalize_client_schema(schema);
            let json = zero_cache_protocol::up_json::client_schema_to_json(&normalized);
            let _ = zero_cache_view_syncer::cvr_client_state::set_client_schema(
                &mut self.cvr_handler.cvr,
                &json,
            );
        }
    }

    fn apply_and_poke(&mut self, patch: &[UpQueriesPatchOp]) -> HandlerOutcome {
        let orig_version = self.cvr_handler.version().clone();
        let mut patches = self
            .cvr_handler
            .apply_desired_queries_patch(&patch.to_vec());

        // Hydrate every newly-put query this connection recognizes, and
        // remember/forget the put ops so they can be re-hydrated on later
        // upstream commits (see [`rehydrate_tracked`]).
        for op in patch {
            match op {
                UpQueriesPatchOp::Put(p) => {
                    self.desired_puts.insert(p.hash.clone(), p.clone());
                    patches.extend(self.hydrate_put(p, &orig_version));
                }
                UpQueriesPatchOp::Del(d) => {
                    self.desired_puts.remove(&d.hash);
                }
                UpQueriesPatchOp::Clear(_) => {
                    self.desired_puts.clear();
                }
            }
        }

        self.build_poke_outcome(orig_version, patches)
    }

    /// Re-hydrates the connection's currently-desired queries against the
    /// (now-updated) replica and returns any resulting incremental poke. Called
    /// on each upstream commit so a client that already holds a query receives
    /// live row changes — the live-sync counterpart to [`apply_and_poke`], which
    /// only hydrates *newly* put queries. Returns `HandlerOutcome::empty()` when
    /// nothing the client tracks changed.
    pub fn rehydrate_tracked(&mut self) -> HandlerOutcome {
        if self.desired_puts.is_empty() {
            return HandlerOutcome::empty();
        }
        let orig_version = self.cvr_handler.version().clone();
        // Re-executing an unchanged query doesn't bump the CVR version on its
        // own (`track_executed` only bumps on a transformation-hash change), but
        // the row-processing path requires the version to be above `orig` before
        // any row is emitted. Bump it once up front for the incremental poke.
        zero_cache_view_syncer::cvr_updater::ensure_new_version(
            &orig_version,
            &mut self.cvr_handler.cvr.version,
        );
        let puts: Vec<UpQueriesPutOp> = self.desired_puts.values().cloned().collect();
        let mut patches = Vec::new();
        for p in &puts {
            // Allow the already-tracked query to be RE-executed: clearing the
            // `tracked` marker lets `track_executed` run again, while the row
            // hydration still diffs against the CVR's existing rows, so only the
            // rows that actually changed since the last version are poked.
            self.tracked.remove(&p.hash);
            patches.extend(self.hydrate_put(p, &orig_version));
        }
        self.build_poke_outcome(orig_version, patches)
    }

    /// Builds a 3-frame poke `HandlerOutcome` from accumulated patches, or empty
    /// if there are none.
    fn build_poke_outcome(
        &mut self,
        orig_version: zero_cache_view_syncer::cvr_version::CvrVersion,
        patches: Vec<zero_cache_view_syncer::client_patch::PatchToVersion>,
    ) -> HandlerOutcome {
        if patches.is_empty() {
            return HandlerOutcome::empty();
        }
        let poke_id = {
            self.poke_seq += 1;
            format!("poke{}", self.poke_seq)
        };
        // The poke's `baseCookie` must equal the client's CURRENT cookie, or the
        // client's Replicache rejects it ("unexpected base cookie during sync").
        // When this CVR is still at the initial/empty version (a fresh client,
        // or a reconnecting one whose cookie we can't resume — this port keeps
        // CVR state per-connection), the base cookie must be NULL: a fresh sync
        // for the fresh client, a reset for the reconnecting one. Both accept
        // `null`; only a stale non-null base cookie errors. Subsequent pokes
        // then chain from the assigned (non-empty) version.
        use zero_cache_view_syncer::cvr_version::empty_cvr_version;
        let is_initial = orig_version.state_version == empty_cvr_version().state_version
            && matches!(orig_version.config_version, None | Some(0));
        let base_version = if is_initial { None } else { Some(orig_version) };
        let Ok(Some(poke)) = zero_cache_view_syncer::poke_builder::build_poke(
            &poke_id,
            &base_version,
            &patches,
            None,
        ) else {
            return HandlerOutcome::empty();
        };
        crate::debug!(
            "poke {} base={:?} cookie={} rows={} got={} desired={}",
            poke_id,
            poke.start.base_cookie,
            poke.end.cookie,
            poke.part.rows_patch.as_ref().map(|r| r.len()).unwrap_or(0),
            poke.part.got_queries_patch.as_ref().map(|g| g.len()).unwrap_or(0),
            poke.part.desired_queries_patches.as_ref().map(|d| d.len()).unwrap_or(0),
        );
        HandlerOutcome::send(vec![
            poke_message_json(&PokeMessage::Start(poke.start)),
            poke_message_json(&PokeMessage::Part(poke.part)),
            poke_message_json(&PokeMessage::End(poke.end)),
        ])
    }

    /// Hydrates one desired PUT op against the replica, returning the patches
    /// (query-state + row) it contributes. Shared by [`apply_and_poke`] and
    /// [`rehydrate_tracked`].
    fn hydrate_put(
        &mut self,
        p: &UpQueriesPutOp,
        orig_version: &zero_cache_view_syncer::cvr_version::CvrVersion,
    ) -> Vec<zero_cache_view_syncer::client_patch::PatchToVersion> {
        let mut patches = Vec::new();
        let started = std::time::Instant::now();
        let catalog = query_catalog();
        {
            let args = p.args.clone().unwrap_or_default();
            let transformed_ast = p.ast.as_ref().or_else(|| {
                p.name
                    .as_deref()
                    .and_then(|name| self.inspector_delegate.transform_custom_query(name, &args))
            });
            let ast_plan =
                transformed_ast.and_then(|ast| hydration_plan_from_ast(&self.db, ast).ok());
            let catalog_plan = catalog.get(p.hash.as_str()).map(|def| HydrationPlan {
                table_name: def.table_name.to_string(),
                primary_key: def.primary_key.iter().map(|s| s.to_string()).collect(),
                columns: def.columns.iter().map(|c| c.name.to_string()).collect(),
            });
            let Some(plan) = ast_plan.or(catalog_plan) else {
                return patches;
            };
            let identity = if transformed_ast.is_some() {
                identity_for_plan(&plan, &p.hash)
            } else {
                let Some(def) = catalog.get(p.hash.as_str()) else {
                    return patches;
                };
                identity_for(def, &p.hash)
            };
            // A client/transformed custom query's real `orderBy` becomes the
            // SQL `ORDER BY`; without one, fall back to primary-key order. The
            // primary key is always appended as a tiebreaker so the top-N under
            // `limit` is deterministic even when the query orders on a
            // non-unique column (matching how the upstream query builder
            // completes an `orderBy` with the primary key).
            let sort =
                sort_for_hydration(&plan, transformed_ast.and_then(|ast| ast.order_by.as_ref()));

            // A client or already-transformed custom query's real `where_`
            // condition — pushed all the way into SQL via `fetch_filtered`,
            // not evaluated in memory.
            let where_ = transformed_ast.and_then(|ast| ast.where_.as_ref());

            // The query's `limit`: hydrate only the top-N rows under `sort`.
            let limit = transformed_ast
                .and_then(|ast| ast.limit)
                .map(|n| n.max(0.0) as usize);

            // The query's `start` cursor bound: resume the SQL read at/after the
            // boundary row under `sort`, pushed into SQL by the fetch path.
            let start = transformed_ast.and_then(|ast| ast.start.as_ref());

            let root_result = hydrate_patches_from_sqlite_with_row_updates(
                &self.db,
                plan.table_name.clone(),
                plan.primary_key.clone(),
                sort,
                plan.columns.clone(),
                &mut self.cvr_handler.cvr,
                orig_version,
                &mut self.tracked,
                &p.hash,
                &p.hash, // transformation hash: reuse the query hash for this slice (no real AST-hash compiler wired here).
                &identity,
                &HashMap::new(),
                &[],
                where_,
                limit,
                start,
            );
            match root_result {
                Ok(mut result) => {
                    if let Some(ast) = transformed_ast {
                        // SQL pushdown uses correlated subqueries only to
                        // decide which root rows match. The Zero client runs
                        // the same pipeline over its local replica, so it also
                        // needs the matching child rows that made each
                        // `whereExists` true (including nodes below AND/OR).
                        if let Some(where_) = &ast.where_ {
                            let exists_related = correlated_subqueries_in_condition(where_);
                            if let Ok(related_result) = hydrate_related_rows_recursive(
                                &self.db,
                                &mut self.cvr_handler.cvr,
                                orig_version,
                                &p.hash,
                                &result.row_bodies,
                                &exists_related,
                            ) {
                                result.row_updates.extend(related_result.row_updates);
                                result.row_bodies.extend(related_result.row_bodies);
                                result.patches.extend(related_result.patches);
                            }
                        }
                        if let Some(related) = &ast.related {
                            if let Ok(related_result) = hydrate_related_rows_recursive(
                                &self.db,
                                &mut self.cvr_handler.cvr,
                                orig_version,
                                &p.hash,
                                &result.row_bodies,
                                related,
                            ) {
                                result.row_updates.extend(related_result.row_updates);
                                result.row_bodies.extend(related_result.row_bodies);
                                result.patches.extend(related_result.patches);
                            }
                        }
                    }
                    let hydrated_rows = result.row_bodies.len();
                    self.apply_row_updates(result.row_updates);
                    self.apply_row_bodies(result.row_bodies);
                    patches.extend(result.patches);
                    // Slow-query observability (ZERO_LOG_SLOW_HYDRATE_THRESHOLD /
                    // ZERO_LOG_SLOW_ROW_THRESHOLD).
                    crate::logging::maybe_log_slow_hydrate(
                        &p.hash,
                        started.elapsed().as_millis() as u64,
                        hydrated_rows,
                    );
                }
                Err(_) => return patches, // a hydration failure for one query doesn't sink the whole poke.
            }
        }
        patches
    }

    fn apply_row_updates(&mut self, updates: Vec<(RowId, Option<RowRecord>)>) {
        for (id, record) in updates {
            self.row_records.retain(|existing| existing.id != id);
            if let Some(record) = record {
                self.row_records.push(record);
            }
        }
    }

    fn apply_row_bodies(&mut self, updates: Vec<(RowId, zero_cache_protocol::row_patch::Row)>) {
        for (id, row) in updates {
            self.row_bodies.retain(|(existing, _)| existing != &id);
            self.row_bodies.push((id, row));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Message;

    fn seeded_db() -> StatementRunner {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
            .unwrap();
        db.run(
            "INSERT INTO issue (id, title) VALUES (1, 'wired end to end')",
            &[],
        )
        .unwrap();
        db
    }

    /// Live-sync core: a handler reading a WAL replica hydrates a query
    /// (initial poke), and after the WRITER commits a new row,
    /// `rehydrate_tracked` emits an incremental poke carrying that row — the
    /// per-connection commit relay `serve_synced_connection` drives.
    #[test]
    fn rehydrate_tracked_pokes_a_newly_committed_row() {
        let path = std::env::temp_dir()
            .join(format!("zc_livesync_{}.db", std::process::id()))
            .to_string_lossy()
            .into_owned();
        for s in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{path}{s}"));
        }

        // Writer (the replicator, here simulated) owns the replica.
        let writer = StatementRunner::open_file(&path).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
            .unwrap();
        writer
            .run("INSERT INTO issue (id, title) VALUES (1, 'first')", &[])
            .unwrap();

        // The connection reads its own read-only view.
        let reader = StatementRunner::open_file_readonly(&path).unwrap();
        let mut handler = DesiredQueriesHandler::new(reader, "cg1", "c1");

        // Desire `issue`; initial poke carries row 1.
        let put = UpQueriesPatchOp::Put(zero_cache_protocol::queries_patch::UpQueriesPutOp {
            hash: "issue-q".into(),
            ttl: None,
            ast: Some(zero_cache_protocol::ast::Ast::table("issue")),
            name: None,
            args: None,
        });
        let initial = handler.on_action(ConnectionAction::UpdateDesiredQueries(
            zero_cache_protocol::change_desired_queries::ChangeDesiredQueriesBody {
                desired_queries_patch: vec![put],
                traceparent: None,
            },
        ));
        assert_eq!(initial.responses.len(), 3, "initial 3-frame poke");
        assert!(
            initial.responses[1].contains("first"),
            "initial poke has row 1: {}",
            initial.responses[1]
        );

        // The writer commits a new row (an upstream change replicated in).
        writer
            .run("INSERT INTO issue (id, title) VALUES (2, 'live-update')", &[])
            .unwrap();

        // On the commit, re-hydration pokes the new row.
        let live = handler.rehydrate_tracked();
        assert_eq!(live.responses.len(), 3, "incremental 3-frame poke");
        assert!(
            live.responses[1].contains("live-update"),
            "live poke carries the newly committed row: {}",
            live.responses[1]
        );

        drop(handler);
        drop(writer);
        for s in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{path}{s}"));
        }
    }

    fn select_permissions(condition: zero_cache_protocol::ast::Condition) -> PermissionsConfig {
        zero_cache_auth::policy::PermissionsConfig {
            tables: Some(BTreeMap::from([(
                "issue".to_string(),
                zero_cache_auth::policy::TablePermissionsEntry {
                    row: Some(zero_cache_auth::policy::AssetPermissions {
                        select: Some(vec![condition]),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )])),
        }
    }

    async fn spawn_transform_response_server(response_body: String) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let body = response_body.clone();
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = [0u8; 4096];
                    let _ = stream.read(&mut buf).await;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        format!("http://{addr}/query")
    }

    async fn spawn_transform_response_server_capturing_request(
        response_body: String,
    ) -> (String, tokio::sync::oneshot::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = [0u8; 4096];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let _ = tx.send(String::from_utf8_lossy(&buf[..n]).into_owned());
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.shutdown().await;
        });
        (format!("http://{addr}/query"), rx)
    }

    #[test]
    fn initialize_retains_the_client_schema_on_the_cvr() {
        use zero_cache_protocol::client_schema::{
            ClientSchema, ColumnSchema, TableSchema, ValueType,
        };
        let mut handler = DesiredQueriesHandler::new(seeded_db(), "group1", "client1");

        // No schema stored before initConnection.
        assert_eq!(handler.cvr_handler.cvr.client_schema, None);

        let schema = ClientSchema {
            tables: vec![(
                "issue".into(),
                TableSchema {
                    columns: vec![(
                        "id".into(),
                        ColumnSchema {
                            value_type: ValueType::String,
                        },
                    )],
                    primary_key: vec!["id".into()],
                },
            )],
        };
        handler.on_action(ConnectionAction::Initialize(Box::new(
            zero_cache_protocol::connect::InitConnectionBody {
                client_schema: Some(schema.clone()),
                ..Default::default()
            },
        )));

        // The received schema is retained on the CVR as its JSON encoding.
        let stored = zero_cache_protocol::up_json::client_schema_to_json(&schema);
        assert_eq!(handler.cvr_handler.cvr.client_schema, Some(stored.clone()));

        // A later initConnection with a DIFFERENT schema must NOT overwrite the
        // stored one (validated `set_client_schema` rejects the change).
        let other = ClientSchema {
            tables: vec![(
                "issue".into(),
                TableSchema {
                    columns: vec![(
                        "id".into(),
                        ColumnSchema {
                            value_type: ValueType::Number,
                        },
                    )],
                    primary_key: vec!["id".into()],
                },
            )],
        };
        handler.on_action(ConnectionAction::Initialize(Box::new(
            zero_cache_protocol::connect::InitConnectionBody {
                client_schema: Some(other),
                ..Default::default()
            },
        )));
        assert_eq!(
            handler.cvr_handler.cvr.client_schema,
            Some(stored),
            "a mismatching schema is rejected, the original is retained"
        );
    }

    #[test]
    fn initialize_client_schema_comparison_is_column_order_insensitive() {
        use zero_cache_protocol::client_schema::{
            ClientSchema, ColumnSchema, TableSchema, ValueType,
        };
        let col = |vt| ColumnSchema { value_type: vt };
        // Same logical schema, columns declared in two different orders.
        let make = |cols: Vec<(&str, ValueType)>| ClientSchema {
            tables: vec![(
                "issue".into(),
                TableSchema {
                    columns: cols
                        .into_iter()
                        .map(|(n, vt)| (n.to_string(), col(vt)))
                        .collect(),
                    primary_key: vec!["id".into()],
                },
            )],
        };
        let schema_a = make(vec![
            ("id", ValueType::String),
            ("title", ValueType::String),
        ]);
        let schema_b = make(vec![
            ("title", ValueType::String),
            ("id", ValueType::String),
        ]);

        let mut handler = DesiredQueriesHandler::new(seeded_db(), "group1", "client1");
        let init = |s| {
            ConnectionAction::Initialize(Box::new(
                zero_cache_protocol::connect::InitConnectionBody {
                    client_schema: Some(s),
                    ..Default::default()
                },
            ))
        };
        handler.on_action(init(schema_a));
        let after_first = handler.cvr_handler.cvr.client_schema.clone();
        // The reordered-but-equivalent schema must be accepted (normalized to
        // the same canonical form), leaving the stored schema unchanged.
        handler.on_action(init(schema_b));
        assert_eq!(
            handler.cvr_handler.cvr.client_schema, after_first,
            "reordered columns normalize to the same schema; not treated as a mismatch"
        );
    }

    #[test]
    fn inspect_action_returns_encoded_downstream_frame() {
        let mut handler = DesiredQueriesHandler::with_inspect_options(
            seeded_db(),
            "group1",
            "client1",
            51,
            "test-version".to_string(),
            true,
            None,
        );

        let outcome = handler.on_action(ConnectionAction::Inspect(
            zero_cache_protocol::inspect_up::InspectUpBody::Version {
                id: "inspect-version".to_string(),
            },
        ));

        assert_eq!(outcome.responses.len(), 1);
        assert_eq!(
            outcome.responses[0],
            r#"["inspect",{"id":"inspect-version","op":"version","value":"test-version"}]"#
        );
    }

    /// The full loop, for real: `run_accept_loop` spawns a connection, its
    /// `make_handler` factory builds a `DesiredQueriesHandler` over a REAL
    /// seeded SQLite replica, a REAL client sends `initConnection` desiring
    /// the `issue-all` query, and the response poke — sent over the REAL
    /// socket by `serve_connection`'s normal send path, no test-only
    /// shortcut — carries the REAL row title from SQLite.
    #[tokio::test]
    async fn run_accept_loop_serves_real_hydrated_pokes_end_to_end() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = seeded_db();
                    let mut handler =
                        DesiredQueriesHandler::new(db, "group1", &format!("client{id}"));
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"issue-all","name":"issue-all","args":[]}
                ]}]"#,
            ))
            .await
            .unwrap();

        // pokeStart, pokePart (with the real row), pokeEnd.
        let start = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(start.contains("pokeStart"), "got {start}");
        let part = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(part.contains("pokePart"), "got {part}");
        assert!(
            part.contains("wired end to end"),
            "real replica content on the wire: {part}"
        );
        let end = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(end.contains("pokeEnd"), "got {end}");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn run_accept_loop_serves_real_inspect_version_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = seeded_db();
                    let mut handler = DesiredQueriesHandler::with_inspect_options(
                        db,
                        "group1",
                        &format!("client{id}"),
                        51,
                        "inspect-live-test".to_string(),
                        true,
                        None,
                    );
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[]}]"#,
            ))
            .await
            .unwrap();
        client
            .send(Message::text(
                r#"["inspect",{"op":"version","id":"inspect-version"}]"#,
            ))
            .await
            .unwrap();

        let response = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert_eq!(
            response,
            r#"["inspect",{"id":"inspect-version","op":"version","value":"inspect-live-test"}]"#
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn live_inspect_queries_include_hydrated_row_count() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = seeded_db();
                    let mut handler = DesiredQueriesHandler::with_inspect_options(
                        db,
                        "group1",
                        &format!("client{id}"),
                        51,
                        "inspect-live-test".to_string(),
                        true,
                        None,
                    );
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"issue-all","name":"issue-all","args":[]}
                ]}]"#,
            ))
            .await
            .unwrap();
        let _start = client.next().await.unwrap().unwrap().into_text().unwrap();
        let _part = client.next().await.unwrap().unwrap().into_text().unwrap();
        let _end = client.next().await.unwrap().unwrap().into_text().unwrap();

        client
            .send(Message::text(
                r#"["inspect",{"op":"queries","id":"inspect-queries"}]"#,
            ))
            .await
            .unwrap();

        let response = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(response.contains("\"op\":\"queries\""), "got {response}");
        assert!(
            response.contains("\"queryID\":\"issue-all\""),
            "got {response}"
        );
        assert!(response.contains("\"rowCount\":1"), "got {response}");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn live_inspect_analyze_query_reads_real_sqlite_rows() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = StatementRunner::open_in_memory().unwrap();
                    db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
                        .unwrap();
                    db.exec("INSERT INTO issue (id, title) VALUES (1, 'match me'), (2, 'skip me')")
                        .unwrap();
                    let mut handler = DesiredQueriesHandler::with_inspect_options(
                        db,
                        "group1",
                        &format!("client{id}"),
                        51,
                        "inspect-live-test".to_string(),
                        true,
                        None,
                    );
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[]}]"#,
            ))
            .await
            .unwrap();
        client
            .send(Message::text(
                r#"["inspect",{"op":"analyze-query","id":"inspect-analyze",
                    "ast":{"table":"issue",
                        "orderBy":[["id","asc"]],
                        "start":{"row":{"id":1},"exclusive":true},
                        "limit":1
                    },
                    "options":{"vendedRows":true}
                }]"#,
            ))
            .await
            .unwrap();

        let response = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(
            response.contains("\"op\":\"analyze-query\""),
            "got {response}"
        );
        assert!(response.contains("\"readRowCount\":1"), "got {response}");
        assert!(response.contains("\"skip me\""), "got {response}");
        assert!(!response.contains("match me"), "got {response}");
        assert!(response.contains("\"sqlitePlans\""), "got {response}");

        server.await.unwrap();
    }

    #[test]
    fn inspect_analyze_query_applies_configured_read_permissions() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
            .unwrap();
        db.exec("INSERT INTO issue (id, title) VALUES (1, 'allowed'), (2, 'denied')")
            .unwrap();
        let allowed_title = zero_cache_protocol::ast::Condition::Simple {
            op: zero_cache_protocol::ast::SimpleOperator::Eq,
            left: zero_cache_protocol::ast::ValuePosition::Column(
                zero_cache_protocol::ast::ColumnReference {
                    name: "title".to_string(),
                },
            ),
            right: zero_cache_protocol::ast::ValuePosition::Literal(
                zero_cache_protocol::ast::LiteralValue::String("allowed".to_string()),
            ),
        };
        let mut handler = DesiredQueriesHandler::with_inspect_options(
            db,
            "group1",
            "client1",
            51,
            "inspect-live-test".to_string(),
            true,
            None,
        )
        .with_read_permissions(select_permissions(allowed_title));

        let outcome = handler.on_action(ConnectionAction::Inspect(
            zero_cache_protocol::inspect_up::InspectUpBody::AnalyzeQuery {
                id: "inspect-authorized".to_string(),
                value: None,
                options: Some(zero_cache_protocol::inspect_up::AnalyzeQueryOptions {
                    vended_rows: Some(true),
                    synced_rows: None,
                    join_plans: None,
                }),
                ast: Some(zero_cache_protocol::ast::Ast::table("issue")),
                name: None,
                args: None,
            },
        ));

        assert_eq!(outcome.responses.len(), 1);
        let response = &outcome.responses[0];
        assert!(
            response.contains("\"op\":\"analyze-query\""),
            "got {response}"
        );
        assert!(response.contains("\"readRowCount\":1"), "got {response}");
        assert!(response.contains("\"allowed\""), "got {response}");
        assert!(!response.contains("\"denied\""), "got {response}");
    }

    #[test]
    fn inspect_analyze_query_introspects_tables_outside_demo_catalog() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE project (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        db.exec("INSERT INTO project (id, name) VALUES (1, 'from introspected schema')")
            .unwrap();
        let mut handler = DesiredQueriesHandler::with_inspect_options(
            db,
            "group1",
            "client1",
            51,
            "inspect-live-test".to_string(),
            true,
            None,
        );

        let outcome = handler.on_action(ConnectionAction::Inspect(
            zero_cache_protocol::inspect_up::InspectUpBody::AnalyzeQuery {
                id: "inspect-introspected".to_string(),
                value: None,
                options: Some(zero_cache_protocol::inspect_up::AnalyzeQueryOptions {
                    vended_rows: Some(true),
                    synced_rows: None,
                    join_plans: None,
                }),
                ast: Some(zero_cache_protocol::ast::Ast::table("project")),
                name: None,
                args: None,
            },
        ));

        assert_eq!(outcome.responses.len(), 1);
        let response = &outcome.responses[0];
        assert!(
            response.contains("\"op\":\"analyze-query\""),
            "got {response}"
        );
        assert!(response.contains("\"readRowCount\":1"), "got {response}");
        assert!(
            response.contains("\"from introspected schema\""),
            "got {response}"
        );
    }

    #[test]
    fn inspect_analyze_query_uses_registered_custom_query_transform() {
        let mut handler = DesiredQueriesHandler::with_inspect_options(
            seeded_db(),
            "group1",
            "client1",
            51,
            "inspect-live-test".to_string(),
            true,
            None,
        );
        handler.add_custom_query_transform(
            "issueByTitle",
            &[JsonValue::String("wired end to end".to_string())],
            zero_cache_protocol::ast::Ast {
                table: "issue".to_string(),
                where_: Some(zero_cache_protocol::ast::Condition::Simple {
                    op: zero_cache_protocol::ast::SimpleOperator::Eq,
                    left: zero_cache_protocol::ast::ValuePosition::Column(
                        zero_cache_protocol::ast::ColumnReference {
                            name: "title".to_string(),
                        },
                    ),
                    right: zero_cache_protocol::ast::ValuePosition::Literal(
                        zero_cache_protocol::ast::LiteralValue::String(
                            "wired end to end".to_string(),
                        ),
                    ),
                }),
                ..Default::default()
            },
        );

        let outcome = handler.on_action(ConnectionAction::Inspect(
            zero_cache_protocol::inspect_up::InspectUpBody::AnalyzeQuery {
                id: "inspect-custom".to_string(),
                value: None,
                options: Some(zero_cache_protocol::inspect_up::AnalyzeQueryOptions {
                    vended_rows: Some(true),
                    synced_rows: None,
                    join_plans: None,
                }),
                ast: None,
                name: Some("issueByTitle".to_string()),
                args: Some(vec![JsonValue::String("wired end to end".to_string())]),
            },
        ));

        assert_eq!(outcome.responses.len(), 1);
        let response = &outcome.responses[0];
        assert!(
            response.contains("\"op\":\"analyze-query\""),
            "got {response}"
        );
        assert!(response.contains("\"readRowCount\":1"), "got {response}");
        assert!(response.contains("\"wired end to end\""), "got {response}");
    }

    #[tokio::test]
    async fn async_inspect_analyze_query_uses_http_custom_query_transform() {
        let args = vec![JsonValue::String("wired end to end".to_string())];
        let query_id =
            zero_cache_protocol::query_hash::hash_of_name_and_args("issueByTitle", &args);
        let url = spawn_transform_response_server(format!(
            r#"{{
                "kind":"QueryResponse",
                "queries":[{{
                    "id":"{query_id}",
                    "name":"issueByTitle",
                    "ast":{{
                        "table":"issue",
                        "where":{{
                            "type":"simple","op":"=",
                            "left":{{"type":"column","name":"title"}},
                            "right":{{"type":"literal","value":"wired end to end"}}
                        }}
                    }}
                }}]
            }}"#
        ))
        .await;
        let mut handler = DesiredQueriesHandler::with_inspect_options(
            seeded_db(),
            "group1",
            "client1",
            51,
            "inspect-live-test".to_string(),
            true,
            None,
        )
        .with_custom_query_transform_http(CustomQueryTransformHttpConfig::new(
            url, "public", "app1",
        ));

        let outcome = handler
            .on_action_async(ConnectionAction::Inspect(
                zero_cache_protocol::inspect_up::InspectUpBody::AnalyzeQuery {
                    id: "inspect-http-custom".to_string(),
                    value: None,
                    options: Some(zero_cache_protocol::inspect_up::AnalyzeQueryOptions {
                        vended_rows: Some(true),
                        synced_rows: None,
                        join_plans: None,
                    }),
                    ast: None,
                    name: Some("issueByTitle".to_string()),
                    args: Some(args),
                },
            ))
            .await;

        assert_eq!(outcome.responses.len(), 1);
        let response = &outcome.responses[0];
        assert!(
            response.contains("\"op\":\"analyze-query\""),
            "got {response}"
        );
        assert!(response.contains("\"readRowCount\":1"), "got {response}");
        assert!(response.contains("\"wired end to end\""), "got {response}");
    }

    /// Proves the AST-to-SQL generalization: a client that sends a real AST
    /// (`ast.where_`, not just a bare `name`+`args` custom query) gets a poke
    /// containing ONLY the rows matching that condition — filtered by real
    /// SQL (`SqliteTableSource::fetch_filtered`), not by a hardcoded catalog
    /// entry. Two rows exist upstream; the query's `where title = 'match me'`
    /// must exclude the other.
    #[tokio::test]
    async fn ast_where_condition_is_pushed_into_real_sql_and_filters_the_poke() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = StatementRunner::open_in_memory().unwrap();
                    db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
                        .unwrap();
                    db.run("INSERT INTO issue (id, title) VALUES (1, 'match me')", &[])
                        .unwrap();
                    db.run(
                        "INSERT INTO issue (id, title) VALUES (2, 'not this one')",
                        &[],
                    )
                    .unwrap();
                    let mut handler =
                        DesiredQueriesHandler::new(db, "group1", &format!("client{id}"));
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        // A real AST with a real where_ condition, exactly what
        // up_json::upstream_from_json decodes off the wire from a real client.
        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"issue-all","ast":{"table":"issue","where":{
                        "type":"simple","op":"=",
                        "left":{"type":"column","name":"title"},
                        "right":{"type":"literal","value":"match me"}
                    }}}
                ]}]"#,
            ))
            .await
            .unwrap();

        let _start = client.next().await.unwrap().unwrap().into_text().unwrap();
        let part = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(part.contains("match me"), "matching row present: {part}");
        assert!(
            !part.contains("not this one"),
            "non-matching row excluded by real SQL: {part}"
        );
        let _end = client.next().await.unwrap().unwrap().into_text().unwrap();

        server.await.unwrap();
    }

    #[test]
    fn top_level_where_exists_hydrates_the_matching_child_rows() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
            .unwrap();
        db.exec("CREATE TABLE comments (id INTEGER PRIMARY KEY, issueID INTEGER, body TEXT)")
            .unwrap();
        db.exec("INSERT INTO issue (id, title) VALUES (1, 'matched parent'), (2, 'other parent')")
            .unwrap();
        db.exec(
            "INSERT INTO comments (id, issueID, body) VALUES \
             (10, 1, 'exists witness'), \
             (11, 1, 'filtered child'), \
             (12, 2, 'other parent child')",
        )
        .unwrap();

        let ast_json = zero_cache_shared::bigint_json::parse(
            r#"{
                "table":"issue",
                "where":{"type":"correlatedSubquery","op":"EXISTS","related":{
                    "correlation":{"parentField":["id"],"childField":["issueID"]},
                    "subquery":{"table":"comments","where":{
                        "type":"simple","op":"=",
                        "left":{"type":"column","name":"body"},
                        "right":{"type":"literal","value":"exists witness"}
                    }}
                }}
            }"#,
        )
        .unwrap();
        let ast = zero_cache_protocol::ast_json::ast_from_json(&ast_json).unwrap();
        let mut handler = DesiredQueriesHandler::new(db, "group1", "client1");
        let outcome = handler.on_action(ConnectionAction::Initialize(Box::new(
            zero_cache_protocol::connect::InitConnectionBody {
                desired_queries_patch: vec![UpQueriesPatchOp::Put(UpQueriesPutOp {
                    hash: "issue-where-exists".to_string(),
                    ttl: None,
                    ast: Some(ast),
                    name: None,
                    args: None,
                })],
                ..Default::default()
            },
        )));

        assert_eq!(outcome.responses.len(), 3);
        let part = &outcome.responses[1];
        assert!(part.contains("matched parent"), "got {part}");
        assert!(part.contains("exists witness"), "got {part}");
        assert!(!part.contains("other parent"), "got {part}");
        assert!(!part.contains("filtered child"), "got {part}");
    }

    #[test]
    fn related_where_exists_hydrates_nested_witness_rows() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
            .unwrap();
        db.exec("CREATE TABLE comments (id INTEGER PRIMARY KEY, issueID INTEGER, body TEXT)")
            .unwrap();
        db.exec("CREATE TABLE reactions (id INTEGER PRIMARY KEY, commentID INTEGER, emoji TEXT)")
            .unwrap();
        db.exec("INSERT INTO issue (id, title) VALUES (1, 'nested parent')")
            .unwrap();
        db.exec(
            "INSERT INTO comments (id, issueID, body) VALUES \
             (10, 1, 'comment with reaction'), \
             (11, 1, 'comment without reaction')",
        )
        .unwrap();
        db.exec(
            "INSERT INTO reactions (id, commentID, emoji) VALUES \
             (100, 10, 'nested witness'), \
             (101, 10, 'filtered reaction')",
        )
        .unwrap();

        let ast_json = zero_cache_shared::bigint_json::parse(
            r#"{
                "table":"issue",
                "related":[{
                    "correlation":{"parentField":["id"],"childField":["issueID"]},
                    "subquery":{"table":"comments","where":{
                        "type":"correlatedSubquery","op":"EXISTS","related":{
                            "correlation":{"parentField":["id"],"childField":["commentID"]},
                            "subquery":{"table":"reactions","where":{
                                "type":"simple","op":"=",
                                "left":{"type":"column","name":"emoji"},
                                "right":{"type":"literal","value":"nested witness"}
                            }}
                        }
                    }}
                }]
            }"#,
        )
        .unwrap();
        let ast = zero_cache_protocol::ast_json::ast_from_json(&ast_json).unwrap();
        let mut handler = DesiredQueriesHandler::new(db, "group1", "client1");
        let outcome = handler.on_action(ConnectionAction::Initialize(Box::new(
            zero_cache_protocol::connect::InitConnectionBody {
                desired_queries_patch: vec![UpQueriesPatchOp::Put(UpQueriesPutOp {
                    hash: "related-where-exists".to_string(),
                    ttl: None,
                    ast: Some(ast),
                    name: None,
                    args: None,
                })],
                ..Default::default()
            },
        )));

        assert_eq!(outcome.responses.len(), 3);
        let part = &outcome.responses[1];
        assert!(part.contains("nested parent"), "got {part}");
        assert!(part.contains("comment with reaction"), "got {part}");
        assert!(part.contains("nested witness"), "got {part}");
        assert!(!part.contains("comment without reaction"), "got {part}");
        assert!(!part.contains("filtered reaction"), "got {part}");
    }

    /// A real AST carrying `orderBy` + `limit`: the top-N rows under the
    /// ordering are the only ones synced. Because `limit` decides *which* rows
    /// are in the top-N (not just their display order), this proves the SQL
    /// `ORDER BY` and the top-N truncation are both live end to end — a
    /// different `orderBy` would sync a different set of rows.
    #[tokio::test]
    async fn ast_order_by_and_limit_hydrate_only_the_top_n_rows() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = StatementRunner::open_in_memory().unwrap();
                    db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
                        .unwrap();
                    db.run("INSERT INTO issue (id, title) VALUES (1, 'apple')", &[])
                        .unwrap();
                    db.run("INSERT INTO issue (id, title) VALUES (2, 'mango')", &[])
                        .unwrap();
                    db.run("INSERT INTO issue (id, title) VALUES (3, 'zebra')", &[])
                        .unwrap();
                    let mut handler =
                        DesiredQueriesHandler::new(db, "group1", &format!("client{id}"));
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        // orderBy title DESC, limit 2 -> only 'zebra' and 'mango' are in the
        // top-2; 'apple' (lowest under DESC) must be excluded.
        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"issue-all","ast":{"table":"issue",
                        "orderBy":[["title","desc"]],"limit":2}}
                ]}]"#,
            ))
            .await
            .unwrap();

        let _start = client.next().await.unwrap().unwrap().into_text().unwrap();
        let part = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(part.contains("zebra"), "top row present: {part}");
        assert!(part.contains("mango"), "second row present: {part}");
        assert!(
            !part.contains("apple"),
            "row beyond the limit under the ordering must be excluded: {part}"
        );
        let _end = client.next().await.unwrap().unwrap().into_text().unwrap();

        server.await.unwrap();
    }

    /// A real AST carrying a `start` cursor bound: the root read resumes
    /// strictly after the boundary row (`exclusive: true`) under the ordering,
    /// so the boundary row and everything before it are excluded from the poke.
    /// Proves the cursor is pushed into SQL end to end, not applied in memory.
    #[tokio::test]
    async fn ast_start_cursor_resumes_the_root_read_after_the_boundary_row() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = StatementRunner::open_in_memory().unwrap();
                    db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
                        .unwrap();
                    db.run("INSERT INTO issue (id, title) VALUES (1, 'first')", &[])
                        .unwrap();
                    db.run("INSERT INTO issue (id, title) VALUES (2, 'second')", &[])
                        .unwrap();
                    db.run("INSERT INTO issue (id, title) VALUES (3, 'third')", &[])
                        .unwrap();
                    let mut handler =
                        DesiredQueriesHandler::new(db, "group1", &format!("client{id}"));
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        // orderBy id ASC, start strictly after {id:1} -> only ids 2 and 3.
        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"issue-all","ast":{"table":"issue",
                        "orderBy":[["id","asc"]],
                        "start":{"row":{"id":1},"exclusive":true}}}
                ]}]"#,
            ))
            .await
            .unwrap();

        let _start = client.next().await.unwrap().unwrap().into_text().unwrap();
        let part = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(
            part.contains("second"),
            "row after the cursor present: {part}"
        );
        assert!(
            part.contains("third"),
            "row after the cursor present: {part}"
        );
        assert!(
            !part.contains("first"),
            "boundary row excluded by exclusive start cursor: {part}"
        );
        let _end = client.next().await.unwrap().unwrap().into_text().unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn desired_query_hydration_uses_ast_table_outside_demo_catalog() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = StatementRunner::open_in_memory().unwrap();
                    db.exec("CREATE TABLE project (id INTEGER PRIMARY KEY, name TEXT)")
                        .unwrap();
                    db.run(
                        "INSERT INTO project (id, name) VALUES (1, 'hydrated by ast')",
                        &[],
                    )
                    .unwrap();
                    let mut handler =
                        DesiredQueriesHandler::new(db, "group1", &format!("client{id}"));
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"project-all","ast":{"table":"project"}}
                ]}]"#,
            ))
            .await
            .unwrap();

        let _start = client.next().await.unwrap().unwrap().into_text().unwrap();
        let part = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(
            part.contains("hydrated by ast"),
            "AST-only query should hydrate from an introspected table: {part}"
        );
        let _end = client.next().await.unwrap().unwrap().into_text().unwrap();

        server.await.unwrap();
    }

    #[test]
    fn desired_query_hydration_uses_registered_custom_query_transform() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE project (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        db.exec(
            "INSERT INTO project (id, name) VALUES \
             (1, 'custom transform match'), \
             (2, 'custom transform skip')",
        )
        .unwrap();
        let args = vec![JsonValue::String("custom transform match".to_string())];
        let mut handler = DesiredQueriesHandler::with_inspect_options(
            db,
            "group1",
            "client1",
            51,
            "inspect-live-test".to_string(),
            true,
            None,
        );
        handler.add_custom_query_transform(
            "projectByName",
            &args,
            zero_cache_protocol::ast::Ast {
                table: "project".to_string(),
                where_: Some(zero_cache_protocol::ast::Condition::Simple {
                    op: zero_cache_protocol::ast::SimpleOperator::Eq,
                    left: zero_cache_protocol::ast::ValuePosition::Column(
                        zero_cache_protocol::ast::ColumnReference {
                            name: "name".to_string(),
                        },
                    ),
                    right: zero_cache_protocol::ast::ValuePosition::Literal(
                        zero_cache_protocol::ast::LiteralValue::String(
                            "custom transform match".to_string(),
                        ),
                    ),
                }),
                ..Default::default()
            },
        );

        let outcome = handler.on_action(ConnectionAction::Initialize(Box::new(
            zero_cache_protocol::connect::InitConnectionBody {
                desired_queries_patch: vec![UpQueriesPatchOp::Put(
                    zero_cache_protocol::queries_patch::UpQueriesPutOp {
                        hash: "custom-project".to_string(),
                        ttl: None,
                        ast: None,
                        name: Some("projectByName".to_string()),
                        args: Some(args),
                    },
                )],
                ..Default::default()
            },
        )));

        assert_eq!(outcome.responses.len(), 3);
        let part = &outcome.responses[1];
        assert!(part.contains("custom transform match"), "got {part}");
        assert!(!part.contains("custom transform skip"), "got {part}");
    }

    #[tokio::test]
    async fn async_desired_query_hydration_fetches_custom_query_transform() {
        let args = vec![JsonValue::String("async transform match".to_string())];
        let query_id =
            zero_cache_protocol::query_hash::hash_of_name_and_args("projectByName", &args);
        let url = spawn_transform_response_server(format!(
            r#"{{
                "kind":"QueryResponse",
                "queries":[{{
                    "id":"{query_id}",
                    "name":"projectByName",
                    "ast":{{
                        "table":"project",
                        "where":{{
                            "type":"simple","op":"=",
                            "left":{{"type":"column","name":"name"}},
                            "right":{{"type":"literal","value":"async transform match"}}
                        }}
                    }}
                }}]
            }}"#
        ))
        .await;

        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE project (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        db.exec(
            "INSERT INTO project (id, name) VALUES \
             (1, 'async transform match'), \
             (2, 'async transform skip')",
        )
        .unwrap();
        let mut handler = DesiredQueriesHandler::with_inspect_options(
            db,
            "group1",
            "client1",
            51,
            "inspect-live-test".to_string(),
            true,
            None,
        )
        .with_custom_query_transform_http(CustomQueryTransformHttpConfig::new(
            url, "public", "app1",
        ));

        let outcome = handler
            .on_action_async(ConnectionAction::Initialize(Box::new(
                zero_cache_protocol::connect::InitConnectionBody {
                    desired_queries_patch: vec![UpQueriesPatchOp::Put(
                        zero_cache_protocol::queries_patch::UpQueriesPutOp {
                            hash: query_id,
                            ttl: None,
                            ast: None,
                            name: Some("projectByName".to_string()),
                            args: Some(args),
                        },
                    )],
                    ..Default::default()
                },
            )))
            .await;

        assert_eq!(outcome.responses.len(), 3);
        let part = &outcome.responses[1];
        assert!(part.contains("async transform match"), "got {part}");
        assert!(!part.contains("async transform skip"), "got {part}");
    }

    #[tokio::test]
    async fn desired_query_hydration_fetches_top_level_related_rows() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = StatementRunner::open_in_memory().unwrap();
                    db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
                        .unwrap();
                    db.exec(
                        "CREATE TABLE comments (id INTEGER PRIMARY KEY, issueID INTEGER, body TEXT)",
                    )
                    .unwrap();
                    db.exec("INSERT INTO issue (id, title) VALUES (1, 'parent issue')")
                        .unwrap();
                    db.exec(
                        "INSERT INTO comments (id, issueID, body) VALUES \
                         (10, 1, 'related child'), \
                         (11, 99, 'unrelated child')",
                    )
                    .unwrap();
                    let mut handler =
                        DesiredQueriesHandler::new(db, "group1", &format!("client{id}"));
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"issue-with-comments","ast":{
                        "table":"issue",
                        "related":[{
                            "correlation":{"parentField":["id"],"childField":["issueID"]},
                            "subquery":{"table":"comments"}
                        }]
                    }}
                ]}]"#,
            ))
            .await
            .unwrap();

        let _start = client.next().await.unwrap().unwrap().into_text().unwrap();
        let part = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(part.contains("parent issue"), "got {part}");
        assert!(part.contains("related child"), "got {part}");
        assert!(!part.contains("unrelated child"), "got {part}");
        let _end = client.next().await.unwrap().unwrap().into_text().unwrap();

        server.await.unwrap();
    }

    /// A related subquery's `limit` is applied PER PARENT, not globally: each
    /// of the two parent issues keeps its own top-2 comments under the child
    /// ordering, so 4 comments survive out of 6 — proving the per-parent fetch
    /// path, since a global truncate would have kept only 2 comments total.
    #[tokio::test]
    async fn desired_query_hydration_applies_related_limit_per_parent() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = StatementRunner::open_in_memory().unwrap();
                    db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
                        .unwrap();
                    db.exec(
                        "CREATE TABLE comments (id INTEGER PRIMARY KEY, issueID INTEGER, body TEXT)",
                    )
                    .unwrap();
                    db.exec(
                        "INSERT INTO issue (id, title) VALUES (1, 'issue one'), (2, 'issue two')",
                    )
                    .unwrap();
                    // Three comments per issue; ids ascending so the kept top-2
                    // per parent are the two lowest-id comments of each issue.
                    db.exec(
                        "INSERT INTO comments (id, issueID, body) VALUES \
                         (10, 1, 'one-keep-a'), (11, 1, 'one-keep-b'), (12, 1, 'one-DROP'), \
                         (20, 2, 'two-keep-a'), (21, 2, 'two-keep-b'), (22, 2, 'two-DROP')",
                    )
                    .unwrap();
                    let mut handler =
                        DesiredQueriesHandler::new(db, "group1", &format!("client{id}"));
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"issue-with-comments","ast":{
                        "table":"issue",
                        "related":[{
                            "correlation":{"parentField":["id"],"childField":["issueID"]},
                            "subquery":{"table":"comments","orderBy":[["id","asc"]],"limit":2}
                        }]
                    }}
                ]}]"#,
            ))
            .await
            .unwrap();

        let _start = client.next().await.unwrap().unwrap().into_text().unwrap();
        let part = client.next().await.unwrap().unwrap().into_text().unwrap();
        // Both parents keep their own top-2.
        assert!(
            part.contains("one-keep-a") && part.contains("one-keep-b"),
            "issue 1 top-2: {part}"
        );
        assert!(
            part.contains("two-keep-a") && part.contains("two-keep-b"),
            "issue 2 top-2: {part}"
        );
        // The 3rd comment of each parent is dropped by the per-parent limit.
        assert!(
            !part.contains("one-DROP"),
            "issue 1 over-limit comment dropped: {part}"
        );
        assert!(
            !part.contains("two-DROP"),
            "issue 2 over-limit comment dropped: {part}"
        );
        let _end = client.next().await.unwrap().unwrap().into_text().unwrap();

        server.await.unwrap();
    }

    /// A related subquery's `start` cursor is now honored (previously root-only,
    /// ignored for related): the child read resumes strictly after the boundary
    /// row under the child ordering, so the boundary comment is excluded while
    /// later ones survive. Per-parent batching itself is covered by the related
    /// `limit` test; this proves the cursor reaches the related fetch at all.
    #[tokio::test]
    async fn desired_query_hydration_applies_related_start_cursor() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = StatementRunner::open_in_memory().unwrap();
                    db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
                        .unwrap();
                    db.exec(
                        "CREATE TABLE comments (id INTEGER PRIMARY KEY, issueID INTEGER, body TEXT)",
                    )
                    .unwrap();
                    db.exec("INSERT INTO issue (id, title) VALUES (1, 'the issue')")
                        .unwrap();
                    db.exec(
                        "INSERT INTO comments (id, issueID, body) VALUES \
                         (10, 1, 'comment-boundary'), (11, 1, 'comment-after-a'), \
                         (12, 1, 'comment-after-b')",
                    )
                    .unwrap();
                    let mut handler =
                        DesiredQueriesHandler::new(db, "group1", &format!("client{id}"));
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        // related comments orderBy id ASC, start strictly after {id:10}.
        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"issue-with-comments","ast":{
                        "table":"issue",
                        "related":[{
                            "correlation":{"parentField":["id"],"childField":["issueID"]},
                            "subquery":{"table":"comments","orderBy":[["id","asc"]],
                                "start":{"row":{"id":10},"exclusive":true}}
                        }]
                    }}
                ]}]"#,
            ))
            .await
            .unwrap();

        let _start = client.next().await.unwrap().unwrap().into_text().unwrap();
        let part = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(
            part.contains("comment-after-a"),
            "row after cursor present: {part}"
        );
        assert!(
            part.contains("comment-after-b"),
            "row after cursor present: {part}"
        );
        assert!(
            !part.contains("comment-boundary"),
            "boundary comment excluded by exclusive related start: {part}"
        );
        let _end = client.next().await.unwrap().unwrap().into_text().unwrap();

        server.await.unwrap();
    }

    /// A many-to-many (junction) relationship: `issue -> issueLabel -> label`,
    /// where the `issueLabel` hop is marked `hidden`. zero-cache syncs junction
    /// rows to the client like any related rows — `hidden` is a CLIENT-side
    /// view-shaping concern (`zql/ivm/view-apply-change.ts`), not a server
    /// hydration one (the server `pipeline-driver` has no `hidden` handling).
    /// So the poke must carry the target label rows, reached by the recursive
    /// related path traversing the nested hop off the junction rows.
    #[tokio::test]
    async fn desired_query_hydration_traverses_a_hidden_junction_many_to_many() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = StatementRunner::open_in_memory().unwrap();
                    db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
                        .unwrap();
                    db.exec(
                        "CREATE TABLE issueLabel (id INTEGER PRIMARY KEY, issueID INTEGER, labelID INTEGER)",
                    )
                    .unwrap();
                    db.exec("CREATE TABLE label (id INTEGER PRIMARY KEY, name TEXT)")
                        .unwrap();
                    db.exec("INSERT INTO issue (id, title) VALUES (1, 'the issue')")
                        .unwrap();
                    db.exec(
                        "INSERT INTO issueLabel (id, issueID, labelID) VALUES \
                         (100, 1, 5), (101, 1, 6), (102, 99, 7)",
                    )
                    .unwrap();
                    db.exec(
                        "INSERT INTO label (id, name) VALUES \
                         (5, 'label-bug'), (6, 'label-urgent'), (7, 'label-unrelated')",
                    )
                    .unwrap();
                    let mut handler =
                        DesiredQueriesHandler::new(db, "group1", &format!("client{id}"));
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        // issue -> issueLabel (hidden junction) -> label
        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"issue-with-labels","ast":{
                        "table":"issue",
                        "related":[{
                            "hidden":true,
                            "correlation":{"parentField":["id"],"childField":["issueID"]},
                            "subquery":{"table":"issueLabel","related":[{
                                "correlation":{"parentField":["labelID"],"childField":["id"]},
                                "subquery":{"table":"label"}
                            }]}
                        }]
                    }}
                ]}]"#,
            ))
            .await
            .unwrap();

        let _start = client.next().await.unwrap().unwrap().into_text().unwrap();
        let part = client.next().await.unwrap().unwrap().into_text().unwrap();
        // Target labels reached through the junction are poked.
        assert!(
            part.contains("label-bug"),
            "target label via junction: {part}"
        );
        assert!(
            part.contains("label-urgent"),
            "target label via junction: {part}"
        );
        // A label whose junction row belongs to a different issue is excluded.
        assert!(
            !part.contains("label-unrelated"),
            "label for another issue's junction excluded: {part}"
        );
        let _end = client.next().await.unwrap().unwrap().into_text().unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn desired_query_hydration_fetches_compound_related_rows() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = StatementRunner::open_in_memory().unwrap();
                    db.exec(
                        "CREATE TABLE locale_issue (\
                         id INTEGER PRIMARY KEY, \
                         tenantID INTEGER, \
                         issueID INTEGER, \
                         title TEXT)",
                    )
                    .unwrap();
                    db.exec(
                        "CREATE TABLE locale_comment (\
                         id INTEGER PRIMARY KEY, \
                         tenantID INTEGER, \
                         issueID INTEGER, \
                         body TEXT)",
                    )
                    .unwrap();
                    db.exec(
                        "INSERT INTO locale_issue (id, tenantID, issueID, title) VALUES \
                         (1, 1, 10, 'tenant one issue ten'), \
                         (2, 2, 20, 'tenant two issue twenty')",
                    )
                    .unwrap();
                    db.exec(
                        "INSERT INTO locale_comment (id, tenantID, issueID, body) VALUES \
                         (10, 1, 10, 'compound child one'), \
                         (20, 2, 20, 'compound child two'), \
                         (30, 1, 20, 'cross-product child'), \
                         (40, 2, 10, 'other cross-product child')",
                    )
                    .unwrap();
                    let mut handler =
                        DesiredQueriesHandler::new(db, "group1", &format!("client{id}"));
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"locale-issue-with-comments","ast":{
                        "table":"locale_issue",
                        "related":[{
                            "correlation":{
                                "parentField":["tenantID","issueID"],
                                "childField":["tenantID","issueID"]
                            },
                            "subquery":{"table":"locale_comment"}
                        }]
                    }}
                ]}]"#,
            ))
            .await
            .unwrap();

        let _start = client.next().await.unwrap().unwrap().into_text().unwrap();
        let part = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(part.contains("tenant one issue ten"), "got {part}");
        assert!(part.contains("tenant two issue twenty"), "got {part}");
        assert!(part.contains("compound child one"), "got {part}");
        assert!(part.contains("compound child two"), "got {part}");
        assert!(!part.contains("cross-product child"), "got {part}");
        assert!(!part.contains("other cross-product child"), "got {part}");
        let _end = client.next().await.unwrap().unwrap().into_text().unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn desired_query_hydration_fetches_nested_related_rows() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = StatementRunner::open_in_memory().unwrap();
                    db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
                        .unwrap();
                    db.exec(
                        "CREATE TABLE comments (id INTEGER PRIMARY KEY, issueID INTEGER, body TEXT)",
                    )
                    .unwrap();
                    db.exec(
                        "CREATE TABLE reactions (id INTEGER PRIMARY KEY, commentID INTEGER, emoji TEXT)",
                    )
                    .unwrap();
                    db.exec("INSERT INTO issue (id, title) VALUES (1, 'nested parent')")
                        .unwrap();
                    db.exec(
                        "INSERT INTO comments (id, issueID, body) VALUES \
                         (10, 1, 'nested child'), \
                         (11, 99, 'unrelated child')",
                    )
                    .unwrap();
                    db.exec(
                        "INSERT INTO reactions (id, commentID, emoji) VALUES \
                         (100, 10, 'nested reaction'), \
                         (101, 11, 'reaction on unrelated child'), \
                         (102, 999, 'unrelated reaction')",
                    )
                    .unwrap();
                    let mut handler =
                        DesiredQueriesHandler::new(db, "group1", &format!("client{id}"));
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"issue-with-nested-related","ast":{
                        "table":"issue",
                        "related":[{
                            "correlation":{"parentField":["id"],"childField":["issueID"]},
                            "subquery":{
                                "table":"comments",
                                "related":[{
                                    "correlation":{"parentField":["id"],"childField":["commentID"]},
                                    "subquery":{"table":"reactions"}
                                }]
                            }
                        }]
                    }}
                ]}]"#,
            ))
            .await
            .unwrap();

        let _start = client.next().await.unwrap().unwrap().into_text().unwrap();
        let part = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(part.contains("nested parent"), "got {part}");
        assert!(part.contains("nested child"), "got {part}");
        assert!(part.contains("nested reaction"), "got {part}");
        assert!(!part.contains("unrelated child"), "got {part}");
        assert!(!part.contains("reaction on unrelated child"), "got {part}");
        assert!(!part.contains("unrelated reaction"), "got {part}");
        let _end = client.next().await.unwrap().unwrap().into_text().unwrap();

        server.await.unwrap();
    }

    /// Proves the write path end to end: a real client connects, sends a real
    /// Live: the production write path. A handler configured with
    /// `with_upstream_push` routes a `push`'s CRUD insert to UPSTREAM Postgres
    /// via `apply_crud_mutation`; the row genuinely lands upstream (read back
    /// independently) and the `lastMutationID` advances. Skips without a test
    /// Postgres.
    #[tokio::test]
    async fn upstream_push_applies_mutation_to_postgres() {
        let base = std::env::var("ZERO_TEST_PG")
            .unwrap_or_else(|_| "host=localhost port=54329 user=postgres dbname=postgres".into());
        let Ok(pg) = zero_cache_change_source::pg_connection::connect(&base).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        pg.batch_execute(
            "DROP SCHEMA IF EXISTS pushup CASCADE; CREATE SCHEMA pushup; \
             CREATE TABLE pushup.clients (\"clientGroupID\" TEXT, \"clientID\" TEXT, \
               \"lastMutationID\" BIGINT, PRIMARY KEY(\"clientGroupID\",\"clientID\")); \
             CREATE TABLE pushup.issue (id TEXT PRIMARY KEY, title TEXT);",
        )
        .await
        .unwrap();

        // Connect with search_path set so unqualified op tables resolve to pushup.
        let conn = format!("{base} options='-c search_path=pushup'");
        let mut handler = DesiredQueriesHandler::new(seeded_db(), "group1", "c1")
            .with_upstream_push(conn, "pushup".to_string());

        let outcome = handler
            .on_action_async(ConnectionAction::Push(PushBody {
                client_group_id: "group1".to_string(),
                mutations: vec![Mutation::Crud(zero_cache_protocol::push::CrudMutation {
                    id: 1.0,
                    client_id: "c1".to_string(),
                    timestamp: 1.0,
                    ops_json: zero_cache_shared::bigint_json::parse(
                        r#"[{"op":"insert","tableName":"issue","primaryKey":["id"],"value":{"id":"a","title":"from client push"}}]"#,
                    )
                    .unwrap(),
                })],
                push_version: 1.0,
                schema_version: None,
                timestamp: 1.0,
                request_id: "req1".to_string(),
                traceparent: None,
            }))
            .await;
        assert_eq!(outcome.responses.len(), 1);
        assert!(
            outcome.responses[0].contains("pushResponse"),
            "got {}",
            outcome.responses[0]
        );

        // The row is genuinely upstream, and lastMutationID advanced.
        let row = pg
            .query_one("SELECT title FROM pushup.issue WHERE id = 'a'", &[])
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>(0), "from client push");
        let lmid = pg
            .query_one(
                "SELECT \"lastMutationID\" FROM pushup.clients WHERE \"clientID\" = 'c1'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(lmid.get::<_, i64>(0), 1);

        pg.batch_execute("DROP SCHEMA pushup CASCADE;").await.ok();
    }

    /// `push` message carrying a CRUD insert, and gets back a real
    /// `pushResponse` frame over the real socket — and the row genuinely
    /// landed in the replica (read back via a fresh query, not just trusting
    /// the response).
    #[tokio::test]
    async fn push_message_applies_a_real_crud_mutation_and_responds() {
        // A temp-file-backed (not `:memory:`) replica, so a second connection
        // opened after the push can independently read back what was
        // committed — proving the row landed, not just that the response
        // claimed success.
        let db_path =
            std::env::temp_dir().join(format!("zero_push_test_{}.sqlite3", std::process::id()));
        let _ = std::fs::remove_file(&db_path);
        {
            let setup = StatementRunner::new(rusqlite::Connection::open(&db_path).unwrap());
            setup
                .exec("CREATE TABLE issue (id TEXT PRIMARY KEY, title TEXT)")
                .unwrap();
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let db_path_for_handler = db_path.clone();
        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                move |id| {
                    let db = StatementRunner::new(
                        rusqlite::Connection::open(&db_path_for_handler).unwrap(),
                    );
                    let mut handler =
                        DesiredQueriesHandler::new(db, "group1", &format!("client{id}"));
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        // A push (like any other data message) requires the connection to
        // already be initialized. An empty desired-queries patch produces no
        // response frames, so no extra read is needed here.
        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[]}]"#,
            ))
            .await
            .unwrap();

        // A real push, exactly the wire shape `up_json`/`push_json` decode.
        client
            .send(Message::text(
                r#"["push", {
                    "clientGroupID": "group1", "pushVersion": 1, "timestamp": 1, "requestID": "req1",
                    "mutations": [
                        {"type": "crud", "id": 1, "clientID": "c1", "timestamp": 1,
                         "args": [{"ops": [
                            {"op": "insert", "tableName": "issue", "primaryKey": ["id"],
                             "value": {"id": "1", "title": "from a real push"}}
                         ]}]}
                    ]
                }]"#,
            ))
            .await
            .unwrap();

        let response = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(response.contains("pushResponse"), "got {response}");
        assert!(response.contains("\"clientID\":\"c1\""), "got {response}");
        assert!(
            !response.contains("\"error\""),
            "no error in a clean insert: {response}"
        );

        server.await.unwrap();

        // Read back via a FRESH connection to the same file — proving the row
        // was actually committed to the replica, not just that the response
        // claimed success.
        let verify = StatementRunner::new(rusqlite::Connection::open(&db_path).unwrap());
        let rows = verify
            .query_uncached("SELECT title FROM issue WHERE id = '1'", &[])
            .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "the row committed by the push is visible to a new connection"
        );
        assert_eq!(
            rows[0][0].1,
            zero_cache_sqlite::Value::Text("from a real push".into())
        );
        let _ = std::fs::remove_file(&db_path);
    }

    #[test]
    fn ack_mutation_responses_clears_pending_response_state() {
        let mut handler = DesiredQueriesHandler::new(seeded_db(), "group1", "client1");
        let mutation_id = zero_cache_protocol::mutation_id::MutationId {
            id: 1.0,
            client_id: "c1".to_string(),
        };

        let push_outcome = handler.on_action(ConnectionAction::Push(PushBody {
            client_group_id: "group1".to_string(),
            mutations: vec![Mutation::Crud(zero_cache_protocol::push::CrudMutation {
                id: mutation_id.id,
                client_id: mutation_id.client_id.clone(),
                timestamp: 1.0,
                ops_json: zero_cache_shared::bigint_json::parse(
                    r#"[{"op":"insert","tableName":"issue","primaryKey":["id"],"value":{"id":2,"title":"pending ack"}}]"#,
                )
                .unwrap(),
            })],
            push_version: 1.0,
            schema_version: None,
            timestamp: 1.0,
            request_id: "req1".to_string(),
            traceparent: None,
        }));

        assert_eq!(push_outcome.responses.len(), 1);
        assert_eq!(handler.pending_mutation_response_count(), 1);

        let ack_outcome = handler.on_action(ConnectionAction::AckMutationResponses(
            zero_cache_protocol::push::AckMutationResponsesBody {
                mutation_id: mutation_id.clone(),
            },
        ));

        assert!(ack_outcome.responses.is_empty());
        assert_eq!(handler.pending_mutation_response_count(), 0);
    }

    #[test]
    fn pull_returns_current_cookie_and_last_mutation_id_changes() {
        let mut handler = DesiredQueriesHandler::new(seeded_db(), "group1", "client1");

        let push_outcome = handler.on_action(ConnectionAction::Push(PushBody {
            client_group_id: "group1".to_string(),
            mutations: vec![Mutation::Crud(zero_cache_protocol::push::CrudMutation {
                id: 1.0,
                client_id: "c1".to_string(),
                timestamp: 1.0,
                ops_json: zero_cache_shared::bigint_json::parse(
                    r#"[{"op":"insert","tableName":"issue","primaryKey":["id"],"value":{"id":2,"title":"pull-visible lmid"}}]"#,
                )
                .unwrap(),
            })],
            push_version: 1.0,
            schema_version: None,
            timestamp: 1.0,
            request_id: "push-req1".to_string(),
            traceparent: None,
        }));
        assert_eq!(push_outcome.responses.len(), 1);

        let pull_outcome = handler.on_action(ConnectionAction::Pull(
            zero_cache_protocol::pull::PullRequestBody {
                client_group_id: "group1".to_string(),
                cookie: None,
                request_id: "pull-req1".to_string(),
            },
        ));

        assert_eq!(pull_outcome.responses.len(), 1);
        assert_eq!(
            pull_outcome.responses[0],
            r#"["pull",{"cookie":"00","requestID":"pull-req1","lastMutationIDChanges":{"c1":1}}]"#
        );
    }

    #[test]
    fn update_auth_stores_latest_auth_token() {
        let mut handler = DesiredQueriesHandler::new(seeded_db(), "group1", "client1");

        let outcome = handler.on_action(ConnectionAction::UpdateAuth(
            zero_cache_protocol::update_auth::UpdateAuthBody {
                auth: "new-token".to_string(),
            },
        ));

        assert!(outcome.responses.is_empty());
        assert_eq!(handler.auth_raw(), Some("new-token"));
    }

    #[tokio::test]
    async fn async_http_custom_query_transform_uses_updated_auth() {
        let args = vec![JsonValue::String("wired end to end".to_string())];
        let query_id =
            zero_cache_protocol::query_hash::hash_of_name_and_args("issueByTitle", &args);
        let (url, request_rx) = spawn_transform_response_server_capturing_request(format!(
            r#"{{
                "kind":"QueryResponse",
                "queries":[{{
                    "id":"{query_id}",
                    "name":"issueByTitle",
                    "ast":{{"table":"issue"}}
                }}]
            }}"#
        ))
        .await;
        let mut handler = DesiredQueriesHandler::with_inspect_options(
            seeded_db(),
            "group1",
            "client1",
            51,
            "inspect-live-test".to_string(),
            true,
            None,
        )
        .with_custom_query_transform_http(CustomQueryTransformHttpConfig::new(
            url, "public", "app1",
        ));

        handler.on_action(ConnectionAction::UpdateAuth(
            zero_cache_protocol::update_auth::UpdateAuthBody {
                auth: "fresh-token".to_string(),
            },
        ));
        let outcome = handler
            .on_action_async(ConnectionAction::Inspect(
                zero_cache_protocol::inspect_up::InspectUpBody::AnalyzeQuery {
                    id: "inspect-http-auth".to_string(),
                    value: None,
                    options: None,
                    ast: None,
                    name: Some("issueByTitle".to_string()),
                    args: Some(args),
                },
            ))
            .await;

        assert_eq!(outcome.responses.len(), 1);
        let request = request_rx.await.unwrap();
        assert!(
            request
                .to_ascii_lowercase()
                .contains("authorization: bearer fresh-token"),
            "request should carry refreshed auth header: {request}"
        );
    }

    /// A retried push with a stale mutation id is reported as
    /// `alreadyProcessed`, not silently duplicated or errored oddly.
    #[tokio::test]
    async fn push_replay_of_the_same_mutation_id_is_reported_already_processed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = StatementRunner::open_in_memory().unwrap();
                    db.exec("CREATE TABLE issue (id TEXT PRIMARY KEY, title TEXT)")
                        .unwrap();
                    let mut handler =
                        DesiredQueriesHandler::new(db, "group1", &format!("client{id}"));
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();
        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[]}]"#,
            ))
            .await
            .unwrap();

        let push = r#"["push", {
            "clientGroupID": "group1", "pushVersion": 1, "timestamp": 1, "requestID": "req1",
            "mutations": [
                {"type": "crud", "id": 1, "clientID": "c1", "timestamp": 1,
                 "args": [{"ops": [
                    {"op": "insert", "tableName": "issue", "primaryKey": ["id"],
                     "value": {"id": "1", "title": "first"}}
                 ]}]}
            ]
        }]"#;
        client.send(Message::text(push)).await.unwrap();
        let first = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(!first.contains("\"error\""), "first push succeeds: {first}");

        // Resend the SAME mutation id (a retried push).
        client.send(Message::text(push)).await.unwrap();
        let second = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(second.contains("alreadyProcessed"), "got {second}");

        server.await.unwrap();
    }
}
