//! Provisioning the separate CVR database (`ZERO_CVR_DB`).
//!
//! Upstream zero-cache stores Client View Records in a Postgres database
//! (`ZERO_CVR_DB`, defaulting to `ZERO_UPSTREAM_DB`) so that multiple
//! view-syncer nodes can share per-client-group query state. This port's
//! horizontal scaling instead streams commits from a single change-streamer to
//! view-syncers that keep CVR state in their local replica — a different
//! topology, functionally equivalent from an app's perspective.
//!
//! To honor `ZERO_CVR_DB` when it is set (rather than silently ignoring it),
//! this module connects to the configured CVR database on startup and
//! provisions the CVR schema there using the fully-ported
//! [`zero_cache_view_syncer::cvr_schema_sql`] DDL. Provisioning is made
//! idempotent by first checking whether the schema is already populated (the
//! DDL itself matches upstream and assumes a fresh schema), so it is safe to
//! run on every startup. This validates connectivity to the configured
//! database and makes the Postgres CVR store (`cvr_store_pg`, already
//! implemented + live-tested) ready for an upstream-compatible shared-CVR
//! deployment.

use zero_cache_types::shards::{cvr_schema, ShardId};
use zero_cache_view_syncer::cvr_schema_sql::create_cvr_schema_statements;

/// Errors provisioning the CVR schema.
#[derive(Debug, thiserror::Error)]
pub enum ProvisionError {
    #[error("could not connect to CVR database: {0}")]
    Connect(String),
    #[error(transparent)]
    Shard(#[from] zero_cache_types::shards::ShardError),
    #[error("CVR schema statement failed: {0}")]
    Statement(String),
}

/// Formats an error plus its full `.source()` chain, so a `tokio_postgres`
/// error surfaces the real server message (SQLSTATE + detail) rather than its
/// useless top-level "db error" Display.
fn error_chain(e: &dyn std::error::Error) -> String {
    let mut msg = e.to_string();
    let mut src = e.source();
    while let Some(s) = src {
        msg.push_str(&format!(": {s}"));
        src = s.source();
    }
    msg
}

/// Connects to `conn_str` and provisions the CVR schema for `shard`.
/// Idempotent: if the schema is already populated it is a no-op, so this is
/// safe to run on every startup. Returns `true` if it created the schema,
/// `false` if it was already present.
pub async fn provision_cvr_schema(conn_str: &str, shard: &ShardId) -> Result<bool, ProvisionError> {
    let client = zero_cache_change_source::pg_connection::connect(conn_str)
        .await
        .map_err(|e| ProvisionError::Connect(error_chain(&e)))?;
    // Already provisioned? The DDL is not itself idempotent (matches upstream,
    // assumes a fresh schema), so gate on the `instances` table's presence.
    let schema = cvr_schema(shard)?;
    let existing = client
        .query(
            "SELECT 1 FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name = 'instances'",
            &[&schema],
        )
        .await
        .map_err(|e| ProvisionError::Statement(format!("existence check: {e}")))?;
    if !existing.is_empty() {
        return Ok(false);
    }
    for stmt in create_cvr_schema_statements(shard)? {
        client
            .batch_execute(&stmt)
            .await
            .map_err(|e| ProvisionError::Statement(format!("{stmt}: {e}")))?;
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard() -> ShardId {
        ShardId {
            app_id: "provtest".into(),
            shard_num: 0,
        }
    }

    /// Live: provisioning against a real Postgres creates the CVR tables in the
    /// configured database, and is idempotent (running twice is a no-op).
    /// Skips gracefully without a local test Postgres.
    #[tokio::test]
    async fn provisions_cvr_schema_in_the_configured_db_idempotently() {
        let conn_str = std::env::var("ZERO_TEST_PG_URL").unwrap_or_else(|_| {
            "host=/tmp/zc-pg-sock port=54329 user=postgres dbname=postgres".to_string()
        });
        let Ok(client) =
            zero_cache_change_source::pg_connection::connect(&conn_str).await
        else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        client
            .batch_execute("DROP SCHEMA IF EXISTS \"provtest_0/cvr\" CASCADE;")
            .await
            .unwrap();

        // First provision creates the schema; the second is a no-op.
        assert!(provision_cvr_schema(&conn_str, &shard()).await.unwrap());
        assert!(!provision_cvr_schema(&conn_str, &shard()).await.unwrap());

        let rows = client
            .query(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema = $1",
                &[&"provtest_0/cvr"],
            )
            .await
            .unwrap();
        let count: i64 = rows[0].get(0);
        assert!(count >= 5, "expected CVR tables to exist, got {count}");
    }

    #[tokio::test]
    async fn bad_connection_string_is_a_connect_error() {
        let err = provision_cvr_schema("host=127.0.0.1 port=1 dbname=nope connect_timeout=1", &shard())
            .await
            .unwrap_err();
        assert!(matches!(err, ProvisionError::Connect(_)));
    }
}
