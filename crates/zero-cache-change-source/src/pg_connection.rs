//! A real `tokio-postgres` connection wrapper plus the upstream Postgres
//! catalog queries `change-source.ts`'s `checkAndUpdateUpstream` performs
//! before starting replication: publication existence, replication-slot
//! status, and `wal_level`.
//!
//! Rationale for depending on `tokio-postgres` rather than hand-rolling the
//! wire protocol: PostgreSQL's frontend/backend protocol (including the
//! `COPY ... START_REPLICATION` logical-replication sub-protocol) is a large,
//! well-specified binary protocol that `tokio-postgres` already implements
//! correctly; reimplementing it from scratch would be substantial, low-value
//! duplication for a project whose distinguishing logic is everything *around*
//! the protocol (schema mapping, CVR/IVM, change application), not the wire
//! format itself. This crate ports zero-cache's *usage* of Postgres, not
//! Postgres's wire protocol.
//!
//! These are the first genuinely-network-tested functions in the port,
//! exercised in this session against a real local Postgres 17 instance
//! (verified manually, not just type-checked).

use tokio_postgres::{Client, NoTls};

/// Errors connecting to or querying upstream Postgres.
#[derive(Debug, thiserror::Error)]
pub enum PgError {
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
}

/// Connects to Postgres at `conn_str`, spawning the connection driver task in
/// the background (mirroring how `postgres.js`/`tokio-postgres` split the
/// query interface from the connection I/O loop). Returns the query client.
pub async fn connect(conn_str: &str) -> Result<Client, PgError> {
    let (client, connection) = tokio_postgres::connect(conn_str, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("postgres connection error: {e}");
        }
    });
    Ok(client)
}

/// Returns Postgres's configured `wal_level` (`replica`, `logical`, etc.).
/// Logical replication requires `wal_level = logical`; this is the first
/// precondition zero-cache's change-source checks. Port of the `wal_level`
/// portion of upstream's startup checks (`SHOW wal_level`).
pub async fn wal_level(client: &Client) -> Result<String, PgError> {
    let row = client.query_one("SHOW wal_level", &[]).await?;
    Ok(row.get::<_, String>(0))
}

/// `server_version_num` for Postgres 15 / 17. Port of `types/pg-versions.ts`.
pub const PG_15: i64 = 150000;
pub const PG_17: i64 = 170000;

/// Why an upstream Postgres is unusable for logical replication. Port of the
/// two error conditions in `initial-sync.ts`'s `checkUpstreamConfig`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UpstreamConfigError {
    #[error("Postgres must be configured with \"wal_level = logical\" (currently: \"{0}\")")]
    WalLevelNotLogical(String),
    #[error("Must be running Postgres 15 or higher (currently: \"{0}\")")]
    VersionTooOld(i64),
}

/// Pure validation half of `checkUpstreamConfig`: logical replication requires
/// `wal_level = logical` and server version >= PG 15. Returns the version on
/// success (matching upstream, which returns it for the failover/`PG_17` check).
pub fn validate_upstream_config(wal_level: &str, version: i64) -> Result<i64, UpstreamConfigError> {
    if wal_level != "logical" {
        return Err(UpstreamConfigError::WalLevelNotLogical(
            wal_level.to_string(),
        ));
    }
    if version < PG_15 {
        return Err(UpstreamConfigError::VersionTooOld(version));
    }
    Ok(version)
}

/// Errors from [`check_upstream_config`]: either the connection/query failed or
/// the upstream is misconfigured.
#[derive(Debug, thiserror::Error)]
pub enum CheckUpstreamError {
    #[error(transparent)]
    Pg(#[from] PgError),
    #[error(transparent)]
    Config(#[from] UpstreamConfigError),
}

/// Live port of `checkUpstreamConfig`: reads `wal_level` and
/// `server_version_num` in one round trip and validates them via
/// [`validate_upstream_config`]. Returns the server version number.
pub async fn check_upstream_config(client: &Client) -> Result<i64, CheckUpstreamError> {
    let row = client
        .query_one(
            "SELECT current_setting('wal_level') AS \"walLevel\", \
             current_setting('server_version_num')::int8 AS \"version\"",
            &[],
        )
        .await
        .map_err(PgError::from)?;
    let wal_level: String = row.get("walLevel");
    let version: i64 = row.get("version");
    Ok(validate_upstream_config(&wal_level, version)?)
}

/// Returns the subset of `names` that exist as publications in
/// `pg_publication`. Port of the `SELECT pubname FROM pg_publication WHERE
/// pubname IN (...)` existence check in `checkAndUpdateUpstream`.
pub async fn existing_publications(
    client: &Client,
    names: &[String],
) -> Result<Vec<String>, PgError> {
    if names.is_empty() {
        return Ok(vec![]);
    }
    let rows = client
        .query(
            "SELECT pubname FROM pg_publication WHERE pubname = ANY($1)",
            &[&names],
        )
        .await?;
    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

/// The status of a replication slot, as queried from `pg_replication_slots`.
/// Port of the inline `{restartLSN, walStatus}` shape in
/// `checkAndUpdateUpstream`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotStatus {
    pub restart_lsn: Option<String>,
    pub wal_status: Option<String>,
}

/// Looks up a replication slot's status by name, or `None` if it doesn't
/// exist. Port of the `SELECT restart_lsn, wal_status FROM
/// pg_replication_slots WHERE slot_name = ...` query.
pub async fn slot_status(client: &Client, slot_name: &str) -> Result<Option<SlotStatus>, PgError> {
    let rows = client
        .query(
            "SELECT restart_lsn::text as restart_lsn, wal_status FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await?;
    Ok(rows.first().map(|r| SlotStatus {
        restart_lsn: r.get::<_, Option<String>>(0),
        wal_status: r.get::<_, Option<String>>(1),
    }))
}

/// Whether a replication slot is usable: it must exist, have a non-null
/// `restart_lsn`, and not have `wal_status = 'lost'` (exceeding
/// `max_slot_wal_keep_size`). Port of the validation logic following the slot
/// status query in `checkAndUpdateUpstream` (the `AutoResetSignal` conditions).
pub fn slot_is_usable(status: &Option<SlotStatus>) -> bool {
    match status {
        None => false,
        Some(s) => s.restart_lsn.is_some() && s.wal_status.as_deref() != Some("lost"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Connection string for the throwaway local Postgres instance started for
    /// this session (see `PORTING.md` / memory for how it was provisioned).
    /// These tests are skipped gracefully if no server is listening — CI/other
    /// environments without a local Postgres won't fail here.
    fn test_conn_str() -> String {
        std::env::var("ZERO_TEST_PG_URL").unwrap_or_else(|_| {
            "host=/tmp/zc-pg-sock port=54329 user=postgres dbname=postgres".to_string()
        })
    }

    async fn try_connect() -> Option<Client> {
        connect(&test_conn_str()).await.ok()
    }

    #[tokio::test]
    async fn connects_and_reads_wal_level() {
        let Some(client) = try_connect().await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        let level = wal_level(&client).await.unwrap();
        // The test instance was provisioned with wal_level=logical.
        assert_eq!(level, "logical");
    }

    #[test]
    fn validate_upstream_config_enforces_wal_level_and_version() {
        // Happy path returns the version.
        assert_eq!(validate_upstream_config("logical", 170000), Ok(170000));
        assert_eq!(validate_upstream_config("logical", PG_15), Ok(PG_15));
        // Wrong wal_level.
        assert_eq!(
            validate_upstream_config("replica", 170000),
            Err(UpstreamConfigError::WalLevelNotLogical("replica".into()))
        );
        // wal_level is checked before version.
        assert_eq!(
            validate_upstream_config("replica", 140000),
            Err(UpstreamConfigError::WalLevelNotLogical("replica".into()))
        );
        // Too-old version.
        assert_eq!(
            validate_upstream_config("logical", 149999),
            Err(UpstreamConfigError::VersionTooOld(149999))
        );
    }

    #[tokio::test]
    async fn check_upstream_config_passes_on_the_provisioned_instance() {
        let Some(client) = try_connect().await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        // The test instance is wal_level=logical and PG >= 15.
        let version = check_upstream_config(&client).await.unwrap();
        assert!(
            version >= PG_15,
            "server version {version} should be >= PG_15"
        );
    }

    #[tokio::test]
    async fn existing_publications_filters_to_real_ones() {
        let Some(client) = try_connect().await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        client
            .batch_execute("DROP PUBLICATION IF EXISTS zero_test_pub;")
            .await
            .unwrap();
        client
            .batch_execute("CREATE TABLE IF NOT EXISTS zero_test_table(id int primary key);")
            .await
            .unwrap();
        client
            .batch_execute("CREATE PUBLICATION zero_test_pub FOR TABLE zero_test_table;")
            .await
            .unwrap();

        let found = existing_publications(
            &client,
            &["zero_test_pub".to_string(), "nonexistent_pub".to_string()],
        )
        .await
        .unwrap();
        assert_eq!(found, vec!["zero_test_pub".to_string()]);

        client
            .batch_execute("DROP PUBLICATION zero_test_pub; DROP TABLE zero_test_table;")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn slot_status_and_usability_for_real_slot() {
        let Some(client) = try_connect().await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        client
            .batch_execute("SELECT pg_drop_replication_slot('zero_test_slot') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'zero_test_slot');")
            .await
            .ok(); // best-effort cleanup from a prior run
        client
            .query(
                "SELECT * FROM pg_create_logical_replication_slot('zero_test_slot', 'pgoutput')",
                &[],
            )
            .await
            .unwrap();

        let status = slot_status(&client, "zero_test_slot").await.unwrap();
        assert!(status.is_some());
        assert!(slot_is_usable(&status));

        let missing = slot_status(&client, "does_not_exist").await.unwrap();
        assert!(missing.is_none());
        assert!(!slot_is_usable(&missing));

        client
            .query("SELECT pg_drop_replication_slot('zero_test_slot')", &[])
            .await
            .unwrap();
    }

    #[test]
    fn slot_is_usable_logic() {
        assert!(!slot_is_usable(&None));
        assert!(!slot_is_usable(&Some(SlotStatus {
            restart_lsn: None,
            wal_status: Some("reserved".into())
        })));
        assert!(!slot_is_usable(&Some(SlotStatus {
            restart_lsn: Some("0/1".into()),
            wal_status: Some("lost".into())
        })));
        assert!(slot_is_usable(&Some(SlotStatus {
            restart_lsn: Some("0/1".into()),
            wal_status: Some("reserved".into())
        })));
    }
}
