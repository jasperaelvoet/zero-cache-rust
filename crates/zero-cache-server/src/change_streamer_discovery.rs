//! Change-streamer discovery (`ZERO_CHANGE_STREAMER_MODE=discover`).
//!
//! Port of upstream's "ChangeDB as DNS": the owning change-streamer registers
//! its externally-reachable address in the change database's cdc schema
//! (`"{app}_{shard}/cdc"."replicationState"` — `owner` = task ID,
//! `ownerAddress` = `host:port`, bare for `ws`, scheme-prefixed for `wss`),
//! and a view-syncer in discover mode reads it back to build the
//! change-streamer URI. `ZERO_CHANGE_STREAMER_URI` always wins over
//! discovery, matching upstream.
//!
//! Address selection ports upstream `getHostIp`: enumerate interfaces, drop
//! nothing but rank — reserved/link-local last, non-loopback first, IPv4 over
//! IPv6, then interface-name prefix rank from
//! `ZERO_CHANGE_STREAMER_DISCOVERY_INTERFACE_PREFERENCES` (default
//! `["eth", "en"]`), then lexicographic. IPv6 winners are bracketed for URLs.

use std::net::IpAddr;

use zero_cache_change_source::pg_connection;
use zero_cache_types::shards::ShardId;

/// The CDC schema name: `{appID}_{shardNum}/cdc` (upstream `cdcSchema`).
pub fn cdc_schema(shard: &ShardId) -> String {
    format!("{}_{}/cdc", shard.app_id, shard.shard_num)
}

/// Upstream's `replicationState` table (subset relevant to discovery; the
/// single-row lock shape matches upstream's DDL so an official server can
/// share the table).
fn create_replication_state_sql(schema: &str) -> Vec<String> {
    vec![
        format!(r#"CREATE SCHEMA IF NOT EXISTS "{schema}""#),
        format!(
            r#"CREATE TABLE IF NOT EXISTS "{schema}"."replicationState" (
                "lastWatermark" TEXT NOT NULL,
                "owner" TEXT,
                "ownerAddress" TEXT,
                "lock" INTEGER PRIMARY KEY DEFAULT 1 CHECK ("lock" = 1)
            )"#
        ),
        format!(
            r#"INSERT INTO "{schema}"."replicationState" ("lastWatermark")
               SELECT '00' WHERE NOT EXISTS (SELECT 1 FROM "{schema}"."replicationState")"#
        ),
    ]
}

/// The registered `ownerAddress` string: bare `host:port` for `ws` (backward
/// compat with old view-syncers, as upstream), `wss://host:port` otherwise.
pub fn address_with_protocol(protocol: &str, host_port: &str) -> String {
    if protocol == "ws" {
        host_port.to_string()
    } else {
        format!("{protocol}://{host_port}")
    }
}

/// Builds the subscription URL from a discovered address (upstream:
/// `address.includes('://') ? address : 'ws://' + address`, plus a trailing
/// slash).
pub fn discovered_url(address: &str) -> String {
    if address.contains("://") {
        format!("{address}/")
    } else {
        format!("ws://{address}/")
    }
}

/// Registers this node as the change-streamer owner. Creates the cdc schema /
/// table when absent (a port-provisioned change DB has none).
pub async fn register_owner(
    change_db: &str,
    shard: &ShardId,
    task_id: &str,
    owner_address: &str,
) -> Result<(), pg_connection::PgError> {
    let client = pg_connection::connect(change_db).await?;
    let schema = cdc_schema(shard);
    for sql in create_replication_state_sql(&schema) {
        client.batch_execute(&sql).await?;
    }
    client
        .execute(
            &format!(
                r#"UPDATE "{schema}"."replicationState"
                   SET "owner" = $1, "ownerAddress" = $2"#
            ),
            &[&task_id, &owner_address],
        )
        .await?;
    Ok(())
}

/// Reads the registered change-streamer address ("ChangeDB as DNS"), on a
/// dedicated short-lived connection as upstream. `None` when unregistered.
pub async fn discover_address(
    change_db: &str,
    shard: &ShardId,
) -> Result<Option<String>, pg_connection::PgError> {
    let client = pg_connection::connect(change_db).await?;
    let schema = cdc_schema(shard);
    let rows = client
        .query(
            &format!(r#"SELECT "ownerAddress" FROM "{schema}"."replicationState""#),
            &[],
        )
        .await;
    match rows {
        Ok(rows) => Ok(rows.first().and_then(|r| r.get::<_, Option<String>>(0))),
        // Missing schema/table = nothing registered yet.
        Err(_) => Ok(None),
    }
}

/// Ranks one interface candidate per upstream `getHostIp`'s sort. Lower keys
/// sort first.
fn rank(name: &str, ip: &IpAddr, prefs: &[String]) -> (u8, u8, u8, usize, String) {
    let reserved = match ip {
        IpAddr::V4(v4) => v4.is_link_local() || v4.is_unspecified() || v4.is_broadcast(),
        IpAddr::V6(v6) => {
            // Upstream de-prioritizes private/reserved IPv6 (link-local
            // fe80::/10, unique-local fc00::/7).
            let seg = v6.segments()[0];
            (seg & 0xffc0) == 0xfe80 || (seg & 0xfe00) == 0xfc00 || v6.is_unspecified()
        }
    };
    let loopback = ip.is_loopback();
    let v6 = matches!(ip, IpAddr::V6(_));
    let pref_rank = prefs
        .iter()
        .position(|p| name.starts_with(p.as_str()))
        .unwrap_or(prefs.len());
    (
        u8::from(reserved),
        u8::from(loopback),
        u8::from(v6),
        pref_rank,
        ip.to_string(),
    )
}

/// Picks the externally-reachable host IP for discovery registration,
/// formatted for URL use (IPv6 bracketed). `None` when no interfaces exist.
pub fn pick_host_ip(prefs: &[String]) -> Option<String> {
    let mut candidates: Vec<(String, IpAddr)> = if_addrs::get_if_addrs()
        .ok()?
        .into_iter()
        .map(|i| (i.name.clone(), i.addr.ip()))
        .collect();
    candidates.sort_by_key(|(name, ip)| rank(name, ip, prefs));
    candidates.first().map(|(_, ip)| match ip {
        IpAddr::V6(v6) => format!("[{v6}]"),
        IpAddr::V4(v4) => v4.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard() -> ShardId {
        ShardId {
            app_id: "zero".into(),
            shard_num: 0,
        }
    }

    #[test]
    fn cdc_schema_matches_upstream_naming() {
        assert_eq!(cdc_schema(&shard()), "zero_0/cdc");
    }

    #[test]
    fn ws_addresses_are_registered_bare_and_wss_with_scheme() {
        assert_eq!(
            address_with_protocol("ws", "10.0.0.5:4849"),
            "10.0.0.5:4849"
        );
        assert_eq!(
            address_with_protocol("wss", "10.0.0.5:4849"),
            "wss://10.0.0.5:4849"
        );
    }

    #[test]
    fn discovered_urls_default_to_ws() {
        assert_eq!(discovered_url("10.0.0.5:4849"), "ws://10.0.0.5:4849/");
        assert_eq!(
            discovered_url("wss://10.0.0.5:4849"),
            "wss://10.0.0.5:4849/"
        );
    }

    #[test]
    fn interface_ranking_matches_upstream_sort() {
        let prefs = vec!["eth".to_string(), "en".to_string()];
        let public_v4: IpAddr = "10.1.2.3".parse().unwrap();
        let loopback: IpAddr = "127.0.0.1".parse().unwrap();
        let link_local_v6: IpAddr = "fe80::1".parse().unwrap();
        let global_v6: IpAddr = "2001:db8::1".parse().unwrap();

        // Non-loopback IPv4 on a preferred interface wins over everything.
        let mut c = [
            ("lo0".to_string(), loopback),
            ("utun3".to_string(), public_v4), // VPN name: worst prefix rank
            ("en0".to_string(), public_v4),
            ("en0".to_string(), global_v6),
            ("en0".to_string(), link_local_v6),
        ];
        c.sort_by_key(|(n, ip)| rank(n, ip, &prefs));
        assert_eq!(c[0].0, "en0");
        assert_eq!(c[0].1, public_v4);
        // Reserved (link-local) sorts last.
        assert_eq!(c.last().unwrap().1, link_local_v6);
    }

    /// Live: register + discover round-trip against the change DB. Skips
    /// without a test Postgres.
    #[tokio::test]
    async fn live_register_and_discover_round_trip() {
        let conn_str = std::env::var("ZERO_TEST_PG_URL")
            .unwrap_or_else(|_| "host=localhost port=54329 user=postgres dbname=postgres".into());
        if pg_connection::connect(&conn_str).await.is_err() {
            eprintln!("skipping: no local test Postgres available");
            return;
        }
        let shard = ShardId {
            app_id: format!("disc_test_{}", std::process::id()),
            shard_num: 0,
        };
        assert_eq!(
            discover_address(&conn_str, &shard).await.unwrap(),
            None,
            "nothing registered yet"
        );
        register_owner(&conn_str, &shard, "task-a", "10.0.0.5:4849")
            .await
            .unwrap();
        assert_eq!(
            discover_address(&conn_str, &shard)
                .await
                .unwrap()
                .as_deref(),
            Some("10.0.0.5:4849")
        );
        // Re-registration (takeover) overwrites.
        register_owner(&conn_str, &shard, "task-b", "wss://10.0.0.9:4849")
            .await
            .unwrap();
        assert_eq!(
            discover_address(&conn_str, &shard)
                .await
                .unwrap()
                .as_deref(),
            Some("wss://10.0.0.9:4849")
        );
        let client = pg_connection::connect(&conn_str).await.unwrap();
        client
            .batch_execute(&format!("DROP SCHEMA \"{}\" CASCADE", cdc_schema(&shard)))
            .await
            .ok();
    }
}
