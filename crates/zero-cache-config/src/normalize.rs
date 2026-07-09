//! Port of `zero-cache/src/config/normalize.ts` — `assertNormalized`/
//! `normalizeZeroConfig`, the config-defaulting business logic. First
//! slice of the previously entirely-unstarted `zero-cache-config` crate
//! (`zero-cache/src/config`, ~3900 LOC incl. tests/snapshots across
//! `zero-config.ts`/`normalize.ts`/`network.ts`/`define-config.ts`).
//!
//! Scope: `zero-config.ts` itself (1266 lines) is a CLI/env-var option
//! DECLARATION file — a config-builder library invocation listing every
//! `--flag`/`ZERO_*` env var zero-cache accepts, not algorithmic logic —
//! and is deliberately not ported wholesale; this port doesn't have (or
//! need) the same CLI-parsing library. `normalize.ts` is different: it's
//! genuine business logic (defaulting rules, cross-field validation) that
//! operates on whatever a `ZeroConfig` value already is, regardless of how
//! it got parsed — so this module ports a trimmed `ZeroConfig` carrying
//! only the fields `normalize.ts` actually reads/writes, plus the real
//! `assert_normalized`/`normalize_zero_config` logic.
//!
//! Determinism convention: `getHostIp()`/`os.availableParallelism()`/
//! `nanoid()`/`process.env.NODE_ENV`/ECS-environment-detection are all
//! ambient OS/environment reads upstream performs inline; this port takes
//! their results as explicit parameters instead (`host_ip`,
//! `available_parallelism`, `generate_task_id`, `is_development_mode`,
//! `is_running_in_ecs`), matching every other "inject what upstream reads
//! ambiently" module in this port. The `env[...] = ...` side effects
//! upstream performs (propagating defaulted values to spawned child-worker
//! environments) are NOT ported — this port has no child-process/env
//! propagation model (see the `worker_message.rs` process-model decision:
//! `tokio::spawn` tasks share a process and its env already).

use thiserror::Error;

/// The subset of `ZeroConfig`'s fields `normalize.ts` reads or writes.
/// Trimmed from the full ~100-field CLI option struct — see module doc.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ZeroConfig {
    pub port: u16,
    pub task_id: Option<String>,
    pub admin_password: Option<String>,
    pub keepalive_timeout_ms: Option<u64>,
    pub num_sync_workers: Option<u32>,
    pub upstream_db: String,
    pub change_db: Option<String>,
    pub cvr_db: Option<String>,
    pub change_streamer_port: Option<u16>,
    pub change_streamer_address: Option<String>,
    pub litestream_port: Option<u16>,
    pub litestream_backup_using_v5: bool,
    pub litestream_restore_using_v5: bool,
    pub litestream_executable: Option<String>,
    pub litestream_executable_v5: Option<String>,
}

/// `ZeroConfig` with every default-eligible field guaranteed present. Port
/// of `NormalizedZeroConfig`.
#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedZeroConfig {
    pub task_id: String,
    pub change_streamer_port: u16,
    pub change_streamer_address: String,
    pub litestream_port: u16,
    pub change_db: String,
    pub cvr_db: String,
    pub num_sync_workers: u32,
}

/// Port of `assertNormalized`'s failure modes.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NormalizeError {
    #[error("missing --task-id")]
    MissingTaskId,
    #[error("missing --change-streamer-port")]
    MissingChangeStreamerPort,
    #[error("missing --change-streamer-address")]
    MissingChangeStreamerAddress,
    #[error("missing --litestream-port")]
    MissingLitestreamPort,
    #[error("--litestream-backup-using-v5 requires --litestream-restore-using-v5")]
    BackupV5RequiresRestoreV5,
    #[error(
        "--litestream-backup-using-v5 requires --litestream-executable to be flipped to the v5 binary (--litestream-executable must equal --litestream-executable-v5)"
    )]
    BackupV5RequiresExecutableV5,
    #[error("missing --change-db")]
    MissingChangeDb,
    #[error("missing --cvr-db")]
    MissingCvrDb,
    #[error("missing --num-sync-workers")]
    MissingNumSyncWorkers,
    #[error("missing --admin-password: required in production mode")]
    MissingAdminPasswordInProduction,
}

/// Port of `assertNormalized`, checked in upstream's exact order (matters
/// for which error a caller sees first when several fields are missing).
pub fn assert_normalized(
    config: &ZeroConfig,
    is_development_mode: bool,
) -> Result<(), NormalizeError> {
    if config.task_id.as_deref().unwrap_or("").is_empty() {
        return Err(NormalizeError::MissingTaskId);
    }
    if config.change_streamer_port.is_none() {
        return Err(NormalizeError::MissingChangeStreamerPort);
    }
    if config
        .change_streamer_address
        .as_deref()
        .unwrap_or("")
        .is_empty()
    {
        return Err(NormalizeError::MissingChangeStreamerAddress);
    }
    if config.litestream_port.is_none() {
        return Err(NormalizeError::MissingLitestreamPort);
    }
    if config.litestream_backup_using_v5 && !config.litestream_restore_using_v5 {
        return Err(NormalizeError::BackupV5RequiresRestoreV5);
    }
    if config.litestream_backup_using_v5
        && (config.litestream_executable_v5.is_none()
            || config.litestream_executable != config.litestream_executable_v5)
    {
        return Err(NormalizeError::BackupV5RequiresExecutableV5);
    }
    if config.change_db.as_deref().unwrap_or("").is_empty() {
        return Err(NormalizeError::MissingChangeDb);
    }
    if config.cvr_db.as_deref().unwrap_or("").is_empty() {
        return Err(NormalizeError::MissingCvrDb);
    }
    if config.num_sync_workers.is_none() {
        return Err(NormalizeError::MissingNumSyncWorkers);
    }
    if !is_development_mode && config.admin_password.as_deref().unwrap_or("").is_empty() {
        return Err(NormalizeError::MissingAdminPasswordInProduction);
    }
    Ok(())
}

const DEFAULT_ECS_KEEPALIVE_TIMEOUT_MS: u64 = 20_000;

/// Port of `normalizeZeroConfig`. Mutates `config` in place (matching
/// upstream) and returns the [`NormalizedZeroConfig`] view. See module doc
/// for the ambient-input-as-parameter convention and the skipped
/// `env[...]` propagation.
pub fn normalize_zero_config(
    config: &mut ZeroConfig,
    is_running_in_ecs: bool,
    host_ip: &str,
    available_parallelism: u32,
    generate_task_id: impl FnOnce() -> String,
) -> NormalizedZeroConfig {
    if config.task_id.as_deref().unwrap_or("").is_empty() {
        config.task_id = Some(generate_task_id());
    }
    if config.change_streamer_port.is_none() {
        config.change_streamer_port = Some(config.port + 1);
    }
    if config.litestream_port.is_none() {
        config.litestream_port = Some(config.port + 2);
    }
    if config.num_sync_workers.is_none() {
        // Reserve 1 core for the replicator. The change-streamer is not CPU heavy.
        config.num_sync_workers = Some(available_parallelism.saturating_sub(1).max(1));
    }

    if config
        .change_streamer_address
        .as_deref()
        .unwrap_or("")
        .is_empty()
    {
        let port = config.change_streamer_port.unwrap();
        config.change_streamer_address = Some(format!("{host_ip}:{port}"));
    }

    if config.change_db.as_deref().unwrap_or("").is_empty() {
        config.change_db = Some(config.upstream_db.clone());
    }

    if config.cvr_db.as_deref().unwrap_or("").is_empty() {
        config.cvr_db = Some(config.upstream_db.clone());
    }

    if config.keepalive_timeout_ms.is_none() && is_running_in_ecs {
        config.keepalive_timeout_ms = Some(DEFAULT_ECS_KEEPALIVE_TIMEOUT_MS);
    }

    NormalizedZeroConfig {
        task_id: config.task_id.clone().unwrap(),
        change_streamer_port: config.change_streamer_port.unwrap(),
        change_streamer_address: config.change_streamer_address.clone().unwrap(),
        litestream_port: config.litestream_port.unwrap(),
        change_db: config.change_db.clone().unwrap(),
        cvr_db: config.cvr_db.clone().unwrap(),
        num_sync_workers: config.num_sync_workers.unwrap(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_config() -> ZeroConfig {
        ZeroConfig {
            port: 4848,
            upstream_db: "postgres://u".into(),
            ..Default::default()
        }
    }

    fn normalized_config() -> ZeroConfig {
        let mut c = minimal_config();
        normalize_zero_config(&mut c, false, "10.0.0.1", 4, || "task-1".to_string());
        c
    }

    #[test]
    fn normalize_fills_task_id_when_missing() {
        let mut c = minimal_config();
        let n = normalize_zero_config(&mut c, false, "10.0.0.1", 4, || "generated-id".to_string());
        assert_eq!(n.task_id, "generated-id");
        assert_eq!(c.task_id, Some("generated-id".to_string()));
    }

    #[test]
    fn normalize_preserves_explicit_task_id() {
        let mut c = minimal_config();
        c.task_id = Some("explicit".into());
        let n = normalize_zero_config(&mut c, false, "10.0.0.1", 4, || "generated-id".to_string());
        assert_eq!(n.task_id, "explicit");
    }

    #[test]
    fn normalize_derives_change_streamer_port_from_base_port() {
        let mut c = minimal_config();
        let n = normalize_zero_config(&mut c, false, "10.0.0.1", 4, || "t".to_string());
        assert_eq!(n.change_streamer_port, 4849);
    }

    #[test]
    fn normalize_derives_litestream_port_from_base_port() {
        let mut c = minimal_config();
        let n = normalize_zero_config(&mut c, false, "10.0.0.1", 4, || "t".to_string());
        assert_eq!(n.litestream_port, 4850);
    }

    #[test]
    fn normalize_derives_change_streamer_address_from_host_ip_and_port() {
        let mut c = minimal_config();
        let n = normalize_zero_config(&mut c, false, "10.0.0.1", 4, || "t".to_string());
        assert_eq!(n.change_streamer_address, "10.0.0.1:4849");
    }

    #[test]
    fn normalize_reserves_one_core_for_the_replicator() {
        let mut c = minimal_config();
        let n = normalize_zero_config(&mut c, false, "10.0.0.1", 8, || "t".to_string());
        assert_eq!(n.num_sync_workers, 7);
    }

    #[test]
    fn normalize_num_sync_workers_never_goes_below_one() {
        let mut c = minimal_config();
        let n = normalize_zero_config(&mut c, false, "10.0.0.1", 1, || "t".to_string());
        assert_eq!(n.num_sync_workers, 1);
        let mut c2 = minimal_config();
        let n2 = normalize_zero_config(&mut c2, false, "10.0.0.1", 0, || "t".to_string());
        assert_eq!(n2.num_sync_workers, 1);
    }

    #[test]
    fn normalize_defaults_change_db_and_cvr_db_to_upstream_db() {
        let mut c = minimal_config();
        let n = normalize_zero_config(&mut c, false, "10.0.0.1", 4, || "t".to_string());
        assert_eq!(n.change_db, "postgres://u");
        assert_eq!(n.cvr_db, "postgres://u");
    }

    #[test]
    fn normalize_sets_ecs_keepalive_default_only_when_running_in_ecs() {
        let mut c = minimal_config();
        normalize_zero_config(&mut c, false, "10.0.0.1", 4, || "t".to_string());
        assert_eq!(c.keepalive_timeout_ms, None);

        let mut c2 = minimal_config();
        normalize_zero_config(&mut c2, true, "10.0.0.1", 4, || "t".to_string());
        assert_eq!(c2.keepalive_timeout_ms, Some(20_000));
    }

    #[test]
    fn normalize_does_not_override_explicit_keepalive_timeout_in_ecs() {
        let mut c = minimal_config();
        c.keepalive_timeout_ms = Some(5000);
        normalize_zero_config(&mut c, true, "10.0.0.1", 4, || "t".to_string());
        assert_eq!(c.keepalive_timeout_ms, Some(5000));
    }

    #[test]
    fn assert_normalized_passes_for_a_fully_normalized_config() {
        let mut c = normalized_config();
        c.num_sync_workers = Some(4);
        c.admin_password = Some("secret".into());
        assert_normalized(&c, false).unwrap();
    }

    #[test]
    fn assert_normalized_catches_missing_task_id() {
        let mut c = normalized_config();
        c.task_id = None;
        assert_eq!(
            assert_normalized(&c, true),
            Err(NormalizeError::MissingTaskId)
        );
    }

    #[test]
    fn assert_normalized_admin_password_required_only_outside_development() {
        let c = normalized_config();
        assert_eq!(
            assert_normalized(&c, false),
            Err(NormalizeError::MissingAdminPasswordInProduction)
        );
        assert!(
            assert_normalized(&c, true).is_ok(),
            "development mode should not require an admin password"
        );
    }

    #[test]
    fn assert_normalized_backup_v5_requires_restore_v5() {
        let mut c = normalized_config();
        c.admin_password = Some("secret".into());
        c.litestream_backup_using_v5 = true;
        assert_eq!(
            assert_normalized(&c, true),
            Err(NormalizeError::BackupV5RequiresRestoreV5)
        );
    }

    #[test]
    fn assert_normalized_backup_v5_requires_matching_executable() {
        let mut c = normalized_config();
        c.admin_password = Some("secret".into());
        c.litestream_backup_using_v5 = true;
        c.litestream_restore_using_v5 = true;
        c.litestream_executable = Some("litestream".into());
        c.litestream_executable_v5 = Some("litestream-v5".into());
        assert_eq!(
            assert_normalized(&c, true),
            Err(NormalizeError::BackupV5RequiresExecutableV5)
        );

        c.litestream_executable = Some("litestream-v5".into());
        assert!(assert_normalized(&c, true).is_ok());
    }
}
