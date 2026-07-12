//! Litestream backup/restore integration (`ZERO_LITESTREAM_BACKUP_URL`).
//!
//! Litestream continuously streams a SQLite database's WAL to an object store
//! (e.g. `s3://…`) and can restore it on startup. This module shells out to the
//! `litestream` binary (the same contract as upstream zero, which bundles it):
//!
//! * [`restore`] — on startup, restore the replica from the backup if one
//!   exists and the local file is missing (avoids a cold re-sync).
//! * [`spawn_replicate`] — run `litestream replicate` as a managed child that
//!   continuously backs the replica up; killed on shutdown.
//!
//! (In this port's horizontally-scaled topology, view-syncer nodes bootstrap
//! their replica from the change-streamer snapshot, so litestream's main role
//! here is disaster-recovery backup of the replicator/change-streamer node.)

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;

/// `ZERO_LITESTREAM_LOG_LEVEL`, forwarded to the litestream child process.
static LOG_LEVEL: OnceLock<Option<String>> = OnceLock::new();

/// Sets the log level forwarded to litestream child processes (call once).
pub fn configure(log_level: Option<String>) {
    let _ = LOG_LEVEL.set(log_level);
}

/// Applies `ZERO_LITESTREAM_LOG_LEVEL` to a litestream command's environment.
fn apply_log_level(cmd: &mut Command) {
    if let Some(Some(level)) = LOG_LEVEL.get() {
        cmd.env("LITESTREAM_LOG_LEVEL", level);
    }
}

/// The litestream executable (overridable via `ZERO_LITESTREAM_EXECUTABLE`).
pub fn executable() -> String {
    std::env::var("ZERO_LITESTREAM_EXECUTABLE")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "litestream".to_string())
}

/// The executable used for RESTORE: the v0.5.x binary when
/// `ZERO_LITESTREAM_RESTORE_USING_V5` is set (upstream: litestream v0.5.8+
/// restores both v0.3.x and v0.5.x backup formats), else the standard fork.
pub fn restore_executable() -> String {
    let use_v5 = std::env::var("ZERO_LITESTREAM_RESTORE_USING_V5")
        .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false);
    if use_v5 {
        if let Some(v5) = std::env::var("ZERO_LITESTREAM_EXECUTABLE_V5")
            .ok()
            .filter(|s| !s.is_empty())
        {
            return v5;
        }
    }
    executable()
}

/// Applies the litestream option environment to a child process: all
/// `ZERO_LITESTREAM_*` variables are already inherited from this process's
/// environment (upstream's config.yml consumes them via `${ENV}`
/// substitution); this additionally exports the values zero-cache computes
/// itself — the metrics port default (`--port + 2`) and the replica path —
/// so a config file referencing them works without the operator re-deriving
/// them.
fn apply_litestream_env(cmd: &mut Command, replica_path: &str) {
    apply_log_level(cmd);
    cmd.env("ZERO_REPLICA_FILE", replica_path);
    if std::env::var("ZERO_LITESTREAM_PORT").is_err() {
        let port: u16 = std::env::var("ZERO_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(4848);
        cmd.env("ZERO_LITESTREAM_PORT", (port + 2).to_string());
    }
}

/// The litestream yaml config path (`ZERO_LITESTREAM_CONFIG_PATH`, upstream
/// default `./src/services/litestream/config.yml`). When the file exists,
/// `replicate` runs in upstream's config-file mode; otherwise the explicit
/// argv form is used.
pub fn config_path() -> String {
    std::env::var("ZERO_LITESTREAM_CONFIG_PATH")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "./src/services/litestream/config.yml".to_string())
}

/// Whether the litestream binary is runnable.
pub fn available() -> bool {
    Command::new(executable())
        .arg("version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// The argv for restoring `replica_path` from `backup_url`. Uses
/// `-if-replica-exists` so a missing backup is a no-op (not an error), and
/// `-if-db-not-exists` so an existing local replica isn't clobbered.
pub fn restore_args(replica_path: &str, backup_url: &str) -> Vec<String> {
    vec![
        "restore".into(),
        "-if-replica-exists".into(),
        "-if-db-not-exists".into(),
        "-o".into(),
        replica_path.into(),
        backup_url.into(),
    ]
}

/// The argv for continuously replicating `replica_path` to `backup_url`.
pub fn replicate_args(replica_path: &str, backup_url: &str) -> Vec<String> {
    vec!["replicate".into(), replica_path.into(), backup_url.into()]
}

/// Restores the replica from `backup_url` if the local file is missing and a
/// backup exists. Returns `true` if a replica file is present afterward.
/// Best-effort: logs and returns `false` if litestream isn't available.
pub fn restore(replica_path: &str, backup_url: &str) -> bool {
    if Path::new(replica_path).exists() {
        return true; // already have a local replica
    }
    if !available() {
        crate::warn!("litestream not found; skipping restore");
        return false;
    }
    let mut cmd = Command::new(restore_executable());
    cmd.args(restore_args(replica_path, backup_url));
    apply_litestream_env(&mut cmd, replica_path);
    let status = cmd.status();
    match status {
        Ok(s) if s.success() => Path::new(replica_path).exists(),
        _ => false,
    }
}

/// Spawns `litestream replicate` as a background child that continuously backs
/// up `replica_path` to `backup_url`. Returns the child so the caller can kill
/// it on shutdown. Errors if litestream can't be started.
pub fn spawn_replicate(replica_path: &str, backup_url: &str) -> std::io::Result<Child> {
    let mut cmd = Command::new(executable());
    // Config-file mode (upstream's contract) when the yaml exists: litestream
    // reads everything — backup location, checkpoint thresholds, backup
    // intervals, multipart tuning, metrics port — from the file, with
    // ZERO_LITESTREAM_* / ZERO_* env vars substituted via ${ENV}. Otherwise
    // the explicit argv form replicates path -> URL directly.
    let cfg = config_path();
    if Path::new(&cfg).exists() {
        cmd.args(["replicate", "-config", &cfg]);
        cmd.env("ZERO_LITESTREAM_BACKUP_LOCATION", backup_url);
    } else {
        cmd.args(replicate_args(replica_path, backup_url));
    }
    cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());
    apply_litestream_env(&mut cmd, replica_path);
    cmd.spawn()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restore_argv_is_safe_and_idempotent() {
        let args = restore_args("/data/zero.db", "s3://bucket/zero");
        assert_eq!(args[0], "restore");
        assert!(args.contains(&"-if-replica-exists".to_string()));
        assert!(args.contains(&"-if-db-not-exists".to_string()));
        assert_eq!(args[args.len() - 2], "/data/zero.db");
        assert_eq!(args[args.len() - 1], "s3://bucket/zero");
    }

    #[test]
    fn replicate_argv() {
        assert_eq!(
            replicate_args("/data/zero.db", "s3://b/z"),
            vec!["replicate", "/data/zero.db", "s3://b/z"]
        );
    }

    #[test]
    fn restore_noops_when_local_replica_exists() {
        // An existing local file short-circuits restore (returns true) without
        // needing litestream installed.
        let path = std::env::temp_dir().join(format!("zc_ls_{}.db", std::process::id()));
        std::fs::write(&path, b"x").unwrap();
        assert!(restore(path.to_str().unwrap(), "s3://irrelevant"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn executable_honors_override() {
        std::env::set_var("ZERO_LITESTREAM_EXECUTABLE", "/opt/litestream");
        assert_eq!(executable(), "/opt/litestream");
        std::env::remove_var("ZERO_LITESTREAM_EXECUTABLE");
    }
}
