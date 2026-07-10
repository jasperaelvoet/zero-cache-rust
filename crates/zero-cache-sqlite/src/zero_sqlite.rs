//! Runtime verification for the native SQLite engine required by Zero v1.7.
//!
//! `rusqlite` is only the Rust API. The workspace patch for `libsqlite3-sys`
//! supplies the amalgamation from `@rocicorp/zero-sqlite3@1.1.2`; these checks
//! prevent an accidental system/vanilla SQLite link from starting a server
//! with subtly incompatible snapshot or query semantics.

use std::collections::BTreeSet;

use rusqlite::{functions::FunctionFlags, types::ValueRef, Connection};

use crate::DbError;

pub const ZERO_SQLITE_VERSION: &str = "3.51.0";

const REQUIRED_OPTIONS: &[&str] = &[
    "DEFAULT_CACHE_SIZE=-16000",
    "DEFAULT_FOREIGN_KEYS",
    "DEFAULT_MEMSTATUS=0",
    "DEFAULT_WAL_SYNCHRONOUS=1",
    "DQS=0",
    "ENABLE_COLUMN_METADATA",
    "ENABLE_DBSTAT_VTAB",
    "ENABLE_FTS3",
    "ENABLE_FTS3_PARENTHESIS",
    "ENABLE_FTS4",
    "ENABLE_FTS5",
    "ENABLE_GEOPOLY",
    "ENABLE_MATH_FUNCTIONS",
    "ENABLE_PERCENTILE",
    "ENABLE_RTREE",
    "ENABLE_STAT4",
    "ENABLE_STMT_SCANSTATUS",
    "ENABLE_UPDATE_DELETE_LIMIT",
    "LIKE_DOESNT_MATCH_BLOBS",
    "OMIT_DEPRECATED",
    "OMIT_PROGRESS_CALLBACK",
    "OMIT_SHARED_CACHE",
    "OMIT_TCL_VARIABLE",
    "SOUNDEX",
    "STAT4_SAMPLES=128",
    "THREADSAFE=2",
    "USE_URI",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineInfo {
    pub version: String,
    pub compile_options: BTreeSet<String>,
}

pub fn engine_info(conn: &Connection) -> Result<EngineInfo, DbError> {
    let version = conn.query_row("SELECT sqlite_version()", [], |row| row.get(0))?;
    let mut statement = conn.prepare("PRAGMA compile_options")?;
    let compile_options = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<BTreeSet<_>, _>>()?;
    Ok(EngineInfo {
        version,
        compile_options,
    })
}

/// Installs Zero's connection-local Unicode-aware `lower()` and `upper()`
/// functions.  The upstream Node driver registers equivalent functions as an
/// SQLite auto-extension because SQLite's built-ins are ASCII-only. Rust's
/// Unicode case conversion is locale-independent and performs the same
/// full-string contextual mappings Zero needs for `ILIKE` (notably final
/// Greek sigma), while keeping `rusqlite` as the connection API.
pub fn install_unicode_case_functions(conn: &Connection) -> Result<(), DbError> {
    let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC;
    conn.create_scalar_function("lower", 1, flags, |ctx| match ctx.get_raw(0) {
        ValueRef::Null => Ok(None::<String>),
        value => Ok(Some(value.as_str()?.to_lowercase())),
    })?;
    conn.create_scalar_function("upper", 1, flags, |ctx| match ctx.get_raw(0) {
        ValueRef::Null => Ok(None::<String>),
        value => Ok(Some(value.as_str()?.to_uppercase())),
    })?;
    Ok(())
}

pub fn verify_engine(conn: &Connection) -> Result<EngineInfo, DbError> {
    let info = engine_info(conn)?;
    if info.version != ZERO_SQLITE_VERSION {
        return Err(DbError(format!(
            "incompatible SQLite engine {}; Zero v1.7 requires the pinned Zero SQLite {ZERO_SQLITE_VERSION}",
            info.version
        )));
    }

    let missing: Vec<_> = REQUIRED_OPTIONS
        .iter()
        .copied()
        .filter(|option| !info.compile_options.contains(*option))
        .collect();
    if !missing.is_empty() {
        return Err(DbError(format!(
            "incompatible SQLite compile options; missing {}",
            missing.join(", ")
        )));
    }

    // This is a parser/runtime capability from the Zero amalgamation, not a
    // compile option. Exercise it directly and leave the connection clean.
    conn.execute_batch("BEGIN CONCURRENT; ROLLBACK")
        .map_err(|error| DbError(format!("Zero SQLite BEGIN CONCURRENT unavailable: {error}")))?;
    Ok(info)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linked_engine_is_the_pinned_zero_build() {
        let conn = Connection::open_in_memory().unwrap();
        let info = verify_engine(&conn).unwrap();
        assert_eq!(info.version, ZERO_SQLITE_VERSION);
        assert!(info.compile_options.contains("ENABLE_STMT_SCANSTATUS"));
        assert!(info.compile_options.contains("THREADSAFE=2"));
    }

    #[test]
    fn unicode_case_functions_match_zero_ilike_requirements() {
        let conn = Connection::open_in_memory().unwrap();
        install_unicode_case_functions(&conn).unwrap();
        let (umlaut, final_sigma): (String, String) = conn
            .query_row("SELECT lower('Ä'), lower('ΟΣ')", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .unwrap();
        assert_eq!(umlaut, "ä");
        assert_eq!(final_sigma, "ος");
    }
}
