//! Live `sqlite3_stmt_scanstatus_v2` extraction — the SQLite-binding half of
//! the query-planner cost model whose pure arithmetic
//! (`zero_cache_zql::planner_cost::{estimate_cost, btree_cost}`) is already
//! ported. This produces the per-loop scan statistics
//! (`zqlite/sqlite-cost-model.ts`'s `getScanstatusLoops`) that `estimate_cost`
//! consumes.
//!
//! GATED behind the off-by-default `scanstatus` cargo feature: the underlying
//! `sqlite3_stmt_scanstatus_v2` symbol only exists when SQLite is compiled with
//! `SQLITE_ENABLE_STMT_SCANSTATUS`, which the default bundled build does NOT
//! enable. Enabling the feature without that build flag is a link error — by
//! design isolated so the default workspace build is unaffected.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;

use rusqlite::ffi;

// `iScanStatusOp` selectors (from sqlite3.h). Only the four the cost model
// reads are declared.
const SQLITE_SCANSTAT_EST: c_int = 2;
const SQLITE_SCANSTAT_EXPLAIN: c_int = 4;
const SQLITE_SCANSTAT_SELECTID: c_int = 5;
const SQLITE_SCANSTAT_PARENTID: c_int = 6;
/// `SQLITE_SCANSTAT_COMPLEX` — include all loops (incl. sorts), matching
/// upstream's flag `1`.
const SQLITE_SCANSTAT_COMPLEX: c_int = 1;

extern "C" {
    /// `int sqlite3_stmt_scanstatus_v2(sqlite3_stmt*, int idx, int op, int
    /// flags, void *pOut)` — returns non-zero when `idx` is out of range.
    fn sqlite3_stmt_scanstatus_v2(
        stmt: *mut ffi::sqlite3_stmt,
        idx: c_int,
        i_scan_status_op: c_int,
        flags: c_int,
        p_out: *mut c_void,
    ) -> c_int;
}

/// One scanstatus loop — the Rust mirror of
/// [`zero_cache_zql::planner_cost::ScanstatusLoop`], produced from a live
/// statement. Kept as a local type so this feature-gated module has no effect
/// on the default build's type surface.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanstatusLoop {
    pub select_id: i64,
    pub parent_id: i64,
    pub est: f64,
    pub explain: String,
}

impl ScanstatusLoop {
    /// Convert to the planner's `ScanstatusLoop` so `estimate_cost` can consume
    /// it directly.
    pub fn to_planner(&self) -> zero_cache_zql::planner_cost::ScanstatusLoop {
        zero_cache_zql::planner_cost::ScanstatusLoop {
            select_id: self.select_id,
            parent_id: self.parent_id,
            est: self.est,
            explain: self.explain.clone(),
        }
    }
}

/// Prepares `sql` on the raw connection `db`, extracts its scanstatus loops
/// (sorted by `select_id`), and finalizes the statement.
///
/// # Safety
/// `db` must be a valid open `sqlite3*` and the linked SQLite must be built
/// with `SQLITE_ENABLE_STMT_SCANSTATUS` (see the module doc). Intended to be
/// called via [`crate::StatementRunner::scanstatus_loops`].
pub unsafe fn loops_for(db: *mut ffi::sqlite3, sql: &str) -> Result<Vec<ScanstatusLoop>, String> {
    let csql = CString::new(sql).map_err(|e| e.to_string())?;
    let mut stmt: *mut ffi::sqlite3_stmt = ptr::null_mut();
    let rc = ffi::sqlite3_prepare_v2(db, csql.as_ptr(), -1, &mut stmt, ptr::null_mut());
    if rc != ffi::SQLITE_OK || stmt.is_null() {
        return Err(format!("prepare failed (rc={rc}) for: {sql}"));
    }

    // Run the statement to completion so the planner populates scan stats.
    loop {
        let s = ffi::sqlite3_step(stmt);
        if s == ffi::SQLITE_ROW {
            continue;
        }
        break;
    }

    let mut out = Vec::new();
    let mut idx: c_int = 0;
    loop {
        let mut select_id: c_int = 0;
        let rc = sqlite3_stmt_scanstatus_v2(
            stmt,
            idx,
            SQLITE_SCANSTAT_SELECTID,
            SQLITE_SCANSTAT_COMPLEX,
            &mut select_id as *mut c_int as *mut c_void,
        );
        if rc != 0 {
            break; // idx past the last loop
        }

        let mut parent_id: c_int = 0;
        sqlite3_stmt_scanstatus_v2(
            stmt,
            idx,
            SQLITE_SCANSTAT_PARENTID,
            SQLITE_SCANSTAT_COMPLEX,
            &mut parent_id as *mut c_int as *mut c_void,
        );

        let mut est: f64 = 0.0;
        sqlite3_stmt_scanstatus_v2(
            stmt,
            idx,
            SQLITE_SCANSTAT_EST,
            SQLITE_SCANSTAT_COMPLEX,
            &mut est as *mut f64 as *mut c_void,
        );

        let mut explain_ptr: *const c_char = ptr::null();
        sqlite3_stmt_scanstatus_v2(
            stmt,
            idx,
            SQLITE_SCANSTAT_EXPLAIN,
            SQLITE_SCANSTAT_COMPLEX,
            &mut explain_ptr as *mut *const c_char as *mut c_void,
        );
        let explain = if explain_ptr.is_null() {
            String::new()
        } else {
            CStr::from_ptr(explain_ptr).to_string_lossy().into_owned()
        };

        out.push(ScanstatusLoop {
            select_id: select_id as i64,
            parent_id: parent_id as i64,
            est,
            explain,
        });
        idx += 1;
    }

    out.sort_by_key(|l| l.select_id);
    ffi::sqlite3_finalize(stmt);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use crate::StatementRunner;

    /// Live extraction against a real SQLite (requires the crate's SQLite to be
    /// built with `SQLITE_ENABLE_STMT_SCANSTATUS`, e.g.
    /// `LIBSQLITE3_FLAGS=SQLITE_ENABLE_STMT_SCANSTATUS cargo test -p
    /// zero-cache-sqlite --features scanstatus`). A `SELECT ... ORDER BY`
    /// yields at least one loop; the sort surfaces as a top-level `ORDER BY`
    /// loop, and the extracted loops drive the ported `estimate_cost`.
    #[test]
    fn scanstatus_extracts_loops_and_drives_estimate_cost() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE issue(id INTEGER PRIMARY KEY, title TEXT)")
            .unwrap();
        for i in 0..50 {
            db.run(
                "INSERT INTO issue(id, title) VALUES (?, ?)",
                &[
                    crate::Value::Integer(i),
                    crate::Value::Text(format!("t{i}")),
                ],
            )
            .unwrap();
        }

        let loops = db
            .scanstatus_loops("SELECT id, title FROM issue ORDER BY title")
            .expect("scanstatus extraction");
        assert!(!loops.is_empty(), "scanstatus returned loops for the query");
        // The temp-b-tree sort for `ORDER BY title` (title is unindexed) shows
        // up as a loop whose explain mentions ORDER BY.
        assert!(
            loops.iter().any(|l| l.explain.contains("ORDER BY")),
            "an ORDER BY sort loop is present: {loops:?}"
        );

        // Feed the extracted loops through the ported cost arithmetic.
        let planner_loops: Vec<_> = loops.iter().map(|l| l.to_planner()).collect();
        let fanout = std::rc::Rc::new(|_cols: &[String]| zero_cache_zql::planner_cost::FanoutEst {
            fanout: 1.0,
            confidence: zero_cache_zql::planner_cost::FanoutConfidence::None,
        });
        let cost = zero_cache_zql::planner_cost::estimate_cost(&planner_loops, fanout);
        // The scan estimate should reflect the ~50-row table, and the ORDER BY
        // adds a positive sort startup cost.
        assert!(cost.rows > 0.0, "row estimate is positive: {}", cost.rows);
        assert!(
            cost.startup_cost > 0.0,
            "ORDER BY produced a sort startup cost: {}",
            cost.startup_cost
        );
    }
}
