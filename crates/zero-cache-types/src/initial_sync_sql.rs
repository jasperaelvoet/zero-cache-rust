//! Port of the pure SQL-string-building helpers from
//! `zero-cache/src/services/change-source/pg/initial-sync.ts` (1473 lines
//! total) — `tableSampleClause`/`limitClause`/`makeBinarySelectExprs`/
//! `makeDownloadStatements`, the download-statement construction that sits
//! ahead of the actual live COPY-streaming/transaction-pool orchestration
//! the rest of that file does. Found while scoping a first tractable slice
//! of `initial-sync.ts` (the single largest unrepresented file found by
//! this session's `zero-cache/src/services` directory-coverage scan).
//!
//! Scope: NOT ported — `initialSync`/`shadowInitialSync` themselves (the
//! actual live orchestration: connecting to upstream Postgres, running
//! `COPY ... TO STDOUT`, streaming binary/text rows into SQLite via a
//! `TransactionPool`, creating the replica schema/indices, replication-slot
//! setup, OTel metrics/histograms), `copyBinary`/`copyText` (the actual
//! streaming decode loop), and `verifyShadowReplica` (shadow-sync
//! comparison). Those need a live-Postgres-COPY-protocol client this port
//! doesn't have yet — a substantial, genuinely separate future increment.
//! This module is the self-contained pure prefix: given a table's spec and
//! sync parameters, what SQL should be issued.

use crate::pg_copy_binary::{has_binary_decoder, BinaryColumnSpec};
use crate::specs::PublishedTableSpec;
use crate::sql::id;

/// Port of `DownloadStatements`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadStatements {
    pub select: String,
    pub get_total_rows: String,
    pub get_total_bytes: String,
}

/// Port of `tableSampleClause`: produces ` TABLESAMPLE BERNOULLI(n)` when
/// `sample_rate` is `< 1`, else `""`. Row-level Bernoulli sampling (rather
/// than SYSTEM) is used because it produces a more uniform sample and,
/// unlike SYSTEM, still returns rows for small tables at low rates.
pub fn table_sample_clause(sample_rate: Option<f64>) -> String {
    match sample_rate {
        None => String::new(),
        Some(rate) if rate >= 1.0 => String::new(),
        Some(rate) => {
            // Round away float noise (e.g. 0.3 * 100 = 30.000000000000004)
            // while still preserving sub-integer rates like 0.001 (0.1%).
            let pct = round_to(rate * 100.0, 6);
            format!(" TABLESAMPLE BERNOULLI({})", format_trimmed(pct))
        }
    }
}

/// Port of `limitClause`.
pub fn limit_clause(max_rows_per_table: Option<i64>) -> String {
    match max_rows_per_table {
        Some(n) => format!(" LIMIT {n}"),
        None => String::new(),
    }
}

fn round_to(v: f64, decimals: u32) -> f64 {
    let factor = 10f64.powi(decimals as i32);
    (v * factor).round() / factor
}

/// Formats a float the way `parseFloat(n.toFixed(6))` does when re-emitted
/// into SQL text: no unnecessary trailing zeros, no trailing decimal point
/// for whole numbers.
fn format_trimmed(v: f64) -> String {
    if v.fract() == 0.0 {
        format!("{}", v as i64)
    } else {
        let s = format!("{v:.6}");
        s.trim_end_matches('0').trim_end_matches('.').to_string()
    }
}

fn to_binary_column_spec(column: &crate::specs::PublishedColumnSpec) -> BinaryColumnSpec {
    BinaryColumnSpec {
        type_oid: column.type_oid,
        data_type: column.column.data_type.clone(),
        pg_type_class: column.column.pg_type_class,
        elem_pg_type_class: column.column.elem_pg_type_class,
    }
}

/// Port of `makeBinarySelectExprs`: the SELECT column expressions for
/// binary COPY, casting columns without a known binary decoder to `::text`.
pub fn make_binary_select_exprs(table: &PublishedTableSpec, cols: &[String]) -> Vec<String> {
    cols.iter()
        .map(|col| {
            let spec = table
                .columns
                .iter()
                .find(|(name, _)| name == col)
                .map(|(_, spec)| to_binary_column_spec(spec));
            match spec {
                Some(spec) if has_binary_decoder(&spec) => id(col),
                _ => format!("{}::text", id(col)),
            }
        })
        .collect()
}

/// Port of `makeDownloadStatements`.
pub fn make_download_statements(
    table: &PublishedTableSpec,
    cols: &[String],
    sample_rate: Option<f64>,
    max_rows_per_table: Option<i64>,
    select_exprs: Option<&[String]>,
) -> DownloadStatements {
    let filter_conditions: Vec<&str> = table
        .publications
        .values()
        .filter_map(|p| p.row_filter.as_deref())
        .collect();
    let where_clause = if filter_conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", filter_conditions.join(" OR "))
    };
    let sample = table_sample_clause(sample_rate);
    let limit = limit_clause(max_rows_per_table);
    let from_table = format!(
        "FROM {}.{}{sample} {where_clause}",
        id(&table.schema),
        id(&table.name)
    );
    let default_select_exprs: Vec<String> = cols.iter().map(|c| id(c)).collect();
    let select_list = select_exprs.unwrap_or(&default_select_exprs).join(",");
    let select = format!("SELECT {select_list} {from_table}{limit}");

    if !limit.is_empty() {
        let bytes_expr = cols
            .iter()
            .map(|c| format!("COALESCE(pg_column_size({}), 0)", id(c)))
            .collect::<Vec<_>>()
            .join(" + ");
        return DownloadStatements {
            select,
            get_total_rows: format!("SELECT COUNT(*)::bigint AS \"totalRows\" FROM (SELECT 1 AS _ {from_table}{limit}) s"),
            get_total_bytes: format!("SELECT COALESCE(SUM(b), 0)::bigint AS \"totalBytes\" FROM (SELECT ({bytes_expr}) AS b {from_table}{limit}) s"),
        };
    }

    let total_bytes = format!(
        "({})",
        cols.iter()
            .map(|c| format!("SUM(COALESCE(pg_column_size({}), 0))", id(c)))
            .collect::<Vec<_>>()
            .join(" + ")
    );
    DownloadStatements {
        select,
        get_total_rows: format!("SELECT COUNT(*) AS \"totalRows\" {from_table}"),
        get_total_bytes: format!("SELECT {total_bytes} AS \"totalBytes\" {from_table}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::specs::{ColumnSpec, PublicationInfo, PublishedColumnSpec};
    use std::collections::BTreeMap;

    #[test]
    fn table_sample_clause_is_empty_for_none_or_full_rate() {
        assert_eq!(table_sample_clause(None), "");
        assert_eq!(table_sample_clause(Some(1.0)), "");
        assert_eq!(table_sample_clause(Some(1.5)), "");
    }

    #[test]
    fn table_sample_clause_formats_a_percentage_and_rounds_away_float_noise() {
        assert_eq!(table_sample_clause(Some(0.3)), " TABLESAMPLE BERNOULLI(30)");
        assert_eq!(
            table_sample_clause(Some(0.001)),
            " TABLESAMPLE BERNOULLI(0.1)"
        );
    }

    #[test]
    fn limit_clause_formats_or_is_empty() {
        assert_eq!(limit_clause(None), "");
        assert_eq!(limit_clause(Some(100)), " LIMIT 100");
    }

    fn table(columns: Vec<(&str, PgTypeClassOpt)>) -> PublishedTableSpec {
        PublishedTableSpec {
            name: "issue".into(),
            schema: "public".into(),
            oid: 1,
            schema_oid: None,
            columns: columns
                .into_iter()
                .map(|(name, class)| {
                    (
                        name.to_string(),
                        PublishedColumnSpec {
                            column: ColumnSpec {
                                pos: 1,
                                data_type: "int4".into(),
                                pg_type_class: class.0,
                                elem_pg_type_class: None,
                                character_maximum_length: None,
                                not_null: None,
                                dflt: None,
                            },
                            type_oid: 23,
                        },
                    )
                })
                .collect(),
            primary_key: Some(vec!["id".into()]),
            replica_identity: None,
            publications: BTreeMap::new(),
        }
    }

    struct PgTypeClassOpt(Option<crate::specs::PgTypeClass>);

    #[test]
    fn make_binary_select_exprs_casts_columns_without_a_binary_decoder() {
        // int4 (no pg_type_class) has a binary decoder; a made-up column
        // with an unrecognized shape (missing from `table.columns`) does not.
        let t = table(vec![("id", PgTypeClassOpt(None))]);
        let exprs = make_binary_select_exprs(&t, &["id".to_string(), "unknown_col".to_string()]);
        assert_eq!(
            exprs[0], "\"id\"",
            "known int4 column has a binary decoder, no cast needed"
        );
        assert_eq!(
            exprs[1], "\"unknown_col\"::text",
            "column missing from the spec falls back to a text cast"
        );
    }

    #[test]
    fn make_download_statements_builds_select_and_count_queries() {
        let t = table(vec![("id", PgTypeClassOpt(None))]);
        let stmts = make_download_statements(&t, &["id".to_string()], None, None, None);
        assert_eq!(stmts.select, "SELECT \"id\" FROM \"public\".\"issue\" ");
        assert!(stmts.get_total_rows.contains("COUNT(*) AS \"totalRows\""));
        assert!(stmts
            .get_total_bytes
            .contains("SUM(COALESCE(pg_column_size(\"id\"), 0))"));
    }

    #[test]
    fn make_download_statements_wraps_counts_in_a_subquery_when_limited() {
        let t = table(vec![("id", PgTypeClassOpt(None))]);
        let stmts = make_download_statements(&t, &["id".to_string()], None, Some(50), None);
        assert!(stmts.select.ends_with(" LIMIT 50"));
        assert!(
            stmts.get_total_rows.contains("FROM (SELECT 1 AS _"),
            "must wrap in a subquery so counts reflect the capped rowset"
        );
        assert!(stmts.get_total_rows.contains("LIMIT 50"));
    }

    #[test]
    fn make_download_statements_includes_a_row_filter_where_clause() {
        let mut t = table(vec![("id", PgTypeClassOpt(None))]);
        t.publications.insert(
            "zero_public".to_string(),
            PublicationInfo {
                row_filter: Some("active = true".to_string()),
            },
        );
        let stmts = make_download_statements(&t, &["id".to_string()], None, None, None);
        assert!(stmts.select.contains("WHERE active = true"));
    }

    #[test]
    fn make_download_statements_applies_table_sample_and_uses_custom_select_exprs() {
        let t = table(vec![("id", PgTypeClassOpt(None))]);
        let select_exprs = vec!["\"id\"::text".to_string()];
        let stmts = make_download_statements(
            &t,
            &["id".to_string()],
            Some(0.5),
            None,
            Some(&select_exprs),
        );
        assert!(stmts.select.contains("TABLESAMPLE BERNOULLI(50)"));
        assert!(stmts.select.starts_with("SELECT \"id\"::text"));
    }
}
