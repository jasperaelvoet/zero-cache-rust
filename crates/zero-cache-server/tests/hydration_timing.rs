//! Micro-timing of the per-connection hydration sub-steps to locate the
//! dominant per-row cost (run with `--ignored --nocapture`). Not a correctness
//! test — a profiler you can run locally without Docker.

use std::time::Instant;

use zero_cache_server::live_hydration::fetch_rows_from_sqlite;
use zero_cache_shared::bigint_json::JsonValue;
use zero_cache_sqlite::change_log::CREATE_CHANGELOG_SCHEMA;
use zero_cache_sqlite::replication_state::init_replication_state;
use zero_cache_sqlite::StatementRunner;
use zero_cache_zql::ivm::operator::FetchRequest;

fn replica_with_rows(n: usize) -> String {
    replica_with_rows_tagged(n, "default")
}

fn replica_with_rows_tagged(n: usize, tag: &str) -> String {
    let path = std::env::temp_dir()
        .join(format!(
            "zero-hydration-timing-{}-{tag}.db",
            std::process::id()
        ))
        .to_string_lossy()
        .into_owned();
    let _ = std::fs::remove_file(&path);
    let w = StatementRunner::open_file(&path).unwrap();
    init_replication_state(&w, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
    w.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
    w.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT, owner TEXT, open INTEGER, rank INTEGER, _0_version TEXT)")
        .unwrap();
    for i in 0..n {
        w.run(
            &format!("INSERT INTO issue VALUES ({i}, 'title-{i}', 'owner-{i}', 1, {i}, '00')"),
            &[],
        )
        .unwrap();
    }
    drop(w);
    path
}

#[test]
#[ignore]
fn time_fetch_rows_from_sqlite() {
    let n = 1000;
    let iters = 200;
    let path = replica_with_rows(n);
    let db = StatementRunner::open_file_readonly(&path).unwrap();
    let pk = vec!["id".to_string()];
    let sort = vec![("id".to_string(), zero_cache_protocol::ast::Direction::Asc)];
    let columns = vec![
        "id".to_string(),
        "title".into(),
        "owner".into(),
        "open".into(),
        "rank".into(),
        "_0_version".into(),
    ];
    let req = FetchRequest::default();

    // Warm up (prepared-statement cache, page cache).
    for _ in 0..5 {
        let rows =
            fetch_rows_from_sqlite(&db, "issue", &pk, &sort, columns.clone(), &req, None, None)
                .unwrap();
        assert_eq!(rows.len(), n);
    }

    let start = Instant::now();
    for _ in 0..iters {
        let rows =
            fetch_rows_from_sqlite(&db, "issue", &pk, &sort, columns.clone(), &req, None, None)
                .unwrap();
        std::hint::black_box(&rows);
    }
    let elapsed = start.elapsed();
    let per = elapsed.as_secs_f64() * 1000.0 / iters as f64;
    println!(
        "\nfetch_rows_from_sqlite: {n} rows x {iters} iters = {:?}  =>  {per:.3} ms/fetch  ({:.1} us/row)",
        elapsed,
        per * 1000.0 / n as f64
    );

    let _ = std::fs::remove_file(path);
}

#[test]
#[ignore]
fn time_hydrate_query_from_rows() {
    use std::collections::{HashMap, HashSet};
    use zero_cache_view_syncer::cvr_types::{Cvr, TtlClock};
    use zero_cache_view_syncer::cvr_version::CvrVersion;
    use zero_cache_view_syncer::query_hydration::hydrate_query_from_rows;
    use zero_cache_zql::ivm::data::Row as ZqlRow;

    let n = 1000usize;
    let iters = 200;
    let path = replica_with_rows_tagged(n, "hydrate");
    let db = StatementRunner::open_file_readonly(&path).unwrap();
    let pk = vec!["id".to_string()];
    let sort = vec![("id".to_string(), zero_cache_protocol::ast::Direction::Asc)];
    let columns = vec![
        "id".to_string(),
        "title".into(),
        "owner".into(),
        "open".into(),
        "rank".into(),
        "_0_version".into(),
    ];
    let req = FetchRequest::default();

    let key_of = |row: &ZqlRow| -> String {
        row.iter()
            .find(|(n, _)| n == "id")
            .map(|(_, v)| v.stringify())
            .unwrap_or_default()
    };

    let start = Instant::now();
    for _ in 0..iters {
        let rows =
            fetch_rows_from_sqlite(&db, "issue", &pk, &sort, columns.clone(), &req, None, None)
                .unwrap();
        let mut cvr = Cvr {
            id: "cg1".into(),
            version: CvrVersion {
                state_version: "01".into(),
                config_version: None,
            },
            last_active: 0.0,
            ttl_clock: TtlClock::from_number(0.0),
            replica_version: None,
            clients: Default::default(),
            queries: Default::default(),
            client_schema: None,
            profile_id: None,
        };
        let orig = CvrVersion {
            state_version: "00".into(),
            config_version: None,
        };
        let mut tracked = HashSet::new();
        let mut received = HashMap::new();
        let mut last = HashMap::new();
        let result = hydrate_query_from_rows(
            &mut cvr,
            &orig,
            &mut tracked,
            "q",
            "q",
            rows,
            key_of,
            |_row| std::collections::BTreeMap::from([("q".to_string(), 1i64)]),
            |row: &ZqlRow| {
                row.iter()
                    .find(|(n, _)| n == "_0_version")
                    .map(|(_, v)| v.stringify())
                    .unwrap_or_default()
            },
            &Default::default(),
            &[],
            &mut received,
            &mut last,
        );
        std::hint::black_box(&result);
    }
    let elapsed = start.elapsed();
    let per = elapsed.as_secs_f64() * 1000.0 / iters as f64;
    println!("\nfetch + hydrate_query_from_rows: {n} rows x {iters} = {:?} => {per:.3} ms/hydration ({:.2} us/row)", elapsed, per * 1000.0 / n as f64);
    let _ = std::fs::remove_file(path);
}
