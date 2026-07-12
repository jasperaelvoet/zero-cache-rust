//! Micro-timing of the per-connection hydration sub-steps to locate the
//! dominant per-row cost (run with `--ignored --nocapture`). Not a correctness
//! test — a profiler you can run locally without Docker.

use std::time::Instant;

use zero_cache_server::live_hydration::fetch_rows_from_sqlite;
use zero_cache_sqlite::change_log::CREATE_CHANGELOG_SCHEMA;
use zero_cache_sqlite::replication_state::init_replication_state;
use zero_cache_sqlite::StatementRunner;
use zero_cache_shared::bigint_json::JsonValue;
use zero_cache_zql::ivm::operator::FetchRequest;

fn replica_with_rows(n: usize) -> String {
    let path = std::env::temp_dir()
        .join(format!("zero-hydration-timing-{}.db", std::process::id()))
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
            &format!(
                "INSERT INTO issue VALUES ({i}, 'title-{i}', 'owner-{i}', 1, {i}, '00')"
            ),
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
        let rows = fetch_rows_from_sqlite(&db, "issue", &pk, &sort, columns.clone(), &req, None, None).unwrap();
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
