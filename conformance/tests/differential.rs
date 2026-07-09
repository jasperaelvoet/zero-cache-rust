//! Env-gated differential conformance tests.
//!
//! These connect to running servers, so they only execute when the relevant
//! URLs are set (otherwise they skip cleanly, like the live-Postgres tests in
//! the main workspace):
//!
//!   ZERO_RUST_URL  — this Rust server (required to run any of these)
//!   ZERO_REF_URL   — the official rocicorp/zero server (for the true diff)
//!
//! Run the whole conformance stack with
//! `docker compose -f conformance/docker-compose.conformance.yml up --build`,
//! then `ZERO_RUST_URL=ws://127.0.0.1:4848/sync
//! ZERO_REF_URL=ws://127.0.0.1:4849/sync cargo test -p zero-conformance`.

use zero_conformance::{diff, run, scenarios};

fn rust_url() -> Option<String> {
    std::env::var("ZERO_RUST_URL").ok()
}

/// Rust vs the reference server: every scenario must produce equivalent
/// normalized responses. Skips unless BOTH URLs are set.
#[tokio::test]
async fn rust_matches_reference_on_every_scenario() {
    let (Some(rust), Ok(reference)) = (rust_url(), std::env::var("ZERO_REF_URL")) else {
        eprintln!("skipping: set ZERO_RUST_URL and ZERO_REF_URL to run the differential test");
        return;
    };

    let mut diffs = Vec::new();
    for scenario in scenarios() {
        let a = run(&rust, &scenario).await.expect("rust run");
        let b = run(&reference, &scenario).await.expect("reference run");
        if let Some(d) = diff(&a, &b, "rust", "ref") {
            diffs.push(format!("scenario `{}` diverged:\n{d}", scenario.name));
        }
    }
    assert!(
        diffs.is_empty(),
        "conformance divergences:\n{}",
        diffs.join("\n")
    );
}

/// Self-check: the Rust server is deterministic — running each scenario twice
/// yields identical normalized responses. Skips unless ZERO_RUST_URL is set.
/// This validates the harness and the server's determinism even without the
/// reference container available.
#[tokio::test]
async fn rust_is_self_consistent() {
    let Some(rust) = rust_url() else {
        eprintln!("skipping: set ZERO_RUST_URL to run the self-consistency test");
        return;
    };

    for scenario in scenarios() {
        let a = run(&rust, &scenario).await.expect("run 1");
        let b = run(&rust, &scenario).await.expect("run 2");
        assert!(
            diff(&a, &b, "run1", "run2").is_none(),
            "scenario `{}` was non-deterministic across two runs",
            scenario.name
        );
    }
}
