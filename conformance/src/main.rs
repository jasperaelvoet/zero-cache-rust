//! Conformance CLI: replays the scenario battery against two sync servers and
//! reports per-scenario PASS / DIFF.
//!
//!   ZERO_RUST_URL  this Rust server        (default ws://127.0.0.1:4848/sync)
//!   ZERO_REF_URL   the reference server     (the official rocicorp/zero)
//!
//! With both set it runs a true differential comparison. With only the Rust URL
//! set it runs a self-check (Rust vs Rust) that validates determinism and the
//! harness itself. Exits non-zero if any scenario differs.

use zero_conformance::{diff, run, scenarios};

#[tokio::main]
async fn main() {
    let rust_url =
        std::env::var("ZERO_RUST_URL").unwrap_or_else(|_| "ws://127.0.0.1:4848/sync".to_string());
    let ref_url = std::env::var("ZERO_REF_URL").ok();

    let (b_label, b_url, self_check) = match &ref_url {
        Some(u) => ("ref", u.clone(), false),
        None => {
            eprintln!(
                "ZERO_REF_URL not set — running SELF-CHECK (Rust vs Rust determinism).\n\
                 Set ZERO_REF_URL=ws://<rocicorp-zero-host>/sync for a real differential run.\n"
            );
            ("rust#2", rust_url.clone(), true)
        }
    };

    println!("A = rust   {rust_url}");
    println!("B = {b_label:<6} {b_url}\n");

    let mut failures = 0;
    for scenario in scenarios() {
        let a = run(&rust_url, &scenario).await;
        let b = run(&b_url, &scenario).await;
        match (a, b) {
            (Ok(a), Ok(b)) => match diff(&a, &b, "A", "B") {
                None => println!("  PASS  {}", scenario.name),
                Some(d) => {
                    // In self-check mode a diff means non-determinism; otherwise
                    // it's a genuine behavioral divergence.
                    let kind = if self_check { "FLAKY" } else { "DIFF" };
                    println!("  {kind}  {}\n{d}", scenario.name);
                    failures += 1;
                }
            },
            (a, b) => {
                if let Err(e) = a {
                    println!("  ERROR {} (A: {e})", scenario.name);
                }
                if let Err(e) = b {
                    println!("  ERROR {} (B: {e})", scenario.name);
                }
                failures += 1;
            }
        }
    }

    println!();
    if failures == 0 {
        println!("All scenarios equivalent.");
    } else {
        println!("{failures} scenario(s) diverged / errored.");
        std::process::exit(1);
    }
}
