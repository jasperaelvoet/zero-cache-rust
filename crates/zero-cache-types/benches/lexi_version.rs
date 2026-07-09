//! Benchmarks for the LexiVersion codec.
//!
//! Establishes the workspace benchmark pattern (Criterion, `harness = false`),
//! the Rust analogue of zero-cache's mitata/vitest `bench` setup. Run with
//! `cargo bench -p zero-cache-types`.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use num_bigint::BigInt;
use zero_cache_types::{version_from_lexi, version_to_lexi};

fn bench_encode(c: &mut Criterion) {
    c.bench_function("versionToLexi/small", |b| {
        b.iter(|| version_to_lexi(black_box(46655i64)).unwrap())
    });
    c.bench_function("versionToLexi/max_safe", |b| {
        b.iter(|| version_to_lexi(black_box(9_007_199_254_740_991i64)).unwrap())
    });
    let big = num_traits::pow(BigInt::from(2), 128);
    c.bench_function("versionToLexi/bigint_128bit", |b| {
        b.iter(|| version_to_lexi(black_box(big.clone())).unwrap())
    });
}

fn bench_decode(c: &mut Criterion) {
    c.bench_function("versionFromLexi/small", |b| {
        b.iter(|| version_from_lexi(black_box("2zzz")).unwrap())
    });
    c.bench_function("versionFromLexi/bigint_128bit", |b| {
        b.iter(|| version_from_lexi(black_box("of5lxx1zz5pnorynqglhzmsp34")).unwrap())
    });
}

criterion_group!(benches, bench_encode, bench_decode);
criterion_main!(benches);
