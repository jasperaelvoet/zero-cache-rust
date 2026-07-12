//! Per-user CRUD mutation rate limiting (`ZERO_PER_USER_MUTATION_LIMIT_MAX` /
//! `ZERO_PER_USER_MUTATION_LIMIT_WINDOW_MS`).
//!
//! Upstream scopes one `SlidingWindowLimiter` per Mutagen service instance —
//! i.e. per client group — and constructs it only when `max` is configured.
//! A denied mutation is refused with "Rate limit exceeded" (upstream error
//! kind `MutationRateLimited`) without disconnecting; throttled attempts do
//! not consume budget, so a retrying client is never locked out permanently.
//! Custom mutations (the push/mutate API-server path) are not limited,
//! matching upstream.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use zero_cache_mutagen::sliding_window_limiter::SlidingWindowLimiter;

struct Limiters {
    max: u64,
    window_ms: i64,
    per_group: Mutex<HashMap<String, SlidingWindowLimiter>>,
}

static LIMITERS: OnceLock<Option<Limiters>> = OnceLock::new();

/// Installs the process-wide limiter configuration once at startup. `None`
/// max = rate limiting disabled (upstream default).
pub fn init(max: Option<u64>, window_ms: u64) {
    let _ = LIMITERS.set(max.map(|max| Limiters {
        max,
        window_ms: window_ms as i64,
        per_group: Mutex::new(HashMap::new()),
    }));
}

/// Whether `client_group_id` may perform a CRUD mutation right now. Counts
/// the call only when allowed. Always true when rate limiting is disabled or
/// [`init`] has not run (tests).
pub fn can_do(client_group_id: &str) -> bool {
    let Some(Some(limiters)) = LIMITERS.get() else {
        return true;
    };
    let mut map = limiters.per_group.lock().unwrap();
    let limiter = map
        .entry(client_group_id.to_string())
        .or_insert_with(|| SlidingWindowLimiter::new(limiters.window_ms, limiters.max));
    limiter.can_do_now()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uninitialized_limiter_allows_everything() {
        // init() is deliberately NOT called here (OnceLock is per-process, so
        // this test relies on running before/without init in this binary).
        assert!(can_do("some-group") || LIMITERS.get().is_some());
    }
}
