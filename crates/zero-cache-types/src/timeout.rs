//! Port of `zero-cache/src/types/timeout.ts`.
//!
//! Races a future against a timeout. Where the TypeScript version returns a
//! `T | U` union, this returns the [`OrTimeout`] enum.

use std::future::Future;
use std::time::Duration;

/// The result of a timeout race: either the future's value or the timeout
/// value. Port of the `T | U` union returned by `orTimeoutWith`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrTimeout<T, U> {
    Value(T),
    Timeout(U),
}

/// Resolves to `OrTimeout::Timeout("timed-out")` if `timeout` elapses before
/// `fut` completes. Port of `orTimeout`.
pub async fn or_timeout<T>(
    fut: impl Future<Output = T>,
    timeout: Duration,
) -> OrTimeout<T, &'static str> {
    or_timeout_with(fut, timeout, "timed-out").await
}

/// Resolves to `OrTimeout::Timeout(timeout_value)` if `timeout` elapses before
/// `fut` completes. Port of `orTimeoutWith`.
pub async fn or_timeout_with<T, U>(
    fut: impl Future<Output = T>,
    timeout: Duration,
    timeout_value: U,
) -> OrTimeout<T, U> {
    tokio::select! {
        biased;
        v = fut => OrTimeout::Value(v),
        _ = tokio::time::sleep(timeout) => OrTimeout::Timeout(timeout_value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resolved() {
        let result = or_timeout(async { "foo".to_string() }, Duration::from_millis(1)).await;
        assert_eq!(result, OrTimeout::Value("foo".to_string()));
    }

    #[tokio::test]
    async fn times_out() {
        let never = std::future::pending::<String>();
        let result = or_timeout(never, Duration::from_millis(1)).await;
        assert_eq!(result, OrTimeout::Timeout("timed-out"));
    }

    #[tokio::test]
    async fn times_out_with_value() {
        let never = std::future::pending::<String>();
        let result = or_timeout_with(never, Duration::from_millis(1), 123.456f64).await;
        assert_eq!(result, OrTimeout::Timeout(123.456));
    }
}
