//! Upstream `ProcessManager` semantics for the port's service threads.
//!
//! Real zero's runner waits for its workers' "ready" signals before the
//! public dispatcher starts listening, and exits the whole process when any
//! worker dies (`services/life-cycle.ts`), so a failed server crash-loops
//! visibly (and gets restarted by the container runner) instead of staying
//! alive while serving nothing. The port hosts the replicator / view-syncer
//! on threads rather than child processes; these helpers give those threads
//! the same lifecycle.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

/// Outcome of waiting for a service thread to signal readiness.
pub enum ReadyWait<T> {
    /// The service signalled readiness. The handle is returned so the caller
    /// can keep watching it (see [`exit_process_when_thread_dies`]).
    Ready(JoinHandle<T>),
    /// Shutdown was requested before the service became ready.
    Shutdown,
    /// The service thread exited before readiness; the message describes how.
    Died(String),
}

/// Wait until `ready` flips true, shutdown is requested, or the service
/// thread exits — whichever comes first. A thread that dies before readiness
/// previously left the server waiting forever with its error silently
/// dropped; here the error becomes the `Died` message for the caller to log
/// before exiting nonzero (upstream exits when any worker dies).
pub async fn wait_for_ready<T, E: std::fmt::Display>(
    name: &str,
    ready: &AtomicBool,
    shutdown: &AtomicBool,
    handle: JoinHandle<Result<T, E>>,
) -> ReadyWait<Result<T, E>> {
    loop {
        // Order matters: a thread may signal readiness and exit legitimately
        // later; readiness observed first wins.
        if ready.load(Ordering::SeqCst) {
            return ReadyWait::Ready(handle);
        }
        if handle.is_finished() {
            let message = match handle.join() {
                Ok(Ok(_)) => format!("{name} exited before signalling readiness"),
                Ok(Err(error)) => format!("{name} failed during startup: {error}"),
                Err(_) => format!("{name} panicked during startup"),
            };
            return ReadyWait::Died(message);
        }
        if shutdown.load(Ordering::SeqCst) {
            return ReadyWait::Shutdown;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Watch a (ready) service thread for the rest of the process lifetime: if it
/// exits while shutdown has not been requested, log why and exit the process
/// nonzero so the container runner restarts it. Without this, a dead
/// replicator leaves the server serving a frozen replica indefinitely.
pub fn exit_process_when_thread_dies<T, E>(
    name: &'static str,
    shutdown: Arc<AtomicBool>,
    handle: JoinHandle<Result<T, E>>,
) where
    T: Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    std::thread::Builder::new()
        .name(format!("{name}-monitor"))
        .spawn(move || {
            let outcome = handle.join();
            if shutdown.load(Ordering::SeqCst) {
                return; // orderly shutdown; the thread is expected to stop
            }
            match outcome {
                Ok(Ok(_)) => crate::error!("{name} exited unexpectedly; shutting down"),
                Ok(Err(error)) => crate::error!("{name} failed: {error}; shutting down"),
                Err(_) => crate::error!("{name} panicked; shutting down"),
            }
            std::process::exit(-1);
        })
        .expect("spawn service monitor thread");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flags() -> (Arc<AtomicBool>, Arc<AtomicBool>) {
        (
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        )
    }

    #[tokio::test]
    async fn thread_error_before_readiness_is_reported_not_awaited_forever() {
        let (ready, shutdown) = flags();
        let handle =
            std::thread::spawn(|| Err::<(), String>("cannot reach upstream Postgres".into()));
        match wait_for_ready("replicator", &ready, &shutdown, handle).await {
            ReadyWait::Died(message) => {
                assert!(
                    message.contains("cannot reach upstream Postgres"),
                    "{message}"
                );
                assert!(message.contains("replicator"), "{message}");
            }
            _ => panic!("a dead thread must surface as Died"),
        }
    }

    #[tokio::test]
    async fn readiness_returns_the_live_handle() {
        let (ready, shutdown) = flags();
        let thread_ready = ready.clone();
        let handle = std::thread::spawn(move || {
            thread_ready.store(true, Ordering::SeqCst);
            // Keep running a moment after readiness, like a real service.
            std::thread::sleep(std::time::Duration::from_millis(50));
            Ok::<(), String>(())
        });
        match wait_for_ready("replicator", &ready, &shutdown, handle).await {
            ReadyWait::Ready(handle) => assert!(handle.join().unwrap().is_ok()),
            _ => panic!("a ready service must return its handle"),
        }
    }

    #[tokio::test]
    async fn shutdown_request_interrupts_the_wait() {
        let (ready, shutdown) = flags();
        shutdown.store(true, Ordering::SeqCst);
        let handle = std::thread::spawn(|| {
            std::thread::sleep(std::time::Duration::from_secs(5));
            Ok::<(), String>(())
        });
        assert!(matches!(
            wait_for_ready("replicator", &ready, &shutdown, handle).await,
            ReadyWait::Shutdown
        ));
    }

    #[tokio::test]
    async fn thread_finishing_ok_before_readiness_is_still_a_death() {
        let (ready, shutdown) = flags();
        let handle = std::thread::spawn(|| Ok::<(), String>(()));
        while !handle.is_finished() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        match wait_for_ready("view-syncer", &ready, &shutdown, handle).await {
            ReadyWait::Died(message) => {
                assert!(
                    message.contains("exited before signalling readiness"),
                    "{message}"
                )
            }
            _ => panic!("an Ok exit before readiness is still a startup failure"),
        }
    }
}
