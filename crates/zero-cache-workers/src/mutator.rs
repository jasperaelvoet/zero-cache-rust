//! Port of `zero-cache/src/workers/mutator.ts`'s `Mutator` — currently a
//! stub upstream too (its own `TODO` comment: "install websocket receiver
//! / spin up pusher services for each unique client group that connects").
//! Ports the `SingletonService` run/stop/drain state machine faithfully;
//! there is no more logic to port until upstream itself grows past the
//! stub.

use std::sync::{Arc, Condvar, Mutex};

/// Port of `Mutator`'s `resolver()`-based stop signal — a promise that
/// resolves once, shared between `run()`'s waiter and `stop()`/`drain()`'s
/// resolver. `resolver()` in TS is a manually-resolvable promise; the
/// closest faithful Rust shape is a condvar-guarded flag rather than a
/// oneshot channel, since `stop()` must be callable (and idempotent) even
/// after `run()` has already returned — matching upstream's `resolve()`
/// being safe to call multiple times.
struct StopSignal {
    stopped: Mutex<bool>,
    condvar: Condvar,
}

impl StopSignal {
    fn new() -> Self {
        StopSignal {
            stopped: Mutex::new(false),
            condvar: Condvar::new(),
        }
    }

    fn resolve(&self) {
        let mut stopped = self.stopped.lock().unwrap();
        *stopped = true;
        self.condvar.notify_all();
    }

    fn wait(&self) {
        let stopped = self.stopped.lock().unwrap();
        let _guard = self.condvar.wait_while(stopped, |s| !*s).unwrap();
    }
}

/// Port of `Mutator`. `id` is `mutator-{pid}` upstream (Node's `process.pid`);
/// this port takes the id as a constructor parameter instead of reading an
/// ambient process id, matching this port's determinism convention of
/// passing in what upstream reads ambiently.
pub struct Mutator {
    pub id: String,
    stop_signal: Arc<StopSignal>,
}

impl Mutator {
    pub fn new(id: impl Into<String>) -> Self {
        Mutator {
            id: id.into(),
            stop_signal: Arc::new(StopSignal::new()),
        }
    }

    /// Port of `run()`: blocks until `stop()`/`drain()` resolves the stop
    /// signal. Synchronous (blocking) rather than `async` — this stub has
    /// no actual work to await yet; a caller running it off the main thread
    /// would use `std::thread::spawn` (or wrap in `tokio::task::spawn_blocking`
    /// once this needs to coexist with the rest of this port's async code).
    pub fn run(&self) {
        self.stop_signal.wait();
    }

    /// Port of `stop()`. Idempotent, matching upstream's resolver being
    /// safe to resolve more than once.
    pub fn stop(&self) {
        self.stop_signal.resolve();
    }

    /// Port of `drain()` — identical to `stop()` upstream (both just
    /// resolve the same signal).
    pub fn drain(&self) {
        self.stop_signal.resolve();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stop_unblocks_run() {
        let mutator = Arc::new(Mutator::new("mutator-1"));
        let m = mutator.clone();
        let handle = std::thread::spawn(move || m.run());
        mutator.stop();
        handle.join().unwrap();
    }

    #[test]
    fn drain_also_unblocks_run() {
        let mutator = Arc::new(Mutator::new("mutator-1"));
        let m = mutator.clone();
        let handle = std::thread::spawn(move || m.run());
        mutator.drain();
        handle.join().unwrap();
    }

    #[test]
    fn stop_is_idempotent() {
        let mutator = Mutator::new("mutator-1");
        mutator.stop();
        mutator.stop();
        mutator.run(); // must not hang
    }

    #[test]
    fn id_is_preserved() {
        let mutator = Mutator::new("mutator-42");
        assert_eq!(mutator.id, "mutator-42");
    }
}
