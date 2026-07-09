//! The process-model decision named across several rounds as blocking
//! `replicator.ts`'s IPC half and `syncer.ts`: how does upstream's
//! `Worker`/`Sender`/`Receiver` (Node `child_process.fork()` + tagged
//! `[type, payload]` messages over `postMessage`, ported from
//! `types/processes.ts`) map onto Rust?
//!
//! **Decision, made here:** a Node `Worker` process maps to a `tokio::spawn`
//! task; `Sender.send`/`Receiver.onMessageType` map to a
//! `tokio::sync::mpsc` channel carrying a tagged `(String, T)` pair —
//! the direct structural analog of upstream's `Message<Payload> =
//! [keyof typeof MESSAGE_TYPES, Payload]` tuple. This is the natural Rust
//! idiom for the same problem (typed, ordered, backpressured message
//! passing between concurrent tasks) and needs no IPC/serialization layer
//! since `tokio::spawn` tasks share a process, unlike Node's
//! `child_process.fork()` — a deliberate simplification: this port doesn't
//! need cross-process isolation for the same reasons it doesn't need a
//! separate SQLite process per replica file mode.
//!
//! Scope: ports the message envelope (`WorkerMessage`) and the
//! subscribe/filter primitives (`WorkerSender::send`,
//! `WorkerReceiver::recv_type`) — the direct analog of `MESSAGE_TYPES`/
//! `Message<Payload>`/`onMessageType`. NOT ported: `onceMessageType`
//! (one-shot filtering — a caller can trivially get this by calling
//! `recv_type` once and dropping the receiver, needs no dedicated API),
//! and process lifecycle (`kill`/`pid`/exit-code handling) — this port has
//! no separate OS processes to manage, only tasks.

use tokio::sync::mpsc;

/// A tagged message, the direct analog of upstream's `Message<Payload> =
/// [type, payload]` tuple.
pub type WorkerMessage<T> = (String, T);

/// The sending half of a worker channel. Cloneable, matching how multiple
/// call sites can hold a `Sender` to the same worker upstream.
#[derive(Clone)]
pub struct WorkerSender<T> {
    inner: mpsc::UnboundedSender<WorkerMessage<T>>,
}

/// Error returned when the receiving half has been dropped. Port of the
/// silent-no-op upstream gets from `send()` on a dead process (channel
/// closed) — surfaced as an explicit `Result` here instead, since a caller
/// choosing to ignore it can `.ok()` it away, but a caller that cares can
/// react (matching `Connection.send`'s explicit dropped-message handling).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("worker channel closed")]
pub struct ChannelClosed;

impl<T> WorkerSender<T> {
    /// Port of `Sender.send`.
    pub fn send(&self, msg_type: impl Into<String>, payload: T) -> Result<(), ChannelClosed> {
        self.inner
            .send((msg_type.into(), payload))
            .map_err(|_| ChannelClosed)
    }
}

/// The receiving half of a worker channel.
pub struct WorkerReceiver<T> {
    inner: mpsc::UnboundedReceiver<WorkerMessage<T>>,
}

impl<T> WorkerReceiver<T> {
    /// Port of `Receiver.onMessageType`, adapted to Rust's pull-based
    /// channel model instead of an `EventEmitter` push callback: awaits
    /// the next message whose tag equals `msg_type`, silently skipping
    /// (and discarding) any differently-tagged messages in between —
    /// matching `onMessageType`'s filter. Returns `None` once the sender
    /// half is dropped and no more matching messages will ever arrive.
    pub async fn recv_type(&mut self, msg_type: &str) -> Option<T> {
        loop {
            let (tag, payload) = self.inner.recv().await?;
            if tag == msg_type {
                return Some(payload);
            }
        }
    }

    /// Receives the next message regardless of tag — for a caller that
    /// wants to dispatch on multiple types itself rather than filtering
    /// down to one via [`Self::recv_type`].
    pub async fn recv(&mut self) -> Option<WorkerMessage<T>> {
        self.inner.recv().await
    }
}

/// Creates a worker channel pair. Port of what `child_process.fork()`
/// implicitly wires up between parent and child — here, just the channel
/// a `tokio::spawn`ed task and its spawner communicate over.
pub fn worker_channel<T>() -> (WorkerSender<T>, WorkerReceiver<T>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (WorkerSender { inner: tx }, WorkerReceiver { inner: rx })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn recv_type_returns_matching_message() {
        let (tx, mut rx) = worker_channel::<i32>();
        tx.send("notify", 42).unwrap();
        assert_eq!(rx.recv_type("notify").await, Some(42));
    }

    #[tokio::test]
    async fn recv_type_skips_non_matching_messages() {
        let (tx, mut rx) = worker_channel::<i32>();
        tx.send("status", 1).unwrap();
        tx.send("status", 2).unwrap();
        tx.send("notify", 42).unwrap();
        assert_eq!(rx.recv_type("notify").await, Some(42));
    }

    #[tokio::test]
    async fn recv_type_returns_none_when_sender_dropped() {
        let (tx, mut rx) = worker_channel::<i32>();
        drop(tx);
        assert_eq!(rx.recv_type("notify").await, None);
    }

    #[tokio::test]
    async fn send_after_receiver_dropped_errors() {
        let (tx, rx) = worker_channel::<i32>();
        drop(rx);
        assert_eq!(tx.send("notify", 1), Err(ChannelClosed));
    }

    #[tokio::test]
    async fn sender_is_cloneable_and_both_halves_feed_the_same_receiver() {
        let (tx, mut rx) = worker_channel::<i32>();
        let tx2 = tx.clone();
        tx.send("a", 1).unwrap();
        tx2.send("a", 2).unwrap();
        assert_eq!(rx.recv_type("a").await, Some(1));
        assert_eq!(rx.recv_type("a").await, Some(2));
    }

    #[tokio::test]
    async fn recv_returns_the_tag_alongside_the_payload() {
        let (tx, mut rx) = worker_channel::<i32>();
        tx.send("ready", 7).unwrap();
        assert_eq!(rx.recv().await, Some(("ready".to_string(), 7)));
    }
}
