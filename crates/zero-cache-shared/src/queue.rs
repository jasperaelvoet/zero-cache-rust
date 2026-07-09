//! Port of `packages/shared/src/queue.ts`.
//!
//! A `Queue` lets consumers await (possibly future) values and producers
//! enqueue values or rejections. The TypeScript version is backed by a
//! `RingBuffer`; here we use [`std::collections::VecDeque`], which is already a
//! growable ring buffer with O(1) push/pop, so `RingBuffer` needs no separate
//! port.
//!
//! `E` is the rejection type (the TS `unknown` reason). Callers pick a concrete
//! type per queue.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::oneshot;

enum Produced<T, E> {
    Value(T),
    Rejection(E),
}

struct Inner<T, E> {
    produced: VecDeque<Produced<T, E>>,
    consumers: VecDeque<(u64, oneshot::Sender<Result<T, E>>)>,
    next_consumer_id: u64,
}

/// A multi-producer/multi-consumer async queue. Port of `Queue<T>`. Cloning
/// shares the same underlying queue.
pub struct Queue<T, E = String> {
    inner: Arc<Mutex<Inner<T, E>>>,
}

impl<T, E> Clone for Queue<T, E> {
    fn clone(&self) -> Self {
        Queue {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T, E> Default for Queue<T, E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, E> Queue<T, E> {
    pub fn new() -> Self {
        Queue {
            inner: Arc::new(Mutex::new(Inner {
                produced: VecDeque::new(),
                consumers: VecDeque::new(),
                next_consumer_id: 0,
            })),
        }
    }

    /// Enqueues a value, handing it directly to a waiting consumer if any. Port
    /// of `enqueue`. A consumer whose receiver was dropped (e.g. a timed-out or
    /// canceled `dequeue`) is skipped.
    pub fn enqueue(&self, mut value: T) {
        let mut inner = self.inner.lock().unwrap();
        while let Some((_, consumer)) = inner.consumers.pop_front() {
            match consumer.send(Ok(value)) {
                Ok(()) => return,
                Err(Ok(v)) => value = v, // consumer gone; try the next one
                Err(Err(_)) => unreachable!(),
            }
        }
        inner.produced.push_back(Produced::Value(value));
    }

    /// Enqueues a rejection, handing it to a waiting consumer if any. Port of
    /// `enqueueRejection`.
    pub fn enqueue_rejection(&self, mut reason: E) {
        let mut inner = self.inner.lock().unwrap();
        while let Some((_, consumer)) = inner.consumers.pop_front() {
            match consumer.send(Err(reason)) {
                Ok(()) => return,
                Err(Err(r)) => reason = r, // consumer gone; try the next one
                Err(Ok(_)) => unreachable!(),
            }
        }
        inner.produced.push_back(Produced::Rejection(reason));
    }

    /// The number of values waiting to be dequeued. Port of `size`.
    pub fn size(&self) -> usize {
        self.inner.lock().unwrap().produced.len()
    }

    /// Non-blocking dequeue: returns the next value/rejection if one is already
    /// available, else `None`. (The sync fast path of the TS `dequeue`.)
    pub fn try_dequeue(&self) -> Option<Result<T, E>> {
        let mut inner = self.inner.lock().unwrap();
        inner.produced.pop_front().map(|p| match p {
            Produced::Value(v) => Ok(v),
            Produced::Rejection(e) => Err(e),
        })
    }

    /// Awaits the next value (or rejection). Port of `dequeue()` with no timeout.
    pub async fn dequeue(&self) -> Result<T, E> {
        let rx = {
            let mut inner = self.inner.lock().unwrap();
            if let Some(p) = inner.produced.pop_front() {
                return match p {
                    Produced::Value(v) => Ok(v),
                    Produced::Rejection(e) => Err(e),
                };
            }
            let (tx, rx) = oneshot::channel();
            let id = inner.next_consumer_id;
            inner.next_consumer_id += 1;
            inner.consumers.push_back((id, tx));
            rx
        };
        rx.await.expect("queue consumer dropped without resolution")
    }

    /// Awaits the next value, resolving to `timeout_value` if nothing is
    /// produced within `timeout`. Port of `dequeue(timeoutValue, timeoutMs)`.
    pub async fn dequeue_or(&self, timeout_value: T, timeout: Duration) -> Result<T, E> {
        let (id, mut rx) = {
            let mut inner = self.inner.lock().unwrap();
            if let Some(p) = inner.produced.pop_front() {
                return match p {
                    Produced::Value(v) => Ok(v),
                    Produced::Rejection(e) => Err(e),
                };
            }
            let (tx, rx) = oneshot::channel();
            let id = inner.next_consumer_id;
            inner.next_consumer_id += 1;
            inner.consumers.push_back((id, tx));
            (id, rx)
        };

        tokio::select! {
            biased;
            r = &mut rx => r.expect("queue consumer dropped without resolution"),
            _ = tokio::time::sleep(timeout) => {
                let removed = {
                    let mut inner = self.inner.lock().unwrap();
                    let before = inner.consumers.len();
                    inner.consumers.retain(|(cid, _)| *cid != id);
                    inner.consumers.len() != before
                };
                if removed {
                    Ok(timeout_value)
                } else {
                    // A value was delivered concurrently; take it.
                    rx.await.expect("queue consumer dropped without resolution")
                }
            }
        }
    }

    /// Drains and returns all currently-queued values in FIFO order (rejections
    /// are skipped). Port of `drain`.
    pub fn drain(&self) -> Vec<T> {
        let mut inner = self.inner.lock().unwrap();
        inner
            .produced
            .drain(..)
            .filter_map(|p| match p {
                Produced::Value(v) => Some(v),
                Produced::Rejection(_) => None,
            })
            .collect()
    }

    /// Returns an async iterator over dequeued values, invoking `cleanup` once
    /// when iteration ends (drop, error, or break). Port of
    /// `asAsyncIterable`/`asAsyncIterator`.
    pub fn iter(&self, cleanup: impl FnOnce() + Send + 'static) -> QueueIter<T, E> {
        QueueIter {
            queue: self.clone(),
            cleanup: Some(Box::new(cleanup)),
        }
    }
}

impl<T: PartialEq, E> Queue<T, E> {
    /// Deletes all queued values equal to `value`, returning the count. Port of
    /// `delete`.
    pub fn delete(&self, value: &T) -> usize {
        let mut inner = self.inner.lock().unwrap();
        let mut kept = VecDeque::with_capacity(inner.produced.len());
        let mut count = 0;
        for p in inner.produced.drain(..) {
            match p {
                Produced::Value(v) if &v == value => count += 1,
                other => kept.push_back(other),
            }
        }
        inner.produced = kept;
        count
    }
}

/// Async iterator over a [`Queue`]. Port of the `asAsyncIterator` result.
pub struct QueueIter<T, E> {
    queue: Queue<T, E>,
    cleanup: Option<Box<dyn FnOnce() + Send>>,
}

impl<T, E> QueueIter<T, E> {
    /// Awaits the next value, or returns the rejection. The queue is logically
    /// infinite, so this never signals normal completion; iteration ends when
    /// the caller stops (dropping the iterator runs `cleanup`).
    pub async fn next(&mut self) -> Result<T, E> {
        self.queue.dequeue().await
    }
}

impl<T, E> Drop for QueueIter<T, E> {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn dequeues_enqueued_value() {
        let q: Queue<&str> = Queue::new();
        assert_eq!(q.size(), 0);
        q.enqueue("foo");
        assert_eq!(q.size(), 1);
        assert_eq!(q.dequeue().await, Ok("foo"));
        assert_eq!(q.size(), 0);
    }

    #[tokio::test]
    async fn dequeues_enqueued_rejection() {
        let q: Queue<&str, &str> = Queue::new();
        q.enqueue_rejection("bar");
        assert_eq!(q.size(), 1);
        assert_eq!(q.dequeue().await, Err("bar"));
        assert_eq!(q.size(), 0);
    }

    #[tokio::test]
    async fn supports_enqueues_after_dequeue() {
        let q: Queue<&str, &str> = Queue::new();
        let v1 = {
            let q = q.clone();
            tokio::spawn(async move { q.dequeue().await })
        };
        // Ensure the consumer is parked before enqueueing.
        tokio::task::yield_now().await;
        q.enqueue("a");
        assert_eq!(v1.await.unwrap(), Ok("a"));

        q.enqueue_rejection("b");
        assert_eq!(q.dequeue().await, Err("b"));
        q.enqueue("c");
        assert_eq!(q.dequeue().await, Ok("c"));
    }

    #[tokio::test]
    async fn dequeues_timed_out_value() {
        let q: Queue<&str> = Queue::new();
        assert_eq!(
            q.dequeue_or("timed out", Duration::from_millis(5)).await,
            Ok("timed out")
        );
        q.enqueue("a");
        q.enqueue("b");
        assert_eq!(q.dequeue().await, Ok("a"));
        assert_eq!(q.dequeue().await, Ok("b"));
        assert_eq!(q.size(), 0);
    }

    #[tokio::test]
    async fn deletes_enqueued_values() {
        let q: Queue<&str> = Queue::new();
        for v in ["b", "a", "c", "b", "b", "d", "b"] {
            q.enqueue(v);
        }
        assert_eq!(q.size(), 7);
        assert_eq!(q.delete(&"b"), 4);
        assert_eq!(q.size(), 3);
        assert_eq!(q.delete(&"b"), 0);
        assert_eq!(q.dequeue().await, Ok("a"));
        assert_eq!(q.dequeue().await, Ok("c"));
        assert_eq!(q.dequeue().await, Ok("d"));
        assert_eq!(q.size(), 0);
    }

    #[tokio::test]
    async fn iterator_cleanup_on_break() {
        let cleaned = Arc::new(AtomicBool::new(false));
        let c = Arc::clone(&cleaned);
        let q: Queue<&str> = Queue::new();
        q.enqueue("foo");
        q.enqueue("bar");
        q.enqueue("baz");

        let mut received = Vec::new();
        {
            let mut it = q.iter(move || c.store(true, Ordering::SeqCst));
            loop {
                received.push(it.next().await.unwrap());
                if received.len() == 3 {
                    break;
                }
            }
        }
        assert!(cleaned.load(Ordering::SeqCst));
        assert_eq!(received, vec!["foo", "bar", "baz"]);
    }

    #[tokio::test]
    async fn iterator_cleanup_on_enqueued_rejection() {
        let cleaned = Arc::new(AtomicBool::new(false));
        let c = Arc::clone(&cleaned);
        let q: Queue<&str, &str> = Queue::new();
        q.enqueue("foo");
        q.enqueue("bar");
        q.enqueue_rejection("bonk");

        let mut received = Vec::new();
        let err = {
            let mut it = q.iter(move || c.store(true, Ordering::SeqCst));
            loop {
                match it.next().await {
                    Ok(v) => received.push(v),
                    Err(e) => break e,
                }
            }
        };
        assert!(cleaned.load(Ordering::SeqCst));
        assert_eq!(received, vec!["foo", "bar"]);
        assert_eq!(err, "bonk");
    }

    #[tokio::test]
    async fn consumer_blocks_until_available_then_drain() {
        let q: Queue<i32> = Queue::new();
        let q2 = q.clone();
        let handle = tokio::spawn(async move {
            let head = q2.dequeue().await.unwrap();
            let rest = q2.drain();
            (head, rest)
        });
        tokio::task::yield_now().await;
        q.enqueue(1);
        q.enqueue(2);
        q.enqueue(3);
        let (head, rest) = handle.await.unwrap();
        assert_eq!(head, 1);
        assert_eq!(rest, vec![2, 3]);
    }

    #[test]
    fn large_queue_drains_efficiently() {
        let q: Queue<usize> = Queue::new();
        let n = 100_000usize;
        for i in 0..n {
            q.enqueue(i);
        }
        assert_eq!(q.size(), n);
        let mut sum = 0usize;
        for _ in 0..n {
            sum += q.try_dequeue().unwrap().unwrap();
        }
        assert_eq!(sum, n * (n - 1) / 2);
        assert_eq!(q.size(), 0);
    }
}
