//! Port of `zero-cache/src/types/subscription.ts`.
//!
//! A [`Subscription`] is a continuous, logically-infinite stream of messages
//! for serial processing, with optional coalescing, per-message and
//! per-subscription cleanup, and an optional pipelined consumption mode.
//!
//! The TypeScript version relies on JavaScript's single-threaded, run-to-
//! completion model. Here the shared state lives behind a `Mutex`; producer
//! methods (`push`, `end`, `cancel`, `fail`) are synchronous and never await
//! while holding the lock, and consumers await on oneshot channels released
//! before the await point. Per-message result promises (`resolver()` in the
//! original) become `oneshot` channels; the "resolve at most once" semantics
//! fall out of `oneshot`'s single-send nature plus `Option::take`.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

/// Post-queueing result of a pushed message. Port of `Result`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushResult {
    Consumed,
    Coalesced,
    Unconsumed,
}

/// The error type carried by a failed subscription. `Arc` so it can be cloned
/// to every waiting consumer.
pub type SubError = Arc<dyn std::error::Error + Send + Sync>;

/// Options for constructing a [`Subscription`]. Port of `Options<M>`. Fields
/// left as `None` use the same defaults as the TS source.
#[allow(clippy::type_complexity)]
pub struct Options<M> {
    /// Coalesces the pending message with a newly-pushed one: `(curr, prev) -> M`.
    pub coalesce: Option<Box<dyn Fn(M, M) -> M + Send + Sync>>,
    /// Called with the previous message when the next is requested / on exit.
    pub consumed: Option<Box<dyn Fn(&M) + Send + Sync>>,
    /// Called once on termination with all unconsumed messages.
    pub cleanup: Option<Box<dyn Fn(Vec<M>, Option<SubError>) + Send + Sync>>,
    /// Explicit pipeline enable/disable; defaults to `coalesce.is_none()`.
    pub pipeline: Option<bool>,
}

impl<M> Default for Options<M> {
    fn default() -> Self {
        Options {
            coalesce: None,
            consumed: None,
            cleanup: None,
            pipeline: None,
        }
    }
}

#[derive(Clone)]
enum Sentinel {
    Canceled,
    Failed(SubError),
}

struct Entry<M> {
    value: M,
    resolve: Option<oneshot::Sender<PushResult>>,
}

enum Slot {
    Msg(u64),
    Terminus,
}

enum ConsumerMsg {
    Deliver(u64),
    Done,
    Fail(SubError),
}

struct Inner<M> {
    slab: HashMap<u64, Entry<M>>,
    messages: VecDeque<Slot>,
    /// Dequeued-but-not-yet-consumed entry ids, in delivery order.
    consuming: Vec<u64>,
    consumers: VecDeque<oneshot::Sender<ConsumerMsg>>,
    sentinel: Option<Sentinel>,
    next_id: u64,
}

impl<M> Inner<M> {
    fn alloc_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

struct Callbacks<T, M> {
    coalesce: Option<Box<dyn Fn(M, M) -> M + Send + Sync>>,
    consumed: Box<dyn Fn(&M) + Send + Sync>,
    cleanup: Box<dyn Fn(Vec<M>, Option<SubError>) + Send + Sync>,
    publish: Box<dyn Fn(&M) -> T + Send + Sync>,
    pipeline_enabled: bool,
}

/// A subscription. Cloning shares the same underlying state. Port of
/// `Subscription<T, M>`.
pub struct Subscription<T, M> {
    inner: Arc<Mutex<Inner<M>>>,
    cb: Arc<Callbacks<T, M>>,
}

impl<T, M> Clone for Subscription<T, M> {
    fn clone(&self) -> Self {
        Subscription {
            inner: Arc::clone(&self.inner),
            cb: Arc::clone(&self.cb),
        }
    }
}

/// The pending result of a [`Subscription::push`]. Port of `PendingResult`.
pub struct PendingResult(oneshot::Receiver<PushResult>);

impl PendingResult {
    /// Awaits the eventual [`PushResult`]. If the subscription is dropped
    /// without resolving (sender gone), resolves to `Unconsumed`.
    pub async fn result(self) -> PushResult {
        self.0.await.unwrap_or(PushResult::Unconsumed)
    }
}

impl<T, M> Subscription<T, M>
where
    T: Send + 'static,
    M: Clone + Send + 'static,
{
    /// Constructs a subscription with an explicit `publish` mapping `&M -> T`.
    /// Port of the `Subscription` constructor.
    pub fn new(options: Options<M>, publish: impl Fn(&M) -> T + Send + Sync + 'static) -> Self {
        let pipeline_enabled = options.pipeline.unwrap_or(options.coalesce.is_none());
        let consumed = options.consumed.unwrap_or_else(|| Box::new(|_| {}));
        let cleanup = options.cleanup.unwrap_or_else(|| Box::new(|_, _| {}));
        Subscription {
            inner: Arc::new(Mutex::new(Inner {
                slab: HashMap::new(),
                messages: VecDeque::new(),
                consuming: Vec::new(),
                consumers: VecDeque::new(),
                sentinel: None,
                next_id: 0,
            })),
            cb: Arc::new(Callbacks {
                coalesce: options.coalesce,
                consumed,
                cleanup,
                publish: Box::new(publish),
                pipeline_enabled,
            }),
        }
    }

    /// Pushes the next message; returns a [`PendingResult`]. Port of `push`.
    pub fn push(&self, value: M) -> PendingResult {
        let (tx, rx) = oneshot::channel();
        let mut inner = self.inner.lock().unwrap();

        if inner.sentinel.is_some() {
            let _ = tx.send(PushResult::Unconsumed);
            return PendingResult(rx);
        }

        if let Some(consumer) = inner.consumers.pop_front() {
            let id = inner.alloc_id();
            inner.slab.insert(
                id,
                Entry {
                    value,
                    resolve: Some(tx),
                },
            );
            let _ = consumer.send(ConsumerMsg::Deliver(id));
            return PendingResult(rx);
        }

        let can_coalesce =
            self.cb.coalesce.is_some() && matches!(inner.messages.back(), Some(Slot::Msg(_)));
        if can_coalesce {
            let last_id = match inner.messages.back() {
                Some(Slot::Msg(id)) => *id,
                _ => unreachable!(),
            };
            let mut prev = inner.slab.remove(&last_id).unwrap();
            let coalesce = self.cb.coalesce.as_ref().unwrap();
            let coalesced = coalesce(value, prev.value);
            if let Some(ptx) = prev.resolve.take() {
                let _ = ptx.send(PushResult::Coalesced);
            }
            let id = inner.alloc_id();
            inner.slab.insert(
                id,
                Entry {
                    value: coalesced,
                    resolve: Some(tx),
                },
            );
            *inner.messages.back_mut().unwrap() = Slot::Msg(id);
        } else {
            let id = inner.alloc_id();
            inner.slab.insert(
                id,
                Entry {
                    value,
                    resolve: Some(tx),
                },
            );
            inner.messages.push_back(Slot::Msg(id));
        }
        PendingResult(rx)
    }

    /// False if the subscription has been canceled or has failed. Port of `active`.
    pub fn active(&self) -> bool {
        self.inner.lock().unwrap().sentinel.is_none()
    }

    /// Number of messages waiting to be dequeued. Port of `queued`.
    pub fn queued(&self) -> usize {
        self.inner.lock().unwrap().messages.len()
    }

    /// Number of messages dequeued but not yet consumed. Port of `consuming`.
    pub fn consuming(&self) -> usize {
        self.inner.lock().unwrap().consuming.len()
    }

    /// Cancels once queued messages are consumed. Port of `end`.
    pub fn end(&self) {
        let mut inner = self.inner.lock().unwrap();
        if inner.sentinel.is_some() {
            // already terminated
        } else if inner.messages.is_empty() {
            drop(inner);
            self.cancel();
        } else {
            inner.messages.push_back(Slot::Terminus);
        }
    }

    /// Cancels the subscription immediately (graceful). Port of `cancel()`.
    pub fn cancel(&self) {
        let mut inner = self.inner.lock().unwrap();
        self.terminate_locked(&mut inner, Sentinel::Canceled);
    }

    /// Cancels the subscription with an error, thrown from any iteration. Port
    /// of `cancel(err)` / `fail`.
    pub fn fail(&self, err: SubError) {
        let mut inner = self.inner.lock().unwrap();
        self.terminate_locked(&mut inner, Sentinel::Failed(err));
    }

    /// Shared dequeue step used by both iteration modes. Port of `#pipeline().next`.
    async fn pipeline_next(&self) -> Step<T> {
        enum Local<T> {
            Ready(Step<T>),
            Terminus,
            Wait(oneshot::Receiver<ConsumerMsg>),
        }

        let local = {
            let mut inner = self.inner.lock().unwrap();
            match inner.messages.pop_front() {
                Some(Slot::Terminus) => Local::Terminus,
                Some(Slot::Msg(id)) => {
                    inner.consuming.push(id);
                    let value = (self.cb.publish)(&inner.slab[&id].value);
                    Local::Ready(Step::Item(value, id))
                }
                None => match &inner.sentinel {
                    Some(Sentinel::Canceled) => Local::Ready(Step::Done),
                    Some(Sentinel::Failed(e)) => Local::Ready(Step::Failed(e.clone())),
                    None => {
                        let (tx, rx) = oneshot::channel();
                        inner.consumers.push_back(tx);
                        Local::Wait(rx)
                    }
                },
            }
        };

        match local {
            Local::Ready(step) => step,
            Local::Terminus => {
                self.cancel();
                Step::Done
            }
            Local::Wait(rx) => match rx.await {
                Ok(ConsumerMsg::Deliver(id)) => {
                    let mut inner = self.inner.lock().unwrap();
                    inner.consuming.push(id);
                    let value = (self.cb.publish)(&inner.slab[&id].value);
                    Step::Item(value, id)
                }
                Ok(ConsumerMsg::Done) | Err(_) => Step::Done,
                Ok(ConsumerMsg::Fail(e)) => Step::Failed(e),
            },
        }
    }

    /// Returns a serial async iterator over published values. Each call to
    /// [`Iter::next`] consumes the previously-yielded message. Dropping the
    /// iterator cancels the subscription. Port of `[Symbol.asyncIterator]`.
    pub fn iter(&self) -> Iter<T, M> {
        Iter {
            sub: self.clone(),
            prev_consumed: None,
        }
    }

    /// Returns the pipelined iterator, or `None` if pipelining is disabled. Port
    /// of the `pipeline` getter.
    pub fn pipeline(&self) -> Option<Pipeline<T, M>> {
        if self.cb.pipeline_enabled {
            Some(Pipeline { sub: self.clone() })
        } else {
            None
        }
    }
}

/// Termination and consumption logic, which needs only `M: Clone` (called from
/// the iterator `Drop` impls, which cannot carry the stronger bounds).
impl<T, M: Clone> Subscription<T, M> {
    fn terminate_locked(&self, inner: &mut Inner<M>, sentinel: Sentinel) {
        if inner.sentinel.is_some() {
            return;
        }
        inner.sentinel = Some(sentinel.clone());

        // Gather unconsumed: consuming (in order) then queued messages.
        let mut ids: Vec<u64> = inner.consuming.clone();
        for slot in &inner.messages {
            if let Slot::Msg(id) = slot {
                ids.push(*id);
            }
        }
        let values: Vec<M> = ids
            .iter()
            .filter_map(|id| inner.slab.get(id).map(|e| e.value.clone()))
            .collect();
        let err = match &sentinel {
            Sentinel::Failed(e) => Some(e.clone()),
            Sentinel::Canceled => None,
        };
        (self.cb.cleanup)(values, err);
        for id in &ids {
            if let Some(e) = inner.slab.get_mut(id) {
                if let Some(tx) = e.resolve.take() {
                    let _ = tx.send(PushResult::Unconsumed);
                }
            }
        }
        inner.messages.clear();

        while let Some(consumer) = inner.consumers.pop_front() {
            let _ = match &sentinel {
                Sentinel::Canceled => consumer.send(ConsumerMsg::Done),
                Sentinel::Failed(e) => consumer.send(ConsumerMsg::Fail(e.clone())),
            };
        }
    }

    /// Marks a dequeued entry as consumed: runs the `consumed` callback and
    /// resolves its result to `Consumed`. Port of `#consumed`.
    fn consumed(&self, id: u64) {
        let entry = {
            let mut inner = self.inner.lock().unwrap();
            inner.consuming.retain(|x| *x != id);
            inner.slab.remove(&id)
        };
        if let Some(mut entry) = entry {
            (self.cb.consumed)(&entry.value);
            if let Some(tx) = entry.resolve.take() {
                let _ = tx.send(PushResult::Consumed);
            }
        }
    }
}

enum Step<T> {
    Item(T, u64),
    Done,
    Failed(SubError),
}

/// Serial async iterator over a [`Subscription`]. Port of the default
/// `AsyncIterator`.
pub struct Iter<T, M: Clone> {
    sub: Subscription<T, M>,
    prev_consumed: Option<u64>,
}

impl<T, M> Iter<T, M>
where
    T: Send + 'static,
    M: Clone + Send + 'static,
{
    /// Returns the next value, `None` when the stream ends gracefully, or an
    /// error if the subscription failed. Consumes the previously-yielded
    /// message first.
    pub async fn next(&mut self) -> Option<Result<T, SubError>> {
        if let Some(id) = self.prev_consumed.take() {
            self.sub.consumed(id);
        }
        match self.sub.pipeline_next().await {
            Step::Item(value, id) => {
                self.prev_consumed = Some(id);
                Some(Ok(value))
            }
            Step::Done => None,
            Step::Failed(e) => Some(Err(e)),
        }
    }
}

impl<T, M: Clone> Drop for Iter<T, M> {
    fn drop(&mut self) {
        // Mirror `return()`: consume the last message, then cancel.
        if let Some(id) = self.prev_consumed.take() {
            // consumed() needs the typed impl; re-lock manually.
            let entry = {
                let mut inner = self.sub.inner.lock().unwrap();
                inner.consuming.retain(|x| *x != id);
                inner.slab.remove(&id)
            };
            if let Some(mut entry) = entry {
                (self.sub.cb.consumed)(&entry.value);
                if let Some(tx) = entry.resolve.take() {
                    let _ = tx.send(PushResult::Consumed);
                }
            }
        }
        let mut inner = self.sub.inner.lock().unwrap();
        self.sub.terminate_locked(&mut inner, Sentinel::Canceled);
    }
}

/// An item yielded by a [`Pipeline`]: the published value plus the id used to
/// signal consumption via [`Pipeline::consumed`].
pub struct PipeItem<T> {
    pub value: T,
    pub id: u64,
}

/// Pipelined async iterator. Unlike [`Iter`], the consumer must explicitly call
/// [`Pipeline::consumed`] for each item. Port of the `pipeline` iterator.
pub struct Pipeline<T, M: Clone> {
    sub: Subscription<T, M>,
}

impl<T, M> Pipeline<T, M>
where
    T: Send + 'static,
    M: Clone + Send + 'static,
{
    /// Returns the next item, `None` on graceful end, or an error on failure.
    pub async fn next(&mut self) -> Option<Result<PipeItem<T>, SubError>> {
        match self.sub.pipeline_next().await {
            Step::Item(value, id) => Some(Ok(PipeItem { value, id })),
            Step::Done => None,
            Step::Failed(e) => Some(Err(e)),
        }
    }

    /// Signals that the item with `id` has been fully consumed.
    pub fn consumed(&self, id: u64) {
        self.sub.consumed(id);
    }
}

impl<T, M: Clone> Drop for Pipeline<T, M> {
    fn drop(&mut self) {
        let mut inner = self.sub.inner.lock().unwrap();
        self.sub.terminate_locked(&mut inner, Sentinel::Canceled);
    }
}

/// Convenience constructor mirroring `Subscription.create`, where `T == M` and
/// the default `publish` clones the message.
pub fn create<M>(options: Options<M>) -> Subscription<M, M>
where
    M: Clone + Send + 'static,
{
    Subscription::new(options, |m: &M| m.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    /// Shared recorder for `consumed`/`cleanup` callbacks used by the tests.
    struct Recorder<M> {
        consumed: Vec<M>,
        cleanup_calls: Vec<Vec<M>>,
    }

    #[allow(clippy::type_complexity)]
    fn recorder<M: Clone + Send + 'static>() -> (
        Arc<StdMutex<Recorder<M>>>,
        Box<dyn Fn(&M) + Send + Sync>,
        Box<dyn Fn(Vec<M>, Option<SubError>) + Send + Sync>,
    ) {
        let rec = Arc::new(StdMutex::new(Recorder::<M> {
            consumed: Vec::new(),
            cleanup_calls: Vec::new(),
        }));
        let r1 = Arc::clone(&rec);
        let consumed = Box::new(move |m: &M| r1.lock().unwrap().consumed.push(m.clone()));
        let r2 = Arc::clone(&rec);
        let cleanup = Box::new(move |ms: Vec<M>, _e: Option<SubError>| {
            r2.lock().unwrap().cleanup_calls.push(ms)
        });
        (rec, consumed, cleanup)
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn end() {
        let (rec, consumed, cleanup) = recorder::<i64>();
        let sub = create(Options {
            consumed: Some(consumed),
            cleanup: Some(cleanup),
            ..Default::default()
        });
        let mut results = Vec::new();
        for i in 0..5 {
            results.push(sub.push(i));
        }

        let mut received = Vec::new();
        let mut j = 0;
        let mut it = sub.iter();
        while let Some(m) = it.next().await {
            let m = m.unwrap();
            received.push(m);
            if j == 2 {
                assert!(sub.active());
                sub.end();
                assert!(sub.active());
            }
            j += 1;
        }
        drop(it);
        assert!(!sub.active());
        assert_eq!(received, vec![0, 1, 2, 3, 4]);
        assert_eq!(rec.lock().unwrap().consumed, vec![0, 1, 2, 3, 4]);
        let cc = &rec.lock().unwrap().cleanup_calls;
        assert_eq!(cc.len(), 1);
        assert_eq!(cc[0], Vec::<i64>::new());

        // Drain the per-push results.
        for r in results.drain(..) {
            assert_eq!(r.result().await, PushResult::Consumed);
        }
        assert_eq!(sub.push(6).result().await, PushResult::Unconsumed);
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn cancel_non_pipelined() {
        let (rec, consumed, cleanup) = recorder::<i64>();
        let sub = create(Options {
            consumed: Some(consumed),
            cleanup: Some(cleanup),
            ..Default::default()
        });
        let mut results = Vec::new();
        for i in 0..5 {
            results.push(sub.push(i));
        }

        let mut received = Vec::new();
        let mut j = 0;
        let mut it = sub.iter();
        while let Some(m) = it.next().await {
            let m = m.unwrap();
            received.push(m);
            if j == 2 {
                assert!(sub.active());
                sub.cancel();
                assert!(!sub.active());
            }
            j += 1;
        }
        drop(it);
        assert_eq!(received, vec![0, 1, 2]);
        assert_eq!(rec.lock().unwrap().consumed, vec![0, 1, 2]);
        let cc = &rec.lock().unwrap().cleanup_calls;
        assert_eq!(cc.len(), 1);
        assert_eq!(cc[0], vec![2, 3, 4]);

        let r3 = results.remove(3);
        assert_eq!(r3.result().await, PushResult::Unconsumed);
    }

    #[tokio::test]
    async fn iteration_break() {
        let (rec, consumed, cleanup) = recorder::<i64>();
        let sub = create(Options {
            consumed: Some(consumed),
            cleanup: Some(cleanup),
            ..Default::default()
        });
        for i in 0..5 {
            sub.push(i);
        }

        let mut received = Vec::new();
        let mut j = 0;
        {
            let mut it = sub.iter();
            while let Some(m) = it.next().await {
                received.push(m.unwrap());
                if j == 2 {
                    assert!(sub.active());
                    break;
                }
                j += 1;
            }
            // it dropped here -> return() semantics.
        }
        assert!(!sub.active());
        assert_eq!(received, vec![0, 1, 2]);
        assert_eq!(rec.lock().unwrap().consumed, vec![0, 1, 2]);
        let cc = &rec.lock().unwrap().cleanup_calls;
        assert_eq!(cc.len(), 1);
        assert_eq!(cc[0], vec![3, 4]);
    }

    #[tokio::test]
    async fn coalesce_cancel() {
        let (rec, consumed, cleanup) = recorder::<String>();
        let sub: Subscription<String, String> = Subscription::new(
            Options {
                consumed: Some(consumed),
                cleanup: Some(cleanup),
                coalesce: Some(Box::new(|curr: String, prev: String| {
                    format!("{prev},{curr}")
                })),
                ..Default::default()
            },
            |m: &String| m.clone(),
        );
        let r0 = sub.push("a".into());
        let r1 = sub.push("b".into());

        let mut received: Vec<String> = Vec::new();
        let mut i = 0;
        let mut extra: Vec<PendingResult> = Vec::new();
        {
            let mut it = sub.iter();
            while let Some(m) = it.next().await {
                received.push(m.unwrap());
                if i == 0 {
                    extra.push(sub.push("c".into()));
                    extra.push(sub.push("d".into()));
                } else {
                    sub.cancel();
                }
                i += 1;
            }
        }
        assert_eq!(received, vec!["a,b".to_string(), "c,d".to_string()]);
        assert_eq!(r0.result().await, PushResult::Coalesced);
        assert_eq!(r1.result().await, PushResult::Consumed);
        assert_eq!(extra.remove(0).result().await, PushResult::Coalesced); // c
        assert_eq!(extra.remove(0).result().await, PushResult::Unconsumed); // d
        assert_eq!(
            rec.lock().unwrap().consumed,
            vec!["a,b".to_string(), "c,d".to_string()]
        );
        let cc = &rec.lock().unwrap().cleanup_calls;
        assert_eq!(cc.len(), 1);
        assert_eq!(cc[0], vec!["c,d".to_string()]);
    }

    #[tokio::test]
    async fn coalesce_end() {
        let (rec, consumed, cleanup) = recorder::<String>();
        let sub: Subscription<String, String> = Subscription::new(
            Options {
                consumed: Some(consumed),
                cleanup: Some(cleanup),
                coalesce: Some(Box::new(|curr: String, prev: String| {
                    format!("{prev},{curr}")
                })),
                ..Default::default()
            },
            |m: &String| m.clone(),
        );
        let r0 = sub.push("a".into());
        let r1 = sub.push("b".into());

        let mut received: Vec<String> = Vec::new();
        let mut i = 0;
        let mut extra: Vec<PendingResult> = Vec::new();
        {
            let mut it = sub.iter();
            while let Some(m) = it.next().await {
                received.push(m.unwrap());
                if i == 0 {
                    extra.push(sub.push("c".into()));
                    extra.push(sub.push("d".into()));
                    sub.end();
                    extra.push(sub.push("e".into()));
                    extra.push(sub.push("f".into()));
                }
                i += 1;
            }
        }
        assert_eq!(received, vec!["a,b".to_string(), "c,d".to_string()]);
        assert_eq!(r0.result().await, PushResult::Coalesced);
        assert_eq!(r1.result().await, PushResult::Consumed);
        assert_eq!(extra.remove(0).result().await, PushResult::Coalesced); // c
        assert_eq!(extra.remove(0).result().await, PushResult::Consumed); // d
        assert_eq!(extra.remove(0).result().await, PushResult::Coalesced); // e
        assert_eq!(extra.remove(0).result().await, PushResult::Unconsumed); // f
        let cc = &rec.lock().unwrap().cleanup_calls;
        assert_eq!(cc[0], vec!["e,f".to_string()]);
    }

    #[tokio::test]
    async fn publish_different_type() {
        #[derive(Clone, Debug, PartialEq)]
        struct Internal {
            foo: i64,
            bar: &'static str,
        }
        let (rec, _c, cleanup) = recorder::<Internal>();
        let consumed_foo: Arc<StdMutex<Vec<i64>>> = Arc::new(StdMutex::new(Vec::new()));
        let cf = Arc::clone(&consumed_foo);
        let sub: Subscription<i64, Internal> = Subscription::new(
            Options {
                consumed: Some(Box::new(move |m: &Internal| cf.lock().unwrap().push(m.foo))),
                cleanup: Some(cleanup),
                ..Default::default()
            },
            |m: &Internal| m.foo,
        );
        for i in 0..5 {
            sub.push(Internal {
                foo: i,
                bar: "internal",
            });
        }

        let mut received = Vec::new();
        let mut j = 0;
        {
            let mut it = sub.iter();
            while let Some(m) = it.next().await {
                received.push(m.unwrap());
                if j == 2 {
                    sub.cancel();
                }
                j += 1;
            }
        }
        assert_eq!(received, vec![0, 1, 2]);
        assert_eq!(*consumed_foo.lock().unwrap(), vec![0, 1, 2]);
        let cc = &rec.lock().unwrap().cleanup_calls;
        assert_eq!(
            cc[0],
            vec![
                Internal {
                    foo: 2,
                    bar: "internal"
                },
                Internal {
                    foo: 3,
                    bar: "internal"
                },
                Internal {
                    foo: 4,
                    bar: "internal"
                },
            ]
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn pipelining() {
        let (rec, consumed, cleanup) = recorder::<i64>();
        let sub = create(Options {
            consumed: Some(consumed),
            cleanup: Some(cleanup),
            ..Default::default()
        });
        let mut results = Vec::new();
        for i in 0..5 {
            results.push(sub.push(i));
        }

        let mut pipeline = sub.pipeline().expect("pipeline defined");
        let mut received: Vec<PipeItem<i64>> = Vec::new();
        let mut j = 0;
        while let Some(item) = pipeline.next().await {
            received.push(item.unwrap());
            if j == 2 {
                for it in received.iter() {
                    pipeline.consumed(it.id);
                }
                assert!(sub.active());
                sub.cancel();
                assert!(!sub.active());
                break;
            }
            j += 1;
        }
        drop(pipeline);

        let values: Vec<i64> = received.iter().map(|r| r.value).collect();
        assert_eq!(values, vec![0, 1, 2]);
        assert_eq!(rec.lock().unwrap().consumed, vec![0, 1, 2]);
        let cc = &rec.lock().unwrap().cleanup_calls;
        assert_eq!(cc[0], vec![3, 4]);
        let r3 = results.remove(3);
        assert_eq!(r3.result().await, PushResult::Unconsumed);
    }

    #[tokio::test]
    async fn pushed_while_iterating() {
        use std::time::Duration;

        let (rec, consumed, cleanup) = recorder::<i64>();
        let sub = create(Options {
            consumed: Some(consumed),
            cleanup: Some(cleanup),
            ..Default::default()
        });
        let received = Arc::new(StdMutex::new(Vec::<i64>::new()));

        // Start iterating before any messages exist: exercises the
        // consumer-waiting path (push delivers directly to a parked consumer).
        let sub2 = sub.clone();
        let recv2 = Arc::clone(&received);
        let iteration = tokio::spawn(async move {
            let mut j = 0;
            let mut it = sub2.iter();
            while let Some(m) = it.next().await {
                recv2.lock().unwrap().push(m.unwrap());
                if j == 2 {
                    sub2.cancel();
                }
                j += 1;
            }
        });

        for i in 0..5 {
            tokio::time::sleep(Duration::from_millis(2)).await;
            sub.push(i);
        }
        iteration.await.unwrap();

        assert_eq!(*received.lock().unwrap(), vec![0, 1, 2]);
        assert_eq!(rec.lock().unwrap().consumed, vec![0, 1, 2]);
        assert_eq!(rec.lock().unwrap().cleanup_calls.len(), 1);
        assert_eq!(sub.push(6).result().await, PushResult::Unconsumed);
    }

    #[test]
    fn pipeline_defaults() {
        let no_coalesce = create::<String>(Options::default());
        assert!(no_coalesce.pipeline().is_some());

        let no_pipeline = create::<String>(Options {
            pipeline: Some(false),
            ..Default::default()
        });
        assert!(no_pipeline.pipeline().is_none());

        let with_coalesce = create::<String>(Options {
            coalesce: Some(Box::new(|c: String, p: String| format!("{p},{c}"))),
            ..Default::default()
        });
        assert!(with_coalesce.pipeline().is_none());

        let with_both = create::<String>(Options {
            coalesce: Some(Box::new(|c: String, p: String| format!("{p},{c}"))),
            pipeline: Some(true),
            ..Default::default()
        });
        assert!(with_both.pipeline().is_some());
    }
}
