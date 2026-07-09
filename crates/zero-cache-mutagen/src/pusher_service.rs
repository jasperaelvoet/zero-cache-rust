//! Port of `pusher.ts`'s `PusherService`/`PushWorker` — the `Queue`-based
//! drain loop and RPC surface that was the last unported piece of
//! pusher.ts. Every dependency this needed already exists in this port:
//! `pusher_batch::combine_pushes` (batching), `api_request`/`api_fetch`
//! (real HTTP), `pusher_response::find_fatal_terminations` (response
//! classification), and `zero-cache-view-syncer::ConnectionContextManager`
//! (connection lifecycle) — this module is the assembly, not new logic.
//!
//! Scope: ports `PushWorker#run`'s actual loop (`dequeue` -> `drain` ->
//! `combinePushes` -> process each combined push in order) and
//! `initConnection`/`enqueuePush`'s registration semantics faithfully.
//! Deliberately generic over the mutation type `M` and the actual
//! push-sending logic (`process_push: FnMut(PusherEntry<M>) -> impl
//! Future<Output = R>`) rather than hard-wiring `api_fetch::
//! fetch_from_api_server` and a concrete `MutateResponse` type — this
//! crate's mutation/response wire types (`MutateResponse`,
//! `mutateResponseSchema`) aren't ported to `zero-cache-protocol` yet (see
//! `pusher_response.rs`'s module doc), so `PushWorker#processPush`'s
//! HTTP-call-plus-response-interpretation body is left to the caller to
//! supply, exactly as `pusher_batch.rs` already made mutations themselves
//! generic for the same reason. What's ported here is the part that's
//! genuinely this module's job: the queue/batching/ordering machinery
//! around that call.
//!
//! NOT ported: `initConnection`'s `Subscription<Downstream>` (no real
//! WebSocket downstream in this port yet — connections are tracked here as
//! a plain `HashMap<clientID, wsID>`, just enough to reproduce the
//! "already initialized for this socket" / "client reconnected, replace
//! it" semantics `PushWorker#initConnection` implements), `#fanOutResponses`
//! (the caller's `process_push` return value is simply collected — see
//! `pusher_response.rs` for the termination-detection half a caller would
//! use on `R`), and `ref()`/`unref()`/`hasRefs()` refcounting (a
//! `RefCountedService` concern belonging to whatever owns a `PusherService`
//! per client group — this port has no service-lifecycle framework these
//! plug into yet).

use std::collections::HashMap;
use std::future::Future;

use zero_cache_shared::queue::Queue;

use crate::pusher_batch::{
    combine_pushes, ConnCtx, IncompatiblePushes, PushBody, PusherEntry, PusherEntryOrStop,
};

/// Port of `PushWorker#initConnection`'s "already initialized" /
/// "reconnected, replace" outcomes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InitConnectionResult {
    /// A fresh registration — no prior record for this `clientID`.
    Registered,
    /// The client reconnected under a new `wsID`; the old socket record was
    /// replaced (upstream cancels the old `Subscription` here — this port
    /// has none to cancel, so this variant just reports the fact).
    Reconnected { previous_ws_id: String },
}

/// Port of `PushWorker#initConnection`'s "already initialized for this
/// socket" throw.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Connection was already initialized")]
pub struct AlreadyInitialized;

/// Port of `PushWorker` — see module doc for exact scope.
pub struct PushWorker<M> {
    queue: Queue<PusherEntryOrStop<M>>,
    /// clientID -> wsID. Stands in for upstream's `#clients: Map<clientID,
    /// {wsID, downstream}>` — see module doc for why there's no
    /// `Subscription` here.
    clients: HashMap<String, String>,
}

impl<M: Clone> Default for PushWorker<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: Clone> PushWorker<M> {
    pub fn new() -> Self {
        PushWorker {
            queue: Queue::new(),
            clients: HashMap::new(),
        }
    }

    /// Port of `PushWorker#initConnection`.
    pub fn init_connection(
        &mut self,
        client_id: &str,
        ws_id: &str,
    ) -> Result<InitConnectionResult, AlreadyInitialized> {
        if let Some(existing_ws_id) = self.clients.get(client_id) {
            if existing_ws_id == ws_id {
                return Err(AlreadyInitialized);
            }
            let previous_ws_id = existing_ws_id.clone();
            self.clients
                .insert(client_id.to_string(), ws_id.to_string());
            return Ok(InitConnectionResult::Reconnected { previous_ws_id });
        }
        self.clients
            .insert(client_id.to_string(), ws_id.to_string());
        Ok(InitConnectionResult::Registered)
    }

    /// Port of `PushWorker#enqueuePush`.
    pub fn enqueue_push(&self, conn_ctx: ConnCtx, push: PushBody<M>) {
        self.queue
            .enqueue(PusherEntryOrStop::Entry(PusherEntry { conn_ctx, push }));
    }

    /// Port of `PusherService::stop` (the `#queue.enqueue('stop')` half —
    /// awaiting `#stopped` is just awaiting the `run()` future itself in
    /// this port, since there's no separate `RefCountedService` wrapper —
    /// see module doc).
    pub fn stop(&self) {
        self.queue.enqueue(PusherEntryOrStop::Stop);
    }

    /// Port of `PushWorker#run`'s loop body: `dequeue` one entry, `drain`
    /// whatever else is queued, `combinePushes` them, then hand each
    /// combined push to `process_push` in order — mirroring upstream's
    /// per-push `await` (mutations for a given connection are processed
    /// strictly in order; different connections' pushes may interleave
    /// across loop iterations exactly as upstream's queue-drain does).
    /// Returns every `process_push` result, in processing order, once a
    /// `stop()` sentinel is drained. `process_push` returning an error
    /// aborts the whole run (a batching/compatibility bug, matching
    /// `combinePushes`'s `assertAreCompatiblePushes` being an unreachable-
    /// invariant assertion upstream).
    pub async fn run<F, Fut, R>(
        &mut self,
        mut process_push: F,
    ) -> Result<Vec<R>, IncompatiblePushes>
    where
        F: FnMut(PusherEntry<M>) -> Fut,
        Fut: Future<Output = R>,
    {
        let mut results = Vec::new();
        loop {
            let task = self.queue.dequeue().await.ok();
            let rest = self.queue.drain();
            let mut entries: Vec<Option<PusherEntryOrStop<M>>> = Vec::with_capacity(1 + rest.len());
            entries.push(task);
            entries.extend(rest.into_iter().map(Some));

            let (pushes, terminate) = combine_pushes(&entries)?;
            for push in pushes {
                results.push(process_push(push).await);
            }

            if terminate {
                break;
            }
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pusher_batch::MutateContext;
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    fn ctx(client_id: &str, ws_id: &str) -> ConnCtx {
        ConnCtx {
            client_id: client_id.into(),
            ws_id: ws_id.into(),
            revision: "r1".into(),
            auth: None,
            user_id: "u1".into(),
            mutate_context: MutateContext {
                url: "https://api.example/push".into(),
                cookie: None,
                origin: None,
            },
        }
    }

    fn push(mutations: Vec<i32>) -> PushBody<i32> {
        PushBody {
            schema_version: Some(1.0),
            push_version: 1.0,
            mutations,
        }
    }

    #[test]
    fn init_connection_reports_fresh_registration() {
        let mut worker: PushWorker<i32> = PushWorker::new();
        assert_eq!(
            worker.init_connection("c1", "ws1"),
            Ok(InitConnectionResult::Registered)
        );
    }

    #[test]
    fn init_connection_same_socket_twice_errors() {
        let mut worker: PushWorker<i32> = PushWorker::new();
        worker.init_connection("c1", "ws1").unwrap();
        assert_eq!(worker.init_connection("c1", "ws1"), Err(AlreadyInitialized));
    }

    #[test]
    fn init_connection_new_socket_for_existing_client_reconnects() {
        let mut worker: PushWorker<i32> = PushWorker::new();
        worker.init_connection("c1", "ws1").unwrap();
        assert_eq!(
            worker.init_connection("c1", "ws2"),
            Ok(InitConnectionResult::Reconnected {
                previous_ws_id: "ws1".into()
            })
        );
    }

    #[tokio::test]
    async fn run_processes_a_single_push_and_stops() {
        let mut worker: PushWorker<i32> = PushWorker::new();
        worker.enqueue_push(ctx("c1", "ws1"), push(vec![1, 2]));
        worker.stop();

        let results = worker
            .run(|entry| async move { entry.push.mutations.clone() })
            .await
            .unwrap();
        assert_eq!(results, vec![vec![1, 2]]);
    }

    #[tokio::test]
    async fn run_batches_same_connection_pushes_queued_before_drain() {
        let mut worker: PushWorker<i32> = PushWorker::new();
        // Both enqueued before run() starts draining, so the first dequeue +
        // drain should see both and combine_pushes should merge them into
        // ONE call to process_push (proving real batching, not just
        // sequential processing).
        worker.enqueue_push(ctx("c1", "ws1"), push(vec![1]));
        worker.enqueue_push(ctx("c1", "ws1"), push(vec![2, 3]));
        worker.stop();

        let call_count = Arc::new(Mutex::new(0));
        let cc = call_count.clone();
        let results = worker
            .run(move |entry| {
                *cc.lock().unwrap() += 1;
                async move { entry.push.mutations.clone() }
            })
            .await
            .unwrap();

        assert_eq!(
            *call_count.lock().unwrap(),
            1,
            "same-connection pushes queued together should batch into one process_push call"
        );
        assert_eq!(results, vec![vec![1, 2, 3]]);
    }

    #[tokio::test]
    async fn run_keeps_different_connections_separate() {
        let mut worker: PushWorker<i32> = PushWorker::new();
        worker.enqueue_push(ctx("c1", "ws1"), push(vec![1]));
        worker.enqueue_push(ctx("c2", "ws1"), push(vec![2]));
        worker.stop();

        let mut results = worker
            .run(|entry| async move { entry.push.mutations.clone() })
            .await
            .unwrap();
        results.sort();
        assert_eq!(results, vec![vec![1], vec![2]]);
    }

    #[tokio::test]
    async fn run_with_no_pushes_stops_immediately() {
        let mut worker: PushWorker<i32> = PushWorker::new();
        worker.stop();
        let results = worker
            .run(|entry| async move { entry.push.mutations.clone() })
            .await
            .unwrap();
        assert!(results.is_empty());
    }

    /// Live proof: `run()`'s per-push processing actually drives a REAL
    /// HTTP POST via the already-ported `api_fetch::fetch_from_api_server`
    /// against a hand-rolled local server — no mocking. Two same-connection
    /// pushes enqueued before the loop starts should still land as exactly
    /// ONE real HTTP request (the batching proof, now end-to-end through a
    /// real network call instead of a synchronous closure).
    #[tokio::test]
    async fn run_drives_a_real_http_push_via_api_fetch() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let request_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let rc = request_count.clone();

        std::thread::spawn(move || {
            use std::io::{Read, Write};
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                rc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let body = r#"{"ok":true}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });

        let mut worker: PushWorker<i32> = PushWorker::new();
        worker.enqueue_push(ctx("c1", "ws1"), push(vec![1]));
        worker.enqueue_push(ctx("c1", "ws1"), push(vec![2]));
        worker.stop();

        let client = reqwest::Client::new();
        let url = format!("http://{addr}/push");
        let results = worker
            .run(|entry| {
                let client = client.clone();
                let url = url.clone();
                async move {
                    let headers = crate::api_request::HeaderOptions {
                        api_key: None,
                        custom_headers: &[],
                        request_headers: &[],
                        auth_raw: None,
                        cookie: None,
                        origin: None,
                    };
                    let body = serde_json::json!({"mutations": entry.push.mutations});
                    crate::api_fetch::fetch_from_api_server(
                        &client,
                        crate::api_fetch::ApiSource::Push,
                        &url,
                        "s",
                        "a",
                        &headers,
                        &body,
                    )
                    .await
                }
            })
            .await
            .unwrap();

        assert_eq!(
            results.len(),
            1,
            "the two enqueued-before-drain pushes should have batched into one process_push call"
        );
        assert!(results[0].is_ok());
        assert_eq!(
            request_count.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "batching should mean exactly one real HTTP request went out over the wire"
        );
    }
}
