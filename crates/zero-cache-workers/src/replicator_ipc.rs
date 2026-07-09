//! Port of `replicator.ts`'s `Worker`-IPC message-relay surface
//! (`setUpMessageHandlers`/`handleSubscriptionsFrom`/`createNotifierFrom`/
//! `subscribeTo`) — the half of `replicator.ts` this port didn't attempt
//! until the `Worker`/process-model decision (see `worker_message.rs`) was
//! actually made. Built on that decision (`tokio::spawn` tasks +
//! `WorkerSender`/`WorkerReceiver` channels) plus the already-ported
//! `zero-cache-services::notifier::Notifier`/`zero_cache_types::subscription`
//! machinery.
//!
//! Scope: this ports the RELAY logic — forwarding a `Notifier`'s
//! subscription stream out over a `WorkerSender`, and the inverse
//! (relaying inbound `WorkerReceiver` notifications into a local
//! `Notifier` for further fan-out) — faithfully, including the
//! subscribe-on-demand semantics (`handleSubscriptionsFrom` only starts
//! forwarding once a `'subscribe'` message arrives, matching upstream not
//! eagerly subscribing). NOT ported: the actual replication worker this
//! would run inside (needs the rest of `replicator.ts`'s `prepare_replica`
//! wiring plus a real replication-stream source), and error-code
//! classification for a dead IPC channel (`ERR_IPC_CHANNEL_CLOSED` —
//! Node-specific; this port's `WorkerSender::send` already surfaces a
//! plain `ChannelClosed` a caller can match on however it likes).

use zero_cache_services::notifier::{Notifier, ReplicaState};

use crate::worker_message::{WorkerReceiver, WorkerSender};

/// Port of `handleSubscriptionsFrom`: waits for a `'subscribe'` message on
/// `subscribe_rx`, then relays every notification from `notifier`'s
/// subscription out over `notify_tx` as `'notify'` messages, until the
/// subscription ends or `notify_tx`'s receiver is dropped. Runs until
/// `subscribe_rx` closes (port of the outer `onMessageType` handler
/// effectively running for the worker's lifetime — one subscription is
/// serviced per call, matching upstream's single subscriber-per-connection
/// pattern; a caller wanting multiple concurrent subscribers spawns this
/// once per subscriber's channel pair).
pub async fn handle_subscriptions_from(
    notifier: Notifier,
    notify_tx: WorkerSender<ReplicaState>,
    mut subscribe_rx: WorkerReceiver<()>,
) {
    if subscribe_rx.recv_type("subscribe").await.is_none() {
        return;
    }

    let subscription = notifier.subscribe();
    let mut iter = subscription.iter();
    while let Some(Ok(state)) = iter.next().await {
        if notify_tx.send("notify", state).is_err() {
            // The subscriber's receiver is gone — matches upstream's
            // dropped-`ERR_IPC_CHANNEL_CLOSED`-is-not-fatal handling,
            // except this port just stops relaying instead of continuing
            // to attempt sends into a channel nothing will ever read.
            subscription.cancel();
            return;
        }
    }
}

/// Port of `createNotifierFrom`: creates a fresh `Notifier` and spawns a
/// task relaying every `'notify'` message received on `source_rx` into it
/// via `notify_subscribers` — "relay the notifications of another
/// worker's `Notifier` into a local one for further fan-out", per
/// upstream's doc comment. Does NOT send the initial `'subscribe'`
/// message (matching upstream — a caller uses [`subscribe_to`] for that).
/// `on_notify` mirrors the optional callback upstream invokes per
/// notification, in addition to fanning it out.
pub fn create_notifier_from(
    mut source_rx: WorkerReceiver<ReplicaState>,
    on_notify: Option<Box<dyn Fn(&ReplicaState) + Send>>,
) -> Notifier {
    let notifier = Notifier::new();
    let notifier_for_task = notifier.clone();
    tokio::spawn(async move {
        while let Some(state) = source_rx.recv_type("notify").await {
            if let Some(cb) = &on_notify {
                cb(&state);
            }
            notifier_for_task.notify_subscribers(state);
        }
    });
    notifier
}

/// Port of `subscribeTo`: sends the initial `'subscribe'` message that
/// starts a [`handle_subscriptions_from`] relay flowing.
pub fn subscribe_to(sender: &WorkerSender<()>) -> Result<(), crate::worker_message::ChannelClosed> {
    sender.send("subscribe", ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker_message::worker_channel;
    use zero_cache_services::notifier::ReplicaStateKind;

    fn state(watermark: &str) -> ReplicaState {
        ReplicaState {
            state: ReplicaStateKind::VersionReady,
            watermark: Some(watermark.to_string()),
            replica_ready_time_ms: None,
        }
    }

    /// Live proof: a real `tokio::spawn`ed task runs `handle_subscriptions_from`
    /// against a real `Notifier`; sending a real `'subscribe'` message over a
    /// real channel causes real notifications pushed into the `Notifier` to
    /// arrive on the other end as real `'notify'` messages — the whole relay
    /// path, end to end, no mocking.
    #[tokio::test]
    async fn handle_subscriptions_from_relays_notifier_state_after_subscribe() {
        let notifier = Notifier::new();
        let (subscribe_tx, subscribe_rx) = worker_channel::<()>();
        let (notify_tx, mut notify_rx) = worker_channel::<ReplicaState>();

        let relay_notifier = notifier.clone();
        let handle = tokio::spawn(async move {
            handle_subscriptions_from(relay_notifier, notify_tx, subscribe_rx).await;
        });

        subscribe_to(&subscribe_tx).unwrap();
        // Let the spawned task process the 'subscribe' message and register
        // with the notifier before we push a new state.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        notifier.notify_subscribers(state("w1"));

        let received = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            notify_rx.recv_type("notify"),
        )
        .await
        .expect("timed out waiting for the relayed notification")
        .expect("channel closed before a notification arrived");

        assert_eq!(received.watermark, Some("w1".to_string()));

        // The relay task's `iter.next()` is still waiting for a NEXT
        // notification (correct: it's a long-lived relay loop that only
        // stops when the subscription itself ends) — abort it rather than
        // awaiting, matching how a real caller would drop/cancel this task
        // when the underlying connection closes, not wait for it to return
        // on its own.
        handle.abort();
        drop(subscribe_tx);
        drop(notify_rx);
    }

    #[tokio::test]
    async fn handle_subscriptions_from_never_subscribes_without_a_subscribe_message() {
        let notifier = Notifier::new();
        let (_subscribe_tx, subscribe_rx) = worker_channel::<()>();
        let (notify_tx, mut notify_rx) = worker_channel::<ReplicaState>();

        // subscribe_tx is held but nothing is ever sent on it; dropping the
        // receiver end (subscribe_rx) inside the task once its channel
        // closes should make handle_subscriptions_from return without ever
        // calling notifier.subscribe().
        drop(_subscribe_tx);
        handle_subscriptions_from(notifier.clone(), notify_tx, subscribe_rx).await;

        notifier.notify_subscribers(state("w1"));
        // `notify_tx` was dropped when `handle_subscriptions_from` returned
        // (it returned immediately since `subscribe_rx` closed without ever
        // receiving a 'subscribe' message), so `notify_rx` sees a closed
        // channel — `recv_type` resolves to `None` rather than hanging.
        assert_eq!(
            notify_rx.recv_type("notify").await,
            None,
            "no subscription should have been created, so no notification should ever arrive"
        );
    }

    #[tokio::test]
    async fn create_notifier_from_relays_inbound_notify_messages() {
        let (source_tx, source_rx) = worker_channel::<ReplicaState>();
        let notifier = create_notifier_from(source_rx, None);
        let sub = notifier.subscribe();

        source_tx.send("notify", state("w2")).unwrap();

        let mut iter = sub.iter();
        let received = tokio::time::timeout(std::time::Duration::from_secs(5), iter.next())
            .await
            .expect("timed out")
            .unwrap()
            .unwrap();
        assert_eq!(received.watermark, Some("w2".to_string()));
    }

    #[tokio::test]
    async fn create_notifier_from_invokes_on_notify_callback() {
        let (source_tx, source_rx) = worker_channel::<ReplicaState>();
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_for_cb = seen.clone();
        let _notifier = create_notifier_from(
            source_rx,
            Some(Box::new(move |s: &ReplicaState| {
                seen_for_cb.lock().unwrap().push(s.watermark.clone())
            })),
        );

        source_tx.send("notify", state("w3")).unwrap();

        for _ in 0..50 {
            if !seen.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(seen.lock().unwrap().as_slice(), [Some("w3".to_string())]);
    }
}
