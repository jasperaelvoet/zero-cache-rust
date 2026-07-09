//! Port of `services/mutagen/pusher.ts`'s `combinePushes` — the first
//! slice of `pusher.ts` (the custom-mutator push-forwarding path,
//! previously entirely unported). `PusherService` receives queued push
//! messages from clients and batches same-connection pushes together
//! before forwarding them to the user's API server over HTTP; this is the
//! pure batching/validation logic that decides what gets combined, not the
//! queue/HTTP-forwarding machinery around it (see module doc below for
//! full scope).
//!
//! Scope: only `combinePushes` and its `assertAreCompatiblePushes`
//! invariant checks are ported — the actual `PusherService`/`PushWorker`
//! (the `Queue`-based streaming service, `fetchFromAPIServer` HTTP calls,
//! `initConnection`/`enqueuePush`/`ackMutationResponses` RPC surface,
//! retry/backoff) are NOT ported; this is "one pure function" the way
//! `cvr_flush_sql.rs`/`mutagen::sql.rs` started their respective threads.
//! `ConnCtx`/`MutateContext` are simplified versions of upstream's
//! `ConnectionContext`/`ConnectionContext['mutateContext']` — only the
//! fields `combinePushes`'s invariant checks actually read (`auth` is
//! collapsed to `Option<String>` rather than the full auth-data JSON
//! object, since `authEquals` upstream is itself just a deep-equality
//! check this crate doesn't need the full shape for). Mutations themselves
//! are generic (`M`) since `combinePushes` never inspects one — it only
//! concatenates `Vec<M>`s.

/// A connection's push-relevant context. Simplified port of the fields of
/// `ConnectionContext` that `combinePushes`'s invariants check — see
/// module doc.
#[derive(Debug, Clone, PartialEq)]
pub struct ConnCtx {
    pub client_id: String,
    pub ws_id: String,
    pub revision: String,
    /// Simplified from upstream's full auth-data object to an opaque
    /// equality-comparable token (see module doc).
    pub auth: Option<String>,
    pub user_id: String,
    pub mutate_context: MutateContext,
}

/// Port of the push-relevant fields of `ConnectionContext['mutateContext']`.
#[derive(Debug, Clone, PartialEq)]
pub struct MutateContext {
    pub url: String,
    pub cookie: Option<String>,
    pub origin: Option<String>,
}

/// Port of `PushBody`'s fields `combinePushes` reads/merges (full
/// `mutations` payload is generic — see module doc).
#[derive(Debug, Clone, PartialEq)]
pub struct PushBody<M> {
    pub schema_version: Option<f64>,
    pub push_version: f64,
    pub mutations: Vec<M>,
}

/// One queued push entry. Port of `PusherEntry`.
#[derive(Debug, Clone, PartialEq)]
pub struct PusherEntry<M> {
    pub conn_ctx: ConnCtx,
    pub push: PushBody<M>,
}

/// Port of `PusherEntryOrStop` (`PusherEntry | 'stop'`).
#[derive(Debug, Clone, PartialEq)]
pub enum PusherEntryOrStop<M> {
    Entry(PusherEntry<M>),
    Stop,
}

/// Error from [`combine_pushes`]: two entries that should have been
/// batchable (same `clientID`/`wsID`/`revision` key) disagreed on a field
/// that must be uniform across them. Port of `assertAreCompatiblePushes`'s
/// assertions — upstream treats this as an unreachable invariant violation
/// (an `assert`, i.e. a bug elsewhere if it fires); this port surfaces it
/// as a `Result` instead of panicking, since a batching bug is exactly the
/// kind of thing worth being able to test/handle rather than crash on.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("incompatible pushes for the same connection: {0}")]
pub struct IncompatiblePushes(pub String);

fn check<T: PartialEq>(a: &T, b: &T, msg: &str) -> Result<(), IncompatiblePushes>
where
    T: std::fmt::Debug,
{
    if a != b {
        return Err(IncompatiblePushes(msg.to_string()));
    }
    Ok(())
}

/// Port of `assertAreCompatiblePushes`: every field that must be uniform
/// across pushes batched for the same connection.
fn assert_compatible<M>(
    left: &PusherEntry<M>,
    right: &PusherEntry<M>,
) -> Result<(), IncompatiblePushes> {
    check(
        &left.conn_ctx.client_id,
        &right.conn_ctx.client_id,
        "clientID must be the same for all pushes",
    )?;
    check(
        &left.conn_ctx.ws_id,
        &right.conn_ctx.ws_id,
        "wsID must be the same for all pushes",
    )?;
    check(
        &left.conn_ctx.revision,
        &right.conn_ctx.revision,
        "revision must be the same for all pushes",
    )?;
    check(
        &left.conn_ctx.auth,
        &right.conn_ctx.auth,
        "auth must be the same for all pushes with the same clientID",
    )?;
    check(
        &left.push.schema_version,
        &right.push.schema_version,
        "schemaVersion must be the same for all pushes with the same clientID",
    )?;
    check(
        &left.push.push_version,
        &right.push.push_version,
        "pushVersion must be the same for all pushes with the same clientID",
    )?;
    check(
        &left.conn_ctx.mutate_context.cookie,
        &right.conn_ctx.mutate_context.cookie,
        "httpCookie must be the same for all pushes with the same clientID",
    )?;
    check(
        &left.conn_ctx.mutate_context.origin,
        &right.conn_ctx.mutate_context.origin,
        "origin must be the same for all pushes with the same clientID",
    )?;
    check(
        &left.conn_ctx.user_id,
        &right.conn_ctx.user_id,
        "userID must be the same for all pushes with the same clientID",
    )?;
    check(
        &left.conn_ctx.mutate_context.url,
        &right.conn_ctx.mutate_context.url,
        "userPushURL must be the same for all pushes with the same clientID",
    )?;
    Ok(())
}

/// Combines same-connection pushes (matched by `clientID:wsID:revision`)
/// into one entry per connection, concatenating their `mutations` in
/// arrival order. Port of `combinePushes`: returns `(combined_entries,
/// saw_stop)` — `saw_stop` is `true` if a `Stop`/missing entry was
/// encountered (upstream returns early on the first `'stop'`/`undefined`
/// entry, matching a queue drain hitting its stop sentinel).
///
/// Groups are collected in first-seen order (matching JS `Map` iteration
/// order), and each group's mutations are concatenated in the order their
/// entries appeared.
pub fn combine_pushes<M: Clone>(
    entries: &[Option<PusherEntryOrStop<M>>],
) -> Result<(Vec<PusherEntry<M>>, bool), IncompatiblePushes> {
    let mut order: Vec<String> = Vec::new();
    let mut groups: std::collections::HashMap<String, Vec<PusherEntry<M>>> =
        std::collections::HashMap::new();

    let mut saw_stop = false;
    for entry in entries {
        match entry {
            None | Some(PusherEntryOrStop::Stop) => {
                saw_stop = true;
                break;
            }
            Some(PusherEntryOrStop::Entry(e)) => {
                let key = format!(
                    "{}:{}:{}",
                    e.conn_ctx.client_id, e.conn_ctx.ws_id, e.conn_ctx.revision
                );
                if !groups.contains_key(&key) {
                    order.push(key.clone());
                }
                groups.entry(key).or_default().push(e.clone());
            }
        }
    }

    let mut combined = Vec::with_capacity(order.len());
    for key in order {
        let group = &groups[&key];
        let mut composite = group[0].clone();
        composite.push.mutations.clear();
        for entry in group {
            assert_compatible(&composite, entry)?;
            composite
                .push
                .mutations
                .extend(entry.push.mutations.iter().cloned());
        }
        combined.push(composite);
    }

    Ok((combined, saw_stop))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(client_id: &str, ws_id: &str, revision: &str) -> ConnCtx {
        ConnCtx {
            client_id: client_id.into(),
            ws_id: ws_id.into(),
            revision: revision.into(),
            auth: None,
            user_id: "u1".into(),
            mutate_context: MutateContext {
                url: "https://api.example/push".into(),
                cookie: None,
                origin: None,
            },
        }
    }

    fn entry(
        client_id: &str,
        ws_id: &str,
        revision: &str,
        mutations: Vec<i32>,
    ) -> PusherEntryOrStop<i32> {
        PusherEntryOrStop::Entry(PusherEntry {
            conn_ctx: ctx(client_id, ws_id, revision),
            push: PushBody {
                schema_version: Some(1.0),
                push_version: 1.0,
                mutations,
            },
        })
    }

    #[test]
    fn combines_same_connection_pushes_in_order() {
        let entries = vec![
            Some(entry("c1", "ws1", "r1", vec![1])),
            Some(entry("c1", "ws1", "r1", vec![2, 3])),
        ];
        let (combined, saw_stop) = combine_pushes(&entries).unwrap();
        assert!(!saw_stop);
        assert_eq!(combined.len(), 1);
        assert_eq!(combined[0].push.mutations, vec![1, 2, 3]);
    }

    #[test]
    fn keeps_different_connections_separate_in_first_seen_order() {
        let entries = vec![
            Some(entry("c1", "ws1", "r1", vec![1])),
            Some(entry("c2", "ws2", "r1", vec![10])),
            Some(entry("c1", "ws1", "r1", vec![2])),
        ];
        let (combined, _) = combine_pushes(&entries).unwrap();
        assert_eq!(combined.len(), 2);
        assert_eq!(combined[0].conn_ctx.client_id, "c1");
        assert_eq!(combined[0].push.mutations, vec![1, 2]);
        assert_eq!(combined[1].conn_ctx.client_id, "c2");
        assert_eq!(combined[1].push.mutations, vec![10]);
    }

    #[test]
    fn stop_entry_halts_collection_and_reports_saw_stop() {
        let entries = vec![
            Some(entry("c1", "ws1", "r1", vec![1])),
            Some(PusherEntryOrStop::Stop),
            Some(entry("c1", "ws1", "r1", vec![2])),
        ];
        let (combined, saw_stop) = combine_pushes(&entries).unwrap();
        assert!(saw_stop);
        // Only the entry before 'stop' should be collected.
        assert_eq!(combined.len(), 1);
        assert_eq!(combined[0].push.mutations, vec![1]);
    }

    #[test]
    fn none_entry_also_halts_like_stop() {
        let entries: Vec<Option<PusherEntryOrStop<i32>>> =
            vec![Some(entry("c1", "ws1", "r1", vec![1])), None];
        let (combined, saw_stop) = combine_pushes(&entries).unwrap();
        assert!(saw_stop);
        assert_eq!(combined.len(), 1);
    }

    #[test]
    fn empty_input_returns_empty_not_stopped() {
        let entries: Vec<Option<PusherEntryOrStop<i32>>> = vec![];
        let (combined, saw_stop) = combine_pushes(&entries).unwrap();
        assert!(!saw_stop);
        assert!(combined.is_empty());
    }

    #[test]
    fn incompatible_revision_for_same_client_ws_errors() {
        let entries = vec![
            Some(entry("c1", "ws1", "r1", vec![1])),
            Some(entry("c1", "ws1", "r2", vec![2])),
        ];
        // Different revisions -> different group keys -> no conflict; this
        // documents that revision IS part of the grouping key, so a real
        // incompatibility only shows up when the SAME key has divergent
        // non-key fields (schema_version below).
        let (combined, _) = combine_pushes(&entries).unwrap();
        assert_eq!(combined.len(), 2);
    }

    #[test]
    fn incompatible_schema_version_within_same_group_errors() {
        let mut e2 = entry("c1", "ws1", "r1", vec![2]);
        if let PusherEntryOrStop::Entry(e) = &mut e2 {
            e.push.schema_version = Some(2.0);
        }
        let entries = vec![Some(entry("c1", "ws1", "r1", vec![1])), Some(e2)];
        let err = combine_pushes(&entries).unwrap_err();
        assert!(err.0.contains("schemaVersion"));
    }

    #[test]
    fn incompatible_url_within_same_group_errors() {
        let mut e2 = entry("c1", "ws1", "r1", vec![2]);
        if let PusherEntryOrStop::Entry(e) = &mut e2 {
            e.conn_ctx.mutate_context.url = "https://other.example/push".into();
        }
        let entries = vec![Some(entry("c1", "ws1", "r1", vec![1])), Some(e2)];
        let err = combine_pushes(&entries).unwrap_err();
        assert!(err.0.contains("userPushURL"));
    }
}
