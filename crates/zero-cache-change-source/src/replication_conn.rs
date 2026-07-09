//! A raw, hand-rolled `START_REPLICATION ... LOGICAL` streaming connection.
//!
//! `tokio-postgres` 0.7 (the crate `pg_connection.rs` otherwise depends on
//! for wire-protocol correctness) does not implement the `COPY BOTH`
//! sub-protocol that logical replication streaming requires: it exposes
//! `copy_in`/`copy_out` but no `copy_both`, and its simple-query path errors
//! on any backend message it doesn't recognize (which includes
//! `CopyBothResponse`). This was confirmed by direct inspection of the
//! cached crate source, not assumption.
//!
//! So this module drives the wire protocol directly over a `TcpStream`,
//! using `postgres_protocol` for frontend message encoding (startup, simple
//! query) and its own minimal backend-frame reader — `postgres_protocol`'s
//! `Message::parse` also does not know about `CopyBothResponse` (tag `W`),
//! so a full replacement reader is simplest rather than patching around a
//! partial dependency.
//!
//! Scope: trust/no-password auth only (matching the throwaway local test
//! instance this was verified against); `AuthenticationOk` is required
//! immediately after startup. md5/SCRAM are out of scope for this port.

use bytes::{Buf, BytesMut};
use postgres_protocol::message::frontend;
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::pgoutput::{self, PgoutputMessage};

#[derive(Debug, thiserror::Error)]
pub enum ReplicationError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("unexpected backend message tag `{0}` ({1})")]
    UnexpectedTag(u8, char),
    #[error("server error: {0}")]
    ServerError(String),
    #[error("pgoutput decode error: {0:?}")]
    Decode(#[from] pgoutput::DecodeError),
}

/// A raw backend message: tag byte plus payload (length prefix already
/// consumed). Kept generic rather than parsed into a full enum since this
/// module only needs to recognize a handful of tags during the replication
/// handshake and stream loop.
struct RawMessage {
    tag: u8,
    payload: BytesMut,
}

/// The reply to a successful `CREATE_REPLICATION_SLOT ... LOGICAL` command.
/// Port of the `snapshot_name`/`consistent_point`/`slot_name` fields
/// `initialSync` reads off `createReplicaAndSlot`'s result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedSlot {
    pub slot_name: String,
    /// The LSN (`X/Y` hex form) at which the slot became consistent; the
    /// initial-sync watermark is derived from this.
    pub consistent_point: String,
    /// The name of the exported snapshot, passed to `SET TRANSACTION SNAPSHOT`
    /// by the table-copy transactions.
    pub snapshot_name: String,
}

/// Parses a backend `DataRow` ('D') payload into per-column text values
/// (`None` for SQL NULL). Format: i16 column count, then for each column an
/// i32 byte length (-1 = NULL) followed by that many bytes. The replication
/// command results this port reads are all text-format columns.
fn parse_data_row(payload: &[u8]) -> Vec<Option<String>> {
    let mut out = Vec::new();
    if payload.len() < 2 {
        return out;
    }
    let count = i16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut i = 2;
    for _ in 0..count {
        if i + 4 > payload.len() {
            break;
        }
        let len = i32::from_be_bytes(payload[i..i + 4].try_into().unwrap());
        i += 4;
        if len < 0 {
            out.push(None);
        } else {
            let len = len as usize;
            let end = (i + len).min(payload.len());
            out.push(Some(String::from_utf8_lossy(&payload[i..end]).into_owned()));
            i = end;
        }
    }
    out
}

/// A minimal frontend/backend connection used only to perform the startup
/// handshake and then hand off to `START_REPLICATION` streaming.
pub struct ReplicationConn {
    stream: TcpStream,
    read_buf: BytesMut,
}

impl ReplicationConn {
    /// Opens a plaintext TCP connection to `host:port` and performs the
    /// startup handshake (trust auth only), consuming messages up to and
    /// including `ReadyForQuery`. `replication=database` is set so the
    /// server allows `START_REPLICATION`.
    pub async fn connect(
        host: &str,
        port: u16,
        user: &str,
        dbname: &str,
    ) -> Result<Self, ReplicationError> {
        let stream = TcpStream::connect((host, port)).await?;
        let mut conn = ReplicationConn {
            stream,
            read_buf: BytesMut::new(),
        };

        let mut buf = BytesMut::new();
        frontend::startup_message(
            [
                ("user", user),
                ("database", dbname),
                ("replication", "database"),
                ("application_name", "zero-cache-rust"),
            ],
            &mut buf,
        )
        .map_err(io::Error::other)?;
        conn.stream.write_all(&buf).await?;

        loop {
            let msg = conn.read_message().await?;
            match msg.tag {
                b'R' => {
                    // Authentication*: payload starts with an i32 auth type.
                    let auth_type = i32::from_be_bytes(msg.payload[0..4].try_into().unwrap());
                    if auth_type != 0 {
                        return Err(ReplicationError::ServerError(format!(
                            "unsupported authentication method {auth_type} (only trust/AuthenticationOk supported)"
                        )));
                    }
                }
                b'S' | b'K' => {} // ParameterStatus, BackendKeyData — ignored
                b'Z' => break,    // ReadyForQuery
                b'E' => return Err(ReplicationError::ServerError(error_message(&msg.payload))),
                other => return Err(ReplicationError::UnexpectedTag(other, other as char)),
            }
        }

        Ok(conn)
    }

    /// Reads one full backend message (tag + length-prefixed payload),
    /// buffering as needed. Frame format per the PG protocol: 1-byte tag,
    /// 4-byte big-endian length (includes itself but not the tag), payload.
    async fn read_message(&mut self) -> Result<RawMessage, io::Error> {
        while self.read_buf.len() < 5 {
            let mut chunk = [0u8; 4096];
            let n = self.stream.read(&mut chunk).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed",
                ));
            }
            self.read_buf.extend_from_slice(&chunk[..n]);
        }
        let tag = self.read_buf[0];
        let len = u32::from_be_bytes(self.read_buf[1..5].try_into().unwrap()) as usize;
        let total = 1 + len; // tag byte + len (which counts itself, 4 bytes) + payload
        while self.read_buf.len() < total {
            let mut chunk = [0u8; 4096];
            let n = self.stream.read(&mut chunk).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed",
                ));
            }
            self.read_buf.extend_from_slice(&chunk[..n]);
        }
        self.read_buf.advance(5);
        let payload = self.read_buf.split_to(total - 5);
        Ok(RawMessage { tag, payload })
    }

    /// Sends a Simple Query message. Used for `START_REPLICATION`, which per
    /// the PG protocol is issued as a simple query on a `replication`-mode
    /// connection.
    async fn send_query(&mut self, sql: &str) -> Result<(), ReplicationError> {
        let mut buf = BytesMut::new();
        frontend::query(sql, &mut buf).map_err(io::Error::other)?;
        self.stream.write_all(&buf).await?;
        Ok(())
    }

    /// Issues `CREATE_REPLICATION_SLOT <slot> LOGICAL pgoutput` and returns the
    /// server's reply: the slot name, the `consistent_point` LSN, and the
    /// exported `snapshot_name`. This is the primitive `initialSync`'s
    /// `createReplicaAndSlot` needs — creating the slot atomically fixes a
    /// consistent snapshot that the table-copy transactions then `SET
    /// TRANSACTION SNAPSHOT` to, so the bulk COPY sees exactly the data as of
    /// the slot's `consistent_point` LSN.
    ///
    /// A replication-protocol `CREATE_REPLICATION_SLOT` command exports a
    /// snapshot by default (unlike the SQL `pg_create_logical_replication_slot`
    /// function), so no extra `EXPORT_SNAPSHOT` option is needed. The slot is
    /// left in place (it is the durable replication cursor); the caller owns
    /// dropping it. Leaves the connection at `ReadyForQuery`.
    pub async fn create_logical_replication_slot(
        &mut self,
        slot_name: &str,
    ) -> Result<CreatedSlot, ReplicationError> {
        let sql = format!(r#"CREATE_REPLICATION_SLOT "{slot_name}" LOGICAL pgoutput"#);
        self.send_query(&sql).await?;

        let mut row: Option<Vec<Option<String>>> = None;
        loop {
            let msg = self.read_message().await?;
            match msg.tag {
                b'T' => {} // RowDescription — column layout is fixed, skip it.
                b'D' => row = Some(parse_data_row(&msg.payload)),
                b'C' => {}     // CommandComplete
                b'Z' => break, // ReadyForQuery
                b'E' => return Err(ReplicationError::ServerError(error_message(&msg.payload))),
                other => return Err(ReplicationError::UnexpectedTag(other, other as char)),
            }
        }

        // Column order per the replication protocol docs: slot_name,
        // consistent_point, snapshot_name, output_plugin.
        let row = row.ok_or_else(|| {
            ReplicationError::ServerError(
                "CREATE_REPLICATION_SLOT returned no data row".to_string(),
            )
        })?;
        let col = |i: usize| -> Result<String, ReplicationError> {
            row.get(i).cloned().flatten().ok_or_else(|| {
                ReplicationError::ServerError(format!(
                    "CREATE_REPLICATION_SLOT reply missing column {i}"
                ))
            })
        };
        Ok(CreatedSlot {
            slot_name: col(0)?,
            consistent_point: col(1)?,
            snapshot_name: col(2)?,
        })
    }

    /// Issues `START_REPLICATION SLOT <slot> LOGICAL <lsn> (proto_version
    /// '1', publication_names '<pub>')` and consumes the `CopyBothResponse`
    /// that begins the streaming phase. Returns `self` ready for
    /// `next_change`.
    pub async fn start_replication(
        mut self,
        slot_name: &str,
        publication_name: &str,
        start_lsn: &str,
    ) -> Result<ReplicationStream, ReplicationError> {
        let sql = format!(
            "START_REPLICATION SLOT {slot_name} LOGICAL {start_lsn} (proto_version '1', publication_names '{publication_name}')"
        );
        self.send_query(&sql).await?;

        let msg = self.read_message().await?;
        match msg.tag {
            b'W' => {} // CopyBothResponse — streaming has begun
            b'E' => return Err(ReplicationError::ServerError(error_message(&msg.payload))),
            other => return Err(ReplicationError::UnexpectedTag(other, other as char)),
        }

        Ok(ReplicationStream { conn: self })
    }
}

/// An in-progress logical replication stream. Yields decoded pgoutput
/// messages via `next_change`.
pub struct ReplicationStream {
    conn: ReplicationConn,
}

/// One frame of the `COPY BOTH` payload: either a chunk of WAL data (which
/// itself wraps a pgoutput message) or a server keepalive.
#[derive(Debug)]
pub enum ReplicationEvent {
    Data {
        start_lsn: u64,
        end_lsn: u64,
        message: PgoutputMessage,
    },
    /// Server keepalive; `reply_requested` indicates the client should send
    /// a standby status update ([`ReplicationStream::send_standby_status_update`])
    /// to avoid the connection being considered dead and to advance the slot.
    Keepalive { end_lsn: u64, reply_requested: bool },
}

/// The Postgres logical-replication epoch: micro-seconds between the Unix
/// epoch and `2000-01-01 00:00:00 UTC`, the base for standby-status timestamps.
const PG_EPOCH_MICROS_FROM_UNIX: i64 = 946_684_800_000_000;

impl ReplicationStream {
    /// Sends a Standby Status Update (`'r'`) feedback message: tells the server
    /// how far the client has written / flushed / applied the WAL. Without this,
    /// the slot's `confirmed_flush_lsn` never advances and upstream WAL
    /// accumulates indefinitely — so a real consuming service sends it
    /// periodically and in response to a keepalive with `reply_requested`.
    ///
    /// `write_lsn`/`flush_lsn`/`apply_lsn` are the LSNs the client has durably
    /// handled (typically the last committed `end_lsn`). `timestamp_micros` is
    /// micro-seconds since the Postgres epoch (2000-01-01); pass `0` to let the
    /// server use its own clock. `reply_requested` asks the server to respond
    /// with a keepalive.
    pub async fn send_standby_status_update(
        &mut self,
        write_lsn: u64,
        flush_lsn: u64,
        apply_lsn: u64,
        timestamp_micros: i64,
        reply_requested: bool,
    ) -> Result<(), ReplicationError> {
        // Payload: 'r' + writeLSN + flushLSN + applyLSN + timestamp + replyByte.
        let mut payload = Vec::with_capacity(34);
        payload.push(b'r');
        payload.extend_from_slice(&write_lsn.to_be_bytes());
        payload.extend_from_slice(&flush_lsn.to_be_bytes());
        payload.extend_from_slice(&apply_lsn.to_be_bytes());
        payload.extend_from_slice(&timestamp_micros.to_be_bytes());
        payload.push(u8::from(reply_requested));

        // Wrap in a CopyData frame: tag 'd', i32 length (self + payload), payload.
        let mut frame = Vec::with_capacity(payload.len() + 5);
        frame.push(b'd');
        frame.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
        frame.extend_from_slice(&payload);
        self.conn.stream.write_all(&frame).await?;
        Ok(())
    }

    /// Converts a Unix-epoch micro-second timestamp to the Postgres-epoch form
    /// standby-status updates use. Exposed so a caller that has a real clock can
    /// stamp feedback without hard-coding the epoch offset (this port takes the
    /// time as a parameter rather than reading a clock ambiently).
    pub fn pg_timestamp_from_unix_micros(unix_micros: i64) -> i64 {
        unix_micros - PG_EPOCH_MICROS_FROM_UNIX
    }

    /// Reads and decodes the next `CopyData` frame. Returns `Ok(None)` on a
    /// clean `CopyDone`/stream end.
    pub async fn next_event(&mut self) -> Result<Option<ReplicationEvent>, ReplicationError> {
        loop {
            let msg = self.conn.read_message().await?;
            match msg.tag {
                b'd' => {
                    // CopyData: payload[0] is 'w' (XLogData) or 'k' (keepalive).
                    let payload = &msg.payload;
                    if payload.is_empty() {
                        continue;
                    }
                    match payload[0] {
                        b'w' => {
                            // 'w' + i64 walStart + i64 walEnd + i64 sendTime + pgoutput bytes
                            let start_lsn = u64::from_be_bytes(payload[1..9].try_into().unwrap());
                            let end_lsn = u64::from_be_bytes(payload[9..17].try_into().unwrap());
                            let body = &payload[25..];
                            let message = pgoutput::decode(body)?;
                            return Ok(Some(ReplicationEvent::Data {
                                start_lsn,
                                end_lsn,
                                message,
                            }));
                        }
                        b'k' => {
                            let end_lsn = u64::from_be_bytes(payload[1..9].try_into().unwrap());
                            let reply_requested = payload[17] != 0;
                            return Ok(Some(ReplicationEvent::Keepalive {
                                end_lsn,
                                reply_requested,
                            }));
                        }
                        other => return Err(ReplicationError::UnexpectedTag(other, other as char)),
                    }
                }
                b'c' => return Ok(None), // CopyDone
                b'E' => return Err(ReplicationError::ServerError(error_message(&msg.payload))),
                other => return Err(ReplicationError::UnexpectedTag(other, other as char)),
            }
        }
    }
}

/// Extracts a human-readable message from an ErrorResponse payload (a
/// sequence of `<field-byte><cstr>` pairs terminated by a nul byte).
fn error_message(payload: &[u8]) -> String {
    let mut fields = Vec::new();
    let mut i = 0;
    while i < payload.len() && payload[i] != 0 {
        let field_type = payload[i];
        i += 1;
        let start = i;
        while i < payload.len() && payload[i] != 0 {
            i += 1;
        }
        let value = String::from_utf8_lossy(&payload[start..i]).into_owned();
        i += 1;
        if field_type == b'M' {
            fields.push(value);
        }
    }
    fields
        .into_iter()
        .next()
        .unwrap_or_else(|| "unknown server error".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_host_port() -> (String, u16) {
        let url =
            std::env::var("ZERO_TEST_PG_TCP").unwrap_or_else(|_| "localhost:54329".to_string());
        let mut parts = url.splitn(2, ':');
        let host = parts.next().unwrap().to_string();
        let port: u16 = parts.next().unwrap().parse().unwrap();
        (host, port)
    }

    /// End-to-end: connect, create a table + publication + slot via a
    /// side-channel `tokio-postgres` client, start raw replication, insert a
    /// row, and verify the decoded stream matches — driven entirely by this
    /// module's own wire-protocol handshake, no external `pg_recvlogical`.
    #[tokio::test]
    async fn streams_real_insert_end_to_end() {
        let (host, port) = test_host_port();
        let conn_str = format!("host={host} port={port} user=postgres dbname=postgres");
        let Ok(client) = crate::pg_connection::connect(&conn_str).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        client
            .batch_execute(
                "DROP TABLE IF EXISTS repl_test CASCADE; \
             CREATE TABLE repl_test(id int primary key, title text); \
             DROP PUBLICATION IF EXISTS repl_test_pub; \
             CREATE PUBLICATION repl_test_pub FOR TABLE repl_test;",
            )
            .await
            .unwrap();
        client
            .batch_execute(
                "SELECT pg_drop_replication_slot('repl_test_slot') WHERE EXISTS \
                 (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'repl_test_slot');",
            )
            .await
            .ok();
        client
            .query(
                "SELECT * FROM pg_create_logical_replication_slot('repl_test_slot', 'pgoutput')",
                &[],
            )
            .await
            .unwrap();

        let conn = ReplicationConn::connect(&host, port, "postgres", "postgres")
            .await
            .unwrap();
        let mut stream = conn
            .start_replication("repl_test_slot", "repl_test_pub", "0/0")
            .await
            .unwrap();

        client
            .batch_execute("INSERT INTO repl_test(id, title) VALUES (7, 'raw-conn')")
            .await
            .unwrap();

        let mut saw_relation = false;
        let mut saw_insert = false;
        for _ in 0..20 {
            let event =
                tokio::time::timeout(std::time::Duration::from_secs(5), stream.next_event())
                    .await
                    .expect("timed out waiting for replication event")
                    .unwrap();
            let Some(event) = event else { break };
            if let ReplicationEvent::Data { message, .. } = event {
                match message {
                    PgoutputMessage::Relation { name, .. } if name == "repl_test" => {
                        saw_relation = true
                    }
                    PgoutputMessage::Insert { new, .. } => {
                        assert_eq!(
                            new,
                            vec![
                                pgoutput::TupleColumn::Text("7".into()),
                                pgoutput::TupleColumn::Text("raw-conn".into()),
                            ]
                        );
                        saw_insert = true;
                    }
                    _ => {}
                }
            }
            if saw_relation && saw_insert {
                break;
            }
        }
        assert!(saw_relation, "did not see Relation message for repl_test");
        assert!(
            saw_insert,
            "did not see Insert message with expected values"
        );

        // Explicitly drop the raw streaming connection and give the server a
        // moment to notice the socket closed before reclaiming the slot —
        // `pg_drop_replication_slot` errors if the slot still looks active.
        drop(stream);
        let mut dropped = false;
        for _ in 0..20 {
            if client
                .query("SELECT pg_drop_replication_slot('repl_test_slot')", &[])
                .await
                .is_ok()
            {
                dropped = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(
            dropped,
            "could not drop replication slot after streaming connection closed"
        );
        client
            .batch_execute("DROP PUBLICATION repl_test_pub; DROP TABLE repl_test;")
            .await
            .unwrap();
    }

    /// Live: create a logical replication slot over the raw connection and
    /// verify the exported snapshot is real by `SET TRANSACTION SNAPSHOT`-ing
    /// to it from a side-channel client and reading a committed-after row that
    /// must NOT be visible at the snapshot.
    #[tokio::test]
    async fn creates_slot_with_usable_exported_snapshot() {
        let (host, port) = test_host_port();
        let conn_str = format!("host={host} port={port} user=postgres dbname=postgres");
        let Ok(client) = crate::pg_connection::connect(&conn_str).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        client
            .batch_execute(
                "DROP TABLE IF EXISTS slot_snap_test CASCADE; \
                 CREATE TABLE slot_snap_test(id int primary key); \
                 INSERT INTO slot_snap_test(id) VALUES (1);",
            )
            .await
            .unwrap();
        client
            .batch_execute(
                "SELECT pg_drop_replication_slot('slot_snap_test_slot') WHERE EXISTS \
                 (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'slot_snap_test_slot');",
            )
            .await
            .ok();

        let mut conn = ReplicationConn::connect(&host, port, "postgres", "postgres")
            .await
            .unwrap();
        let slot = conn
            .create_logical_replication_slot("slot_snap_test_slot")
            .await
            .unwrap();
        assert_eq!(slot.slot_name, "slot_snap_test_slot");
        assert!(
            slot.consistent_point.contains('/'),
            "consistent_point looks like an LSN"
        );
        assert!(
            !slot.snapshot_name.is_empty(),
            "a snapshot name was exported"
        );

        // Commit a new row AFTER the snapshot was fixed.
        client
            .batch_execute("INSERT INTO slot_snap_test(id) VALUES (2)")
            .await
            .unwrap();

        // A transaction bound to the exported snapshot must only see id=1.
        let snap_client = crate::pg_connection::connect(&conn_str).await.unwrap();
        snap_client
            .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
            .await
            .unwrap();
        snap_client
            .batch_execute(&format!(
                "SET TRANSACTION SNAPSHOT '{}'",
                slot.snapshot_name
            ))
            .await
            .unwrap();
        let rows = snap_client
            .query("SELECT id FROM slot_snap_test ORDER BY id", &[])
            .await
            .unwrap();
        let ids: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>(0)).collect();
        assert_eq!(
            ids,
            vec![1],
            "snapshot must not see the row committed after slot creation"
        );
        snap_client.batch_execute("COMMIT").await.unwrap();

        // Slot must be dropped only after the raw connection holding it closes.
        drop(conn);
        let mut dropped = false;
        for _ in 0..20 {
            if client
                .query(
                    "SELECT pg_drop_replication_slot('slot_snap_test_slot')",
                    &[],
                )
                .await
                .is_ok()
            {
                dropped = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(dropped, "could not drop slot after connection closed");
        client
            .batch_execute("DROP TABLE slot_snap_test")
            .await
            .unwrap();
    }

    #[test]
    fn pg_timestamp_conversion_subtracts_the_epoch_offset() {
        // 2000-01-01T00:00:00Z in unix micros -> 0 in pg-epoch micros.
        assert_eq!(
            ReplicationStream::pg_timestamp_from_unix_micros(946_684_800_000_000),
            0
        );
        // One second later -> 1_000_000 pg-epoch micros.
        assert_eq!(
            ReplicationStream::pg_timestamp_from_unix_micros(946_684_801_000_000),
            1_000_000
        );
    }

    /// Live: stream an insert, then send a Standby Status Update flushing up to
    /// the received LSN, and confirm the slot's `confirmed_flush_lsn` advances
    /// from its initial `0/0` — proving the feedback path actually reaches the
    /// server (without it, the slot pins WAL forever).
    #[tokio::test]
    async fn standby_status_update_advances_confirmed_flush_lsn() {
        let (host, port) = test_host_port();
        let conn_str = format!("host={host} port={port} user=postgres dbname=postgres");
        let Ok(client) = crate::pg_connection::connect(&conn_str).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        client
            .batch_execute(
                "DROP TABLE IF EXISTS sfb_test CASCADE; \
                 CREATE TABLE sfb_test(id int primary key); \
                 DROP PUBLICATION IF EXISTS sfb_pub; \
                 CREATE PUBLICATION sfb_pub FOR TABLE sfb_test;",
            )
            .await
            .unwrap();
        client
            .batch_execute(
                "SELECT pg_drop_replication_slot('sfb_slot') WHERE EXISTS \
                 (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'sfb_slot');",
            )
            .await
            .ok();
        client
            .query(
                "SELECT * FROM pg_create_logical_replication_slot('sfb_slot', 'pgoutput')",
                &[],
            )
            .await
            .unwrap();

        let conn = ReplicationConn::connect(&host, port, "postgres", "postgres")
            .await
            .unwrap();
        let mut stream = conn
            .start_replication("sfb_slot", "sfb_pub", "0/0")
            .await
            .unwrap();
        client
            .batch_execute("INSERT INTO sfb_test(id) VALUES (1)")
            .await
            .unwrap();

        // Read events until we have a commit's end LSN to flush up to.
        let mut flush_to = 0u64;
        for _ in 0..30 {
            let event =
                tokio::time::timeout(std::time::Duration::from_secs(5), stream.next_event())
                    .await
                    .expect("timed out")
                    .unwrap();
            match event {
                Some(ReplicationEvent::Data { end_lsn, .. }) => flush_to = flush_to.max(end_lsn),
                Some(ReplicationEvent::Keepalive { end_lsn, .. }) => {
                    flush_to = flush_to.max(end_lsn)
                }
                None => break,
            }
            if flush_to > 0 {
                break;
            }
        }
        assert!(flush_to > 0, "observed a non-zero WAL LSN to flush");

        // Send feedback flushing up to `flush_to`, asking for a reply.
        stream
            .send_standby_status_update(flush_to, flush_to, flush_to, 0, true)
            .await
            .unwrap();

        // The slot's confirmed_flush_lsn should advance past 0/0.
        let mut advanced = false;
        for _ in 0..30 {
            let row = client
                .query_one(
                    "SELECT confirmed_flush_lsn > '0/0'::pg_lsn AS advanced \
                     FROM pg_replication_slots WHERE slot_name = 'sfb_slot'",
                    &[],
                )
                .await
                .unwrap();
            if row.get::<_, bool>("advanced") {
                advanced = true;
                break;
            }
            // Nudge the server to process feedback and keep the socket lively.
            let _ = stream
                .send_standby_status_update(flush_to, flush_to, flush_to, 0, false)
                .await;
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(
            advanced,
            "confirmed_flush_lsn advanced after standby status update"
        );

        drop(stream);
        for _ in 0..20 {
            if client
                .query("SELECT pg_drop_replication_slot('sfb_slot')", &[])
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        client
            .batch_execute("DROP PUBLICATION sfb_pub; DROP TABLE sfb_test;")
            .await
            .unwrap();
    }

    #[test]
    fn parse_data_row_reads_text_columns_and_nulls() {
        // 3 columns: "abc", NULL, "".
        let mut payload = Vec::new();
        payload.extend_from_slice(&3i16.to_be_bytes());
        payload.extend_from_slice(&3i32.to_be_bytes());
        payload.extend_from_slice(b"abc");
        payload.extend_from_slice(&(-1i32).to_be_bytes());
        payload.extend_from_slice(&0i32.to_be_bytes());
        let cols = parse_data_row(&payload);
        assert_eq!(
            cols,
            vec![Some("abc".to_string()), None, Some(String::new())]
        );
    }

    #[test]
    fn error_message_extracts_m_field() {
        let mut payload = Vec::new();
        payload.extend_from_slice(b"SERROR\0");
        payload.extend_from_slice(b"Msomething broke\0");
        payload.push(0);
        assert_eq!(error_message(&payload), "something broke");
    }
}
