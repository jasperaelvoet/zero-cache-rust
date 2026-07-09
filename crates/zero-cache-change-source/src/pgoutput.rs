//! Decoder for PostgreSQL's `pgoutput` logical-replication wire format (the
//! messages carried in `COPY BOTH` after `START_REPLICATION ... LOGICAL`,
//! protocol version 1). This is the binary format `tokio-postgres`'s
//! `copy_both_simple` hands back as raw bytes per `XLogData` message; this
//! module decodes those bytes into structured messages, which
//! `change-source.ts`'s `makeRelation`/message-switch logic then maps to
//! [`crate::data::Change`].
//!
//! Reference: <https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html>
//!
//! Only the message kinds zero-cache's replication stream actually needs are
//! decoded: `Begin`, `Commit`, `Relation`, `Insert`, `Update`, `Delete`,
//! `Truncate`. `Origin`/`Type`/`Message`/streaming-transaction variants are
//! skipped (returned as [`PgoutputMessage::Unsupported`]) — none of these
//! carry data zero-cache's replicator applies.

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("pgoutput message too short")]
    TooShort,
    #[error("unknown pgoutput message type byte: {0:#x}")]
    UnknownType(u8),
    #[error("invalid UTF-8 in pgoutput message")]
    InvalidUtf8,
    #[error("unknown replica identity byte: {0:#x}")]
    UnknownReplicaIdentity(u8),
    #[error("unknown tuple column kind byte: {0:#x}")]
    UnknownColumnKind(u8),
}

/// A single decoded tuple column value. Port of the per-column encoding in a
/// pgoutput `Tuple` (the `'n'`/`'u'`/`'t'` kind byte).
#[derive(Debug, Clone, PartialEq)]
pub enum TupleColumn {
    /// SQL NULL.
    Null,
    /// An unchanged TOASTed value (not included in the message; the
    /// replicator must retain whatever value it already has).
    UnchangedToast,
    /// The column's value, text-encoded (the common case; pgoutput does not
    /// send binary column values without an explicit opt-in this port
    /// doesn't use).
    Text(String),
}

/// A decoded row tuple: one [`TupleColumn`] per column, in relation order.
pub type Tuple = Vec<TupleColumn>;

/// A column definition from a `Relation` message.
#[derive(Debug, Clone, PartialEq)]
pub struct RelationColumn {
    pub is_key: bool,
    pub name: String,
    pub type_oid: i32,
    pub atttypmod: i32,
}

/// Replica identity, from a `Relation` message's identity byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaIdentity {
    Default,
    Nothing,
    Full,
    Index,
}

/// A decoded pgoutput message.
#[derive(Debug, Clone, PartialEq)]
pub enum PgoutputMessage {
    Begin {
        final_lsn: u64,
        /// Microseconds since 2000-01-01, as sent on the wire (unconverted).
        commit_timestamp: i64,
        xid: i32,
    },
    Commit {
        commit_lsn: u64,
        end_lsn: u64,
        commit_timestamp: i64,
    },
    Relation {
        relation_id: i32,
        namespace: String,
        name: String,
        replica_identity: ReplicaIdentity,
        columns: Vec<RelationColumn>,
    },
    Insert {
        relation_id: i32,
        new: Tuple,
    },
    Update {
        relation_id: i32,
        /// `Some` iff a `K` (key-only) or `O` (full old row, for
        /// `REPLICA IDENTITY FULL`) tuple preceded the new one.
        old: Option<Tuple>,
        /// Whether `old` is a key-only tuple (`K`) vs. the full old row (`O`).
        old_is_key_only: bool,
        new: Tuple,
    },
    Delete {
        relation_id: i32,
        /// The key or full old row, per the relation's replica identity.
        key: Tuple,
        is_key_only: bool,
    },
    Truncate {
        relation_ids: Vec<i32>,
        cascade: bool,
        restart_identity: bool,
    },
    /// A message type this decoder doesn't need (Origin, Type, logical
    /// Message, streaming-transaction framing).
    Unsupported(u8),
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        let b = *self.buf.get(self.pos).ok_or(DecodeError::TooShort)?;
        self.pos += 1;
        Ok(b)
    }

    fn i16(&mut self) -> Result<i16, DecodeError> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn i32(&mut self) -> Result<i32, DecodeError> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn i64(&mut self) -> Result<i64, DecodeError> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.pos + n > self.buf.len() {
            return Err(DecodeError::TooShort);
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    /// A null-terminated string (pgoutput's `String` field encoding).
    fn cstr(&mut self) -> Result<String, DecodeError> {
        let start = self.pos;
        while *self.buf.get(self.pos).ok_or(DecodeError::TooShort)? != 0 {
            self.pos += 1;
        }
        let s = std::str::from_utf8(&self.buf[start..self.pos])
            .map_err(|_| DecodeError::InvalidUtf8)?;
        self.pos += 1; // skip the null terminator
        Ok(s.to_string())
    }

    fn tuple(&mut self) -> Result<Tuple, DecodeError> {
        let n = self.i16()? as usize;
        let mut cols = Vec::with_capacity(n);
        for _ in 0..n {
            match self.u8()? {
                b'n' => cols.push(TupleColumn::Null),
                b'u' => cols.push(TupleColumn::UnchangedToast),
                b't' | b'b' => {
                    let len = self.i32()? as usize;
                    let bytes = self.take(len)?;
                    let s = std::str::from_utf8(bytes).map_err(|_| DecodeError::InvalidUtf8)?;
                    cols.push(TupleColumn::Text(s.to_string()));
                }
                other => return Err(DecodeError::UnknownColumnKind(other)),
            }
        }
        Ok(cols)
    }
}

/// Decodes a single pgoutput message from the payload of an `XLogData`
/// message (i.e. the bytes after stripping the `XLogData` envelope's own
/// LSN/timestamp header, which the replication-stream reader strips before
/// handing off the pgoutput payload).
pub fn decode(data: &[u8]) -> Result<PgoutputMessage, DecodeError> {
    Ok(decode_with_len(data)?.0)
}

/// [`decode`], additionally returning the number of bytes consumed. Used by
/// [`decode_all`] to walk a buffer containing multiple back-to-back messages
/// (as e.g. `pg_recvlogical -f -` produces with no inter-message framing).
fn decode_with_len(data: &[u8]) -> Result<(PgoutputMessage, usize), DecodeError> {
    let mut r = Reader::new(data);
    let kind = r.u8()?;
    let msg = match kind {
        b'B' => {
            let final_lsn = r.u64()?;
            let commit_timestamp = r.i64()?;
            let xid = r.i32()?;
            PgoutputMessage::Begin {
                final_lsn,
                commit_timestamp,
                xid,
            }
        }
        b'C' => {
            let _flags = r.u8()?;
            let commit_lsn = r.u64()?;
            let end_lsn = r.u64()?;
            let commit_timestamp = r.i64()?;
            PgoutputMessage::Commit {
                commit_lsn,
                end_lsn,
                commit_timestamp,
            }
        }
        b'R' => {
            let relation_id = r.i32()?;
            let namespace = r.cstr()?;
            let name = r.cstr()?;
            let identity_byte = r.u8()?;
            let replica_identity = match identity_byte {
                b'd' => ReplicaIdentity::Default,
                b'n' => ReplicaIdentity::Nothing,
                b'f' => ReplicaIdentity::Full,
                b'i' => ReplicaIdentity::Index,
                other => return Err(DecodeError::UnknownReplicaIdentity(other)),
            };
            let num_cols = r.i16()? as usize;
            let mut columns = Vec::with_capacity(num_cols);
            for _ in 0..num_cols {
                let flags = r.u8()?;
                let name = r.cstr()?;
                let type_oid = r.i32()?;
                let atttypmod = r.i32()?;
                columns.push(RelationColumn {
                    is_key: flags & 1 != 0,
                    name,
                    type_oid,
                    atttypmod,
                });
            }
            PgoutputMessage::Relation {
                relation_id,
                namespace,
                name,
                replica_identity,
                columns,
            }
        }
        b'I' => {
            let relation_id = r.i32()?;
            let _marker = r.u8()?; // always 'N'
            let new = r.tuple()?;
            PgoutputMessage::Insert { relation_id, new }
        }
        b'U' => {
            let relation_id = r.i32()?;
            let mut marker = r.u8()?;
            let mut old = None;
            let mut old_is_key_only = false;
            if marker == b'K' || marker == b'O' {
                old_is_key_only = marker == b'K';
                old = Some(r.tuple()?);
                marker = r.u8()?;
            }
            debug_assert_eq!(marker, b'N');
            let new = r.tuple()?;
            PgoutputMessage::Update {
                relation_id,
                old,
                old_is_key_only,
                new,
            }
        }
        b'D' => {
            let relation_id = r.i32()?;
            let marker = r.u8()?;
            let is_key_only = marker == b'K';
            let key = r.tuple()?;
            PgoutputMessage::Delete {
                relation_id,
                key,
                is_key_only,
            }
        }
        b'T' => {
            let n = r.i32()? as usize;
            let flags = r.u8()?;
            let mut relation_ids = Vec::with_capacity(n);
            for _ in 0..n {
                relation_ids.push(r.i32()?);
            }
            PgoutputMessage::Truncate {
                relation_ids,
                cascade: flags & 1 != 0,
                restart_identity: flags & 2 != 0,
            }
        }
        other => PgoutputMessage::Unsupported(other),
    };
    Ok((msg, r.pos))
}

/// Decodes a buffer containing zero or more back-to-back pgoutput messages
/// with no inter-message framing (as produced by, e.g., `pg_recvlogical -f`).
/// Stops at the end of `data`; errors if a trailing partial message is found.
pub fn decode_all(data: &[u8]) -> Result<Vec<PgoutputMessage>, DecodeError> {
    let mut messages = Vec::new();
    let mut offset = 0;
    while offset < data.len() {
        let (msg, consumed) = decode_with_len(&data[offset..])?;
        messages.push(msg);
        offset += consumed;
    }
    Ok(messages)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn buf(parts: &[&[u8]]) -> Vec<u8> {
        parts.concat()
    }

    #[test]
    fn decodes_begin() {
        let msg = buf(&[
            b"B",
            &1234u64.to_be_bytes(),
            &(-500i64).to_be_bytes(),
            &42i32.to_be_bytes(),
        ]);
        assert_eq!(
            decode(&msg).unwrap(),
            PgoutputMessage::Begin {
                final_lsn: 1234,
                commit_timestamp: -500,
                xid: 42
            }
        );
    }

    #[test]
    fn decodes_commit() {
        let msg = buf(&[
            b"C",
            &[0u8],
            &100u64.to_be_bytes(),
            &200u64.to_be_bytes(),
            &9999i64.to_be_bytes(),
        ]);
        assert_eq!(
            decode(&msg).unwrap(),
            PgoutputMessage::Commit {
                commit_lsn: 100,
                end_lsn: 200,
                commit_timestamp: 9999
            }
        );
    }

    #[test]
    fn decodes_relation_with_key_column() {
        let msg = buf(&[
            b"R",
            &7i32.to_be_bytes(),
            b"public\0",
            b"issues\0",
            b"d",
            &2i16.to_be_bytes(),
            // col 0: id, key=true, type_oid=23 (int4), atttypmod=-1
            &[1u8],
            b"id\0",
            &23i32.to_be_bytes(),
            &(-1i32).to_be_bytes(),
            // col 1: title, key=false, type_oid=25 (text), atttypmod=-1
            &[0u8],
            b"title\0",
            &25i32.to_be_bytes(),
            &(-1i32).to_be_bytes(),
        ]);
        assert_eq!(
            decode(&msg).unwrap(),
            PgoutputMessage::Relation {
                relation_id: 7,
                namespace: "public".into(),
                name: "issues".into(),
                replica_identity: ReplicaIdentity::Default,
                columns: vec![
                    RelationColumn {
                        is_key: true,
                        name: "id".into(),
                        type_oid: 23,
                        atttypmod: -1
                    },
                    RelationColumn {
                        is_key: false,
                        name: "title".into(),
                        type_oid: 25,
                        atttypmod: -1
                    },
                ],
            }
        );
    }

    #[test]
    fn decodes_insert_with_text_and_null_columns() {
        let msg = buf(&[
            b"I",
            &7i32.to_be_bytes(),
            b"N",
            &2i16.to_be_bytes(),
            b"t",
            &1i32.to_be_bytes(),
            b"1",
            b"n",
        ]);
        assert_eq!(
            decode(&msg).unwrap(),
            PgoutputMessage::Insert {
                relation_id: 7,
                new: vec![TupleColumn::Text("1".into()), TupleColumn::Null],
            }
        );
    }

    #[test]
    fn decodes_update_with_key_only_old_tuple() {
        let msg = buf(&[
            b"U",
            &7i32.to_be_bytes(),
            b"K",
            &1i16.to_be_bytes(),
            b"t",
            &1i32.to_be_bytes(),
            b"1",
            b"N",
            &2i16.to_be_bytes(),
            b"t",
            &1i32.to_be_bytes(),
            b"1",
            b"t",
            &3i32.to_be_bytes(),
            b"new",
        ]);
        assert_eq!(
            decode(&msg).unwrap(),
            PgoutputMessage::Update {
                relation_id: 7,
                old: Some(vec![TupleColumn::Text("1".into())]),
                old_is_key_only: true,
                new: vec![
                    TupleColumn::Text("1".into()),
                    TupleColumn::Text("new".into())
                ],
            }
        );
    }

    #[test]
    fn decodes_update_without_old_tuple() {
        // No K/O marker: goes straight to 'N'.
        let msg = buf(&[b"U", &7i32.to_be_bytes(), b"N", &1i16.to_be_bytes(), b"n"]);
        assert_eq!(
            decode(&msg).unwrap(),
            PgoutputMessage::Update {
                relation_id: 7,
                old: None,
                old_is_key_only: false,
                new: vec![TupleColumn::Null],
            }
        );
    }

    #[test]
    fn decodes_delete_with_full_old_row() {
        let msg = buf(&[
            b"D",
            &7i32.to_be_bytes(),
            b"O",
            &1i16.to_be_bytes(),
            b"t",
            &1i32.to_be_bytes(),
            b"1",
        ]);
        assert_eq!(
            decode(&msg).unwrap(),
            PgoutputMessage::Delete {
                relation_id: 7,
                key: vec![TupleColumn::Text("1".into())],
                is_key_only: false,
            }
        );
    }

    #[test]
    fn decodes_truncate_multiple_relations() {
        let msg = buf(&[
            b"T",
            &2i32.to_be_bytes(),
            &3u8.to_be_bytes(), // cascade | restart_identity
            &7i32.to_be_bytes(),
            &8i32.to_be_bytes(),
        ]);
        assert_eq!(
            decode(&msg).unwrap(),
            PgoutputMessage::Truncate {
                relation_ids: vec![7, 8],
                cascade: true,
                restart_identity: true
            }
        );
    }

    #[test]
    fn unsupported_message_type_does_not_error() {
        assert_eq!(decode(b"O").unwrap(), PgoutputMessage::Unsupported(b'O'));
    }

    #[test]
    fn too_short_message_errors() {
        assert_eq!(decode(b"B\x00\x00").unwrap_err(), DecodeError::TooShort);
    }

    /// Real bytes captured from a live local Postgres 17 instance via
    /// `pg_recvlogical` against an actual replication slot, after running:
    /// `CREATE TABLE decode_test(id int primary key, title text);
    ///  CREATE PUBLICATION decode_test_pub FOR TABLE decode_test;
    ///  INSERT INTO decode_test(id, title) VALUES (2, 'world');`
    /// This is not a synthetic/hand-crafted fixture — it verifies the decoder
    /// against PostgreSQL's actual wire output, not just the documented spec.
    #[test]
    fn decodes_real_captured_pgoutput_stream() {
        let data = include_bytes!("../testdata/pgoutput_insert.bin");
        let all = decode_all(data).unwrap();

        // `pg_recvlogical -f <file>` inserts a single `\n` (0x0a) byte between
        // successive decoded records when writing plain output to a file —
        // this is that tool's own file-writing behavior, not part of the
        // wire protocol itself (a real streaming replication client receives
        // properly length-framed `CopyData` messages with no such
        // separator). Filter those out; every *actual* message below decoded
        // correctly from PostgreSQL's real output.
        let messages: Vec<&PgoutputMessage> = all
            .iter()
            .filter(|m| !matches!(m, PgoutputMessage::Unsupported(10)))
            .collect();

        assert!(
            matches!(messages[0], PgoutputMessage::Begin { .. }),
            "{:?}",
            messages[0]
        );

        let PgoutputMessage::Relation {
            namespace,
            name,
            replica_identity,
            columns,
            ..
        } = &messages[1]
        else {
            panic!("expected Relation, got {:?}", messages[1]);
        };
        assert_eq!(namespace, "public");
        assert_eq!(name, "decode_test");
        assert_eq!(*replica_identity, ReplicaIdentity::Default);
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert!(columns[0].is_key);
        assert_eq!(columns[1].name, "title");
        assert!(!columns[1].is_key);

        let PgoutputMessage::Insert { new, .. } = &messages[2] else {
            panic!("expected Insert, got {:?}", messages[2]);
        };
        assert_eq!(
            new,
            &vec![
                TupleColumn::Text("2".into()),
                TupleColumn::Text("world".into())
            ]
        );

        assert!(
            matches!(messages[3], PgoutputMessage::Commit { .. }),
            "{:?}",
            messages[3]
        );
        assert_eq!(messages.len(), 4);
    }
}
