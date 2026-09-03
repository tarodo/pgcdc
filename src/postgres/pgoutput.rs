use crate::error::PgcdcError;
use crate::schema::{Column, Relation};

/// A cursor over the payload with a length check on every read. Any read past the
/// buffer's boundary is a Decode error, not a panic: a corrupted WAL must not bring
/// down the process without a clear message.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], PgcdcError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| PgcdcError::Decode("length overflow".into()))?;
        if end > self.buf.len() {
            return Err(PgcdcError::Decode(format!(
                "need {n} bytes at offset {}, only {} remain",
                self.pos,
                self.buf.len() - self.pos
            )));
        }
        let out = &self.buf[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, PgcdcError> {
        Ok(self.take(1)?[0])
    }

    fn i16(&mut self) -> Result<i16, PgcdcError> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32, PgcdcError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn i32(&mut self) -> Result<i32, PgcdcError> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, PgcdcError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn i64(&mut self) -> Result<i64, PgcdcError> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    /// C string: bytes up to the null terminator, the terminator itself is consumed.
    fn cstr(&mut self) -> Result<String, PgcdcError> {
        let rest = &self.buf[self.pos..];
        let nul = rest.iter().position(|&b| b == 0).ok_or_else(|| {
            PgcdcError::Decode(format!("unterminated string at offset {}", self.pos))
        })?;
        let s = std::str::from_utf8(&rest[..nul])
            .map_err(|e| PgcdcError::Decode(format!("invalid utf8 at offset {}: {e}", self.pos)))?
            .to_owned();
        self.pos += nul + 1;
        Ok(s)
    }

    fn finish(&self) -> Result<(), PgcdcError> {
        if self.pos != self.buf.len() {
            return Err(PgcdcError::Decode(format!(
                "{} trailing bytes after message",
                self.buf.len() - self.pos
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnValue {
    /// Tag 'n'. In a 'N'/'O' tuple — a real SQL NULL.
    /// In a 'K' tuple — "the column was not sent", which is NOT the same thing.
    Null,
    /// Tag 'u'. The TOAST value did not change, the server did not forward it.
    UnchangedToast,
    /// Tag 't'. The value's text representation.
    Text(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleData {
    pub columns: Vec<ColumnValue>,
}

/// What exactly the server sent in the old tuple. The distinction carries weight:
/// with `Key`, non-key columns arrive with the tag `'n'`, and that means "not sent", not NULL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OldTupleKind {
    /// Tag `'K'` — replica identity columns only.
    Key,
    /// Tag `'O'` — the full old row (REPLICA IDENTITY FULL).
    Full,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PgOutputMessage {
    Begin {
        final_lsn: u64,
        commit_timestamp: i64,
        xid: u32,
    },
    Commit {
        flags: u8,
        commit_lsn: u64,
        end_lsn: u64,
        commit_timestamp: i64,
    },
    Relation(Relation),
    Insert {
        relation_id: u32,
        tuple: TupleData,
    },
    Update {
        relation_id: u32,
        old: Option<(OldTupleKind, TupleData)>,
        new: TupleData,
    },
    Delete {
        relation_id: u32,
        old_kind: OldTupleKind,
        old: TupleData,
    },
    /// A TRUNCATE of one or more relations, all inside one transaction.
    /// The flags byte (CASCADE, RESTART IDENTITY) is read and discarded: its
    /// effects reach a row-level consumer as the extra relation ids in this
    /// same message, or not at all (RESTART IDENTITY touches sequences).
    Truncate {
        relation_ids: Vec<u32>,
    },
}

pub fn decode(payload: &[u8]) -> Result<PgOutputMessage, PgcdcError> {
    let mut r = Reader::new(payload);
    let kind = r.u8()? as char;
    let msg = match kind {
        'B' => PgOutputMessage::Begin {
            final_lsn: r.u64()?,
            commit_timestamp: r.i64()?,
            xid: r.u32()?,
        },
        'C' => PgOutputMessage::Commit {
            flags: r.u8()?,
            commit_lsn: r.u64()?,
            end_lsn: r.u64()?,
            commit_timestamp: r.i64()?,
        },
        'R' => {
            let id = r.u32()?;
            let namespace = r.cstr()?;
            let name = r.cstr()?;
            let replica_identity = r.u8()?;
            let ncols = r.i16()?;
            if ncols < 0 {
                return Err(PgcdcError::Decode(format!("negative column count {ncols}")));
            }
            let mut columns = Vec::with_capacity(ncols as usize);
            for _ in 0..ncols {
                columns.push(Column {
                    is_key: r.u8()? == 1,
                    name: r.cstr()?,
                    type_oid: r.u32()?,
                    atttypmod: r.i32()?,
                });
            }
            PgOutputMessage::Relation(Relation {
                id,
                namespace,
                name,
                replica_identity,
                columns,
            })
        }
        'I' => {
            let relation_id = r.u32()?;
            let tag = r.u8()?;
            if tag != b'N' {
                return Err(PgcdcError::Decode(format!(
                    "INSERT expects tuple tag 'N', got {:?}",
                    tag as char
                )));
            }
            PgOutputMessage::Insert {
                relation_id,
                tuple: read_tuple(&mut r)?,
            }
        }
        'U' => {
            let relation_id = r.u32()?;
            // The byte at position 5 decides everything: 'O'/'K' — an old tuple follows,
            // 'N' — there is no old one and this is already the new one. There is no third option.
            let tag = r.u8()?;
            let (old, new_tag) = match tag {
                b'O' => (Some((OldTupleKind::Full, read_tuple(&mut r)?)), r.u8()?),
                b'K' => (Some((OldTupleKind::Key, read_tuple(&mut r)?)), r.u8()?),
                b'N' => (None, b'N'),
                other => {
                    return Err(PgcdcError::Decode(format!(
                        "UPDATE expects tuple tag 'O', 'K' or 'N', got {:?}",
                        other as char
                    )))
                }
            };
            if new_tag != b'N' {
                return Err(PgcdcError::Decode(format!(
                    "UPDATE expects new tuple tag 'N', got {:?}",
                    new_tag as char
                )));
            }
            PgOutputMessage::Update {
                relation_id,
                old,
                new: read_tuple(&mut r)?,
            }
        }
        'D' => {
            let relation_id = r.u32()?;
            // Unlike UPDATE the tag is mandatory: there is no "nothing" case.
            let old_kind = match r.u8()? {
                b'K' => OldTupleKind::Key,
                b'O' => OldTupleKind::Full,
                other => {
                    return Err(PgcdcError::Decode(format!(
                        "DELETE expects tuple tag 'K' or 'O', got {:?}",
                        other as char
                    )))
                }
            };
            PgOutputMessage::Delete {
                relation_id,
                old_kind,
                old: read_tuple(&mut r)?,
            }
        }
        'T' => {
            let nrelations = r.i32()?;
            if nrelations < 0 {
                return Err(PgcdcError::Decode(format!(
                    "negative truncate relation count {nrelations}"
                )));
            }
            // The flags byte (CASCADE / RESTART IDENTITY) is read to keep the offsets
            // right, then discarded — see the doc comment on the Truncate variant.
            let _flags = r.u8()?;
            let mut relation_ids = Vec::with_capacity(nrelations as usize);
            for _ in 0..nrelations {
                relation_ids.push(r.u32()?);
            }
            PgOutputMessage::Truncate { relation_ids }
        }
        other => return Err(PgcdcError::UnsupportedMessage { kind: other }),
    };
    r.finish()?;
    Ok(msg)
}

/// Reads TupleData: an Int16 column count, then for each one — a tag and,
/// only for 't'/'b', a length and data. 'n' and 'u' have NO length.
fn read_tuple(r: &mut Reader<'_>) -> Result<TupleData, PgcdcError> {
    let ncols = r.i16()?;
    if ncols < 0 {
        return Err(PgcdcError::Decode(format!(
            "negative tuple column count {ncols}"
        )));
    }
    let mut columns = Vec::with_capacity(ncols as usize);
    for i in 0..ncols {
        let tag = r.u8()?;
        let value = match tag {
            b'n' => ColumnValue::Null,
            b'u' => ColumnValue::UnchangedToast,
            b't' => {
                let len = r.i32()?;
                if len < 0 {
                    return Err(PgcdcError::Decode(format!(
                        "negative value length {len} at column {i}"
                    )));
                }
                let bytes = r.take(len as usize)?;
                let text = std::str::from_utf8(bytes)
                    .map_err(|e| PgcdcError::Decode(format!("invalid utf8 in column {i}: {e}")))?;
                ColumnValue::Text(text.to_owned())
            }
            // The 'b' layout (Int32 length + data) is taken from the documentation and is not
            // confirmed by any captured bytes (docs/pgoutput-notes.md §14.2): the `binary` option
            // was never enabled anywhere, there isn't a single fixture with this tag. Decoding it
            // as text would be unverified code; a typed error is more honest.
            b'b' => {
                return Err(PgcdcError::Decode("binary format not supported".into()));
            }
            other => {
                return Err(PgcdcError::Decode(format!(
                    "unknown column tag {:?} at column {i}",
                    other as char
                )))
            }
        };
        columns.push(value);
    }
    Ok(TupleData { columns })
}

#[cfg(test)]
mod tests {
    use super::*;

    const BEGIN: &[u8] = include_bytes!("../../tests/fixtures/0001_begin.bin");
    const COMMIT: &[u8] = include_bytes!("../../tests/fixtures/0004_commit.bin");

    #[test]
    fn decodes_begin() {
        assert_eq!(BEGIN.len(), 21, "BEGIN is always 21 bytes");
        match decode(BEGIN).unwrap() {
            PgOutputMessage::Begin {
                final_lsn,
                commit_timestamp,
                xid,
            } => {
                assert_eq!(final_lsn, 0x0193_00D0);
                assert_eq!(commit_timestamp, 841_423_351_314_489);
                assert_eq!(xid, 737);
            }
            other => panic!("expected Begin, got {other:?}"),
        }
    }

    #[test]
    fn decodes_commit_without_swapping_the_two_lsns() {
        // commit_lsn is at offset 2, end_lsn at offset 10, a difference of 0x30.
        // Mixing them up would mean re-reading every transaction after a restart.
        match decode(COMMIT).unwrap() {
            PgOutputMessage::Commit {
                flags,
                commit_lsn,
                end_lsn,
                commit_timestamp,
            } => {
                assert_eq!(flags, 0);
                assert_eq!(commit_lsn, 0x0193_00D0, "commit_lsn is first, offset 2");
                assert_eq!(end_lsn, 0x0193_0100, "end_lsn is second, offset 10");
                assert_eq!(end_lsn - commit_lsn, 0x30);
                assert_eq!(commit_timestamp, 841_423_351_314_489);
            }
            other => panic!("expected Commit, got {other:?}"),
        }
    }

    #[test]
    fn begin_final_lsn_equals_commit_commit_lsn() {
        // Invariant from notes §8: BEGIN already knows where the transaction will end.
        let (b, c) = (decode(BEGIN).unwrap(), decode(COMMIT).unwrap());
        let (PgOutputMessage::Begin { final_lsn, .. }, PgOutputMessage::Commit { commit_lsn, .. }) =
            (b, c)
        else {
            panic!("wrong types")
        };
        assert_eq!(final_lsn, commit_lsn);
    }

    const RELATION_USERS: &[u8] = include_bytes!("../../tests/fixtures/0002_relation.bin");
    const RELATION_ITEMS: &[u8] = include_bytes!("../../tests/fixtures/0012_relation.bin");

    #[test]
    fn decodes_relation_with_full_replica_identity() {
        let PgOutputMessage::Relation(rel) = decode(RELATION_USERS).unwrap() else {
            panic!("expected Relation")
        };
        assert_eq!(rel.id, 16385);
        assert_eq!(rel.namespace, "public");
        assert_eq!(rel.name, "users");
        assert_eq!(
            rel.replica_identity, b'f',
            "users was created with REPLICA IDENTITY FULL"
        );
        let names: Vec<&str> = rel.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "name", "email", "bio"]);
        assert!(
            rel.columns.iter().all(|c| c.is_key),
            "with FULL, all columns are marked as key"
        );
        assert!(
            rel.columns.iter().all(|c| c.atttypmod == -1),
            "atttypmod is read as signed"
        );
    }

    #[test]
    fn decodes_relation_with_default_replica_identity() {
        let PgOutputMessage::Relation(rel) = decode(RELATION_ITEMS).unwrap() else {
            panic!("expected Relation")
        };
        assert_eq!(rel.name, "items");
        assert_eq!(rel.replica_identity, b'd');
        let names: Vec<&str> = rel.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "title", "qty"]);
        let keys: Vec<bool> = rel.columns.iter().map(|c| c.is_key).collect();
        assert_eq!(
            keys,
            [true, false, false],
            "with DEFAULT, only the PK is key"
        );
    }

    const INSERT_USERS: &[u8] = include_bytes!("../../tests/fixtures/0003_insert.bin");
    const INSERT_ITEMS: &[u8] = include_bytes!("../../tests/fixtures/0013_insert.bin");
    const INSERT_TOAST: &[u8] = include_bytes!("../../tests/fixtures/0022_insert.bin");

    #[test]
    fn decodes_insert_and_does_not_read_length_after_null_tag() {
        // The last byte of 0003_insert.bin is tag 'n' with no length and no data.
        // A decoder that unconditionally reads 4 length bytes after the tag would fall apart here.
        let PgOutputMessage::Insert { relation_id, tuple } = decode(INSERT_USERS).unwrap() else {
            panic!("expected Insert")
        };
        assert_eq!(relation_id, 16385);
        assert_eq!(
            tuple.columns,
            vec![
                ColumnValue::Text("1".into()),
                ColumnValue::Text("Alice".into()),
                ColumnValue::Text("alice@example.com".into()),
                ColumnValue::Null,
            ]
        );
    }

    #[test]
    fn values_arrive_as_text_not_binary() {
        // id BIGINT = 1 arrives as a single byte 0x31 = ASCII '1', not eight bytes of int8.
        let PgOutputMessage::Insert { tuple, .. } = decode(INSERT_ITEMS).unwrap() else {
            panic!("expected Insert")
        };
        assert_eq!(tuple.columns[0], ColumnValue::Text("10".into()));
        assert_eq!(tuple.columns[2], ColumnValue::Text("5".into()));
    }

    #[test]
    fn decodes_large_toast_value_in_full() {
        let PgOutputMessage::Insert { tuple, .. } = decode(INSERT_TOAST).unwrap() else {
            panic!("expected Insert")
        };
        let ColumnValue::Text(bio) = &tuple.columns[3] else {
            panic!("bio must arrive as text in full on INSERT")
        };
        assert_eq!(bio.len(), 9600);
    }

    #[test]
    fn every_column_has_an_entry_even_when_null() {
        // TupleData always has exactly ncols entries, no gaps (notes §6).
        let PgOutputMessage::Insert { tuple, .. } = decode(INSERT_USERS).unwrap() else {
            panic!("expected Insert")
        };
        assert_eq!(tuple.columns.len(), 4);
    }

    #[test]
    fn other_message_kinds_are_still_explicitly_unsupported() {
        // TYPE, ORIGIN, MESSAGE and anything unknown must still produce an
        // explicit error, not a silent skip (spec §8). TRUNCATE ('T') is now
        // handled and is covered by its own tests below.
        for kind in *b"YOMS" {
            let payload = [kind, 0x00, 0x00, 0x00, 0x00];
            assert!(
                matches!(decode(&payload), Err(PgcdcError::UnsupportedMessage { .. })),
                "kind {:?} must be explicitly unsupported",
                kind as char
            );
        }
    }

    const DELETE_FULL: &[u8] = include_bytes!("../../tests/fixtures/0009_delete.bin");
    const DELETE_KEY: &[u8] = include_bytes!("../../tests/fixtures/0019_delete.bin");

    #[test]
    fn decodes_delete_with_full_old_tuple() {
        let PgOutputMessage::Delete {
            relation_id,
            old_kind,
            old,
        } = decode(DELETE_FULL).unwrap()
        else {
            panic!("expected Delete")
        };
        assert_eq!(relation_id, 16385);
        assert_eq!(old_kind, OldTupleKind::Full);
        assert_eq!(old.columns.len(), 4);
        assert_eq!(old.columns[1], ColumnValue::Text("Bob".into()));
    }

    #[test]
    fn decodes_delete_with_key_only_tuple_carrying_a_slot_per_column() {
        // ncols = 3, not 1: in a 'K' tuple there's an entry for EVERY column of the table,
        // non-key ones are simply filled with 'n'.
        let PgOutputMessage::Delete { old_kind, old, .. } = decode(DELETE_KEY).unwrap() else {
            panic!("expected Delete")
        };
        assert_eq!(old_kind, OldTupleKind::Key);
        assert_eq!(old.columns.len(), 3);
        assert_eq!(old.columns[0], ColumnValue::Text("10".into()));
        assert_eq!(old.columns[1], ColumnValue::Null);
        assert_eq!(old.columns[2], ColumnValue::Null);
    }

    #[test]
    fn delete_without_a_tuple_tag_is_an_error() {
        // For DELETE the tag is mandatory: the deleted row needs to be identified by something.
        let bad = [0x44u8, 0x00, 0x00, 0x40, 0x08, 0x4E, 0x00, 0x00];
        assert!(matches!(decode(&bad), Err(PgcdcError::Decode(_))));
    }

    const TRUNCATE: &[u8] = include_bytes!("../../tests/fixtures/0032_truncate.bin");

    #[test]
    fn truncate_decodes_to_the_relations_it_names() {
        // Captured with pg_logical_slot_peek_binary_changes from `TRUNCATE public.users;`
        // (no CASCADE/RESTART IDENTITY): tag 'T', count 1, flags 0x00, OID 16385 (users) —
        // the same OID as the frozen 0002_relation.bin. 10 bytes, no remainder.
        assert_eq!(TRUNCATE.len(), 10);
        match decode(TRUNCATE).expect("the capture must decode") {
            PgOutputMessage::Truncate { relation_ids } => {
                assert_eq!(
                    relation_ids,
                    vec![16385],
                    "a plain TRUNCATE public.users names exactly the users OID"
                );
            }
            other => panic!("expected Truncate, got {other:?}"),
        }
    }

    const UPDATE_FULL: &[u8] = include_bytes!("../../tests/fixtures/0006_update.bin");
    const UPDATE_NO_OLD: &[u8] = include_bytes!("../../tests/fixtures/0016_update.bin");
    const UPDATE_TOAST: &[u8] = include_bytes!("../../tests/fixtures/0025_update.bin");

    /// UPDATE with tag 'K' — DEFAULT identity and a changed key.
    /// No capture of this form exists (docs/pgoutput-notes.md §14 item 3), so the bytes
    /// are assembled by hand per the layout in §10 and §7: 'U', OID 16392 (items),
    /// 'K', old tuple {id:"10", n, n}, 'N', new tuple {"11","Widget","7"}.
    /// These bytes cannot go into tests/fixtures/ — that holds only real captures.
    const SYNTHETIC_UPDATE_KEY: &[u8] = &[
        0x55, 0x00, 0x00, 0x40, 0x08, // 'U', OID 16392
        0x4B, 0x00, 0x03, // 'K', ncols=3
        0x74, 0x00, 0x00, 0x00, 0x02, 0x31, 0x30, // t(2)="10"
        0x6E, 0x6E, // 'n', 'n' — stubs for non-key columns
        0x4E, 0x00, 0x03, // 'N', ncols=3
        0x74, 0x00, 0x00, 0x00, 0x02, 0x31, 0x31, // t(2)="11"
        0x74, 0x00, 0x00, 0x00, 0x06, 0x57, 0x69, 0x64, 0x67, 0x65, 0x74, // t(6)="Widget"
        0x74, 0x00, 0x00, 0x00, 0x01, 0x37, // t(1)="7"
    ];

    #[test]
    fn decodes_update_with_full_old_tuple() {
        let PgOutputMessage::Update {
            relation_id,
            old,
            new,
        } = decode(UPDATE_FULL).unwrap()
        else {
            panic!("expected Update")
        };
        assert_eq!(relation_id, 16385);
        let (kind, old_tuple) = old.expect("with REPLICA IDENTITY FULL an old tuple exists");
        assert_eq!(kind, OldTupleKind::Full);
        assert_eq!(old_tuple.columns[1], ColumnValue::Text("Alice".into()));
        assert_eq!(new.columns[1], ColumnValue::Text("Bob".into()));
    }

    #[test]
    fn decodes_update_without_an_old_tuple() {
        // Offset 5 is 'N', not 'O'/'K'. One byte distinguishes "has before" from "no before";
        // it cannot be told apart by message length or by counting tags.
        assert_eq!(UPDATE_NO_OLD[5], b'N');
        let PgOutputMessage::Update {
            relation_id,
            old,
            new,
        } = decode(UPDATE_NO_OLD).unwrap()
        else {
            panic!("expected Update")
        };
        assert_eq!(relation_id, 16392);
        assert!(
            old.is_none(),
            "the key did not change — there is no old version of the row at all"
        );
        assert_eq!(
            new.columns,
            vec![
                ColumnValue::Text("10".into()),
                ColumnValue::Text("Widget".into()),
                ColumnValue::Text("7".into()),
            ]
        );
    }

    #[test]
    fn decodes_update_with_key_only_old_tuple() {
        let PgOutputMessage::Update { old, new, .. } = decode(SYNTHETIC_UPDATE_KEY).unwrap() else {
            panic!("expected Update")
        };
        let (kind, old_tuple) = old.expect("tag 'K' produces an old tuple");
        assert_eq!(kind, OldTupleKind::Key);
        assert_eq!(
            old_tuple.columns.len(),
            3,
            "a 'K' tuple has an entry for every column"
        );
        assert_eq!(old_tuple.columns[0], ColumnValue::Text("10".into()));
        assert_eq!(old_tuple.columns[1], ColumnValue::Null, "a stub, not NULL");
        assert_eq!(new.columns[0], ColumnValue::Text("11".into()));
    }

    #[test]
    fn decodes_update_with_unchanged_toast_marker() {
        // Asymmetry: the old tuple carries bio in full (9600 bytes), the new one — a single byte 'u'.
        let PgOutputMessage::Update { old, new, .. } = decode(UPDATE_TOAST).unwrap() else {
            panic!("expected Update")
        };
        let (kind, old_tuple) = old.expect("FULL");
        assert_eq!(kind, OldTupleKind::Full);
        let ColumnValue::Text(old_bio) = &old_tuple.columns[3] else {
            panic!("the old bio must arrive as text")
        };
        assert_eq!(old_bio.len(), 9600);
        assert_eq!(new.columns[3], ColumnValue::UnchangedToast);
        assert_eq!(new.columns[1], ColumnValue::Text("Caroline".into()));
    }

    #[test]
    fn truncated_payload_is_an_error_not_a_panic() {
        let truncated = &BEGIN[..10];
        assert!(matches!(decode(truncated), Err(PgcdcError::Decode(_))));
    }

    #[test]
    fn trailing_bytes_are_an_error() {
        let mut extended = BEGIN.to_vec();
        extended.push(0xFF);
        assert!(matches!(decode(&extended), Err(PgcdcError::Decode(_))));
    }

    #[test]
    fn empty_payload_is_an_error() {
        assert!(matches!(decode(&[]), Err(PgcdcError::Decode(_))));
    }

    #[test]
    fn binary_tagged_column_is_a_typed_decode_error() {
        // Tag 'b' does not appear in any fixture (the binary option was never requested),
        // so the bytes here are synthetic. decode must return a typed
        // error, not silently read the value as text (docs/pgoutput-notes.md §14.2).
        let mut payload = vec![b'I'];
        payload.extend_from_slice(&1u32.to_be_bytes()); // relation_id
        payload.push(b'N'); // INSERT tuple tag
        payload.extend_from_slice(&1i16.to_be_bytes()); // ncols
        payload.push(b'b'); // decode must fail here, without reading the length/data
        assert!(matches!(
            decode(&payload),
            Err(PgcdcError::Decode(msg)) if msg.contains("binary format not supported")
        ));
    }
}
