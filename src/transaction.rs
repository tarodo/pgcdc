use chrono::{DateTime, Utc};

use crate::error::PgcdcError;
use crate::event::{pg_micros_to_utc, BeforeKind, ChangeEvent, Operation, Row};
use crate::lsn::Lsn;
use crate::postgres::pgoutput::{ColumnValue, OldTupleKind, PgOutputMessage, TupleData};
use crate::schema::{Relation, RelationCache};

#[derive(Debug, Clone, PartialEq)]
pub struct Transaction {
    pub xid: u32,
    /// LSN of the commit record itself. Identical for every event in the
    /// transaction, so it groups events by transaction — it is not a key.
    /// The dedup key is `(lsn, event_index)`.
    pub commit_lsn: Lsn,
    /// LSN right after the commit record. THIS is what we acknowledge to PostgreSQL.
    pub end_lsn: Lsn,
    pub commit_timestamp: DateTime<Utc>,
    pub changes: Vec<ChangeEvent>,
}

/// Accumulates changes between BEGIN and COMMIT. Emits nothing outward
/// until it sees COMMIT: PostgreSQL never sends rolled-back transactions at all,
/// but it does send unfinished ones, and those must not be emitted.
#[derive(Debug)]
pub struct Assembler {
    open: Option<OpenTx>,
    max_events: usize,
}

#[derive(Debug)]
struct OpenTx {
    xid: u32,
    changes: Vec<PendingChange>,
}

#[derive(Debug)]
struct PendingChange {
    schema: String,
    table: String,
    operation: Operation,
    before: Option<Row>,
    before_kind: Option<BeforeKind>,
    after: Option<Row>,
    unchanged_columns: Vec<String>,
    lsn: Lsn,
}

impl Assembler {
    pub fn new(max_events: usize) -> Self {
        Self {
            open: None,
            max_events,
        }
    }

    /// Whether the buffer is empty. Emptiness is part of the condition from Q18,
    /// under which keepalive is allowed to advance the slot, but it is NOT the
    /// only part on its own: it is sufficient only as long as write, mark-durable
    /// and ack happen as one synchronous step. Group ACK on a timer breaks that
    /// assumption — see DECISIONS.md Q26(a): for stage 3 the condition is
    /// empty buffer AND processed == durable.
    pub fn is_empty(&self) -> bool {
        self.open.is_none()
    }

    /// How many changes have accumulated in the open transaction. For the
    /// `transaction_buffer_size` counter; does not affect decisions in the code.
    ///
    /// NOT equivalent to `is_empty()`, and this is deliberate: an open transaction
    /// without a single row (for example, right after BEGIN) gives `len() == 0`
    /// while `is_empty()` is already `false`, because one counts buffered
    /// changes, while the other tracks whether a transaction is open at all. The
    /// keepalive slot-advancement gate must stay on `is_empty()`: replacing it
    /// with `len() == 0` would let through an open but still empty transaction
    /// and would allow the slot to advance past not-yet-born rows — a silent
    /// loss of data that would look like a harmless simplification.
    pub fn len(&self) -> usize {
        self.open.as_ref().map_or(0, |o| o.changes.len())
    }

    pub fn reset(&mut self) {
        self.open = None;
    }

    pub fn handle(
        &mut self,
        msg: PgOutputMessage,
        wal_start: Lsn,
        cache: &mut RelationCache,
    ) -> Result<Option<Transaction>, PgcdcError> {
        match msg {
            PgOutputMessage::Relation(rel) => {
                cache.put(rel);
                Ok(None)
            }
            PgOutputMessage::Begin { xid, .. } => {
                self.open = Some(OpenTx {
                    xid,
                    changes: Vec::new(),
                });
                Ok(None)
            }
            PgOutputMessage::Insert { relation_id, tuple } => {
                // The order of checks matters:
                // without an open transaction, the error must be named this way, not
                // "unknown relation", even if the relation isn't in the cache either.
                // The limit is the next check by cost, before hitting the cache and
                // building the row.
                let open = self.open.as_mut().ok_or_else(|| {
                    PgcdcError::Decode("row message outside a transaction".into())
                })?;
                if open.changes.len() >= self.max_events {
                    return Err(PgcdcError::TransactionTooLarge {
                        limit: self.max_events,
                    });
                }
                let rel = cache
                    .get(relation_id)
                    .ok_or(PgcdcError::UnknownRelation { relation_id })?;
                let (after, unchanged) = build_full_row(rel, &tuple)?;
                if !unchanged.is_empty() {
                    // This tag never arrives on INSERT: the value is written in the
                    // same transaction and the reorder buffer resolves it. If it
                    // shows up anyway — that's not our case, and staying silent is not an option.
                    return Err(PgcdcError::Decode(format!(
                        "unexpected unchanged-TOAST markers on INSERT: {unchanged:?}"
                    )));
                }
                let pending = PendingChange {
                    schema: rel.namespace.clone(),
                    table: rel.name.clone(),
                    operation: Operation::Insert,
                    before: None,
                    before_kind: None,
                    after: Some(after),
                    unchanged_columns: Vec::new(),
                    lsn: wal_start,
                };
                open.changes.push(pending);
                Ok(None)
            }
            PgOutputMessage::Update {
                relation_id,
                old,
                new,
            } => {
                let open = self.open.as_mut().ok_or_else(|| {
                    PgcdcError::Decode("row message outside a transaction".into())
                })?;
                if open.changes.len() >= self.max_events {
                    return Err(PgcdcError::TransactionTooLarge {
                        limit: self.max_events,
                    });
                }
                let rel = cache
                    .get(relation_id)
                    .ok_or(PgcdcError::UnknownRelation { relation_id })?;
                let (before, before_kind) = match &old {
                    Some((OldTupleKind::Full, tuple)) => {
                        let (row, unchanged) = build_full_row(rel, tuple)?;
                        reject_unchanged_toast_in_full_old_tuple(&unchanged)?;
                        (Some(row), Some(BeforeKind::Full))
                    }
                    Some((OldTupleKind::Key, tuple)) => {
                        (Some(build_key_row(rel, tuple)?), Some(BeforeKind::Key))
                    }
                    None => (None, None),
                };
                let (after, unchanged_columns) = build_full_row(rel, &new)?;
                open.changes.push(PendingChange {
                    schema: rel.namespace.clone(),
                    table: rel.name.clone(),
                    operation: Operation::Update,
                    before,
                    before_kind,
                    after: Some(after),
                    unchanged_columns,
                    lsn: wal_start,
                });
                Ok(None)
            }
            PgOutputMessage::Delete {
                relation_id,
                old_kind,
                old,
            } => {
                let open = self.open.as_mut().ok_or_else(|| {
                    PgcdcError::Decode("row message outside a transaction".into())
                })?;
                if open.changes.len() >= self.max_events {
                    return Err(PgcdcError::TransactionTooLarge {
                        limit: self.max_events,
                    });
                }
                let rel = cache
                    .get(relation_id)
                    .ok_or(PgcdcError::UnknownRelation { relation_id })?;
                let (before, before_kind) = match old_kind {
                    OldTupleKind::Full => {
                        let (row, unchanged) = build_full_row(rel, &old)?;
                        reject_unchanged_toast_in_full_old_tuple(&unchanged)?;
                        (Some(row), Some(BeforeKind::Full))
                    }
                    OldTupleKind::Key => (Some(build_key_row(rel, &old)?), Some(BeforeKind::Key)),
                };
                open.changes.push(PendingChange {
                    schema: rel.namespace.clone(),
                    table: rel.name.clone(),
                    operation: Operation::Delete,
                    before,
                    before_kind,
                    after: None,
                    unchanged_columns: Vec::new(),
                    lsn: wal_start,
                });
                Ok(None)
            }
            PgOutputMessage::Commit {
                commit_lsn,
                end_lsn,
                commit_timestamp,
                ..
            } => {
                let open = self
                    .open
                    .take()
                    .ok_or_else(|| PgcdcError::Decode("COMMIT without BEGIN".into()))?;
                let ts = pg_micros_to_utc(commit_timestamp);
                let changes = open
                    .changes
                    .into_iter()
                    .enumerate()
                    .map(|(event_index, c)| ChangeEvent {
                        schema: c.schema,
                        table: c.table,
                        operation: c.operation,
                        before: c.before,
                        before_kind: c.before_kind,
                        after: c.after,
                        unchanged_columns: c.unchanged_columns,
                        transaction_id: open.xid,
                        // The buffer's position is the source of truth: it needs
                        // no separate counter that could drift from its length,
                        // and it reproduces identically on redelivery because the
                        // slot replays the same transaction with the same
                        // changes in the same order.
                        //
                        // usize -> u32 truncates only past 4_294_967_296 events in
                        // one transaction: `self.max_events` (`--max-transaction-events`,
                        // default 100_000) refuses the buffer long before that, and
                        // even with that flag raised past u32::MAX, holding that many
                        // buffered `PendingChange`s would exhaust RAM first.
                        event_index: event_index as u32,
                        lsn: c.lsn,
                        commit_lsn: Lsn(commit_lsn),
                        commit_timestamp: ts,
                    })
                    .collect();
                Ok(Some(Transaction {
                    xid: open.xid,
                    commit_lsn: Lsn(commit_lsn),
                    end_lsn: Lsn(end_lsn),
                    commit_timestamp: ts,
                    changes,
                }))
            }
            // One TRUNCATE message can name several tables; it becomes one
            // event per relation so every event still carries exactly one
            // schema and one table, as every consumer of this output assumes.
            // Neither tuple has row identity here — a truncate says "this
            // table is now empty", not "these rows are gone" — so before and
            // after both stay None, same as the row arms leave them None
            // where the tag they saw doesn't supply one.
            PgOutputMessage::Truncate { relation_ids } => {
                let open = self.open.as_mut().ok_or_else(|| {
                    PgcdcError::Decode("TRUNCATE message outside a transaction".into())
                })?;
                for relation_id in relation_ids {
                    if open.changes.len() >= self.max_events {
                        return Err(PgcdcError::TransactionTooLarge {
                            limit: self.max_events,
                        });
                    }
                    // Same lookup as the row arms, so an id the RELATION
                    // message never announced is the same fatal UnknownRelation.
                    let rel = cache
                        .get(relation_id)
                        .ok_or(PgcdcError::UnknownRelation { relation_id })?;
                    open.changes.push(PendingChange {
                        schema: rel.namespace.clone(),
                        table: rel.name.clone(),
                        operation: Operation::Truncate,
                        before: None,
                        before_kind: None,
                        after: None,
                        unchanged_columns: Vec::new(),
                        lsn: wal_start,
                    });
                }
                Ok(None)
            }
        }
    }
}

/// A full tuple — tag `'N'` or `'O'`. It carries an entry for every column.
/// `'n'` here means a real SQL NULL. `'u'` means the server did not forward
/// an unchanged TOAST value: the column doesn't make it into the row at all, its name
/// is returned as the second element so it can end up in `unchanged_columns`.
/// Writing it as `null` would be silent corruption — the consumer would conclude the value was nulled out.
fn build_full_row(rel: &Relation, tuple: &TupleData) -> Result<(Row, Vec<String>), PgcdcError> {
    check_arity(rel, tuple)?;
    let mut row = Row::new();
    let mut unchanged = Vec::new();
    for (col, value) in rel.columns.iter().zip(&tuple.columns) {
        match value {
            ColumnValue::Text(s) => {
                row.insert(col.name.clone(), serde_json::Value::String(s.clone()));
            }
            ColumnValue::Null => {
                row.insert(col.name.clone(), serde_json::Value::Null);
            }
            ColumnValue::UnchangedToast => unchanged.push(col.name.clone()),
        }
    }
    Ok((row, unchanged))
}

/// The `'K'` tuple — replica identity only. The number of elements equals the number
/// of columns in the table, but non-key ones are filled with `'n'`, and that is NOT
/// NULL, it means "the server did not send it". So only what actually arrived makes it into the row.
fn build_key_row(rel: &Relation, tuple: &TupleData) -> Result<Row, PgcdcError> {
    check_arity(rel, tuple)?;
    let mut row = Row::new();
    for (col, value) in rel.columns.iter().zip(&tuple.columns) {
        if let ColumnValue::Text(s) = value {
            row.insert(col.name.clone(), serde_json::Value::String(s.clone()));
        }
    }
    Ok(row)
}

fn check_arity(rel: &Relation, tuple: &TupleData) -> Result<(), PgcdcError> {
    if tuple.columns.len() != rel.columns.len() {
        return Err(PgcdcError::Decode(format!(
            "tuple has {} columns, relation {} has {}",
            tuple.columns.len(),
            rel.id,
            rel.columns.len()
        )));
    }
    Ok(())
}

/// A full old tuple (tag `'O'`, UPDATE or DELETE) must not carry an
/// unchanged-TOAST marker. On PostgreSQL 16 this is unreachable: the server
/// flattens external (TOASTed) attributes into the old row in full before the
/// plugin sees it — this is exactly why the frozen `'u'`-marker capture
/// (`tests/fixtures/0025_update.bin`) carries the full 9600 bytes in the old
/// tuple specifically, and the single-byte marker only in the new one. But
/// unreachability on today's server is an assumption about its behavior, not
/// a guarantee; silently dropping a column without a record of it, should
/// that assumption ever break, is exactly the option rejected in Q15.
/// So we check it as an invariant.
fn reject_unchanged_toast_in_full_old_tuple(unchanged: &[String]) -> Result<(), PgcdcError> {
    if !unchanged.is_empty() {
        return Err(PgcdcError::Decode(format!(
            "unexpected unchanged-TOAST markers in a full old tuple: {unchanged:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::pgoutput::{ColumnValue, PgOutputMessage, TupleData};
    use crate::schema::{Column, Relation};

    fn users_relation() -> Relation {
        Relation {
            id: 16385,
            namespace: "public".into(),
            name: "users".into(),
            replica_identity: b'f',
            columns: ["id", "name"]
                .iter()
                .map(|c| Column {
                    name: (*c).into(),
                    is_key: true,
                    type_oid: 25,
                    atttypmod: -1,
                })
                .collect(),
        }
    }

    fn items_relation() -> Relation {
        Relation {
            id: 16392,
            namespace: "public".into(),
            name: "items".into(),
            replica_identity: b'd',
            columns: vec![
                Column {
                    name: "id".into(),
                    is_key: true,
                    type_oid: 20,
                    atttypmod: -1,
                },
                Column {
                    name: "title".into(),
                    is_key: false,
                    type_oid: 25,
                    atttypmod: -1,
                },
                Column {
                    name: "qty".into(),
                    is_key: false,
                    type_oid: 23,
                    atttypmod: -1,
                },
            ],
        }
    }

    #[test]
    fn key_tuple_omits_columns_the_server_did_not_send() {
        // In 0019_delete.bin the row at the time of deletion had title='Widget', qty=7.
        // Both arrived as 'n'. Reporting them as null would be lying about the data: the
        // values existed, the server simply did not send them.
        let tuple = TupleData {
            columns: vec![
                ColumnValue::Text("10".into()),
                ColumnValue::Null,
                ColumnValue::Null,
            ],
        };
        let row = build_key_row(&items_relation(), &tuple).unwrap();
        assert_eq!(row.len(), 1, "only the column that arrived");
        assert_eq!(row.get("id").unwrap(), "10");
        assert!(
            !row.contains_key("title"),
            "title is absent, not equal to null"
        );
        assert!(!row.contains_key("qty"), "qty is absent, not equal to null");
    }

    #[test]
    fn full_tuple_keeps_real_nulls_and_reports_unchanged_toast() {
        let tuple = TupleData {
            columns: vec![
                ColumnValue::Text("10".into()),
                ColumnValue::Null,
                ColumnValue::UnchangedToast,
            ],
        };
        let (row, unchanged) = build_full_row(&items_relation(), &tuple).unwrap();
        assert!(
            row.get("title").unwrap().is_null(),
            "'n' in a full tuple is a real NULL"
        );
        assert!(
            !row.contains_key("qty"),
            "'u' does not end up in the row at all"
        );
        assert_eq!(unchanged, vec!["qty".to_string()]);
    }

    fn begin(xid: u32) -> PgOutputMessage {
        PgOutputMessage::Begin {
            final_lsn: 0x1000,
            commit_timestamp: 841_423_351_314_489,
            xid,
        }
    }

    fn commit() -> PgOutputMessage {
        PgOutputMessage::Commit {
            flags: 0,
            commit_lsn: 0x1000,
            end_lsn: 0x1030,
            commit_timestamp: 841_423_351_314_489,
        }
    }

    fn insert() -> PgOutputMessage {
        PgOutputMessage::Insert {
            relation_id: 16385,
            tuple: TupleData {
                columns: vec![ColumnValue::Text("1".into()), ColumnValue::Null],
            },
        }
    }

    #[test]
    fn nothing_is_emitted_before_commit() {
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        assert!(a
            .handle(begin(737), Lsn(0x100), &mut cache)
            .unwrap()
            .is_none());
        assert!(a
            .handle(
                PgOutputMessage::Relation(users_relation()),
                Lsn(0),
                &mut cache
            )
            .unwrap()
            .is_none());
        assert!(a
            .handle(insert(), Lsn(0x200), &mut cache)
            .unwrap()
            .is_none());
        assert!(
            !a.is_empty(),
            "an open transaction keeps the buffer non-empty"
        );
    }

    #[test]
    fn commit_emits_the_whole_transaction() {
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Relation(users_relation()),
            Lsn(0),
            &mut cache,
        )
        .unwrap();
        a.handle(insert(), Lsn(0x200), &mut cache).unwrap();
        let tx = a
            .handle(commit(), Lsn(0x1000), &mut cache)
            .unwrap()
            .expect("commit emits the transaction");
        assert_eq!(tx.xid, 737);
        assert_eq!(tx.commit_lsn, Lsn(0x1000));
        assert_eq!(
            tx.end_lsn,
            Lsn(0x1030),
            "end_lsn is separate from commit_lsn"
        );
        assert_eq!(tx.changes.len(), 1);
        let ev = &tx.changes[0];
        assert_eq!(ev.table, "users");
        assert_eq!(ev.transaction_id, 737);
        assert_eq!(
            ev.lsn,
            Lsn(0x200),
            "the event carries its own row's wal_start"
        );
        assert_eq!(
            ev.commit_lsn,
            Lsn(0x1000),
            "while commit_lsn is shared across the transaction"
        );
        assert!(a.is_empty(), "after commit the buffer is empty");
    }

    #[test]
    fn buffer_length_grows_with_changes_and_empties_on_commit() {
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        assert_eq!(a.len(), 0);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Relation(users_relation()),
            Lsn(0),
            &mut cache,
        )
        .unwrap();
        assert_eq!(a.len(), 0, "BEGIN by itself does not add any changes");
        a.handle(insert(), Lsn(0x200), &mut cache).unwrap();
        assert_eq!(a.len(), 1);
        a.handle(commit(), Lsn(0x1000), &mut cache).unwrap();
        assert_eq!(a.len(), 0, "commit empties the buffer");
    }

    #[test]
    fn column_names_come_from_the_relation_by_position() {
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Relation(users_relation()),
            Lsn(0),
            &mut cache,
        )
        .unwrap();
        a.handle(insert(), Lsn(0x200), &mut cache).unwrap();
        let tx = a
            .handle(commit(), Lsn(0x1000), &mut cache)
            .unwrap()
            .unwrap();
        let after = tx.changes[0].after.as_ref().unwrap();
        assert_eq!(after.get("id").unwrap(), "1");
        assert!(
            after.get("name").unwrap().is_null(),
            "SQL NULL becomes JSON null"
        );
    }

    #[test]
    fn row_outside_a_transaction_wins_over_unknown_relation() {
        // Without BEGIN the error must be "row message outside a transaction",
        // not UnknownRelation, even if the relation isn't in the cache either:
        // the order of checks in the Insert arm — open, then limit, then relation lookup.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        let err = a.handle(insert(), Lsn(0x200), &mut cache).unwrap_err();
        assert!(
            matches!(&err, PgcdcError::Decode(msg) if msg.contains("outside a transaction")),
            "got {err:?}"
        );
    }

    #[test]
    fn truncate_outside_a_transaction_does_not_call_itself_a_row_message() {
        // TRUNCATE copied its guard from the row arms (Insert/Update/Delete), which
        // correctly call themselves "row message" — but a TRUNCATE is not a row
        // message, and the diagnostic must not claim it is. It still needs to say
        // "outside a transaction" so the failure mode reads the same as the row arms'.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        let err = a
            .handle(
                PgOutputMessage::Truncate {
                    relation_ids: vec![16385],
                },
                Lsn(0x200),
                &mut cache,
            )
            .unwrap_err();
        let PgcdcError::Decode(msg) = &err else {
            panic!("got {err:?}")
        };
        assert!(msg.contains("outside a transaction"), "got {msg:?}");
        assert!(
            !msg.contains("row message"),
            "TRUNCATE is not a row message, got {msg:?}"
        );
    }

    #[test]
    fn row_for_unknown_relation_is_fatal() {
        // A failed relation lookup is a fatal error per spec §15,
        // not a reason to skip the row.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        let err = a.handle(insert(), Lsn(0x200), &mut cache).unwrap_err();
        assert!(matches!(
            err,
            PgcdcError::UnknownRelation { relation_id: 16385 }
        ));
    }

    #[test]
    fn transaction_larger_than_the_limit_fails_loudly() {
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(2);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Relation(users_relation()),
            Lsn(0),
            &mut cache,
        )
        .unwrap();
        a.handle(insert(), Lsn(0x200), &mut cache).unwrap();
        a.handle(insert(), Lsn(0x210), &mut cache).unwrap();
        let err = a.handle(insert(), Lsn(0x220), &mut cache).unwrap_err();
        assert!(matches!(err, PgcdcError::TransactionTooLarge { limit: 2 }));
    }

    #[test]
    fn reset_drops_a_half_assembled_transaction() {
        // On reconnect, an incompletely assembled transaction is discarded: its BEGIN was
        // after confirmed_flush_lsn, so it will arrive again in full.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        assert!(!a.is_empty());
        a.reset();
        assert!(a.is_empty());
    }

    #[test]
    fn relation_outside_a_transaction_is_accepted() {
        // RELATION arrives inside a transaction in our fixtures, but the cache is
        // session-scoped, and the message is not required to be part of a transaction.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        assert!(a
            .handle(
                PgOutputMessage::Relation(users_relation()),
                Lsn(0),
                &mut cache
            )
            .unwrap()
            .is_none());
        assert_eq!(cache.len(), 1);
        assert!(a.is_empty(), "RELATION does not open a transaction");
    }

    #[test]
    fn column_count_mismatch_is_a_decode_error() {
        // check_arity must reject a tuple whose column count disagrees with
        // the relation. Without that guard, zip would silently truncate to the
        // shorter side, producing quietly wrong rows instead of an error.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Relation(users_relation()),
            Lsn(0),
            &mut cache,
        )
        .unwrap();
        // users_relation has 2 columns, but we feed only 1
        let mismatched_insert = PgOutputMessage::Insert {
            relation_id: 16385,
            tuple: TupleData {
                columns: vec![ColumnValue::Text("1".into())],
            },
        };
        let err = a
            .handle(mismatched_insert, Lsn(0x200), &mut cache)
            .unwrap_err();
        // Verify the error carries both counts so the message stays diagnostic
        assert!(
            matches!(err, PgcdcError::Decode(msg) if msg.contains("1 columns") && msg.contains("2"))
        );
    }

    fn users_relation_full() -> Relation {
        Relation {
            id: 16385,
            namespace: "public".into(),
            name: "users".into(),
            replica_identity: b'f',
            columns: vec![
                Column {
                    name: "id".into(),
                    is_key: true,
                    type_oid: 20,
                    atttypmod: -1,
                },
                Column {
                    name: "bio".into(),
                    is_key: true,
                    type_oid: 25,
                    atttypmod: -1,
                },
            ],
        }
    }

    #[test]
    fn update_with_full_old_tuple_reports_before_kind_full() {
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Relation(users_relation_full()),
            Lsn(0),
            &mut cache,
        )
        .unwrap();
        a.handle(
            PgOutputMessage::Update {
                relation_id: 16385,
                old: Some((
                    OldTupleKind::Full,
                    TupleData {
                        columns: vec![
                            ColumnValue::Text("2".into()),
                            ColumnValue::Text("old bio".into()),
                        ],
                    },
                )),
                new: TupleData {
                    columns: vec![ColumnValue::Text("2".into()), ColumnValue::UnchangedToast],
                },
            },
            Lsn(0x200),
            &mut cache,
        )
        .unwrap();
        let tx = a
            .handle(commit(), Lsn(0x1000), &mut cache)
            .unwrap()
            .unwrap();
        let ev = &tx.changes[0];
        assert_eq!(ev.operation, Operation::Update);
        assert_eq!(ev.before_kind, Some(BeforeKind::Full));
        assert_eq!(ev.before.as_ref().unwrap().get("bio").unwrap(), "old bio");
        assert!(
            !ev.after.as_ref().unwrap().contains_key("bio"),
            "an unsent TOAST value does not end up in after"
        );
        assert_eq!(ev.unchanged_columns, vec!["bio".to_string()]);
        assert_eq!(
            ev.lsn,
            Lsn(0x200),
            "lsn is the position of the row itself (wal_start), not Lsn(0)"
        );
    }

    #[test]
    fn update_without_an_old_tuple_reports_no_before_at_all() {
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Relation(items_relation()),
            Lsn(0),
            &mut cache,
        )
        .unwrap();
        a.handle(
            PgOutputMessage::Update {
                relation_id: 16392,
                old: None,
                new: TupleData {
                    columns: vec![
                        ColumnValue::Text("10".into()),
                        ColumnValue::Text("Widget".into()),
                        ColumnValue::Text("7".into()),
                    ],
                },
            },
            Lsn(0x200),
            &mut cache,
        )
        .unwrap();
        let tx = a
            .handle(commit(), Lsn(0x1000), &mut cache)
            .unwrap()
            .unwrap();
        let ev = &tx.changes[0];
        assert!(ev.before.is_none());
        assert_eq!(ev.before_kind, None);
        assert_eq!(ev.after.as_ref().unwrap().get("qty").unwrap(), "7");
    }

    #[test]
    fn delete_with_key_tuple_reports_only_the_columns_that_arrived() {
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Relation(items_relation()),
            Lsn(0),
            &mut cache,
        )
        .unwrap();
        a.handle(
            PgOutputMessage::Delete {
                relation_id: 16392,
                old_kind: OldTupleKind::Key,
                old: TupleData {
                    columns: vec![
                        ColumnValue::Text("10".into()),
                        ColumnValue::Null,
                        ColumnValue::Null,
                    ],
                },
            },
            Lsn(0x200),
            &mut cache,
        )
        .unwrap();
        let tx = a
            .handle(commit(), Lsn(0x1000), &mut cache)
            .unwrap()
            .unwrap();
        let ev = &tx.changes[0];
        assert_eq!(ev.operation, Operation::Delete);
        assert_eq!(ev.before_kind, Some(BeforeKind::Key));
        assert!(ev.after.is_none(), "DELETE has no new tuple");
        assert_eq!(
            ev.lsn,
            Lsn(0x200),
            "the event carries its own row's wal_start"
        );
        let before = ev.before.as_ref().unwrap();
        assert_eq!(before.len(), 1);
        assert!(
            !before.contains_key("title"),
            "a stub does not turn into null"
        );
    }

    #[test]
    fn serialized_delete_event_matches_the_contract() {
        // Checking the outward shape, not just internal structures.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Relation(items_relation()),
            Lsn(0),
            &mut cache,
        )
        .unwrap();
        a.handle(
            PgOutputMessage::Delete {
                relation_id: 16392,
                old_kind: OldTupleKind::Key,
                old: TupleData {
                    columns: vec![
                        ColumnValue::Text("10".into()),
                        ColumnValue::Null,
                        ColumnValue::Null,
                    ],
                },
            },
            Lsn(0x200),
            &mut cache,
        )
        .unwrap();
        let tx = a
            .handle(commit(), Lsn(0x1000), &mut cache)
            .unwrap()
            .unwrap();
        let json = serde_json::to_string(&tx.changes[0]).unwrap();
        assert!(json.contains(r#""operation":"delete""#));
        assert!(json.contains(r#""before_kind":"key""#));
        assert!(json.contains(r#""before":{"id":"10"}"#));
        assert!(json.contains(r#""after":null"#));
        assert!(json.contains(r#""unchanged_columns":[]"#));
    }

    #[test]
    fn update_with_key_only_old_tuple_omits_unsent_columns() {
        // Swapping build_key_row for build_full_row on the Key arm would
        // turn the server's "did not send" stub into a lying `null` for
        // title and qty. before must carry only the column that arrived.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Relation(items_relation()),
            Lsn(0),
            &mut cache,
        )
        .unwrap();
        a.handle(
            PgOutputMessage::Update {
                relation_id: 16392,
                old: Some((
                    OldTupleKind::Key,
                    TupleData {
                        columns: vec![
                            ColumnValue::Text("10".into()),
                            ColumnValue::Null,
                            ColumnValue::Null,
                        ],
                    },
                )),
                new: TupleData {
                    columns: vec![
                        ColumnValue::Text("10".into()),
                        ColumnValue::Text("Widget2".into()),
                        ColumnValue::Text("8".into()),
                    ],
                },
            },
            Lsn(0x200),
            &mut cache,
        )
        .unwrap();
        let tx = a
            .handle(commit(), Lsn(0x1000), &mut cache)
            .unwrap()
            .unwrap();
        let ev = &tx.changes[0];
        assert_eq!(ev.before_kind, Some(BeforeKind::Key));
        let before = ev.before.as_ref().unwrap();
        assert_eq!(before.len(), 1, "only the column that arrived");
        assert!(
            !before.contains_key("title"),
            "a stub does not turn into null"
        );
    }

    #[test]
    fn delete_with_full_old_tuple_keeps_real_nulls() {
        // Collapsing this arm to build_key_row + BeforeKind::Key would
        // both mislabel before_kind and silently drop a genuinely-NULL
        // column: this test's old tuple carries title = NULL as a real 'n'
        // under an 'O' tag on the three-column items relation, and that
        // null is meaningful.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Relation(items_relation()),
            Lsn(0),
            &mut cache,
        )
        .unwrap();
        a.handle(
            PgOutputMessage::Delete {
                relation_id: 16392,
                old_kind: OldTupleKind::Full,
                old: TupleData {
                    columns: vec![
                        ColumnValue::Text("10".into()),
                        ColumnValue::Null,
                        ColumnValue::Text("7".into()),
                    ],
                },
            },
            Lsn(0x200),
            &mut cache,
        )
        .unwrap();
        let tx = a
            .handle(commit(), Lsn(0x1000), &mut cache)
            .unwrap()
            .unwrap();
        let ev = &tx.changes[0];
        assert_eq!(ev.before_kind, Some(BeforeKind::Full));
        let before = ev.before.as_ref().unwrap();
        assert!(
            before.get("title").unwrap().is_null(),
            "a real NULL in a full old tuple must stay null"
        );
    }

    #[test]
    fn delete_key_tuple_arity_mismatch_is_a_decode_error() {
        // check_arity must also guard the key path. A short key tuple
        // would otherwise zip-truncate to the shorter side in silence.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Relation(items_relation()),
            Lsn(0),
            &mut cache,
        )
        .unwrap();
        let err = a
            .handle(
                PgOutputMessage::Delete {
                    relation_id: 16392,
                    old_kind: OldTupleKind::Key,
                    old: TupleData {
                        columns: vec![ColumnValue::Text("10".into()), ColumnValue::Null],
                    },
                },
                Lsn(0x200),
                &mut cache,
            )
            .unwrap_err();
        assert!(matches!(err, PgcdcError::Decode(_)), "got {err:?}");
    }

    #[test]
    fn insert_rejects_unchanged_toast_marker() {
        // 'u' cannot legitimately arrive on an INSERT — the value is
        // written in the same transaction and the reorder buffer resolves
        // it before the plugin sees it. Silence here would be the worst
        // response, so the guard must actually fire.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Relation(items_relation()),
            Lsn(0),
            &mut cache,
        )
        .unwrap();
        let err = a
            .handle(
                PgOutputMessage::Insert {
                    relation_id: 16392,
                    tuple: TupleData {
                        columns: vec![
                            ColumnValue::Text("10".into()),
                            ColumnValue::UnchangedToast,
                            ColumnValue::Text("7".into()),
                        ],
                    },
                },
                Lsn(0x200),
                &mut cache,
            )
            .unwrap_err();
        assert!(
            matches!(&err, PgcdcError::Decode(msg) if msg.contains("title")),
            "got {err:?}"
        );
    }

    #[test]
    fn update_full_old_tuple_rejects_unchanged_toast_marker() {
        // This arm used to do `let (row, _) = build_full_row(...)`, throwing
        // away the unchanged-TOAST list. Unreachable on PostgreSQL 16 — the server
        // flattens external attributes into the old-tuple WAL image before the
        // plugin sees it — but that is an assumption about server behaviour, not
        // a guarantee, so it must be an enforced invariant, not silent trust.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Relation(items_relation()),
            Lsn(0),
            &mut cache,
        )
        .unwrap();
        let err = a
            .handle(
                PgOutputMessage::Update {
                    relation_id: 16392,
                    old: Some((
                        OldTupleKind::Full,
                        TupleData {
                            columns: vec![
                                ColumnValue::Text("10".into()),
                                ColumnValue::UnchangedToast,
                                ColumnValue::Text("7".into()),
                            ],
                        },
                    )),
                    new: TupleData {
                        columns: vec![
                            ColumnValue::Text("10".into()),
                            ColumnValue::Text("Widget".into()),
                            ColumnValue::Text("8".into()),
                        ],
                    },
                },
                Lsn(0x200),
                &mut cache,
            )
            .unwrap_err();
        assert!(
            matches!(&err, PgcdcError::Decode(msg) if msg.contains("title")),
            "got {err:?}"
        );
    }

    #[test]
    fn delete_full_old_tuple_rejects_unchanged_toast_marker() {
        // Same guard, DELETE's Full arm. Before the fix it silently dropped
        // the unchanged-TOAST list the same way the UPDATE arm did.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Relation(items_relation()),
            Lsn(0),
            &mut cache,
        )
        .unwrap();
        let err = a
            .handle(
                PgOutputMessage::Delete {
                    relation_id: 16392,
                    old_kind: OldTupleKind::Full,
                    old: TupleData {
                        columns: vec![
                            ColumnValue::Text("10".into()),
                            ColumnValue::UnchangedToast,
                            ColumnValue::Text("7".into()),
                        ],
                    },
                },
                Lsn(0x200),
                &mut cache,
            )
            .unwrap_err();
        assert!(
            matches!(&err, PgcdcError::Decode(msg) if msg.contains("title")),
            "got {err:?}"
        );
    }

    #[test]
    fn update_respects_the_max_events_limit() {
        // The max_events guard is duplicated per match arm. A test that only
        // ever sends INSERT (transaction_larger_than_the_limit_fails_loudly)
        // exercises none of the copy living in the Update arm — deleting that
        // copy would leave the whole suite green without this test.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Relation(items_relation()),
            Lsn(0),
            &mut cache,
        )
        .unwrap();
        let update_msg = || PgOutputMessage::Update {
            relation_id: 16392,
            old: None,
            new: TupleData {
                columns: vec![
                    ColumnValue::Text("10".into()),
                    ColumnValue::Text("Widget".into()),
                    ColumnValue::Text("7".into()),
                ],
            },
        };
        a.handle(update_msg(), Lsn(0x200), &mut cache).unwrap();
        let err = a.handle(update_msg(), Lsn(0x210), &mut cache).unwrap_err();
        assert!(matches!(err, PgcdcError::TransactionTooLarge { limit: 1 }));
    }

    #[test]
    fn a_truncate_becomes_one_event_per_relation() {
        // A single TRUNCATE can name several tables. One event each keeps every
        // event carrying exactly one schema and table, which every consumer of
        // this output already assumes.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(100);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Relation(users_relation()),
            Lsn(0),
            &mut cache,
        )
        .unwrap();
        a.handle(
            PgOutputMessage::Relation(items_relation()),
            Lsn(0),
            &mut cache,
        )
        .unwrap();
        a.handle(
            PgOutputMessage::Truncate {
                relation_ids: vec![16385, 16392],
            },
            Lsn(0x200),
            &mut cache,
        )
        .unwrap();
        let tx = a
            .handle(commit(), Lsn(0x1000), &mut cache)
            .unwrap()
            .unwrap();
        assert_eq!(tx.changes.len(), 2, "one event per relation named");
        for ev in &tx.changes {
            assert_eq!(ev.operation, Operation::Truncate);
            assert!(ev.before.is_none(), "a truncate has no row identity");
            assert!(ev.before_kind.is_none());
            assert!(ev.after.is_none());
        }
        let tables: Vec<&str> = tx.changes.iter().map(|ev| ev.table.as_str()).collect();
        assert_eq!(tables, vec!["users", "items"]);
    }

    #[test]
    fn every_event_in_a_transaction_gets_a_distinct_index() {
        // One TRUNCATE naming two tables becomes two events that share a WAL
        // position, so the index is what tells them apart. Row changes in the
        // same transaction must keep counting from the same sequence — the
        // ordinal is per transaction, not per message.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(100);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Relation(users_relation()),
            Lsn(0),
            &mut cache,
        )
        .unwrap();
        a.handle(
            PgOutputMessage::Relation(items_relation()),
            Lsn(0),
            &mut cache,
        )
        .unwrap();
        a.handle(insert(), Lsn(0x200), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Truncate {
                relation_ids: vec![16385, 16392],
            },
            Lsn(0x300),
            &mut cache,
        )
        .unwrap();
        let tx = a
            .handle(commit(), Lsn(0x1000), &mut cache)
            .unwrap()
            .unwrap();
        assert_eq!(tx.changes.len(), 3);
        let indices: Vec<u32> = tx.changes.iter().map(|ev| ev.event_index).collect();
        assert_eq!(
            indices,
            vec![0, 1, 2],
            "indices follow emission order across the whole transaction"
        );
        let truncates: Vec<&ChangeEvent> = tx
            .changes
            .iter()
            .filter(|ev| ev.operation == Operation::Truncate)
            .collect();
        assert_eq!(truncates.len(), 2);
        assert_eq!(
            truncates[0].lsn, truncates[1].lsn,
            "both truncate events carry the one message's wal_start"
        );
        assert_ne!(
            truncates[0].event_index, truncates[1].event_index,
            "event_index is what tells the two truncate events apart"
        );
    }

    #[test]
    fn delete_respects_the_max_events_limit() {
        // Same guard, DELETE arm's own copy.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Relation(items_relation()),
            Lsn(0),
            &mut cache,
        )
        .unwrap();
        let delete_msg = || PgOutputMessage::Delete {
            relation_id: 16392,
            old_kind: OldTupleKind::Key,
            old: TupleData {
                columns: vec![
                    ColumnValue::Text("10".into()),
                    ColumnValue::Null,
                    ColumnValue::Null,
                ],
            },
        };
        a.handle(delete_msg(), Lsn(0x200), &mut cache).unwrap();
        let err = a.handle(delete_msg(), Lsn(0x210), &mut cache).unwrap_err();
        assert!(matches!(err, PgcdcError::TransactionTooLarge { limit: 1 }));
    }

    #[test]
    fn truncate_respects_the_max_events_limit() {
        // Same guard, TRUNCATE arm's own copy. Unlike the other three arms this
        // one loops — one message can name several relations — so the stronger
        // property to check is not just "eventually errors" but "errors exactly
        // on the relation that overflows the limit", leaving the ones that fit
        // already buffered rather than either silently dropping the whole
        // message or letting it all through uncounted.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Relation(users_relation()),
            Lsn(0),
            &mut cache,
        )
        .unwrap();
        a.handle(
            PgOutputMessage::Relation(items_relation()),
            Lsn(0),
            &mut cache,
        )
        .unwrap();
        let err = a
            .handle(
                PgOutputMessage::Truncate {
                    relation_ids: vec![16385, 16392],
                },
                Lsn(0x200),
                &mut cache,
            )
            .unwrap_err();
        assert!(matches!(err, PgcdcError::TransactionTooLarge { limit: 1 }));
        assert_eq!(
            a.len(),
            1,
            "the first relation was buffered before the second hit the limit"
        );
    }
}
