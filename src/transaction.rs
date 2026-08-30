use chrono::{DateTime, Utc};

use crate::error::PgcdcError;
use crate::event::{pg_micros_to_utc, ChangeEvent, Operation, Row};
use crate::lsn::Lsn;
use crate::postgres::pgoutput::{ColumnValue, PgOutputMessage, TupleData};
use crate::schema::{Relation, RelationCache};

#[derive(Debug, Clone, PartialEq)]
pub struct Transaction {
    pub xid: u32,
    /// LSN самой записи коммита. Идёт в JSON как ключ дедупликации.
    pub commit_lsn: Lsn,
    /// LSN сразу за записью коммита. ЭТО подтверждаем PostgreSQL.
    pub end_lsn: Lsn,
    pub commit_timestamp: DateTime<Utc>,
    pub changes: Vec<ChangeEvent>,
}

/// Накапливает изменения между BEGIN и COMMIT. Ничего не отдаёт наружу,
/// пока не увидит COMMIT: откаченные транзакции PostgreSQL не присылает вовсе,
/// но незавершённые — вполне, и отдавать их нельзя.
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
    after: Row,
    lsn: Lsn,
}

impl Assembler {
    pub fn new(max_events: usize) -> Self {
        Self {
            open: None,
            max_events,
        }
    }

    /// Пуст ли буфер. От этого зависит правило keepalive (DECISIONS Q18):
    /// подтверждать позицию из keepalive можно ТОЛЬКО при пустом буфере.
    pub fn is_empty(&self) -> bool {
        self.open.is_none()
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
                // Порядок проверок значим (M12 разбора всей ветки): без открытой
                // транзакции ошибка обязана называться так, а не «неизвестное
                // отношение», даже если relation тоже не в кэше. Лимит — следующая
                // по дешевизне проверка, до похода в кэш и построения строки.
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
                let after = build_row(rel, &tuple)?;
                let pending = PendingChange {
                    schema: rel.namespace.clone(),
                    table: rel.name.clone(),
                    operation: Operation::Insert,
                    after,
                    lsn: wal_start,
                };
                open.changes.push(pending);
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
                    .map(|c| ChangeEvent {
                        schema: c.schema,
                        table: c.table,
                        operation: c.operation,
                        before: None,
                        before_kind: None,
                        after: Some(c.after),
                        unchanged_columns: Vec::new(),
                        transaction_id: open.xid,
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
        }
    }
}

/// Имена колонок берутся из RELATION по индексу — row-сообщения их не несут.
fn build_row(rel: &Relation, tuple: &TupleData) -> Result<Row, PgcdcError> {
    if tuple.columns.len() != rel.columns.len() {
        return Err(PgcdcError::Decode(format!(
            "tuple has {} columns, relation {} has {}",
            tuple.columns.len(),
            rel.id,
            rel.columns.len()
        )));
    }
    let mut row = Row::new();
    for (col, value) in rel.columns.iter().zip(&tuple.columns) {
        let json = match value {
            ColumnValue::Text(s) => serde_json::Value::String(s.clone()),
            ColumnValue::Null => serde_json::Value::Null,
            ColumnValue::UnchangedToast => {
                // На INSERT этот тег не приходит: значение записывается в той же
                // транзакции и reorder buffer его разрешает. Если он всё-таки
                // появился — это не наш случай, и молчать нельзя.
                return Err(PgcdcError::Decode(format!(
                    "unexpected unchanged-TOAST marker on INSERT, column {}",
                    col.name
                )));
            }
        };
        row.insert(col.name.clone(), json);
    }
    Ok(row)
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
        assert!(!a.is_empty(), "открытая транзакция держит буфер непустым");
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
            .expect("commit отдаёт транзакцию");
        assert_eq!(tx.xid, 737);
        assert_eq!(tx.commit_lsn, Lsn(0x1000));
        assert_eq!(tx.end_lsn, Lsn(0x1030), "end_lsn отдельно от commit_lsn");
        assert_eq!(tx.changes.len(), 1);
        let ev = &tx.changes[0];
        assert_eq!(ev.table, "users");
        assert_eq!(ev.transaction_id, 737);
        assert_eq!(ev.lsn, Lsn(0x200), "у события — wal_start своей строки");
        assert_eq!(
            ev.commit_lsn,
            Lsn(0x1000),
            "а commit_lsn общий на транзакцию"
        );
        assert!(a.is_empty(), "после коммита буфер пуст");
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
            "SQL NULL становится JSON null"
        );
    }

    #[test]
    fn row_outside_a_transaction_wins_over_unknown_relation() {
        // Без BEGIN ошибка обязана быть "row message outside a transaction",
        // а не UnknownRelation, даже если relation тоже не в кэше (M12: порядок
        // проверок в ветке Insert — open, затем лимит, затем поиск relation).
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        let err = a.handle(insert(), Lsn(0x200), &mut cache).unwrap_err();
        assert!(
            matches!(&err, PgcdcError::Decode(msg) if msg.contains("outside a transaction")),
            "получили {err:?}"
        );
    }

    #[test]
    fn row_for_unknown_relation_is_fatal() {
        // Невозможный поиск отношения — фатальная ошибка по спеке §15,
        // а не повод пропустить строку.
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
        // При реконнекте недособранная транзакция выбрасывается: её BEGIN был
        // после confirmed_flush_lsn, значит она придёт заново целиком.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        assert!(!a.is_empty());
        a.reset();
        assert!(a.is_empty());
    }

    #[test]
    fn relation_outside_a_transaction_is_accepted() {
        // RELATION приходит внутри транзакции в наших фикстурах, но кэш —
        // сессионный, и сообщение не обязано быть частью транзакции.
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
        assert!(a.is_empty(), "RELATION не открывает транзакцию");
    }

    #[test]
    fn column_count_mismatch_is_a_decode_error() {
        // build_row must reject a tuple whose column count disagrees with
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
}
