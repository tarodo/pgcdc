use chrono::{DateTime, Utc};

use crate::error::PgcdcError;
use crate::event::{pg_micros_to_utc, BeforeKind, ChangeEvent, Operation, Row};
use crate::lsn::Lsn;
use crate::postgres::pgoutput::{ColumnValue, OldTupleKind, PgOutputMessage, TupleData};
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
                let (after, unchanged) = build_full_row(rel, &tuple)?;
                if !unchanged.is_empty() {
                    // На INSERT этот тег не приходит: значение записывается в той
                    // же транзакции и reorder buffer его разрешает. Если он
                    // всё-таки появился — это не наш случай, и молчать нельзя.
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
                        let (row, _) = build_full_row(rel, tuple)?;
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
                        let (row, _) = build_full_row(rel, &old)?;
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
                    .map(|c| ChangeEvent {
                        schema: c.schema,
                        table: c.table,
                        operation: c.operation,
                        before: c.before,
                        before_kind: c.before_kind,
                        after: c.after,
                        unchanged_columns: c.unchanged_columns,
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

/// Полный кортеж — тег `'N'` или `'O'`. В нём запись на каждую колонку.
/// `'n'` здесь означает настоящий SQL NULL. `'u'` означает, что сервер не переслал
/// неизменившееся TOAST-значение: колонка в строку не попадает вовсе, её имя
/// возвращается вторым элементом, чтобы уехать в `unchanged_columns`.
/// Записать её как `null` было бы тихой порчей — потребитель решил бы, что значение обнулили.
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

/// Кортеж `'K'` — только replica identity. Число элементов равно числу колонок
/// таблицы, но неключевые заполнены `'n'`, и это НЕ NULL, а «сервер не прислал».
/// Поэтому в строку попадает только то, что реально приехало.
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
        // В 0019_delete.bin строка на момент удаления имела title='Widget', qty=7.
        // Оба приехали как 'n'. Подать их как null — вранье о данных: значения
        // существовали, сервер их просто не прислал.
        let tuple = TupleData {
            columns: vec![
                ColumnValue::Text("10".into()),
                ColumnValue::Null,
                ColumnValue::Null,
            ],
        };
        let row = build_key_row(&items_relation(), &tuple).unwrap();
        assert_eq!(row.len(), 1, "только присланная колонка");
        assert_eq!(row.get("id").unwrap(), "10");
        assert!(
            !row.contains_key("title"),
            "title отсутствует, а не равен null"
        );
        assert!(!row.contains_key("qty"), "qty отсутствует, а не равен null");
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
            "'n' в полном кортеже — настоящий NULL"
        );
        assert!(!row.contains_key("qty"), "'u' не попадает в строку вообще");
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
            "несланное TOAST-значение не попадает в after"
        );
        assert_eq!(ev.unchanged_columns, vec!["bio".to_string()]);
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
        assert!(ev.after.is_none(), "у DELETE нового кортежа нет");
        let before = ev.before.as_ref().unwrap();
        assert_eq!(before.len(), 1);
        assert!(
            !before.contains_key("title"),
            "заглушка не превращается в null"
        );
    }

    #[test]
    fn serialized_delete_event_matches_the_contract() {
        // Проверка формы наружу, а не только внутренних структур.
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
}
