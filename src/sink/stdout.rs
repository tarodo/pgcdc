use super::{Durability, Sink};
use crate::error::PgcdcError;
use crate::lsn::Lsn;
use crate::transaction::Transaction;

/// JSONL на stdout: одна строка на изменение. Только для разработки —
/// durability у трубы нет, и это объявлено честно.
#[derive(Debug, Default)]
pub struct StdoutSink {
    /// Наибольшая принятая позиция с прошлого барьера.
    pending: Option<Lsn>,
}

impl StdoutSink {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl Sink for StdoutSink {
    fn durability(&self) -> Durability {
        Durability::BestEffort
    }

    async fn write_transaction(&mut self, tx: &Transaction) -> Result<(), PgcdcError> {
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        write_changes(&mut out, tx)?;
        self.pending = Some(tx.end_lsn);
        Ok(())
    }

    async fn flush(&mut self) -> Result<Option<Lsn>, PgcdcError> {
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        flush_pending(&mut out, &mut self.pending)
    }
}

/// Сериализация транзакции в JSONL. Вынесена из `StdoutSink`, чтобы её можно было
/// проверить напрямую, а не через тестовый дублёр.
pub(crate) fn write_changes<W: std::io::Write>(
    w: &mut W,
    tx: &Transaction,
) -> Result<(), PgcdcError> {
    for change in &tx.changes {
        let line = serde_json::to_string(change)
            .map_err(|e| PgcdcError::Sink(format!("serialize: {e}")))?;
        writeln!(w, "{line}").map_err(|e| PgcdcError::Sink(format!("write: {e}")))?;
    }
    Ok(())
}

/// Барьер: доводит `w` до устройства и возвращает то, что накопилось в
/// `pending`. Вынесена из `StdoutSink`, чтобы можно было проверить напрямую,
/// что `flush` потока действительно вызывается, а не только что метод
/// возвращает верную позицию (review Task 2, round 1, F3) — дублёр в тестах
/// мог бы забыть вызов `flush` и остаться неотличим от честной реализации.
pub(crate) fn flush_pending<W: std::io::Write>(
    w: &mut W,
    pending: &mut Option<Lsn>,
) -> Result<Option<Lsn>, PgcdcError> {
    w.flush()
        .map_err(|e| PgcdcError::Sink(format!("flush: {e}")))?;
    Ok(pending.take())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{pg_micros_to_utc, ChangeEvent, Operation, Row};
    use crate::lsn::Lsn;

    fn change(id: &str) -> ChangeEvent {
        let mut after = Row::new();
        after.insert("id".into(), id.into());
        ChangeEvent {
            schema: "public".into(),
            table: "users".into(),
            operation: Operation::Insert,
            before: None,
            before_kind: None,
            after: Some(after),
            unchanged_columns: Vec::new(),
            transaction_id: 737,
            lsn: Lsn(0x200),
            commit_lsn: Lsn(0x1000),
            commit_timestamp: pg_micros_to_utc(841_423_351_314_489),
        }
    }

    fn two_change_tx() -> Transaction {
        Transaction {
            xid: 737,
            commit_lsn: Lsn(0x1000),
            end_lsn: Lsn(0x1030),
            commit_timestamp: pg_micros_to_utc(841_423_351_314_489),
            changes: vec![change("1"), change("2")],
        }
    }

    #[test]
    fn shipped_serializer_writes_one_line_per_change() {
        // Прямое покрытие настоящего кода записи, а не дублёра: подмени здесь
        // JSONL на один массив на транзакцию — и этот тест обязан покраснеть.
        let mut buf: Vec<u8> = Vec::new();
        write_changes(&mut buf, &two_change_tx()).unwrap();
        let text = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2, "две строки на две записи");
        assert!(lines[0].contains(r#""id":"1""#));
        assert!(lines[1].contains(r#""id":"2""#));
        assert!(
            text.ends_with('\n'),
            "каждая строка завершена переводом строки"
        );
        for line in lines {
            serde_json::from_str::<serde_json::Value>(line).expect("каждая строка — валидный JSON");
        }
    }

    #[tokio::test]
    async fn flush_with_nothing_pending_reports_no_position() {
        // F1 (review, round 1): барьер на пустом sink'е не имеет права
        // изобретать позицию — на пустом тике идле следующей задачи это
        // означало бы подтвердить то, что никогда не писалось.
        let mut s = StdoutSink::new();
        assert_eq!(
            s.flush().await.unwrap(),
            None,
            "нечего было принимать — нечего подтверждать"
        );
    }

    #[tokio::test]
    async fn a_second_flush_right_after_the_first_reports_nothing_new() {
        // F1 (review, round 1): второй барьер подряд, без новой транзакции
        // между ними, обязан отчитаться `None`, а не повторить прошлую позицию.
        let mut s = StdoutSink::new();
        s.write_transaction(&two_change_tx()).await.unwrap();
        assert_eq!(s.flush().await.unwrap(), Some(Lsn(0x1030)));
        assert_eq!(
            s.flush().await.unwrap(),
            None,
            "повторный барьер без новой транзакции не должен ничего доводить"
        );
    }

    /// Пишет в память и запоминает, был ли реально вызван `flush`, — чтобы
    /// проверить сам барьер, а не только его возвращаемое значение.
    struct RecordingWriter {
        flushed: bool,
    }

    impl std::io::Write for RecordingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.flushed = true;
            Ok(())
        }
    }

    #[test]
    fn flush_pending_actually_flushes_the_writer() {
        // F3 (review, round 1): убери вызов `w.flush()` внутри барьера, оставь
        // только возврат позиции — и этот тест обязан покраснеть, потому что
        // проверяет реальный код `StdoutSink::flush`, а не дублёра.
        let mut w = RecordingWriter { flushed: false };
        let mut pending = Some(Lsn(0x1030));
        let durable = flush_pending(&mut w, &mut pending).unwrap();
        assert!(
            w.flushed,
            "барьер обязан довести поток до устройства, а не только вернуть позицию"
        );
        assert_eq!(durable, Some(Lsn(0x1030)));
        assert_eq!(
            pending, None,
            "барьер обязан забрать ожидающую позицию, оставив None"
        );
    }
}
