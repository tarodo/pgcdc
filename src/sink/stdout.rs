use std::io::Write;

use super::{Durability, Sink};
use crate::error::PgcdcError;
use crate::transaction::Transaction;

/// JSONL на stdout: одна строка на изменение. Только для разработки —
/// durability у трубы нет, и это объявлено честно.
#[derive(Debug, Default)]
pub struct StdoutSink;

impl StdoutSink {
    pub fn new() -> Self {
        Self
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
        // Один flush на транзакцию: атомарность записи — свойство транзакции,
        // а не отдельной строки.
        out.flush()
            .map_err(|e| PgcdcError::Sink(format!("flush: {e}")))?;
        Ok(())
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
}
