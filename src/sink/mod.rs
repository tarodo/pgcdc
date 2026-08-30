pub mod stdout;

pub use stdout::StdoutSink;

use crate::error::PgcdcError;
use crate::transaction::Transaction;

/// Что sink может обещать про запись. Kafka с `acks=all` встанет сюда же
/// как `Fsync`, а труба честно останется `BestEffort`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Данные доведены до диска: подтверждать LSN безопасно.
    Fsync,
    /// Байты отданы ядру, но их судьба неизвестна. Для разработки.
    BestEffort,
}

#[async_trait::async_trait]
pub trait Sink: Send {
    fn durability(&self) -> Durability;

    /// Получает транзакцию целиком и обязан либо записать её всю,
    /// либо вернуть ошибку. Частичная запись — это ошибка.
    async fn write_transaction(&mut self, tx: &Transaction) -> Result<(), PgcdcError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{pg_micros_to_utc, ChangeEvent, Operation, Row};
    use crate::lsn::Lsn;
    use crate::transaction::Transaction;

    fn tx() -> Transaction {
        let mut after = Row::new();
        after.insert("id".into(), "1".into());
        Transaction {
            xid: 737,
            commit_lsn: Lsn(0x1000),
            end_lsn: Lsn(0x1030),
            commit_timestamp: pg_micros_to_utc(841_423_351_314_489),
            changes: vec![ChangeEvent {
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
            }],
        }
    }

    /// Пишет в буфер вместо stdout — так проверяется сериализация,
    /// а не поведение терминала.
    struct BufferSink {
        lines: Vec<String>,
    }

    #[async_trait::async_trait]
    impl Sink for BufferSink {
        fn durability(&self) -> Durability {
            Durability::BestEffort
        }
        async fn write_transaction(&mut self, tx: &Transaction) -> Result<(), PgcdcError> {
            for ch in &tx.changes {
                self.lines.push(serde_json::to_string(ch).unwrap());
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn sink_writes_one_line_per_change_not_one_per_transaction() {
        // Атомарность записи не равна атомарности формата (DECISIONS Q20):
        // sink получает транзакцию целиком, но сериализует её в N строк JSONL.
        let mut s = BufferSink { lines: Vec::new() };
        s.write_transaction(&tx()).await.unwrap();
        assert_eq!(s.lines.len(), 1);
        assert!(s.lines[0].starts_with(r#"{"schema":"public""#));
        assert!(
            !s.lines[0].contains('\n'),
            "внутри строки переводов быть не должно"
        );
    }

    #[test]
    fn stdout_sink_is_honest_about_not_being_durable() {
        // Труба не даёт durability в принципе, и делать вид иначе — хуже,
        // чем признать это (DECISIONS Q6).
        assert_eq!(StdoutSink::new().durability(), Durability::BestEffort);
    }
}
