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
        for change in &tx.changes {
            let line = serde_json::to_string(change)
                .map_err(|e| PgcdcError::Sink(format!("serialize: {e}")))?;
            writeln!(out, "{line}").map_err(|e| PgcdcError::Sink(format!("write: {e}")))?;
        }
        // Один flush на транзакцию: атомарность записи — свойство транзакции,
        // а не отдельной строки.
        out.flush()
            .map_err(|e| PgcdcError::Sink(format!("flush: {e}")))?;
        Ok(())
    }
}
