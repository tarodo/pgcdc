use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

use super::stdout::write_changes;
use super::{Durability, Sink};
use crate::error::PgcdcError;
use crate::lsn::Lsn;
use crate::transaction::Transaction;

/// JSONL с дозаписью в файл. Единственный sink этапа, способный честно
/// обещать `Fsync`: барьер вызывает `sync_data`, и только после его успеха
/// позиция может быть отмечена durable.
#[derive(Debug)]
pub struct FileSink {
    writer: BufWriter<File>,
    /// Наибольшая принятая позиция с прошлого барьера.
    pending: Option<Lsn>,
}

impl FileSink {
    /// Открывает файл на дозапись, создавая при отсутствии.
    pub fn open(path: &Path) -> Result<Self, PgcdcError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| PgcdcError::Sink(format!("open {}: {e}", path.display())))?;
        Ok(Self {
            writer: BufWriter::new(file),
            pending: None,
        })
    }
}

#[async_trait::async_trait]
impl Sink for FileSink {
    fn durability(&self) -> Durability {
        Durability::Fsync
    }

    async fn write_transaction(&mut self, tx: &Transaction) -> Result<(), PgcdcError> {
        write_changes(&mut self.writer, tx)?;
        self.pending = Some(tx.end_lsn);
        Ok(())
    }

    async fn flush(&mut self) -> Result<Option<Lsn>, PgcdcError> {
        // Порядок обязателен: сначала вытолкнуть буфер пользовательского
        // пространства, потом заставить ядро довести до носителя. Пропустить
        // второе — значит обещать Fsync и не выполнять обещание.
        self.writer
            .flush()
            .map_err(|e| PgcdcError::Sink(format!("flush: {e}")))?;
        self.writer
            .get_ref()
            .sync_data()
            .map_err(|e| PgcdcError::Sink(format!("fsync: {e}")))?;
        Ok(self.pending.take())
    }
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

    fn tx(end: u64, ids: &[&str]) -> Transaction {
        Transaction {
            xid: 737,
            commit_lsn: Lsn(0x1000),
            end_lsn: Lsn(end),
            commit_timestamp: pg_micros_to_utc(841_423_351_314_489),
            changes: ids.iter().map(|i| change(i)).collect(),
        }
    }

    fn temp_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("pgcdc-test-{}-{}.jsonl", name, std::process::id()));
        let _ = std::fs::remove_file(&p);
        p
    }

    #[tokio::test]
    async fn file_sink_declares_real_durability() {
        let p = temp_path("durability");
        let s = FileSink::open(&p).unwrap();
        assert_eq!(s.durability(), Durability::Fsync);
        let _ = std::fs::remove_file(&p);
    }

    #[tokio::test]
    async fn writes_one_json_line_per_change_and_appends() {
        let p = temp_path("append");
        let mut s = FileSink::open(&p).unwrap();
        s.write_transaction(&tx(0x1030, &["1", "2"])).await.unwrap();
        s.flush().await.unwrap();
        s.write_transaction(&tx(0x1060, &["3"])).await.unwrap();
        s.flush().await.unwrap();
        let text = std::fs::read_to_string(&p).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 3, "две транзакции, три изменения");
        for line in &lines {
            serde_json::from_str::<serde_json::Value>(line).expect("каждая строка — JSON");
        }
        assert!(
            lines[2].contains(r#""id":"3""#),
            "вторая транзакция дописана, а не затёрла"
        );
        let _ = std::fs::remove_file(&p);
    }

    #[tokio::test]
    async fn flush_reports_the_last_accepted_position_then_clears_it() {
        let p = temp_path("position");
        let mut s = FileSink::open(&p).unwrap();
        assert_eq!(s.flush().await.unwrap(), None, "принимать было нечего");
        s.write_transaction(&tx(0x1030, &["1"])).await.unwrap();
        assert_eq!(s.flush().await.unwrap(), Some(Lsn(0x1030)));
        assert_eq!(
            s.flush().await.unwrap(),
            None,
            "повторный барьер ничего не добавляет"
        );
        let _ = std::fs::remove_file(&p);
    }

    #[tokio::test]
    async fn opening_an_unwritable_path_fails_loudly() {
        // Каталог вместо файла: открыть на запись нельзя.
        let err = FileSink::open(std::path::Path::new("/")).unwrap_err();
        assert!(matches!(err, PgcdcError::Sink(_)));
        assert!(
            err.is_fatal(),
            "sink, который не может писать, — фатальная ошибка"
        );
    }
}
