use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

use super::stdout::write_changes;
use super::{Durability, Sink};
use crate::error::PgcdcError;
use crate::lsn::Lsn;
use crate::transaction::Transaction;

/// Абстракция над «довести до устройства», отдельная от `std::io::Write`.
/// Единственная причина её существования — дать барьеру подставной тип в
/// тестах (review Task 3, round 1, F2): `sync_data` — это не второстепенная
/// деталь, а единственная строка, которая делает `Durability::Fsync` правдой
/// для этого sink'а, а цикл репликации отмечает позиции durable именно на её
/// основании. Держать её к более низкому стандарту проверки, чем `flush`
/// трубы в stdout-sink'е (у которой durability нет вовсе), было бы задом
/// наперёд.
trait DurableWrite: Write {
    fn durable_sync(&self) -> std::io::Result<()>;
}

impl DurableWrite for File {
    fn durable_sync(&self) -> std::io::Result<()> {
        self.sync_data()
    }
}

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
        flush_durable(&mut self.writer, &mut self.pending)
    }
}

/// Барьер: сначала выталкивает буфер пользовательского пространства
/// (`flush`), затем заставляет ядро довести его до носителя (`durable_sync`),
/// и только потом отдаёт накопившуюся позицию. Порядок обязателен — пропустить
/// второй вызов или поменять их местами значит обещать `Fsync` и не выполнять
/// обещание. Вынесена из `FileSink::flush`, как `flush_pending` вынесена из
/// `StdoutSink::flush`, чтобы её можно было проверить напрямую через
/// подставной writer, а не через дублёра всего sink'а.
fn flush_durable<W: DurableWrite>(
    writer: &mut BufWriter<W>,
    pending: &mut Option<Lsn>,
) -> Result<Option<Lsn>, PgcdcError> {
    writer
        .flush()
        .map_err(|e| PgcdcError::Sink(format!("flush: {e}")))?;
    writer
        .get_ref()
        .durable_sync()
        .map_err(|e| PgcdcError::Sink(format!("fsync: {e}")))?;
    Ok(pending.take())
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

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
    async fn append_survives_closing_and_reopening_the_sink() {
        // F1 (review Task 3, round 1): `OpenOptions::append` действует, только
        // пока файл открыт — тест, который пишет дважды через ОДИН и тот же
        // `FileSink`, не отличит `.append(true)` от `.truncate(true).write(true)`:
        // обе формы ведут себя одинаково, пока дескриптор не закрывался и не
        // открывался заново. Отличить их может только повторное открытие того
        // же пути новым sink'ом.
        let p = temp_path("reopen");
        {
            let mut s = FileSink::open(&p).unwrap();
            s.write_transaction(&tx(0x1030, &["1"])).await.unwrap();
            s.flush().await.unwrap();
        } // sink роняется здесь — файл закрывается по-настоящему
        {
            let mut s = FileSink::open(&p).unwrap();
            s.write_transaction(&tx(0x1060, &["2"])).await.unwrap();
            s.flush().await.unwrap();
        }
        let text = std::fs::read_to_string(&p).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(
            lines.len(),
            2,
            "повторное открытие того же пути не должно затирать прошлую запись"
        );
        assert!(
            lines[0].contains(r#""id":"1""#),
            "запись из первого открытия обязана пережить закрытие"
        );
        assert!(
            lines[1].contains(r#""id":"2""#),
            "запись из второго открытия обязана дописаться, а не затереть первую"
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

    /// Подставной writer, который не хранит байты, а только фиксирует ПОРЯДОК,
    /// в котором были вызваны `flush` и `durable_sync`. Мирроит `RecordingWriter`
    /// из stdout-sink'а (review Task 2, round 1, F3), где дублёр доказывал, что
    /// `flush` потока реально вызывается, а не только что барьер возвращает
    /// верную позицию. Здесь ставка выше: `durable_sync` — единственная
    /// строка, которая делает `Durability::Fsync` правдой, — поэтому дублёр
    /// проверяет не только сам вызов, но и то, что он идёт СТРОГО ПОСЛЕ
    /// `flush`. Это пришпиливает сразу два регресса: удаление вызова и
    /// перестановку вызовов местами — рефактор двумя этапами позже, в файле,
    /// который никто не передиффит, тихо уронивший или переставивший строку,
    /// не пройдёт мимо этого теста.
    ///
    /// Чего этот тест НЕ доказывает: что байты реально дошли до физического
    /// носителя. Это может показать только трассировка системного вызова или
    /// стенд для проверки поведения при сбое (crash-consistency harness), и
    /// этот набор тестов не пытается сделать ни то, ни другое.
    #[derive(Default)]
    struct RecordingSyncWriter {
        calls: RefCell<Vec<&'static str>>,
    }

    impl Write for RecordingSyncWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.calls.borrow_mut().push("flush");
            Ok(())
        }
    }

    impl DurableWrite for RecordingSyncWriter {
        fn durable_sync(&self) -> std::io::Result<()> {
            self.calls.borrow_mut().push("durable_sync");
            Ok(())
        }
    }

    #[test]
    fn flush_durable_calls_flush_then_sync_in_order() {
        // F2 (review Task 3, round 1): проверяет реальный код `flush_durable`,
        // а не дублёра. Удали вызов `durable_sync` — красный. Поменяй местами
        // `flush` и `durable_sync` — тоже красный, потому что проверяется
        // ПОРЯДОК вызовов, а не только их количество.
        let mut writer = BufWriter::new(RecordingSyncWriter::default());
        let mut pending = Some(Lsn(0x1030));
        let durable = flush_durable(&mut writer, &mut pending).unwrap();
        assert_eq!(durable, Some(Lsn(0x1030)));
        assert_eq!(
            writer.get_ref().calls.borrow().as_slice(),
            ["flush", "durable_sync"],
            "durable_sync обязан идти строго после flush, иначе Fsync — пустое обещание"
        );
        assert_eq!(
            pending, None,
            "барьер обязан забрать ожидающую позицию, оставив None"
        );
    }
}
