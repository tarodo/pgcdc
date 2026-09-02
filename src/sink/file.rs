use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

use super::stdout::write_changes;
use super::{Durability, Sink};
use crate::error::PgcdcError;
use crate::lsn::Lsn;
use crate::transaction::Transaction;

/// An abstraction over "make it durable on the device", separate from `std::io::Write`.
/// The only reason it exists is to give the barrier a test double type in
/// tests: `sync_data` is not a secondary
/// detail, it is the single line that makes `Durability::Fsync` true
/// for this sink, and the replication loop marks positions durable precisely on its
/// basis. Holding it to a lower verification standard than the pipe's `flush`
/// in the stdout sink (which has no durability at all) would be backwards.
trait DurableWrite: Write {
    fn durable_sync(&self) -> std::io::Result<()>;
}

impl DurableWrite for File {
    fn durable_sync(&self) -> std::io::Result<()> {
        self.sync_data()
    }
}

/// JSONL appended to a file. The only sink at this stage able to honestly
/// promise `Fsync`: the barrier calls `sync_data`, and only after it succeeds
/// can a position be marked durable.
#[derive(Debug)]
pub struct FileSink {
    writer: BufWriter<File>,
    /// The highest position accepted since the last barrier.
    pending: Option<Lsn>,
}

impl FileSink {
    /// Opens the file for appending, creating it if it doesn't exist.
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

/// The barrier: first flushes the user-space buffer
/// (`flush`), then makes the kernel commit it to the storage medium (`durable_sync`),
/// and only then returns the accumulated position. The order is mandatory — skipping
/// the second call or swapping them means promising `Fsync` and not delivering on
/// the promise. Extracted out of `FileSink::flush`, the same way `flush_pending` is
/// extracted out of `StdoutSink::flush`, so it can be tested directly through a
/// test-double writer, rather than through a test double of the whole sink.
fn flush_durable<W: DurableWrite>(
    writer: &mut BufWriter<W>,
    pending: &mut Option<Lsn>,
) -> Result<Option<Lsn>, PgcdcError> {
    // Nothing accepted since the last barrier => the buffer is empty and already
    // synchronized — otherwise there would be something to accept. Without this
    // check, the timer barrier would call flush and fsync on every tick
    // unconditionally, including a fully idle stream — several
    // fsyncs a second on a file no one has touched.
    if pending.is_none() {
        return Ok(None);
    }
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
        assert_eq!(lines.len(), 3, "two transactions, three changes");
        for line in &lines {
            serde_json::from_str::<serde_json::Value>(line).expect("each line is JSON");
        }
        assert!(
            lines[2].contains(r#""id":"3""#),
            "the second transaction was appended, not overwritten"
        );
        let _ = std::fs::remove_file(&p);
    }

    #[tokio::test]
    async fn append_survives_closing_and_reopening_the_sink() {
        // `OpenOptions::append` only takes effect
        // while the file is open — a test that writes twice through ONE and the same
        // `FileSink` cannot distinguish `.append(true)` from `.truncate(true).write(true)`:
        // both forms behave the same as long as the descriptor hasn't been closed and
        // reopened. Only reopening the same path with a new sink can tell them apart.
        let p = temp_path("reopen");
        {
            let mut s = FileSink::open(&p).unwrap();
            s.write_transaction(&tx(0x1030, &["1"])).await.unwrap();
            s.flush().await.unwrap();
        } // the sink is dropped here — the file is truly closed
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
            "reopening the same path must not erase the previous write"
        );
        assert!(
            lines[0].contains(r#""id":"1""#),
            "the write from the first opening must survive the close"
        );
        assert!(
            lines[1].contains(r#""id":"2""#),
            "the write from the second opening must be appended, not overwrite the first"
        );
        let _ = std::fs::remove_file(&p);
    }

    #[tokio::test]
    async fn flush_reports_the_last_accepted_position_then_clears_it() {
        let p = temp_path("position");
        let mut s = FileSink::open(&p).unwrap();
        assert_eq!(
            s.flush().await.unwrap(),
            None,
            "there was nothing to accept"
        );
        s.write_transaction(&tx(0x1030, &["1"])).await.unwrap();
        assert_eq!(s.flush().await.unwrap(), Some(Lsn(0x1030)));
        assert_eq!(
            s.flush().await.unwrap(),
            None,
            "a repeated barrier adds nothing"
        );
        let _ = std::fs::remove_file(&p);
    }

    #[tokio::test]
    async fn opening_an_unwritable_path_fails_loudly() {
        // A directory instead of a file: cannot be opened for writing.
        let err = FileSink::open(std::path::Path::new("/")).unwrap_err();
        assert!(matches!(err, PgcdcError::Sink(_)));
        assert!(err.is_fatal(), "a sink that cannot write is a fatal error");
    }

    /// A test-double writer that stores no bytes, only records the ORDER
    /// in which `flush` and `durable_sync` were called. Mirrors `RecordingWriter`
    /// from the stdout sink, where the test double proved
    /// that the stream's `flush` is actually called, not just that the barrier returns
    /// the right position. Here the stakes are higher: `durable_sync` is the only
    /// line that makes `Durability::Fsync` true — so the test double checks not just
    /// the call itself, but also that it comes STRICTLY AFTER
    /// `flush`. This pins down two regressions at once: dropping the call and
    /// swapping the calls — a refactor two stages later, in a file that
    /// no one will re-diff, that silently drops or reorders the line
    /// will not slip past this test.
    ///
    /// What this test does NOT prove: that the bytes actually reached the physical
    /// storage medium. Only a syscall trace or a
    /// crash-consistency harness could show that, and
    /// this test suite does not attempt either.
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
        // Tests the real `flush_durable` code,
        // not a test double. Remove the `durable_sync` call — red. Swap
        // `flush` and `durable_sync` — also red, because what's checked is
        // the ORDER of calls, not just their count.
        let mut writer = BufWriter::new(RecordingSyncWriter::default());
        let mut pending = Some(Lsn(0x1030));
        let durable = flush_durable(&mut writer, &mut pending).unwrap();
        assert_eq!(durable, Some(Lsn(0x1030)));
        assert_eq!(
            writer.get_ref().calls.borrow().as_slice(),
            ["flush", "durable_sync"],
            "durable_sync must come strictly after flush, otherwise Fsync is an empty promise"
        );
        assert_eq!(
            pending, None,
            "the barrier must take the pending position, leaving None"
        );
    }

    #[test]
    fn flush_durable_does_not_touch_the_device_when_nothing_is_pending() {
        // The timer barrier is reached on every
        // tick, including idle ones — calling flush/fsync unconditionally would
        // mean synchronizing an untouched file several times a second on a forever
        // idle stream. Nothing accepted => the buffer is already empty and already
        // synchronized, so not a single call should reach here.
        let mut writer = BufWriter::new(RecordingSyncWriter::default());
        let mut pending = None;
        let durable = flush_durable(&mut writer, &mut pending).unwrap();
        assert_eq!(durable, None);
        assert!(
            writer.get_ref().calls.borrow().is_empty(),
            "nothing to acknowledge — flush and durable_sync should not have been called: {:?}",
            writer.get_ref().calls.borrow()
        );
    }
}
