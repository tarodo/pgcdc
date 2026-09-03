use super::{Durability, Sink};
use crate::error::PgcdcError;
use crate::lsn::Lsn;
use crate::transaction::Transaction;

/// JSONL on stdout: one line per change. Development only —
/// a pipe has no durability, and that is stated honestly.
#[derive(Debug, Default)]
pub struct StdoutSink {
    /// The highest position accepted since the last barrier.
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

/// Serializes a transaction into JSONL. Extracted out of `StdoutSink` so it can be
/// tested directly, rather than through a test double.
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

/// The barrier: commits `w` to the device and returns what has accumulated in
/// `pending`. Extracted out of `StdoutSink` so it's possible to check directly
/// that the stream's `flush` is really called, not just that the method
/// returns the right position — a test double in tests
/// could forget the `flush` call and remain indistinguishable from a genuine implementation.
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
            event_index: 0,
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
        // Direct coverage of the real write code, not a test double: swap
        // JSONL here for one array per transaction — and this test must go red.
        let mut buf: Vec<u8> = Vec::new();
        write_changes(&mut buf, &two_change_tx()).unwrap();
        let text = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2, "two lines for two changes");
        assert!(lines[0].contains(r#""id":"1""#));
        assert!(lines[1].contains(r#""id":"2""#));
        assert!(text.ends_with('\n'), "each line ends with a newline");
        for line in lines {
            serde_json::from_str::<serde_json::Value>(line).expect("each line is valid JSON");
        }
    }

    #[tokio::test]
    async fn flush_with_nothing_pending_reports_no_position() {
        // A barrier on an empty sink has no right to
        // invent a position — on an idle tick of the next task this would
        // mean acknowledging something that was never written.
        let mut s = StdoutSink::new();
        assert_eq!(
            s.flush().await.unwrap(),
            None,
            "there was nothing to accept — nothing to acknowledge"
        );
    }

    #[tokio::test]
    async fn a_second_flush_right_after_the_first_reports_nothing_new() {
        // A second barrier right after the first, with no new
        // transaction in between, must report `None`, not repeat the previous position.
        let mut s = StdoutSink::new();
        s.write_transaction(&two_change_tx()).await.unwrap();
        assert_eq!(s.flush().await.unwrap(), Some(Lsn(0x1030)));
        assert_eq!(
            s.flush().await.unwrap(),
            None,
            "a repeated barrier with no new transaction must not commit anything"
        );
    }

    /// Writes into memory and remembers whether `flush` was actually called — to
    /// check the barrier itself, not just its return value.
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
        // Remove the `w.flush()` call inside the barrier, leave
        // only the position return — and this test must go red, because it
        // checks the real `StdoutSink::flush` code, not a test double.
        let mut w = RecordingWriter { flushed: false };
        let mut pending = Some(Lsn(0x1030));
        let durable = flush_pending(&mut w, &mut pending).unwrap();
        assert!(
            w.flushed,
            "the barrier must commit the stream to the device, not just return the position"
        );
        assert_eq!(durable, Some(Lsn(0x1030)));
        assert_eq!(
            pending, None,
            "the barrier must take the pending position, leaving None"
        );
    }
}
