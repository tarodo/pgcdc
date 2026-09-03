pub mod file;
pub mod stdout;

pub use file::FileSink;
pub use stdout::StdoutSink;

use crate::error::PgcdcError;
use crate::lsn::Lsn;
use crate::transaction::Transaction;

/// What the sink promises about a write AFTER a successful `flush`.
/// This has no bearing on what `write_transaction` returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// After `flush`, the data has been committed to disk: acknowledging the position is safe.
    Fsync,
    /// After `flush`, the bytes have been handed to the kernel, but their fate is unknown. For development.
    BestEffort,
}

#[async_trait::async_trait]
pub trait Sink: Send {
    fn durability(&self) -> Durability;

    /// Accept a transaction in full. Returning `Ok` means "accepted", NOT "durable":
    /// a window exists between acceptance and the barrier, and acknowledging a position
    /// inside it is forbidden by invariant 1.
    async fn write_transaction(&mut self, tx: &Transaction) -> Result<(), PgcdcError>;

    /// Commit to the storage medium everything accepted since the last barrier.
    /// Returns the highest position that became durable, or `None` if
    /// there was nothing to accept. Only after `Ok(Some(lsn))` does the caller have
    /// the right to mark `lsn` as durable.
    async fn flush(&mut self) -> Result<Option<Lsn>, PgcdcError>;
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
                event_index: 0,
                lsn: Lsn(0x200),
                commit_lsn: Lsn(0x1000),
                commit_timestamp: pg_micros_to_utc(841_423_351_314_489),
            }],
        }
    }

    /// Writes into a buffer instead of stdout — this checks serialization,
    /// not terminal behavior.
    struct BufferSink {
        lines: Vec<String>,
        /// The highest position accepted since the last barrier.
        pending: Option<Lsn>,
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
            self.pending = Some(tx.end_lsn);
            Ok(())
        }
        async fn flush(&mut self) -> Result<Option<Lsn>, PgcdcError> {
            Ok(self.pending.take())
        }
    }

    #[tokio::test]
    async fn sink_writes_one_line_per_change_not_one_per_transaction() {
        // Write atomicity is not the same as format atomicity (DECISIONS Q20):
        // the sink receives the transaction whole, but serializes it into N lines of JSONL.
        // The transaction must carry TWO changes: with one change the test would pass
        // the same way for both the correct implementation and the "one line per
        // transaction" regression, distinguishing nothing between them.
        let mut two_changes = tx();
        let mut second_after = Row::new();
        second_after.insert("id".into(), "2".into());
        two_changes.changes.push(ChangeEvent {
            schema: "public".into(),
            table: "users".into(),
            operation: Operation::Insert,
            before: None,
            before_kind: None,
            after: Some(second_after),
            unchanged_columns: Vec::new(),
            transaction_id: 737,
            event_index: 1,
            lsn: Lsn(0x210),
            commit_lsn: Lsn(0x1000),
            commit_timestamp: pg_micros_to_utc(841_423_351_314_489),
        });

        let mut s = BufferSink {
            lines: Vec::new(),
            pending: None,
        };
        s.write_transaction(&two_changes).await.unwrap();
        assert_eq!(
            s.lines.len(),
            2,
            "two changes in the transaction — two lines of JSONL, not one blob"
        );
        assert!(s.lines[0].starts_with(r#"{"schema":"public""#));
        assert!(s.lines[0].contains(r#""id":"1""#));
        assert!(s.lines[1].contains(r#""id":"2""#));
        assert!(
            s.lines.iter().all(|l| !l.contains('\n')),
            "there must be no line breaks inside a line"
        );
    }

    #[test]
    fn stdout_sink_is_honest_about_not_being_durable() {
        // A pipe gives no durability in principle, and pretending otherwise is worse
        // than admitting it (DECISIONS Q6).
        assert_eq!(StdoutSink::new().durability(), Durability::BestEffort);
    }

    /// Counts calls and remembers what was accepted but not yet committed.
    struct CountingSink {
        accepted: Vec<Lsn>,
        flushed: Vec<Lsn>,
        flush_calls: usize,
    }

    #[async_trait::async_trait]
    impl Sink for CountingSink {
        fn durability(&self) -> Durability {
            Durability::Fsync
        }
        async fn write_transaction(&mut self, tx: &Transaction) -> Result<(), PgcdcError> {
            self.accepted.push(tx.end_lsn);
            Ok(())
        }
        async fn flush(&mut self) -> Result<Option<Lsn>, PgcdcError> {
            self.flush_calls += 1;
            let last = self.accepted.last().copied();
            self.flushed.append(&mut self.accepted);
            Ok(last)
        }
    }

    #[tokio::test]
    async fn accepting_a_transaction_does_not_make_it_durable() {
        // This is exactly the point of the separation: a window exists between acceptance
        // and the barrier, and acknowledging a position inside it is not allowed.
        let mut s = CountingSink {
            accepted: vec![],
            flushed: vec![],
            flush_calls: 0,
        };
        s.write_transaction(&tx()).await.unwrap();
        assert!(s.flushed.is_empty(), "accepting on its own commits nothing");
        assert_eq!(s.flush_calls, 0);
    }

    #[tokio::test]
    async fn flush_reports_the_highest_position_it_made_durable() {
        let mut s = CountingSink {
            accepted: vec![],
            flushed: vec![],
            flush_calls: 0,
        };
        s.write_transaction(&tx()).await.unwrap();
        let durable = s.flush().await.unwrap();
        assert_eq!(
            durable,
            Some(Lsn(0x1030)),
            "the barrier reports back with a position"
        );
        assert_eq!(s.flushed.len(), 1);
    }

    #[tokio::test]
    async fn flush_with_nothing_accepted_reports_no_new_position() {
        // Important for the loop: an empty tick must not move durable.
        let mut s = CountingSink {
            accepted: vec![],
            flushed: vec![],
            flush_calls: 0,
        };
        assert_eq!(s.flush().await.unwrap(), None);
    }
}
