use std::fmt;

use crate::error::PgcdcError;

/// A position in the WAL. PostgreSQL prints it as two hexadecimal halves
/// separated by a slash, with no leading zeros: `0/19300D0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Lsn(pub u64);

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

impl serde::Serialize for Lsn {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

/// Four positions that must not be confused (DECISIONS Q4, Q26a). `processed` is
/// the work of stage 3 (`DECISIONS.md` §4): it can run ahead of `durable`, and that
/// is the gap it was introduced for. There is no persistence: the PostgreSQL slot is
/// the sole source of truth, the tracker lives only in the process's memory.
#[derive(Debug, Default)]
pub struct LsnTracker {
    received: Lsn,
    durable: Lsn,
    acked: Lsn,
    processed: Lsn,
}

impl LsnTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn note_received(&mut self, lsn: Lsn) {
        if lsn > self.received {
            self.received = lsn;
        }
    }

    /// Called from exactly two places, and both must survive any
    /// refactoring of the replication loop. The first is a successful `Sink::flush`
    /// barrier: a position the sink has actually confirmed as written. The second is
    /// keepalive advancement on an idle publication: a server position that
    /// the sink never wrote at all, but which is nonetheless vacuously durable —
    /// the range between the previous durable position and this one provably contains
    /// not a single row of our publication (DECISIONS Q26b). This precondition is not an
    /// implementation detail: a future stage's reconnect guard reads `durable()` as
    /// "what can be safely taken as a starting point", and for the keepalive branch
    /// this holds only because there was nothing to lose there, not because
    /// someone wrote it down.
    pub fn note_durable(&mut self, lsn: Lsn) {
        if lsn > self.durable {
            self.durable = lsn;
        }
    }

    /// Rejects an attempt to acknowledge a position beyond durable. This is not
    /// defensive programming, it is the invariant itself: let such an acknowledgement
    /// through, and a crash between it and the write would mean a silent loss.
    pub fn try_ack(&mut self, lsn: Lsn) -> Result<(), PgcdcError> {
        if lsn > self.durable {
            return Err(PgcdcError::AckBeyondDurable {
                attempted: lsn.to_string(),
                durable: self.durable.to_string(),
            });
        }
        if lsn > self.acked {
            self.acked = lsn;
        }
        Ok(())
    }

    pub fn received(&self) -> Lsn {
        self.received
    }

    pub fn durable(&self) -> Lsn {
        self.durable
    }

    pub fn acked(&self) -> Lsn {
        self.acked
    }

    /// The position up to which messages have been parsed and handed to the sink. Can
    /// run ahead of `durable`: a window exists between the write and the fsync, and it
    /// is precisely because of it that the keepalive advancement condition (Q26a)
    /// requires `processed == durable`, not just an empty assembler buffer.
    pub fn note_processed(&mut self, lsn: Lsn) {
        if lsn > self.processed {
            self.processed = lsn;
        }
    }

    pub fn processed(&self) -> Lsn {
        self.processed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_display_matches_postgres_format() {
        // Values from docs/pgoutput-notes.md §4, 0004_commit.bin
        assert_eq!(Lsn(0x0000_0000_0193_00D0).to_string(), "0/19300D0");
        assert_eq!(Lsn(0x0000_0000_0193_0100).to_string(), "0/1930100");
        // High half is non-zero
        assert_eq!(Lsn(0x0000_0001_0000_00FF).to_string(), "1/FF");
        assert_eq!(Lsn(0).to_string(), "0/0");
    }

    #[test]
    fn tracker_refuses_to_ack_beyond_durable() {
        // The single invariant the project exists for:
        // never acknowledge a position the sink has not written.
        let mut t = LsnTracker::new();
        t.note_received(Lsn(0x2000));
        t.note_durable(Lsn(0x1000));
        assert!(
            t.try_ack(Lsn(0x1000)).is_ok(),
            "acknowledging exactly durable is allowed"
        );
        assert!(
            t.try_ack(Lsn(0x1001)).is_err(),
            "one byte past durable is not allowed"
        );
        assert_eq!(
            t.acked(),
            Lsn(0x1000),
            "a failed attempt does not move acked"
        );
    }

    #[test]
    fn tracker_never_moves_acked_backwards() {
        let mut t = LsnTracker::new();
        t.note_durable(Lsn(0x2000));
        t.try_ack(Lsn(0x2000)).unwrap();
        t.try_ack(Lsn(0x1000)).unwrap();
        assert_eq!(
            t.acked(),
            Lsn(0x2000),
            "a rollback of the acknowledgement is silently ignored"
        );
    }

    #[test]
    fn durable_never_moves_backwards() {
        let mut t = LsnTracker::new();
        t.note_durable(Lsn(0x2000));
        t.note_durable(Lsn(0x1000));
        assert_eq!(t.durable(), Lsn(0x2000));
    }

    #[test]
    fn processed_is_tracked_separately_and_moves_forward_only() {
        let mut t = LsnTracker::new();
        t.note_received(Lsn(0x3000));
        t.note_processed(Lsn(0x2000));
        assert_eq!(t.processed(), Lsn(0x2000));
        t.note_processed(Lsn(0x1000));
        assert_eq!(
            t.processed(),
            Lsn(0x2000),
            "the position does not roll back"
        );
    }

    #[test]
    fn processed_may_run_ahead_of_durable() {
        // Exactly the situation this position was introduced for: a transaction
        // has been handed to the sink, but the fsync hasn't happened yet.
        let mut t = LsnTracker::new();
        t.note_processed(Lsn(0x2000));
        assert_eq!(t.durable(), Lsn(0));
        assert!(t.processed() > t.durable());
        assert!(
            t.try_ack(Lsn(0x2000)).is_err(),
            "acknowledging by processed is not allowed"
        );
    }
}
