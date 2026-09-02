use thiserror::Error;

/// The recoverable/fatal split lives in the type, not in a comment: `is_fatal`
/// is implemented as an exhaustive match with no `_ =>`, so the compiler forces
/// classification of every new variant. A forgotten classification is the path
/// to "went down the retry branch and silently lost events".
#[derive(Debug, Error)]
pub enum PgcdcError {
    #[error("replication slot {slot} does not exist")]
    SlotMissing { slot: String },

    #[error("replication slot {slot} is ahead of our durable position: slot={slot_lsn}, durable={durable}")]
    SlotAhead {
        slot: String,
        slot_lsn: String,
        durable: String,
    },

    /// Reached by two different paths, and the message below is deliberately
    /// neutral to which one fired: (1) the server RESPONDED to `START_REPLICATION`
    /// and explicitly refused — the slot is invalidated (`SQLSTATE 55000`) or
    /// carries a foreign output plugin (`SQLSTATE 22023`); or (2) the pre-flight
    /// check (`preflight_slot`/`slot_health_is_terminal`, `src/postgres/guard.rs`,
    /// `src/postgres/replication.rs`) already read `wal_status = 'lost'` from
    /// `pg_replication_slots` and refuses BEFORE `START_REPLICATION` is ever sent,
    /// saving the round trip. Either way, retrying is not a matter of transport
    /// luck: the same slot will get the same refusal an hour from now too. Unlike
    /// `SlotAhead`, we don't even know the position discrepancy here — path (1)
    /// never gets that far, and path (2) never asks.
    #[error("replication slot {slot} is unusable: {reason}")]
    SlotUnusable { slot: String, reason: String },

    /// The slot responds with a "busy" race (`SQLSTATE 55006`,
    /// `ERRCODE_OBJECT_IN_USE`) in a row for longer than the configured patience
    /// budget (`--slot-busy-budget-ms`, `SlotBusyPatience` in `replication.rs`). Under
    /// the same status code, "our prior session hasn't disconnected yet" and
    /// "someone else is holding the slot forever" are indistinguishable — the only
    /// discriminator is physical: our own walsender releases the slot within
    /// tens of milliseconds (measured), a foreign consumer does not. Exceeding the
    /// budget means this is not a race with ourselves, and a silent perpetual
    /// reconnect here is exactly the class of failure invariant 3 was written
    /// against (DECISIONS §1): the process looks alive, but the slot is unreachable.
    #[error(
        "replication slot {slot} stayed busy (SQLSTATE 55006) for {waited_ms}ms, exceeding the \
         configured patience budget of {budget_ms}ms — most likely held by a foreign consumer, \
         not our own prior session"
    )]
    SlotBusyTimedOut {
        slot: String,
        waited_ms: u64,
        budget_ms: u64,
    },

    #[error("malformed pgoutput message: {0}")]
    Decode(String),

    #[error("unsupported pgoutput message kind {kind:?}")]
    UnsupportedMessage { kind: char },

    #[error("unknown relation id {relation_id}")]
    UnknownRelation { relation_id: u32 },

    #[error("transaction exceeded {limit} events")]
    TransactionTooLarge { limit: usize },

    #[error("refusing to acknowledge {attempted} beyond durable position {durable}")]
    AckBeyondDurable { attempted: String, durable: String },

    #[error("sink failed: {0}")]
    Sink(String),

    #[error("postgres connection error: {0}")]
    Connection(String),

    #[error("database URL must start with postgres:// or postgresql:// (libpq key=value connection strings are not supported)")]
    InvalidDatabaseUrl,

    #[error(
        "reconnect_initial_ms ({initial}) must not exceed reconnect_max_ms ({max}): the first \
         retry would sleep for the ceiling duration and then collapse to it on every attempt after"
    )]
    InvalidReconnectBounds { initial: u64, max: u64 },
}

impl PgcdcError {
    /// A machine-readable label for the structured log (DECISIONS Q22).
    pub fn kind(&self) -> &'static str {
        match self {
            Self::SlotMissing { .. } => "slot_missing",
            Self::SlotAhead { .. } => "slot_ahead",
            Self::SlotUnusable { .. } => "slot_unusable",
            Self::SlotBusyTimedOut { .. } => "slot_busy_timed_out",
            Self::Decode(_) => "decode",
            Self::UnsupportedMessage { .. } => "unsupported_message",
            Self::UnknownRelation { .. } => "unknown_relation",
            Self::TransactionTooLarge { .. } => "transaction_too_large",
            Self::AckBeyondDurable { .. } => "ack_beyond_durable",
            Self::Sink(_) => "sink",
            Self::Connection(_) => "connection",
            Self::InvalidDatabaseUrl => "invalid_database_url",
            Self::InvalidReconnectBounds { .. } => "invalid_reconnect_bounds",
        }
    }

    pub fn is_fatal(&self) -> bool {
        match self {
            Self::SlotMissing { .. } => true,
            Self::SlotAhead { .. } => true,
            Self::SlotUnusable { .. } => true,
            Self::SlotBusyTimedOut { .. } => true,
            Self::Decode(_) => true,
            Self::UnsupportedMessage { .. } => true,
            Self::UnknownRelation { .. } => true,
            Self::TransactionTooLarge { .. } => true,
            Self::AckBeyondDurable { .. } => true,
            Self::Sink(_) => true,
            Self::Connection(_) => false,
            Self::InvalidDatabaseUrl => true,
            Self::InvalidReconnectBounds { .. } => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_connection_errors_are_recoverable() {
        assert!(!PgcdcError::Connection("boom".into()).is_fatal());
        assert!(PgcdcError::SlotMissing { slot: "s".into() }.is_fatal());
        assert!(PgcdcError::Decode("bad".into()).is_fatal());
        assert!(PgcdcError::UnsupportedMessage { kind: 'T' }.is_fatal());
    }

    #[test]
    fn a_slot_that_the_server_refuses_to_stream_from_is_fatal() {
        // A server that refused START_REPLICATION (invalidation,
        // foreign output plugin) is not the same as a dropped connection.
        let err = PgcdcError::SlotUnusable {
            slot: "s".into(),
            reason: "boom".into(),
        };
        assert!(err.is_fatal());
        assert_eq!(err.kind(), "slot_unusable");
    }

    #[test]
    fn a_slot_busy_race_that_outlives_the_patience_budget_is_fatal() {
        let err = PgcdcError::SlotBusyTimedOut {
            slot: "s".into(),
            waited_ms: 5000,
            budget_ms: 3000,
        };
        assert!(err.is_fatal());
        assert_eq!(err.kind(), "slot_busy_timed_out");
    }

    #[test]
    fn every_error_has_a_machine_readable_kind() {
        assert_eq!(PgcdcError::Decode("x".into()).kind(), "decode");
        assert_eq!(
            PgcdcError::SlotMissing { slot: "s".into() }.kind(),
            "slot_missing"
        );
    }
}
