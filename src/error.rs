use thiserror::Error;

/// Разделение recoverable/fatal живёт в типе, а не в комментарии: `is_fatal`
/// реализован исчерпывающим match без `_ =>`, поэтому компилятор заставит
/// классифицировать каждый новый вариант. Забытая классификация — это путь
/// «поехали по ветке ретрая и молча потеряли события».
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
    /// Машиночитаемая метка для структурированного лога (DECISIONS Q22).
    pub fn kind(&self) -> &'static str {
        match self {
            Self::SlotMissing { .. } => "slot_missing",
            Self::SlotAhead { .. } => "slot_ahead",
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
    fn every_error_has_a_machine_readable_kind() {
        assert_eq!(PgcdcError::Decode("x".into()).kind(), "decode");
        assert_eq!(
            PgcdcError::SlotMissing { slot: "s".into() }.kind(),
            "slot_missing"
        );
    }
}
