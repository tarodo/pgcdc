use std::fmt;

use crate::error::PgcdcError;

/// Позиция в WAL. PostgreSQL печатает её как две шестнадцатеричные половины
/// через слэш, без ведущих нулей: `0/19300D0`.
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

/// Четыре позиции, которые нельзя путать (DECISIONS Q4, Q26a). `processed` —
/// работа этапа 3 (`DECISIONS.md` §4): она может опережать `durable`, и это
/// разрыв, ради которого её завели. Персистентности нет: слот PostgreSQL —
/// единственный источник истины, трекер живёт только в памяти процесса.
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

    /// Вызывается только после того, как sink подтвердил запись.
    pub fn note_durable(&mut self, lsn: Lsn) {
        if lsn > self.durable {
            self.durable = lsn;
        }
    }

    /// Отвергает попытку подтвердить позицию дальше durable. Это не
    /// оборонительное программирование, а тот самый инвариант: пройди такое
    /// подтверждение, и крах между ним и записью означал бы тихую потерю.
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

    /// Позиция, до которой сообщения разобраны и отданы sink'у. Может опережать
    /// `durable`: между записью и fsync существует окно, и именно из-за него
    /// условие продвижения по keepalive (Q26a) требует `processed == durable`,
    /// а не только пустого буфера сборщика.
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
        // Значения из docs/pgoutput-notes.md §4, 0004_commit.bin
        assert_eq!(Lsn(0x0000_0000_0193_00D0).to_string(), "0/19300D0");
        assert_eq!(Lsn(0x0000_0000_0193_0100).to_string(), "0/1930100");
        // Старшая половина не нулевая
        assert_eq!(Lsn(0x0000_0001_0000_00FF).to_string(), "1/FF");
        assert_eq!(Lsn(0).to_string(), "0/0");
    }

    #[test]
    fn tracker_refuses_to_ack_beyond_durable() {
        // Единственный инвариант, ради которого существует проект:
        // никогда не подтверждать позицию, которую sink не записал.
        let mut t = LsnTracker::new();
        t.note_received(Lsn(0x2000));
        t.note_durable(Lsn(0x1000));
        assert!(
            t.try_ack(Lsn(0x1000)).is_ok(),
            "подтвердить ровно durable можно"
        );
        assert!(
            t.try_ack(Lsn(0x1001)).is_err(),
            "на байт дальше durable — нельзя"
        );
        assert_eq!(
            t.acked(),
            Lsn(0x1000),
            "неудачная попытка не сдвигает acked"
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
            "откат подтверждения молча игнорируется"
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
        assert_eq!(t.processed(), Lsn(0x2000), "позиция не откатывается");
    }

    #[test]
    fn processed_may_run_ahead_of_durable() {
        // Ровно та ситуация, ради которой позиция и заведена: транзакция
        // отдана в sink, но fsync ещё не случился.
        let mut t = LsnTracker::new();
        t.note_processed(Lsn(0x2000));
        assert_eq!(t.durable(), Lsn(0));
        assert!(t.processed() > t.durable());
        assert!(
            t.try_ack(Lsn(0x2000)).is_err(),
            "подтверждать по processed нельзя"
        );
    }
}
