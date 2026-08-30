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

/// Три позиции, которые нельзя путать (DECISIONS Q4). Четвёртая, `processed`, —
/// работа этапа 3 (`DECISIONS.md` §4) и здесь намеренно ещё не появляется.
/// Персистентности нет: слот PostgreSQL — единственный источник истины,
/// трекер живёт только в памяти процесса.
#[derive(Debug, Default)]
pub struct LsnTracker {
    received: Lsn,
    durable: Lsn,
    acked: Lsn,
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
}
