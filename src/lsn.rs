use std::fmt;

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
}
