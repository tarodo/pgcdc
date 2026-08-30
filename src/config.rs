use std::fmt;

use clap::{Parser, ValueEnum};

/// Обёртка над строкой подключения. Ручные Debug и Display вырезают пароль,
/// поэтому утечь он может только через явный `expose()`.
#[derive(Clone)]
pub struct DatabaseUrl(String);

impl DatabaseUrl {
    pub fn new(raw: String) -> Self {
        Self(raw)
    }

    /// Единственный способ получить строку с паролем. Использовать только
    /// при передаче в драйвер, никогда в лог.
    pub fn expose(&self) -> &str {
        &self.0
    }

    fn redacted(&self) -> String {
        // Ищем `://user:password@` и заменяем пароль звёздочками.
        let Some(scheme_end) = self.0.find("://") else {
            return self.0.clone();
        };
        let rest = &self.0[scheme_end + 3..];
        let Some(at) = rest.find('@') else {
            return self.0.clone();
        };
        let creds = &rest[..at];
        match creds.find(':') {
            Some(colon) => format!(
                "{}://{}:****@{}",
                &self.0[..scheme_end],
                &creds[..colon],
                &rest[at + 1..]
            ),
            None => self.0.clone(),
        }
    }
}

impl fmt::Display for DatabaseUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.redacted())
    }
}

impl fmt::Debug for DatabaseUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DatabaseUrl({})", self.redacted())
    }
}

/// clap требует именно `FromStr`: одного `From<String>` для `#[arg]` недостаточно,
/// и без этой реализации derive не соберётся.
impl std::str::FromStr for DatabaseUrl {
    type Err = std::convert::Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(s.to_owned()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputKind {
    Stdout,
}

#[derive(Debug, Parser)]
#[command(name = "pgcdc", about = "PostgreSQL CDC via logical replication")]
pub struct Config {
    #[arg(long, env = "PGCDC_DATABASE_URL")]
    pub database_url: DatabaseUrl,

    #[arg(long, env = "PGCDC_PUBLICATION")]
    pub publication: String,

    #[arg(long, env = "PGCDC_SLOT")]
    pub slot: String,

    #[arg(long, env = "PGCDC_OUTPUT", value_enum, default_value = "stdout")]
    pub output: OutputKind,

    #[arg(long, env = "PGCDC_MAX_TRANSACTION_EVENTS", default_value = "100000")]
    pub max_transaction_events: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_never_reaches_debug_or_display() {
        // Требование §4 спеки — это тип, а не «не забыть»: раз Debug вырезает
        // пароль, ни #[derive(Debug)] на конфиге, ни поле tracing не смогут его слить.
        let url = DatabaseUrl::new("postgres://cdc:hunter2@db.example:5432/app".into());
        assert!(!format!("{url:?}").contains("hunter2"));
        assert!(!format!("{url}").contains("hunter2"));
        assert!(
            format!("{url}").contains("cdc"),
            "имя пользователя остаётся видимым"
        );
        assert!(format!("{url}").contains("db.example"));
        assert_eq!(url.expose(), "postgres://cdc:hunter2@db.example:5432/app");
    }

    #[test]
    fn url_without_a_password_is_unchanged() {
        let url = DatabaseUrl::new("postgres://cdc@db.example:5432/app".into());
        assert!(format!("{url}").contains("cdc@db.example"));
    }
}
