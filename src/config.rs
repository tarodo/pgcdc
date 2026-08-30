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

/// libpq принимает `key=value`-строки (`host=... user=... password=...`), но ни
/// `redacted()` выше, ни `replication_url` в `postgres/replication.rs` не умеют
/// с ними работать: первая не находит `://` и возвращает пароль как есть, вторая
/// приклеивает `?replication=database` к строке без `?`/`&`-грамматики. Принять
/// такую форму и потом не суметь её ни спрятать, ни расширить — хуже, чем
/// отказать сразу, поэтому здесь отвергается всё, что не выглядит как URL.
#[derive(Debug, thiserror::Error)]
#[error(
    "database URL must be a URL, e.g. postgres://user:password@host:5432/dbname \
     (libpq `key=value` connection strings are not supported)"
)]
pub struct InvalidDatabaseUrl;

/// clap требует именно `FromStr`: одного `From<String>` для `#[arg]` недостаточно,
/// и без этой реализации derive не соберётся.
impl std::str::FromStr for DatabaseUrl {
    type Err = InvalidDatabaseUrl;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !s.contains("://") {
            return Err(InvalidDatabaseUrl);
        }
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
    // hide_env_values: без него clap печатает СЫРОЕ значение переменной
    // окружения в `--help` (`[env: PGCDC_DATABASE_URL=postgres://...:hunter2@...]`),
    // в обход DatabaseUrl и его редактирующих Debug/Display целиком.
    #[arg(long, env = "PGCDC_DATABASE_URL", hide_env_values = true)]
    pub database_url: DatabaseUrl,

    // Осознанное решение: publication/slot/output/max_transaction_events секретов
    // не несут, поэтому им hide_env_values не нужен — видимость текущего значения
    // в `--help` тут помогает отладке конфигурации и ничего не утекает.
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
    use clap::CommandFactory;

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

    #[test]
    fn libpq_key_value_form_is_rejected() {
        // Живой прогон ревьюера: этот вид строки проходит preflight guard
        // (tokio-postgres его понимает) и утекает через Debug/Display, а
        // replication_url ломает его наложением `?replication=database`.
        // Отказ на этапе парсинга должен наступить раньше, чем что-либо из
        // этого случится.
        use std::str::FromStr;
        let err = DatabaseUrl::from_str(
            "host=127.0.0.1 port=5432 user=postgres password=postgres dbname=app",
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("URL"),
            "сообщение должно направить к форме URL"
        );
        assert!(
            !msg.contains("host=127.0.0.1"),
            "сообщение об ошибке не должно эхом повторять введённую строку"
        );
    }

    #[test]
    fn url_form_is_accepted() {
        use std::str::FromStr;
        let url = DatabaseUrl::from_str("postgres://cdc:hunter2@db.example:5432/app").unwrap();
        assert_eq!(url.expose(), "postgres://cdc:hunter2@db.example:5432/app");
    }

    #[test]
    fn help_output_never_shows_the_env_var_password() {
        // clap's default `[env: VAR=value]` help hint bypasses DatabaseUrl entirely:
        // it prints the raw environment string, not our redacting Display. A test
        // that only exercises Debug/Display would stay green while this leaks.
        // SAFETY: cargo test runs this binary's tests in one process and no other
        // test reads or writes PGCDC_DATABASE_URL, so this mutation is not racy
        // with the rest of the suite.
        unsafe {
            std::env::set_var(
                "PGCDC_DATABASE_URL",
                "postgres://cdc:hunter2@db.example:5432/app",
            );
        }
        let help = Config::command().render_help().to_string();
        unsafe {
            std::env::remove_var("PGCDC_DATABASE_URL");
        }
        assert!(
            !help.contains("hunter2"),
            "password leaked into --help output:\n{help}"
        );
    }
}
