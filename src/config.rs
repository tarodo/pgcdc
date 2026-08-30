use std::fmt;

use clap::{Parser, ValueEnum};

use crate::error::PgcdcError;

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
        // Ищем `://user:password@` и заменяем пароль звёздочками. Пароль
        // может также приехать параметром запроса (`?password=...`) — это
        // не выдумка, драйверы принимают обе формы, — поэтому вторым
        // проходом чистим и его (C3 разбора всей ветки).
        redact_query_password(&self.redact_credentials())
    }

    fn redact_credentials(&self) -> String {
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

    /// Принимаем только URL-форму. Строку libpq (`host=... password=...`) отвергаем:
    /// её нельзя ни отредактировать (`redacted()` не найдёт `@` и вернёт ввод дословно),
    /// ни корректно дополнить параметром репликации. Принять формат, который мы не умеем
    /// обработать, — значит слить секрет и всё равно упасть.
    pub fn validate(&self) -> Result<(), PgcdcError> {
        if self.0.starts_with("postgres://") || self.0.starts_with("postgresql://") {
            Ok(())
        } else {
            Err(PgcdcError::InvalidDatabaseUrl)
        }
    }
}

/// Заменяет значение параметра запроса `password` на звёздочки, если он есть.
/// Работает поверх уже обработанной (или необработанной, если `@` не нашлось)
/// строки — `redacted()` — единственный вызывающий, отдельная функция просто
/// потому, что тут своя, не связанная с учётными данными в URL-authority логика.
fn redact_query_password(url: &str) -> String {
    let Some(q) = url.find('?') else {
        return url.to_string();
    };
    let (before, query) = url.split_at(q);
    let mut changed = false;
    let redacted_pairs: Vec<String> = query[1..]
        .split('&')
        .map(|pair| match pair.split_once('=') {
            Some((key, _)) if key == "password" => {
                changed = true;
                format!("{key}=****")
            }
            _ => pair.to_string(),
        })
        .collect();
    if !changed {
        return url.to_string();
    }
    format!("{before}?{}", redacted_pairs.join("&"))
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

/// Разбор намеренно не может провалиться. Если вернуть здесь ошибку, clap напечатает
/// отвергнутое значение целиком в своей обёртке «invalid value '...'», и пароль
/// окажется в stderr. Проверка живёт в `validate()`, который зовётся первой строкой
/// `run()`, где текст ошибки контролируем мы.
impl std::str::FromStr for DatabaseUrl {
    type Err = std::convert::Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(s.to_owned()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputKind {
    Stdout,
    File,
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

    /// Путь для `--output file`. Обязателен при этом варианте.
    #[arg(long, env = "PGCDC_OUTPUT_PATH")]
    pub output_path: Option<std::path::PathBuf>,

    #[arg(long, env = "PGCDC_MAX_TRANSACTION_EVENTS", default_value = "100000")]
    pub max_transaction_events: usize,
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn from_str_accepts_anything_so_clap_never_echoes_the_input() {
        // clap печатает отвергнутое значение в собственной обёртке «invalid value '...'».
        // Единственный способ этого избежать — не давать clap повода отвергнуть:
        // разбор всегда успешен, а проверка живёт в validate().
        let libpq = "host=db user=cdc password=hunter2 dbname=app";
        let parsed: DatabaseUrl = libpq.parse().expect("разбор обязан быть инфаллибельным");
        assert!(
            parsed.validate().is_err(),
            "но validate обязан это отвергнуть"
        );
    }

    #[test]
    fn validate_rejects_libpq_key_value_form() {
        let url = DatabaseUrl::new("host=db user=cdc password=hunter2".into());
        let err = url.validate().unwrap_err();
        assert!(matches!(err, PgcdcError::InvalidDatabaseUrl));
        assert!(
            !err.to_string().contains("hunter2"),
            "текст ошибки не должен содержать ввод: {err}"
        );
    }

    #[test]
    fn validate_rejects_a_password_containing_a_scheme_separator() {
        // Подстрочная проверка «содержит ://» пропускала libpq-строку, в ПАРОЛЕ
        // которой есть ://, а redacted() возвращал такую строку дословно.
        let url = DatabaseUrl::new("host=db password=weird://leak dbname=app".into());
        assert!(url.validate().is_err());
    }

    #[test]
    fn validate_accepts_both_url_schemes() {
        assert!(DatabaseUrl::new("postgres://u:p@h:5432/db".into())
            .validate()
            .is_ok());
        assert!(DatabaseUrl::new("postgresql://u:p@h:5432/db".into())
            .validate()
            .is_ok());
    }

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

        // C3: пароль может приехать и параметром запроса, а не в credentials
        // segment. `validate()` смотрит только на схему и такой URL пропустит —
        // редактирование обязано отдельно закрыть эту форму.
        let query_form = DatabaseUrl::new("postgres://cdc@db.example/app?password=hunter2".into());
        assert!(!format!("{query_form:?}").contains("hunter2"));
        assert!(!format!("{query_form}").contains("hunter2"));
        assert!(
            format!("{query_form}").contains("password=****"),
            "параметр остаётся в форме, но без значения: {query_form}"
        );
        assert!(format!("{query_form}").contains("cdc@db.example"));
    }

    #[test]
    fn url_without_a_password_is_unchanged() {
        let url = DatabaseUrl::new("postgres://cdc@db.example:5432/app".into());
        assert!(format!("{url}").contains("cdc@db.example"));
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
