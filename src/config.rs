use std::fmt;

use clap::{Parser, ValueEnum};

use crate::error::PgcdcError;

/// Wrapper over the connection string. Manual Debug and Display strip the password,
/// so it can only leak through an explicit `expose()`.
#[derive(Clone)]
pub struct DatabaseUrl(String);

impl DatabaseUrl {
    pub fn new(raw: String) -> Self {
        Self(raw)
    }

    /// The only way to get the string with the password. Use only
    /// when passing it to the driver, never for logging.
    pub fn expose(&self) -> &str {
        &self.0
    }

    fn redacted(&self) -> String {
        // Look for `://user:password@` and replace the password with asterisks. The
        // password can also arrive as a query parameter (`?password=...`) — this
        // is not made up, drivers accept both forms — so a second pass
        // cleans that too.
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

    /// We accept only the URL form. We reject the libpq string (`host=... password=...`):
    /// it cannot be redacted (`redacted()` won't find `@` and will return the input verbatim),
    /// nor correctly extended with the replication parameter. Accepting a format we don't know
    /// how to handle would mean leaking the secret and still failing.
    pub fn validate(&self) -> Result<(), PgcdcError> {
        if self.0.starts_with("postgres://") || self.0.starts_with("postgresql://") {
            Ok(())
        } else {
            Err(PgcdcError::InvalidDatabaseUrl)
        }
    }
}

/// Replaces the `password` query parameter's value with asterisks, if present.
/// Works on top of an already-processed (or unprocessed, if `@` wasn't found)
/// string — `redacted()` is the only caller, it's a separate function simply
/// because this is its own logic, unrelated to credentials in the URL authority.
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

/// Parsing intentionally cannot fail. Returning an error here would make clap print
/// the rejected value in full inside its own "invalid value '...'" wrapper, and the
/// password would end up in stderr. The check lives in `validate()`, called on the
/// first line of `run()`, where we control the error text.
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
    // hide_env_values: without it, clap prints the RAW environment variable
    // value in `--help` (`[env: PGCDC_DATABASE_URL=postgres://...:hunter2@...]`),
    // bypassing DatabaseUrl and its redacting Debug/Display entirely.
    #[arg(long, env = "PGCDC_DATABASE_URL", hide_env_values = true)]
    pub database_url: DatabaseUrl,

    // Deliberate decision: publication/slot/output/max_transaction_events carry no
    // secrets, so they don't need hide_env_values — the current value being visible
    // in `--help` helps debug the configuration here and nothing leaks.
    #[arg(long, env = "PGCDC_PUBLICATION")]
    pub publication: String,

    #[arg(long, env = "PGCDC_SLOT")]
    pub slot: String,

    #[arg(long, env = "PGCDC_OUTPUT", value_enum, default_value = "stdout")]
    pub output: OutputKind,

    /// Path for `--output file`. Required for that variant.
    #[arg(long, env = "PGCDC_OUTPUT_PATH")]
    pub output_path: Option<std::path::PathBuf>,

    #[arg(long, env = "PGCDC_MAX_TRANSACTION_EVENTS", default_value = "100000")]
    pub max_transaction_events: usize,

    // The lower bound — 1 — is forbidden by the parser, not just described in
    // the help text, because with zero the "interval elapsed" condition is true
    // on every loop iteration — a busy spin with an fsync on every idle tick.
    // Reading is bounded by a separate constant SHUTDOWN_POLL_INTERVAL (200ms,
    // replication.rs), not by this field — both consequences of that are
    // spelled out in the --help text itself below.
    /// How often the durability barrier is invoked and an acknowledgement goes out.
    /// A delayed acknowledgement does not affect correctness: invariant 1
    /// still holds, and the contract allows duplicates after a failure. The lower
    /// bound is 1: with zero, the "interval elapsed" condition is true on every
    /// iteration of the loop, i.e. a busy spin with an fsync on every idle tick.
    /// We forbid this at the parser level, not just in a comment.
    ///
    /// Two consequences of the fact that reading is bounded by a separate constant
    /// `SHUTDOWN_POLL_INTERVAL` (200ms, `replication.rs`) rather than by this field:
    /// - Above 200ms: the loop still wakes up once every 200ms, and
    ///   keepalive advancement is checked on every iteration — so on an idle
    ///   publication, the acknowledgement to the slot now goes out at the
    ///   polling frequency rather than at this interval's frequency. Probably
    ///   for the better (the slot stays fresher), but it is a behavioral change.
    /// - Below 200ms: the barrier's elapsed check runs only after
    ///   returning from a read, and reading is capped at 200ms. On an
    ///   idle stream this sets the barrier period's effective floor at
    ///   200ms: at the parser's minimum (1ms), a transaction accepted right
    ///   before a lull in traffic will wait about 200ms for the barrier, not 1ms.
    ///   Under a real stream of events, the loop is driven by frame arrival, and
    ///   the configured period is honored.
    #[arg(
        long,
        env = "PGCDC_ACK_INTERVAL_MS",
        default_value = "200",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub ack_interval_ms: u64,

    /// Initial pause before the first reconnect attempt.
    /// Deliberately NOT derived from `ack_interval_ms`: that one only sets the
    /// barrier period (the read timeout is a separate, non-configurable constant,
    /// see `SHUTDOWN_POLL_INTERVAL`), and tying the reconnect pause to it
    /// would mean that trying to speed up acknowledgement would make hammering
    /// a downed server more frequent.
    #[arg(long, env = "PGCDC_RECONNECT_INITIAL_MS", default_value = "100",
          value_parser = clap::value_parser!(u64).range(1..))]
    pub reconnect_initial_ms: u64,

    /// Ceiling for the pause. Exponential growth stops here and further
    /// attempts repeat indefinitely: a network failure is not a reason to bring
    /// down the process (DECISIONS Q19).
    #[arg(long, env = "PGCDC_RECONNECT_MAX_MS", default_value = "30000",
          value_parser = clap::value_parser!(u64).range(1..))]
    pub reconnect_max_ms: u64,

    /// Upper bound on the total time the slot can respond with a "busy" race
    /// (`SQLSTATE 55006`, `ERRCODE_OBJECT_IN_USE`) in a row before
    /// this stops counting as a race resolving itself with our own prior
    /// session and becomes a fatal error (`PgcdcError::SlotBusyTimedOut`).
    /// Postgres responds with the SAME status code in both cases —
    /// "our own walsender hasn't disconnected yet" and "someone else
    /// is holding the slot forever" — the only physical discriminator
    /// is DURATION, not the code itself (`SlotBusyPatience`, `replication.rs`).
    ///
    /// The default is justified by measurement, not guesswork: 30 cycles of
    /// "walsender holds the slot → drop → timing to the next successful
    /// `START_REPLICATION` from scratch, including establishing a new connection"
    /// yielded 45–124ms (median ~76ms) — this is the same operation
    /// `stream_once` performs on every reconnect. The raw time to clear the
    /// `pg_replication_slots.active` flag, measured separately (without the
    /// overhead of a new connection), was 1.1–3.5ms. The default of 30000ms gives
    /// a margin of ~240× over the worst observed full reconnect cycle and
    /// ~8500× over the raw slot-release time.
    ///
    /// The counter accumulates only genuinely continuous race time: a failure of
    /// a different nature (transport failure, unreachable server) does not take
    /// away the accumulated time, but it does break the chain — the entire interval
    /// from the last race observation to the next does not count toward the
    /// budget, because we don't know whether busyness held throughout it.
    /// So it is not any perpetual busyness that escalates, but one that has
    /// an unbroken chain of observations spanning the budget: it accumulates
    /// under rare unrelated failures, but not under a failure on every other
    /// attempt.
    /// Only a successful session start (`classify_start_outcome`, the `Ok`
    /// branch) fully closes the counter — the only observation that proves
    /// the slot is free right now; so unrelated rare races that happen over
    /// months of a long-running process's uptime, separated by at least one
    /// successful connection, do not sum into a single fatal exit
    /// (`SlotBusyPatience`, `replication.rs`).
    #[arg(long, env = "PGCDC_SLOT_BUSY_BUDGET_MS", default_value = "30000",
          value_parser = clap::value_parser!(u64).range(1..))]
    pub slot_busy_budget_ms: u64,
}

impl Config {
    /// `clap` checks each bound separately (`range(1..)`), but not their
    /// relationship to each other. An initial pause greater than the ceiling means
    /// the first attempt would sleep through the long pause, and `next_backoff`
    /// would immediately collapse it back to the ceiling — a configuration that is
    /// technically valid for the parser but pointless.
    pub fn validate_reconnect_bounds(&self) -> Result<(), PgcdcError> {
        if self.reconnect_initial_ms > self.reconnect_max_ms {
            return Err(PgcdcError::InvalidReconnectBounds {
                initial: self.reconnect_initial_ms,
                max: self.reconnect_max_ms,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;
    use clap::Parser;

    use super::*;

    /// The minimal set of arguments sufficient for a successful parse, without
    /// `--ack-interval-ms` — the test appends it itself.
    fn base_args() -> Vec<&'static str> {
        vec![
            "pgcdc",
            "--database-url",
            "postgres://u:p@h:5432/db",
            "--publication",
            "p",
            "--slot",
            "s",
        ]
    }

    #[test]
    fn ack_interval_ms_rejects_zero_at_the_parser_level() {
        // 0 means `elapsed() >= interval` is true on every loop iteration —
        // a busy spin with an fsync on every pass. The parser must reject
        // this, so that a configuration with zero cannot be expressed at
        // all, rather than merely hoping the loop somehow survives such a
        // value.
        let mut args = base_args();
        args.extend(["--ack-interval-ms", "0"]);
        let err = Config::try_parse_from(args).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
    }

    #[test]
    fn ack_interval_ms_accepts_one_as_the_lowest_valid_value() {
        let mut args = base_args();
        args.extend(["--ack-interval-ms", "1"]);
        let cfg = Config::try_parse_from(args).expect("1 must be valid");
        assert_eq!(cfg.ack_interval_ms, 1);
    }

    // `ack_interval_ms` had two such tests (zero forbidden, one allowed),
    // the new backoff flags had none — so the disappearance of the parser
    // constraint would have gone unnoticed.
    #[test]
    fn reconnect_initial_ms_rejects_zero_at_the_parser_level() {
        let mut args = base_args();
        args.extend(["--reconnect-initial-ms", "0"]);
        let err = Config::try_parse_from(args).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
    }

    #[test]
    fn reconnect_initial_ms_accepts_one_as_the_lowest_valid_value() {
        let mut args = base_args();
        args.extend(["--reconnect-initial-ms", "1"]);
        let cfg = Config::try_parse_from(args).expect("1 must be valid");
        assert_eq!(cfg.reconnect_initial_ms, 1);
    }

    #[test]
    fn reconnect_max_ms_rejects_zero_at_the_parser_level() {
        let mut args = base_args();
        args.extend(["--reconnect-max-ms", "0"]);
        let err = Config::try_parse_from(args).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
    }

    #[test]
    fn reconnect_max_ms_accepts_one_as_the_lowest_valid_value() {
        let mut args = base_args();
        args.extend(["--reconnect-max-ms", "1", "--reconnect-initial-ms", "1"]);
        let cfg = Config::try_parse_from(args).expect("1 must be valid");
        assert_eq!(cfg.reconnect_max_ms, 1);
    }

    #[test]
    fn slot_busy_budget_ms_rejects_zero_at_the_parser_level() {
        let mut args = base_args();
        args.extend(["--slot-busy-budget-ms", "0"]);
        let err = Config::try_parse_from(args).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
    }

    #[test]
    fn slot_busy_budget_ms_accepts_one_as_the_lowest_valid_value() {
        let mut args = base_args();
        args.extend(["--slot-busy-budget-ms", "1"]);
        let cfg = Config::try_parse_from(args).expect("1 must be valid");
        assert_eq!(cfg.slot_busy_budget_ms, 1);
    }

    #[test]
    fn slot_busy_budget_ms_defaults_to_thirty_seconds() {
        let cfg = Config::try_parse_from(base_args()).expect("minimal arguments are valid");
        assert_eq!(cfg.slot_busy_budget_ms, 30_000);
    }

    #[test]
    fn an_initial_delay_above_the_ceiling_is_rejected() {
        // The parser only sees each bound separately; the relationship between
        // them is checked by validate_reconnect_bounds().
        let mut args = base_args();
        args.extend([
            "--reconnect-initial-ms",
            "5000",
            "--reconnect-max-ms",
            "1000",
        ]);
        let cfg = Config::try_parse_from(args).expect("both bounds are individually valid");
        let err = cfg.validate_reconnect_bounds().unwrap_err();
        assert!(matches!(err, PgcdcError::InvalidReconnectBounds { .. }));
        assert!(err.is_fatal());
    }

    #[test]
    fn equal_initial_and_max_are_accepted() {
        let mut args = base_args();
        args.extend([
            "--reconnect-initial-ms",
            "1000",
            "--reconnect-max-ms",
            "1000",
        ]);
        let cfg = Config::try_parse_from(args).expect("both bounds are individually valid");
        assert!(cfg.validate_reconnect_bounds().is_ok());
    }

    #[test]
    fn from_str_accepts_anything_so_clap_never_echoes_the_input() {
        // clap prints the rejected value in its own "invalid value '...'" wrapper.
        // The only way to avoid this is to give clap no reason to reject:
        // parsing always succeeds, and the check lives in validate().
        let libpq = "host=db user=cdc password=hunter2 dbname=app";
        let parsed: DatabaseUrl = libpq.parse().expect("parsing must be infallible");
        assert!(parsed.validate().is_err(), "but validate must reject this");
    }

    #[test]
    fn validate_rejects_libpq_key_value_form() {
        let url = DatabaseUrl::new("host=db user=cdc password=hunter2".into());
        let err = url.validate().unwrap_err();
        assert!(matches!(err, PgcdcError::InvalidDatabaseUrl));
        assert!(
            !err.to_string().contains("hunter2"),
            "error text must not contain the input: {err}"
        );
    }

    #[test]
    fn validate_rejects_a_password_containing_a_scheme_separator() {
        // A substring check for "contains ://" let through a libpq string whose
        // PASSWORD contains ://, and redacted() would return such a string verbatim.
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
        // The requirement from spec §4 is enforced by the type, not by "don't forget":
        // since Debug strips the password, neither #[derive(Debug)] on the config nor
        // a tracing field can leak it.
        let url = DatabaseUrl::new("postgres://cdc:hunter2@db.example:5432/app".into());
        assert!(!format!("{url:?}").contains("hunter2"));
        assert!(!format!("{url}").contains("hunter2"));
        assert!(
            format!("{url}").contains("cdc"),
            "the username stays visible"
        );
        assert!(format!("{url}").contains("db.example"));
        assert_eq!(url.expose(), "postgres://cdc:hunter2@db.example:5432/app");

        // The password can also arrive as a query parameter, not in the
        // credentials segment. `validate()` only looks at the scheme and would let
        // such a URL through — redaction must separately cover this form.
        let query_form = DatabaseUrl::new("postgres://cdc@db.example/app?password=hunter2".into());
        assert!(!format!("{query_form:?}").contains("hunter2"));
        assert!(!format!("{query_form}").contains("hunter2"));
        assert!(
            format!("{query_form}").contains("password=****"),
            "the parameter stays in the form, but without a value: {query_form}"
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
