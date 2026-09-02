#![allow(dead_code)]

use std::sync::{Arc, Mutex, OnceLock};

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// Shared container setup: the base flags every test needs (logical
/// decoding enabled, room for slots/senders), plus whatever extra `-c`
/// flags the caller wants appended. Kept private — `start_postgres` and
/// `start_postgres_with_tight_wal_retention` are the two public entry
/// points, so a change to the wait-strategy or port-retry logic below
/// cannot accidentally diverge between them.
async fn start_postgres_with_extra_args(extra: &[&str]) -> (ContainerAsync<GenericImage>, String) {
    let mut cmd = vec![
        "postgres",
        "-c",
        "wal_level=logical",
        "-c",
        "max_replication_slots=10",
        "-c",
        "max_wal_senders=10",
    ];
    cmd.extend_from_slice(extra);

    let container = GenericImage::new("postgres", "16-alpine")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "app")
        .with_cmd(cmd)
        .start()
        .await
        .expect("start postgres");

    // The wait-strategy checks that Postgres accepts connections, not that
    // Docker's port forwarding already answers requests — that's a separate race
    // that was caught roughly once every ten runs. A bounded retry without a
    // blocking sleep in the wait loop: tokio::time::sleep is not a thread-blocking sleep.
    let port = {
        let mut attempt = 0;
        loop {
            match container.get_host_port_ipv4(5432.tcp()).await {
                Ok(port) => break port,
                Err(_) if attempt < 20 => {
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(err) => panic!("port after {attempt} retries: {err}"),
            }
        }
    };
    let conn = format!("postgres://postgres:postgres@127.0.0.1:{port}/app");
    (container, conn)
}

/// A fresh PostgreSQL for every test. The replication slot is a global
/// stateful object, and on a shared instance tests would fight over it and
/// depend on run order (DECISIONS Q10).
pub async fn start_postgres() -> (ContainerAsync<GenericImage>, String) {
    start_postgres_with_extra_args(&[]).await
}

/// A PostgreSQL tuned so a replication slot's WAL retention can be blown
/// through with a few megabytes of writes instead of gigabytes: pinning
/// `max_slot_wal_keep_size` this low means a single checkpoint past that
/// budget is enough for the server to mark the slot `wal_status = 'lost'`.
/// `min_wal_size`/`max_wal_size` are kept small too, purely so the
/// checkpoint that does the marking is cheap. Verified directly against
/// this image before use: one ~8MB insert plus one `CHECKPOINT` reliably
/// flips a freshly created slot straight to `lost` in well under a second,
/// matching the "reserved → lost in one step" transition this project's own
/// lab measured (docs/superpowers/plans/2026-09-02-slot-health-preflight.md).
pub async fn start_postgres_with_tight_wal_retention() -> (ContainerAsync<GenericImage>, String) {
    start_postgres_with_extra_args(&[
        "-c",
        "max_slot_wal_keep_size=1MB",
        "-c",
        "min_wal_size=32MB",
        "-c",
        "max_wal_size=64MB",
    ])
    .await
}

pub async fn connect(conn_str: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(conn_str, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// The demo schema from docker/init.sql, but created from the test code
/// so the slot's starting position can be controlled.
pub async fn setup_schema(client: &tokio_postgres::Client) {
    client
        .batch_execute(
            "CREATE TABLE public.users (id BIGINT PRIMARY KEY, name TEXT, email TEXT, bio TEXT);
             ALTER TABLE public.users REPLICA IDENTITY FULL;
             ALTER TABLE public.users ALTER COLUMN bio SET STORAGE EXTERNAL;
             CREATE PUBLICATION pgcdc_pub FOR TABLE public.users;",
        )
        .await
        .expect("setup schema");
}

/// A table with REPLICA IDENTITY DEFAULT — needed to get the 'K' tag.
/// `users` has identity FULL, which gives only 'O'.
pub async fn setup_items_table(client: &tokio_postgres::Client) {
    client
        .batch_execute(
            "CREATE TABLE public.items (id BIGINT PRIMARY KEY, title TEXT, qty INT);
             ALTER PUBLICATION pgcdc_pub ADD TABLE public.items;",
        )
        .await
        .expect("setup items");
}

pub async fn create_slot(client: &tokio_postgres::Client, slot: &str) {
    client
        .query(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot],
        )
        .await
        .expect("create slot");
}

/// Waits until the slot's `confirmed_flush_lsn` catches up with `target`.
/// The budget is time-based, not attempt-based.
///
/// The previous design bounded the number of ATTEMPTS (100 × 100ms sleep),
/// not the time: under the suite's parallel load (`RUST_TEST_THREADS=4`,
/// `.cargo/config.toml`), the check itself (`query_one`) sometimes takes
/// orders of magnitude longer than the sleep between attempts, and all 100
/// attempts get eaten up long before the actual 10 seconds pass, never
/// having seen the target position — that is exactly how
/// `a_productive_session_resets_the_backoff` failed about once every five
/// full runs. Measured during the stage 5 review: 50 checks under the suite's own
/// clean 4-thread load landed in 207-631ms; under extra unrelated host load,
/// a reproduced case had a single check stretch to 209 seconds and still
/// SUCCEED in waiting for the target — the slot kept acknowledging, just
/// slowly. 60 seconds is two orders of magnitude above the clean case and
/// below the reproduced stall, so the test fails at the bottom of a hang
/// rather than masking it with an unbounded wait.
pub async fn wait_for_slot_at_least(
    client: &tokio_postgres::Client,
    slot: &str,
    target: pgcdc::lsn::Lsn,
) -> pgcdc::lsn::Lsn {
    const BUDGET: std::time::Duration = std::time::Duration::from_secs(60);
    let deadline = tokio::time::Instant::now() + BUDGET;
    let mut last = pgcdc::lsn::Lsn(0);
    loop {
        let row = client
            .query_one(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .expect("query slot");
        let text: Option<String> = row.get(0);
        if let Some(t) = text {
            if let Some(lsn) = parse_lsn(&t) {
                last = lsn;
                if lsn >= target {
                    return lsn;
                }
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("slot did not catch up with {target} within {BUDGET:?}, stopped at {last}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Drops our replication connection from the server side. This is cheaper
/// than restarting the container and more accurately reproduces a network drop.
pub async fn terminate_replication_backend(client: &tokio_postgres::Client) {
    client
        .execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE backend_type = 'walsender'",
            &[],
        )
        .await
        .expect("terminate walsender");
}

/// Waits until the slot becomes active — that is, the walsender of the
/// just-spawned process has actually connected. A drop sent before that
/// point finds nobody and silently does nothing: the
/// `terminate_replication_backend` query filters by
/// `backend_type = 'walsender'`, and a noticeable amount of time really
/// passes between `spawn()`ing the binary and its first `START_REPLICATION`
/// (argument parsing, TCP setup, the preflight query) — not the few
/// milliseconds the test code needs to trigger a drop. Without this
/// synchronization, in a scenario with two drops the first drop is wasted,
/// and the whole test sees only one backoff series instead of two.
pub async fn wait_until_slot_active(client: &tokio_postgres::Client, slot: &str) {
    for _ in 0..100 {
        let row = client
            .query_one(
                "SELECT active FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .expect("query slot");
        let active: bool = row.get(0);
        if active {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("slot {slot} did not become active within 5 seconds");
}

/// Waits until the slot stops being listed as active (that is, the
/// walsender has actually disconnected) — needed before
/// `pg_replication_slot_advance`, which fails on an active slot.
pub async fn wait_until_slot_inactive(client: &tokio_postgres::Client, slot: &str) {
    for _ in 0..100 {
        let row = client
            .query_one(
                "SELECT active FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .expect("query slot");
        let active: bool = row.get(0);
        if !active {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("slot {slot} remains active");
}

/// Drops the slot, but only once it is actually inactive: `pg_terminate_backend`
/// only sends a signal and returns without waiting for the actual close —
/// `pg_drop_replication_slot` fails with "slot is active" during that window.
/// We retry instead of sleeping once at random.
pub async fn drop_slot_once_inactive(client: &tokio_postgres::Client, slot: &str) {
    for _ in 0..100 {
        if client
            .execute("SELECT pg_drop_replication_slot($1)", &[&slot])
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("failed to drop slot {slot}: it remains active");
}

/// A tracing subscriber that accumulates the text of each event's `message`
/// field into a shared buffer. Does not use `tracing-subscriber` — a minimal
/// hand-rolled implementation is enough and avoids a new dependency.
///
/// The buffer is shared across the whole test binary (the tracing
/// dispatcher is global for the process), so events from ALL tests running
/// in parallel land in one list. The message text alone — e.g.
/// `"postgres_connection_restored"` — is not tied to the test that triggered
/// it; if that same message ever starts being logged by another
/// successfully-reconnecting test, matching on the text alone becomes a
/// coincidence. So the visitor also records the `slot` field when the event
/// has one (`info!(slot = %..., "message")` — this is how preflight, start,
/// and connection restoration are all logged), and appends it to the message
/// as `"message slot=value"` — the caller can check both instead of relying
/// on one text being unique.
struct CapturingSubscriber {
    events: Arc<Mutex<Vec<String>>>,
}

#[derive(Default)]
struct MessageVisitor {
    message: String,
    slot: Option<String>,
}

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "message" => self.message = format!("{value:?}"),
            "slot" => self.slot = Some(format!("{value:?}")),
            _ => {}
        }
    }
}

impl tracing::Subscriber for CapturingSubscriber {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        let combined = match visitor.slot {
            Some(slot) => format!("{} slot={slot}", visitor.message),
            None => visitor.message,
        };
        self.events.lock().unwrap().push(combined);
    }
    fn enter(&self, _span: &tracing::span::Id) {}
    fn exit(&self, _span: &tracing::span::Id) {}
}

static LOG_EVENTS: OnceLock<Arc<Mutex<Vec<String>>>> = OnceLock::new();

/// Enables capturing tracing messages exactly once for the whole test
/// binary (the tracing dispatcher is global for the process,
/// `set_global_default` cannot be called twice) and returns the shared
/// buffer. Messages from ALL tests running at the same time land in one
/// list, but the strings looked for here are unique to the reconnect
/// scenario, so there will be no false matches.
pub fn capture_log_events() -> Arc<Mutex<Vec<String>>> {
    LOG_EVENTS
        .get_or_init(|| {
            let events = Arc::new(Mutex::new(Vec::new()));
            let subscriber = CapturingSubscriber {
                events: events.clone(),
            };
            // Ignore the error: if the dispatcher is already set (by another
            // test in the same binary, before `OnceLock::get_or_init`), the
            // shared buffer is exactly the one already in use.
            let _ = tracing::subscriber::set_global_default(subscriber);
            events
        })
        .clone()
}

/// A guard around `std::process::Child`: kills the process on panic if it is
/// still alive. `std::process::Child`, unlike `tokio::process::Child` with
/// `kill_on_drop(true)`, does nothing on `Drop` — an ordinary child process
/// outlives its handle. Tests kill the child binary explicitly before the
/// end of the scenario, but between `spawn()` and that explicit `kill()`
/// there are `.await`s and `unwrap()`/`assert!`s that can panic first;
/// without this guard, a panic partway through a test would orphan a
/// process that (now that it retries reconnecting forever) would keep
/// hammering the Postgres container even after it disappeared along with
/// the test.
pub struct KillOnDrop(pub std::process::Child);

impl std::ops::Deref for KillOnDrop {
    type Target = std::process::Child;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for KillOnDrop {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        // `try_wait` first: if the test already waited for the process
        // itself (the normal path), reaping has already happened, and a
        // blind repeated `kill`/`wait` would risk hitting a different
        // process if the OS has reused the pid by then. We kill and wait
        // only when the process is confirmed to still be alive — exactly
        // the case this guard needs to cover (a panic before the test's
        // explicit kill).
        if matches!(self.0.try_wait(), Ok(None)) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

/// Spawns the binary with stderr captured, wrapped in the existing guard, so
/// that a test failure does not leave behind a process that keeps
/// reconnecting forever after the container is gone.
pub fn spawn_with_stderr(args: &[&str]) -> KillOnDrop {
    KillOnDrop(
        std::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"))
            .args(args)
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("spawn the binary"),
    )
}

/// Strips ANSI sequences of the form `ESC [ ... <letter>` (SGR color/style
/// codes). Originally `tracing_subscriber::fmt()` in `main.rs` colored
/// output UNCONDITIONALLY, even when stderr was a plain pipe rather than a
/// terminal: in the raw bytes the field looked like `ESC[3m` (italic),
/// `"backoff_ms"`, `ESC[0m` (reset), `ESC[2m` (dim), `"="`, one more
/// `ESC[0m`, and only then the value — that is, TWO codes sit between the
/// field name and the equals sign, not one, and the string `"backoff_ms="`
/// never appeared in the bytes at all (an earlier version of this comment
/// mentioned only one code between them).
/// Since then `main.rs` enables coloring only when stderr is a real
/// terminal, and the pipe this test uses no
/// longer gets colored, but the stripping stays: the setting could become
/// unconditional again, and this helper must survive that regression
/// without relying on production not breaking it. Without this cleanup,
/// `collect_backoff_delays` would never find the field, whether or not the
/// backoff actually reset — the test would be blind, not merely wrong.
fn strip_ansi(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            // A CSI sequence: ESC '[' ... ends with the first letter.
            let mut lookahead = chars.clone();
            if lookahead.next() == Some('[') {
                chars = lookahead;
                for c2 in chars.by_ref() {
                    if c2.is_ascii_alphabetic() {
                        break;
                    }
                }
                continue;
            }
        }
        out.push(c);
    }
    out
}

/// Reads the child's stderr and returns the first `n` delays of the FIRST
/// attempt of each reconnect series — lines with `retry=1`, not the first
/// `n` reconnect lines overall. The distinction matters: if the first
/// series ever needs a second attempt (`retry=2`), its delay will also
/// grow — that is growth WITHIN one series along the same exponential, not
/// a broken reset between series, and "the first n lines in a row" would
/// conflate the two, failing red on a correct exponential.
/// Filtering by `retry=1` picks exactly the first attempt of
/// each series regardless of how many attempts that series ultimately
/// took. The budget is bounded: if not enough delays showed up, we fail
/// with what we actually saw instead of hanging.
pub async fn collect_backoff_delays(child: &mut KillOnDrop, n: usize) -> Vec<u64> {
    use std::io::{BufRead, BufReader};

    let stderr = child.stderr.take().expect("stderr captured at spawn");
    let handle = tokio::task::spawn_blocking(move || {
        let mut found = Vec::new();
        for raw_line in BufReader::new(stderr).lines().map_while(Result::ok) {
            let line = strip_ansi(&raw_line);
            if !line.contains("reconnecting") {
                continue;
            }
            // Fields are written structurally: we look them up by name, not position.
            let is_first_attempt_of_series = line.split("retry=").nth(1).and_then(|rest| {
                rest.chars()
                    .take_while(|c| c.is_ascii_digit())
                    .collect::<String>()
                    .parse::<u32>()
                    .ok()
            }) == Some(1);
            if !is_first_attempt_of_series {
                continue;
            }
            if let Some(rest) = line.split("backoff_ms=").nth(1) {
                let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
                if let Ok(v) = digits.parse::<u64>() {
                    found.push(v);
                    if found.len() >= n {
                        break;
                    }
                }
            }
        }
        found
    });

    match tokio::time::timeout(std::time::Duration::from_secs(20), handle).await {
        Ok(Ok(found)) if found.len() >= n => found,
        Ok(Ok(found)) => panic!("found only {} of {n} delays: {found:?}", found.len()),
        Ok(Err(e)) => panic!("reading stderr failed: {e}"),
        Err(_) => panic!("did not see {n} delays within 20 seconds"),
    }
}

/// PostgreSQL prints the position as two hex halves separated by a slash.
pub fn parse_lsn(text: &str) -> Option<pgcdc::lsn::Lsn> {
    let (hi, lo) = text.split_once('/')?;
    let hi = u64::from_str_radix(hi, 16).ok()?;
    let lo = u64::from_str_radix(lo, 16).ok()?;
    Some(pgcdc::lsn::Lsn((hi << 32) | lo))
}
