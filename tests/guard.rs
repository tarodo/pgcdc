mod common;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use pgcdc::config::{Config, DatabaseUrl, OutputKind};
use pgcdc::error::PgcdcError;
use pgcdc::lsn::Lsn;
use pgcdc::metrics::Metrics;
use pgcdc::postgres::guard::preflight_slot;
use pgcdc::sink::{Durability, Sink};
use pgcdc::transaction::Transaction;
use tokio::sync::mpsc;

#[tokio::test(flavor = "multi_thread")]
async fn cold_start_fails_when_the_slot_is_missing_and_does_not_create_it() {
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;

    let err = preflight_slot(&conn, "pgcdc_slot").await.unwrap_err();
    assert!(matches!(err, PgcdcError::SlotMissing { .. }));
    assert!(err.is_fatal(), "a missing slot is a fatal error");

    // Key point: guard must not have created the slot as a side effect.
    let rows = client
        .query("SELECT slot_name FROM pg_replication_slots", &[])
        .await
        .unwrap();
    assert!(
        rows.is_empty(),
        "guard does not create the slot; that would mask data loss"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cold_start_returns_slot_positions_when_the_slot_exists() {
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let info = preflight_slot(&conn, "pgcdc_slot").await.unwrap();
    assert!(
        info.confirmed_flush_lsn.is_some(),
        "a fresh slot already has a position"
    );
    assert!(info.restart_lsn.is_some());

    // A freshly created slot on a server with default retention: reserved, not
    // yet held by anyone, with a catalog horizon pinned. safe_wal_size is NULL
    // under the default max_slot_wal_keep_size = -1 — unlimited retention is
    // not a number, and reading it as one would be wrong.
    assert_eq!(info.wal_status.as_deref(), Some("reserved"));
    assert!(!info.active, "nothing is streaming from it yet");
    assert!(
        info.catalog_xmin.is_some(),
        "a logical slot always pins a catalog horizon"
    );
    assert!(
        info.safe_wal_size.is_none(),
        "unlimited retention reports NULL, not a size"
    );
}

/// Feeds transactions into a channel so the test can wait for them and learn
/// each transaction's `end_lsn`. Duplicated from the equivalent `ChannelSink`
/// in `tests/integration.rs` rather than shared: each file in `tests/` is
/// its own test binary, and the two don't otherwise depend on each other.
struct ChannelSink(mpsc::UnboundedSender<Transaction>, Option<Lsn>);

#[async_trait]
impl Sink for ChannelSink {
    fn durability(&self) -> Durability {
        Durability::Fsync
    }
    async fn write_transaction(&mut self, tx: &Transaction) -> Result<(), PgcdcError> {
        self.0.send(tx.clone()).expect("send");
        self.1 = Some(tx.end_lsn);
        Ok(())
    }
    async fn flush(&mut self) -> Result<Option<Lsn>, PgcdcError> {
        Ok(self.1.take())
    }
}

/// Pulls one named field's value out of a composed log line, e.g. `"0/1"`
/// out of `"... restart_lsn=Some(\"0/1\") ..."` for `name = "restart_lsn"`.
/// Every value `slot_preflight_ok` logs is a single whitespace-free token
/// (an `Option<String>`/`Option<i64>`/`bool` in Debug form), so splitting on
/// whitespace and matching the `name=` prefix is exact — no logged value
/// contains a space.
fn log_field(line: &str, name: &str) -> String {
    let prefix = format!("{name}=");
    line.split_whitespace()
        .find(|tok| tok.starts_with(&prefix))
        .unwrap_or_else(|| panic!("field {name} not found in log line: {line}"))[prefix.len()..]
        .to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn preflight_log_line_does_not_swap_restart_lsn_and_confirmed_flush_lsn() {
    // Mutation coverage for the one line this whole feature exists to produce
    // (README's "slot_preflight_ok" section): swap the two `?info_slot...`
    // labels at the `info!` call site in `stream_once` (`replication.rs`) —
    // `restart_lsn = ?info_slot.confirmed_flush_lsn` and vice versa — and
    // every other test in the suite stays green, because both fields are
    // still `Option<Lsn>` and nothing else in the suite parses this log
    // line. An operator reading `restart_lsn` as the disk-retention risk
    // would actually be looking at the acknowledged position, and vice versa.
    //
    // The check: read `SlotInfo` directly from the guard (ground truth,
    // immune to a labeling bug in the log statement) and compare each field
    // in the log line against it BY NAME. On a slot that has only ever been
    // created and never streamed from, restart_lsn and confirmed_flush_lsn
    // start out equal — a swap would be invisible there. So this test drives
    // one real transaction through and forces a reconnect, so the SECOND
    // preflight log line reports a confirmed_flush_lsn that has moved past
    // restart_lsn (README: one acknowledgement moves confirmed_flush_lsn far
    // more than restart_lsn).
    let log_events = common::capture_log_events();

    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    let slot = "pgcdc_slot_label_check";
    common::create_slot(&client, slot).await;

    let (tx_send, mut tx_recv) = mpsc::unbounded_channel();
    let cfg = Config {
        database_url: DatabaseUrl::new(conn.clone()),
        publication: "pgcdc_pub".into(),
        slot: slot.into(),
        output: OutputKind::Stdout,
        output_path: None,
        max_transaction_events: 100_000,
        ack_interval_ms: 200,
        reconnect_initial_ms: 100,
        reconnect_max_ms: 30_000,
        slot_busy_budget_ms: 30_000,
    };
    let metrics = Arc::new(Metrics::new());
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(cfg, Box::new(ChannelSink(tx_send, None)), metrics).await
    });

    client
        .execute("INSERT INTO users VALUES (1, 'Alice', NULL, NULL)", &[])
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(20), tx_recv.recv())
        .await
        .expect("the first transaction should arrive within 20 seconds")
        .expect("channel closed");

    // Wait for our own barrier to bring the insert to durable and for the
    // slot on the server to reflect it — otherwise the reconnect below
    // could race ahead of the ack, and the next preflight would log the
    // same unmoved position as the cold start.
    common::wait_for_slot_at_least(&client, slot, first.end_lsn).await;

    // Force a reconnect: stream_once runs the guard again on every
    // reconnect, not just on cold start, so this produces a SECOND
    // slot_preflight_ok line — this time with confirmed_flush_lsn advanced
    // past restart_lsn.
    common::terminate_replication_backend(&client).await;

    const DRIVE_BUDGET: Duration = Duration::from_secs(20);
    let deadline = tokio::time::Instant::now() + DRIVE_BUDGET;
    let log_line = loop {
        let matches: Vec<String> = log_events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.starts_with("slot_preflight_ok") && e.contains(&format!("slot={slot}")))
            .cloned()
            .collect();
        // >= 2, not == 2: the busy race can make the server answer preflight
        // again before start() succeeds (our own prior session may not have
        // released the slot instantly yet) — every such retry re-runs the
        // guard and logs another line, all reporting the same (by then
        // stable — nothing else is writing) advanced position. Taking the
        // LAST one is correct no matter how many retries happened.
        if matches.len() >= 2 {
            break matches.last().unwrap().clone();
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "did not see a second slot_preflight_ok for {slot} after the reconnect \
                 within {DRIVE_BUDGET:?}; saw: {matches:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // Ground truth, read directly and independently of the log line.
    let info = preflight_slot(&conn, slot).await.unwrap();
    let actual_restart = format!("{:?}", info.restart_lsn.map(|l| l.to_string()));
    let actual_confirmed = format!("{:?}", info.confirmed_flush_lsn.map(|l| l.to_string()));
    assert_ne!(
        actual_restart, actual_confirmed,
        "test precondition: restart_lsn and confirmed_flush_lsn must differ on a slot that \
         has streamed and acknowledged data, or a label swap in the log line would be \
         invisible to this test"
    );

    assert_eq!(
        log_field(&log_line, "restart_lsn"),
        actual_restart,
        "the log line's restart_lsn does not match the guard's own restart_lsn — the two \
         labels may be swapped at the info! call site in stream_once: {log_line}"
    );
    assert_eq!(
        log_field(&log_line, "confirmed_flush_lsn"),
        actual_confirmed,
        "the log line's confirmed_flush_lsn does not match the guard's own \
         confirmed_flush_lsn — the two labels may be swapped at the info! call site in \
         stream_once: {log_line}"
    );

    // Clean shutdown, following the same pattern the reconnect tests in
    // tests/integration.rs use: force a fatal SlotMissing rather than
    // leaving the spawned run() an orphaned background task.
    common::terminate_replication_backend(&client).await;
    common::drop_slot_once_inactive(&client, slot).await;
    let result = tokio::time::timeout(Duration::from_secs(20), handle)
        .await
        .expect("run must fail on a missing slot, not retry forever")
        .expect("join");
    assert!(matches!(
        result.unwrap_err(),
        PgcdcError::SlotMissing { .. }
    ));
}
