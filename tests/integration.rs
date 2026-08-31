mod common;

use std::time::Duration;

use pgcdc::config::{Config, DatabaseUrl, OutputKind};
use pgcdc::error::PgcdcError;
use pgcdc::lsn::Lsn;
use pgcdc::sink::{Durability, Sink};
use pgcdc::transaction::Transaction;
use tokio::sync::mpsc;

/// Accumulates transactions into a channel so the test can wait for them.
/// The highest received position since the last barrier is stored
/// separately: a `write_transaction` return does not mean durable (that is
/// the whole point of Task 2).
struct ChannelSink(mpsc::UnboundedSender<Transaction>, Option<Lsn>);

#[async_trait::async_trait]
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

/// Always fails — checks that the acknowledgement never gets ahead of the sink.
struct FailingSink;

#[async_trait::async_trait]
impl Sink for FailingSink {
    fn durability(&self) -> Durability {
        Durability::Fsync
    }
    async fn write_transaction(&mut self, _tx: &Transaction) -> Result<(), PgcdcError> {
        Err(PgcdcError::Sink("deliberate test failure".into()))
    }
    async fn flush(&mut self) -> Result<Option<Lsn>, PgcdcError> {
        // The barrier never gets reached here: write_transaction always fails first.
        // An honest implementation also fails here, rather than silently claiming durable.
        Err(PgcdcError::Sink("deliberate test failure".into()))
    }
}

/// Accepts the write successfully, but the barrier fails every time there
/// is something to fail on. Exists separately from `FailingSink` because
/// that one fails inside `write_transaction` and never reaches the code
/// that marks durable — so it does not guard the "write went through" /
/// "barrier went through" split that task 2 was set up for in the first
/// place (review Task 2, round 1, F2).
///
/// Task 4 review, round 1, F1: `flush` used to fail UNCONDITIONALLY,
/// including on an empty tick with no writes at all. After task 4 the
/// barrier is reachable on idle ticks too (that is the whole point of the
/// timer), so a test double like that could abort `run()` with the expected
/// error before the first `write_transaction` even ran — the test would
/// pass without checking anything. Its shape matches the other sinks:
/// `write_transaction` records the received position, `flush` on an empty
/// accumulator honestly returns `Ok(None)` (the trait contract, and already
/// covered by a unit test for the other test doubles), and it only fails
/// when there was actually something to acknowledge.
struct FlushFailsSink(Option<Lsn>);

#[async_trait::async_trait]
impl Sink for FlushFailsSink {
    fn durability(&self) -> Durability {
        Durability::Fsync
    }
    async fn write_transaction(&mut self, tx: &Transaction) -> Result<(), PgcdcError> {
        self.0 = Some(tx.end_lsn);
        Ok(())
    }
    async fn flush(&mut self) -> Result<Option<Lsn>, PgcdcError> {
        if self.0.is_none() {
            return Ok(None);
        }
        // The position is deliberately not taken: the barrier failed, the
        // data did not become durable, and a retry must see the same
        // pending position, not silently lose it.
        Err(PgcdcError::Sink("deliberate barrier failure".into()))
    }
}

/// Mirror image of `FlushFailsSink`: the write fails while the (empty)
/// barrier succeeds. Needed separately from `FailingSink`, whose BOTH
/// methods fail — because of that, `FailingSink` cannot tell "a swallowed
/// write failure" apart from "a barrier failure": even a mutation that
/// ignores the `Err` from `write_transaction` would still bring `run()`
/// down on the very next barrier (which fails unconditionally), and the
/// sink-failure test would pass green for the wrong reason (I4).
///
/// `WriteFailsSink` holds no state at all (unlike `FlushFailsSink`, which
/// has an `Option<Lsn>` field) — the write always fails before there is
/// anything to remember, and the barrier has nothing to acknowledge, so it
/// honestly returns `Ok(None)`, as the trait contract requires for an empty
/// accumulator. If a mutation in the replication loop replaced
/// `sink.write_transaction(&tx).await?` with ignoring the result, the loop
/// would keep going as if nothing happened: `note_processed` would advance
/// to the position of a transaction the sink never actually accepted, and
/// the next barrier would honestly report `Ok(None)` — ack would never
/// advance, `run()` would never return an error, and the process would hang
/// exactly where correct code would fail with a fatal error right after the
/// very first write.
struct WriteFailsSink;

#[async_trait::async_trait]
impl Sink for WriteFailsSink {
    fn durability(&self) -> Durability {
        Durability::Fsync
    }
    async fn write_transaction(&mut self, _tx: &Transaction) -> Result<(), PgcdcError> {
        Err(PgcdcError::Sink("deliberate write failure".into()))
    }
    async fn flush(&mut self) -> Result<Option<Lsn>, PgcdcError> {
        // Nothing was ever accepted (the write always fails first) — the
        // barrier has nothing to acknowledge, and it honestly returns
        // Ok(None) rather than Err.
        Ok(None)
    }
}

fn config(conn: &str) -> Config {
    Config {
        database_url: DatabaseUrl::new(conn.to_string()),
        publication: "pgcdc_pub".into(),
        slot: "pgcdc_slot".into(),
        output: OutputKind::Stdout,
        output_path: None,
        max_transaction_events: 100_000,
        ack_interval_ms: 200,
        reconnect_initial_ms: 100,
        reconnect_max_ms: 30_000,
        slot_busy_budget_ms: 30_000,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn reconnect_bounds_above_the_ceiling_fail_before_any_connection() {
    // M6: `validate_reconnect_bounds()` was added to `run()` in a previous
    // round, but without a test that specifically hits the call inside
    // `run()` — the call itself could be deleted and the whole suite would
    // still stay green (the unit tests in `config.rs` only check the method
    // in isolation). The check sits BEFORE preflight and before any
    // connection, so no container is needed at all: the address below will
    // never be resolved or used if the call is in place.
    let mut cfg = config("postgres://u:p@127.0.0.1:1/db");
    cfg.reconnect_initial_ms = 5000;
    cfg.reconnect_max_ms = 1000;

    // If the call is removed, run() will go into preflight against an
    // unreachable address and keep retrying within the timeout below — the
    // timeout will expire and `expect` will panic: the mutation is caught,
    // not left unnoticed.
    let err = tokio::time::timeout(
        Duration::from_secs(5),
        pgcdc::postgres::replication::run(
            cfg,
            Box::new(FailingSink),
            std::sync::Arc::new(pgcdc::metrics::Metrics::new()),
        ),
    )
    .await
    .expect("the bounds check must return an error immediately, without waiting on the network")
    .unwrap_err();
    assert!(
        matches!(err, PgcdcError::InvalidReconnectBounds { .. }),
        "got {err:?}"
    );
    assert!(err.is_fatal());
}

#[tokio::test(flavor = "multi_thread")]
async fn sigterm_is_honored_while_stuck_reconnecting_to_a_dead_port() {
    // I1: previously a signal arriving while the DB was unreachable went
    // unnoticed entirely — neither did the outer reconnect loop read the
    // shutdown flag, nor was the backoff sleep interruptible. The reviewer
    // reproduced this live: the binary on a dead port, SIGTERM, still alive
    // five seconds later, and only SIGKILL changed anything. Port 1 never
    // listens on any ordinary machine, so preflight will fail immediately
    // and predictably — no Postgres container is needed here at all.
    let mut child = common::KillOnDrop(
        std::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"))
            .args([
                "--database-url",
                "postgres://u:p@127.0.0.1:1/db",
                "--publication",
                "pgcdc_pub",
                "--slot",
                "pgcdc_slot",
                // Short and close-together backoff bounds: several
                // reconnect attempts fit within seconds instead of the
                // default half-minute ceiling.
                "--reconnect-initial-ms",
                "50",
                "--reconnect-max-ms",
                "3000",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("spawn the binary"),
    );

    let stderr = child.stderr.take().expect("stderr was requested as piped");
    let lines: std::sync::Arc<std::sync::Mutex<Vec<String>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let lines_writer = lines.clone();
    std::thread::spawn(move || {
        use std::io::BufRead;
        let reader = std::io::BufReader::new(stderr);
        for line in reader.lines().map_while(Result::ok) {
            lines_writer.lock().unwrap().push(line);
        }
    });

    // Proof, not a guess: wait until the backoff climbs all the way to the
    // ceiling (50 → 100 → 200 → 400 → 800 → 1600 → 3000 — seven
    // "reconnecting" lines). The threshold used to be "at least two", but
    // with a ceiling equal to the poll interval (200ms), a pause of that
    // size always fits inside a single slice — a sliced pause and a whole
    // one are indistinguishable. The check below must catch the SIGNAL
    // exactly while a pause up to the ceiling (3000ms, several times
    // SHUTDOWN_POLL_INTERVAL) is in progress — only that way does the test
    // tell a sliced pause apart from a whole sleep(delay) (stage 5 round 1, F1).
    //
    // The budget is 20s (400×50ms), not the previous 10s (round 1 review,
    // F3): the mandatory part of the wait alone (the sum of pauses up to
    // the seventh retry) is already ~3.15s, and on a locally loaded machine
    // this test suite has been measured to vary by 2.4x (9.5-22.4s across
    // runs) — 10s left only a threefold margin, not the sixtyfold margin
    // the old "two" threshold had. 20s costs nothing on a green run and
    // removes the historical flakiness.
    let mut retries_seen = 0usize;
    for _ in 0..400 {
        retries_seen = lines
            .lock()
            .unwrap()
            .iter()
            .filter(|l| l.contains("reconnecting"))
            .count();
        if retries_seen >= 7 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        retries_seen >= 7,
        "did not see seven retries (backoff up to the ceiling) within 20 seconds, saw: {:?}",
        lines.lock().unwrap()
    );

    // SIGTERM in the middle of an endless retry against a dead port: there
    // is no buffered data on this path — the session was never opened even
    // once — so the only correct outcome is a fast exit with code 0, not
    // hanging until SIGKILL.
    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };

    // Polling via try_wait() rather than a blocking wait() inside
    // spawn_blocking: if this test catches an I1 regression and the process
    // does NOT react to SIGTERM, a blocking wait() would hang forever, and
    // tokio's runtime Drop waits specifically for blocking tasks to finish —
    // the test would hang the whole test binary instead of simply failing
    // red. try_wait() does not block the thread, so the timeout below fires
    // either way.
    //
    // The budget is 1.5s, not the previous 5s: the signal just caught a
    // pause of up to 3000ms in progress. A sliced pause notices the flag no
    // later than SHUTDOWN_POLL_INTERVAL (200ms) — 1.5s leaves it a generous
    // margin; whereas a whole sleep(delay) must keep the process alive for
    // almost all of the remaining ~3s and would not fit in this budget —
    // otherwise there is no point tightening it, and the test again could
    // not tell a sliced pause apart from a whole sleep.
    let mut status = None;
    for _ in 0..30 {
        if let Ok(Some(s)) = child.try_wait() {
            status = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let status =
        status.expect("SIGTERM must stop the process within 1.5 seconds, not only SIGKILL");
    assert_eq!(
        status.code(),
        Some(0),
        "there was nothing left to bring to the barrier — a reconnect against an unreachable DB must exit with zero"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sigterm_in_the_last_backoff_chunk_needs_the_top_of_loop_check() {
    // Round 1 review, F1/F2: backoff slicing checks the flag BEFORE each
    // chunk and never AFTER the last one — a signal that lands in exactly
    // the last chunk does not reach that check and is only caught by the
    // check at the top of the NEXT pass of the outer loop. This test's
    // neighbor (the dead port) does not exercise this check: against a
    // refused port, the "one extra connection attempt" that this check
    // saves costs less than a millisecond — indistinguishable from zero
    // against any reasonable budget.
    //
    // Here, instead of a dead port, there is a peer that ACCEPTS the TCP
    // connection and silently holds it for ~3s before dropping it:
    // preflight fails neither instantly nor never, but after a bounded yet
    // real delay. Without the check at the top of the pass, this delay
    // shows up IN FULL — exactly the duration that the new
    // `spawn_shutdown_listener` documentation calls unbounded.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake peer");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => break,
            };
            tokio::spawn(async move {
                // Never speaks the Postgres protocol: just holds the
                // socket so the client's connect() blocks for a
                // predictable ~3s, instead of failing instantly or hanging
                // forever.
                tokio::time::sleep(Duration::from_secs(3)).await;
                drop(stream);
            });
        }
    });

    let mut child = common::KillOnDrop(
        std::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"))
            .args([
                "--database-url",
                &format!("postgres://u:p@{addr}/db"),
                "--publication",
                "pgcdc_pub",
                "--slot",
                "pgcdc_slot",
                // The initial and maximum bounds are equal: every backoff
                // pause is exactly 1s (5 chunks of 200ms), with no growth,
                // so the moment the signal lands within the pause is
                // predictable.
                "--reconnect-initial-ms",
                "1000",
                "--reconnect-max-ms",
                "1000",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("spawn the binary"),
    );

    let stderr = child.stderr.take().expect("stderr was requested as piped");
    let lines: std::sync::Arc<std::sync::Mutex<Vec<String>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let lines_writer = lines.clone();
    std::thread::spawn(move || {
        use std::io::BufRead;
        let reader = std::io::BufReader::new(stderr);
        for line in reader.lines().map_while(Result::ok) {
            lines_writer.lock().unwrap().push(line);
        }
    });

    // Wait for the second "reconnecting" line: the first attempt and the
    // first pause are already behind us, the second attempt has failed,
    // and the second pause (exactly 1s) is now in progress. The budget is
    // 15s: two connections to the holding peer (~3s each) plus the pause
    // between them (~1s) plus margin.
    let mut retries_seen = 0usize;
    for _ in 0..300 {
        retries_seen = lines
            .lock()
            .unwrap()
            .iter()
            .filter(|l| l.contains("reconnecting"))
            .count();
        if retries_seen >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        retries_seen >= 2,
        "did not see the second reconnecting line within 15 seconds, saw: {:?}",
        lines.lock().unwrap()
    );

    // The pause after the second retry is exactly 1s, sliced into 5 chunks
    // of 200ms: chunk boundaries are 0, 200, 400, 600, 800, 1000. The last
    // chunk (800-1000) is the very one that the slicing does not re-check
    // after its own end; the signal must land exactly there.
    //
    // Round 2 review, F5: our "now" is always LATER than the true moment
    // of the log line — that's what polling every 50ms would give. So the
    // offset can only INCREASE the actual point where we land inside the
    // pause, never decrease it. Hence: aim closer to the start of the
    // window (800ms), not its end (900ms, as it used to be) — this costs
    // nothing on the green side (the sliced code doesn't care exactly how
    // much of the last chunk is left — 200ms or 20ms, as long as it's
    // after the end of the last chunk, not before) and buys margin on the
    // red side (overshooting past 1000ms would mean landing in the NEXT
    // connection attempt, which blocks for the same ~3s on both correct
    // and mutated code — a false-red result even without a mutation).
    //
    // The target is 820ms: +20ms above the lower bound (800) as insurance
    // in case the assumption about chunk boundaries is even slightly off;
    // 180ms remain before the upper bound (1000) for polling delay and
    // task dispatch — it used to be 100ms with a target of 900ms. The exit
    // budget below is widened along with this (see the comment there).
    let signal_target = Duration::from_millis(820);
    tokio::time::sleep(signal_target).await;
    let signal_sent_at = std::time::Instant::now();
    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };

    // The budget is 1s (used to be 600ms): with an 820ms target, the worst
    // case for the sliced code is landing right on the 820ms boundary
    // (zero polling delay) and sitting out the rest of the last chunk,
    // ~180ms, plus check reaction and signal delivery — with margin up to
    // ~250-300ms. 1s gives this a several-fold margin and stays a third
    // below the holding peer's delay (~3s), so the mutated path definitely
    // does not fit.
    let mut status = None;
    for _ in 0..20 {
        if let Ok(Some(s)) = child.try_wait() {
            status = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let elapsed = signal_sent_at.elapsed();
    eprintln!(
        "sigterm_in_the_last_backoff_chunk_needs_the_top_of_loop_check: exit latency = {elapsed:?}"
    );
    let status = status
        .expect("the top-of-pass check must catch a signal from the last pause chunk within 1s");
    assert_eq!(
        status.code(),
        Some(0),
        "there was nothing to bring to the barrier"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_travels_end_to_end_and_arrives_as_one_event() {
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let (tx_send, mut tx_recv) = mpsc::unbounded_channel();
    let cfg = config(&conn);
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(
            cfg,
            Box::new(ChannelSink(tx_send, None)),
            std::sync::Arc::new(pgcdc::metrics::Metrics::new()),
        )
        .await
    });

    client
        .execute(
            "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', NULL)",
            &[],
        )
        .await
        .unwrap();

    let tx = tokio::time::timeout(Duration::from_secs(20), tx_recv.recv())
        .await
        .expect("the transaction should arrive within 20 seconds")
        .expect("channel closed");

    assert_eq!(tx.changes.len(), 1);
    let ev = &tx.changes[0];
    assert_eq!(ev.schema, "public");
    assert_eq!(ev.table, "users");
    let json = serde_json::to_value(ev).unwrap();
    assert_eq!(json["operation"], "insert");
    assert_eq!(json["after"]["id"], "1");
    assert_eq!(json["after"]["name"], "Alice");
    assert!(json["after"]["bio"].is_null());
    assert!(json["before"].is_null());
    assert_eq!(json["unchanged_columns"], serde_json::json!([]));

    // The core of the LSN contract: PostgreSQL must acknowledge NOT EARLIER
    // than end_lsn, not commit_lsn (they differ by a fixed 0x30 bytes —
    // DECISIONS/Task 6 brief). We wait for "not earlier", not an exact
    // match: idle-keepalive-advance (stage 3) can move confirmed_flush_lsn
    // past end_lsn even before our poll — under load this was caught in
    // about 20% of runs (review Task 2, round 1, F5), and it's not that
    // the wrong thing got acknowledged, but that the server kept doing its
    // own WAL activity in the background (a recorded example — 0x38 bytes
    // from one background standby-snapshot record).
    //
    // The trade-off: this test no longer distinguishes "acknowledged
    // exactly end_lsn" from "acknowledged something PAST it via keepalive"
    // — under a mutation that sends commit_lsn instead of end_lsn in the
    // feedback, the keepalive branch will later drag the slot above
    // end_lsn anyway, and the `>=` check will not notice. Only watching OUR
    // OWN acknowledgement (acked) rather than the slot's position on the
    // server can distinguish these two cases — that is, this is lost here
    // deliberately, not by oversight. The discrimination this test gave up
    // is covered by
    // `we_acknowledge_the_end_of_the_commit_record_not_its_start` in this
    // same file: it reads `metrics.last_acknowledged_lsn`, not the slot's
    // position, and so distinguishes substituting commit_lsn for end_lsn in
    // a case where keepalive would drag the slot ahead of both anyway.
    let expected_end = tx.end_lsn;
    assert_ne!(
        tx.end_lsn, tx.commit_lsn,
        "end_lsn and commit_lsn must differ, otherwise the check below proves nothing"
    );

    let confirmed = common::wait_for_slot_at_least(&client, "pgcdc_slot", expected_end).await;
    assert!(
        confirmed >= expected_end,
        "PostgreSQL should have acknowledged at least the transaction's end_lsn: {confirmed} < {expected_end}"
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_does_not_send_rolled_back_transactions() {
    // Checks OUR understanding of the protocol, not our code: logical
    // decoding physically never hands out rolled-back transactions. If
    // this test fails, it means the world doesn't work the way we think.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let (tx_send, mut tx_recv) = mpsc::unbounded_channel();
    let cfg = config(&conn);
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(
            cfg,
            Box::new(ChannelSink(tx_send, None)),
            std::sync::Arc::new(pgcdc::metrics::Metrics::new()),
        )
        .await
    });

    client
        .batch_execute("BEGIN; INSERT INTO users VALUES (99, 'Ghost', NULL, NULL); ROLLBACK;")
        .await
        .unwrap();
    client
        .execute("INSERT INTO users VALUES (1, 'Alice', NULL, NULL)", &[])
        .await
        .unwrap();

    let tx = tokio::time::timeout(Duration::from_secs(20), tx_recv.recv())
        .await
        .expect("timeout")
        .expect("channel closed");

    // The first transaction to arrive is the one after the rollback.
    assert_eq!(tx.changes.len(), 1);
    let json = serde_json::to_value(&tx.changes[0]).unwrap();
    assert_eq!(
        json["after"]["id"], "1",
        "the rolled-back row must not arrive"
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn sink_failure_stops_us_before_the_slot_advances() {
    // The core of the contract: acknowledgement never gets ahead of what
    // the sink wrote.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let before: String = client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 'pgcdc_slot'",
            &[],
        )
        .await
        .unwrap()
        .get(0);

    let cfg = config(&conn);
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(
            cfg,
            Box::new(FailingSink),
            std::sync::Arc::new(pgcdc::metrics::Metrics::new()),
        )
        .await
    });

    client
        .execute("INSERT INTO users VALUES (1, 'Alice', NULL, NULL)", &[])
        .await
        .unwrap();

    let result = tokio::time::timeout(Duration::from_secs(20), handle)
        .await
        .expect("run should finish, not hang")
        .expect("join");
    let err = result.unwrap_err();
    assert!(matches!(err, PgcdcError::Sink(_)), "got {err:?}");
    assert!(
        err.is_fatal(),
        "a sink that cannot proceed is a fatal error"
    );

    let after: String = client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 'pgcdc_slot'",
            &[],
        )
        .await
        .unwrap()
        .get(0);

    assert_eq!(
        before, after,
        "the slot must not have moved: the sink wrote nothing"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn barrier_failure_stops_us_before_the_slot_advances() {
    // Complements sink_failure_stops_us_before_the_slot_advances: that one
    // checks a failure INSIDE write_transaction, this one checks a barrier
    // failure AFTER a successful write. Without this test, the code path
    // task 2 added (durable is marked only by flush's return, not
    // write_transaction's) is completely unguarded: FailingSink never
    // reaches it (review Task 2, round 1, F2).
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let before: String = client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 'pgcdc_slot'",
            &[],
        )
        .await
        .unwrap()
        .get(0);

    let cfg = config(&conn);
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(
            cfg,
            Box::new(FlushFailsSink(None)),
            std::sync::Arc::new(pgcdc::metrics::Metrics::new()),
        )
        .await
    });

    client
        .execute("INSERT INTO users VALUES (1, 'Alice', NULL, NULL)", &[])
        .await
        .unwrap();

    let result = tokio::time::timeout(Duration::from_secs(20), handle)
        .await
        .expect("run should finish, not hang")
        .expect("join");
    let err = result.unwrap_err();
    assert!(matches!(err, PgcdcError::Sink(_)), "got {err:?}");
    assert!(
        err.is_fatal(),
        "a barrier that cannot acknowledge is a fatal error"
    );

    let after: String = client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 'pgcdc_slot'",
            &[],
        )
        .await
        .unwrap()
        .get(0);

    assert_eq!(
        before, after,
        "the slot must not have moved: the barrier never brought the write to disk"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_failure_stops_us_before_the_slot_advances_and_is_not_swallowed() {
    // I4: complements sink_failure_stops_us_before_the_slot_advances and
    // barrier_failure_stops_us_before_the_slot_advances with a test double
    // where ONLY the write fails while the (empty) barrier succeeds —
    // WriteFailsSink, the mirror of FlushFailsSink. FailingSink does not
    // work here: BOTH its methods fail, so a mutation "sink.write_transaction(&tx).await? →
    // ignore the result" would still bring run() down on the very next
    // barrier, and sink_failure_stops_us_before_the_slot_advances would
    // pass green for the wrong reason — the suite was blind to a mutation
    // of the very thing it claimed to cover.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let before: String = client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 'pgcdc_slot'",
            &[],
        )
        .await
        .unwrap()
        .get(0);

    let cfg = config(&conn);
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(
            cfg,
            Box::new(WriteFailsSink),
            std::sync::Arc::new(pgcdc::metrics::Metrics::new()),
        )
        .await
    });

    client
        .execute("INSERT INTO users VALUES (1, 'Alice', NULL, NULL)", &[])
        .await
        .unwrap();

    let result = tokio::time::timeout(Duration::from_secs(20), handle)
        .await
        .expect(
            "run should finish with a write failure, not hang — under a \
             mutation that swallows the Err from write_transaction, it hangs \
             forever: the loop keeps reading WAL, but ack never advances \
             because the sink never accepted anything",
        )
        .expect("join");
    let err = result.unwrap_err();
    assert!(matches!(err, PgcdcError::Sink(_)), "got {err:?}");
    assert!(err.is_fatal(), "a sink that cannot write is a fatal error");

    let after: String = client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 'pgcdc_slot'",
            &[],
        )
        .await
        .unwrap()
        .get(0);

    assert_eq!(
        before, after,
        "the slot must not have moved: the sink never accepted a write"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn we_acknowledge_the_end_of_the_commit_record_not_its_start() {
    // Carried over from stage 3. This used to be checked against the
    // slot's position for exact equality, but the slot advancing via
    // keepalive made equality unreachable: background WAL activity
    // legitimately moves the slot further, and the weakened "not less
    // than" check stopped distinguishing a substitution of end_lsn for
    // commit_lsn — keepalive would drag the slot past end_lsn in both
    // cases. The counter reads OUR decision, not the server's state, and
    // so it does distinguish.
    //
    // M5 (review round after task 4): this is a pin test for the dam
    // between acknowledge_durable (src/postgres/replication.rs) and
    // Transaction::end_lsn (src/transaction.rs), going through
    // ChannelSink — the test double sink declared in this same file, which
    // stores the needed position itself (`self.1 = Some(tx.end_lsn)`). It
    // does NOT catch the mutation "the production sinks (FileSink/StdoutSink)
    // report the start instead of the end" — ChannelSink simply doesn't
    // touch that path. That mutation is caught by
    // `flush_reports_the_last_accepted_position_then_clears_it`
    // (src/sink/file.rs) and `a_second_flush_right_after_the_first_reports_nothing_new`
    // (src/sink/stdout.rs) — both use a fixture with end_lsn ≠ commit_lsn
    // and check `flush()`'s return for exact equality. Verified by hand
    // with a mutation: substituting `tx.end_lsn` for `tx.commit_lsn` in
    // both production sinks fails exactly these two unit tests and leaves
    // this test green.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let metrics = std::sync::Arc::new(pgcdc::metrics::Metrics::new());
    let (tx_send, mut tx_recv) = mpsc::unbounded_channel();
    let cfg = config(&conn);
    let m = metrics.clone();
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(cfg, Box::new(ChannelSink(tx_send, None)), m).await
    });

    client
        .execute("INSERT INTO users VALUES (1, 'Alice', NULL, NULL)", &[])
        .await
        .unwrap();

    let tx = tokio::time::timeout(Duration::from_secs(20), tx_recv.recv())
        .await
        .expect("the transaction should arrive")
        .expect("channel closed");

    // Wait for the counter to catch up: acknowledgement leaves the barrier on a timer.
    let mut acked = 0;
    for _ in 0..200 {
        acked = metrics.snapshot().last_acknowledged_lsn;
        if acked >= tx.end_lsn.0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(
        acked, tx.end_lsn.0,
        "we acknowledge the transaction's end_lsn, not something else"
    );
    assert_ne!(
        acked, tx.commit_lsn.0,
        "commit_lsn points to the start of the commit record — a restart would reread it"
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_servers_confirmed_position_never_races_ahead_of_what_we_acknowledged() {
    // I3: DECISIONS Q25(2) forbids five calls to `pg_walstream` leading into
    // `recover_connection`, precisely because they restart the stream from
    // the RECEIVED position rather than the DURABLE one. The test above
    // (we_acknowledge_the_end_of_the_commit_record_not_its_start) reads OUR
    // decision through `metrics.last_acknowledged_lsn`, not what actually
    // went out on the wire — the "decision → wire" step stays blind: swap
    // `acked` for `received`/`processed` inside `acknowledge_durable` (in
    // the calls to `stream.shared_lsn_feedback.update_flushed_lsn`/
    // `update_applied_lsn`), and the whole suite stays green, because
    // `metrics.set_last_acknowledged_lsn` in that same function never sees
    // that substitution at all.
    //
    // The scenario (a reviewer's probe, turned into a test).
    // `acknowledge_durable` is only called when the barrier has something
    // to acknowledge (`sink.flush()` returned `Some`) — a single small
    // transaction would be a dud: the very first barrier tick would
    // acknowledge it almost immediately, long before the next, large
    // transaction even starts streaming, and a substitution inside
    // `acknowledge_durable` would never get to see anything but a tiny
    // `received`. So a background task keeps writing separate small
    // transactions throughout the test — this produces MANY separate calls
    // to `acknowledge_durable`, and one of them is guaranteed to land at a
    // moment when the large transaction B is already STREAMING (received
    // grows frame by frame) but not yet fully parsed (its COMMIT has not
    // arrived yet, write_transaction has not been called for it yet). At
    // that moment the barrier only acknowledges the small background
    // transaction — the sink has not accepted anything beyond it. If
    // received/processed went out on the wire instead of acked, the server
    // would see `confirmed_flush_lsn` somewhere in the middle of B — far
    // ahead of what actually reached the sink.
    //
    // B is one INSERT...SELECT, one commit: 300 rows of 200KB each in the
    // TOAST column `bio`, STORAGE EXTERNAL, uncompressed (≈60MB). What
    // matters is not B's total volume but the gap between two barriers
    // that B manages to create while it streams — and that's calibrated by
    // the time it takes to parse one row, not the sum of all rows. The
    // previous version of the test ran 3000 rows (≈600MB): five times more
    // expensive in time and memory, without buying a bigger gap (review
    // round 2 after task 4 finale, P2) — at the same threshold
    // (`max_gap_after_some_ack >= 1_000_000` below), the observed gap
    // stays in the tens of megabytes, with margin to spare by orders of
    // magnitude.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let (tx_send, mut tx_recv) = mpsc::unbounded_channel();
    let mut cfg = config(&conn);
    cfg.ack_interval_ms = 150;
    let metrics = std::sync::Arc::new(pgcdc::metrics::Metrics::new());
    let m = metrics.clone();
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(cfg, Box::new(ChannelSink(tx_send, None)), m).await
    });

    // Background task: continuously writes separate small transactions
    // (unique ids starting at 500,000, outside B's range) until the test
    // tells it to stop. Each one keeps the barrier busy with something
    // small throughout the test — including the moment when B is midway.
    let bg_stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let bg_stop_writer = bg_stop.clone();
    let bg_conn = conn.clone();
    let bg_task = tokio::spawn(async move {
        let bg_client = common::connect(&bg_conn).await;
        let mut id = 500_000i64;
        while !bg_stop_writer.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = bg_client
                .execute("INSERT INTO users VALUES ($1, 'bg', NULL, NULL)", &[&id])
                .await;
            id += 1;
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
    });

    // Wait for the first background transaction — proof the mechanism
    // works at all, before launching B.
    let first_bg = tokio::time::timeout(Duration::from_secs(20), tx_recv.recv())
        .await
        .expect("the first background transaction should arrive")
        .expect("channel closed");
    assert_eq!(
        first_bg.changes.len(),
        1,
        "a background transaction is one row"
    );

    // The large transaction B — with no artificial delay.
    let client_b = common::connect(&conn).await;
    let insert_b = tokio::spawn(async move {
        client_b
            .execute(
                "INSERT INTO users SELECT gs, 'x', NULL, repeat('y', 200000) \
                 FROM generate_series(1000, 1299) AS gs",
                &[],
            )
            .await
            .expect("insert large transaction B");
    });

    // Poll the SERVER'S confirmed_flush_lsn against OUR acknowledged
    // position while B is in flight, until B arrives whole over the
    // channel (or the safety timeout expires). Transactions shorter than
    // 300 rows (all background ones) fly past this loop — only B matters.
    // The threshold is invariant 1 (DECISIONS §1): `acked_lsn <=
    // durable_lsn`, and what actually reached the server must match what
    // we ourselves decided to acknowledge.
    let probe_client = common::connect(&conn).await;
    let mut max_gap_after_some_ack: i64 = -1;
    let tx_b = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            tokio::select! {
                recv = tx_recv.recv() => {
                    let tx = recv.expect("channel closed");
                    if tx.changes.len() >= 300 {
                        return tx;
                    }
                    // A background transaction — not what we're waiting for, keep polling.
                }
                _ = tokio::time::sleep(Duration::from_millis(15)) => {
                    let row = probe_client
                        .query_one(
                            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots \
                             WHERE slot_name = 'pgcdc_slot'",
                            &[],
                        )
                        .await
                        .unwrap();
                    let text: Option<String> = row.get(0);
                    let Some(server_confirmed) = text.as_deref().and_then(common::parse_lsn) else {
                        continue;
                    };
                    let ours = Lsn(metrics.snapshot().last_acknowledged_lsn);
                    // Only check AFTER at least one acknowledgement from
                    // OUR process (ours > 0): a freshly created slot's
                    // confirmed_flush_lsn already sits at the creation
                    // position (not at zero), and before the first call to
                    // acknowledge_durable, comparing against ours=0 proves
                    // nothing about the call itself.
                    if ours.0 == 0 {
                        continue;
                    }
                    assert!(
                        server_confirmed <= ours,
                        "the server acknowledged {server_confirmed}, but we actually \
                         acknowledged only {ours} — the position that went out on the wire \
                         got ahead of what we ourselves decided to acknowledge"
                    );
                    let received = metrics.snapshot().last_received_lsn as i64;
                    let gap = received - ours.0 as i64;
                    if gap > max_gap_after_some_ack {
                        max_gap_after_some_ack = gap;
                    }
                }
            }
        }
    })
    .await
    .expect("transaction B should arrive within 30 seconds");

    bg_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    insert_b
        .await
        .expect("inserting B should not have panicked");
    bg_task
        .await
        .expect("the background task should not have panicked");

    assert!(
        max_gap_after_some_ack >= 1_000_000,
        "the test must have caught received noticeably ahead of what was already acknowledged \
         (observed {max_gap_after_some_ack} bytes) — otherwise the large transaction transferred \
         faster than the background inserts created new acknowledgements, and the race window \
         was not exercised; increase B's size, make background inserts more frequent, or \
         decrease ack_interval_ms"
    );

    // Final check: the slot eventually catches up with B in full, and does
    // not disagree with what we actually acknowledged.
    let confirmed = common::wait_for_slot_at_least(&client, "pgcdc_slot", tx_b.end_lsn).await;
    assert!(
        confirmed >= tx_b.end_lsn,
        "the slot must catch up with B: {confirmed} < {}",
        tx_b.end_lsn
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failing_barrier_leaves_the_acknowledged_counter_at_zero() {
    // Q23's wording verbatim: "after a sink failure, last_acknowledged_lsn
    // has not moved". Previously this could only be checked via the slot;
    // now our own decision is visible too.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let metrics = std::sync::Arc::new(pgcdc::metrics::Metrics::new());
    let cfg = config(&conn);
    let m = metrics.clone();
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(cfg, Box::new(FlushFailsSink(None)), m).await
    });

    client
        .execute("INSERT INTO users VALUES (1, 'Alice', NULL, NULL)", &[])
        .await
        .unwrap();

    let result = tokio::time::timeout(Duration::from_secs(20), handle)
        .await
        .expect("run should finish, not hang")
        .expect("join");
    assert!(matches!(result.unwrap_err(), PgcdcError::Sink(_)));

    let snap = metrics.snapshot();
    assert_eq!(
        snap.last_acknowledged_lsn, 0,
        "the barrier did not go through — there is nothing to acknowledge"
    );
    assert!(
        snap.transactions_total >= 1,
        "but the transaction was accepted and counted"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stdout_stays_json_only_when_the_real_binary_hits_a_fatal_error() {
    // I2: "JSONL on stdout, logs on stderr" is behaviorally correct, but
    // nothing would have failed on a regression. `--help` doesn't work for
    // this: it doesn't go through any branch that logs. A missing slot is a
    // deterministic and fast way to guaranteedly hit the logging branch:
    // the guard fails before the first replication event, with no need to
    // wait for an INSERT and no timing races.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    // The slot is intentionally not created.

    // tokio::process::Command, not std + spawn_blocking: with std, if the
    // timeout below fires, only waiting on the join handle gets cancelled —
    // the blocking thread inside cmd.output() and the orphaned child
    // process itself keep living. Now that the process retries reconnecting
    // forever, such an orphan never finishes on its own and hangs the
    // whole test binary on runtime shutdown (review Task 2, round 1, F9).
    // kill_on_drop(true) gives us a handle that kills the process if the
    // future is cancelled by the timeout: an async Child, unlike std, gets
    // dropped along with the future.
    let mut cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"));
    cmd.env("PGCDC_DATABASE_URL", &conn)
        .env("PGCDC_PUBLICATION", "pgcdc_pub")
        .env("PGCDC_SLOT", "pgcdc_slot")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let child = cmd.spawn().expect("spawn pgcdc");

    let output = tokio::time::timeout(Duration::from_secs(20), child.wait_with_output())
        .await
        .expect("the binary should finish within 20 seconds")
        .expect("wait for pgcdc to finish");

    assert!(
        !output.status.success(),
        "a missing slot must be fatal for the real binary"
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout must be valid UTF-8");
    assert!(
        stdout.is_empty(),
        "stdout must stay empty on a fatal startup error, got: {stdout:?}"
    );
    // There will be no lines here at this point, but this is a
    // future-proofing assertion: if non-journal text ever leaks into
    // stdout, it will not parse as JSON and the test will fail red.
    for line in stdout.lines() {
        serde_json::from_str::<serde_json::Value>(line)
            .unwrap_or_else(|e| panic!("stdout line is not JSON: {line:?}: {e}"));
    }

    let stderr = String::from_utf8(output.stderr).expect("stderr must be valid UTF-8");
    assert!(
        // Error text lives in src/error.rs and is English-only, so a
        // Russian fallback here would never match — a dead branch, removed.
        stderr.contains("slot"),
        "stderr should report the missing slot, got: {stderr}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn libpq_connection_string_is_rejected_without_echoing_the_password() {
    // We learned to reject such a string in stage 1, but clap used to
    // print it in full in its error text. This specifically checks the
    // absence of that echo.
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"))
        .args([
            "--database-url",
            "host=127.0.0.1 port=5432 user=postgres password=SUPERSECRET_XYZZY dbname=app",
            "--publication",
            "pgcdc_pub",
            "--slot",
            "pgcdc_slot",
        ])
        .output()
        .expect("spawn the binary");

    assert!(
        !output.status.success(),
        "an invalid URL must produce a nonzero exit code"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stdout.is_empty(), "stdout carries only the payload");
    assert!(
        !stderr.contains("SUPERSECRET_XYZZY"),
        "the password must not appear in stderr: {stderr}"
    );
    assert!(
        stderr.contains("postgres://"),
        "the message should hint at the expected form: {stderr}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn file_output_without_a_path_is_rejected_by_the_binary() {
    // Checks the behavior of the whole binary: clap parses the
    // configuration, and main decides whether the path is required.
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"))
        .args([
            "--database-url",
            "postgres://u:p@127.0.0.1:1/db",
            "--publication",
            "p",
            "--slot",
            "s",
            "--output",
            "file",
        ])
        .output()
        .expect("spawn the binary");
    assert!(!output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.is_empty(), "stdout carries only the payload");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--output-path"),
        "the message names the missing flag: {stderr}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_slot_is_fatal_and_the_slot_is_not_created() {
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    // The slot is intentionally NOT created.

    // The slot is currently classified as immediately fatal, but run() can
    // now retry forever on recoverable errors — without the timeout, this
    // await would hang the test forever if the classification ever broke
    // (review Task 2, round 1, F9).
    let err = tokio::time::timeout(
        Duration::from_secs(20),
        pgcdc::postgres::replication::run(
            config(&conn),
            Box::new(FailingSink),
            std::sync::Arc::new(pgcdc::metrics::Metrics::new()),
        ),
    )
    .await
    .expect("run should finish, not hang")
    .unwrap_err();
    assert!(matches!(err, PgcdcError::SlotMissing { .. }));
    assert!(err.is_fatal());

    let rows = client
        .query("SELECT slot_name FROM pg_replication_slots", &[])
        .await
        .unwrap();
    assert!(
        rows.is_empty(),
        "the slot must not be created — otherwise we would be masking data loss"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn slot_with_a_foreign_output_plugin_is_fatal_and_the_process_exits() {
    // C1 (review round after task 4): §20 item 14 requires a nonzero exit
    // code when the slot is either missing OR unusable. "Missing" is
    // covered by missing_slot_is_fatal_and_the_slot_is_not_created above;
    // "unusable" wasn't covered at all — a slot where START_REPLICATION
    // gets an explicit server refusal fell into PgcdcError::Connection and
    // went into an endless reconnect with no nonzero exit code whatsoever.
    //
    // A cheap unusable branch: the slot is created with a foreign output
    // plugin (`test_decoding` instead of `pgoutput`). The slot's existence
    // passes preflight (it doesn't look at the plugin), and
    // START_REPLICATION replies "option \"proto_version\" = \"1\" is
    // unknown" (SQLSTATE 22023) — the same error envelope (the server
    // RESPONDED and refused) as a genuine invalidation from the amount of
    // retained WAL, but without needing to push gigabytes of WAL through
    // to provoke it.
    //
    // We run the real compiled binary, not run() in-process: checklist item
    // 14 is about the PROCESS exit code, not about the library's Result.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    client
        .query(
            "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
            &[&"pgcdc_slot"],
        )
        .await
        .expect("create a slot with a foreign output plugin");

    let mut cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"));
    cmd.env("PGCDC_DATABASE_URL", &conn)
        .env("PGCDC_PUBLICATION", "pgcdc_pub")
        .env("PGCDC_SLOT", "pgcdc_slot")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        // async Child, not std + spawn_blocking: if the timeout below
        // fires, kill_on_drop(true) will actually kill the process instead
        // of leaving it as an endlessly reconnecting orphan (the same
        // technique as in stdout_stays_json_only_when_the_real_binary_hits_a_fatal_error).
        .kill_on_drop(true);
    let child = cmd.spawn().expect("spawn pgcdc");

    let output = tokio::time::timeout(Duration::from_secs(20), child.wait_with_output())
        .await
        .expect("the process must finish within 20 seconds, not go into an endless reconnect")
        .expect("wait for pgcdc to finish");

    assert!(
        !output.status.success(),
        "an unusable (foreign plugin) slot must be fatal for the real binary"
    );
    assert_eq!(
        output.status.code(),
        Some(1),
        "a fatal error must produce exit code 1 (DECISIONS Q22)"
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout must be valid UTF-8");
    assert!(
        stdout.is_empty(),
        "stdout must stay empty on a fatal startup error, got: {stdout:?}"
    );

    let stderr = String::from_utf8(output.stderr).expect("stderr must be valid UTF-8");
    assert!(
        stderr.contains("slot_unusable"),
        "stderr must name the reason via the machine-readable error_kind label, got: {stderr}"
    );
    assert!(
        !stderr.contains("reconnecting"),
        "a fatal server refusal must stop the process, not send it into a reconnect loop: {stderr}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn slot_busy_with_our_own_prior_session_is_recoverable_not_fatal() {
    // The flip side of C1: the server ALSO responds with a refusal on
    // START_REPLICATION ("replication slot ... is active for PID ...",
    // ERRCODE_OBJECT_IN_USE), but this isn't about the slot being unusable
    // — a concurrent reader is still holding it. A naive "any server
    // refusal is fatal" would break ordinary reconnecting: after a drop,
    // our own previous session might momentarily fail to release the slot
    // before a new session tries to grab it.
    //
    // Here the race is made deterministic: a separate pg_walstream holds
    // the slot busy BEFORE pgcdc even starts — pgcdc's first attempt is
    // guaranteed to get "is active for PID" and nothing else. If the
    // classification ever becomes "any server refusal is fatal", this test
    // fails red deterministically: run() will return SlotUnusable on the
    // very first attempt, the channel will close, and recv() below will
    // get None instead of a transaction.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let blocker_config = pg_walstream::ReplicationStreamConfig::new(
        "pgcdc_slot".to_string(),
        "pgcdc_pub".to_string(),
        1,
        pg_walstream::StreamingMode::Off,
        Duration::from_secs(10),
        Duration::from_secs(30),
        Duration::from_secs(60),
        pg_walstream::RetryConfig::default(),
    )
    .with_binary(false)
    .with_messages(false);
    let blocker_url = format!("{conn}?replication=database");
    let mut blocker = pg_walstream::LogicalReplicationStream::new(&blocker_url, blocker_config)
        .await
        .expect("open the blocking connection");
    blocker
        .start(None)
        .await
        .expect("the blocking connection must grab the slot first");

    let cfg = config(&conn);
    let (tx_send, mut tx_recv) = mpsc::unbounded_channel();
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(
            cfg,
            Box::new(ChannelSink(tx_send, None)),
            std::sync::Arc::new(pgcdc::metrics::Metrics::new()),
        )
        .await
    });

    // Let pgcdc actually run into the busy slot at least once (100ms —
    // the default reconnect_initial_ms from config()) before releasing it.
    // Dropping the blocking stream on a multi-thread runtime synchronously
    // sends CopyDone+Terminate (pg_walstream::connection::native,
    // close_connection), so the slot is released before drop() returns
    // control.
    tokio::time::sleep(Duration::from_millis(500)).await;
    drop(blocker);

    client
        .execute("INSERT INTO users VALUES (1, 'Alice', NULL, NULL)", &[])
        .await
        .unwrap();

    let tx = tokio::time::timeout(Duration::from_secs(20), tx_recv.recv())
        .await
        .expect("pgcdc must carry the reconnect through to completion, not get stuck or fail")
        .expect("channel closed — run() finished with an error instead of retrying");
    assert_eq!(tx.changes.len(), 1);

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn slot_busy_forever_exhausts_the_patience_budget_and_the_process_exits_nonzero() {
    // The flip side of the race above (slot_busy_with_our_own_prior_session_is_recoverable_not_fatal):
    // a slot held busy by a FOREIGN consumer FOREVER responds with literally
    // the same SQLSTATE 55006 — by status code alone the two cases are
    // indistinguishable (see the detailed breakdown at
    // classify_start_error/SlotBusyPatience in
    // src/postgres/replication.rs). The only physical discriminator is
    // DURATION: our own prior session releases the slot within tens of
    // milliseconds (measured), a foreign consumer does not. Here a blocking
    // connection holds the slot and does NOT release it for the whole
    // test; with a small patience budget the process must exhaust it and
    // exit with a nonzero code instead of going into an endless reconnect
    // — exactly this was reproduced by hand (34 cycles, not a single
    // nonzero exit code) in the task 4 report.
    //
    // We run the real compiled binary: checklist item 14 is about the
    // PROCESS exit code, not about the library's Result.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let blocker_config = pg_walstream::ReplicationStreamConfig::new(
        "pgcdc_slot".to_string(),
        "pgcdc_pub".to_string(),
        1,
        pg_walstream::StreamingMode::Off,
        Duration::from_secs(10),
        Duration::from_secs(30),
        Duration::from_secs(60),
        pg_walstream::RetryConfig::default(),
    )
    .with_binary(false)
    .with_messages(false);
    let blocker_url = format!("{conn}?replication=database");
    let mut blocker = pg_walstream::LogicalReplicationStream::new(&blocker_url, blocker_config)
        .await
        .expect("open the blocking connection");
    blocker
        .start(None)
        .await
        .expect("the blocking connection must grab the slot and never release it");

    let mut cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"));
    cmd.env("PGCDC_DATABASE_URL", &conn)
        .env("PGCDC_PUBLICATION", "pgcdc_pub")
        .env("PGCDC_SLOT", "pgcdc_slot")
        // A fast backoff and a small budget — so patience runs out within
        // hundreds of milliseconds instead of the real default 30 seconds.
        .env("PGCDC_RECONNECT_INITIAL_MS", "20")
        .env("PGCDC_RECONNECT_MAX_MS", "50")
        .env("PGCDC_SLOT_BUSY_BUDGET_MS", "300")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        // kill_on_drop: if the timeout below fires (patience somehow did
        // not run out), the process will not be orphaned reconnecting forever.
        .kill_on_drop(true);
    let child = cmd.spawn().expect("spawn pgcdc");

    let output = tokio::time::timeout(Duration::from_secs(20), child.wait_with_output())
        .await
        .expect(
            "the process must exhaust the patience budget and finish, not go into an endless reconnect",
        )
        .expect("wait for pgcdc to finish");

    // Keep the blocking connection alive up to this point: the slot must
    // stay busy for the whole test, otherwise this would be testing the
    // ordinary race (already covered by the test above), not a
    // permanently busy slot.
    drop(blocker);

    assert!(
        !output.status.success(),
        "a slot permanently held busy by a foreign consumer must be fatal once the patience budget is exhausted"
    );
    assert_eq!(
        output.status.code(),
        Some(1),
        "a fatal error must produce exit code 1 (DECISIONS Q22)"
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout must be valid UTF-8");
    assert!(
        stdout.is_empty(),
        "stdout must stay empty on a fatal startup error, got: {stdout:?}"
    );

    let stderr = String::from_utf8(output.stderr).expect("stderr must be valid UTF-8");
    assert!(
        stderr.contains("slot_busy_timed_out"),
        "stderr must name the reason via the machine-readable error_kind label, got: {stderr}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn reconnect_zeroes_the_buffer_gauge_at_the_run_call_site() {
    // M4 (review round after task 4): removing the call to
    // state.reset_for_reconnect(&metrics) from run() leaves every other
    // test green — the function itself is covered by a unit test
    // (reconnect_zeroes_the_buffer_gauge_even_with_an_open_transaction in
    // src/postgres/replication.rs), but its one call site inside run() is
    // not. The README explicitly promises that the buffer-size gauge drops
    // to zero on reconnect too — this test pins down that call site, not
    // the function itself.
    //
    // Two pitfalls that would make "just drop the connection and check the
    // counter" not work:
    // 1) A single-row transaction arrives as one chunk (BEGIN+row+COMMIT
    //    back to back) — there's no window where the buffer is actually
    //    nonzero while COMMIT hasn't arrived yet. The transaction here is
    //    large enough that decoding and transfer take measurable time, and
    //    the test waits (by polling, not a blind sleep) until the gauge
    //    turns positive.
    // 2) The very next BEGIN of the replayed transaction would zero out
    //    `len()` naturally (Assembler::handle overwrites the whole open
    //    transaction, with no explicit reset) — without an extra measure,
    //    the mutation "remove reset_for_reconnect" would go unnoticed,
    //    because zero would show up anyway, just for a different reason.
    //    Here a second reader holds the slot busy right after the drop, so
    //    no new frame can arrive while the blocker is alive — and any zero
    //    that shows up can only come from reset_for_reconnect itself.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let metrics = std::sync::Arc::new(pgcdc::metrics::Metrics::new());
    let (tx_send, _tx_recv) = mpsc::unbounded_channel();
    let mut cfg = config(&conn);
    // Large enough that the blocker is guaranteed to grab the slot (racing
    // only against the old backend disconnecting on the server, not
    // against this attempt) before run() even tries to reconnect.
    cfg.reconnect_initial_ms = 2000;
    let m = metrics.clone();
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(cfg, Box::new(ChannelSink(tx_send, None)), m).await
    });

    common::wait_until_slot_active(&client, "pgcdc_slot").await;

    client
        .execute(
            "INSERT INTO users SELECT g, 'x', NULL, repeat('y', 4000) \
             FROM generate_series(1, 20000) g",
            &[],
        )
        .await
        .unwrap();

    let mut caught_mid_transaction = false;
    for _ in 0..400 {
        if metrics.snapshot().transaction_buffer_size > 0 {
            caught_mid_transaction = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        caught_mid_transaction,
        "the test did not catch the transaction in flight — increase the insert size"
    );

    common::terminate_replication_backend(&client).await;

    // Grab the slot before run() itself tries to reconnect (it has a full
    // 2-second head start, cfg.reconnect_initial_ms above). The only race
    // here is with how long the server takes to disconnect the old backend
    // after pg_terminate_backend, not with pgcdc.
    let make_blocker_config = || {
        pg_walstream::ReplicationStreamConfig::new(
            "pgcdc_slot".to_string(),
            "pgcdc_pub".to_string(),
            1,
            pg_walstream::StreamingMode::Off,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            pg_walstream::RetryConfig::default(),
        )
        .with_binary(false)
        .with_messages(false)
    };
    let blocker_url = format!("{conn}?replication=database");
    let mut blocker = None;
    for _ in 0..200 {
        if let Ok(mut s) =
            pg_walstream::LogicalReplicationStream::new(&blocker_url, make_blocker_config()).await
        {
            if s.start(None).await.is_ok() {
                blocker = Some(s);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let _blocker = blocker.expect("failed to grab the slot with the blocker before pgcdc");

    // By this point run() should have already gone through the backoff
    // pause and reset_for_reconnect(), but failed to get a single new
    // frame — the slot is held by the blocker.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(
        metrics.snapshot().transaction_buffer_size,
        0,
        "the gauge must drop to zero on reconnect, even while the slot is busy and there are no new frames"
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn changing_a_key_column_produces_a_key_only_before_image() {
    // The one form of UPDATE not present in the frozen capture: the 'K'
    // tag. The unit test checks it with synthetic bytes, i.e. our own
    // understanding of the wire format; here it's produced by a real PostgreSQL.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::setup_items_table(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let (tx_send, mut tx_recv) = mpsc::unbounded_channel();
    let cfg = config(&conn);
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(
            cfg,
            Box::new(ChannelSink(tx_send, None)),
            std::sync::Arc::new(pgcdc::metrics::Metrics::new()),
        )
        .await
    });

    client
        .execute("INSERT INTO items VALUES (10, 'Widget', 5)", &[])
        .await
        .unwrap();
    client
        .execute("UPDATE items SET id = 11 WHERE id = 10", &[])
        .await
        .unwrap();

    // The first transaction is the INSERT, the second is the UPDATE we care about.
    let _insert_tx = tokio::time::timeout(Duration::from_secs(20), tx_recv.recv())
        .await
        .expect("the insert should arrive")
        .expect("channel closed");
    let update_tx = tokio::time::timeout(Duration::from_secs(20), tx_recv.recv())
        .await
        .expect("the update should arrive")
        .expect("channel closed");

    let ev = &update_tx.changes[0];
    let json = serde_json::to_value(ev).unwrap();
    assert_eq!(json["operation"], "update");
    assert_eq!(
        json["before_kind"], "key",
        "the key changed — the server sends the 'K' tag"
    );
    assert_eq!(json["before"]["id"], "10", "the old key value");
    assert!(
        json["before"].get("title").is_none(),
        "the server did not send non-key columns, and before must not have them: {json}"
    );
    assert_eq!(json["after"]["id"], "11");
    assert_eq!(json["after"]["title"], "Widget");

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_change_resends_relation_and_the_cache_takes_the_new_one() {
    // pgoutput resends RELATION when a cache entry is invalidated — for
    // example after DDL. Stage 0's capture doesn't contain such a case, and
    // replacing the cache entry depends directly on this behavior.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::setup_items_table(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let (tx_send, mut tx_recv) = mpsc::unbounded_channel();
    let cfg = config(&conn);
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(
            cfg,
            Box::new(ChannelSink(tx_send, None)),
            std::sync::Arc::new(pgcdc::metrics::Metrics::new()),
        )
        .await
    });

    client
        .execute("INSERT INTO items VALUES (1, 'before ddl', 1)", &[])
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(20), tx_recv.recv())
        .await
        .expect("the first insert")
        .expect("channel closed");
    assert_eq!(first.changes[0].after.as_ref().unwrap().len(), 3);

    client
        .batch_execute("ALTER TABLE items ADD COLUMN note TEXT")
        .await
        .unwrap();
    client
        .execute("INSERT INTO items VALUES (2, 'after ddl', 2, 'hello')", &[])
        .await
        .unwrap();

    let second = tokio::time::timeout(Duration::from_secs(20), tx_recv.recv())
        .await
        .expect("the second insert")
        .expect("channel closed");
    let after = second.changes[0].after.as_ref().unwrap();
    assert_eq!(
        after.len(),
        4,
        "the cache must have picked up the new schema"
    );
    assert_eq!(after.get("note").unwrap(), "hello");

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn several_transactions_are_not_lost_and_the_slot_catches_up() {
    // The name used to promise a check of acknowledgement grouping, but
    // ChannelSink reports a durable position on EVERY flush call
    // regardless of how many transactions accumulated — for this test
    // double, grouped and per-transaction acknowledgement are
    // indistinguishable. What is actually checked here is only that
    // grouping does not lose transactions and that the slot eventually
    // catches up with the last delivered position (review round 2, F3).
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let (tx_send, mut tx_recv) = mpsc::unbounded_channel();
    let mut cfg = config(&conn);
    cfg.ack_interval_ms = 500;
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(
            cfg,
            Box::new(ChannelSink(tx_send, None)),
            std::sync::Arc::new(pgcdc::metrics::Metrics::new()),
        )
        .await
    });

    for id in 1..=5 {
        client
            .execute(
                "INSERT INTO users VALUES ($1, 'x', NULL, NULL)",
                &[&(id as i64)],
            )
            .await
            .unwrap();
    }

    let mut seen = Vec::new();
    for _ in 0..5 {
        let tx = tokio::time::timeout(Duration::from_secs(20), tx_recv.recv())
            .await
            .expect("all five transactions should arrive")
            .expect("channel closed");
        seen.push(tx.end_lsn);
    }
    assert_eq!(seen.len(), 5, "grouping does not lose transactions");

    // The slot must catch up with the last delivered position.
    let last = seen.last().copied().unwrap();
    let confirmed = common::wait_for_slot_at_least(&client, "pgcdc_slot", last).await;
    assert!(
        confirmed >= last,
        "the slot caught up with the last group: {confirmed} < {last}"
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn slot_advances_while_the_publication_is_idle() {
    // A classic problem: writes go to tables outside the publication, we
    // never receive a single event, the slot stands still, WAL grows.
    // Keepalive-based advancement exists precisely for this.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    // A table OUTSIDE the publication: writes to it move WAL forward but produce no events.
    client
        .batch_execute("CREATE TABLE public.noise (id BIGINT PRIMARY KEY, payload TEXT)")
        .await
        .unwrap();

    let (tx_send, _tx_recv) = mpsc::unbounded_channel();
    let cfg = config(&conn);
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(
            cfg,
            Box::new(ChannelSink(tx_send, None)),
            std::sync::Arc::new(pgcdc::metrics::Metrics::new()),
        )
        .await
    });

    for id in 1..=50 {
        client
            .execute(
                "INSERT INTO noise VALUES ($1, repeat('x', 1000))",
                &[&(id as i64)],
            )
            .await
            .unwrap();
    }

    let target: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .unwrap()
        .get(0);
    let target = common::parse_lsn(&target).expect("server position");

    let confirmed = common::wait_for_slot_at_least(&client, "pgcdc_slot", target).await;
    assert!(
        confirmed >= target,
        "the slot must catch up with the server on an idle publication: {confirmed} < {target}"
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn keepalive_does_not_advance_the_slot_past_an_unwritten_transaction() {
    // FlushFailsSink, not the test double that fails unconditionally
    // (review round 2, F2): that one failed the barrier on an EMPTY tick
    // too, aborting run() with an error before the first write_transaction
    // — the INSERT below never got the chance to happen, and the "slot did
    // not move" assertion would pass without checking anything about the
    // keepalive branch. FlushFailsSink answers Ok(None) on an empty barrier
    // and only fails when there is something to acknowledge, so the write
    // really does reach the sink, and only the barrier fails — the slot
    // must stand still.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let before: String = client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 'pgcdc_slot'",
            &[],
        )
        .await
        .unwrap()
        .get(0);

    let cfg = config(&conn);
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(
            cfg,
            Box::new(FlushFailsSink(None)),
            std::sync::Arc::new(pgcdc::metrics::Metrics::new()),
        )
        .await
    });

    client
        .execute("INSERT INTO users VALUES (1, 'Alice', NULL, NULL)", &[])
        .await
        .unwrap();

    let result = tokio::time::timeout(Duration::from_secs(20), handle)
        .await
        .expect("run should finish, not hang")
        .expect("join");
    assert!(matches!(result.unwrap_err(), PgcdcError::Sink(_)));

    let after: String = client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 'pgcdc_slot'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        before, after,
        "the barrier did not go through — the slot does not move"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_connection_is_recovered_without_losing_rows() {
    // Capture logs before run() starts: the check below (F3) must see the
    // recovery event, not just guess that it happened.
    let log_events = common::capture_log_events();

    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    // M8: the slot name here is unique to this test, rather than the
    // common "pgcdc_slot" most neighbors use — `log_events` accumulates
    // messages from ALL tests running in parallel within one process (a
    // shared global buffer), and without a unique marker inside the
    // message itself, matching on the text "postgres_connection_restored"
    // below would become a coincidence, not proof that a reconnect
    // happened right here.
    let slot = "pgcdc_slot_recover_no_loss";
    common::create_slot(&client, slot).await;

    // F5 (review Task 2, round 1): a shared instance, not a throwaway one —
    // this is the only test that is guaranteed to cross the reconnect
    // branch, and hence the only mutation coverage `reconnects_total` will
    // ever get at all.
    let metrics = std::sync::Arc::new(pgcdc::metrics::Metrics::new());
    let (tx_send, mut tx_recv) = mpsc::unbounded_channel();
    let mut cfg = config(&conn);
    cfg.slot = slot.into();
    let m = metrics.clone();
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(cfg, Box::new(ChannelSink(tx_send, None)), m).await
    });

    client
        .execute("INSERT INTO users VALUES (1, 'before', NULL, NULL)", &[])
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(20), tx_recv.recv())
        .await
        .expect("the first transaction")
        .expect("channel closed");
    assert_eq!(
        first.changes[0]
            .after
            .as_ref()
            .unwrap()
            .get("name")
            .unwrap(),
        "before"
    );

    // Wait for our own barrier to bring the first transaction to durable
    // and for the slot on the server to acknowledge it: the channel
    // delivers the transaction right after write_transaction, but reaching
    // durable/acked still needs the next barrier tick. Without this, the
    // drop below would almost certainly happen before the first flush,
    // `state.durable()` would stay at zero, is_reconnect() would never
    // become true, and the whole reconnect-check block would go
    // unexercised by this test — exactly the risk from review Task 2,
    // round 1, F3.
    common::wait_for_slot_at_least(&client, slot, first.end_lsn).await;

    // The server drops our replication connection.
    common::terminate_replication_backend(&client).await;

    client
        .execute("INSERT INTO users VALUES (2, 'after', NULL, NULL)", &[])
        .await
        .unwrap();

    // The row inserted after the drop must arrive. A duplicate of the
    // first row is acceptable and allowed by the contract, so we search
    // for the one we need instead of taking the first one.
    let mut names = Vec::new();
    for _ in 0..5 {
        match tokio::time::timeout(Duration::from_secs(20), tx_recv.recv()).await {
            Ok(Some(tx)) => {
                for ch in &tx.changes {
                    if let Some(after) = &ch.after {
                        names.push(after.get("name").unwrap().as_str().unwrap().to_string());
                    }
                }
                if names.iter().any(|n| n == "after") {
                    break;
                }
            }
            _ => break,
        }
    }
    assert!(
        names.iter().any(|n| n == "after"),
        "the row after the drop did not arrive, saw: {names:?}"
    );

    // F3: this is the task's headline check — that check_reconnect
    // actually ran and the recovery was actually logged, not just "the row
    // somehow arrived". Deleting the whole reconnect-check block would not
    // touch any of the earlier assertions in this test.
    //
    // M8: the message must carry EXACTLY our `slot` — `log_events` is
    // shared across the whole test binary, and another test that lives
    // long enough for a successful reconnect would log the same message
    // with a DIFFERENT slot name. Today the only reconnect neighbor
    // (`a_slot_advanced_past_our_durable_position_is_fatal_on_reconnect`)
    // fails at check_reconnect before this log line, so matching on the
    // message text alone would still be (accidentally) correct without a
    // marker; with the marker, it stops depending on that fragile assumption.
    let expected_log = format!("postgres_connection_restored slot={slot}");
    assert!(
        log_events.lock().unwrap().contains(&expected_log),
        "the connection-restored event was not logged for slot {slot}"
    );

    // F5 (review Task 2, round 1): by this point the outer reconnect loop
    // has already made at least one full lap (proven by the restoration
    // log above), so the counter must have advanced. This is the only
    // mutation check `reconnects_total` gets at all — removing the
    // increment or wiring it up wrong would go unnoticed by the rest of
    // the suite.
    assert!(
        metrics.snapshot().reconnects_total >= 1,
        "reconnects_total must have advanced after the drop and recovery"
    );

    // F4: a slot that disappears during a drop must be fatal immediately,
    // not retried forever — otherwise the process would sit in the
    // reconnect loop while the data it was supposed to capture aged out of WAL.
    common::terminate_replication_backend(&client).await;
    common::drop_slot_once_inactive(&client, slot).await;

    let result = tokio::time::timeout(Duration::from_secs(20), handle)
        .await
        .expect("run must fail on a missing slot, not retry forever")
        .expect("join");
    let err = result.unwrap_err();
    assert!(matches!(err, PgcdcError::SlotMissing { .. }), "got {err:?}");
    assert!(err.is_fatal());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_slot_advanced_past_our_durable_position_is_fatal_on_reconnect() {
    // Direct proof that check_reconnect() is now ACTUALLY called, not just
    // sitting there untouched (review Task 2, round 1, F3). The test in the
    // neighboring function doesn't check this: there, on an ordinary
    // reconnect, the slot either exactly matches durable or lags behind —
    // it never touches the asymmetric case "the slot is AHEAD — fatal".
    // Here we manually move the slot forward via
    // pg_replication_slot_advance, past our sink — exactly the case where
    // someone acknowledged WAL we never wrote.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let (tx_send, mut tx_recv) = mpsc::unbounded_channel();
    let mut cfg = config(&conn);
    // The window is needed to advance the slot BEFORE the process itself
    // attempts to reconnect.
    cfg.reconnect_initial_ms = 3000;
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(
            cfg,
            Box::new(ChannelSink(tx_send, None)),
            std::sync::Arc::new(pgcdc::metrics::Metrics::new()),
        )
        .await
    });

    client
        .execute("INSERT INTO users VALUES (1, 'before', NULL, NULL)", &[])
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(20), tx_recv.recv())
        .await
        .expect("the first transaction")
        .expect("channel closed");
    // Durable must actually become nonzero, otherwise is_reconnect() on
    // the next connection stays false and check_reconnect never gets called.
    common::wait_for_slot_at_least(&client, "pgcdc_slot", first.end_lsn).await;

    common::terminate_replication_backend(&client).await;
    common::wait_until_slot_inactive(&client, "pgcdc_slot").await;

    // A row our (now disconnected) sink will never see.
    client
        .execute("INSERT INTO users VALUES (2, 'ghost', NULL, NULL)", &[])
        .await
        .unwrap();
    let target: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .unwrap()
        .get(0);
    client
        .query(
            // A double cast, not a direct $2::pg_lsn: otherwise Postgres
            // infers the placeholder's type as pg_lsn directly, and
            // tokio-postgres can't bind a `String` into it (WrongType).
            // Through ::text::pg_lsn the placeholder stays textual for the
            // driver, and the cast to pg_lsn happens on the server.
            "SELECT * FROM pg_replication_slot_advance($1, $2::text::pg_lsn)",
            &[&"pgcdc_slot", &target],
        )
        .await
        .expect("advance slot past our durable position");

    let result = tokio::time::timeout(Duration::from_secs(20), handle)
        .await
        .expect("run must fail with SlotAhead, not silently continue reconnecting")
        .expect("join");
    let err = result.unwrap_err();
    assert!(matches!(err, PgcdcError::SlotAhead { .. }), "got {err:?}");
    assert!(err.is_fatal());
}

#[tokio::test(flavor = "multi_thread")]
async fn file_output_binary_writes_durable_json_lines() {
    // The `--output file` branch in main.rs is completely uncovered:
    // replace FileSink with StdoutSink in the match and no test fails red.
    // FileSink is the stage's only sink that honestly promises Fsync, and
    // this exact branch of the binary never exercises it at all (review
    // round 2, F7). We run the real binary end to end: CLI parsing, guard,
    // the replication loop, and the file sink with fsync.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let mut path = std::env::temp_dir();
    path.push(format!("pgcdc-integration-{}.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&path);

    let mut child = common::KillOnDrop(
        std::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"))
            .env("PGCDC_DATABASE_URL", &conn)
            .env("PGCDC_PUBLICATION", "pgcdc_pub")
            .env("PGCDC_SLOT", "pgcdc_slot")
            .env("PGCDC_OUTPUT", "file")
            .env("PGCDC_OUTPUT_PATH", &path)
            .env("PGCDC_ACK_INTERVAL_MS", "50")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn the binary"),
    );

    client
        .execute("INSERT INTO users VALUES (1, 'Alice', NULL, NULL)", &[])
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let text = loop {
        if let Ok(text) = std::fs::read_to_string(&path) {
            if !text.trim().is_empty() {
                break text;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let _ = std::fs::remove_file(&path);
            panic!("the file got no lines within 20s");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_file(&path);

    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 1, "one INSERT — one line: {text:?}");
    let json: serde_json::Value =
        serde_json::from_str(lines[0]).expect("the file line is valid JSON");
    assert_eq!(json["operation"], "insert");
    assert_eq!(json["table"], "users");
    assert_eq!(json["after"]["id"], "1");
    assert_eq!(json["after"]["name"], "Alice");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_terminated_process_exits_zero_after_draining() {
    // A graceful stop must produce zero. Otherwise a supervisor would keep
    // endlessly restarting a process that was stopped intentionally.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let out = std::env::temp_dir().join(format!("pgcdc-sigterm-{}.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&out);

    let mut child = common::KillOnDrop(
        std::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"))
            .args([
                "--database-url",
                &conn,
                "--publication",
                "pgcdc_pub",
                "--slot",
                "pgcdc_slot",
                "--output",
                "file",
                "--output-path",
                out.to_str().unwrap(),
            ])
            .spawn()
            .expect("spawn the binary"),
    );

    client
        .execute("INSERT INTO users VALUES (1, 'Alice', NULL, NULL)", &[])
        .await
        .unwrap();

    // Wait until the line shows up in the file — that means the process reached the barrier.
    let mut seen = false;
    for _ in 0..200 {
        if std::fs::read_to_string(&out)
            .map(|t| !t.trim().is_empty())
            .unwrap_or(false)
        {
            seen = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        seen,
        "the line did not appear in the file within 20 seconds"
    );

    // SIGTERM, not kill: we're specifically checking the graceful shutdown.
    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
    let status = tokio::task::spawn_blocking(move || child.wait())
        .await
        .unwrap()
        .expect("wait");
    assert_eq!(status.code(), Some(0), "a graceful stop gives zero");

    let _ = std::fs::remove_file(&out);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_over_the_limit_is_fatal_and_the_slot_stays_put() {
    // The limit doesn't fix a restart loop on a giant transaction — it
    // changes the diagnostic from "killed for memory" to an intelligible
    // message (DECISIONS Q7).
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let before: String = client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 'pgcdc_slot'",
            &[],
        )
        .await
        .unwrap()
        .get(0);

    let mut cfg = config(&conn);
    cfg.max_transaction_events = 2;
    let (tx_send, _rx) = mpsc::unbounded_channel();
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(
            cfg,
            Box::new(ChannelSink(tx_send, None)),
            std::sync::Arc::new(pgcdc::metrics::Metrics::new()),
        )
        .await
    });

    client
        .execute(
            "INSERT INTO users SELECT g, 'x', NULL, NULL FROM generate_series(1, 10) g",
            &[],
        )
        .await
        .unwrap();

    let result = tokio::time::timeout(Duration::from_secs(20), handle)
        .await
        .expect("run should finish, not hang")
        .expect("join");
    let err = result.unwrap_err();
    assert!(
        matches!(err, PgcdcError::TransactionTooLarge { limit: 2 }),
        "got {err:?}"
    );
    assert!(
        err.is_fatal(),
        "exceeding the limit is a fatal error, not a reason to retry"
    );

    let after: String = client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 'pgcdc_slot'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, after, "a fatal error does not move the slot");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_terminated_process_drains_before_the_periodic_barrier_would() {
    // Round 1, F2: `a_terminated_process_exits_zero_after_draining` stays
    // green even without a barrier in the shutdown branch itself, because
    // at the default interval (200ms) the periodic barrier almost always
    // manages to fire before we even send the signal. This test closes
    // exactly that gap: the barrier interval is cranked up so high that the
    // periodic branch is guaranteed not to fire within the pre-check window.
    //
    // The check is NOT on the file's contents: `FileSink` writes through a
    // `BufWriter<File>`, and on a normal (non-panic) process exit, Rust
    // itself does a best-effort `flush()` for it in `Drop` — without the
    // shutdown branch's barrier call, the line would still end up in the
    // file, just without fsync and without acknowledging the slot. The
    // first attempt at writing this test checked exactly that — the file
    // is nonempty after SIGTERM — and stayed green AFTER a mutation that
    // removed the barrier: `Drop` masked the missing call. The one source
    // of truth that can't be faked through `Drop` is the slot's
    // `confirmed_flush_lsn` on the server: it advances only via a call to
    // `send_feedback`, and in the shutdown branch that only happens inside
    // `flush_and_acknowledge`.
    //
    // Round 3, F1: previously a fixed `sleep` stood here instead of waiting
    // for proof. The reviewer uncovered the real cause of the flakiness: at
    // 150ms the child process hadn't even managed to install its signal
    // handler yet, at 700ms it passed by a hair (~0.4s of margin) — under
    // twenty parallel containers that budget isn't enough, and SIGTERM goes
    // out BEFORE the transaction was even accepted and parsed. In that
    // case the barrier simply has nothing to flush, and the slot doesn't
    // move — the same failure signature as the mutation that removes the
    // barrier call from the shutdown branch, but for a different reason.
    // The clock has been replaced with proof: we capture the child
    // process's stderr under debug logging and wait for the
    // `transaction_accepted` line — it's logged right after
    // `sink.write_transaction`, i.e. before any barrier, and means the
    // transaction is guaranteed to be sitting in the sink and the barrier
    // will have something to flush. SIGTERM is only sent after that. The
    // check below (`before_signal < target`) isn't weakened to match: if
    // we had waited even longer — long enough for the periodic barrier to
    // fire — it would fail on its own, proving the test had stopped
    // isolating the shutdown branch.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let out = std::env::temp_dir().join(format!(
        "pgcdc-sigterm-barrier-{}.jsonl",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&out);

    let mut child = common::KillOnDrop(
        std::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"))
            .args([
                "--database-url",
                &conn,
                "--publication",
                "pgcdc_pub",
                "--slot",
                "pgcdc_slot",
                "--output",
                "file",
                "--output-path",
                out.to_str().unwrap(),
                // An order of magnitude larger than the time to the signal
                // below: if the timer barrier fires within this window
                // anyway, the test proves nothing about the shutdown branch.
                "--ack-interval-ms",
                "10000",
            ])
            // debug — to see transaction_accepted (logged at this level
            // right after the sink accepts the transaction); pg_walstream
            // is muted separately so we don't drown in its own debug noise.
            .env("RUST_LOG", "pgcdc=debug,pg_walstream=warn")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("spawn the binary"),
    );

    let stderr = child.stderr.take().expect("stderr was requested as piped");
    let lines: std::sync::Arc<std::sync::Mutex<Vec<String>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let lines_writer = lines.clone();
    std::thread::spawn(move || {
        use std::io::BufRead;
        let reader = std::io::BufReader::new(stderr);
        for line in reader.lines().map_while(Result::ok) {
            lines_writer.lock().unwrap().push(line);
        }
    });

    client
        .execute("INSERT INTO users VALUES (1, 'Alice', NULL, NULL)", &[])
        .await
        .unwrap();

    // The WAL position right after the commit — a lower bound on what the
    // process will have to acknowledge to the slot once it does
    // acknowledge this transaction (via the periodic barrier or the
    // shutdown barrier).
    let target: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .unwrap()
        .get(0);
    let target = common::parse_lsn(&target).expect("parse the LSN");

    // Wait for proof, not for time: `transaction_accepted` means the
    // transaction has already been accepted by the sink and is sitting
    // there waiting for the barrier.
    let mut accepted = false;
    for _ in 0..200 {
        if lines
            .lock()
            .unwrap()
            .iter()
            .any(|l| l.contains("transaction_accepted"))
        {
            accepted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        accepted,
        "did not see transaction_accepted within 20 seconds, saw: {:?}",
        lines.lock().unwrap()
    );

    let before_signal: String = client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 'pgcdc_slot'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let before_signal = common::parse_lsn(&before_signal).expect("parse the LSN");
    assert!(
        before_signal < target,
        "the slot advanced to {target} BEFORE the signal (currently {before_signal}) — the \
         periodic barrier fired in time, the test isn't isolating the shutdown branch"
    );

    // SIGTERM: if the barrier lives in the shutdown branch (as it should),
    // the slot must acknowledge the transaction only AFTER the signal.
    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
    let status = tokio::task::spawn_blocking(move || child.wait())
        .await
        .unwrap()
        .expect("wait");
    assert_eq!(status.code(), Some(0), "a graceful stop gives zero");

    let after_signal = common::wait_for_slot_at_least(&client, "pgcdc_slot", target).await;
    assert!(
        after_signal >= target,
        "the slot did not acknowledge the transaction after a graceful stop: {after_signal} < {target}"
    );

    let _ = std::fs::remove_file(&out);
}

#[tokio::test(flavor = "multi_thread")]
async fn sending_sigterm_after_a_reconnect_still_exits_zero() {
    // Checks only what the name claims: after a reconnect, SIGTERM still
    // brings the process to a graceful exit with code 0.
    //
    // Round 2, F3: this test used to claim more — that it catches
    // `spawn_shutdown_listener()` being moved inside the reconnect loop
    // (recreating the listener on every session). That's wrong:
    // `tokio::signal::unix::signal` and `ctrl_c()` deliver the signal to
    // EVERY registered listener of that kind, not just the most recently
    // created one — so even a listener recreated on the second session
    // would still get SIGTERM, and the test would stay green regardless of
    // where the call sits. The listener still lives above the outer loop
    // (recreating it on every reconnect would be a task leak per session),
    // but this is a test of behavior (the signal still works after a
    // reconnect), not of code placement.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let out = std::env::temp_dir().join(format!(
        "pgcdc-reconnect-sigterm-{}.jsonl",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&out);

    let mut child = common::KillOnDrop(
        std::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"))
            .args([
                "--database-url",
                &conn,
                "--publication",
                "pgcdc_pub",
                "--slot",
                "pgcdc_slot",
                "--output",
                "file",
                "--output-path",
                out.to_str().unwrap(),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("spawn the binary"),
    );

    // Read the child process's stderr line by line on a background thread:
    // we need to PROVE that a reconnect happened before sending the
    // signal, not just hope based on timing.
    // `postgres_connection_restored` is only logged on a successful
    // reconnection (stream_once, review Task 2).
    let stderr = child.stderr.take().expect("stderr was requested as piped");
    let lines: std::sync::Arc<std::sync::Mutex<Vec<String>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let lines_writer = lines.clone();
    std::thread::spawn(move || {
        use std::io::BufRead;
        let reader = std::io::BufReader::new(stderr);
        for line in reader.lines().map_while(Result::ok) {
            lines_writer.lock().unwrap().push(line);
        }
    });

    client
        .execute("INSERT INTO users VALUES (1, 'before', NULL, NULL)", &[])
        .await
        .unwrap();

    // Wait for the first line in the file — proof the process reached the
    // barrier during the first session.
    let mut seen = false;
    for _ in 0..200 {
        if std::fs::read_to_string(&out)
            .map(|t| !t.trim().is_empty())
            .unwrap_or(false)
        {
            seen = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        seen,
        "the first line did not appear in the file within 20 seconds"
    );

    // Drop the replication connection from the server side — the process
    // must reconnect on its own (task 2), not fail.
    common::terminate_replication_backend(&client).await;

    // Wait for the connection-restored log line: without it, the check
    // below would just be re-testing an ordinary signal scenario, saying
    // nothing about the reconnect.
    let mut reconnected = false;
    for _ in 0..200 {
        if lines
            .lock()
            .unwrap()
            .iter()
            .any(|l| l.contains("postgres_connection_restored"))
        {
            reconnected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        reconnected,
        "did not see the connection-restored log line within 20 seconds, saw: {:?}",
        lines.lock().unwrap()
    );

    // SIGTERM AFTER the reconnect: see the comment above the function —
    // this is a test of behavior (the signal still works), not of code placement.
    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
    let status = tokio::task::spawn_blocking(move || child.wait())
        .await
        .unwrap()
        .expect("wait");
    assert_eq!(
        status.code(),
        Some(0),
        "a graceful stop after a reconnect must also give zero"
    );

    let _ = std::fs::remove_file(&out);
}

#[tokio::test(flavor = "multi_thread")]
async fn sigint_also_stops_the_process_cleanly() {
    // The checklist claims SIGINT is handled on par with SIGTERM, but there
    // was no test for it; SIGTERM is covered by a separate test, while
    // SIGINT so far has only relied on the listener merging both signals
    // into one select. We check that the merge actually works.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let out = std::env::temp_dir().join(format!("pgcdc-sigint-{}.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&out);

    let mut child = common::KillOnDrop(
        std::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"))
            .args([
                "--database-url",
                &conn,
                "--publication",
                "pgcdc_pub",
                "--slot",
                "pgcdc_slot",
                "--output",
                "file",
                "--output-path",
                out.to_str().unwrap(),
            ])
            .spawn()
            .expect("spawn the binary"),
    );

    client
        .execute("INSERT INTO users VALUES (1, 'Alice', NULL, NULL)", &[])
        .await
        .unwrap();

    let target: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .unwrap()
        .get(0);
    let target = common::parse_lsn(&target).expect("parse the LSN");
    common::wait_for_slot_at_least(&client, "pgcdc_slot", target).await;

    unsafe { libc::kill(child.id() as i32, libc::SIGINT) };

    // Polling via try_wait() rather than a blocking wait() (round 1
    // review, F5): just like in this test's dead-port neighbor, a
    // regression that installs the handler but never sets the flag would
    // leave a blocking wait() hanging forever, and tokio's runtime Drop
    // specifically waits for blocking tasks — the test would hang the
    // whole test binary instead of simply failing red.
    let mut status = None;
    for _ in 0..100 {
        if let Ok(Some(s)) = child.try_wait() {
            status = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let status = status.expect("SIGINT must stop the process within 5 seconds, not only SIGKILL");
    assert_eq!(status.code(), Some(0), "SIGINT also gives zero");

    let _ = std::fs::remove_file(&out);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_productive_session_resets_the_backoff() {
    // Carried over from stage 4. The reset was considered unverifiable, but
    // the delay lands in the log as a structured field on every attempt,
    // and that's enough. The scenario: two drops in a row with a productive
    // session between them — the second series' delay must start over from
    // the initial value, not keep growing from where it left off.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let out = std::env::temp_dir().join(format!("pgcdc-backoff-{}.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&out);

    let mut child = common::spawn_with_stderr(&[
        "--database-url",
        &conn,
        "--publication",
        "pgcdc_pub",
        "--slot",
        "pgcdc_slot",
        "--output",
        "file",
        "--output-path",
        out.to_str().unwrap(),
        "--reconnect-initial-ms",
        "100",
        "--reconnect-max-ms",
        "800",
    ]);

    // Wait for the first (cold-start) session's walsender to actually
    // connect: a noticeable amount of time passes between `spawn()` and
    // `START_REPLICATION` (argument parsing, TCP, preflight), and a drop
    // sent earlier finds nobody — the first backoff series would then
    // never happen at all.
    common::wait_until_slot_active(&client, "pgcdc_slot").await;

    // The first drop and an insert, so the session after it is productive.
    common::terminate_replication_backend(&client).await;
    client
        .execute("INSERT INTO users VALUES (1, 'Alice', NULL, NULL)", &[])
        .await
        .unwrap();
    let target: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .unwrap()
        .get(0);
    let target = common::parse_lsn(&target).expect("parse the LSN");
    common::wait_for_slot_at_least(&client, "pgcdc_slot", target).await;

    // The second drop. The first attempt after it must take the initial delay.
    common::terminate_replication_backend(&client).await;

    // Read BOTH series: the first always starts with the initial delay
    // regardless, and only the second one tells them apart. With the reset
    // you get [100, 100]; without it, the second series continues with the
    // doubled value — [100, 200].
    let delays = common::collect_backoff_delays(&mut child, 2).await;
    assert_eq!(
        delays.get(1).copied(),
        Some(100),
        "after a productive session the backoff must restart, not continue: {delays:?}"
    );

    let _ = std::fs::remove_file(&out);
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_report_line_is_periodic_and_its_countdown_survives_a_reconnect() {
    // I2: deleting the whole periodic-report block (`metrics_report`,
    // `METRICS_REPORT_INTERVAL`) left all 168 tests green — neither that
    // the line comes out at all, nor the interval, nor that the countdown
    // survives a reconnect, was pinned down by anything but a manual demo
    // run. And it was precisely surviving a reconnect that justified
    // hoisting `last_report` outside the reconnect loop in a previous round
    // (review Task 3, round 1, F1): without it, a process reconnecting more
    // often than ten seconds would never live long enough inside a single
    // session for the report to come out.
    //
    // The scenario pins down both halves separately: the reconnect is
    // forced EARLY (within the first few seconds), well before the
    // ten-second interval. If the countdown were incorrectly reset on
    // reconnect, the line would appear no earlier than t_reconnect + 10s; a
    // countdown that survives the reconnect prints it around t_start + 10s
    // regardless of when the reconnect happened. The gap between these two
    // predictions is many seconds, and that's exactly what splits the test
    // into "survives" and "doesn't survive".
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let out =
        std::env::temp_dir().join(format!("pgcdc-metrics-report-{}.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&out);

    let t_start = std::time::Instant::now();
    let mut child = common::spawn_with_stderr(&[
        "--database-url",
        &conn,
        "--publication",
        "pgcdc_pub",
        "--slot",
        "pgcdc_slot",
        "--output",
        "file",
        "--output-path",
        out.to_str().unwrap(),
    ]);

    let stderr = child.stderr.take().expect("stderr captured at spawn");
    let lines: std::sync::Arc<std::sync::Mutex<Vec<String>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let lines_writer = lines.clone();
    std::thread::spawn(move || {
        use std::io::BufRead;
        let reader = std::io::BufReader::new(stderr);
        for line in reader.lines().map_while(Result::ok) {
            lines_writer.lock().unwrap().push(line);
        }
    });

    // Wait for the first (cold-start) session — a drop sent earlier would
    // find nobody (the backend hasn't connected yet).
    common::wait_until_slot_active(&client, "pgcdc_slot").await;

    client
        .execute("INSERT INTO users VALUES (1, 'before', NULL, NULL)", &[])
        .await
        .unwrap();

    let mut seen = false;
    for _ in 0..100 {
        if std::fs::read_to_string(&out)
            .map(|t| !t.trim().is_empty())
            .unwrap_or(false)
        {
            seen = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        seen,
        "the first line did not appear in the file within 5 seconds"
    );

    // Force a reconnect at a known, controlled mark (~6s from process
    // start) — neither immediately nor close to the ten-second interval.
    // The spacing matters: it's exactly what separates the two
    // predictions. A countdown that survives the reconnect prints the
    // report around t_start + 10s regardless of when the reconnect
    // happened; a countdown incorrectly reset ON reconnect prints it
    // around t_reconnect + 10s ≈ 16s — these two predictions are several
    // seconds apart, and that gap is exactly what the test targets.
    let elapsed_before_reconnect = t_start.elapsed();
    let reconnect_at = Duration::from_secs(6);
    if elapsed_before_reconnect < reconnect_at {
        tokio::time::sleep(reconnect_at - elapsed_before_reconnect).await;
    }
    common::terminate_replication_backend(&client).await;

    let mut reconnected = false;
    for _ in 0..100 {
        if lines
            .lock()
            .unwrap()
            .iter()
            .any(|l| l.contains("postgres_connection_restored"))
        {
            reconnected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        reconnected,
        "did not see the connection restored within 5 seconds"
    );
    let t_reconnected = t_start.elapsed();
    assert!(
        t_reconnected < Duration::from_secs(9),
        "the reconnect had to happen well before the report interval, but took {t_reconnected:?} — \
         the test cannot tell 'survived the reconnect' apart from 'coincided in time' without this margin"
    );

    // Wait for the report line, no more than 20 seconds from process start.
    let mut t_report = None;
    while t_start.elapsed() < Duration::from_secs(20) {
        if lines
            .lock()
            .unwrap()
            .iter()
            .any(|l| l.contains("metrics_report"))
        {
            t_report = Some(t_start.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let t_report = t_report
        .expect("the metrics_report line did not appear within 20 seconds of process start");

    assert!(
        t_report >= Duration::from_secs(9),
        "the report came out earlier than METRICS_REPORT_INTERVAL: {t_report:?} from process start"
    );
    assert!(
        t_report <= Duration::from_secs(13),
        "the report came out later than a countdown that survived the reconnect would allow: \
         {t_report:?} from process start, the reconnect happened at {t_reconnected:?} — resetting \
         the countdown on reconnect would push it to t_reconnected + 10s = {:?}, which is well \
         past this bound",
        t_reconnected + Duration::from_secs(10)
    );

    let _ = std::fs::remove_file(&out);
}
