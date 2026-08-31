mod common;

/// Scenario §18 of the base spec. The only test that checks the promise
/// "duplicates are allowed, silent loss is not" across the whole chain at
/// once: a real binary, a real PostgreSQL, a real SIGKILL, a real file.
///
/// We must kill the process right after the slot has ACKNOWLEDGED the
/// position, not after the rows became visible in the output file.
/// `FileSink` keeps bytes in a `BufWriter`, and in general that does NOT
/// mean it hands them to the OS only inside the durability barrier: `Drop`
/// on a normal (non-SIGKILL) process exit does a best-effort `flush()`, and
/// `BufWriter` itself flushes its buffer on its own once it fills up before
/// the barrier is called — see the comment on
/// `a_terminated_process_drains_before_the_periodic_barrier_would` in
/// `tests/integration.rs`. Neither exception applies here: SIGKILL does not
/// run destructors, and this test's volume (a handful of JSON lines) is far
/// from filling the default `BufWriter` enough for it to flush itself —
/// which means here "the line is visible in the file" does still mean "a
/// real fsync from the honest timer branch has already run", and killing
/// after that point cannot catch task 4's mutation (step 3: acknowledging
/// the position BEFORE the barrier). We must wait for exactly what that
/// mutation fakes — the slot's `confirmed_flush_lsn`, not a side effect in
/// the file.
#[tokio::test(flavor = "multi_thread")]
async fn no_committed_row_is_lost_across_a_hard_restart() {
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let out = std::env::temp_dir().join(format!("pgcdc-restart-{}.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&out);

    let spawn = |path: &std::path::Path| {
        common::KillOnDrop(
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
                    path.to_str().unwrap(),
                    // A short barrier so the test doesn't wait long.
                    "--ack-interval-ms",
                    "100",
                ])
                .spawn()
                .expect("spawn the binary"),
        )
    };

    // First run: rows 1..5.
    let mut child = spawn(&out);
    for id in 1..=5 {
        client
            .execute(
                "INSERT INTO users VALUES ($1, 'x', NULL, NULL)",
                &[&(id as i64)],
            )
            .await
            .unwrap();
    }
    let target = current_wal_lsn(&client).await;
    common::wait_for_slot_at_least(&client, "pgcdc_slot", target).await;

    // Kill hard: SIGKILL, not SIGTERM — the process is not given a chance to finish anything.
    child.kill().expect("kill");
    let _ = tokio::task::spawn_blocking(move || child.wait())
        .await
        .unwrap();

    // While we're gone — more rows.
    for id in 6..=10 {
        client
            .execute(
                "INSERT INTO users VALUES ($1, 'x', NULL, NULL)",
                &[&(id as i64)],
            )
            .await
            .unwrap();
    }
    let target = current_wal_lsn(&client).await;

    // The second run appends to the same file.
    let mut child = spawn(&out);
    common::wait_for_slot_at_least(&client, "pgcdc_slot", target).await;
    child.kill().expect("kill");
    let _ = tokio::task::spawn_blocking(move || child.wait())
        .await
        .unwrap();

    let text = std::fs::read_to_string(&out).expect("read the output");
    let ids = collect_ids(&text);
    for id in 1..=10 {
        assert!(
            ids.contains(&id.to_string()),
            "row {id} lost; output contains: {ids:?}"
        );
    }

    let _ = std::fs::remove_file(&out);
}

/// The server's current WAL write position. A lower bound on what the
/// process will have to acknowledge to the slot once it catches up with the
/// already-committed rows (the same idiom as the reconnect test in
/// `tests/integration.rs`, reused here).
async fn current_wal_lsn(client: &tokio_postgres::Client) -> pgcdc::lsn::Lsn {
    let text: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .unwrap()
        .get(0);
    common::parse_lsn(&text).expect("parse the LSN")
}

/// Collects the values of the `id` column from all JSONL rows. An
/// incomplete last line is ignored: the process may have been killed
/// mid-write.
fn collect_ids(text: &str) -> Vec<String> {
    text.lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|v| v["after"]["id"].as_str().map(|s| s.to_string()))
        .collect()
}
