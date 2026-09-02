mod common;

use pgcdc::error::PgcdcError;
use pgcdc::postgres::guard::preflight_slot;

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
