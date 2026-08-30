mod common;

use pgcdc::error::PgcdcError;
use pgcdc::postgres::guard::preflight_cold_start;

#[tokio::test(flavor = "multi_thread")]
async fn cold_start_fails_when_the_slot_is_missing_and_does_not_create_it() {
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;

    let err = preflight_cold_start(&conn, "pgcdc_slot").await.unwrap_err();
    assert!(matches!(err, PgcdcError::SlotMissing { .. }));
    assert!(err.is_fatal(), "отсутствующий слот — фатальная ошибка");

    // Главное: guard не должен был создать слот в качестве побочного эффекта.
    let rows = client
        .query("SELECT slot_name FROM pg_replication_slots", &[])
        .await
        .unwrap();
    assert!(
        rows.is_empty(),
        "guard не создаёт слот, это маскировало бы потерю данных"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cold_start_returns_slot_positions_when_the_slot_exists() {
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let info = preflight_cold_start(&conn, "pgcdc_slot").await.unwrap();
    assert!(
        info.confirmed_flush_lsn.is_some(),
        "у свежего слота позиция уже есть"
    );
    assert!(info.restart_lsn.is_some());
}
