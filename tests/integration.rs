mod common;

use std::time::Duration;

use pgcdc::config::{Config, DatabaseUrl, OutputKind};
use pgcdc::error::PgcdcError;
use pgcdc::sink::{Durability, Sink};
use pgcdc::transaction::Transaction;
use tokio::sync::mpsc;

/// Копит транзакции в канал, чтобы тест мог их дождаться.
struct ChannelSink(mpsc::UnboundedSender<Transaction>);

#[async_trait::async_trait]
impl Sink for ChannelSink {
    fn durability(&self) -> Durability {
        Durability::Fsync
    }
    async fn write_transaction(&mut self, tx: &Transaction) -> Result<(), PgcdcError> {
        self.0.send(tx.clone()).expect("send");
        Ok(())
    }
}

/// Всегда падает — проверяет, что подтверждение не уходит вперёд sink'а.
struct FailingSink;

#[async_trait::async_trait]
impl Sink for FailingSink {
    fn durability(&self) -> Durability {
        Durability::Fsync
    }
    async fn write_transaction(&mut self, _tx: &Transaction) -> Result<(), PgcdcError> {
        Err(PgcdcError::Sink("deliberate test failure".into()))
    }
}

fn config(conn: &str) -> Config {
    Config {
        database_url: DatabaseUrl::new(conn.to_string()),
        publication: "pgcdc_pub".into(),
        slot: "pgcdc_slot".into(),
        output: OutputKind::Stdout,
        max_transaction_events: 100_000,
    }
}

#[tokio::test]
async fn insert_travels_end_to_end_and_arrives_as_one_event() {
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let (tx_send, mut tx_recv) = mpsc::unbounded_channel();
    let cfg = config(&conn);
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(cfg, Box::new(ChannelSink(tx_send))).await
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
        .expect("транзакция должна приехать за 20 секунд")
        .expect("канал закрыт");

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

    // Ядро контракта на LSN: PostgreSQL должен подтвердить ровно end_lsn, а не
    // commit_lsn (они различаются на фиксированные 0x30 байт — DECISIONS/бриф
    // Task 6). Опрашиваем в цикле: send_feedback() уходит из цикла репликации
    // асинхронно относительно этого запроса, поэтому однократное чтение гонится
    // с нашим же подтверждением.
    let expected_end = tx.end_lsn.to_string();
    let expected_commit = tx.commit_lsn.to_string();
    assert_ne!(
        expected_end, expected_commit,
        "end_lsn и commit_lsn обязаны отличаться, иначе проверка равенства ничего не доказывает"
    );

    let mut confirmed = String::new();
    for _ in 0..100 {
        confirmed = client
            .query_one(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 'pgcdc_slot'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        if confirmed == expected_end {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        confirmed, expected_end,
        "PostgreSQL должен был подтвердить end_lsn транзакции"
    );
    assert_ne!(
        confirmed, expected_commit,
        "подтверждена не должна быть позиция начала COMMIT-записи"
    );

    handle.abort();
}

#[tokio::test]
async fn nothing_is_emitted_for_a_rolled_back_transaction() {
    // Проверяет НАШЕ понимание протокола, а не наш код: logical decoding
    // физически не отдаёт откаченные транзакции. Если тест покраснеет,
    // значит мир устроен не так, как мы думаем.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let (tx_send, mut tx_recv) = mpsc::unbounded_channel();
    let cfg = config(&conn);
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(cfg, Box::new(ChannelSink(tx_send))).await
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
        .expect("таймаут")
        .expect("канал закрыт");

    // Первая приехавшая транзакция — та, что после отката.
    assert_eq!(tx.changes.len(), 1);
    let json = serde_json::to_value(&tx.changes[0]).unwrap();
    assert_eq!(
        json["after"]["id"], "1",
        "откаченная строка не должна приехать"
    );

    handle.abort();
}

#[tokio::test]
async fn sink_failure_stops_us_before_the_slot_advances() {
    // Ядро контракта: подтверждение не уходит вперёд того, что записал sink.
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
    let handle =
        tokio::spawn(
            async move { pgcdc::postgres::replication::run(cfg, Box::new(FailingSink)).await },
        );

    client
        .execute("INSERT INTO users VALUES (1, 'Alice', NULL, NULL)", &[])
        .await
        .unwrap();

    let result = tokio::time::timeout(Duration::from_secs(20), handle)
        .await
        .expect("run должен завершиться, а не висеть")
        .expect("join");
    let err = result.unwrap_err();
    assert!(matches!(err, PgcdcError::Sink(_)), "получили {err:?}");
    assert!(
        err.is_fatal(),
        "sink, который не может двигаться, — фатальная ошибка"
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
        "слот не должен был сдвинуться: sink ничего не записал"
    );
}

#[tokio::test]
async fn missing_slot_is_fatal_and_the_slot_is_not_created() {
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    // Слот намеренно НЕ создаём.

    let err = pgcdc::postgres::replication::run(config(&conn), Box::new(FailingSink))
        .await
        .unwrap_err();
    assert!(matches!(err, PgcdcError::SlotMissing { .. }));
    assert!(err.is_fatal());

    let rows = client
        .query("SELECT slot_name FROM pg_replication_slots", &[])
        .await
        .unwrap();
    assert!(
        rows.is_empty(),
        "слот не создан — иначе мы маскировали бы потерю данных"
    );
}
