mod common;

use std::time::Duration;

use pgcdc::config::{Config, DatabaseUrl, OutputKind};
use pgcdc::error::PgcdcError;
use pgcdc::lsn::Lsn;
use pgcdc::sink::{Durability, Sink};
use pgcdc::transaction::Transaction;
use tokio::sync::mpsc;

/// Копит транзакции в канал, чтобы тест мог их дождаться. Наибольшая
/// принятая позиция с прошлого барьера хранится отдельно: возврат
/// `write_transaction` не означает durable (это и есть смысл Task 2).
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
    async fn flush(&mut self) -> Result<Option<Lsn>, PgcdcError> {
        // Барьер сюда никогда не доходит: write_transaction всегда падает первым.
        // Честная реализация — тоже падать, а не молча заявлять durable.
        Err(PgcdcError::Sink("deliberate test failure".into()))
    }
}

/// Принимает запись успешно, но барьер у него всегда падает. Существует
/// отдельно от `FailingSink`, потому что тот падает внутри `write_transaction`
/// и никогда не доходит до кода, который отмечает durable, — так что он не
/// охраняет разделение «запись прошла» / «барьер прошёл», ради которого
/// затевалась задача 2 (review Task 2, round 1, F2).
struct FlushFailsSink;

#[async_trait::async_trait]
impl Sink for FlushFailsSink {
    fn durability(&self) -> Durability {
        Durability::Fsync
    }
    async fn write_transaction(&mut self, _tx: &Transaction) -> Result<(), PgcdcError> {
        Ok(())
    }
    async fn flush(&mut self) -> Result<Option<Lsn>, PgcdcError> {
        Err(PgcdcError::Sink("deliberate barrier failure".into()))
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

#[tokio::test(flavor = "multi_thread")]
async fn insert_travels_end_to_end_and_arrives_as_one_event() {
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let (tx_send, mut tx_recv) = mpsc::unbounded_channel();
    let cfg = config(&conn);
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(cfg, Box::new(ChannelSink(tx_send, None))).await
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

#[tokio::test(flavor = "multi_thread")]
async fn postgres_does_not_send_rolled_back_transactions() {
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
        pgcdc::postgres::replication::run(cfg, Box::new(ChannelSink(tx_send, None))).await
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

#[tokio::test(flavor = "multi_thread")]
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

#[tokio::test(flavor = "multi_thread")]
async fn barrier_failure_stops_us_before_the_slot_advances() {
    // Дополняет sink_failure_stops_us_before_the_slot_advances: та проверяет
    // отказ ВНУТРИ write_transaction, этот — отказ барьера ПОСЛЕ успешной
    // записи. Без этого теста ветка кода, которую добавила задача 2 (durable
    // отмечается только по возврату flush, а не write_transaction), ничем не
    // защищена: FailingSink никогда её не достигает (review Task 2, round 1, F2).
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
        pgcdc::postgres::replication::run(cfg, Box::new(FlushFailsSink)).await
    });

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
        "барьер, который не может подтвердить, — фатальная ошибка"
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
        "слот не должен был сдвинуться: барьер не довёл запись до диска"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stdout_stays_json_only_when_the_real_binary_hits_a_fatal_error() {
    // I2: "JSONL на stdout, логи на stderr" — поведенчески верно, но ничего не
    // упало бы при регрессии. `--help` для этого не годится: он не проходит ни
    // одной ветки, которая логирует. Отсутствующий слот — детерминированный и
    // быстрый способ гарантированно попасть в ветку логирования: guard падает
    // до первого события репликации, без ожидания INSERT и без гонок по времени.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    // Слот намеренно не создаём.

    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"));
    cmd.env("PGCDC_DATABASE_URL", &conn)
        .env("PGCDC_PUBLICATION", "pgcdc_pub")
        .env("PGCDC_SLOT", "pgcdc_slot")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    let output = tokio::time::timeout(
        Duration::from_secs(20),
        tokio::task::spawn_blocking(move || cmd.output()),
    )
    .await
    .expect("бинарь должен завершиться за 20 секунд")
    .expect("spawn_blocking join")
    .expect("запуск pgcdc");

    assert!(
        !output.status.success(),
        "отсутствующий слот обязан быть фатальным для реального бинаря"
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout должен быть валидным UTF-8");
    assert!(
        stdout.is_empty(),
        "stdout обязан остаться пустым при фатальной ошибке старта, получили: {stdout:?}"
    );
    // Пустых строк тут не будет, но это ассерт-на-будущее: если когда-нибудь в
    // stdout протечёт нежурнальный текст, он не пройдёт как JSON и тест покраснеет.
    for line in stdout.lines() {
        serde_json::from_str::<serde_json::Value>(line)
            .unwrap_or_else(|e| panic!("строка stdout не является JSON: {line:?}: {e}"));
    }

    let stderr = String::from_utf8(output.stderr).expect("stderr должен быть валидным UTF-8");
    assert!(
        stderr.contains("slot") || stderr.contains("слот"),
        "stderr должен сообщить об отсутствующем слоте, получили: {stderr}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn libpq_connection_string_is_rejected_without_echoing_the_password() {
    // Отвергать такую строку мы научились в этапе 1, но clap печатал её целиком
    // в тексте своей ошибки. Здесь проверяется именно отсутствие эха.
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
        .expect("запустить бинарь");

    assert!(
        !output.status.success(),
        "невалидный URL обязан давать ненулевой код"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stdout.is_empty(), "stdout несёт только полезную нагрузку");
    assert!(
        !stderr.contains("SUPERSECRET_XYZZY"),
        "пароль не должен появляться в stderr: {stderr}"
    );
    assert!(
        stderr.contains("postgres://"),
        "сообщение должно подсказывать нужную форму: {stderr}"
    );
}

#[tokio::test(flavor = "multi_thread")]
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

#[tokio::test(flavor = "multi_thread")]
async fn changing_a_key_column_produces_a_key_only_before_image() {
    // Единственная форма UPDATE, которой нет в замороженном захвате: тег 'K'.
    // Юнит-тест проверяет её синтетическими байтами, то есть нашим пониманием
    // разметки; здесь её выдаёт настоящий PostgreSQL.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::setup_items_table(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let (tx_send, mut tx_recv) = mpsc::unbounded_channel();
    let cfg = config(&conn);
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(cfg, Box::new(ChannelSink(tx_send, None))).await
    });

    client
        .execute("INSERT INTO items VALUES (10, 'Widget', 5)", &[])
        .await
        .unwrap();
    client
        .execute("UPDATE items SET id = 11 WHERE id = 10", &[])
        .await
        .unwrap();

    // Первая транзакция — INSERT, вторая — интересующий нас UPDATE.
    let _insert_tx = tokio::time::timeout(Duration::from_secs(20), tx_recv.recv())
        .await
        .expect("insert должен приехать")
        .expect("канал закрыт");
    let update_tx = tokio::time::timeout(Duration::from_secs(20), tx_recv.recv())
        .await
        .expect("update должен приехать")
        .expect("канал закрыт");

    let ev = &update_tx.changes[0];
    let json = serde_json::to_value(ev).unwrap();
    assert_eq!(json["operation"], "update");
    assert_eq!(
        json["before_kind"], "key",
        "ключ менялся — сервер шлёт тег 'K'"
    );
    assert_eq!(json["before"]["id"], "10", "старое значение ключа");
    assert!(
        json["before"].get("title").is_none(),
        "неключевые колонки сервер не прислал, и в before их быть не должно: {json}"
    );
    assert_eq!(json["after"]["id"], "11");
    assert_eq!(json["after"]["title"], "Widget");

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_change_resends_relation_and_the_cache_takes_the_new_one() {
    // pgoutput пересылает RELATION при инвалидации записи — например после DDL.
    // Захват этапа 0 такого случая не содержит, а замена записи в кэше
    // от этого поведения прямо зависит.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::setup_items_table(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let (tx_send, mut tx_recv) = mpsc::unbounded_channel();
    let cfg = config(&conn);
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(cfg, Box::new(ChannelSink(tx_send, None))).await
    });

    client
        .execute("INSERT INTO items VALUES (1, 'before ddl', 1)", &[])
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(20), tx_recv.recv())
        .await
        .expect("первый insert")
        .expect("канал закрыт");
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
        .expect("второй insert")
        .expect("канал закрыт");
    let after = second.changes[0].after.as_ref().unwrap();
    assert_eq!(after.len(), 4, "кэш обязан был принять новую схему");
    assert_eq!(after.get("note").unwrap(), "hello");

    handle.abort();
}
