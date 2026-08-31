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

/// Принимает запись успешно, но барьер падает всякий раз, когда есть что
/// проваливать. Существует отдельно от `FailingSink`, потому что тот падает
/// внутри `write_transaction` и никогда не доходит до кода, который отмечает
/// durable, — так что он не охраняет разделение «запись прошла» / «барьер
/// прошёл», ради которого затевалась задача 2 (review Task 2, round 1, F2).
///
/// Task 4 review, round 1, F1: раньше `flush` падал БЕЗУСЛОВНО, включая
/// пустой тик без единой записи. После задачи 4 барьер достижим и на
/// холостых тиках (в этом весь смысл таймера), поэтому такой дублёр мог
/// оборвать `run()` ожидаемой ошибкой ещё до первого `write_transaction` —
/// тест проходил бы, ничего не проверив. Форма — как у остальных sink'ов:
/// `write_transaction` запоминает принятую позицию, `flush` при пустом
/// накопителе честно отвечает `Ok(None)` (контракт трейта и уже
/// существующий юнит-тест для других дублёров), а падает только тогда,
/// когда действительно было что подтверждать.
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
        // Позицию нарочно не забираем: барьер провалился, данные не стали
        // durable, и повторная попытка обязана видеть ту же ожидающую
        // позицию, а не молча её терять.
        Err(PgcdcError::Sink("deliberate barrier failure".into()))
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
        pgcdc::postgres::replication::run(cfg, Box::new(FlushFailsSink(None))).await
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
async fn file_output_without_a_path_is_rejected_by_the_binary() {
    // Проверяется поведение бинаря целиком: clap разбирает конфигурацию,
    // а решение об обязательности пути принимает main.
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
        .expect("запустить бинарь");
    assert!(!output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.is_empty(), "stdout несёт только полезную нагрузку");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--output-path"),
        "сообщение называет недостающий флаг: {stderr}"
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

#[tokio::test(flavor = "multi_thread")]
async fn several_transactions_are_not_lost_and_the_slot_catches_up() {
    // Название раньше обещало проверку группировки подтверждений, но
    // ChannelSink отдаёт durable-позицию на КАЖДОМ вызове flush независимо от
    // того, сколько транзакций накопилось, — этому дублёру групповое и
    // потранзакционное подтверждение неотличимы. Реально проверяемо здесь
    // только то, что группировка не теряет транзакции и что слот в итоге
    // догоняет последнюю доведённую позицию (review round 2, F3).
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let (tx_send, mut tx_recv) = mpsc::unbounded_channel();
    let mut cfg = config(&conn);
    cfg.ack_interval_ms = 500;
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(cfg, Box::new(ChannelSink(tx_send, None))).await
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
            .expect("все пять транзакций должны приехать")
            .expect("канал закрыт");
        seen.push(tx.end_lsn);
    }
    assert_eq!(seen.len(), 5, "группировка не теряет транзакции");

    // Слот обязан догнать последнюю доведённую позицию.
    let last = seen.last().copied().unwrap();
    let confirmed = common::wait_for_slot_at_least(&client, "pgcdc_slot", last).await;
    assert!(
        confirmed >= last,
        "слот догнал последнюю группу: {confirmed} < {last}"
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn slot_advances_while_the_publication_is_idle() {
    // Классическая проблема: пишут в таблицы вне публикации, нам не приходит
    // ни одного события, слот стоит, WAL растёт. Продвижение по keepalive
    // существует ровно ради этого.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    // Таблица ВНЕ публикации: записи в неё двигают WAL, но не порождают событий.
    client
        .batch_execute("CREATE TABLE public.noise (id BIGINT PRIMARY KEY, payload TEXT)")
        .await
        .unwrap();

    let (tx_send, _tx_recv) = mpsc::unbounded_channel();
    let cfg = config(&conn);
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(cfg, Box::new(ChannelSink(tx_send, None))).await
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
    let target = common::parse_lsn(&target).expect("позиция сервера");

    let confirmed = common::wait_for_slot_at_least(&client, "pgcdc_slot", target).await;
    assert!(
        confirmed >= target,
        "слот обязан догнать сервер на простаивающей публикации: {confirmed} < {target}"
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn keepalive_does_not_advance_the_slot_past_an_unwritten_transaction() {
    // FlushFailsSink, не дублёр, который падает безусловно (review round 2,
    // F2): тот проваливал барьер и на ПУСТОМ тике тоже, обрывая run() ошибкой
    // до первого write_transaction — INSERT ниже не успевал случиться, и
    // ассерт «слот не сдвинулся» проходил бы, ничего не проверив про
    // keepalive-ветку. FlushFailsSink отвечает Ok(None) на пустой барьер и
    // падает только когда есть что подтверждать, так что запись реально
    // доходит до sink'а, а падает только барьер — и слот обязан стоять.
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
        pgcdc::postgres::replication::run(cfg, Box::new(FlushFailsSink(None))).await
    });

    client
        .execute("INSERT INTO users VALUES (1, 'Alice', NULL, NULL)", &[])
        .await
        .unwrap();

    let result = tokio::time::timeout(Duration::from_secs(20), handle)
        .await
        .expect("run должен завершиться, а не висеть")
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
    assert_eq!(before, after, "барьер не прошёл — слот не двигается");
}

#[tokio::test(flavor = "multi_thread")]
async fn file_output_binary_writes_durable_json_lines() {
    // Ветка `--output file` в main.rs ничем не покрыта: замени в match'e
    // FileSink на StdoutSink — ни один тест не покраснеет. FileSink — единственный
    // sink этапа, который честно обещает Fsync, и именно эта ветка бинаря его
    // не проверяет вовсе (review round 2, F7). Гоняем настоящий бинарь целиком:
    // CLI-разбор, guard, цикл репликации и файловый sink с fsync.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let mut path = std::env::temp_dir();
    path.push(format!("pgcdc-integration-{}.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&path);

    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"))
        .env("PGCDC_DATABASE_URL", &conn)
        .env("PGCDC_PUBLICATION", "pgcdc_pub")
        .env("PGCDC_SLOT", "pgcdc_slot")
        .env("PGCDC_OUTPUT", "file")
        .env("PGCDC_OUTPUT_PATH", &path)
        .env("PGCDC_ACK_INTERVAL_MS", "50")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("запустить бинарь");

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
            panic!("файл не получил ни одной строки за 20с");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_file(&path);

    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 1, "один INSERT — одна строка: {text:?}");
    let json: serde_json::Value =
        serde_json::from_str(lines[0]).expect("строка файла — валидный JSON");
    assert_eq!(json["operation"], "insert");
    assert_eq!(json["table"], "users");
    assert_eq!(json["after"]["id"], "1");
    assert_eq!(json["after"]["name"], "Alice");
}
