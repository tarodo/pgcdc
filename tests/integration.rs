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

/// Зеркало `FlushFailsSink`: запись падает, а барьер (пустой) успешен. Нужен
/// отдельно от `FailingSink`, у которого падают ОБА метода — из-за этого
/// `FailingSink` не может отличить "проглоченный отказ записи" от "отказ
/// барьера": даже мутация, игнорирующая `Err` от `write_transaction`, всё
/// равно уронит `run()` на следующем же барьере (тот падает безусловно), и
/// тест на отказ приёмника прошёл бы зелёным не по той причине, по которой
/// заявлен (I4).
///
/// `write_transaction` здесь никогда не трогает `pending` — запись не
/// удалась, sink'у нечего было принять, и барьер это честно отражает через
/// `Ok(None)`, как и контракт трейта требует для пустого накопителя. Если
/// мутация в цикле репликации заменит `sink.write_transaction(&tx).await?`
/// на игнорирование результата, цикл продолжит идти как ни в чём не бывало:
/// `note_processed` сдвинется на позицию транзакции, которую sink на самом
/// деле не принял, а следующий барьер честно отчитается `Ok(None)` — ack
/// никогда не продвинется, `run()` никогда не вернёт ошибку, и процесс
/// зависнет там, где корректный код упал бы с фатальной ошибкой сразу после
/// первой же записи.
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
        // Ничего никогда не было принято (запись всегда падает раньше) —
        // барьеру нечего подтверждать, и он честно отвечает Ok(None), а не Err.
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
    // M6: `validate_reconnect_bounds()` было добавлено в `run()` в прошлом
    // раунде, но без теста, бьющего именно по вызову внутри `run()`, — сам
    // по себе вызов можно было бы стереть, и весь набор остался бы зелёным
    // (юнит-тесты в `config.rs` проверяют только сам метод в изоляции). Проверка
    // стоит ДО preflight и до всякого подключения, поэтому контейнер не нужен
    // вовсе: адрес ниже никогда не будет резолвиться или использоваться, если
    // вызов на месте.
    let mut cfg = config("postgres://u:p@127.0.0.1:1/db");
    cfg.reconnect_initial_ms = 5000;
    cfg.reconnect_max_ms = 1000;

    // Если вызов удалить, run() уйдёт в preflight на недостижимый адрес и
    // будет ретраить внутри таймаута ниже — таймаут истечёт, и `expect`
    // запаникует: мутация ловится, а не проходит незамеченной.
    let err = tokio::time::timeout(
        Duration::from_secs(5),
        pgcdc::postgres::replication::run(
            cfg,
            Box::new(FailingSink),
            std::sync::Arc::new(pgcdc::metrics::Metrics::new()),
        ),
    )
    .await
    .expect("проверка границ обязана вернуть ошибку немедленно, не дожидаясь сети")
    .unwrap_err();
    assert!(
        matches!(err, PgcdcError::InvalidReconnectBounds { .. }),
        "получили {err:?}"
    );
    assert!(err.is_fatal());
}

#[tokio::test(flavor = "multi_thread")]
async fn sigterm_is_honored_while_stuck_reconnecting_to_a_dead_port() {
    // I1: раньше сигнал, пришедший пока БД недостижима, не замечался вовсе —
    // ни внешний цикл реконнекта не читал флаг завершения, ни пауза бэкоффа
    // не была прерываемой. Ревьюер воспроизвёл это вживую: бинарь на мёртвом
    // порту, SIGTERM, жив ещё пять секунд спустя, и только SIGKILL что-то
    // менял. Порт 1 никогда не слушает ни на одной обычной машине, поэтому
    // preflight будет валиться немедленно и предсказуемо — контейнер
    // Postgres здесь не нужен вовсе.
    let mut child = common::KillOnDrop(
        std::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"))
            .args([
                "--database-url",
                "postgres://u:p@127.0.0.1:1/db",
                "--publication",
                "pgcdc_pub",
                "--slot",
                "pgcdc_slot",
                // Короткие и близкие друг к другу границы бэкоффа: несколько
                // попыток реконнекта укладываются в секунды, а не в полминуты
                // потолка по умолчанию.
                "--reconnect-initial-ms",
                "50",
                "--reconnect-max-ms",
                "3000",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("запустить бинарь"),
    );

    let stderr = child.stderr.take().expect("stderr был запрошен как piped");
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

    // Доказательство, а не догадка: ждём, пока бэкофф не долезет до потолка
    // (50 → 100 → 200 → 400 → 800 → 1600 → 3000 — семь строк "reconnecting").
    // Раньше порог был "минимум две", но при потолке, равном интервалу опроса
    // (200мс), пауза этого размера всегда укладывается в один кусок нарезки —
    // нарезанная и цельная паузы неотличимы. Проверка ниже обязана застать
    // СИГНАЛ ровно тогда, когда в работе паузa вплоть до потолка (3000мс,
    // что в разы больше SHUTDOWN_POLL_INTERVAL) — только так тест различает
    // нарезку и цельный sleep(delay) (round 1 стадии 5, F1).
    //
    // Бюджет — 20с (400×50мс), а не прежние 10с (round 1 review, F3): сама
    // обязательная часть ожидания (сумма пауз до седьмого ретрая) — уже
    // ~3.15с, и на локально нагруженной машине этот набор тестов измеренно
    // гуляет 2.4x (9.5–22.4с по прогонам) — 10с оставляли запас всего
    // втрое, а не в шестьдесят раз, как было при пороге "две". 20с ничего
    // не стоят в зелёном прогоне и убирают историческую нестабильность.
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
        "не увидели семь ретраев (бэкофф до потолка) за 20 секунд, видели: {:?}",
        lines.lock().unwrap()
    );

    // SIGTERM посреди бесконечного ретрая на мёртвом порту: буферизованных
    // данных на этом пути нет — сессия так ни разу и не открылась, — поэтому
    // единственно верный исход - быстрый выход с кодом 0, а не зависание до
    // SIGKILL.
    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };

    // Опрос через try_wait(), а не блокирующий wait() внутри spawn_blocking:
    // если этот тест ловит регресс I1 и процесс НЕ реагирует на SIGTERM,
    // блокирующий wait() повис бы навсегда, а Drop рантайма tokio ждёт
    // завершения именно blocking-задач — тест повесил бы весь тестовый
    // бинарь вместо того, чтобы просто покраснеть. try_wait() поток не
    // блокирует, так что таймаут ниже отрабатывает в обоих случаях.
    //
    // Бюджет — 1.5с, а не прежние 5с: сигнал только что застал паузу
    // длиной вплоть до 3000мс в работе. Нарезанная пауза замечает флаг не
    // позже чем через SHUTDOWN_POLL_INTERVAL (200мс) — 1.5с оставляют ей
    // щедрый запас; а вот цельный sleep(delay) обязан продержать процесс
    // почти все оставшиеся ~3с и в этот бюджет не уложится — иначе смысла
    // в его сужении нет, тест снова не отличит нарезку от целого sleep.
    let mut status = None;
    for _ in 0..30 {
        if let Ok(Some(s)) = child.try_wait() {
            status = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let status =
        status.expect("SIGTERM обязан остановить процесс за 1.5 секунды, а не только SIGKILL");
    assert_eq!(
        status.code(),
        Some(0),
        "нечего было доводить до барьера — реконнект на недостижимой БД обязан завершаться нулём"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sigterm_in_the_last_backoff_chunk_needs_the_top_of_loop_check() {
    // Round 1 review, F1/F2: нарезка бэкоффа проверяет флаг ПЕРЕД каждым
    // куском и ни разу ПОСЛЕ последнего — сигнал, попавший именно в
    // последний кусок, до неё не доходит и ловится только проверкой в
    // начале СЛЕДУЮЩЕГО прохода внешнего цикла. Сосед этого теста (мёртвый
    // порт) эту проверку не пин: против отказанного порта "лишняя попытка
    // подключения", которую эта проверка экономит, стоит меньше
    // миллисекунды — неотличимо от нуля на фоне любого разумного бюджета.
    //
    // Здесь вместо мёртвого порта — пир, который ПРИНИМАЕТ TCP-соединение
    // и молча держит его ~3с перед тем, как сбросить: preflight падает не
    // мгновенно и не никогда, а на ограниченной, но реальной задержке.
    // Без проверки в начале прохода эта задержка проявляется ПОЛНОСТЬЮ —
    // именно то время, которое новая документация `spawn_shutdown_listener`
    // называет неограниченным.
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
                // Никогда не говорит протокол Postgres: просто держит
                // сокет, чтобы clientский connect() заблокировался на
                // предсказуемые ~3с, а не упал мгновенно и не завис навечно.
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
                // Начальная и максимальная границы равны: каждая пауза
                // бэкоффа — ровно 1с (5 кусков по 200мс), без роста, так
                // что момент сигнала внутри паузы предсказуем.
                "--reconnect-initial-ms",
                "1000",
                "--reconnect-max-ms",
                "1000",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("запустить бинарь"),
    );

    let stderr = child.stderr.take().expect("stderr был запрошен как piped");
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

    // Ждём вторую строку "reconnecting": первая попытка и первая пауза уже
    // позади, вторая попытка провалилась, и сейчас в работе вторая пауза
    // (ровно 1с). Бюджет — 15с: два подключения к держащему пиру (~3с
    // каждое) плюс пауза между ними (~1с) плюс запас.
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
        "не увидели вторую строку reconnecting за 15 секунд, видели: {:?}",
        lines.lock().unwrap()
    );

    // Пауза после второго ретрая — ровно 1с, нарезана на 5 кусков по
    // 200мс: границы кусков — 0, 200, 400, 600, 800, 1000. Последний кусок
    // (800–1000) — тот самый, который нарезка не перепроверяет после
    // своего конца; сигнал должен попасть именно туда.
    //
    // Round 2 review, F5: наше "сейчас" всегда ПОЗЖЕ истинного момента
    // строки лога — опрос раз в 50мс и было бы. Значит смещение может
    // только УВЕЛИЧИТЬ фактическую точку попадания внутрь паузы, никогда
    // не уменьшить её. Отсюда: целиться нужно ближе к началу окна
    // (800мс), а не к его концу (900мс, как было) — это ничего не стоит
    // на зелёной стороне (нарезанному коду всё равно, сколько именно
    // последнего куска осталось — 200мс или 20мс, лишь бы после конца
    // последнего куска, а не раньше) и покупает запас на красной стороне
    // (перелёт за 1000мс значил бы попадание уже в СЛЕДУЮЩУЮ попытку
    // подключения, которая блокирует одинаковые ~3с что на верном, что на
    // мутированном коде, — ложнокрасный результат даже без мутации).
    //
    // Цель — 820мс: +20мс над нижней границей (800) как страховка на
    // случай, если предположение о границах кусков хоть немного неточно;
    // до верхней границы (1000) остаётся 180мс на задержку опроса и
    // диспетчеризацию задачи — было 100мс при цели 900мс. Бюджет выхода
    // ниже расширен вместе с этим (см. комментарий там).
    let signal_target = Duration::from_millis(820);
    tokio::time::sleep(signal_target).await;
    let signal_sent_at = std::time::Instant::now();
    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };

    // Бюджет — 1с (было 600мс): при цели 820мс худший случай для
    // нарезанного кода — приземлиться сразу на границе 820мс (нулевая
    // задержка опроса) и досидеть остаток последнего куска, ~180мс, плюс
    // реакция проверки и доставка сигнала — с запасом до ~250–300мс.
    // 1с даёт этому кратный запас и остаётся втрое ниже задержки
    // держащего пира (~3с), так что мутированный путь однозначно не
    // укладывается.
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
        .expect("проверка в начале прохода обязана поймать сигнал из последнего куска паузы за 1с");
    assert_eq!(status.code(), Some(0), "нечего было доводить до барьера");
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

    // Ядро контракта на LSN: PostgreSQL должен подтвердить НЕ РАНЬШЕ end_lsn,
    // а не commit_lsn (они различаются на фиксированные 0x30 байт —
    // DECISIONS/бриф Task 6). Ждём "не раньше", а не точного совпадения:
    // idle-keepalive-advance (этап 3) может увести confirmed_flush_lsn дальше
    // end_lsn ещё до нашего опроса — под нагрузкой это ловилось примерно в
    // 20% прогонов (review Task 2, round 1, F5), и дело не в том, что
    // подтвердили что-то не то, а в том, что сервер продолжил собственную
    // WAL-активность в фоне (зафиксированный пример — 0x38 байт от одной
    // фоновой standby-snapshot-записи).
    //
    // Расплата: этот тест больше не различает "подтвердили ровно end_lsn" от
    // "подтвердили что-то ЗА него keepalive'ом" — под мутацию, отправляющую
    // в feedback commit_lsn вместо end_lsn, keepalive-ветка позже всё равно
    // дотащит слот выше end_lsn, и проверка `>=` этого не заметит.
    // Различить эти два случая может только наблюдение за НАШИМ собственным
    // подтверждением (acked), а не за позицией слота на сервере — то есть
    // потеряно здесь осознанно, а не по недосмотру. Ту дискриминацию, от
    // которой этот тест отказался, закрывает
    // `we_acknowledge_the_end_of_the_commit_record_not_its_start` в этом же
    // файле: он читает `metrics.last_acknowledged_lsn`, а не позицию слота,
    // и потому различает подмену end_lsn на commit_lsn там, где keepalive
    // всё равно увёл бы слот вперёд обоих вариантов.
    let expected_end = tx.end_lsn;
    assert_ne!(
        tx.end_lsn, tx.commit_lsn,
        "end_lsn и commit_lsn обязаны отличаться, иначе проверка ниже ничего не доказывает"
    );

    let confirmed = common::wait_for_slot_at_least(&client, "pgcdc_slot", expected_end).await;
    assert!(
        confirmed >= expected_end,
        "PostgreSQL должен был подтвердить хотя бы end_lsn транзакции: {confirmed} < {expected_end}"
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
async fn a_write_failure_stops_us_before_the_slot_advances_and_is_not_swallowed() {
    // I4: дополняет sink_failure_stops_us_before_the_slot_advances и
    // barrier_failure_stops_us_before_the_slot_advances дублёром, у которого
    // падает ТОЛЬКО запись, а барьер (пустой) успешен — WriteFailsSink,
    // зеркало FlushFailsSink. FailingSink не годится здесь: у него падают
    // ОБА метода, поэтому мутация "sink.write_transaction(&tx).await? →
    // игнорировать результат" всё равно уронила бы run() на следующем же
    // барьере, и sink_failure_stops_us_before_the_slot_advances прошёл бы
    // зелёным не по той причине, по которой заявлен — набор был слеп к
    // мутации того, что якобы покрывает.
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
            "run должен завершиться отказом записи, а не висеть — под мутацией, \
             проглатывающей Err от write_transaction, он висит вечно: цикл продолжает \
             читать WAL, но ack никогда не продвигается, потому что sink ничего не принял",
        )
        .expect("join");
    let err = result.unwrap_err();
    assert!(matches!(err, PgcdcError::Sink(_)), "получили {err:?}");
    assert!(
        err.is_fatal(),
        "sink, который не может писать, — фатальная ошибка"
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
        "слот не должен был сдвинуться: sink ни разу не принял запись"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn we_acknowledge_the_end_of_the_commit_record_not_its_start() {
    // Перенесено с этапа 3. Раньше это проверялось по позиции слота на точное
    // равенство, но продвижение слота по keepalive сделало равенство
    // недостижимым: фоновая запись WAL законно уводит слот дальше, и
    // ослабленная проверка «не меньше» перестала различать подмену
    // end_lsn на commit_lsn — keepalive увёл бы слот за end_lsn в обоих
    // случаях. Счётчик читает НАШЕ решение, а не состояние сервера,
    // и потому различает.
    //
    // M5 (review round after task 4): это пин-тест плотины acknowledge_durable
    // (src/postgres/replication.rs) и Transaction::end_lsn (src/transaction.rs)
    // через ChannelSink — тестового дублёра приёмника, объявленного в этом же
    // файле, который сам кладёт нужную позицию (`self.1 = Some(tx.end_lsn)`).
    // Он НЕ ловит мутацию «боевые приёмники (FileSink/StdoutSink) отдают
    // начало вместо конца» — ChannelSink её попросту не касается. Ту мутацию
    // ловят `flush_reports_the_last_accepted_position_then_clears_it`
    // (src/sink/file.rs) и `a_second_flush_right_after_the_first_reports_nothing_new`
    // (src/sink/stdout.rs) — обе используют фикстуру с end_lsn ≠ commit_lsn и
    // сверяют возврат `flush()` на точное равенство. Проверено мутацией
    // руками: подмена `tx.end_lsn` на `tx.commit_lsn` в обоих боевых sink'ах
    // красит ровно эти два юнит-теста и оставляет этот тест зелёным.
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
        .expect("транзакция должна приехать")
        .expect("канал закрыт");

    // Ждём, пока счётчик догонит: подтверждение уходит из барьера по таймеру.
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
        "подтверждаем end_lsn транзакции, а не что-то ещё"
    );
    assert_ne!(
        acked, tx.commit_lsn.0,
        "commit_lsn указывает на начало записи коммита — рестарт перечитал бы её"
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_servers_confirmed_position_never_races_ahead_of_what_we_acknowledged() {
    // I3: DECISIONS Q25(2) запрещает пять вызовов `pg_walstream`, ведущих в
    // `recover_connection`, именно потому, что они рестартуют поток с
    // ПРИНЯТОЙ позицией, а не с ДУРАБЛЬНОЙ. Тест выше
    // (we_acknowledge_the_end_of_the_commit_record_not_its_start) читает
    // НАШЕ решение через `metrics.last_acknowledged_lsn`, а не то, что
    // реально ушло на провод — шаг "решение → провод" остаётся слепым: подмени
    // `acked` на `received`/`processed` внутри `acknowledge_durable` (в
    // вызовах `stream.shared_lsn_feedback.update_flushed_lsn`/
    // `update_applied_lsn`), и весь набор остаётся зелёным, потому что
    // `metrics.set_last_acknowledged_lsn` в той же функции эту подмену не
    // видит вовсе.
    //
    // Сценарий (зонд ревьюера, сделанный тестом). `acknowledge_durable`
    // зовётся ТОЛЬКО когда барьеру есть что подтверждать (`sink.flush()`
    // вернул `Some`) — единственная маленькая транзакция стала бы пустышкой:
    // первый же тик барьера подтвердил бы её почти сразу, задолго до того,
    // как следующая, большая транзакция вообще начнёт стримиться, и
    // подмена в `acknowledge_durable` не успела бы увидеть ничего, кроме
    // крошечного `received`. Поэтому фоновая задача непрерывно пишет
    // отдельные маленькие транзакции на протяжении всего теста — это даёт
    // МНОГО отдельных вызовов `acknowledge_durable`, и какой-то из них
    // гарантированно попадёт на момент, когда большая транзакция B уже
    // СТРИМИТСЯ (received растёт кадр за кадром), но ещё не разобрана до
    // конца (её COMMIT ещё не дошёл, write_transaction для неё ещё не
    // звали). В этот момент барьер подтверждает только маленькую фоновую
    // транзакцию — sink ничего не принимал сверх неё. Если бы на провод
    // уходила received/processed вместо acked, сервер увидел бы
    // `confirmed_flush_lsn` где-то в середине B — далеко впереди того, что
    // реально ушло sink'у.
    //
    // B — один INSERT...SELECT, один коммит: 300 строк по 200КБ в
    // TOAST-колонке `bio`, STORAGE EXTERNAL, без сжатия (≈60МБ). Решает не
    // полный объём B, а разрыв между двумя барьерами, который B успевает
    // создать, пока стримится, — а это калибруется временем разбора одной
    // строки, а не суммой всех строк. Прежняя версия теста гоняла 3000
    // строк (≈600МБ): впятеро дороже по времени и памяти, не покупая
    // большего разрыва (review round 2 after task 4 finale, P2) — при этом
    // же пороге (`max_gap_after_some_ack >= 1_000_000` ниже) наблюдаемый
    // разрыв остаётся десятками мегабайт, с запасом на порядки.
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

    // Фоновая задача: непрерывно пишет отдельные маленькие транзакции
    // (уникальные id, начиная с 500 000, вне диапазона B), пока тест не
    // велит ей остановиться. Каждая держит барьер занятым чем-то маленьким
    // на протяжении всего теста — включая момент, когда B в середине пути.
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

    // Ждём первую фоновую транзакцию — доказательство, что механизм вообще
    // работает, прежде чем запускать B.
    let first_bg = tokio::time::timeout(Duration::from_secs(20), tx_recv.recv())
        .await
        .expect("первая фоновая транзакция должна приехать")
        .expect("канал закрыт");
    assert_eq!(
        first_bg.changes.len(),
        1,
        "фоновая транзакция — одна строка"
    );

    // Большая транзакция B — без искусственной задержки.
    let client_b = common::connect(&conn).await;
    let insert_b = tokio::spawn(async move {
        client_b
            .execute(
                "INSERT INTO users SELECT gs, 'x', NULL, repeat('y', 200000) \
                 FROM generate_series(1000, 1299) AS gs",
                &[],
            )
            .await
            .expect("вставить большую транзакцию B");
    });

    // Опрашиваем СЕРВЕРНУЮ confirmed_flush_lsn против НАШЕЙ подтверждённой
    // позиции, пока B в пути, до тех пор, пока B не придёт целиком по каналу
    // (или пока не истечёт защитный таймаут). Транзакции короче 300 строк
    // (все фоновые) пролетают мимо этого цикла — интересна только B.
    // Порог — инвариант 1 (DECISIONS §1): `acked_lsn <= durable_lsn`, а то,
    // что реально ушло серверу, обязано совпадать с тем, что мы сами решили
    // подтвердить.
    let probe_client = common::connect(&conn).await;
    let mut max_gap_after_some_ack: i64 = -1;
    let tx_b = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            tokio::select! {
                recv = tx_recv.recv() => {
                    let tx = recv.expect("канал закрыт");
                    if tx.changes.len() >= 300 {
                        return tx;
                    }
                    // Фоновая транзакция — не то, что мы ждём, продолжаем опрос.
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
                    // Сверяем только ПОСЛЕ хотя бы одного подтверждения
                    // НАШИМ процессом (ours > 0): у свежесозданного слота
                    // confirmed_flush_lsn уже стоит на позиции создания (не
                    // на нуле), и до первого вызова acknowledge_durable
                    // сравнение с ours=0 ничего не проверяет про сам вызов.
                    if ours.0 == 0 {
                        continue;
                    }
                    assert!(
                        server_confirmed <= ours,
                        "сервер подтвердил {server_confirmed}, а мы реально признали только \
                         {ours} — позиция, ушедшая на провод, обогнала то, что мы сами решили \
                         подтвердить"
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
    .expect("транзакция B должна приехать за 30 секунд");

    bg_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    insert_b
        .await
        .expect("вставка B не должна была запаниковать");
    bg_task
        .await
        .expect("фоновая задача не должна была запаниковать");

    assert!(
        max_gap_after_some_ack >= 1_000_000,
        "тест обязан был застать received заметно впереди уже подтверждённого (замечено \
         {max_gap_after_some_ack} байт) — иначе большая транзакция передалась быстрее, чем \
         фоновые вставки создавали новые подтверждения, и окно гонки не было пройдено; \
         увеличьте размер B, участите фоновые вставки или уменьшите ack_interval_ms"
    );

    // Финальная сверка: слот в итоге догоняет B целиком, и не спорит с тем,
    // что мы реально подтвердили.
    let confirmed = common::wait_for_slot_at_least(&client, "pgcdc_slot", tx_b.end_lsn).await;
    assert!(
        confirmed >= tx_b.end_lsn,
        "слот обязан догнать B: {confirmed} < {}",
        tx_b.end_lsn
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failing_barrier_leaves_the_acknowledged_counter_at_zero() {
    // Формулировка Q23 дословно: «после sink-failure last_acknowledged_lsn
    // не сдвинулся». Раньше это можно было проверить только по слоту;
    // теперь видно и наше собственное решение.
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
        .expect("run должен завершиться, а не висеть")
        .expect("join");
    assert!(matches!(result.unwrap_err(), PgcdcError::Sink(_)));

    let snap = metrics.snapshot();
    assert_eq!(
        snap.last_acknowledged_lsn, 0,
        "барьер не прошёл — подтверждать нечего"
    );
    assert!(
        snap.transactions_total >= 1,
        "но транзакция была принята и посчитана"
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

    // tokio::process::Command, не std + spawn_blocking: с std, если таймаут
    // ниже срабатывает, отменяется только ожидание join-хендла — блокирующий
    // поток внутри cmd.output() и сам осиротевший дочерний процесс продолжают
    // жить. Теперь, когда процесс умеет ретраить реконнект бесконечно, такой
    // сирота никогда не завершается сам и вешает весь тестовый бинарь при
    // выключении рантайма (review Task 2, round 1, F9). kill_on_drop(true)
    // даёт нам хендл, который убивает процесс, если future отменяется по
    // таймауту: async Child, в отличие от std, дропается вместе с future.
    let mut cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"));
    cmd.env("PGCDC_DATABASE_URL", &conn)
        .env("PGCDC_PUBLICATION", "pgcdc_pub")
        .env("PGCDC_SLOT", "pgcdc_slot")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let child = cmd.spawn().expect("запустить pgcdc");

    let output = tokio::time::timeout(Duration::from_secs(20), child.wait_with_output())
        .await
        .expect("бинарь должен завершиться за 20 секунд")
        .expect("дождаться завершения pgcdc");

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

    // Слот сейчас классифицирован как немедленно фатальный, но run() теперь
    // умеет ретраить бесконечно на восстановимых ошибках — без таймаута этот
    // await навсегда повесил бы тест, если бы классификация когда-нибудь
    // сломалась (review Task 2, round 1, F9).
    let err = tokio::time::timeout(
        Duration::from_secs(20),
        pgcdc::postgres::replication::run(
            config(&conn),
            Box::new(FailingSink),
            std::sync::Arc::new(pgcdc::metrics::Metrics::new()),
        ),
    )
    .await
    .expect("run должен завершиться, а не висеть")
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
async fn slot_with_a_foreign_output_plugin_is_fatal_and_the_process_exits() {
    // C1 (review round after task 4): §20 п.14 требует ненулевой код выхода,
    // когда слот отсутствует ИЛИ непригоден. "Отсутствует" покрывает
    // missing_slot_is_fatal_and_the_slot_is_not_created выше;
    // "непригоден" не был покрыт вовсе — слот, на котором START_REPLICATION
    // получает явный отказ сервера, попадал в PgcdcError::Connection и уходил
    // в вечный реконнект без единого ненулевого кода выхода.
    //
    // Дешёвая ветка непригодности: слот создан с чужим output-плагином
    // (`test_decoding` вместо `pgoutput`). Существование слота проходит
    // preflight (он не смотрит на плагин), а START_REPLICATION отвечает
    // "option \"proto_version\" = \"1\" is unknown" (SQLSTATE 22023) — тот же
    // конверт ошибки (сервер ОТВЕТИЛ и отказал), что и настоящая инвалидация
    // по объёму удержанного WAL, но без необходимости прогонять гигабайты
    // WAL, чтобы её спровоцировать.
    //
    // Гоняем настоящий скомпилированный бинарь, а не run() in-process: пункт
    // 14 чек-листа — про код выхода ПРОЦЕССА, а не про Result библиотеки.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    client
        .query(
            "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
            &[&"pgcdc_slot"],
        )
        .await
        .expect("создать слот с чужим output-плагином");

    let mut cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"));
    cmd.env("PGCDC_DATABASE_URL", &conn)
        .env("PGCDC_PUBLICATION", "pgcdc_pub")
        .env("PGCDC_SLOT", "pgcdc_slot")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        // async Child, а не std + spawn_blocking: если таймаут ниже
        // сработает, kill_on_drop(true) действительно убьёт процесс, вместо
        // того чтобы оставить его вечно реконнектящейся сиротой (тот же
        // приём, что и в stdout_stays_json_only_when_the_real_binary_hits_a_fatal_error).
        .kill_on_drop(true);
    let child = cmd.spawn().expect("запустить pgcdc");

    let output = tokio::time::timeout(Duration::from_secs(20), child.wait_with_output())
        .await
        .expect("процесс обязан завершиться за 20 секунд, а не уйти в вечный реконнект")
        .expect("дождаться завершения pgcdc");

    assert!(
        !output.status.success(),
        "непригодный (чужой плагин) слот обязан быть фатальным для реального бинаря"
    );
    assert_eq!(
        output.status.code(),
        Some(1),
        "фатальная ошибка обязана давать код выхода 1 (DECISIONS Q22)"
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout должен быть валидным UTF-8");
    assert!(
        stdout.is_empty(),
        "stdout обязан остаться пустым при фатальной ошибке старта, получили: {stdout:?}"
    );

    let stderr = String::from_utf8(output.stderr).expect("stderr должен быть валидным UTF-8");
    assert!(
        stderr.contains("slot_unusable"),
        "stderr обязан назвать причину машиночитаемой меткой error_kind, получили: {stderr}"
    );
    assert!(
        !stderr.contains("reconnecting"),
        "фатальный отказ сервера обязан остановить процесс, а не уйти в цикл реконнекта: {stderr}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn slot_busy_with_our_own_prior_session_is_recoverable_not_fatal() {
    // Обратная сторона C1: сервер ТОЖЕ отвечает отказом на START_REPLICATION
    // ("replication slot ... is active for PID ...", ERRCODE_OBJECT_IN_USE),
    // но это не про непригодность слота — конкурентный читатель ещё держит
    // его. Наивный "любой отказ сервера фатален" сломал бы обычный реконнект:
    // после разрыва наша же предыдущая сессия может на мгновение не успеть
    // отпустить слот раньше, чем новая сессия попробует его перехватить.
    //
    // Здесь гонка сделана детерминированной: отдельный pg_walstream держит
    // слот занятым ДО того, как pgcdc вообще запускается, — первая попытка
    // pgcdc гарантированно получает "is active for PID", а не что-то ещё.
    // Если классификация когда-нибудь станет "любой отказ сервера фатален",
    // этот тест покраснеет детерминированно: run() вернёт SlotUnusable на
    // первой же попытке, канал закроется, и recv() ниже получит None вместо
    // транзакции.
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
        .expect("открыть блокирующее соединение");
    blocker
        .start(None)
        .await
        .expect("блокирующее соединение обязано первым захватить слот");

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

    // Дать pgcdc реально наткнуться на занятый слот хотя бы раз (100мс —
    // reconnect_initial_ms по умолчанию из config()), прежде чем освободить
    // его. Drop блокирующего потока на multi-thread рантайме синхронно шлёт
    // CopyDone+Terminate (pg_walstream::connection::native, close_connection),
    // так что слот освобождается раньше, чем drop() возвращает управление.
    tokio::time::sleep(Duration::from_millis(500)).await;
    drop(blocker);

    client
        .execute("INSERT INTO users VALUES (1, 'Alice', NULL, NULL)", &[])
        .await
        .unwrap();

    let tx = tokio::time::timeout(Duration::from_secs(20), tx_recv.recv())
        .await
        .expect("pgcdc обязан довести реконнект до конца, а не застрять или упасть")
        .expect("канал закрыт — run() завершился ошибкой вместо ретрая");
    assert_eq!(tx.changes.len(), 1);

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn slot_busy_forever_exhausts_the_patience_budget_and_the_process_exits_nonzero() {
    // Обратная сторона гонки выше (slot_busy_with_our_own_prior_session_is_recoverable_not_fatal):
    // слот, занятый ЧУЖИМ потребителем НАВСЕГДА, отвечает буквально тем же
    // SQLSTATE 55006 — по коду состояния эти два случая неотличимы (см. §
    // "Что осталось открытым" в task-4-report.md по задаче 4, п.3, и
    // подробный разбор у classify_start_error/SlotBusyPatience в
    // src/postgres/replication.rs). Единственный физический различитель —
    // ДЛИТЕЛЬНОСТЬ: наша прошлая сессия отпускает слот за десятки
    // миллисекунд (измерено), чужой потребитель — нет. Здесь блокирующее
    // соединение держит слот и НЕ освобождается на протяжении всего теста;
    // с маленьким бюджетом терпения процесс обязан исчерпать его и
    // завершиться ненулевым кодом, а не уйти в вечный реконнект — именно
    // это было воспроизведено вручную (34 цикла, ни одного ненулевого
    // кода выхода) в отчёте по задаче 4.
    //
    // Гоняем настоящий скомпилированный бинарь: пункт 14 чек-листа — про код
    // выхода ПРОЦЕССА, а не про Result библиотеки.
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
        .expect("открыть блокирующее соединение");
    blocker
        .start(None)
        .await
        .expect("блокирующее соединение обязано захватить слот и не отпускать его вовсе");

    let mut cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"));
    cmd.env("PGCDC_DATABASE_URL", &conn)
        .env("PGCDC_PUBLICATION", "pgcdc_pub")
        .env("PGCDC_SLOT", "pgcdc_slot")
        // Быстрый бэкофф и маленький бюджет — чтобы терпение исчерпалось за
        // сотни миллисекунд, а не за настоящие 30 секунд умолчания.
        .env("PGCDC_RECONNECT_INITIAL_MS", "20")
        .env("PGCDC_RECONNECT_MAX_MS", "50")
        .env("PGCDC_SLOT_BUSY_BUDGET_MS", "300")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        // kill_on_drop: если таймаут ниже сработает (терпение почему-то не
        // исчерпалось), процесс не осиротеет вечно реконнектящимся.
        .kill_on_drop(true);
    let child = cmd.spawn().expect("запустить pgcdc");

    let output = tokio::time::timeout(Duration::from_secs(20), child.wait_with_output())
        .await
        .expect(
            "процесс обязан исчерпать бюджет терпения и завершиться, а не уйти в вечный реконнект",
        )
        .expect("дождаться завершения pgcdc");

    // Держим блокирующее соединение живым до этой точки: слот обязан
    // оставаться занятым весь тест, иначе это тестировало бы обычную гонку
    // (уже покрытую тестом выше), а не вечно занятый слот.
    drop(blocker);

    assert!(
        !output.status.success(),
        "вечно занятый чужим потребителем слот обязан быть фатальным по исчерпании бюджета терпения"
    );
    assert_eq!(
        output.status.code(),
        Some(1),
        "фатальная ошибка обязана давать код выхода 1 (DECISIONS Q22)"
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout должен быть валидным UTF-8");
    assert!(
        stdout.is_empty(),
        "stdout обязан остаться пустым при фатальной ошибке старта, получили: {stdout:?}"
    );

    let stderr = String::from_utf8(output.stderr).expect("stderr должен быть валидным UTF-8");
    assert!(
        stderr.contains("slot_busy_timed_out"),
        "stderr обязан назвать причину машиночитаемой меткой error_kind, получили: {stderr}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn reconnect_zeroes_the_buffer_gauge_at_the_run_call_site() {
    // M4 (review round after task 4): удаление вызова
    // state.reset_for_reconnect(&metrics) из run() оставляет все остальные
    // тесты зелёными — сама функция покрыта юнит-тестом
    // (reconnect_zeroes_the_buffer_gauge_even_with_an_open_transaction в
    // src/postgres/replication.rs), а её единственный вызов внутри run() —
    // нет. README прямо обещает, что датчик размера буфера падает до нуля
    // и на реконнекте тоже — этот тест пришпиливает именно вызов, а не саму
    // функцию.
    //
    // Две ловушки, из-за которых "просто разорвать соединение и проверить
    // счётчик" не сработало бы:
    // 1) Однострочная транзакция приходит одним куском (BEGIN+row+COMMIT
    //    подряд) — окна, где буфер реально ненулевой, а COMMIT ещё не
    //    пришёл, не существует. Транзакция здесь достаточно большая, чтобы
    //    decode и передача заняли измеримое время, и тест ждёт (опросом, а
    //    не сном наугад), пока датчик не станет положительным.
    // 2) Сама следующая BEGIN переигранной транзакции обнулила бы `len()`
    //    естественно (Assembler::handle перезаписывает открытую транзакцию
    //    целиком, без явного reset) — без дополнительной меры мутация
    //    "убрать reset_for_reconnect" осталась бы незамеченной, потому что
    //    ноль появился бы всё равно, просто по другой причине. Слот здесь
    //    держит занятым второй читатель сразу после обрыва, так что ни
    //    один новый кадр не может прийти, пока блокировщик жив, — и ноль,
    //    если он появится, может быть только от самого reset_for_reconnect.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let metrics = std::sync::Arc::new(pgcdc::metrics::Metrics::new());
    let (tx_send, _tx_recv) = mpsc::unbounded_channel();
    let mut cfg = config(&conn);
    // Достаточно большой, чтобы блокировщик гарантированно успел захватить
    // слот (гонится только с отсоединением старого backend'а на сервере, а
    // не с этой попыткой) раньше, чем run() вообще попробует реконнект.
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
        "тест не поймал транзакцию в процессе передачи — увеличьте объём вставки"
    );

    common::terminate_replication_backend(&client).await;

    // Захватить слот раньше, чем run() сам попробует реконнект (у него есть
    // целых 2 секунды форы, cfg.reconnect_initial_ms выше). Гонка здесь —
    // только с тем, сколько сервер отсоединяет старый backend после
    // pg_terminate_backend, а не с pgcdc.
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
    let _blocker = blocker.expect("захватить слот блокировщиком раньше pgcdc не удалось");

    // К этому моменту run() уже должен был пройти через backoff-паузу и
    // reset_for_reconnect(), но не смочь получить ни одного нового кадра —
    // слот занят блокировщиком.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(
        metrics.snapshot().transaction_buffer_size,
        0,
        "датчик обязан упасть до нуля на реконнекте, даже пока слот занят и новых кадров нет"
    );

    handle.abort();
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
async fn a_dropped_connection_is_recovered_without_losing_rows() {
    // Захватываем логи до старта run(): проверка ниже (F3) должна увидеть
    // событие восстановления, а не просто угадать, что оно случилось.
    let log_events = common::capture_log_events();

    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    // M8: имя слота здесь уникально для этого теста, а не общее "pgcdc_slot",
    // которым пользуется большинство соседей, — `log_events` копит сообщения
    // ВСЕХ параллельно идущих тестов в одном процессе (общий глобальный
    // буфер), и без уникального маркера в самом сообщении совпадение по
    // тексту "postgres_connection_restored" ниже стало бы случайным поводом,
    // а не доказательством, что реконнект произошёл именно здесь.
    let slot = "pgcdc_slot_recover_no_loss";
    common::create_slot(&client, slot).await;

    // F5 (review Task 2, round 1): общий экземпляр, а не одноразовый — это
    // единственный тест, который заведомо пересекает ветку реконнекта, и
    // потому единственное мутационное покрытие, которое `reconnects_total`
    // вообще когда-либо получит.
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
        .expect("первая транзакция")
        .expect("канал закрыт");
    assert_eq!(
        first.changes[0]
            .after
            .as_ref()
            .unwrap()
            .get("name")
            .unwrap(),
        "before"
    );

    // Ждём, пока наш собственный барьер доведёт первую транзакцию до durable
    // и слот на сервере это подтвердит: канал отдаёт транзакцию сразу после
    // write_transaction, а до durable/acked ещё нужно дождаться следующего
    // тика барьера. Без этого обрыв ниже почти наверняка случится раньше
    // первого flush, `state.durable()` останется нулём, is_reconnect()
    // никогда не станет true, и весь блок проверки реконнекта останется
    // непройденным этим тестом — ровно риск из review Task 2, round 1, F3.
    common::wait_for_slot_at_least(&client, slot, first.end_lsn).await;

    // Сервер обрывает наше репликационное соединение.
    common::terminate_replication_backend(&client).await;

    client
        .execute("INSERT INTO users VALUES (2, 'after', NULL, NULL)", &[])
        .await
        .unwrap();

    // Строка, вставленная после обрыва, обязана приехать. Дубликат первой
    // допустим и контрактом разрешён, поэтому ищем нужную, а не берём первую.
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
        "строка после обрыва не приехала, видели: {names:?}"
    );

    // F3: это headline-проверка задачи — check_reconnect действительно
    // выполнился и recovery действительно залогирован, а не просто
    // "строка как-то приехала". Удаление всего блока проверки реконнекта
    // не тронуло бы ни одно из предыдущих утверждений в этом тесте.
    //
    // M8: сообщение обязано нести ИМЕННО наш `slot` — `log_events` общий на
    // весь тестовый бинарь, и другой тест, доживший до успешного реконнекта,
    // залогировал бы то же сообщение с ДРУГИМ именем слота. Сегодня
    // единственный сосед-реконнект (`a_slot_advanced_past_our_durable_position_is_fatal_on_reconnect`)
    // падает на check_reconnect раньше этого лога, так что без маркера
    // совпадение по одному тексту сообщения ещё было бы (случайно) верным;
    // с маркером оно перестаёт зависеть от этой хрупкой предпосылки.
    let expected_log = format!("postgres_connection_restored slot={slot}");
    assert!(
        log_events.lock().unwrap().contains(&expected_log),
        "событие восстановления соединения не залогировано для слота {slot}"
    );

    // F5 (review Task 2, round 1): к этой точке внешний цикл реконнекта уже
    // прошёл хотя бы один полный оборот (доказано логом восстановления
    // выше), так что счётчик обязан был продвинуться. Это единственная
    // мутационная проверка, которую `reconnects_total` вообще получает —
    // удаление или неверная привязка инкремента остались бы незамеченными
    // всем остальным набором.
    assert!(
        metrics.snapshot().reconnects_total >= 1,
        "reconnects_total обязан был продвинуться после обрыва и восстановления"
    );

    // F4: слот, пропавший во время обрыва, обязан быть фатальным немедленно,
    // а не ретраиться бесконечно — иначе процесс сидел бы в цикле реконнекта,
    // пока данные, которые он должен был захватить, не состарились в WAL.
    common::terminate_replication_backend(&client).await;
    common::drop_slot_once_inactive(&client, slot).await;

    let result = tokio::time::timeout(Duration::from_secs(20), handle)
        .await
        .expect("run должен упасть на пропавшем слоте, а не ретраиться вечно")
        .expect("join");
    let err = result.unwrap_err();
    assert!(
        matches!(err, PgcdcError::SlotMissing { .. }),
        "получили {err:?}"
    );
    assert!(err.is_fatal());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_slot_advanced_past_our_durable_position_is_fatal_on_reconnect() {
    // Прямое доказательство, что check_reconnect() теперь ДЕЙСТВИТЕЛЬНО
    // вызывается, а не просто существует нетронутым (review Task 2, round 1,
    // F3). Тест из соседней функции этого не проверяет: там слот на обычном
    // реконнекте либо точно совпадает с durable, либо отстаёт — асимметрию
    // "слот ВПЕРЁД — фатально" ни разу не задевает. Здесь слот двигаем вручную
    // через pg_replication_slot_advance мимо нашего sink — ровно тот случай,
    // когда кто-то подтвердил WAL, который мы не записали.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let (tx_send, mut tx_recv) = mpsc::unbounded_channel();
    let mut cfg = config(&conn);
    // Окно нужно, чтобы успеть продвинуть слот ДО того, как процесс сам
    // предпримет попытку реконнекта.
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
        .expect("первая транзакция")
        .expect("канал закрыт");
    // Durable должен реально стать ненулевым, иначе is_reconnect() на
    // следующем подключении останется false и check_reconnect не вызовется.
    common::wait_for_slot_at_least(&client, "pgcdc_slot", first.end_lsn).await;

    common::terminate_replication_backend(&client).await;
    common::wait_until_slot_inactive(&client, "pgcdc_slot").await;

    // Строка, которую наш (сейчас отключённый) sink никогда не увидит.
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
            // Двойное приведение, а не прямое $2::pg_lsn: иначе Postgres выводит
            // тип плейсхолдера как pg_lsn напрямую, а tokio-postgres не умеет
            // биндить в него `String` (WrongType). Через ::text::pg_lsn плейсхолдер
            // остаётся текстовым для драйвера, а привод в pg_lsn происходит на сервере.
            "SELECT * FROM pg_replication_slot_advance($1, $2::text::pg_lsn)",
            &[&"pgcdc_slot", &target],
        )
        .await
        .expect("advance slot past our durable position");

    let result = tokio::time::timeout(Duration::from_secs(20), handle)
        .await
        .expect("run должен упасть на SlotAhead, а не тихо продолжить реконнект")
        .expect("join");
    let err = result.unwrap_err();
    assert!(
        matches!(err, PgcdcError::SlotAhead { .. }),
        "получили {err:?}"
    );
    assert!(err.is_fatal());
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
            .expect("запустить бинарь"),
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

#[tokio::test(flavor = "multi_thread")]
async fn a_terminated_process_exits_zero_after_draining() {
    // Штатная остановка обязана давать ноль. Иначе супервизор будет
    // бесконечно перезапускать процесс, который остановили намеренно.
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
            .expect("запустить бинарь"),
    );

    client
        .execute("INSERT INTO users VALUES (1, 'Alice', NULL, NULL)", &[])
        .await
        .unwrap();

    // Ждём, пока строка окажется в файле, — значит процесс дошёл до барьера.
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
    assert!(seen, "строка не появилась в файле за 20 секунд");

    // SIGTERM, а не kill: проверяем именно штатное завершение.
    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
    let status = tokio::task::spawn_blocking(move || child.wait())
        .await
        .unwrap()
        .expect("wait");
    assert_eq!(status.code(), Some(0), "штатная остановка даёт ноль");

    let _ = std::fs::remove_file(&out);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_over_the_limit_is_fatal_and_the_slot_stays_put() {
    // Лимит не чинит цикл рестартов на гигантской транзакции — он меняет
    // диагностику с «убит по памяти» на внятное сообщение (DECISIONS Q7).
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
        .expect("run должен завершиться, а не висеть")
        .expect("join");
    let err = result.unwrap_err();
    assert!(
        matches!(err, PgcdcError::TransactionTooLarge { limit: 2 }),
        "получили {err:?}"
    );
    assert!(
        err.is_fatal(),
        "превышение лимита — фатальная ошибка, а не повод для ретрая"
    );

    let after: String = client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 'pgcdc_slot'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, after, "фатальная ошибка не двигает слот");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_terminated_process_drains_before_the_periodic_barrier_would() {
    // Round 1, F2: `a_terminated_process_exits_zero_after_draining` остаётся
    // зелёным даже без барьера в самой ветке завершения, потому что при
    // интервале по умолчанию (200мс) периодический барьер почти наверняка
    // успевает сработать раньше, чем мы вообще отправим сигнал. Этот тест
    // закрывает именно эту дыру: интервал барьера задран настолько, что
    // периодическая ветка заведомо не успевает сработать за время
    // предпроверки.
    //
    // Проверка НЕ по содержимому файла: `FileSink` пишет через
    // `BufWriter<File>`, и при обычном (не панике) выходе из процесса Rust
    // сам делает для него best-effort `flush()` в `Drop` — без вызова
    // барьера ветки завершения строка всё равно окажется в файле, просто
    // без fsync и без подтверждения слоту. Первая попытка написать этот
    // тест так и проверяла — файл непуст после SIGTERM — и осталась
    // зелёной ПОСЛЕ мутации, убирающей барьер: `Drop` замаскировал
    // отсутствие вызова. Источник истины, который не подделать через
    // `Drop`, только один — `confirmed_flush_lsn` слота на сервере: он
    // продвигается исключительно вызовом `send_feedback`, а тот в ветке
    // завершения происходит только внутри `flush_and_acknowledge`.
    //
    // Round 3, F1: раньше вместо ожидания доказательства здесь стоял
    // фиксированный `sleep`. Ревьюер вскрыл настоящую причину флейка: при
    // 150мс дочерний процесс ещё не успевал даже установить обработчик
    // сигнала, при 700мс проходило впритык (~0.4с запаса) — под двадцатью
    // параллельными контейнерами такого бюджета не хватает, и SIGTERM
    // уходит ДО того, как транзакция вообще была принята и разобрана. В
    // этом случае барьеру просто нечего флашить, и слот не двигается — та
    // же сигнатура отказа, что и у мутации, убирающей вызов барьера из
    // ветки завершения, но по другой причине. Часы заменены на
    // доказательство: пишем stderr дочернего процесса под debug-логами и
    // ждём строку `transaction_accepted` — она логируется сразу после
    // `sink.write_transaction`, то есть до всякого барьера, и означает,
    // что транзакция гарантированно лежит в sink'е и барьеру будет что
    // флашить. SIGTERM уходит только после этого. Проверка ниже
    // (`before_signal < target`) не ослаблена ей в пару: если бы мы
    // прождали ещё дольше — вплоть до срабатывания периодического барьера
    // — она бы упала сама, доказывая, что тест перестал изолировать ветку
    // завершения.
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
                // На порядок больше, чем время до сигнала ниже: если барьер
                // таймера всё-таки сработает в этом окне, тест ничего не
                // докажет о ветке завершения.
                "--ack-interval-ms",
                "10000",
            ])
            // debug — чтобы увидеть transaction_accepted (логируется на этом
            // уровне сразу после приёма транзакции sink'ом); pg_walstream
            // приглушён отдельно, чтобы не тонуть в его собственном debug-шуме.
            .env("RUST_LOG", "pgcdc=debug,pg_walstream=warn")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("запустить бинарь"),
    );

    let stderr = child.stderr.take().expect("stderr был запрошен как piped");
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

    // Позиция WAL сразу после коммита — нижняя граница того, что процесс
    // обязан будет подтвердить слоту, когда всё-таки подтвердит эту
    // транзакцию (через периодический барьер или через барьер завершения).
    let target: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .unwrap()
        .get(0);
    let target = common::parse_lsn(&target).expect("распарсить LSN");

    // Ждём доказательство, а не время: `transaction_accepted` означает,
    // что транзакция уже принята sink'ом и лежит там, ожидая барьера.
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
        "не увидели transaction_accepted за 20 секунд, видели: {:?}",
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
    let before_signal = common::parse_lsn(&before_signal).expect("распарсить LSN");
    assert!(
        before_signal < target,
        "слот продвинулся до {target} ДО сигнала (сейчас {before_signal}) — периодический \
         барьер успел сработать, тест не изолирует ветку завершения"
    );

    // SIGTERM: если барьер живёт в ветке завершения (как и должно быть),
    // слот обязан подтвердить транзакцию уже ПОСЛЕ сигнала.
    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
    let status = tokio::task::spawn_blocking(move || child.wait())
        .await
        .unwrap()
        .expect("wait");
    assert_eq!(status.code(), Some(0), "штатная остановка даёт ноль");

    let after_signal = common::wait_for_slot_at_least(&client, "pgcdc_slot", target).await;
    assert!(
        after_signal >= target,
        "слот не подтвердил транзакцию после штатной остановки: {after_signal} < {target}"
    );

    let _ = std::fs::remove_file(&out);
}

#[tokio::test(flavor = "multi_thread")]
async fn sending_sigterm_after_a_reconnect_still_exits_zero() {
    // Проверяет только то, что заявлено в имени: после реконнекта SIGTERM
    // по-прежнему доводит процесс до штатного завершения с кодом 0.
    //
    // Round 2, F3: этот тест раньше претендовал на большее — что он ловит
    // перенос `spawn_shutdown_listener()` внутрь цикла реконнекта (создание
    // слушателя заново на каждую сессию). Это неверно: у
    // `tokio::signal::unix::signal` и `ctrl_c()` доставка сигнала идёт
    // КАЖДОМУ зарегистрированному слушателю данного вида, а не только
    // последнему созданному, — так что даже слушатель, пересозданный
    // заново на второй сессии, всё равно получил бы SIGTERM и тест остался
    // бы зелёным независимо от места вызова. Слушатель по-прежнему живёт
    // над внешним циклом (пересоздавать его на каждый реконнект — течь по
    // задаче на сессию), но это тест на поведение (сигнал работает и после
    // реконнекта), а не на размещение кода.
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
            .expect("запустить бинарь"),
    );

    // Читаем stderr дочернего процесса построчно в фоновом потоке: нам
    // нужно ДОКАЗАТЬ, что реконнект произошёл, до отправки сигнала, а не
    // просто понадеяться на время. `postgres_connection_restored` логируется
    // только на успешном повторном подключении (stream_once, review Task 2).
    let stderr = child.stderr.take().expect("stderr был запрошен как piped");
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

    // Ждём первую строку в файле — доказательство, что процесс дошёл до
    // барьера на первой сессии.
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
    assert!(seen, "первая строка не появилась в файле за 20 секунд");

    // Обрываем репликационное соединение со стороны сервера — процесс
    // обязан переподключиться сам (задача 2), а не упасть.
    common::terminate_replication_backend(&client).await;

    // Ждём лог восстановления соединения: без него ниже мы бы просто
    // проверяли обычный сигнальный сценарий ещё раз, ничего не говоря про
    // реконнект.
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
        "не увидели лог восстановления соединения за 20 секунд, видели: {:?}",
        lines.lock().unwrap()
    );

    // SIGTERM ПОСЛЕ реконнекта: см. комментарий над функцией — это тест на
    // поведение (сигнал по-прежнему работает), а не на размещение кода.
    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
    let status = tokio::task::spawn_blocking(move || child.wait())
        .await
        .unwrap()
        .expect("wait");
    assert_eq!(
        status.code(),
        Some(0),
        "штатная остановка после реконнекта тоже обязана давать ноль"
    );

    let _ = std::fs::remove_file(&out);
}

#[tokio::test(flavor = "multi_thread")]
async fn sigint_also_stops_the_process_cleanly() {
    // Чек-лист заявляет обработку SIGINT наравне с SIGTERM, но теста на неё
    // не было; SIGTERM закрыт отдельным тестом, а SIGINT до сих пор держался
    // только на том, что слушатель объединяет оба сигнала в один select.
    // Проверяем, что объединение работает.
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
            .expect("запустить бинарь"),
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
    let target = common::parse_lsn(&target).expect("распарсить LSN");
    common::wait_for_slot_at_least(&client, "pgcdc_slot", target).await;

    unsafe { libc::kill(child.id() as i32, libc::SIGINT) };

    // Опрос через try_wait(), а не блокирующий wait() (round 1 review, F5):
    // как и в мёртвопортовом соседе этого теста, регрессия, которая ставит
    // обработчик, но никогда не взводит флаг, оставила бы блокирующий
    // wait() висеть навсегда, а Drop рантайма tokio ждёт именно
    // blocking-задачи — тест повесил бы весь тестовый бинарь вместо того,
    // чтобы просто покраснеть.
    let mut status = None;
    for _ in 0..100 {
        if let Ok(Some(s)) = child.try_wait() {
            status = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let status = status.expect("SIGINT обязан остановить процесс за 5 секунд, а не только SIGKILL");
    assert_eq!(status.code(), Some(0), "SIGINT тоже даёт ноль");

    let _ = std::fs::remove_file(&out);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_productive_session_resets_the_backoff() {
    // Перенесено с этапа 4. Сброс считался непроверяемым, но задержка
    // попадает в лог структурным полем на каждой попытке, и этого достаточно.
    // Сценарий: два обрыва подряд с продуктивной сессией между ними —
    // задержка второй серии обязана начаться заново с начальной, а не
    // продолжить расти от достигнутой.
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

    // Ждём, пока walsender первой (холодный старт) сессии реально
    // подключится: между `spawn()` и `START_REPLICATION` проходит заметное
    // время (разбор аргументов, TCP, preflight), и обрыв, посланный раньше,
    // никого не находит — первая серия бэкоффа тогда вообще не случится.
    common::wait_until_slot_active(&client, "pgcdc_slot").await;

    // Первый обрыв и вставка, чтобы сессия после него была продуктивной.
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
    let target = common::parse_lsn(&target).expect("распарсить LSN");
    common::wait_for_slot_at_least(&client, "pgcdc_slot", target).await;

    // Второй обрыв. Первая попытка после него обязана взять начальную задержку.
    common::terminate_replication_backend(&client).await;

    // Читаем ОБЕ серии: первая начинается с начальной задержки в любом случае,
    // и различает их только вторая. Со сбросом получится [100, 100], без него
    // вторая серия продолжит с удвоенной — [100, 200].
    let delays = common::collect_backoff_delays(&mut child, 2).await;
    assert_eq!(
        delays.get(1).copied(),
        Some(100),
        "после продуктивной сессии бэкофф обязан начаться заново, а не продолжить: {delays:?}"
    );

    let _ = std::fs::remove_file(&out);
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_report_line_is_periodic_and_its_countdown_survives_a_reconnect() {
    // I2: удаление всего блока периодической сводки (`metrics_report`,
    // `METRICS_REPORT_INTERVAL`) оставляло все 168 тестов зелёными — ни то,
    // что строка вообще выходит, ни интервал, ни то, что отсчёт переживает
    // переподключение, не было пришпилено ничем, кроме ручного прогона демо.
    // А ведь именно переживание реконнекта обосновывало вынос `last_report`
    // наружу цикла реконнекта в прошлом раунде (review Task 3, round 1, F1):
    // без этого процесс, переподключающийся чаще десяти секунд, никогда не
    // прожил бы достаточно долго внутри одной сессии, чтобы сводка вышла.
    //
    // Сценарий раздельно пришпиливает обе половины: реконнект форсируется
    // РАНО (в первые секунды), задолго до десятисекундного интервала. Если
    // бы отсчёт неверно обнулялся на реконнекте, строка появилась бы не
    // раньше t_reconnect + 10с; отсчёт, переживающий реконнект, печатает её
    // около t_start + 10с независимо от того, когда случился реконнект.
    // Разница между этими двумя предсказаниями — много секунд, и именно она
    // разделяет тест на "переживает" и "не переживает".
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

    let stderr = child.stderr.take().expect("stderr перехвачен при запуске");
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

    // Ждём первую (холодный старт) сессию — обрыв, посланный раньше, никого
    // не найдёт (backend ещё не подключился).
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
    assert!(seen, "первая строка не появилась в файле за 5 секунд");

    // Форсируем реконнект на известной, контролируемой отметке (~6с от
    // старта процесса) — не сразу и не близко к десятисекундному интервалу.
    // Разнос важен: он и разделяет два предсказания. Отсчёт, переживающий
    // реконнект, печатает сводку около t_start + 10с независимо от того,
    // когда случился реконнект; отсчёт, ошибочно обнуляемый НА реконнекте,
    // печатает её около t_reconnect + 10с ≈ 16с — эти два предсказания
    // разделяет несколько секунд, и именно в этот разрыв целится тест.
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
        "не увидели восстановление соединения за 5 секунд"
    );
    let t_reconnected = t_start.elapsed();
    assert!(
        t_reconnected < Duration::from_secs(9),
        "реконнект обязан был случиться задолго до интервала сводки, а занял {t_reconnected:?} — \
         тест не может отличить 'пережил реконнект' от 'совпало по времени' без этого запаса"
    );

    // Ждём строку сводки, не дольше 20 секунд от старта процесса.
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
    let t_report =
        t_report.expect("строка metrics_report не появилась за 20 секунд от старта процесса");

    assert!(
        t_report >= Duration::from_secs(9),
        "сводка вышла раньше интервала METRICS_REPORT_INTERVAL: {t_report:?} от старта процесса"
    );
    assert!(
        t_report <= Duration::from_secs(13),
        "сводка вышла позже, чем допускает переживший реконнект отсчёт: {t_report:?} от старта \
         процесса, реконнект случился на {t_reconnected:?} — обнуление отсчёта на реконнекте \
         отодвинуло бы её к t_reconnected + 10с = {:?}, что выходит далеко за этот предел",
        t_reconnected + Duration::from_secs(10)
    );

    let _ = std::fs::remove_file(&out);
}
