#![allow(dead_code)]

use std::sync::{Arc, Mutex, OnceLock};

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// Свежий PostgreSQL на каждый тест. Слот репликации — глобальный объект
/// с состоянием, и на общем инстансе тесты дрались бы за него и зависели
/// от порядка запуска (DECISIONS Q10).
pub async fn start_postgres() -> (ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("postgres", "16-alpine")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "app")
        .with_cmd(vec![
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=10",
            "-c",
            "max_wal_senders=10",
        ])
        .start()
        .await
        .expect("start postgres");

    // C4: wait-strategy проверяет, что Postgres принимает соединения, а не что
    // проброс порта Docker уже отвечает на запрос — это отдельная гонка, которая
    // ловилась примерно раз в десять прогонов. Ограниченный ретрай без sleep
    // в цикле ожидания: tokio::time::sleep — не блокирующий поток sleep.
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

pub async fn connect(conn_str: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(conn_str, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// Схема демо из docker/init.sql, но создаваемая из кода теста,
/// чтобы контролировать стартовую позицию слота.
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

/// Таблица с REPLICA IDENTITY DEFAULT — нужна, чтобы получить тег 'K'.
/// У `users` идентичность FULL, и она даёт только 'O'.
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

/// Ждёт, пока `confirmed_flush_lsn` слота не догонит `target`.
/// Опрос ограничен: если не догнал, тест падает с фактической позицией,
/// а не висит.
pub async fn wait_for_slot_at_least(
    client: &tokio_postgres::Client,
    slot: &str,
    target: pgcdc::lsn::Lsn,
) -> pgcdc::lsn::Lsn {
    let mut last = pgcdc::lsn::Lsn(0);
    for _ in 0..100 {
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
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("слот не догнал {target}, остановился на {last}");
}

/// Обрывает наше репликационное соединение со стороны сервера. Это дешевле
/// перезапуска контейнера и точнее воспроизводит сетевой обрыв.
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

/// Ждёт, пока слот не станет активным — то есть walsender только что
/// запущенного процесса действительно подключился. Обрыв, посланный раньше
/// этого момента, никого не находит и молча ничего не делает: запрос
/// `terminate_replication_backend` фильтрует по `backend_type = 'walsender'`,
/// а между `spawn()` бинаря и первым `START_REPLICATION` реально проходит
/// заметное время (разбор аргументов, установление TCP, preflight-запрос) —
/// не единицы миллисекунд, за которые тестовый код успевает вызвать обрыв.
/// Без этой синхронизации в сценарии из двух обрывов первый обрыв пропадает
/// впустую, и весь тест видит только одну серию бэкоффа вместо двух.
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
    panic!("слот {slot} не стал активным за 5 секунд");
}

/// Ждёт, пока слот не перестанет числиться активным (то есть walsender
/// действительно отключился) — нужно перед `pg_replication_slot_advance`,
/// которая на активном слоте отказывает.
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
    panic!("слот {slot} остаётся активным");
}

/// Роняет слот, но только когда он действительно неактивен: `pg_terminate_backend`
/// лишь посылает сигнал и возвращается, не дожидаясь фактического закрытия —
/// `pg_drop_replication_slot` в это окно падает с ошибкой "slot is active".
/// Ретраим, а не спим один раз наугад (review Task 2, round 1, F4).
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
    panic!("не удалось удалить слот {slot}: остаётся активным");
}

/// Подписчик tracing, копящий текст поля `message` каждого события в общий
/// буфер. Не использует `tracing-subscriber` — минимальная ручная реализация
/// достаточна и не требует новой зависимости.
///
/// M8: буфер общий на весь тестовый бинарь (диспетчер tracing глобален для
/// процесса), поэтому события ВСЕХ параллельно идущих тестов попадают в один
/// список. Само по себе сообщение — например, `"postgres_connection_restored"`
/// — не привязано к тесту, который его вызвал; если это же сообщение когда-
/// нибудь начнёт логировать другой успешно реконнектящийся тест, совпадение
/// по одному тексту станет случайным. Поэтому визитор также запоминает поле
/// `slot`, когда оно есть у события (`info!(slot = %..., "сообщение")` — так
/// залогированы и preflight, и старт, и восстановление соединения), и
/// склеивает его с сообщением как `"сообщение slot=значение"` — вызывающий
/// может сверять и то, и другое, а не полагаться на уникальность одного текста.
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

/// Включает перехват сообщений трейсинга ровно один раз на весь тестовый
/// бинарь (диспетчер tracing глобален для процесса, `set_global_default`
/// нельзя вызывать дважды) и возвращает общий буфер. Сообщения из ВСЕХ
/// одновременно идущих тестов попадают в один список, но искомые здесь
/// строки уникальны для сценария реконнекта, так что ложных совпадений
/// не будет (review Task 2, round 1, F3).
pub fn capture_log_events() -> Arc<Mutex<Vec<String>>> {
    LOG_EVENTS
        .get_or_init(|| {
            let events = Arc::new(Mutex::new(Vec::new()));
            let subscriber = CapturingSubscriber {
                events: events.clone(),
            };
            // Игнорируем ошибку: если диспетчер уже установлен (другим тестом
            // в этом же бинаре до `OnceLock::get_or_init`), общий буфер —
            // именно тот, что уже используется.
            let _ = tracing::subscriber::set_global_default(subscriber);
            events
        })
        .clone()
}

/// Guard вокруг `std::process::Child`: убивает процесс при паде, если он ещё
/// жив. `std::process::Child`, в отличие от `tokio::process::Child` с
/// `kill_on_drop(true)`, ничего не делает при `Drop` — обычный дочерний
/// процесс переживает свой хендл. Тесты убивают дочерний бинарь явно перед
/// концом сценария, но между `spawn()` и этим явным `kill()` лежат `.await` и
/// `unwrap()`/`assert!`, которые могут запаниковать раньше; без этого guard'а
/// паника в середине теста осиротила бы процесс, который (теперь, когда он
/// умеет ретраить реконнект бесконечно) продолжил бы долбиться в контейнер
/// Postgres даже после того, как тот исчез вместе с тестом (M7).
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
        // `try_wait` первым делом: если тест уже сам дождался процесса
        // (обычный путь), реаппинг уже случился, и слепой повторный
        // `kill`/`wait` рисковал бы бить по чужому процессу, если ОС успела
        // переиспользовать pid. Убиваем и дожидаемся только когда процесс
        // подтверждённо ещё жив — именно тот случай, который и нужно
        // прикрыть (паника до явного kill в тесте).
        if matches!(self.0.try_wait(), Ok(None)) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

/// Порождает бинарь с перехваченным stderr, обёрнутый в существующий страж,
/// чтобы падение теста не оставило процесс, который будет вечно
/// переподключаться уже после того, как контейнер исчезнет.
pub fn spawn_with_stderr(args: &[&str]) -> KillOnDrop {
    KillOnDrop(
        std::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"))
            .args(args)
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("запустить бинарь"),
    )
}

/// Вырезает ANSI-последовательности вида `ESC [ ... <буква>` (SGR-коды
/// цвета/стиля). Изначально `tracing_subscriber::fmt()` в `main.rs` красил
/// вывод БЕЗУСЛОВНО, даже когда stderr — обычная труба, а не терминал: в
/// сырых байтах поле выглядело как `ESC[3m` (курсив), `"backoff_ms"`,
/// `ESC[0m` (сброс), `ESC[2m` (тусклый), `"="`, ещё один `ESC[0m` и только
/// потом значение — то есть между именем поля и знаком равенства лежат ДВА
/// кода, а не один, и строка `"backoff_ms="` в байтах не встречалась вовсе
/// (review Task 3, round 1, F6 — предыдущая версия этого комментария
/// упоминала только один код между ними). С тех пор `main.rs` включает
/// раскраску только когда stderr — реальный терминал (F4, review Task 3,
/// round 2), и труба, которой пользуется этот тест, раскраски больше не
/// получает, но вырезание остаётся: настройка может снова стать
/// безусловной, а этот помощник обязан пережить такой откат, не полагаясь
/// на то, что прод его не сломает. Без этой очистки `collect_backoff_delays`
/// не находит поле никогда, независимо от того, сброшен бэкофф или нет, —
/// тест был бы слеп, а не просто неверен.
fn strip_ansi(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            // CSI-последовательность: ESC '[' ... завершается первой буквой.
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

/// Читает stderr потомка и возвращает первые `n` задержек ПЕРВОЙ попытки
/// каждой серии реконнекта — строки с `retry=1`, а не первые `n` строк
/// переподключения вообще. Различие важно: если первая серия когда-нибудь
/// потребует второй попытки (`retry=2`), её задержка тоже вырастет — это
/// рост ВНУТРИ одной серии по той же экспоненте, а не сломанный сброс между
/// сериями, и «первые n строк подряд» спутали бы одно с другим, ложно
/// покраснев на исправной экспоненте (review Task 3, round 1, F5). Фильтр
/// по `retry=1` отбирает ровно первую попытку каждой серии независимо от
/// того, сколько попыток эта серия в итоге заняла. Бюджет ограничен: если
/// задержек не набралось, падаем с тем, что действительно увидели, а не
/// висим.
pub async fn collect_backoff_delays(child: &mut KillOnDrop, n: usize) -> Vec<u64> {
    use std::io::{BufRead, BufReader};

    let stderr = child.stderr.take().expect("stderr перехвачен при запуске");
    let handle = tokio::task::spawn_blocking(move || {
        let mut found = Vec::new();
        for raw_line in BufReader::new(stderr).lines().map_while(Result::ok) {
            let line = strip_ansi(&raw_line);
            if !line.contains("reconnecting") {
                continue;
            }
            // Поля пишутся структурно: ищем их по имени, а не по позиции.
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
        Ok(Ok(found)) => panic!("нашли только {} задержек из {n}: {found:?}", found.len()),
        Ok(Err(e)) => panic!("чтение stderr упало: {e}"),
        Err(_) => panic!("не дождались {n} задержек за 20 секунд"),
    }
}

/// PostgreSQL печатает позицию как две шестнадцатеричные половины через слэш.
pub fn parse_lsn(text: &str) -> Option<pgcdc::lsn::Lsn> {
    let (hi, lo) = text.split_once('/')?;
    let hi = u64::from_str_radix(hi, 16).ok()?;
    let lo = u64::from_str_radix(lo, 16).ok()?;
    Some(pgcdc::lsn::Lsn((hi << 32) | lo))
}
