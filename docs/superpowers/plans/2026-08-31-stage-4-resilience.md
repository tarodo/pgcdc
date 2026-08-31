# pgcdc Этап 4 (Устойчивость) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Пережить обрыв соединения, перезапуск PostgreSQL и собственный `kill -9`, не потеряв ни одной закоммиченной строки и не подтвердив ни одной незаписанной.

**Architecture:** `run()` расщепляется надвое: долгоживущее состояние поднимается выше подключения, а всё, что живёт в рамках одной сессии репликации, уезжает в отдельную функцию, которую внешний цикл вызывает заново после обрыва. Трекер позиций **переносится** через реконнект, кэш отношений и сборщик — **сбрасываются**. Завершение по сигналу доводит текущую группу до барьера и выходит с нулём; всё остальное — ненулевой код с машиночитаемой меткой.

**Tech Stack:** Rust 1.95.0 (Homebrew), tokio, `pg_walstream` 0.8, serde_json, chrono, clap, tracing, testcontainers (dev), PostgreSQL 16 в Docker.

**Spec:** [DECISIONS.md](../../../DECISIONS.md) — инвариант 1, Q4, Q19, Q22, Q25, Q26. Базовая спека [input/pgcdc_mvp_task.md](../../../input/pgcdc_mvp_task.md) §14, §15, §18.

---

## Global Constraints

Действуют во **всех** задачах. Нарушение любого — основание отклонить задачу на ревью.

1. **PATH в песочнице урезан.** Каждая команда Bash обязана начинаться с:
   ```bash
   export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
   ```
   Это покрывает `docker` (в `/usr/local/bin`) и `cargo`/`psql` (в `/opt/homebrew/bin`).
2. **Псевдонимы перекрывают базовые утилиты, а их целей нет в PATH.** `cat` → `bat`,
   `ls` → `eza`. Использовать `/bin/cat` и `/bin/ls`.
3. **Рабочая директория:** `/Users/roman/Projects/HP/rust_cdc`.
4. **Rust 1.95.0 из Homebrew, `rustup` отсутствует.** Никаких `+nightly`.
5. **Foreground `sleep` заблокирован песочницей.** В async-коде — `tokio::time`.
6. **Инвариант 1 нерушим:** `acked <= durable`. `try_ack` не ослабляется.
7. **Порядок в цикле не меняется:** приём → барьер → durable → подтверждение → feedback.
   Он проверен мутационно; любое изменение — критический дефект.
8. **Гейт продвижения по keepalive не меняется:** буфер пуст И `processed <= durable`.
9. **Разрешён только `next_raw_event`.** Пять API восстановления
   (`next_event_with_retry`, `check_connection_health`, `into_stream`, `stream`,
   `for_each_event`) рестартуют поток с принятой, а не durable позиции.
10. **Каждый интеграционный тест несёт `flavor = "multi_thread"`.** Транспорт выбирает
    драйвер соединения по флейвору рантайма, и тест обязан гонять тот же, что и прод.
11. **Ни один файл в `tests/fixtures/` не изменяется и не добавляется.**
12. **TDD обязателен.** Сначала падающий тест, запуск, **реальный вывод падения в отчёт
    по ходу дела**, затем реализация.
13. **Названное поведение обязано краснеть при регрессии.** Для каждого теста, закрывающего
    инвариант, применить мутацию, убедиться что краснеет, откатить, убедиться что зеленеет,
    записать оба исхода.
14. **`cargo test`, `cargo fmt --check`, `cargo clippy --all-targets` чистые перед коммитом.**
15. **Коммиты:** Conventional Commits, subject **не длиннее 50 символов — посчитать**.
    Автор `tarodo` настроен глобально. Только заголовок и, при необходимости, тело по
    существу. **Запрещены любые трейлеры соавторства и любые футеры об инструменте.**

---

## Два предупреждения, которые определяют порядок задач

**Наивная обёртка «повторить `run()`» уничтожила бы то, ради чего реконнект и делается.**
Сегодня `run()` создаёт трекер, сборщик и кэш **после** `stream.start()`. Обернуть её в
цикл повторов значит на каждой попытке заново пройти холодный guard (который умеет только
проверять существование слота) и обнулить трекер — а именно его durable-позиция и есть то,
с чем `check_reconnect` должен сравнивать `confirmed_flush_lsn` слота. Поэтому задача 1 —
чистый рефакторинг без изменения поведения, и её ревью обязано это подтвердить.

**`--ack-interval-ms` уже задаёт три темпа сразу:** период барьера, таймаут чтения и частоту
проверки гейта keepalive. Бэкофф реконнекта **не выводится** из него и получает собственные
параметры. Связать их значило бы, что попытка ускорить подтверждение учащает попытки
переподключения к упавшему серверу.

---

## File Structure

| Файл | Что меняется |
|------|--------------|
| `src/postgres/replication.rs` | Расщепление `run()`, внешний цикл реконнекта, завершение по сигналу |
| `src/postgres/guard.rs` | Появляется вызывающий у `check_reconnect` |
| `src/config.rs` | Параметры бэкоффа |
| `Cargo.toml` | Возвращается фича `signal` у tokio |
| `tests/integration.rs` | Реконнект, коды возврата, лимит транзакции |
| `tests/restart.rs` | Новый бинарь тестов: сценарий §18 с настоящим `kill` |
| `tests/common/mod.rs` | Помощники для перезапуска контейнера |

---

### Task 1: Расщепить `run()` и поднять состояние

Чистый рефакторинг. Поведение не меняется ни на байт — это и есть критерий приёмки.
Задача существует, чтобы следующая могла обернуть сессию в цикл, не обнуляя позиции.

**Files:**
- Modify: `src/postgres/replication.rs`

**Interfaces:**
- Produces:
  ```rust
  /// Состояние, которое переживает обрыв соединения.
  pub(crate) struct SessionState {
      tracker: LsnTracker,
      assembler: Assembler,
      cache: RelationCache,
  }
  impl SessionState {
      fn new(max_transaction_events: usize) -> Self;
      /// Кэш и сборщик сбрасываются, трекер — НЕТ.
      fn reset_for_reconnect(&mut self);
      fn durable(&self) -> Lsn;
  }
  /// Одна сессия репликации: preflight, подключение, цикл. Возвращается
  /// при обрыве или при штатном завершении.
  async fn stream_once(
      config: &Config,
      sink: &mut Box<dyn Sink>,
      state: &mut SessionState,
  ) -> Result<SessionOutcome, PgcdcError>;
  enum SessionOutcome { Disconnected, ShutdownRequested }
  ```

- [ ] **Step 1: Написать падающие тесты на состояние сессии**

Добавить в блок тестов `src/postgres/replication.rs`:

```rust
    #[test]
    fn reconnect_resets_the_cache_and_the_assembler() {
        // Кэш живёт в рамках сессии: сервер перешлёт RELATION в новой сессии,
        // а старое описание могло устареть, пока нас не было. Недособранная
        // транзакция придёт заново целиком — её BEGIN был после confirmed_flush_lsn.
        let mut s = SessionState::new(1000);
        s.cache.put(crate::schema::Relation {
            id: 1,
            namespace: "public".into(),
            name: "users".into(),
            replica_identity: b'f',
            columns: vec![],
        });
        s.assembler
            .handle(
                crate::postgres::pgoutput::PgOutputMessage::Begin {
                    final_lsn: 0x1000,
                    commit_timestamp: 0,
                    xid: 7,
                },
                Lsn(0x100),
                &mut s.cache,
            )
            .unwrap();
        assert_eq!(s.cache.len(), 1);
        assert!(!s.assembler.is_empty());

        s.reset_for_reconnect();

        assert_eq!(s.cache.len(), 0, "кэш сбрасывается целиком");
        assert!(s.assembler.is_empty(), "недособранная транзакция выбрасывается");
    }

    #[test]
    fn reconnect_carries_the_tracker_positions_forward() {
        // Позиции НЕ сбрасываются. Обнулить трекер значило бы потерять
        // durable-позицию, с которой check_reconnect сравнивает слот, — и
        // заодно открыть гейт keepalive в момент, когда replay ещё не догнал.
        let mut s = SessionState::new(1000);
        s.tracker.note_received(Lsn(0x3000));
        s.tracker.note_processed(Lsn(0x2000));
        s.tracker.note_durable(Lsn(0x2000));
        s.tracker.try_ack(Lsn(0x2000)).unwrap();

        s.reset_for_reconnect();

        assert_eq!(s.durable(), Lsn(0x2000), "durable переносится");
        assert_eq!(s.tracker.acked(), Lsn(0x2000), "подтверждённая переносится");
        assert_eq!(s.tracker.processed(), Lsn(0x2000), "processed переносится");
    }

    #[test]
    fn replayed_transactions_cannot_move_positions_backwards() {
        // После реконнекта сервер отдаёт заново всё после confirmed_flush_lsn.
        // Позиции монотонны, поэтому повторная обработка их не откатывает,
        // а гейт keepalive остаётся закрытым, пока replay не догонит processed.
        let mut s = SessionState::new(1000);
        s.tracker.note_processed(Lsn(0x2000));
        s.tracker.note_durable(Lsn(0x2000));
        s.reset_for_reconnect();
        s.tracker.note_processed(Lsn(0x1000));
        assert_eq!(s.tracker.processed(), Lsn(0x2000));
    }
```

- [ ] **Step 2: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib replication 2>&1 | tail -25
```

Ожидается ошибка компиляции: нет `SessionState`. Вставить реальный вывод в отчёт.

- [ ] **Step 3: Реализовать `SessionState`**

Добавить в `src/postgres/replication.rs` перед `run`:

```rust
/// Состояние, которое переживает обрыв соединения.
///
/// Разделение здесь не косметическое. Позиции трекера **переносятся** через
/// реконнект: они монотонны, поэтому replay уже обработанных транзакций не
/// может сдвинуть их назад, а durable-позиция — это ровно то, с чем
/// `check_reconnect` сравнивает `confirmed_flush_lsn` слота. Обнулить трекер
/// значило бы уничтожить единственный вход этой проверки.
///
/// Кэш отношений и сборщик, наоборот, **сбрасываются**: кэш живёт в рамках
/// сессии репликации и после разрыва может описывать устаревшую схему
/// (DECISIONS Q19), а недособранная транзакция придёт заново целиком, потому
/// что её BEGIN был после `confirmed_flush_lsn`.
pub(crate) struct SessionState {
    tracker: LsnTracker,
    assembler: Assembler,
    cache: RelationCache,
}

impl SessionState {
    fn new(max_transaction_events: usize) -> Self {
        Self {
            tracker: LsnTracker::new(),
            assembler: Assembler::new(max_transaction_events),
            cache: RelationCache::new(),
        }
    }

    fn reset_for_reconnect(&mut self) {
        self.cache.clear();
        self.assembler.reset();
    }

    fn durable(&self) -> Lsn {
        self.tracker.durable()
    }
}

/// Чем закончилась одна сессия репликации.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionOutcome {
    /// Соединение оборвалось. Внешний цикл решает, переподключаться ли.
    Disconnected,
    /// Пришёл сигнал завершения и текущая группа доведена до барьера.
    ShutdownRequested,
}
```

- [ ] **Step 4: Расщепить `run` на внешнюю и сессионную части**

Переписать `run` так, чтобы она только проверяла URL, создавала состояние и один раз
вызывала `stream_once`. Всё, что было между `preflight_cold_start` и концом цикла,
переезжает в `stream_once` без изменений — включая порядок операций, гейт keepalive и
все комментарии, объясняющие почему.

```rust
pub async fn run(config: Config, mut sink: Box<dyn Sink>) -> Result<(), PgcdcError> {
    // Первым делом — до любого подключения и любого лога, где могла бы всплыть строка.
    config.database_url.validate()?;

    let mut state = SessionState::new(config.max_transaction_events);

    // Внешний цикл появится в следующей задаче. Пока одна сессия: обрыв —
    // это ошибка, как и было.
    match stream_once(&config, &mut sink, &mut state).await? {
        SessionOutcome::ShutdownRequested => Ok(()),
        SessionOutcome::Disconnected => Err(PgcdcError::Connection(
            "replication stream ended".to_string(),
        )),
    }
}
```

В `stream_once` тело цикла меняется в двух местах, и только в них:

- обращения `tracker`/`assembler`/`cache` становятся `state.tracker` и так далее;
- ветка `Ok(Err(e))` вместо `return Err(...)` возвращает
  `Ok(SessionOutcome::Disconnected)` **после** записи предупреждения:

```rust
            Ok(Err(e)) => {
                warn!(error = %e, "postgres_connection_lost");
                return Ok(SessionOutcome::Disconnected);
            }
```

Возврат обрыва как значения, а не как ошибки, — это то, что позволит следующей задаче
отличить «переподключаемся» от «падаем», не разбирая текст ошибки.

- [ ] **Step 5: Прогнать всё**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test 2>&1 | /usr/bin/grep -E '^test result'
cargo fmt --check && echo "fmt clean"
cargo clippy --all-targets 2>&1 | tail -5
```

Ожидается: **все существующие тесты проходят без единой правки**. Это и есть критерий
того, что рефакторинг чистый. Если пришлось поправить хоть один существующий тест —
поведение изменилось, и это надо разобрать, а не подогнать.

- [ ] **Step 6: Проверить мутацией, что перенос позиций несущий**

Временно добавить `self.tracker = LsnTracker::new();` в `reset_for_reconnect`, выполнить
`cargo test --lib replication`, убедиться что тест `reconnect_carries_the_tracker_positions_forward`
краснеет, откатить, убедиться что зеленеет. Записать оба вывода.

- [ ] **Step 7: Коммит**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git add src/postgres/replication.rs
git commit -m "refactor: hoist session state above the connect"
```

---

### Task 2: Реконнект с бэкоффом и проверкой слота

Здесь `check_reconnect`, написанный два этапа назад и с тех пор не вызывавшийся, наконец
получает вызывающего.

**Files:**
- Modify: `src/postgres/replication.rs`, `src/config.rs`, `tests/integration.rs`,
  `tests/common/mod.rs`

**Interfaces:**
- Consumes: `SessionState`, `SessionOutcome`, `check_reconnect`, `preflight_cold_start`.
- Produces: поля `pub reconnect_initial_ms: u64` (дефолт 100) и
  `pub reconnect_max_ms: u64` (дефолт 30000) в `Config`;
  функция `fn next_backoff(current: Duration, max: Duration) -> Duration`.

- [ ] **Step 1: Добавить параметры бэкоффа**

В `src/config.rs`:

```rust
    /// Начальная пауза перед первой попыткой переподключения.
    /// Намеренно НЕ выводится из `ack_interval_ms`: тот задаёт период барьера,
    /// таймаут чтения и частоту проверки гейта keepalive сразу, и связать их
    /// значило бы, что попытка ускорить подтверждение учащает долбёжку
    /// упавшего сервера.
    #[arg(long, env = "PGCDC_RECONNECT_INITIAL_MS", default_value = "100",
          value_parser = clap::value_parser!(u64).range(1..))]
    pub reconnect_initial_ms: u64,

    /// Потолок паузы. Экспоненциальный рост останавливается здесь и дальше
    /// повторяет попытки бесконечно: сетевой сбой не повод ронять процесс
    /// (DECISIONS Q19).
    #[arg(long, env = "PGCDC_RECONNECT_MAX_MS", default_value = "30000",
          value_parser = clap::value_parser!(u64).range(1..))]
    pub reconnect_max_ms: u64,
```

- [ ] **Step 2: Написать падающий тест на бэкофф**

Добавить в блок тестов `src/postgres/replication.rs`:

```rust
    #[test]
    fn backoff_doubles_until_it_reaches_the_ceiling() {
        let max = Duration::from_millis(1000);
        assert_eq!(next_backoff(Duration::from_millis(100), max), Duration::from_millis(200));
        assert_eq!(next_backoff(Duration::from_millis(400), max), Duration::from_millis(800));
        assert_eq!(next_backoff(Duration::from_millis(800), max), max, "упирается в потолок");
        assert_eq!(next_backoff(max, max), max, "и остаётся на нём");
    }

    #[test]
    fn backoff_cannot_overflow() {
        // Удвоение у самого верха диапазона не должно паниковать в debug-сборке.
        let huge = Duration::from_millis(u64::MAX / 2 + 1);
        assert_eq!(next_backoff(huge, Duration::from_millis(1000)), Duration::from_millis(1000));
    }
```

- [ ] **Step 3: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib replication 2>&1 | tail -20
```

Ожидается ошибка компиляции: нет `next_backoff`.

- [ ] **Step 4: Реализовать бэкофф и внешний цикл**

Добавить в `src/postgres/replication.rs`:

```rust
/// Удвоение с потолком. `saturating_mul` вместо `*` — чтобы удвоение у верха
/// диапазона не паниковало в debug-сборке.
fn next_backoff(current: Duration, max: Duration) -> Duration {
    let doubled = current.saturating_mul(2);
    if doubled > max {
        max
    } else {
        doubled
    }
}
```

И переписать `run`:

```rust
pub async fn run(config: Config, mut sink: Box<dyn Sink>) -> Result<(), PgcdcError> {
    config.database_url.validate()?;

    let mut state = SessionState::new(config.max_transaction_events);
    let initial = Duration::from_millis(config.reconnect_initial_ms);
    let max = Duration::from_millis(config.reconnect_max_ms);
    let mut backoff = initial;
    let mut attempt: u32 = 0;

    loop {
        let received_before = state.tracker.received();

        match stream_once(&config, &mut sink, &mut state).await {
            Ok(SessionOutcome::ShutdownRequested) => return Ok(()),
            Ok(SessionOutcome::Disconnected) => {}
            // Восстановимые ошибки ведут в реконнект, фатальные — наружу.
            // Классификация живёт в типе (`is_fatal`), а не в разборе текста.
            Err(e) if !e.is_fatal() => {
                warn!(error = %e, error_kind = e.kind(), "postgres_connection_lost");
            }
            Err(e) => return Err(e),
        }

        // Продуктивная сессия сбрасывает бэкофф. Без этого один долгий простой
        // навсегда оставлял бы паузу на потолке, и следующий одиночный сбой
        // через неделю ждал бы полминуты впустую. Признак продуктивности —
        // сессия сдвинула принятую позицию, то есть реально что-то прочитала.
        if state.tracker.received() > received_before {
            backoff = initial;
            attempt = 0;
        }

        attempt += 1;
        warn!(retry = attempt, backoff_ms = backoff.as_millis() as u64, "reconnecting");
        tokio::time::sleep(backoff).await;
        backoff = next_backoff(backoff, max);

        // Кэш и сборщик сбрасываются, позиции переносятся.
        state.reset_for_reconnect();
    }
}
```

И в начале `stream_once`, **после** холодного preflight, добавить проверку реконнекта —
она осмысленна только когда durable-позиция уже не нулевая, то есть на втором и
последующих подключениях:

```rust
    // Проверка реконнекта: на холодном старте сравнивать не с чем, durable ещё
    // ноль. На повторном подключении позиция в памяти есть, и сверка ничего не
    // стоит. Слот ВПЕРЁД нашей durable-точки означает, что кто-то подтвердил
    // WAL, который мы не довели до sink, — падаем. Слот ПОЗАДИ — ожидаемый
    // исход обрыва: последний feedback мог не дойти. Пишем предупреждение и
    // продолжаем, промежуток перечитается дубликатами (DECISIONS R11 этапа 0).
    if state.durable() > Lsn(0) {
        if let Some(warning) = check_reconnect(&config.slot, &info_slot, state.durable())? {
            warn!("{warning}");
        }
        // Успешное восстановление отмечаем только после того, как проверка прошла.
        info!(slot = %config.slot, "postgres_connection_restored");
    }
```

- [ ] **Step 5: Добавить помощник перезапуска контейнера**

В `tests/common/mod.rs`:

Обрыв воспроизводится изнутри базы, а не перезапуском контейнера: это не зависит от
версии testcontainers, выполняется мгновенно и точнее воспроизводит именно сетевой обрыв,
а не полный рестарт сервера.

```rust
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
```

- [ ] **Step 6: Написать интеграционный тест на реконнект**

В `tests/integration.rs`:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_connection_is_recovered_without_losing_rows() {
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
        .execute("INSERT INTO users VALUES (1, 'before', NULL, NULL)", &[])
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(20), tx_recv.recv())
        .await
        .expect("первая транзакция")
        .expect("канал закрыт");
    assert_eq!(first.changes[0].after.as_ref().unwrap().get("name").unwrap(), "before");

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

    handle.abort();
}
```

- [ ] **Step 7: Запустить и проверить мутацией**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --test integration a_dropped_connection 2>&1 | tail -20
```

Затем мутация: заменить ветку `Ok(SessionOutcome::Disconnected) => {}` на
`Ok(SessionOutcome::Disconnected) => return Ok(())`. Тест обязан покраснеть — вторая
строка не приедет. Откатить, убедиться что зеленеет. Записать оба исхода.

Вторая мутация: убрать `state.reset_for_reconnect()` из внешнего цикла. Тест может
остаться зелёным — сброс защищает от устаревшей схемы, а не от потери строк, и в этом
сценарии схема не менялась. Записать фактический исход; если тест краснеет, значит сброс
несёт смысл, которого мы не заметили, и это надо разобрать.

- [ ] **Step 8: Прогнать всё и закоммитить**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test 2>&1 | /usr/bin/grep -E '^test result'
cargo fmt --check && echo "fmt clean"
cargo clippy --all-targets 2>&1 | tail -5
git add src/postgres/replication.rs src/config.rs tests/
git commit -m "feat: reconnect with backoff after a drop"
```

---

### Task 3: Завершение по сигналу, коды возврата, лимит транзакции

Спека §15 требует: ничто, способное потерять события, не завершается с кодом 0.
Обратное тоже верно — штатное завершение обязано давать 0, иначе супервизор будет
бесконечно перезапускать корректно остановленный процесс.

**Files:**
- Modify: `Cargo.toml`, `src/postgres/replication.rs`, `tests/integration.rs`

**Interfaces:**
- Produces: `fn spawn_shutdown_listener() -> Arc<AtomicBool>`.
- Изменяет сигнатуру: `stream_once` получает четвёртый параметр
  `shutdown: &Arc<AtomicBool>`, а `run` создаёт флаг один раз до внешнего цикла и
  передаёт одну и ту же ссылку в каждую сессию — иначе после реконнекта процесс
  перестал бы реагировать на сигнал.

- [ ] **Step 1: Вернуть фичу сигналов**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo add tokio --features signal
/usr/bin/grep -A 2 '^tokio' Cargo.toml
```

Фича `signal` была снята в этапе 2 как неиспользуемая. Теперь она нужна.

- [ ] **Step 2: Написать интеграционный тест на код возврата при штатной остановке**

В `tests/integration.rs`:

```rust
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

    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"))
        .args([
            "--database-url", &conn,
            "--publication", "pgcdc_pub",
            "--slot", "pgcdc_slot",
            "--output", "file",
            "--output-path", out.to_str().unwrap(),
        ])
        .spawn()
        .expect("запустить бинарь");

    client
        .execute("INSERT INTO users VALUES (1, 'Alice', NULL, NULL)", &[])
        .await
        .unwrap();

    // Ждём, пока строка окажется в файле, — значит процесс дошёл до барьера.
    let mut seen = false;
    for _ in 0..200 {
        if std::fs::read_to_string(&out).map(|t| !t.trim().is_empty()).unwrap_or(false) {
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
```

Для `libc::kill` добавить dev-зависимость:

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo add --dev libc
```

- [ ] **Step 3: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --test integration a_terminated_process 2>&1 | tail -20
```

Ожидается падение: сейчас SIGTERM убивает процесс сигналом, и `status.code()` даёт `None`.

- [ ] **Step 4: Реализовать завершение по сигналу**

В `src/postgres/replication.rs` добавить перед `stream_once`:

```rust
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Ставит флаг по SIGTERM или SIGINT. Флаг проверяется в начале каждого прохода
/// цикла; поскольку чтение и так ограничено по времени, задержка реакции не
/// превышает `ack_interval`. Это проще, чем городить select вокруг чтения, и
/// не трогает порядок операций, проверенный мутационно.
fn spawn_shutdown_listener() -> Arc<AtomicBool> {
    let flag = Arc::new(AtomicBool::new(false));
    let f = flag.clone();
    tokio::spawn(async move {
        let mut term = match tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate(),
        ) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "cannot install SIGTERM handler");
                return;
            }
        };
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
        f.store(true, Ordering::Relaxed);
    });
    flag
}
```

В `run` создать флаг один раз и передать его в `stream_once`. В начале тела цикла
`stream_once`:

```rust
        if shutdown.load(Ordering::Relaxed) {
            // Довести принятое до барьера и подтвердить, прежде чем выйти.
            // Выйти раньше значило бы потерять уже принятые транзакции.
            if let Some(durable) = sink.flush().await? {
                state.tracker.note_durable(durable);
                state.tracker.try_ack(durable)?;
                let acked = state.tracker.acked();
                stream.shared_lsn_feedback.update_flushed_lsn(acked.0);
                stream.shared_lsn_feedback.update_applied_lsn(acked.0);
                stream
                    .send_feedback()
                    .await
                    .map_err(|e| PgcdcError::Connection(format!("send_feedback: {e}")))?;
            }
            info!("shutdown_requested");
            return Ok(SessionOutcome::ShutdownRequested);
        }
```

- [ ] **Step 5: Написать интеграционный тест на лимит транзакции**

```rust
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
        pgcdc::postgres::replication::run(cfg, Box::new(ChannelSink(tx_send, None))).await
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
    assert!(matches!(err, PgcdcError::TransactionTooLarge { limit: 2 }), "получили {err:?}");
    assert!(err.is_fatal(), "превышение лимита — фатальная ошибка, а не повод для ретрая");

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
```

- [ ] **Step 6: Прогнать всё и проверить мутацией**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test 2>&1 | /usr/bin/grep -E '^test result'
cargo fmt --check && echo "fmt clean"
cargo clippy --all-targets 2>&1 | tail -5
```

Мутация: сделать `TransactionTooLarge` невосстановимой ошибкой восстановимой — заменить
её арм в `is_fatal` на `false`. Тест на лимит обязан покраснеть: процесс уйдёт в
бесконечный реконнект вместо падения. Откатить, убедиться что зеленеет.

Вторая мутация: убрать блок барьера из ветки завершения по сигналу. Тест на SIGTERM
может остаться зелёным, если строка успела попасть в файл до сигнала, — записать
фактический исход и, если он зелёный, отметить это как известный предел теста.

- [ ] **Step 7: Коммит**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git add Cargo.toml Cargo.lock src/postgres/replication.rs tests/integration.rs
git commit -m "feat: graceful shutdown and exit codes"
```

---

### Task 4: Restart-тест — главный тест этапа

Сценарий §18 базовой спеки целиком: потребить строки, убить процесс, вставить ещё,
запустить заново, убедиться что ни одна закоммиченная строка не пропала. Дубликаты
допустимы, пропуски — нет.

**Files:**
- Create: `tests/restart.rs`
- Modify: `README.md`

**Interfaces:**
- Consumes: `common::{start_postgres, connect, setup_schema, create_slot}`.

- [ ] **Step 1: Написать restart-тест**

Создать `tests/restart.rs`:

```rust
mod common;

use std::time::Duration;

/// Сценарий §18 базовой спеки. Единственный тест, проверяющий обещание
/// «дубликаты допустимы, тихая потеря — нет» на всей цепочке сразу:
/// настоящий бинарь, настоящий PostgreSQL, настоящий SIGKILL, настоящий файл.
#[tokio::test(flavor = "multi_thread")]
async fn no_committed_row_is_lost_across_a_hard_restart() {
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let out = std::env::temp_dir().join(format!("pgcdc-restart-{}.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&out);

    let spawn = |path: &std::path::Path| {
        std::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"))
            .args([
                "--database-url", &conn,
                "--publication", "pgcdc_pub",
                "--slot", "pgcdc_slot",
                "--output", "file",
                "--output-path", path.to_str().unwrap(),
                // Короткий барьер, чтобы тест не ждал долго.
                "--ack-interval-ms", "100",
            ])
            .spawn()
            .expect("запустить бинарь")
    };

    // Первый прогон: строки 1..5.
    let mut child = spawn(&out);
    for id in 1..=5 {
        client
            .execute("INSERT INTO users VALUES ($1, 'x', NULL, NULL)", &[&(id as i64)])
            .await
            .unwrap();
    }
    wait_for_ids(&out, &[1, 2, 3, 4, 5]).await;

    // Убиваем жёстко: не SIGTERM, а SIGKILL — процессу не дают ничего доделать.
    child.kill().expect("kill");
    let _ = tokio::task::spawn_blocking(move || child.wait()).await.unwrap();

    // Пока нас нет — ещё строки.
    for id in 6..=10 {
        client
            .execute("INSERT INTO users VALUES ($1, 'x', NULL, NULL)", &[&(id as i64)])
            .await
            .unwrap();
    }

    // Второй прогон дописывает в тот же файл.
    let mut child = spawn(&out);
    wait_for_ids(&out, &[6, 7, 8, 9, 10]).await;
    child.kill().expect("kill");
    let _ = tokio::task::spawn_blocking(move || child.wait()).await.unwrap();

    let text = std::fs::read_to_string(&out).expect("прочитать вывод");
    let ids = collect_ids(&text);
    for id in 1..=10 {
        assert!(
            ids.contains(&id.to_string()),
            "строка {id} потеряна; в выводе: {ids:?}"
        );
    }

    let _ = std::fs::remove_file(&out);
}

/// Ждёт, пока в файле не появятся все перечисленные идентификаторы.
/// Ограничено по времени: если не появились, тест падает с тем, что видел.
async fn wait_for_ids(path: &std::path::Path, want: &[i32]) {
    for _ in 0..300 {
        if let Ok(text) = std::fs::read_to_string(path) {
            let ids = collect_ids(&text);
            if want.iter().all(|w| ids.contains(&w.to_string())) {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let text = std::fs::read_to_string(path).unwrap_or_default();
    panic!("не дождались {want:?}; в выводе: {:?}", collect_ids(&text));
}

/// Собирает значения колонки `id` из всех строк JSONL. Неполная последняя
/// строка игнорируется: процесс могли убить посреди записи.
fn collect_ids(text: &str) -> Vec<String> {
    text.lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|v| v["after"]["id"].as_str().map(|s| s.to_string()))
        .collect()
}
```

- [ ] **Step 2: Запустить**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --test restart 2>&1 | tail -25
```

Ожидается: тест проходит. Если какая-то строка потеряна — это настоящий дефект
подтверждения, а не проблема теста, и разбираться надо с ним.

- [ ] **Step 3: Проверить, что тест ловит настоящую потерю**

Мутация: в `src/postgres/replication.rs` заменить `tracker.try_ack(durable)?` на
подтверждение позиции **до** барьера — то есть отметить durable и подтвердить
`state.tracker.processed()` сразу после `write_transaction`, а барьер оставить в
таймерной ветке. Тогда после SIGKILL слот окажется впереди того, что попало в файл, и
строки между последним fsync и подтверждением пропадут.

Выполнить `cargo test --test restart`, убедиться что тест краснеет с перечислением
потерянных идентификаторов. Откатить, убедиться что зеленеет. Записать оба вывода.

Это единственный тест проекта, проверяющий обещание «тихая потеря недопустима» на всей
цепочке сразу; если он не краснеет под этой мутацией, он не закрывает ничего.

- [ ] **Step 4: Обновить README**

В разделе «Что уже работает» добавить: переподключение с экспоненциальным бэкоффом,
штатная остановка по сигналу с нулевым кодом возврата, переживание жёсткого перезапуска.
В разделе «Гарантии» добавить предложение о том, что после жёсткого перезапуска
возможны дубликаты вокруг границы падения, но ни одна закоммиченная строка не теряется,
и что это проверяется тестом `no_committed_row_is_lost_across_a_hard_restart`.

- [ ] **Step 5: Прогнать всё и закоммитить**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test 2>&1 | /usr/bin/grep -E '^test result'
cargo fmt --check && echo "fmt clean"
cargo clippy --all-targets 2>&1 | tail -5
git add tests/restart.rs README.md
git commit -m "test: prove no loss across a hard restart"
```

---

## Definition of Done для этапа 4

- [ ] `cargo test`, `cargo fmt --check`, `cargo clippy --all-targets` чистые;
- [ ] состояние сессии поднято выше подключения; позиции трекера переносятся через
      реконнект, кэш и сборщик сбрасываются, и обе половины проверены мутацией;
- [ ] обрыв соединения приводит к переподключению с экспоненциальным бэкоффом от
      начального значения до потолка, без лимита попыток;
- [ ] бэкофф имеет собственные параметры и не выводится из интервала подтверждения;
- [ ] `check_reconnect` вызывается на повторных подключениях: слот впереди durable —
      фатально, позади — предупреждение и продолжение;
- [ ] фатальные ошибки не уходят в реконнект, а классификация берётся из типа;
- [ ] SIGTERM и SIGINT доводят принятое до барьера, подтверждают и дают код 0;
- [ ] превышение лимита событий в транзакции фатально и не двигает слот;
- [ ] restart-тест проходит: после SIGKILL и перезапуска ни одна из десяти строк не
      потеряна, и тест краснеет под мутацией подтверждения до барьера;
- [ ] существующий тест на отсутствующий слот продолжает проходить: слот не создаётся,
      код возврата ненулевой, и реконнект в этом случае НЕ включается;
- [ ] порядок операций в цикле и гейт keepalive не изменены;
- [ ] ни один файл в `tests/fixtures/` не изменён.
