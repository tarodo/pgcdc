# pgcdc Этап 5 (Обвязка) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Сделать процесс наблюдаемым снаружи и закрыть чек-лист §20 базовой спеки целиком.

**Architecture:** Счётчики живут в собственной структуре на атомиках, без фасада: значения нужны тестам напрямую, а не экспортёру, которого пока нет. Структура прокидывается тем же способом, что и флаг завершения, и переживает реконнект. Подтверждённая позиция имеет ровно одно место записи — это и делает возможными два теста, которые ждали счётчика с прошлых этапов. Логи остаются событийными и структурными; добавляется только периодическая сводка.

**Tech Stack:** Rust 1.95.0 (Homebrew), tokio, `pg_walstream` 0.8, serde_json, chrono, clap, tracing, testcontainers (dev), PostgreSQL 16 и Docker Compose для демо.

**Spec:** [DECISIONS.md](../../../DECISIONS.md) — Q22, Q23, Q26 и инварианты §1. Базовая спека [input/pgcdc_mvp_task.md](../../../input/pgcdc_mvp_task.md) §16 (логи), §17 (метрики), §19 (демо), §20 (чек-лист).

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
6. **Порядок операций в цикле, гейт продвижения по keepalive и подтверждаемая позиция
   не меняются.** Все три проверены мутационно; изменение любого — критический дефект.
7. **Разрешён только `next_raw_event`.** Пять API восстановления запрещены.
8. **Каждый интеграционный тест несёт `flavor = "multi_thread"`.** Параллелизм тестов
   ограничен четырьмя потоками через закоммиченный `.cargo/config.toml`.
9. **Ни один файл в `tests/fixtures/` не изменяется и не добавляется.**
10. **Полезная нагрузка строк не логируется** (§16 базовой спеки). Счётчики и позиции —
    можно, содержимое колонок — нет.
11. **TDD обязателен.** Сначала падающий тест, запуск, **реальный вывод падения в отчёт
    по ходу дела**, затем реализация.
12. **Названное поведение обязано краснеть при регрессии.** Для каждого теста, закрывающего
    инвариант или заявленное свойство, применить мутацию, убедиться что краснеет, откатить,
    убедиться что зеленеет, записать оба исхода.
13. **`cargo test`, `cargo fmt --check`, `cargo clippy --all-targets` чистые перед коммитом.**
14. **Коммиты:** Conventional Commits, subject **не длиннее 50 символов — посчитать**.
    Автор `tarodo` настроен глобально. Только заголовок и, при необходимости, тело по
    существу. **Запрещены любые трейлеры соавторства и любые футеры об инструменте.**

---

## Что этот этап наконец закрывает

Два теста ждут счётчика подтверждённой позиции с прошлых этапов, и оба ждут по одной причине:
**наблюдать за слотом сервера недостаточно, надо наблюдать за нашим собственным подтверждением.**

- Тест `insert_travels_end_to_end_and_arrives_as_one_event` когда-то сверял позицию слота
  на точное равенство с `end_lsn` транзакции — это была самая сильная формулировка обещания
  «подтверждаем `end_lsn`, а не `commit_lsn`». Продвижение слота по keepalive из этапа 3
  сделало равенство недостижимым: фоновая запись `XLOG_RUNNING_XACTS` законно уводит слот
  дальше. Проверку ослабили до «не меньше», и она перестала различать: под мутацией
  «подтверждать `commit_lsn`» keepalive всё равно уведёт слот за `end_lsn`.
- Признак продуктивности сессии для сброса бэкоффа сравнивает подтверждённую позицию.
  Ничто не ловит подмену её на принятую, потому что расхождение этих двух возникает только
  на простаивающей публикации.

Счётчик `last_acknowledged_lsn` пишется ровно в одном месте — в общей хвостовой части
подтверждения, извлечённой в этапе 4 именно ради этого. Он читает наше решение, а не
состояние сервера, и потому детерминирован.

---

## File Structure

| Файл | Ответственность |
|------|-----------------|
| `src/metrics.rs` | Счётчики на атомиках и их снимок |
| `src/lib.rs` | Реэкспорт модуля |
| `src/postgres/replication.rs` | Инкременты счётчиков, периодическая сводка |
| `src/transaction.rs` | `Assembler::len` для счётчика буфера |
| `src/main.rs` | Создание счётчиков |
| `tests/integration.rs` | Тесты по счётчикам, перенесённые дефекты |
| `Dockerfile` | Сборка образа для демо |
| `docker-compose.yml` | Профиль `demo` |
| `README.md` | Демо, флаги, гарантии, закрытый чек-лист |

---

### Task 1: Перенесённые дефекты этапа 4

Четыре пункта, унаследованных с прошлого этапа. Три из них — комментарии, описывающие
механизм, которого нет; это пятый случай такого класса в проекте, и потому они не
откладываются дальше.

**Files:**
- Modify: `src/postgres/replication.rs`, `tests/integration.rs`, `README.md`

**Interfaces:**
- Ничего нового наружу.

- [ ] **Step 1: Исправить ложную посылку в комментарии раннего выхода**

В `src/postgres/replication.rs`, в ветке внешнего цикла, проверяющей флаг завершения,
комментарий утверждает, что данных, которые sink принял и не довёл до барьера, там нет.
Это неверно: после обрыва посреди окна подтверждения транзакция, отданная в
`write_transaction`, остаётся в буфере писателя неслитой и неподтверждённой, и ранний
выход пропускает тот самый слив, который делает его собрат внутри сессии.

Поведение при этом правильное и менять его не нужно: эти события не подтверждены, слот
их перечитает, дубликаты контракт разрешает. Переписать комментарий так:

```rust
        // Сигнал во внешнем цикле. Выходим с нулём, но НЕ потому, что доводить
        // нечего — после обрыва посреди окна подтверждения в буфере писателя
        // вполне может лежать принятая, но не слитая транзакция, и этот путь
        // пропускает слив, который делает ветка внутри сессии. Ноль корректен
        // по другой причине: непроведённое через барьер не было и подтверждено,
        // поэтому слот отдаст его заново, а дубликаты разрешает инвариант 2.
        // Терять здесь нечего, и это не то же самое, что «нечего доводить».
```

- [ ] **Step 2: Исправить утверждение «единственное место»**

Тот же комментарий называет себя единственным местом, где внешний цикл смотрит на флаг,
хотя нарезанная пауза двадцатью пятью строками ниже — второе такое место, добавленное тем
же коммитом. Убрать слово «единственное» и назвать оба места.

- [ ] **Step 3: Исправить обещание границы задержки в документации слушателя**

Доккомментарий `spawn_shutdown_listener` утверждает, что обе точки чтения флага ограничены
одной величиной — интервалом опроса. Это неверно: между ними лежит окно из preflight,
установки соединения и старта репликации, ни одно из которых не ограничено по времени и не
смотрит на флаг. Против отказанного порта это мгновенно, но против чёрной дыры сигнал
может не замечаться десятки секунд. Дописать это ограничение честно.

- [ ] **Step 4: Закрепить нарезанную паузу тестом**

Тест `sigterm_is_honored_while_stuck_reconnecting_to_a_dead_port` не различает нарезанную
паузу и цельную: он задаёт максимум бэкоффа, равный интервалу опроса, и при таком значении
одна пауза неотличима от нарезки. Поднять `--reconnect-max-ms` в этом тесте до 3000, оставив
бюджет ожидания прежним: тогда цельная пауза съест сигнал на три секунды, а нарезанная — нет.

- [ ] **Step 5: Проверить обеими мутациями**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
```

Мутация A: убрать проверку флага в начале тела внешнего цикла. Тест обязан покраснеть.
Мутация B: вернуть цельную `tokio::time::sleep(delay).await` вместо нарезанной, оставив
проверку в начале цикла. Тест **тоже** обязан покраснеть — до правки шага 4 он оставался
зелёным. Каждую мутацию откатить и убедиться, что тест зеленеет. Записать все четыре вывода.

- [ ] **Step 6: Добавить тест на SIGINT**

Чек-лист заявляет обработку SIGINT наравне с SIGTERM, но теста на неё нет. Добавить в
`tests/integration.rs` копию проверки штатной остановки, отправляющую `libc::SIGINT`:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn sigint_also_stops_the_process_cleanly() {
    // Чек-лист заявляет оба сигнала; SIGTERM закрыт отдельным тестом,
    // а SIGINT до сих пор держался только на том, что слушатель их
    // объединяет в один select. Проверяем, что объединение работает.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let out = std::env::temp_dir().join(format!("pgcdc-sigint-{}.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&out);

    let mut child = common::KillOnDrop::new(
        std::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"))
            .args([
                "--database-url", &conn,
                "--publication", "pgcdc_pub",
                "--slot", "pgcdc_slot",
                "--output", "file",
                "--output-path", out.to_str().unwrap(),
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
    let status = tokio::task::spawn_blocking(move || child.wait())
        .await
        .unwrap()
        .expect("wait");
    assert_eq!(status.code(), Some(0), "SIGINT тоже даёт ноль");

    let _ = std::fs::remove_file(&out);
}
```

Обрати внимание: `KillOnDrop` реализует `Deref`/`DerefMut` к `Child`, поэтому `child.id()`
и `child.wait()` вызываются напрямую. Страж при этом остаётся: после явного `wait()` его
`Drop` увидит, что процесс уже пожат, и второй раз убивать не станет.

- [ ] **Step 7: Дописать README о реконнекте**

В README нет ни флагов `--reconnect-initial-ms` и `--reconnect-max-ms`, ни контракта
«слот впереди нашей durable-позиции — фатально». Добавить оба: флаги в перечень
конфигурации, контракт — в раздел о гарантиях, одним абзацем о том, что слот впереди
означает подтверждённый кем-то WAL, который мы не записывали, и это повод остановиться,
а не продолжать.

- [ ] **Step 8: Прогнать всё и закоммитить**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test 2>&1 | /usr/bin/grep -E '^test result'
cargo fmt --check && echo "fmt clean"
cargo clippy --all-targets 2>&1 | tail -5
git add src/postgres/replication.rs tests/integration.rs README.md
git commit -m "fix: correct reconnect comments, pin sliced sleep"
```

---

### Task 2: Счётчики

Не фасад: значения нужны тестам напрямую. Фасад без экспортёра отправляет их в никуда,
а два теста ждут именно возможности прочитать подтверждённую позицию.

**Files:**
- Create: `src/metrics.rs`
- Modify: `src/lib.rs`, `src/postgres/replication.rs`, `src/transaction.rs`, `src/main.rs`,
  `tests/integration.rs`, `tests/restart.rs`

**Interfaces:**
- Produces:
  ```rust
  pub struct Metrics { /* восемь AtomicU64 */ }
  impl Metrics {
      pub fn new() -> Self;
      pub fn snapshot(&self) -> MetricsSnapshot;
  }
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub struct MetricsSnapshot {
      pub events_total: u64,
      pub transactions_total: u64,
      pub bytes_received_total: u64,
      pub reconnects_total: u64,
      pub errors_total: u64,
      pub last_received_lsn: u64,
      pub last_acknowledged_lsn: u64,
      pub transaction_buffer_size: u64,
  }
  // и в src/transaction.rs
  impl Assembler { pub fn len(&self) -> usize; }
  // сигнатура точки входа получает третий параметр
  pub async fn run(config: Config, sink: Box<dyn Sink>, metrics: Arc<Metrics>)
      -> Result<(), PgcdcError>;
  ```

- [ ] **Step 1: Написать падающие тесты счётчиков**

Создать `src/metrics.rs` с блоком тестов:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_start_at_zero_and_accumulate() {
        let m = Metrics::new();
        assert_eq!(m.snapshot().events_total, 0);
        m.add_events(3);
        m.add_events(2);
        assert_eq!(m.snapshot().events_total, 5);
    }

    #[test]
    fn positions_are_set_not_added() {
        // Позиция — не счётчик: она заменяется, а не накапливается.
        let m = Metrics::new();
        m.set_last_acknowledged_lsn(0x1000);
        m.set_last_acknowledged_lsn(0x2000);
        assert_eq!(m.snapshot().last_acknowledged_lsn, 0x2000);
    }

    #[test]
    fn a_position_never_moves_backwards() {
        // Тот же довод, что и у трекера: replay уже обработанного не должен
        // откатывать наблюдаемую позицию, иначе график лжёт о прогрессе.
        let m = Metrics::new();
        m.set_last_acknowledged_lsn(0x2000);
        m.set_last_acknowledged_lsn(0x1000);
        assert_eq!(m.snapshot().last_acknowledged_lsn, 0x2000);
    }

    #[test]
    fn buffer_size_is_a_gauge_and_may_fall() {
        // А вот размер буфера — не позиция: он обязан падать до нуля на коммите.
        let m = Metrics::new();
        m.set_transaction_buffer_size(17);
        m.set_transaction_buffer_size(0);
        assert_eq!(m.snapshot().transaction_buffer_size, 0);
    }
}
```

- [ ] **Step 2: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib metrics 2>&1 | tail -20
```

Ожидается ошибка компиляции: нет модуля `metrics`.

- [ ] **Step 3: Реализовать счётчики**

Добавить `pub mod metrics;` в `src/lib.rs` и написать `src/metrics.rs` перед тестами:

```rust
use std::sync::atomic::{AtomicU64, Ordering};

/// Счётчики процесса. Своя структура, а не фасад вроде `metrics-rs`: фасад без
/// подключённого экспортёра отправляет значения в никуда, а нам они нужны прямо
/// в тестах — «после отказа sink подтверждённая позиция не сдвинулась» это
/// утверждение о счётчике (DECISIONS Q23). Обернуть это экспортёром позже
/// тривиально; вернуть наблюдаемость фасаду — нет.
///
/// Все поля — `Relaxed`: это наблюдение, а не синхронизация. Ни одно решение
/// в коде не принимается по значению счётчика, поэтому упорядочивание между
/// ними не нужно и стоило бы дороже.
#[derive(Debug, Default)]
pub struct Metrics {
    events_total: AtomicU64,
    transactions_total: AtomicU64,
    bytes_received_total: AtomicU64,
    reconnects_total: AtomicU64,
    errors_total: AtomicU64,
    last_received_lsn: AtomicU64,
    last_acknowledged_lsn: AtomicU64,
    transaction_buffer_size: AtomicU64,
}

/// Согласованный по полям снимок. Нужен и периодической сводке, и тестам:
/// читать восемь атомиков по отдельности в ассерте — значит получить
/// значения из разных моментов.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub events_total: u64,
    pub transactions_total: u64,
    pub bytes_received_total: u64,
    pub reconnects_total: u64,
    pub errors_total: u64,
    pub last_received_lsn: u64,
    pub last_acknowledged_lsn: u64,
    pub transaction_buffer_size: u64,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_events(&self, n: u64) {
        self.events_total.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_transaction(&self) {
        self.transactions_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_bytes(&self, n: u64) {
        self.bytes_received_total.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_reconnect(&self) {
        self.reconnects_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Позиции монотонны по той же причине, что и в трекере: replay уже
    /// обработанного не должен откатывать наблюдаемый прогресс.
    pub fn set_last_received_lsn(&self, lsn: u64) {
        self.last_received_lsn.fetch_max(lsn, Ordering::Relaxed);
    }

    pub fn set_last_acknowledged_lsn(&self, lsn: u64) {
        self.last_acknowledged_lsn.fetch_max(lsn, Ordering::Relaxed);
    }

    /// Размер буфера — датчик, а не позиция: он обязан падать до нуля на коммите.
    pub fn set_transaction_buffer_size(&self, n: u64) {
        self.transaction_buffer_size.store(n, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            events_total: self.events_total.load(Ordering::Relaxed),
            transactions_total: self.transactions_total.load(Ordering::Relaxed),
            bytes_received_total: self.bytes_received_total.load(Ordering::Relaxed),
            reconnects_total: self.reconnects_total.load(Ordering::Relaxed),
            errors_total: self.errors_total.load(Ordering::Relaxed),
            last_received_lsn: self.last_received_lsn.load(Ordering::Relaxed),
            last_acknowledged_lsn: self.last_acknowledged_lsn.load(Ordering::Relaxed),
            transaction_buffer_size: self.transaction_buffer_size.load(Ordering::Relaxed),
        }
    }
}
```

- [ ] **Step 4: Добавить длину буфера сборщику**

В `src/transaction.rs`, рядом с `is_empty`:

```rust
    /// Сколько изменений накоплено в открытой транзакции. Для счётчика
    /// `transaction_buffer_size`; на решения в коде не влияет.
    pub fn len(&self) -> usize {
        self.open.as_ref().map_or(0, |o| o.changes.len())
    }
```

И тест рядом с существующими тестами сборщика:

```rust
    #[test]
    fn buffer_length_grows_with_changes_and_empties_on_commit() {
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        assert_eq!(a.len(), 0);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(PgOutputMessage::Relation(users_relation()), Lsn(0), &mut cache).unwrap();
        assert_eq!(a.len(), 0, "BEGIN сам по себе изменений не добавляет");
        a.handle(insert(), Lsn(0x200), &mut cache).unwrap();
        assert_eq!(a.len(), 1);
        a.handle(commit(), Lsn(0x1000), &mut cache).unwrap();
        assert_eq!(a.len(), 0, "коммит опустошает буфер");
    }
```

- [ ] **Step 5: Прокинуть счётчики через точку входа**

`run` получает третий параметр. Это не «ещё один аргумент ради тестов»: снаружи процесса
счётчики читать пока нечем, а Q23 прямо требует, чтобы утверждения о них делались в тестах.

В `src/postgres/replication.rs` изменить сигнатуру `run` и `stream_once`, прокинув
`&Arc<Metrics>` так же, как уже прокинут флаг завершения. В `src/main.rs` создать
`Arc::new(Metrics::new())` и передать.

Расставить инкременты ровно в этих местах и нигде больше:

- в ветке приёма кадра, рядом с `note_received`: `metrics.add_bytes(raw.data.len() as u64)`
  и `metrics.set_last_received_lsn(raw.wal_end.0)`;
- там же, после `assembler.handle`, независимо от результата:
  `metrics.set_transaction_buffer_size(state.assembler.len() as u64)`;
- в ветке собранной транзакции, рядом с `note_processed`:
  `metrics.add_transaction()` и `metrics.add_events(tx.changes.len() as u64)`;
- в `acknowledge_durable`, сразу после `try_ack`:
  `metrics.set_last_acknowledged_lsn(acked.0)` — **единственное** место записи этой позиции;
- во внешнем цикле, там где логируется попытка переподключения: `metrics.add_reconnect()`;
- там же, в ветке восстановимой ошибки: `metrics.add_error()`.

- [ ] **Step 6: Обновить все места вызова `run` в тестах**

Каждый вызов получает `Arc::new(Metrics::new())`, кроме тех тестов, которые счётчики
читают. Пройти по `tests/integration.rs` и `tests/restart.rs`.

- [ ] **Step 7: Написать тест, закрывающий перенесённое различение**

Это то, ради чего счётчик и заводился. Добавить в `tests/integration.rs`:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn we_acknowledge_the_end_of_the_commit_record_not_its_start() {
    // Перенесено с этапа 3. Раньше это проверялось по позиции слота на точное
    // равенство, но продвижение слота по keepalive сделало равенство
    // недостижимым: фоновая запись WAL законно уводит слот дальше, и
    // ослабленная проверка «не меньше» перестала различать подмену
    // end_lsn на commit_lsn — keepalive увёл бы слот за end_lsn в обоих
    // случаях. Счётчик читает НАШЕ решение, а не состояние сервера,
    // и потому различает.
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
```

- [ ] **Step 8: Написать тест на счётчик при отказе sink**

```rust
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
    assert_eq!(snap.last_acknowledged_lsn, 0, "барьер не прошёл — подтверждать нечего");
    assert!(snap.transactions_total >= 1, "но транзакция была принята и посчитана");
}
```

- [ ] **Step 9: Прогнать всё и проверить мутациями**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test 2>&1 | /usr/bin/grep -E '^test result'
cargo fmt --check && echo "fmt clean"
cargo clippy --all-targets 2>&1 | tail -5
```

Две мутации, каждую применить, прогнать, откатить, записать оба исхода:

1. В `acknowledge_durable` записать в счётчик не `acked`, а `commit_lsn` собираемой
   транзакции — тест из шага 7 обязан покраснеть. Если для этого нужен доступ к
   `commit_lsn`, которого там нет, это само по себе хороший знак: подмену трудно
   сделать случайно. В таком случае мутировать проще — записать `acked.0 - 0x30`,
   имитируя сдвиг на длину записи коммита, и убедиться что тест краснеет.
2. Убрать вызов `metrics.set_last_acknowledged_lsn` целиком — оба теста, из шагов 7 и 8,
   должны повести себя по-разному: первый покраснеет, второй останется зелёным, потому что
   он утверждает ноль. Это ожидаемо и подтверждает, что тесты проверяют разные вещи.

- [ ] **Step 10: Коммит**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git add src/metrics.rs src/lib.rs src/postgres/replication.rs src/transaction.rs src/main.rs tests/
git commit -m "feat(metrics): add counters and acked position"
```

---

### Task 3: Периодическая сводка и тест на сброс бэкоффа

**Files:**
- Modify: `src/postgres/replication.rs`, `tests/integration.rs`

**Interfaces:**
- Consumes: `Metrics`, `MetricsSnapshot`.

- [ ] **Step 1: Добавить периодическую сводку**

Спека §16 показывает `INFO transaction_committed` на каждую транзакцию. На тысяче
транзакций в секунду это тысяча строк лога в секунду — лог становится и узким местом,
и мусором. Поэтому пособытийная строка остаётся на `DEBUG`, а на `INFO` выходит сводка
раз в десять секунд (DECISIONS Q23).

В `src/postgres/replication.rs`, рядом с отсчётом барьера, завести второй отсчёт и
выводить сводку:

```rust
/// Как часто выходит сводная строка. Не конфигурируется: это не поведение, а
/// громкость, и десять секунд — компромисс между «видно, что процесс жив» и
/// «лог не забивается».
const METRICS_REPORT_INTERVAL: Duration = Duration::from_secs(10);
```

```rust
        if last_report.elapsed() >= METRICS_REPORT_INTERVAL {
            last_report = tokio::time::Instant::now();
            let s = metrics.snapshot();
            info!(
                events = s.events_total,
                transactions = s.transactions_total,
                bytes = s.bytes_received_total,
                reconnects = s.reconnects_total,
                errors = s.errors_total,
                last_received_lsn = %Lsn(s.last_received_lsn),
                last_acknowledged_lsn = %Lsn(s.last_acknowledged_lsn),
                buffer = s.transaction_buffer_size,
                "metrics_report"
            );
        }
```

Полезной нагрузки строк здесь нет и быть не должно — только счётчики и позиции (§16).

- [ ] **Step 2: Написать тест на сброс бэкоффа**

Перенесено с этапа 4. Тогда сброс сочли непроверяемым, потому что бэкофф якобы не виден
снаружи процесса. Он виден: каждая попытка пишет строку с задержкой структурным полем.

```rust
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
        "--database-url", &conn,
        "--publication", "pgcdc_pub",
        "--slot", "pgcdc_slot",
        "--output", "file",
        "--output-path", out.to_str().unwrap(),
        "--reconnect-initial-ms", "100",
        "--reconnect-max-ms", "800",
    ]);

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
    // и различает их только вторая. Со сбросом получится [100, 100]; без него
    // вторая серия продолжит с удвоенной — [100, 200].
    let delays = common::collect_backoff_delays(&mut child, 2).await;
    assert_eq!(
        delays.get(1).copied(),
        Some(100),
        "после продуктивной сессии бэкофф обязан начаться заново, а не продолжить: {delays:?}"
    );

    let _ = std::fs::remove_file(&out);
}
```

Добавить оба помощника в `tests/common/mod.rs`:

```rust
/// Порождает бинарь с перехваченным stderr, обёрнутый в существующий страж,
/// чтобы падение теста не оставило процесс, который будет вечно
/// переподключаться уже после того, как контейнер исчезнет.
pub fn spawn_with_stderr(args: &[&str]) -> KillOnDrop {
    KillOnDrop::new(
        std::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"))
            .args(args)
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("запустить бинарь"),
    )
}

/// Читает stderr потомка и возвращает первые `n` задержек бэкоффа из строк
/// события переподключения. Бюджет ограничен: если задержек не набралось,
/// падаем с тем, что действительно увидели, а не висим.
pub async fn collect_backoff_delays(child: &mut KillOnDrop, n: usize) -> Vec<u64> {
    use std::io::{BufRead, BufReader};

    let stderr = child.stderr.take().expect("stderr перехвачен при запуске");
    let handle = tokio::task::spawn_blocking(move || {
        let mut found = Vec::new();
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            if !line.contains("reconnecting") {
                continue;
            }
            // Поле пишется структурно: ищем его по имени, а не по позиции.
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

    match tokio::time::timeout(Duration::from_secs(20), handle).await {
        Ok(Ok(found)) if found.len() >= n => found,
        Ok(Ok(found)) => panic!("нашли только {} задержек из {n}: {found:?}", found.len()),
        Ok(Err(e)) => panic!("чтение stderr упало: {e}"),
        Err(_) => panic!("не дождались {n} задержек за 20 секунд"),
    }
}
```

Обрати внимание: `child.stderr.take()` работает через `DerefMut` стража, а чтение идёт в
блокирующем потоке — построчное чтение трубы блокирует, и держать его в асинхронной задаче
нельзя.

- [ ] **Step 3: Запустить и проверить мутацией**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --test integration a_productive_session 2>&1 | tail -20
```

Мутация: убрать сброс бэкоффа внутри его структуры. Тест обязан покраснеть — вторая серия
начнётся с удвоенной задержки, а не с начальной. Откатить, убедиться что зеленеет.

Вторая мутация: вернуть признак продуктивности к принятой позиции вместо подтверждённой.
Записать фактический исход. Если тест останется зелёным, значит в этом сценарии обе позиции
двигаются вместе, и различение по-прежнему не покрыто — так и написать, а не подгонять.

- [ ] **Step 4: Прогнать всё и закоммитить**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test 2>&1 | /usr/bin/grep -E '^test result'
cargo fmt --check && echo "fmt clean"
cargo clippy --all-targets 2>&1 | tail -5
git add src/postgres/replication.rs tests/
git commit -m "feat: add periodic metrics report line"
```

---

### Task 4: Демо и закрытие чек-листа

**Files:**
- Create: `Dockerfile`, `.dockerignore`
- Modify: `docker-compose.yml`, `README.md`

**Interfaces:**
- Ничего в коде.

- [ ] **Step 1: Выяснить доступный базовый образ**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
docker pull rust:1-slim 2>&1 | tail -3
docker run --rm rust:1-slim rustc --version
```

Записать фактическую версию в отчёт. Если она ниже 1.95, взять `rust:slim` и проверить
снова; если и там ниже — сообщить, не выдумывая тег.

- [ ] **Step 2: Написать `.dockerignore`**

```
target
.git
.superpowers
docs
tests
```

Без этого контекст сборки утащит весь `target`, который весит гигабайты.

- [ ] **Step 3: Написать `Dockerfile`**

Двухстадийная сборка: компилируем в образе с тулчейном, кладём бинарь в тонкий образ.

```dockerfile
# Тег и версию подставить фактические, из шага 1.
FROM rust:1-slim AS build
WORKDIR /src
# Сначала манифесты: слой с зависимостями переиспользуется, пока они не менялись.
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --bin pgcdc

FROM debian:stable-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/pgcdc /usr/local/bin/pgcdc
# Логи идут в stderr, полезная нагрузка — в stdout, поэтому вывод контейнера
# можно направлять в конвейер без фильтрации.
ENTRYPOINT ["/usr/local/bin/pgcdc"]
```

- [ ] **Step 4: Добавить профиль демо в compose**

В `docker-compose.yml` добавить сервис, не трогая существующий:

```yaml
  pgcdc:
    profiles: ["demo"]
    build: .
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      PGCDC_DATABASE_URL: postgres://postgres:postgres@postgres:5432/app
      PGCDC_PUBLICATION: pgcdc_pub
      PGCDC_SLOT: pgcdc_slot
      PGCDC_OUTPUT: stdout
```

Профиль означает, что обычный `docker compose up -d` по-прежнему поднимает только базу, а
демо запускается явно.

- [ ] **Step 5: Прогнать демо целиком**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
docker compose down -v
docker compose up -d --wait
docker compose --profile demo build 2>&1 | tail -5
docker compose --profile demo up -d pgcdc
export PGPASSWORD=postgres
psql -h 127.0.0.1 -U postgres -d app -c "INSERT INTO users VALUES (1,'Alice','alice@example.com',NULL);"
psql -h 127.0.0.1 -U postgres -d app -c "UPDATE users SET name='Bob' WHERE id=1;"
psql -h 127.0.0.1 -U postgres -d app -c "DELETE FROM users WHERE id=1;"
for i in $(seq 1 60); do
  n=$(docker compose logs pgcdc 2>/dev/null | /usr/bin/grep -c '"operation"' || true)
  [ "$n" -ge 3 ] && break
done
docker compose logs pgcdc | /usr/bin/grep '"operation"'
docker compose --profile demo down -v
```

Ожидается три строки JSON: вставка, обновление, удаление. Записать в отчёт фактический
вывод и время сборки образа.

- [ ] **Step 6: Дописать README**

Раздел с демо привести к тому, что реально работает, включая профиль. Добавить раздел о
наблюдаемости: имена счётчиков, что сводная строка выходит раз в десять секунд на INFO,
что пособытийные строки на DEBUG, и что полезная нагрузка строк не логируется.

- [ ] **Step 7: Пройти чек-лист §20 пункт за пунктом**

Открыть `input/pgcdc_mvp_task.md` §20 и для **каждого** из восемнадцати пунктов записать в
отчёт: закрыт или нет, и чем именно — имя теста, файл, или команда, которую выполнил.
Не «да», а доказательство. Если какой-то пункт не закрыт — так и написать; закрывать его
задним числом формулировкой запрещено.

- [ ] **Step 8: Прогнать всё и закоммитить**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test 2>&1 | /usr/bin/grep -E '^test result'
cargo fmt --check && echo "fmt clean"
cargo clippy --all-targets 2>&1 | tail -5
git add Dockerfile .dockerignore docker-compose.yml README.md
git commit -m "feat: add demo image and compose profile"
```

---

## Definition of Done для этапа 5

- [ ] `cargo test`, `cargo fmt --check`, `cargo clippy --all-targets` чистые;
- [ ] три комментария в пути реконнекта описывают то, что код действительно делает;
- [ ] нарезанная пауза бэкоффа закреплена тестом, краснеющим при возврате к цельной;
- [ ] SIGINT покрыт тестом наравне с SIGTERM;
- [ ] восемь счётчиков из §17 существуют и инкрементируются;
- [ ] подтверждённая позиция имеет ровно одно место записи;
- [ ] тест доказывает, что подтверждается `end_lsn`, а не `commit_lsn`, читая счётчик,
      а не позицию слота, и краснеет при подмене;
- [ ] тест доказывает, что при непройденном барьере подтверждённая позиция остаётся нулевой;
- [ ] сброс бэкоффа после продуктивной сессии покрыт тестом;
- [ ] сводная строка выходит раз в десять секунд на INFO, пособытийные — на DEBUG,
      полезная нагрузка строк не логируется;
- [ ] `docker compose --profile demo` собирает образ и печатает три события на вставку,
      обновление и удаление;
- [ ] чек-лист §20 пройден пункт за пунктом с доказательством на каждый;
- [ ] ни один файл в `tests/fixtures/` не изменён.
