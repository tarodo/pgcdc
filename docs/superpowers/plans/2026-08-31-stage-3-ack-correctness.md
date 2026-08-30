# pgcdc Этап 3 (Корректность подтверждений) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Довести подтверждение позиций WAL до промышленного вида: файловый sink с настоящим fsync, групповое подтверждение по таймеру и продвижение слота по keepalive — не нарушив инвариант «подтверждаем только то, что записано».

**Architecture:** Трейт `Sink` разделяется на запись и барьер durability: `write_transaction` больше не означает «на диске», а отметка durable переезжает на вызов `flush`. Цикл репликации получает ограниченное по времени чтение, чтобы простаивающий поток всё равно доходил до обработки тика. Продвижение слота по keepalive гейтится условием строго сильнее «буфер пуст».

**Tech Stack:** Rust 1.95.0 (Homebrew), tokio, `pg_walstream` 0.8, serde_json, chrono, clap, tracing, testcontainers (dev), PostgreSQL 16 в Docker.

**Spec:** [DECISIONS.md](../../../DECISIONS.md) — в особенности инвариант 1, Q4, Q5, Q6, Q17, Q18 и Q26. Обязательства этапа записаны в Q26 как решения, а не как возможности: их не переспоривают, их исполняют.

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
6. **Инвариант 1 нерушим:** `acked <= durable` всегда. Единственное исключение прописано
   в Q26(b) и работает не через ослабление проверки, а через явную отметку durable.
7. **Порядок в цикле остаётся: запись → durable → подтверждение → отправка feedback.**
   Меняется только то, ЧТО считается моментом durable: не возврат `write_transaction`,
   а успешный `flush`.
8. **Запрещены пять API `pg_walstream`**, ведущих в `recover_connection`:
   `next_event_with_retry`, `check_connection_health`, `into_stream`, `stream`,
   `for_each_event`. Разрешён только `next_raw_event`.
9. **Ни один файл в `tests/fixtures/` не изменяется и не добавляется.**
10. **TDD обязателен.** Сначала падающий тест, запуск, **реальный вывод падения в отчёт по
    ходу дела**, затем реализация. Если красный шаг неожиданно прошёл — так и написать.
11. **Названное поведение обязано краснеть при регрессии.** Для каждого теста, который
    закрывает инвариант, применить мутацию, убедиться что тест краснеет, откатить,
    убедиться что зеленеет, и записать оба исхода. Тест, не падающий под собственной
    мутацией, — это имитация покрытия.
12. **`cargo test`, `cargo fmt --check`, `cargo clippy --all-targets` чистые перед коммитом.**
13. **Коммиты:** Conventional Commits, subject **не длиннее 50 символов — посчитать**.
    Автор `tarodo` настроен глобально. Только заголовок и, при необходимости, тело по
    существу. **Запрещены любые трейлеры соавторства и любые футеры об инструменте.**

---

## Факт, который определяет весь этап

`pg_walstream` выбирает драйвер соединения по флейвору текущего рантайма tokio.
`Connection::prefer_inline_driver()` (`connection/native/connection.rs:400`) возвращает
`true`, только если рантайм **многопоточный**; иначе берётся `Threaded`.

Разница между ними — не в производительности, а в поведении при отмене:

- **`Inline`** при отмене сливает буфер чтения и **возвращает уже готовое сообщение**, если
  оно есть (`connection/native/copy.rs:73-88`). У крейта есть отдельный тест на это —
  `test_get_copy_data_cancelled_with_buffered_data`. Буферы `read_buf` и `pending` живут на
  соединении, а не во future, поэтому переживают отмену.
- **`Threaded`** при отмене делает `*batch_rx = None` (`connection.rs:645-650`), то есть
  **роняет приёмник вместе со всеми батчами**, которые воркер уже успел в него положить.
  Это кадры, которые сервер прислал, а мы не увидим никогда.

Следствие, которое надо осознать до написания кода: `#[tokio::main]` по умолчанию даёт
многопоточный рантайм, а `#[tokio::test]` — **однопоточный**. Значит прод работает на
`Inline`, а все наши интеграционные тесты до сих пор работали на `Threaded`. Тесты
проверяют не тот драйвер, который эксплуатируется. Для этапа, который вводит ограниченное
по времени чтение, это недопустимо: мы бы тестировали ровно тот путь, который теряет кадры,
и не тестировали тот, на котором работаем.

Отсюда два решения этапа, оба реализуются в задаче 1:

1. Все интеграционные тесты переводятся на `#[tokio::test(flavor = "multi_thread")]`.
2. Ограниченное чтение делается через `tokio::time::timeout` вокруг `next_raw_event`.
   Это безопасно **именно и только** потому, что мы на `Inline`: `AsyncReadExt::read_buf`
   в tokio cancel-safe, а буфер, в который он читает, принадлежит соединению. На `Threaded`
   тот же приём терял бы кадры.

---

## File Structure

| Файл | Что меняется |
|------|--------------|
| `src/lsn.rs` | Четвёртая позиция `processed` и её монотонность |
| `src/sink/mod.rs` | Барьер `flush` в трейте, отдельный от записи |
| `src/sink/stdout.rs` | Реализация `flush` |
| `src/sink/file.rs` | Новый sink: JSONL с fsync |
| `src/config.rs` | `--output-path`, `--ack-interval-ms`, вариант `file` |
| `src/error.rs` | Вариант ошибки для sink-файла |
| `src/postgres/replication.rs` | Ограниченное чтение, групповой ACK, keepalive |
| `src/main.rs` | Выбор sink по конфигурации |
| `tests/integration.rs` | Многопоточный флейвор, новые сценарии |
| `tests/common/mod.rs` | Помощник ожидания позиции слота |

---

### Task 1: Перенесённый дефект, четвёртая позиция, выравнивание рантайма

Три вещи, каждая из которых должна быть на месте до того, как что-то в цикле сдвинется.

**Files:**
- Modify: `src/lsn.rs`, `src/transaction.rs`, `tests/integration.rs`, `tests/guard.rs`,
  `docs/spike-findings.md`

**Interfaces:**
- Produces:
  ```rust
  impl LsnTracker {
      pub fn note_processed(&mut self, lsn: Lsn);
      pub fn processed(&self) -> Lsn;
  }
  ```

- [ ] **Step 1: Закрыть перенесённый дефект — проверка `lsn` на событии DELETE**

Этап 2 закрылся с одной незакрытой находкой: поле `lsn` у события DELETE не проверяется
нигде, и мутация это доказала. Добавить в существующий тест
`delete_with_key_tuple_reports_only_the_columns_that_arrived` в `src/transaction.rs`:

```rust
        assert_eq!(ev.lsn, Lsn(0x200), "у события — wal_start своей строки");
```

- [ ] **Step 2: Проверить мутацией, что новая проверка работает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
```

Временно заменить в ветке `Delete` в `src/transaction.rs` строку `lsn: wal_start` на
`lsn: Lsn(0)`, выполнить `cargo test --lib`, убедиться что тест покраснел, вернуть
`lsn: wal_start`, убедиться что позеленел. Записать оба вывода в отчёт. Тест, не падающий
под этой мутацией, ничего не закрывает.

- [ ] **Step 3: Написать падающий тест на четвёртую позицию**

`processed` — позиция, до которой сообщения разобраны и переданы в sink, но ещё не
обязательно доведены до диска. Она нужна условию Q26(a): подтверждать позицию из keepalive
можно только когда буфер пуст **и** `processed` догнала `durable`.

Добавить в блок тестов `src/lsn.rs`:

```rust
    #[test]
    fn processed_is_tracked_separately_and_moves_forward_only() {
        let mut t = LsnTracker::new();
        t.note_received(Lsn(0x3000));
        t.note_processed(Lsn(0x2000));
        assert_eq!(t.processed(), Lsn(0x2000));
        t.note_processed(Lsn(0x1000));
        assert_eq!(t.processed(), Lsn(0x2000), "позиция не откатывается");
    }

    #[test]
    fn processed_may_run_ahead_of_durable() {
        // Ровно та ситуация, ради которой позиция и заведена: транзакция
        // отдана в sink, но fsync ещё не случился.
        let mut t = LsnTracker::new();
        t.note_processed(Lsn(0x2000));
        assert_eq!(t.durable(), Lsn(0));
        assert!(t.processed() > t.durable());
        assert!(t.try_ack(Lsn(0x2000)).is_err(), "подтверждать по processed нельзя");
    }
```

- [ ] **Step 4: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib lsn 2>&1 | tail -20
```

Ожидается ошибка компиляции: нет `note_processed` и `processed`.

- [ ] **Step 5: Реализовать четвёртую позицию**

В `src/lsn.rs` добавить поле в структуру и два метода:

```rust
    processed: Lsn,
```

```rust
    /// Позиция, до которой сообщения разобраны и отданы sink'у. Может опережать
    /// `durable`: между записью и fsync существует окно, и именно из-за него
    /// условие продвижения по keepalive (Q26a) требует `processed == durable`,
    /// а не только пустого буфера сборщика.
    pub fn note_processed(&mut self, lsn: Lsn) {
        if lsn > self.processed {
            self.processed = lsn;
        }
    }

    pub fn processed(&self) -> Lsn {
        self.processed
    }
```

- [ ] **Step 6: Перевести интеграционные тесты на многопоточный рантайм**

Это не косметика. `pg_walstream` выбирает драйвер соединения по флейвору рантайма:
многопоточный даёт `Inline`, однопоточный — `Threaded`, и они по-разному ведут себя при
отмене чтения. `#[tokio::main]` в `src/main.rs` даёт многопоточный, а `#[tokio::test]` по
умолчанию однопоточный — то есть до сих пор тесты проверяли не тот драйвер, на котором
работает прод.

Заменить **каждый** `#[tokio::test]` в `tests/integration.rs` и `tests/guard.rs` на:

```rust
#[tokio::test(flavor = "multi_thread")]
```

- [ ] **Step 7: Записать находку в заметки по транспорту**

Добавить в `docs/spike-findings.md` §3 новый обходной путь, коротко:

```markdown
### Обходной путь 6: рантайм тестов обязан совпадать с продом

`Connection::prefer_inline_driver()` (connection/native/connection.rs:400) выбирает
драйвер по флейвору текущего рантайма tokio: многопоточный → `Inline`,
однопоточный → `Threaded`. Разница проявляется при отмене чтения:

- `Inline` (copy.rs:73-88) при отмене сливает буфер чтения и возвращает уже готовое
  сообщение, если оно есть. У крейта на это есть собственный тест
  `test_get_copy_data_cancelled_with_buffered_data`. Буферы живут на соединении,
  а не во future.
- `Threaded` (connection.rs:645-650) при отмене делает `*batch_rx = None`, роняя
  приёмник вместе со всеми батчами, которые воркер уже туда положил. Это потеря
  кадров, которые сервер прислал.

`#[tokio::main]` даёт многопоточный рантайм, `#[tokio::test]` — однопоточный. Поэтому
все интеграционные тесты обязаны нести `flavor = "multi_thread"`, иначе они проверяют
драйвер, который в проде не используется. Введено в этапе 3.
```

- [ ] **Step 8: Прогнать всё**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test 2>&1 | /usr/bin/grep -E '^test result'
cargo fmt --check && echo "fmt clean"
cargo clippy --all-targets 2>&1 | tail -5
```

Ожидается: всё зелёное. Интеграционные тесты теперь поднимают многопоточный рантайм и
могут идти чуть иначе по времени — если какой-то стал нестабильным, это сигнал, что он
зависел от однопоточного порядка, и об этом надо написать в отчёте, а не подгонять таймаут.

- [ ] **Step 9: Коммит**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git add src/lsn.rs src/transaction.rs tests/ docs/spike-findings.md
git commit -m "feat(lsn): add processed position, align runtimes"
```

---

### Task 2: Барьер durability в трейте Sink

Сегодня трейт утверждает три вещи разом: «пиши всё или падай», «`Fsync` значит на диске»,
и «`Ok` от записи означает durable». Групповое подтверждение делает все три ложными
одновременно. Разделяем запись и барьер (Q26c).

**Files:**
- Modify: `src/sink/mod.rs`, `src/sink/stdout.rs`, `src/postgres/replication.rs`

**Interfaces:**
- Produces:
  ```rust
  #[async_trait::async_trait]
  pub trait Sink: Send {
      fn durability(&self) -> Durability;
      /// Принять транзакцию. Возврат `Ok` НЕ означает durable.
      async fn write_transaction(&mut self, tx: &Transaction) -> Result<(), PgcdcError>;
      /// Довести всё принятое до носителя. Возвращает наибольшую позицию,
      /// ставшую durable, или `None`, если с прошлого раза ничего не принималось.
      async fn flush(&mut self) -> Result<Option<Lsn>, PgcdcError>;
  }
  ```

- [ ] **Step 1: Написать падающий тест на разделение записи и durability**

Добавить в блок тестов `src/sink/mod.rs`:

```rust
    /// Считает вызовы и запоминает, что было принято, но ещё не доведено.
    struct CountingSink {
        accepted: Vec<Lsn>,
        flushed: Vec<Lsn>,
        flush_calls: usize,
    }

    #[async_trait::async_trait]
    impl Sink for CountingSink {
        fn durability(&self) -> Durability {
            Durability::Fsync
        }
        async fn write_transaction(&mut self, tx: &Transaction) -> Result<(), PgcdcError> {
            self.accepted.push(tx.end_lsn);
            Ok(())
        }
        async fn flush(&mut self) -> Result<Option<Lsn>, PgcdcError> {
            self.flush_calls += 1;
            let last = self.accepted.last().copied();
            self.flushed.extend(self.accepted.drain(..));
            Ok(last)
        }
    }

    #[tokio::test]
    async fn accepting_a_transaction_does_not_make_it_durable() {
        // Это и есть смысл разделения: между приёмом и барьером существует окно,
        // и подтверждать позицию внутри него нельзя.
        let mut s = CountingSink { accepted: vec![], flushed: vec![], flush_calls: 0 };
        s.write_transaction(&tx()).await.unwrap();
        assert!(s.flushed.is_empty(), "запись сама по себе ничего не доводит");
        assert_eq!(s.flush_calls, 0);
    }

    #[tokio::test]
    async fn flush_reports_the_highest_position_it_made_durable() {
        let mut s = CountingSink { accepted: vec![], flushed: vec![], flush_calls: 0 };
        s.write_transaction(&tx()).await.unwrap();
        let durable = s.flush().await.unwrap();
        assert_eq!(durable, Some(Lsn(0x1030)), "барьер отчитывается позицией");
        assert_eq!(s.flushed.len(), 1);
    }

    #[tokio::test]
    async fn flush_with_nothing_accepted_reports_no_new_position() {
        // Важно для цикла: пустой тик не должен двигать durable.
        let mut s = CountingSink { accepted: vec![], flushed: vec![], flush_calls: 0 };
        assert_eq!(s.flush().await.unwrap(), None);
    }
```

- [ ] **Step 2: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib sink 2>&1 | tail -20
```

Ожидается ошибка компиляции: у трейта нет метода `flush`.

- [ ] **Step 3: Добавить барьер в трейт**

В `src/sink/mod.rs` заменить объявление трейта и уточнить документацию `Durability`:

```rust
/// Что sink обещает про запись ПОСЛЕ успешного `flush`.
/// К возврату `write_transaction` это отношения не имеет.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// После `flush` данные доведены до диска: подтверждать позицию безопасно.
    Fsync,
    /// После `flush` байты отданы ядру, но их судьба неизвестна. Для разработки.
    BestEffort,
}

#[async_trait::async_trait]
pub trait Sink: Send {
    fn durability(&self) -> Durability;

    /// Принять транзакцию целиком. Возврат `Ok` означает «принято», а НЕ «durable»:
    /// между приёмом и барьером существует окно, и подтверждать позицию внутри него
    /// запрещено инвариантом 1.
    async fn write_transaction(&mut self, tx: &Transaction) -> Result<(), PgcdcError>;

    /// Довести до носителя всё, что было принято с прошлого барьера.
    /// Возвращает наибольшую позицию, ставшую durable, либо `None`, если
    /// принимать было нечего. Только после `Ok(Some(lsn))` вызывающий имеет
    /// право отметить `lsn` как durable.
    async fn flush(&mut self) -> Result<Option<Lsn>, PgcdcError>;
}
```

Добавить `use crate::lsn::Lsn;` в начало файла.

- [ ] **Step 4: Реализовать `flush` в stdout-sink**

В `src/sink/stdout.rs` добавить поле и метод. `StdoutSink` перестаёт быть unit-структурой:

```rust
/// JSONL на stdout: одна строка на изменение. Только для разработки —
/// durability у трубы нет, и это объявлено честно.
#[derive(Debug, Default)]
pub struct StdoutSink {
    /// Наибольшая принятая позиция с прошлого барьера.
    pending: Option<Lsn>,
}

impl StdoutSink {
    pub fn new() -> Self {
        Self::default()
    }
}
```

```rust
    async fn write_transaction(&mut self, tx: &Transaction) -> Result<(), PgcdcError> {
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        write_changes(&mut out, tx)?;
        self.pending = Some(tx.end_lsn);
        Ok(())
    }

    async fn flush(&mut self) -> Result<Option<Lsn>, PgcdcError> {
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        out.flush().map_err(|e| PgcdcError::Sink(format!("flush: {e}")))?;
        Ok(self.pending.take())
    }
```

Добавить `use std::io::Write;` и `use crate::lsn::Lsn;`, если их нет.

- [ ] **Step 5: Перенести отметку durable на вызов барьера**

В `src/postgres/replication.rs`, в ветке обработки собранной транзакции, заменить связку
«записали → durable» на «записали → барьер → durable»:

```rust
            sink.write_transaction(&tx).await?;
            tracker.note_processed(end_lsn);

            // Отметить durable имеет право только успешный барьер, а не приём записи.
            if let Some(durable) = sink.flush().await? {
                tracker.note_durable(durable);
                tracker.try_ack(durable)?;
                stream.shared_lsn_feedback.update_flushed_lsn(durable.0);
                stream.shared_lsn_feedback.update_applied_lsn(durable.0);
                stream
                    .send_feedback()
                    .await
                    .map_err(|e| PgcdcError::Connection(format!("send_feedback: {e}")))?;
                debug!(xid = tx.xid, changes, lsn = %durable, "transaction_committed");
            }
```

Барьер здесь вызывается на каждой транзакции — группировка по таймеру появится в задаче 4.
Поведение снаружи пока не меняется, меняется только то, что считается моментом durable.

- [ ] **Step 6: Прогнать всё**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test 2>&1 | /usr/bin/grep -E '^test result'
cargo fmt --check && echo "fmt clean"
cargo clippy --all-targets 2>&1 | tail -5
```

Ожидается: всё зелёное, включая интеграционный тест «sink упал → слот не сдвинулся» —
он обязан продолжать проходить, потому что порядок операций не изменился.

- [ ] **Step 7: Коммит**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git add src/sink src/postgres/replication.rs
git commit -m "feat(sink): split durability barrier from write"
```

---

### Task 3: Файловый sink с fsync

Первый sink, который может честно сказать `Durability::Fsync`.

**Files:**
- Create: `src/sink/file.rs`
- Modify: `src/sink/mod.rs`, `src/error.rs`, `src/config.rs`, `src/main.rs`

**Interfaces:**
- Produces:
  ```rust
  pub struct FileSink { /* ... */ }
  impl FileSink {
      /// Открывает файл на дозапись, создавая при отсутствии.
      pub fn open(path: &std::path::Path) -> Result<Self, PgcdcError>;
  }
  ```
  и в `src/config.rs`: вариант `OutputKind::File`, поле `pub output_path: Option<PathBuf>`.

- [ ] **Step 1: Написать падающие тесты файлового sink**

Создать `src/sink/file.rs` с тестами:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{pg_micros_to_utc, ChangeEvent, Operation, Row};
    use crate::lsn::Lsn;

    fn change(id: &str) -> ChangeEvent {
        let mut after = Row::new();
        after.insert("id".into(), id.into());
        ChangeEvent {
            schema: "public".into(),
            table: "users".into(),
            operation: Operation::Insert,
            before: None,
            before_kind: None,
            after: Some(after),
            unchanged_columns: Vec::new(),
            transaction_id: 737,
            lsn: Lsn(0x200),
            commit_lsn: Lsn(0x1000),
            commit_timestamp: pg_micros_to_utc(841_423_351_314_489),
        }
    }

    fn tx(end: u64, ids: &[&str]) -> Transaction {
        Transaction {
            xid: 737,
            commit_lsn: Lsn(0x1000),
            end_lsn: Lsn(end),
            commit_timestamp: pg_micros_to_utc(841_423_351_314_489),
            changes: ids.iter().map(|i| change(i)).collect(),
        }
    }

    fn temp_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("pgcdc-test-{}-{}.jsonl", name, std::process::id()));
        let _ = std::fs::remove_file(&p);
        p
    }

    #[tokio::test]
    async fn file_sink_declares_real_durability() {
        let p = temp_path("durability");
        let s = FileSink::open(&p).unwrap();
        assert_eq!(s.durability(), Durability::Fsync);
        let _ = std::fs::remove_file(&p);
    }

    #[tokio::test]
    async fn writes_one_json_line_per_change_and_appends() {
        let p = temp_path("append");
        let mut s = FileSink::open(&p).unwrap();
        s.write_transaction(&tx(0x1030, &["1", "2"])).await.unwrap();
        s.flush().await.unwrap();
        s.write_transaction(&tx(0x1060, &["3"])).await.unwrap();
        s.flush().await.unwrap();
        let text = std::fs::read_to_string(&p).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 3, "две транзакции, три изменения");
        for line in &lines {
            serde_json::from_str::<serde_json::Value>(line).expect("каждая строка — JSON");
        }
        assert!(lines[2].contains(r#""id":"3""#), "вторая транзакция дописана, а не затёрла");
        let _ = std::fs::remove_file(&p);
    }

    #[tokio::test]
    async fn flush_reports_the_last_accepted_position_then_clears_it() {
        let p = temp_path("position");
        let mut s = FileSink::open(&p).unwrap();
        assert_eq!(s.flush().await.unwrap(), None, "принимать было нечего");
        s.write_transaction(&tx(0x1030, &["1"])).await.unwrap();
        assert_eq!(s.flush().await.unwrap(), Some(Lsn(0x1030)));
        assert_eq!(s.flush().await.unwrap(), None, "повторный барьер ничего не добавляет");
        let _ = std::fs::remove_file(&p);
    }

    #[tokio::test]
    async fn opening_an_unwritable_path_fails_loudly() {
        // Каталог вместо файла: открыть на запись нельзя.
        let err = FileSink::open(std::path::Path::new("/")).unwrap_err();
        assert!(matches!(err, PgcdcError::Sink(_)));
        assert!(err.is_fatal(), "sink, который не может писать, — фатальная ошибка");
    }
}
```

- [ ] **Step 2: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib file 2>&1 | tail -20
```

Ожидается ошибка компиляции: нет `FileSink`.

- [ ] **Step 3: Реализовать файловый sink**

В `src/sink/file.rs` перед тестами:

```rust
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

use super::stdout::write_changes;
use super::{Durability, Sink};
use crate::error::PgcdcError;
use crate::lsn::Lsn;
use crate::transaction::Transaction;

/// JSONL с дозаписью в файл. Единственный sink этапа, способный честно
/// обещать `Fsync`: барьер вызывает `sync_data`, и только после его успеха
/// позиция может быть отмечена durable.
#[derive(Debug)]
pub struct FileSink {
    writer: BufWriter<File>,
    /// Наибольшая принятая позиция с прошлого барьера.
    pending: Option<Lsn>,
}

impl FileSink {
    pub fn open(path: &Path) -> Result<Self, PgcdcError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| PgcdcError::Sink(format!("open {}: {e}", path.display())))?;
        Ok(Self { writer: BufWriter::new(file), pending: None })
    }
}

#[async_trait::async_trait]
impl Sink for FileSink {
    fn durability(&self) -> Durability {
        Durability::Fsync
    }

    async fn write_transaction(&mut self, tx: &Transaction) -> Result<(), PgcdcError> {
        write_changes(&mut self.writer, tx)?;
        self.pending = Some(tx.end_lsn);
        Ok(())
    }

    async fn flush(&mut self) -> Result<Option<Lsn>, PgcdcError> {
        // Порядок обязателен: сначала вытолкнуть буфер пользовательского
        // пространства, потом заставить ядро довести до носителя. Пропустить
        // второе — значит обещать Fsync и не выполнять обещание.
        self.writer
            .flush()
            .map_err(|e| PgcdcError::Sink(format!("flush: {e}")))?;
        self.writer
            .get_ref()
            .sync_data()
            .map_err(|e| PgcdcError::Sink(format!("fsync: {e}")))?;
        Ok(self.pending.take())
    }
}
```

`write_changes` уже существует в `src/sink/stdout.rs` и объявлен `pub(crate)` — сделать
его видимым для этого модуля, если потребуется, изменив путь импорта, но **не** копировать
его тело: одна сериализация на оба sink'а.

Добавить `pub mod file;` и `pub use file::FileSink;` в `src/sink/mod.rs`.

- [ ] **Step 4: Запустить**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib file 2>&1 | tail -12
```

Ожидается: четыре теста зелёные.

- [ ] **Step 5: Добавить sink в конфигурацию и выбор в main**

В `src/config.rs` расширить перечисление и добавить поле:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputKind {
    Stdout,
    File,
}
```

```rust
    /// Путь для `--output file`. Обязателен при этом варианте.
    #[arg(long, env = "PGCDC_OUTPUT_PATH")]
    pub output_path: Option<std::path::PathBuf>,
```

В `src/main.rs` заменить построение sink'а:

```rust
    let sink: Box<dyn Sink> = match (config.output, &config.output_path) {
        (OutputKind::Stdout, _) => Box::new(StdoutSink::new()),
        (OutputKind::File, Some(path)) => match FileSink::open(path) {
            Ok(s) => Box::new(s),
            Err(e) => {
                error!(error_kind = e.kind(), fatal = e.is_fatal(), "{e}");
                return ExitCode::FAILURE;
            }
        },
        (OutputKind::File, None) => {
            error!("--output file requires --output-path");
            return ExitCode::FAILURE;
        }
    };
```

Исчерпывающий `match` по `(output, output_path)` здесь намеренный: он заставит компилятор
потребовать решения, если появится третий вариант вывода.

- [ ] **Step 6: Написать тест на то, что путь обязателен**

Тест использует `env!("CARGO_BIN_EXE_pgcdc")`, доступный только в интеграционных тестах,
поэтому он идёт в `tests/integration.rs`, рядом с существующим тестом на отвергнутый URL:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn file_output_without_a_path_is_rejected_by_the_binary() {
        // Проверяется поведение бинаря целиком: clap разбирает конфигурацию,
        // а решение об обязательности пути принимает main.
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"))
            .args([
                "--database-url", "postgres://u:p@127.0.0.1:1/db",
                "--publication", "p",
                "--slot", "s",
                "--output", "file",
            ])
            .output()
            .expect("запустить бинарь");
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("--output-path"), "сообщение называет недостающий флаг: {stderr}");
    }
```

- [ ] **Step 7: Прогнать всё и проверить fsync вживую**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test 2>&1 | /usr/bin/grep -E '^test result'
cargo fmt --check && echo "fmt clean"
cargo clippy --all-targets 2>&1 | tail -5
```

- [ ] **Step 8: Коммит**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git add src/sink src/config.rs src/main.rs src/error.rs tests/integration.rs
git commit -m "feat(sink): add file sink with fsync"
```

---

### Task 4: Групповое подтверждение по таймеру

Барьер на каждой транзакции означает fsync на каждую транзакцию — потолок порядка сотни
транзакций в секунду. Группируем (Q5), не трогая порядок операций.

**Files:**
- Modify: `src/config.rs`, `src/postgres/replication.rs`, `tests/integration.rs`

**Interfaces:**
- Produces: поле `pub ack_interval_ms: u64` в `Config` (дефолт 200).

- [ ] **Step 1: Добавить интервал в конфигурацию**

В `src/config.rs`:

```rust
    /// Как часто вызывается барьер durability и уходит подтверждение.
    /// Задержка подтверждения на корректность не влияет: инвариант 1
    /// сохраняется, а дубликаты после сбоя контракт разрешает.
    #[arg(long, env = "PGCDC_ACK_INTERVAL_MS", default_value = "200")]
    pub ack_interval_ms: u64,
```

- [ ] **Step 2: Написать интеграционный тест на группировку**

Добавить в `tests/integration.rs`:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn several_transactions_are_acknowledged_as_one_group() {
    // Группировка не должна ни терять транзакции, ни подтверждать позицию
    // раньше, чем барьер её довёл.
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let (tx_send, mut tx_recv) = mpsc::unbounded_channel();
    let mut cfg = config(&conn);
    cfg.ack_interval_ms = 500;
    let handle = tokio::spawn(async move {
        pgcdc::postgres::replication::run(cfg, Box::new(ChannelSink(tx_send))).await
    });

    for id in 1..=5 {
        client
            .execute("INSERT INTO users VALUES ($1, 'x', NULL, NULL)", &[&(id as i64)])
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
    assert!(confirmed >= last, "слот догнал последнюю группу: {confirmed} < {last}");

    handle.abort();
}
```

- [ ] **Step 3: Добавить помощник ожидания позиции слота**

В `tests/common/mod.rs`:

```rust
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

/// PostgreSQL печатает позицию как две шестнадцатеричные половины через слэш.
pub fn parse_lsn(text: &str) -> Option<pgcdc::lsn::Lsn> {
    let (hi, lo) = text.split_once('/')?;
    let hi = u64::from_str_radix(hi, 16).ok()?;
    let lo = u64::from_str_radix(lo, 16).ok()?;
    Some(pgcdc::lsn::Lsn((hi << 32) | lo))
}
```

- [ ] **Step 4: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --test integration several_transactions 2>&1 | tail -20
```

Ожидается ошибка компиляции: у `Config` нет поля `ack_interval_ms`, если шаг 1 ещё не
применён, либо тест падает на `wait_for_slot_at_least`.

- [ ] **Step 5: Перестроить цикл на ограниченное чтение и групповой барьер**

В `src/postgres/replication.rs` заменить тело цикла. Ограниченное чтение здесь безопасно
именно потому, что прод работает на `Inline`-драйвере: `AsyncReadExt::read_buf` в tokio
cancel-safe, а буфер принадлежит соединению, а не сброшенной future. На `Threaded` тот же
приём терял бы кадры — поэтому интеграционные тесты и переведены на многопоточный рантайм
в задаче 1.

```rust
    let ack_interval = Duration::from_millis(config.ack_interval_ms);
    let mut last_flush = tokio::time::Instant::now();

    loop {
        let read = tokio::time::timeout(ack_interval, stream.next_raw_event(&cancel)).await;

        match read {
            Ok(Ok(raw)) => {
                tracker.note_received(Lsn(raw.wal_end.0));
                let msg = decode(&raw.data)?;
                if let Some(tx) = assembler.handle(msg, Lsn(raw.wal_start.0), &mut cache)? {
                    let changes = tx.changes.len();
                    let end_lsn = tx.end_lsn;
                    sink.write_transaction(&tx).await?;
                    tracker.note_processed(end_lsn);
                    debug!(xid = tx.xid, changes, lsn = %end_lsn, "transaction_accepted");
                }
            }
            Ok(Err(e)) => return Err(PgcdcError::Connection(format!("next_raw_event: {e}"))),
            // Тик: читать было нечего. Не ошибка — повод дойти до барьера.
            Err(_elapsed) => {}
        }

        if last_flush.elapsed() >= ack_interval {
            last_flush = tokio::time::Instant::now();
            if let Some(durable) = sink.flush().await? {
                tracker.note_durable(durable);
                tracker.try_ack(durable)?;
                stream.shared_lsn_feedback.update_flushed_lsn(durable.0);
                stream.shared_lsn_feedback.update_applied_lsn(durable.0);
                stream
                    .send_feedback()
                    .await
                    .map_err(|e| PgcdcError::Connection(format!("send_feedback: {e}")))?;
                debug!(lsn = %durable, "group_acknowledged");
            }
        }
    }
```

Добавить `use std::time::Duration;` и `use tokio::time;`, если их нет.

- [ ] **Step 6: Прогнать всё, включая тест на отказ sink**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test 2>&1 | /usr/bin/grep -E '^test result'
```

Тест `sink_failure_stops_us_before_the_slot_advances` обязан продолжать проходить. Если он
покраснел — это настоящий дефект порядка операций, а не проблема теста: разбираться, а не
подгонять.

- [ ] **Step 7: Проверить мутацией, что группировка не сломала инвариант**

Временно перенести блок `if last_flush.elapsed() ...` **перед** `sink.write_transaction`,
выполнить `cargo test --test integration sink_failure`, убедиться что тест краснеет,
вернуть порядок, убедиться что зеленеет. Записать оба исхода. Это та же мутация, которой
проверялся порядок в этапе 1, и она обязана продолжать работать после перестройки цикла.

- [ ] **Step 8: Коммит**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git add src/config.rs src/postgres/replication.rs tests/
git commit -m "feat: group acknowledgements on a timer"
```

---

### Task 5: Продвижение слота по keepalive

Последняя и самая опасная часть этапа. Без неё активная запись в таблицы **вне** нашей
публикации держит слот на месте, WAL растёт, и через сутки кончается диск. С ней, сделанной
неправильно, мы подтверждаем позицию, содержимое которой не записано.

**Files:**
- Modify: `src/postgres/replication.rs`, `tests/integration.rs`

**Interfaces:**
- Consumes: `Assembler::is_empty()`, `LsnTracker::{processed, durable, note_durable, try_ack}`.

- [ ] **Step 1: Написать интеграционный тест на простаивающую публикацию**

Добавить в `tests/integration.rs`:

```rust
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
        pgcdc::postgres::replication::run(cfg, Box::new(ChannelSink(tx_send))).await
    });

    for id in 1..=50 {
        client
            .execute("INSERT INTO noise VALUES ($1, repeat('x', 1000))", &[&(id as i64)])
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
```

Функция `parse_lsn` из задачи 4 объявляется в `tests/common/mod.rs` как `pub fn parse_lsn`,
и этот тест зовёт её как `common::parse_lsn(&target)`.

- [ ] **Step 2: Написать юнит-тест на условие продвижения**

Продвижение гейтится условием строго сильнее «буфер пуст» (Q26a). Вынести условие в
отдельную функцию, чтобы его можно было проверить без сервера. Добавить в
`src/postgres/replication.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keepalive_advance_requires_an_empty_buffer() {
        // Открытая транзакция означает, что часть WAL мы ещё должны sink'у.
        assert!(!may_advance_from_keepalive(false, Lsn(0x1000), Lsn(0x1000)));
    }

    #[test]
    fn keepalive_advance_requires_processed_to_have_caught_up() {
        // Буфер пуст, но транзакция принята sink'ом и ещё не доведена барьером.
        // Подтвердить позицию из keepalive здесь — значит подтвердить сверх durable.
        assert!(!may_advance_from_keepalive(true, Lsn(0x2000), Lsn(0x1000)));
    }

    #[test]
    fn keepalive_advance_is_allowed_when_nothing_is_owed() {
        assert!(may_advance_from_keepalive(true, Lsn(0x1000), Lsn(0x1000)));
    }
}
```

- [ ] **Step 3: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib replication 2>&1 | tail -20
```

Ожидается ошибка компиляции: нет `may_advance_from_keepalive`.

- [ ] **Step 4: Реализовать условие и продвижение**

В `src/postgres/replication.rs` добавить функцию рядом с `run`:

```rust
/// Можно ли подтвердить позицию, пришедшую в keepalive.
///
/// «Буфер пуст» само по себе НЕ достаточно (DECISIONS Q26a). Оно было достаточным,
/// пока запись, отметка durable и подтверждение происходили в одной итерации; с
/// групповым барьером появляется окно, где открытых транзакций нет, а принятые
/// sink'ом и не доведённые до носителя данные есть. Подтвердить позицию внутри
/// этого окна — значит подтвердить сверх durable и потерять данные при крахе.
fn may_advance_from_keepalive(assembler_empty: bool, processed: Lsn, durable: Lsn) -> bool {
    assembler_empty && processed <= durable
}
```

И в ветке тика цикла, после блока группового барьера:

```rust
        // Продвижение по keepalive: если мы ничего не должны sink'у, вся позиция,
        // которую сервер уже отдал, вакуумно durable — в ней не было ни одной
        // строки нашей публикации. Отмечаем это явно, а не ослабляем try_ack
        // (DECISIONS Q26b).
        let server_lsn = Lsn(stream.current_lsn());
        if may_advance_from_keepalive(assembler.is_empty(), tracker.processed(), tracker.durable())
            && server_lsn > tracker.acked()
        {
            tracker.note_durable(server_lsn);
            tracker.try_ack(server_lsn)?;
            stream.shared_lsn_feedback.update_flushed_lsn(server_lsn.0);
            stream.shared_lsn_feedback.update_applied_lsn(server_lsn.0);
            stream
                .send_feedback()
                .await
                .map_err(|e| PgcdcError::Connection(format!("send_feedback: {e}")))?;
            debug!(lsn = %server_lsn, "advanced_from_keepalive");
        }
```

`stream.current_lsn()` — публичный метод крейта (`stream.rs:1257`), возвращающий
`state.last_received_lsn`, которую обновляет обработчик keepalive
(`process_keepalive_message`, `stream.rs:1126`). Именно поэтому нам и нужен был
ограниченный по времени цикл: keepalive-кадры крейт съедает внутри и наружу не отдаёт,
так что без тика мы бы никогда не дошли до этой проверки.

- [ ] **Step 5: Написать интеграционный тест на запрет при непустом буфере**

```rust
#[tokio::test(flavor = "multi_thread")]
async fn keepalive_does_not_advance_the_slot_past_an_unwritten_transaction() {
    // Sink принимает записи, но барьер всегда падает: durable не двигается.
    // Даже при активном keepalive слот обязан стоять.
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
        pgcdc::postgres::replication::run(cfg, Box::new(FailingFlushSink::default())).await
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

/// Принимает записи, но всегда падает на барьере.
#[derive(Default)]
struct FailingFlushSink;

#[async_trait::async_trait]
impl Sink for FailingFlushSink {
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
```

Обрати внимание: `handle` здесь поглощается `timeout`, поэтому `handle.abort()` в конце
не нужен и его быть не должно — в отличие от тестов, которые оставляют задачу работать.

- [ ] **Step 6: Прогнать всё**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test 2>&1 | /usr/bin/grep -E '^test result'
cargo fmt --check && echo "fmt clean"
cargo clippy --all-targets 2>&1 | tail -5
```

- [ ] **Step 7: Проверить мутацией, что условие несущее**

Три мутации, каждая: применить, прогнать, убедиться что краснеет, откатить, убедиться что
зеленеет. Записать все шесть исходов.

1. Ослабить условие до `assembler_empty` — обязан покраснеть юнит-тест
   `keepalive_advance_requires_processed_to_have_caught_up`.
2. Заменить `tracker.note_durable(server_lsn)` перед `try_ack` на ничего — обязан
   покраснеть тест на простаивающую публикацию (подтверждение будет отвергнуто как
   выходящее за durable, и `run` вернёт ошибку).
3. Убрать условие `server_lsn > tracker.acked()` — тест обязан остаться зелёным; это
   оптимизация, а не инвариант. Если он краснеет, значит условие несёт смысл, которого мы
   не заметили, и это надо записать.

- [ ] **Step 8: Обновить README**

В разделе «Что уже работает» добавить: файловый sink с fsync, групповое подтверждение по
таймеру, продвижение слота при простаивающей публикации. В разделе «Гарантии» добавить
предложение о том, что позиция подтверждается только после успешного барьера durability,
а не после приёма записи.

- [ ] **Step 9: Коммит**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git add src/postgres/replication.rs tests/ README.md
git commit -m "feat: advance slot from keepalive when idle"
```

---

## Definition of Done для этапа 3

- [ ] `cargo test`, `cargo fmt --check`, `cargo clippy --all-targets` чистые;
- [ ] трекер различает четыре позиции, `processed` может опережать `durable`;
- [ ] `Sink` имеет барьер `flush`, отдельный от `write_transaction`, и `Durability`
      документирован как обещание **после барьера**, а не после приёма;
- [ ] отметка durable выполняется только на успешном барьере;
- [ ] файловый sink дозаписывает JSONL и вызывает `sync_data` перед тем, как отчитаться;
- [ ] `--output file` без `--output-path` отвергается с внятным сообщением;
- [ ] подтверждение группируется по `--ack-interval-ms`, транзакции при этом не теряются;
- [ ] тест «sink упал → слот не сдвинулся» продолжает проходить после перестройки цикла,
      и это подтверждено мутацией порядка операций;
- [ ] слот продвигается при простаивающей публикации;
- [ ] при непустом буфере или непройденном барьере keepalive слот **не** двигает;
- [ ] условие продвижения проверено тремя мутациями;
- [ ] все интеграционные тесты помечены `flavor = "multi_thread"`, и причина записана
      в `docs/spike-findings.md`;
- [ ] поле `lsn` события DELETE проверяется тестом, подтверждённым мутацией;
- [ ] ни один файл в `tests/fixtures/` не изменён.
