# pgcdc Этап 1 (Сквозной срез) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Провести один INSERT из PostgreSQL через все слои — транспорт, декодер, relation cache, сборщик транзакций, sink — до JSON-строки на stdout, и подтвердить LSN только после того, как sink отчитался об успехе.

**Architecture:** Вся логика в `src/lib.rs`, `src/main.rs` — только разбор CLI и вызов `run()`. Декодер `pgoutput` пишем сами, транспорт берём из `pg_walstream` через единственный разрешённый метод `next_raw_event`. Из шести типов сообщений этап 1 обрабатывает четыре: `BEGIN`, `RELATION`, `INSERT`, `COMMIT`; `UPDATE`, `DELETE` и всё остальное — явная ошибка, а не молчаливый пропуск. Тесты декодера читают байтовые фикстуры этапа 0 и не требуют Docker.

**Tech Stack:** Rust 1.95.0 (Homebrew), tokio, `pg_walstream` 0.8, `tokio-postgres` (только для pre-flight проверки слота), serde_json, chrono, clap, tracing, testcontainers (dev), PostgreSQL 16 в Docker.

**Spec:** [DECISIONS.md](../../../DECISIONS.md). Артефакты этапа 0, на которые опирается план: [docs/spike-findings.md](../../spike-findings.md) (сигнатуры API и обязательства Q25), [docs/pgoutput-notes.md](../../pgoutput-notes.md) (байтовая разметка), [tests/fixtures/](../../../tests/fixtures/) (31 фикстура + MANIFEST).

---

## Global Constraints

Действуют во **всех** задачах. Нарушение любого — основание отклонить задачу на ревью.

1. **PATH в песочнице урезан.** Каждая команда Bash обязана начинаться с:
   ```bash
   export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
   ```
2. **Псевдонимы перекрывают базовые утилиты, а их целей нет в PATH.** `cat` → `bat`,
   `ls` → `eza`. Использовать `/bin/cat` для heredoc и `/bin/ls` для листинга.
3. **Рабочая директория:** `/Users/roman/Projects/HP/rust_cdc`.
4. **Rust 1.95.0 из Homebrew, `rustup` отсутствует.** Никаких `rustup component add`,
   `+nightly`. `rustfmt` и `cargo clippy` доступны и обязаны быть чистыми перед коммитом.
5. **Три обязательства из `DECISIONS.md` Q25 — обоснование в `docs/spike-findings.md` §3:**
   - **(а) Pre-flight guard, два режима.** Холодный старт: только проверка существования
     слота; нет слота — fatal, слот не создаём. Реконнект внутри процесса: сверка
     `confirmed_flush_lsn` слота с in-memory durable-позицией; слот **впереди** — fatal,
     слот **позади** — WARN и продолжаем. На каждом старте логируем `restart_lsn` и
     `confirmed_flush_lsn` на INFO.
   - **(б) Запрещены пять API `pg_walstream`:** `next_event_with_retry`,
     `check_connection_health`, `into_stream`, `stream`, `for_each_event`. Все ведут в
     `recover_connection`, который рестартует поток с `last_received_lsn` — принятой, а не
     durable позиции, и молча пропускает WAL. Разрешён **только** `next_raw_event`.
   - **(в) После каждой durable-записи обязателен явный `stream.send_feedback().await`.**
     Без него подтверждение уходит с задержкой 18–22 с.
6. **`proto_version` = 1, `StreamingMode::Off`.** Не менять.
7. **Все значения колонок — строки; SQL NULL — настоящий JSON `null`.** Никакого приведения
   типов по `type_oid` (DECISIONS Q16).
8. **Подтверждаем `end_lsn`, никогда `commit_lsn`.** В сообщении COMMIT `commit_lsn` лежит
   на offset 2, `end_lsn` — на offset 10. Перепутать их означает перечитывать каждую
   транзакцию после рестарта. Поле JSON `commit_lsn` при этом несёт именно `commit_lsn` —
   это идентичность транзакции для дедупликации, а не позиция подтверждения.
9. **Инвариант `acked <= durable`.** Не комментарий, а проверка в коде: попытка подтвердить
   позицию больше durable — это баг, и он обязан быть пойман, а не пропущен.
10. **Foreground `sleep` заблокирован песочницей.** Готовность контейнера —
    `docker compose up -d --wait`; всё остальное — ограниченные циклы опроса.
11. **Коммиты:** Conventional Commits, `type(scope): subject`, subject не длиннее 50 символов.
    Автор `tarodo`, почта `rsvolozhanin@gmail.com` — настроено глобально, не менять.
    **В сообщениях коммитов запрещены любые трейлеры соавторства и любые футеры о том, каким
    инструментом сгенерирован код.**
12. **TDD обязателен.** Сначала падающий тест, затем минимальная реализация. Тесты задач 1–4
    не требуют Docker и обязаны проходить за секунды.
13. **`src/bin/spike.rs` удаляется в задаче 6.** До неё — не трогать.

---

## File Structure

| Файл | Ответственность |
|------|-----------------|
| `src/lib.rs` | Публичный корень: реэкспорт модулей, `run()` |
| `src/main.rs` | Только разбор CLI и вызов `run()`, преобразование `Result` в код возврата |
| `src/config.rs` | `Config` на clap, тип-обёртка над URL с вырезанием пароля |
| `src/error.rs` | `PgcdcError` с исчерпывающим `is_fatal()` |
| `src/lsn.rs` | Тип `Lsn` с форматированием `X/Y`, трекер четырёх позиций |
| `src/event.rs` | `ChangeEvent`, `Operation`, `BeforeKind`, `Row`, JSON-контракт |
| `src/postgres/pgoutput.rs` | Декодер байтов в `PgOutputMessage`, `TupleData` |
| `src/postgres/guard.rs` | Pre-flight проверка слота на отдельном соединении |
| `src/postgres/replication.rs` | Цикл `next_raw_event` → декодер → сборщик → sink → ack |
| `src/schema.rs` | `Relation`, `Column`, `RelationCache` |
| `src/transaction.rs` | `Transaction`, `Assembler` |
| `src/sink/mod.rs` | Трейт `Sink`, `Durability` |
| `src/sink/stdout.rs` | `StdoutSink` — JSONL, `BestEffort` |
| `tests/integration.rs` | Сценарий §19 на testcontainers |
| `tests/common/mod.rs` | `FailingSink`, помощники для testcontainers |

---

### Task 1: Каркас lib+bin, модель события и JSON-контракт

Начинаем с контракта вывода, потому что он — единственное, что видит потребитель, и всё
остальное существует ради него. Задача чистая: ни Docker, ни сети, тесты мгновенные.

**Files:**
- Create: `src/lib.rs`, `src/lsn.rs`, `src/event.rs`, `src/error.rs`
- Modify: `Cargo.toml`

**Interfaces:**
- Produces: `pgcdc::lsn::Lsn(pub u64)` с `Display` в формате `X/Y`;
  `pgcdc::event::{ChangeEvent, Operation, BeforeKind, Row}`;
  `pgcdc::error::PgcdcError` с `fn is_fatal(&self) -> bool`.
  `Row` — это `serde_json::Map<String, serde_json::Value>`, где значение либо
  `Value::String`, либо `Value::Null`.

- [ ] **Step 1: Добавить зависимости**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo add serde --features derive
cargo add serde_json --features preserve_order
cargo add chrono --no-default-features --features std,clock,serde
cargo add thiserror
```

`preserve_order` у `serde_json` обязателен: без него порядок колонок в `after` будет
алфавитным, а не как в таблице, и вывод станет нечитаемым.

- [ ] **Step 2: Написать падающий тест на формат LSN**

Создать `src/lsn.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_display_matches_postgres_format() {
        // Значения из docs/pgoutput-notes.md §4, 0004_commit.bin
        assert_eq!(Lsn(0x0000_0000_0193_00D0).to_string(), "0/19300D0");
        assert_eq!(Lsn(0x0000_0000_0193_0100).to_string(), "0/1930100");
        // Старшая половина не нулевая
        assert_eq!(Lsn(0x0000_0001_0000_00FF).to_string(), "1/FF");
        assert_eq!(Lsn(0).to_string(), "0/0");
    }
}
```

- [ ] **Step 3: Запустить, убедиться что не компилируется**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib lsn 2>&1 | tail -20
```

Ожидается ошибка компиляции: нет типа `Lsn`, нет `src/lib.rs`.

- [ ] **Step 4: Реализовать `Lsn`**

В `src/lsn.rs` перед блоком тестов:

```rust
use std::fmt;

/// Позиция в WAL. PostgreSQL печатает её как две шестнадцатеричные половины
/// через слэш, без ведущих нулей: `0/19300D0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Lsn(pub u64);

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

impl serde::Serialize for Lsn {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}
```

Создать `src/lib.rs`. Объявляем **только** `lsn` — остальные модули появятся ниже
в этой же задаче, и объявить их сейчас значит не собраться на следующем шаге:

```rust
pub mod lsn;
```

- [ ] **Step 5: Запустить тест**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib lsn 2>&1 | tail -10
```

Ожидается: `test result: ok. 1 passed`.

- [ ] **Step 6: Написать падающий тест на JSON-контракт**

Создать `src/event.rs` с тестом. Ожидаемый JSON взят из `DECISIONS.md` §3 и из фактических
значений фикстуры `0003_insert.bin` (`docs/pgoutput-notes.md` §9): xid 737,
`commit_lsn` `0/19300D0`, `wal_start` строки `0/192FFC0`, timestamp `841423351314489` мкс
от эпохи 2000-01-01, что даёт `2026-08-30T16:42:31.314489Z`.

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn sample_insert() -> ChangeEvent {
        let mut after = Row::new();
        after.insert("id".into(), "1".into());
        after.insert("name".into(), "Alice".into());
        after.insert("email".into(), "alice@example.com".into());
        after.insert("bio".into(), serde_json::Value::Null);
        ChangeEvent {
            schema: "public".into(),
            table: "users".into(),
            operation: Operation::Insert,
            before: None,
            before_kind: None,
            after: Some(after),
            unchanged_columns: Vec::new(),
            transaction_id: 737,
            lsn: Lsn(0x0192_FFC0),
            commit_lsn: Lsn(0x0193_00D0),
            commit_timestamp: pg_micros_to_utc(841_423_351_314_489),
        }
    }

    #[test]
    fn insert_event_serializes_to_the_contract() {
        let json = serde_json::to_string(&sample_insert()).unwrap();
        let expected = concat!(
            r#"{"schema":"public","table":"users","operation":"insert","#,
            r#""before":null,"before_kind":null,"#,
            r#""after":{"id":"1","name":"Alice","email":"alice@example.com","bio":null},"#,
            r#""unchanged_columns":[],"transaction_id":737,"#,
            r#""lsn":"0/192FFC0","commit_lsn":"0/19300D0","#,
            r#""commit_timestamp":"2026-08-30T16:42:31.314489Z"}"#
        );
        assert_eq!(json, expected);
    }

    #[test]
    fn optional_fields_are_present_not_omitted() {
        // Стабильная форма важнее компактности (DECISIONS Q20): потребитель
        // не должен писать `if "unchanged_columns" in event`.
        let json = serde_json::to_string(&sample_insert()).unwrap();
        assert!(json.contains(r#""before":null"#));
        assert!(json.contains(r#""before_kind":null"#));
        assert!(json.contains(r#""unchanged_columns":[]"#));
    }

    #[test]
    fn column_order_follows_the_table_not_the_alphabet() {
        let json = serde_json::to_string(&sample_insert()).unwrap();
        let id_at = json.find(r#""id""#).unwrap();
        let bio_at = json.find(r#""bio""#).unwrap();
        assert!(id_at < bio_at, "порядок колонок должен быть как в таблице");
    }

    #[test]
    fn timestamp_uses_the_2000_epoch_not_1970() {
        // Ровно та ловушка, что описана в docs/pgoutput-notes.md §5: от эпохи 1970
        // это же число даёт 1996-08-30 с тем же днём месяца и тем же временем суток,
        // то есть выглядит правдоподобной датой. Поэтому сверяем точное значение.
        let ts = pg_micros_to_utc(841_423_351_314_489);
        assert_eq!(
            ts.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            "2026-08-30T16:42:31.314489Z"
        );
    }
}
```

- [ ] **Step 7: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib event 2>&1 | tail -20
```

Ожидается ошибка компиляции: нет `ChangeEvent`, `Operation`, `Row`, `pg_micros_to_utc`.

- [ ] **Step 8: Реализовать модель события**

Дописать в `src/lib.rs` строку `pub mod event;`, затем в `src/event.rs` перед блоком тестов:

```rust
use chrono::{DateTime, TimeZone, Utc};
use serde::Serialize;

use crate::lsn::Lsn;

/// Значения колонок всегда строки либо JSON null (DECISIONS Q16).
/// `serde_json::Map` с включённым `preserve_order` держит порядок колонок таблицы.
pub type Row = serde_json::Map<String, serde_json::Value>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Insert,
    Update,
    Delete,
}

/// Что именно сервер прислал в старом кортеже. Потребитель обязан различать
/// «полная старая строка» и «только ключ», иначе примет заглушку за NULL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BeforeKind {
    Key,
    Full,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ChangeEvent {
    pub schema: String,
    pub table: String,
    pub operation: Operation,
    pub before: Option<Row>,
    pub before_kind: Option<BeforeKind>,
    pub after: Option<Row>,
    pub unchanged_columns: Vec<String>,
    pub transaction_id: u32,
    pub lsn: Lsn,
    pub commit_lsn: Lsn,
    #[serde(serialize_with = "serialize_ts")]
    pub commit_timestamp: DateTime<Utc>,
}

fn serialize_ts<S: serde::Serializer>(ts: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&ts.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
}

/// Микросекунды от эпохи PostgreSQL (2000-01-01T00:00:00Z), а не от Unix-эпохи.
/// Смещение — 946684800 секунд.
pub fn pg_micros_to_utc(micros: i64) -> DateTime<Utc> {
    const PG_EPOCH_UNIX_SECS: i64 = 946_684_800;
    let secs = micros.div_euclid(1_000_000) + PG_EPOCH_UNIX_SECS;
    let nanos = (micros.rem_euclid(1_000_000) * 1_000) as u32;
    Utc.timestamp_opt(secs, nanos).single().expect("valid pg timestamp")
}
```

- [ ] **Step 9: Запустить все четыре теста**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib 2>&1 | tail -15
```

Ожидается: 5 passed (один на LSN, четыре на событие).

- [ ] **Step 10: Написать тип ошибки**

Дописать в `src/lib.rs` строку `pub mod error;`, затем создать `src/error.rs`:

```rust
use thiserror::Error;

/// Разделение recoverable/fatal живёт в типе, а не в комментарии: `is_fatal`
/// реализован исчерпывающим match без `_ =>`, поэтому компилятор заставит
/// классифицировать каждый новый вариант. Забытая классификация — это путь
/// «поехали по ветке ретрая и молча потеряли события».
#[derive(Debug, Error)]
pub enum PgcdcError {
    #[error("replication slot {slot} does not exist")]
    SlotMissing { slot: String },

    #[error("replication slot {slot} is ahead of our durable position: slot={slot_lsn}, durable={durable}")]
    SlotAhead { slot: String, slot_lsn: String, durable: String },

    #[error("malformed pgoutput message: {0}")]
    Decode(String),

    #[error("unsupported pgoutput message kind {kind:?}")]
    UnsupportedMessage { kind: char },

    #[error("unknown relation id {relation_id}")]
    UnknownRelation { relation_id: u32 },

    #[error("transaction exceeded {limit} events")]
    TransactionTooLarge { limit: usize },

    #[error("refusing to acknowledge {attempted} beyond durable position {durable}")]
    AckBeyondDurable { attempted: String, durable: String },

    #[error("sink failed: {0}")]
    Sink(String),

    #[error("postgres connection error: {0}")]
    Connection(String),
}

impl PgcdcError {
    /// Машиночитаемая метка для структурированного лога (DECISIONS Q22).
    pub fn kind(&self) -> &'static str {
        match self {
            Self::SlotMissing { .. } => "slot_missing",
            Self::SlotAhead { .. } => "slot_ahead",
            Self::Decode(_) => "decode",
            Self::UnsupportedMessage { .. } => "unsupported_message",
            Self::UnknownRelation { .. } => "unknown_relation",
            Self::TransactionTooLarge { .. } => "transaction_too_large",
            Self::AckBeyondDurable { .. } => "ack_beyond_durable",
            Self::Sink(_) => "sink",
            Self::Connection(_) => "connection",
        }
    }

    pub fn is_fatal(&self) -> bool {
        match self {
            Self::SlotMissing { .. } => true,
            Self::SlotAhead { .. } => true,
            Self::Decode(_) => true,
            Self::UnsupportedMessage { .. } => true,
            Self::UnknownRelation { .. } => true,
            Self::TransactionTooLarge { .. } => true,
            Self::AckBeyondDurable { .. } => true,
            Self::Sink(_) => true,
            Self::Connection(_) => false,
        }
    }
}
```

- [ ] **Step 11: Тест на исчерпываемость классификации**

Добавить в конец `src/error.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_connection_errors_are_recoverable() {
        assert!(!PgcdcError::Connection("boom".into()).is_fatal());
        assert!(PgcdcError::SlotMissing { slot: "s".into() }.is_fatal());
        assert!(PgcdcError::Decode("bad".into()).is_fatal());
        assert!(PgcdcError::UnsupportedMessage { kind: 'T' }.is_fatal());
    }

    #[test]
    fn every_error_has_a_machine_readable_kind() {
        assert_eq!(PgcdcError::Decode("x".into()).kind(), "decode");
        assert_eq!(PgcdcError::SlotMissing { slot: "s".into() }.kind(), "slot_missing");
    }
}
```

- [ ] **Step 12: Прогнать всё, проверить формат и линтер**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib 2>&1 | tail -10
cargo fmt --check && echo "fmt clean"
cargo clippy --lib 2>&1 | tail -5
```

Ожидается: 7 passed, fmt чистый, clippy без предупреждений.

- [ ] **Step 13: Коммит**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git add Cargo.toml Cargo.lock src/lib.rs src/lsn.rs src/event.rs src/error.rs
git commit -m "feat: add cdc event model and json contract"
```

---

### Task 2: Декодер pgoutput — BEGIN, COMMIT, RELATION, INSERT

Ядро проекта. Тесты читают замороженные фикстуры этапа 0 и не требуют Docker, поэтому
цикл TDD здесь измеряется секундами. Все ожидаемые значения взяты из
`docs/pgoutput-notes.md` — это спецификация, писать тест «под реализацию» запрещено.

**Files:**
- Create: `src/postgres/mod.rs`, `src/postgres/pgoutput.rs`, `src/schema.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: `crate::error::PgcdcError`.
- Produces:
  ```rust
  pub enum ColumnValue { Null, UnchangedToast, Text(String) }
  pub struct TupleData { pub columns: Vec<ColumnValue> }
  pub enum PgOutputMessage {
      Begin { final_lsn: u64, commit_timestamp: i64, xid: u32 },
      Commit { flags: u8, commit_lsn: u64, end_lsn: u64, commit_timestamp: i64 },
      Relation(Relation),
      Insert { relation_id: u32, tuple: TupleData },
  }
  pub fn decode(payload: &[u8]) -> Result<PgOutputMessage, PgcdcError>
  ```
  и в `src/schema.rs`:
  ```rust
  pub struct Column { pub name: String, pub is_key: bool, pub type_oid: u32, pub atttypmod: i32 }
  pub struct Relation { pub id: u32, pub namespace: String, pub name: String,
                        pub replica_identity: u8, pub columns: Vec<Column> }
  ```

- [ ] **Step 1: Написать падающие тесты на BEGIN и COMMIT**

Создать `src/postgres/pgoutput.rs` с блоком тестов. Значения — из
`docs/pgoutput-notes.md` §3 и §4.

```rust
#[cfg(test)]
mod tests {
    use super::*;

    const BEGIN: &[u8] = include_bytes!("../../tests/fixtures/0001_begin.bin");
    const COMMIT: &[u8] = include_bytes!("../../tests/fixtures/0004_commit.bin");

    #[test]
    fn decodes_begin() {
        assert_eq!(BEGIN.len(), 21, "BEGIN всегда 21 байт");
        match decode(BEGIN).unwrap() {
            PgOutputMessage::Begin { final_lsn, commit_timestamp, xid } => {
                assert_eq!(final_lsn, 0x0193_00D0);
                assert_eq!(commit_timestamp, 841_423_351_314_489);
                assert_eq!(xid, 737);
            }
            other => panic!("ожидался Begin, получен {other:?}"),
        }
    }

    #[test]
    fn decodes_commit_without_swapping_the_two_lsns() {
        // commit_lsn на offset 2, end_lsn на offset 10, разница 0x30.
        // Перепутать их — значит перечитывать каждую транзакцию после рестарта.
        match decode(COMMIT).unwrap() {
            PgOutputMessage::Commit { flags, commit_lsn, end_lsn, commit_timestamp } => {
                assert_eq!(flags, 0);
                assert_eq!(commit_lsn, 0x0193_00D0, "commit_lsn — первый, offset 2");
                assert_eq!(end_lsn, 0x0193_0100, "end_lsn — второй, offset 10");
                assert_eq!(end_lsn - commit_lsn, 0x30);
                assert_eq!(commit_timestamp, 841_423_351_314_489);
            }
            other => panic!("ожидался Commit, получен {other:?}"),
        }
    }

    #[test]
    fn begin_final_lsn_equals_commit_commit_lsn() {
        // Инвариант из §8 заметок: BEGIN уже знает, где транзакция закончится.
        let (b, c) = (decode(BEGIN).unwrap(), decode(COMMIT).unwrap());
        let (PgOutputMessage::Begin { final_lsn, .. }, PgOutputMessage::Commit { commit_lsn, .. }) = (b, c)
        else { panic!("не те типы") };
        assert_eq!(final_lsn, commit_lsn);
    }
}
```

- [ ] **Step 2: Запустить, убедиться что не компилируется**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib pgoutput 2>&1 | tail -20
```

Ожидается ошибка компиляции: нет `decode`, нет `PgOutputMessage`.

- [ ] **Step 3: Реализовать читатель байтов и BEGIN/COMMIT**

Создать `src/postgres/mod.rs`:

```rust
pub mod pgoutput;
```

Добавить в `src/lib.rs`:

```rust
pub mod postgres;
pub mod schema;
```

В `src/postgres/pgoutput.rs` перед тестами:

```rust
use crate::error::PgcdcError;
use crate::schema::{Column, Relation};

/// Курсор по payload'у с проверкой длины на каждом чтении. Любое чтение за границей
/// буфера — это Decode-ошибка, а не паника: битый WAL не должен ронять процесс
/// без внятного сообщения.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], PgcdcError> {
        let end = self.pos.checked_add(n).ok_or_else(|| PgcdcError::Decode("length overflow".into()))?;
        if end > self.buf.len() {
            return Err(PgcdcError::Decode(format!(
                "need {n} bytes at offset {}, only {} remain",
                self.pos,
                self.buf.len() - self.pos
            )));
        }
        let out = &self.buf[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, PgcdcError> {
        Ok(self.take(1)?[0])
    }

    fn i16(&mut self) -> Result<i16, PgcdcError> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32, PgcdcError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn i32(&mut self) -> Result<i32, PgcdcError> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, PgcdcError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn i64(&mut self) -> Result<i64, PgcdcError> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    /// C-строка: байты до нулевого терминатора, сам терминатор проглатывается.
    fn cstr(&mut self) -> Result<String, PgcdcError> {
        let rest = &self.buf[self.pos..];
        let nul = rest
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| PgcdcError::Decode(format!("unterminated string at offset {}", self.pos)))?;
        let s = std::str::from_utf8(&rest[..nul])
            .map_err(|e| PgcdcError::Decode(format!("invalid utf8 at offset {}: {e}", self.pos)))?
            .to_owned();
        self.pos += nul + 1;
        Ok(s)
    }

    fn finish(&self) -> Result<(), PgcdcError> {
        if self.pos != self.buf.len() {
            return Err(PgcdcError::Decode(format!(
                "{} trailing bytes after message",
                self.buf.len() - self.pos
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnValue {
    /// Тег 'n'. В кортеже 'N'/'O' — настоящий SQL NULL.
    /// В кортеже 'K' — «колонку не прислали», что НЕ то же самое.
    Null,
    /// Тег 'u'. TOAST-значение не менялось, сервер его не переслал.
    UnchangedToast,
    /// Тег 't'. Текстовое представление значения.
    Text(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleData {
    pub columns: Vec<ColumnValue>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PgOutputMessage {
    Begin { final_lsn: u64, commit_timestamp: i64, xid: u32 },
    Commit { flags: u8, commit_lsn: u64, end_lsn: u64, commit_timestamp: i64 },
    Relation(Relation),
    Insert { relation_id: u32, tuple: TupleData },
}

pub fn decode(payload: &[u8]) -> Result<PgOutputMessage, PgcdcError> {
    let mut r = Reader::new(payload);
    let kind = r.u8()? as char;
    let msg = match kind {
        'B' => PgOutputMessage::Begin {
            final_lsn: r.u64()?,
            commit_timestamp: r.i64()?,
            xid: r.u32()?,
        },
        'C' => PgOutputMessage::Commit {
            flags: r.u8()?,
            commit_lsn: r.u64()?,
            end_lsn: r.u64()?,
            commit_timestamp: r.i64()?,
        },
        other => return Err(PgcdcError::UnsupportedMessage { kind: other }),
    };
    r.finish()?;
    Ok(msg)
}
```

- [ ] **Step 4: Запустить, убедиться что три теста проходят**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib pgoutput 2>&1 | tail -10
```

Ожидается: 3 passed.

- [ ] **Step 5: Написать падающий тест на RELATION**

Добавить в блок тестов. Значения — из `docs/pgoutput-notes.md` §6.

```rust
    const RELATION_USERS: &[u8] = include_bytes!("../../tests/fixtures/0002_relation.bin");
    const RELATION_ITEMS: &[u8] = include_bytes!("../../tests/fixtures/0012_relation.bin");

    #[test]
    fn decodes_relation_with_full_replica_identity() {
        let PgOutputMessage::Relation(rel) = decode(RELATION_USERS).unwrap() else {
            panic!("ожидался Relation")
        };
        assert_eq!(rel.id, 16385);
        assert_eq!(rel.namespace, "public");
        assert_eq!(rel.name, "users");
        assert_eq!(rel.replica_identity, b'f', "users создана с REPLICA IDENTITY FULL");
        let names: Vec<&str> = rel.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "name", "email", "bio"]);
        assert!(rel.columns.iter().all(|c| c.is_key), "при FULL все колонки помечены ключевыми");
        assert!(rel.columns.iter().all(|c| c.atttypmod == -1), "atttypmod читается как знаковый");
    }

    #[test]
    fn decodes_relation_with_default_replica_identity() {
        let PgOutputMessage::Relation(rel) = decode(RELATION_ITEMS).unwrap() else {
            panic!("ожидался Relation")
        };
        assert_eq!(rel.name, "items");
        assert_eq!(rel.replica_identity, b'd');
        let names: Vec<&str> = rel.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "title", "qty"]);
        let keys: Vec<bool> = rel.columns.iter().map(|c| c.is_key).collect();
        assert_eq!(keys, [true, false, false], "при DEFAULT ключевой только PK");
    }
```

- [ ] **Step 6: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib pgoutput 2>&1 | tail -20
```

Ожидается ошибка компиляции: `Relation` не имеет нужных полей / ветка `'R'` отсутствует.

- [ ] **Step 7: Реализовать RELATION**

Создать `src/schema.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    /// Флаг 1 в сообщении RELATION: колонка входит в replica identity.
    pub is_key: bool,
    pub type_oid: u32,
    /// Знаковый: -1 означает «модификатор не задан».
    pub atttypmod: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    pub id: u32,
    pub namespace: String,
    pub name: String,
    /// relreplident из pg_class: b'd' DEFAULT, b'n' NOTHING, b'f' FULL, b'i' INDEX.
    pub replica_identity: u8,
    pub columns: Vec<Column>,
}
```

В `decode` добавить ветку перед `other =>`:

```rust
        'R' => {
            let id = r.u32()?;
            let namespace = r.cstr()?;
            let name = r.cstr()?;
            let replica_identity = r.u8()?;
            let ncols = r.i16()?;
            if ncols < 0 {
                return Err(PgcdcError::Decode(format!("negative column count {ncols}")));
            }
            let mut columns = Vec::with_capacity(ncols as usize);
            for _ in 0..ncols {
                columns.push(Column {
                    is_key: r.u8()? == 1,
                    name: r.cstr()?,
                    type_oid: r.u32()?,
                    atttypmod: r.i32()?,
                });
            }
            PgOutputMessage::Relation(Relation { id, namespace, name, replica_identity, columns })
        }
```

Обрати внимание на порядок полей колонки: в байтах сначала идёт флаг, потом имя. В
структуре они объявлены в другом порядке, но инициализация в Rust вычисляет поля в
порядке записи, поэтому `is_key: r.u8()?` обязан стоять первым в литерале.

- [ ] **Step 8: Запустить**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib pgoutput 2>&1 | tail -10
```

Ожидается: 5 passed.

- [ ] **Step 9: Написать падающий тест на INSERT и TupleData**

Добавить в блок тестов. Значения — из `docs/pgoutput-notes.md` §7 и §9.

```rust
    const INSERT_USERS: &[u8] = include_bytes!("../../tests/fixtures/0003_insert.bin");
    const INSERT_ITEMS: &[u8] = include_bytes!("../../tests/fixtures/0013_insert.bin");
    const INSERT_TOAST: &[u8] = include_bytes!("../../tests/fixtures/0022_insert.bin");

    #[test]
    fn decodes_insert_and_does_not_read_length_after_null_tag() {
        // Последний байт 0003_insert.bin — тег 'n' без длины и данных.
        // Декодер, который безусловно читает 4 байта длины после тега, здесь развалится.
        let PgOutputMessage::Insert { relation_id, tuple } = decode(INSERT_USERS).unwrap() else {
            panic!("ожидался Insert")
        };
        assert_eq!(relation_id, 16385);
        assert_eq!(
            tuple.columns,
            vec![
                ColumnValue::Text("1".into()),
                ColumnValue::Text("Alice".into()),
                ColumnValue::Text("alice@example.com".into()),
                ColumnValue::Null,
            ]
        );
    }

    #[test]
    fn values_arrive_as_text_not_binary() {
        // id BIGINT = 1 приезжает одним байтом 0x31 = ASCII '1', а не восемью байтами int8.
        let PgOutputMessage::Insert { tuple, .. } = decode(INSERT_ITEMS).unwrap() else {
            panic!("ожидался Insert")
        };
        assert_eq!(tuple.columns[0], ColumnValue::Text("10".into()));
        assert_eq!(tuple.columns[2], ColumnValue::Text("5".into()));
    }

    #[test]
    fn decodes_large_toast_value_in_full() {
        let PgOutputMessage::Insert { tuple, .. } = decode(INSERT_TOAST).unwrap() else {
            panic!("ожидался Insert")
        };
        let ColumnValue::Text(bio) = &tuple.columns[3] else {
            panic!("bio должен приехать текстом целиком в INSERT")
        };
        assert_eq!(bio.len(), 9600);
    }

    #[test]
    fn every_column_has_an_entry_even_when_null() {
        // В TupleData всегда ровно ncols записей, пропусков нет (§6 заметок).
        let PgOutputMessage::Insert { tuple, .. } = decode(INSERT_USERS).unwrap() else {
            panic!("ожидался Insert")
        };
        assert_eq!(tuple.columns.len(), 4);
    }
```

- [ ] **Step 10: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib pgoutput 2>&1 | tail -20
```

Ожидается ошибка: ветки `'I'` нет.

- [ ] **Step 11: Реализовать TupleData и INSERT**

Добавить функцию рядом с `decode`:

```rust
/// Читает TupleData: Int16 число колонок, затем на каждую — тег и,
/// только для 't'/'b', длину и данные. У 'n' и 'u' длины НЕТ.
fn read_tuple(r: &mut Reader<'_>) -> Result<TupleData, PgcdcError> {
    let ncols = r.i16()?;
    if ncols < 0 {
        return Err(PgcdcError::Decode(format!("negative tuple column count {ncols}")));
    }
    let mut columns = Vec::with_capacity(ncols as usize);
    for i in 0..ncols {
        let tag = r.u8()?;
        let value = match tag {
            b'n' => ColumnValue::Null,
            b'u' => ColumnValue::UnchangedToast,
            b't' | b'b' => {
                let len = r.i32()?;
                if len < 0 {
                    return Err(PgcdcError::Decode(format!("negative value length {len} at column {i}")));
                }
                let bytes = r.take(len as usize)?;
                let text = std::str::from_utf8(bytes)
                    .map_err(|e| PgcdcError::Decode(format!("invalid utf8 in column {i}: {e}")))?;
                ColumnValue::Text(text.to_owned())
            }
            other => {
                return Err(PgcdcError::Decode(format!(
                    "unknown column tag {:?} at column {i}",
                    other as char
                )))
            }
        };
        columns.push(value);
    }
    Ok(TupleData { columns })
}
```

И ветку в `decode` перед `other =>`:

```rust
        'I' => {
            let relation_id = r.u32()?;
            let tag = r.u8()?;
            if tag != b'N' {
                return Err(PgcdcError::Decode(format!(
                    "INSERT expects tuple tag 'N', got {:?}",
                    tag as char
                )));
            }
            PgOutputMessage::Insert { relation_id, tuple: read_tuple(&mut r)? }
        }
```

- [ ] **Step 12: Написать тесты на отказы**

Спека §8 требует: неподдерживаемые сообщения не должны игнорироваться молча.

```rust
    const UPDATE: &[u8] = include_bytes!("../../tests/fixtures/0006_update.bin");
    const DELETE: &[u8] = include_bytes!("../../tests/fixtures/0009_delete.bin");

    #[test]
    fn update_and_delete_are_explicitly_unsupported_in_this_stage() {
        // Этап 1 их не обрабатывает — но обязан сказать об этом явно, а не пропустить.
        assert!(matches!(
            decode(UPDATE),
            Err(PgcdcError::UnsupportedMessage { kind: 'U' })
        ));
        assert!(matches!(
            decode(DELETE),
            Err(PgcdcError::UnsupportedMessage { kind: 'D' })
        ));
    }

    #[test]
    fn truncated_payload_is_an_error_not_a_panic() {
        let truncated = &BEGIN[..10];
        assert!(matches!(decode(truncated), Err(PgcdcError::Decode(_))));
    }

    #[test]
    fn trailing_bytes_are_an_error() {
        let mut extended = BEGIN.to_vec();
        extended.push(0xFF);
        assert!(matches!(decode(&extended), Err(PgcdcError::Decode(_))));
    }

    #[test]
    fn empty_payload_is_an_error() {
        assert!(matches!(decode(&[]), Err(PgcdcError::Decode(_))));
    }
```

- [ ] **Step 13: Запустить всё, проверить формат и линтер**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib 2>&1 | tail -15
cargo fmt --check && echo "fmt clean"
cargo clippy --lib 2>&1 | tail -5
```

Ожидается: 20 passed (7 из задачи 1 + 13 здесь), fmt чистый, clippy молчит.

- [ ] **Step 14: Коммит**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git add src/postgres src/schema.rs src/lib.rs
git commit -m "feat(pgoutput): decode begin, commit, relation, insert"
```

---

### Task 3: Relation cache и сборщик транзакций

Здесь появляется главное поведенческое требование этапа: события не выходят наружу до
`COMMIT`. Тесты по-прежнему без Docker — они кормят сборщик уже декодированными
сообщениями.

**Files:**
- Create: `src/transaction.rs`
- Modify: `src/schema.rs` (добавить `RelationCache`), `src/lib.rs`

**Interfaces:**
- Consumes: `PgOutputMessage`, `Relation`, `ChangeEvent`, `Lsn`, `PgcdcError`.
- Produces:
  ```rust
  pub struct RelationCache { /* HashMap<u32, Relation> */ }
  impl RelationCache {
      pub fn new() -> Self
      pub fn put(&mut self, relation: Relation)          // повторный OID заменяет запись
      pub fn get(&self, id: u32) -> Option<&Relation>
      pub fn clear(&mut self)                             // вызывается при реконнекте
      pub fn len(&self) -> usize
  }
  pub struct Transaction { pub xid: u32, pub commit_lsn: Lsn, pub end_lsn: Lsn,
                           pub commit_timestamp: DateTime<Utc>, pub changes: Vec<ChangeEvent> }
  pub struct Assembler { /* ... */ }
  impl Assembler {
      pub fn new(max_events: usize) -> Self
      pub fn handle(&mut self, msg: PgOutputMessage, wal_start: Lsn, cache: &mut RelationCache)
          -> Result<Option<Transaction>, PgcdcError>
      pub fn is_empty(&self) -> bool                      // нужен для правила keepalive (Q18)
      pub fn reset(&mut self)                             // вызывается при реконнекте
  }
  ```

- [ ] **Step 1: Написать падающий тест на замену записи в кэше**

Добавить в `src/schema.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn rel(id: u32, name: &str, cols: &[&str]) -> Relation {
        Relation {
            id,
            namespace: "public".into(),
            name: name.into(),
            replica_identity: b'd',
            columns: cols
                .iter()
                .map(|c| Column { name: (*c).into(), is_key: false, type_oid: 25, atttypmod: -1 })
                .collect(),
        }
    }

    #[test]
    fn repeated_relation_for_same_oid_replaces_the_entry() {
        // Повторный RELATION — штатное сообщение (DDL, смена replica identity,
        // изменение публикации), и он обязан ЗАМЕНИТЬ запись, а не быть ошибкой
        // и не быть проигнорированным.
        let mut cache = RelationCache::new();
        cache.put(rel(1, "users", &["id", "name"]));
        cache.put(rel(1, "users", &["id", "name", "email"]));
        assert_eq!(cache.len(), 1, "тот же OID не создаёт вторую запись");
        assert_eq!(cache.get(1).unwrap().columns.len(), 3, "победила новая схема");
    }

    #[test]
    fn clear_drops_everything() {
        // Кэш живёт в рамках сессии репликации: при реконнекте сбрасывается целиком,
        // потому что сервер перешлёт RELATION заново, а старая схема может быть устаревшей.
        let mut cache = RelationCache::new();
        cache.put(rel(1, "users", &["id"]));
        cache.put(rel(2, "items", &["id"]));
        cache.clear();
        assert_eq!(cache.len(), 0);
        assert!(cache.get(1).is_none());
    }
}
```

- [ ] **Step 2: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib schema 2>&1 | tail -15
```

Ожидается ошибка: нет `RelationCache`.

- [ ] **Step 3: Реализовать `RelationCache`**

Добавить в `src/schema.rs` перед тестами:

```rust
use std::collections::HashMap;

/// Кэш описаний таблиц, живущий в рамках одной сессии репликации.
/// Row-сообщения ссылаются на таблицу по OID и не несут имён колонок —
/// имена берутся отсюда по индексу.
#[derive(Debug, Default)]
pub struct RelationCache {
    by_id: HashMap<u32, Relation>,
}

impl RelationCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Повторный RELATION для известного OID заменяет запись целиком.
    pub fn put(&mut self, relation: Relation) {
        self.by_id.insert(relation.id, relation);
    }

    pub fn get(&self, id: u32) -> Option<&Relation> {
        self.by_id.get(&id)
    }

    /// Полный сброс. Вызывается при реконнекте: сервер перешлёт RELATION
    /// перед первым row-сообщением каждой таблицы в новой сессии.
    pub fn clear(&mut self) {
        self.by_id.clear();
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
}
```

- [ ] **Step 4: Запустить**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib schema 2>&1 | tail -10
```

Ожидается: 2 passed.

- [ ] **Step 5: Написать падающие тесты на сборщик**

Создать `src/transaction.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::pgoutput::{ColumnValue, PgOutputMessage, TupleData};
    use crate::schema::{Column, Relation};

    fn users_relation() -> Relation {
        Relation {
            id: 16385,
            namespace: "public".into(),
            name: "users".into(),
            replica_identity: b'f',
            columns: ["id", "name"]
                .iter()
                .map(|c| Column { name: (*c).into(), is_key: true, type_oid: 25, atttypmod: -1 })
                .collect(),
        }
    }

    fn begin(xid: u32) -> PgOutputMessage {
        PgOutputMessage::Begin { final_lsn: 0x1000, commit_timestamp: 841_423_351_314_489, xid }
    }

    fn commit() -> PgOutputMessage {
        PgOutputMessage::Commit {
            flags: 0,
            commit_lsn: 0x1000,
            end_lsn: 0x1030,
            commit_timestamp: 841_423_351_314_489,
        }
    }

    fn insert() -> PgOutputMessage {
        PgOutputMessage::Insert {
            relation_id: 16385,
            tuple: TupleData {
                columns: vec![ColumnValue::Text("1".into()), ColumnValue::Null],
            },
        }
    }

    #[test]
    fn nothing_is_emitted_before_commit() {
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        assert!(a.handle(begin(737), Lsn(0x100), &mut cache).unwrap().is_none());
        assert!(a.handle(PgOutputMessage::Relation(users_relation()), Lsn(0), &mut cache).unwrap().is_none());
        assert!(a.handle(insert(), Lsn(0x200), &mut cache).unwrap().is_none());
        assert!(!a.is_empty(), "открытая транзакция держит буфер непустым");
    }

    #[test]
    fn commit_emits_the_whole_transaction() {
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(PgOutputMessage::Relation(users_relation()), Lsn(0), &mut cache).unwrap();
        a.handle(insert(), Lsn(0x200), &mut cache).unwrap();
        let tx = a.handle(commit(), Lsn(0x1000), &mut cache).unwrap().expect("commit отдаёт транзакцию");
        assert_eq!(tx.xid, 737);
        assert_eq!(tx.commit_lsn, Lsn(0x1000));
        assert_eq!(tx.end_lsn, Lsn(0x1030), "end_lsn отдельно от commit_lsn");
        assert_eq!(tx.changes.len(), 1);
        let ev = &tx.changes[0];
        assert_eq!(ev.table, "users");
        assert_eq!(ev.transaction_id, 737);
        assert_eq!(ev.lsn, Lsn(0x200), "у события — wal_start своей строки");
        assert_eq!(ev.commit_lsn, Lsn(0x1000), "а commit_lsn общий на транзакцию");
        assert!(a.is_empty(), "после коммита буфер пуст");
    }

    #[test]
    fn column_names_come_from_the_relation_by_position() {
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(PgOutputMessage::Relation(users_relation()), Lsn(0), &mut cache).unwrap();
        a.handle(insert(), Lsn(0x200), &mut cache).unwrap();
        let tx = a.handle(commit(), Lsn(0x1000), &mut cache).unwrap().unwrap();
        let after = tx.changes[0].after.as_ref().unwrap();
        assert_eq!(after.get("id").unwrap(), "1");
        assert!(after.get("name").unwrap().is_null(), "SQL NULL становится JSON null");
    }

    #[test]
    fn row_for_unknown_relation_is_fatal() {
        // Невозможный поиск отношения — фатальная ошибка по спеке §15,
        // а не повод пропустить строку.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        let err = a.handle(insert(), Lsn(0x200), &mut cache).unwrap_err();
        assert!(matches!(err, PgcdcError::UnknownRelation { relation_id: 16385 }));
    }

    #[test]
    fn transaction_larger_than_the_limit_fails_loudly() {
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(2);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(PgOutputMessage::Relation(users_relation()), Lsn(0), &mut cache).unwrap();
        a.handle(insert(), Lsn(0x200), &mut cache).unwrap();
        a.handle(insert(), Lsn(0x210), &mut cache).unwrap();
        let err = a.handle(insert(), Lsn(0x220), &mut cache).unwrap_err();
        assert!(matches!(err, PgcdcError::TransactionTooLarge { limit: 2 }));
    }

    #[test]
    fn reset_drops_a_half_assembled_transaction() {
        // При реконнекте недособранная транзакция выбрасывается: её BEGIN был
        // после confirmed_flush_lsn, значит она придёт заново целиком.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        assert!(!a.is_empty());
        a.reset();
        assert!(a.is_empty());
    }

    #[test]
    fn relation_outside_a_transaction_is_accepted() {
        // RELATION приходит внутри транзакции в наших фикстурах, но кэш —
        // сессионный, и сообщение не обязано быть частью транзакции.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        assert!(a.handle(PgOutputMessage::Relation(users_relation()), Lsn(0), &mut cache).unwrap().is_none());
        assert_eq!(cache.len(), 1);
        assert!(a.is_empty(), "RELATION не открывает транзакцию");
    }
}
```

- [ ] **Step 6: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib transaction 2>&1 | tail -15
```

Ожидается ошибка: нет `Assembler`, нет `Transaction`.

- [ ] **Step 7: Реализовать сборщик**

Добавить `pub mod transaction;` в `src/lib.rs` и написать `src/transaction.rs` перед тестами:

```rust
use chrono::{DateTime, Utc};

use crate::error::PgcdcError;
use crate::event::{pg_micros_to_utc, ChangeEvent, Operation, Row};
use crate::lsn::Lsn;
use crate::postgres::pgoutput::{ColumnValue, PgOutputMessage, TupleData};
use crate::schema::{Relation, RelationCache};

#[derive(Debug, Clone, PartialEq)]
pub struct Transaction {
    pub xid: u32,
    /// LSN самой записи коммита. Идёт в JSON как ключ дедупликации.
    pub commit_lsn: Lsn,
    /// LSN сразу за записью коммита. ЭТО подтверждаем PostgreSQL.
    pub end_lsn: Lsn,
    pub commit_timestamp: DateTime<Utc>,
    pub changes: Vec<ChangeEvent>,
}

/// Накапливает изменения между BEGIN и COMMIT. Ничего не отдаёт наружу,
/// пока не увидит COMMIT: откаченные транзакции PostgreSQL не присылает вовсе,
/// но незавершённые — вполне, и отдавать их нельзя.
#[derive(Debug)]
pub struct Assembler {
    open: Option<OpenTx>,
    max_events: usize,
}

#[derive(Debug)]
struct OpenTx {
    xid: u32,
    changes: Vec<PendingChange>,
}

#[derive(Debug)]
struct PendingChange {
    schema: String,
    table: String,
    operation: Operation,
    after: Row,
    lsn: Lsn,
}

impl Assembler {
    pub fn new(max_events: usize) -> Self {
        Self { open: None, max_events }
    }

    /// Пуст ли буфер. От этого зависит правило keepalive (DECISIONS Q18):
    /// подтверждать позицию из keepalive можно ТОЛЬКО при пустом буфере.
    pub fn is_empty(&self) -> bool {
        self.open.is_none()
    }

    pub fn reset(&mut self) {
        self.open = None;
    }

    pub fn handle(
        &mut self,
        msg: PgOutputMessage,
        wal_start: Lsn,
        cache: &mut RelationCache,
    ) -> Result<Option<Transaction>, PgcdcError> {
        match msg {
            PgOutputMessage::Relation(rel) => {
                cache.put(rel);
                Ok(None)
            }
            PgOutputMessage::Begin { xid, .. } => {
                self.open = Some(OpenTx { xid, changes: Vec::new() });
                Ok(None)
            }
            PgOutputMessage::Insert { relation_id, tuple } => {
                let rel = cache
                    .get(relation_id)
                    .ok_or(PgcdcError::UnknownRelation { relation_id })?;
                let after = build_row(rel, &tuple)?;
                let pending = PendingChange {
                    schema: rel.namespace.clone(),
                    table: rel.name.clone(),
                    operation: Operation::Insert,
                    after,
                    lsn: wal_start,
                };
                let open = self
                    .open
                    .as_mut()
                    .ok_or_else(|| PgcdcError::Decode("row message outside a transaction".into()))?;
                if open.changes.len() >= self.max_events {
                    return Err(PgcdcError::TransactionTooLarge { limit: self.max_events });
                }
                open.changes.push(pending);
                Ok(None)
            }
            PgOutputMessage::Commit { commit_lsn, end_lsn, commit_timestamp, .. } => {
                let open = self
                    .open
                    .take()
                    .ok_or_else(|| PgcdcError::Decode("COMMIT without BEGIN".into()))?;
                let ts = pg_micros_to_utc(commit_timestamp);
                let changes = open
                    .changes
                    .into_iter()
                    .map(|c| ChangeEvent {
                        schema: c.schema,
                        table: c.table,
                        operation: c.operation,
                        before: None,
                        before_kind: None,
                        after: Some(c.after),
                        unchanged_columns: Vec::new(),
                        transaction_id: open.xid,
                        lsn: c.lsn,
                        commit_lsn: Lsn(commit_lsn),
                        commit_timestamp: ts,
                    })
                    .collect();
                Ok(Some(Transaction {
                    xid: open.xid,
                    commit_lsn: Lsn(commit_lsn),
                    end_lsn: Lsn(end_lsn),
                    commit_timestamp: ts,
                    changes,
                }))
            }
        }
    }
}

/// Имена колонок берутся из RELATION по индексу — row-сообщения их не несут.
fn build_row(rel: &Relation, tuple: &TupleData) -> Result<Row, PgcdcError> {
    if tuple.columns.len() != rel.columns.len() {
        return Err(PgcdcError::Decode(format!(
            "tuple has {} columns, relation {} has {}",
            tuple.columns.len(),
            rel.id,
            rel.columns.len()
        )));
    }
    let mut row = Row::new();
    for (col, value) in rel.columns.iter().zip(&tuple.columns) {
        let json = match value {
            ColumnValue::Text(s) => serde_json::Value::String(s.clone()),
            ColumnValue::Null => serde_json::Value::Null,
            ColumnValue::UnchangedToast => {
                // На INSERT этот тег не приходит: значение записывается в той же
                // транзакции и reorder buffer его разрешает. Если он всё-таки
                // появился — это не наш случай, и молчать нельзя.
                return Err(PgcdcError::Decode(format!(
                    "unexpected unchanged-TOAST marker on INSERT, column {}",
                    col.name
                )));
            }
        };
        row.insert(col.name.clone(), json);
    }
    Ok(row)
}
```

- [ ] **Step 8: Запустить, проверить формат и линтер**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib 2>&1 | tail -12
cargo fmt --check && echo "fmt clean"
cargo clippy --lib 2>&1 | tail -5
```

Ожидается: 29 passed, fmt чистый, clippy молчит.

- [ ] **Step 9: Коммит**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git add src/schema.rs src/transaction.rs src/lib.rs
git commit -m "feat: add relation cache and transaction assembler"
```

---

### Task 4: Sink и трекер LSN

Здесь кодируется главный инвариант проекта. Он должен держаться типом и тестом, а не
дисциплиной программиста.

**Files:**
- Create: `src/sink/mod.rs`, `src/sink/stdout.rs`
- Modify: `src/lsn.rs` (добавить `LsnTracker`), `src/lib.rs`, `Cargo.toml`

**Interfaces:**
- Consumes: `Transaction`, `Lsn`, `PgcdcError`.
- Produces:
  ```rust
  pub enum Durability { Fsync, BestEffort }
  #[async_trait::async_trait]
  pub trait Sink: Send {
      fn durability(&self) -> Durability;
      async fn write_transaction(&mut self, tx: &Transaction) -> Result<(), PgcdcError>;
  }
  pub struct StdoutSink;  // impl Sink, Durability::BestEffort
  pub struct LsnTracker { /* ... */ }
  impl LsnTracker {
      pub fn new() -> Self
      pub fn note_received(&mut self, lsn: Lsn)
      pub fn note_durable(&mut self, lsn: Lsn)
      pub fn try_ack(&mut self, lsn: Lsn) -> Result<(), PgcdcError>   // отвергает lsn > durable
      pub fn received(&self) -> Lsn
      pub fn durable(&self) -> Lsn
      pub fn acked(&self) -> Lsn
  }
  ```

- [ ] **Step 1: Написать падающий тест на инвариант трекера**

Добавить в блок тестов `src/lsn.rs`:

```rust
    #[test]
    fn tracker_refuses_to_ack_beyond_durable() {
        // Единственный инвариант, ради которого существует проект:
        // никогда не подтверждать позицию, которую sink не записал.
        let mut t = LsnTracker::new();
        t.note_received(Lsn(0x2000));
        t.note_durable(Lsn(0x1000));
        assert!(t.try_ack(Lsn(0x1000)).is_ok(), "подтвердить ровно durable можно");
        assert!(t.try_ack(Lsn(0x1001)).is_err(), "на байт дальше durable — нельзя");
        assert_eq!(t.acked(), Lsn(0x1000), "неудачная попытка не сдвигает acked");
    }

    #[test]
    fn tracker_never_moves_acked_backwards() {
        let mut t = LsnTracker::new();
        t.note_durable(Lsn(0x2000));
        t.try_ack(Lsn(0x2000)).unwrap();
        t.try_ack(Lsn(0x1000)).unwrap();
        assert_eq!(t.acked(), Lsn(0x2000), "откат подтверждения молча игнорируется");
    }

    #[test]
    fn durable_never_moves_backwards() {
        let mut t = LsnTracker::new();
        t.note_durable(Lsn(0x2000));
        t.note_durable(Lsn(0x1000));
        assert_eq!(t.durable(), Lsn(0x2000));
    }
```

- [ ] **Step 2: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib lsn 2>&1 | tail -15
```

Ожидается ошибка: нет `LsnTracker`.

- [ ] **Step 3: Реализовать трекер**

Добавить в `src/lsn.rs`:

```rust
use crate::error::PgcdcError;

/// Четыре позиции, которые нельзя путать (DECISIONS Q4).
/// Персистентности нет: слот PostgreSQL — единственный источник истины,
/// трекер живёт только в памяти процесса.
#[derive(Debug, Default)]
pub struct LsnTracker {
    received: Lsn,
    durable: Lsn,
    acked: Lsn,
}

impl LsnTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn note_received(&mut self, lsn: Lsn) {
        if lsn > self.received {
            self.received = lsn;
        }
    }

    /// Вызывается только после того, как sink подтвердил запись.
    pub fn note_durable(&mut self, lsn: Lsn) {
        if lsn > self.durable {
            self.durable = lsn;
        }
    }

    /// Отвергает попытку подтвердить позицию дальше durable. Это не
    /// оборонительное программирование, а тот самый инвариант: пройди такое
    /// подтверждение, и крах между ним и записью означал бы тихую потерю.
    pub fn try_ack(&mut self, lsn: Lsn) -> Result<(), PgcdcError> {
        if lsn > self.durable {
            return Err(PgcdcError::AckBeyondDurable {
                attempted: lsn.to_string(),
                durable: self.durable.to_string(),
            });
        }
        if lsn > self.acked {
            self.acked = lsn;
        }
        Ok(())
    }

    pub fn received(&self) -> Lsn {
        self.received
    }

    pub fn durable(&self) -> Lsn {
        self.durable
    }

    pub fn acked(&self) -> Lsn {
        self.acked
    }
}
```

- [ ] **Step 4: Запустить**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib lsn 2>&1 | tail -10
```

Ожидается: 4 passed.

- [ ] **Step 5: Добавить зависимости для sink**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo add async-trait
cargo add tracing
```

- [ ] **Step 6: Написать падающий тест на sink**

Создать `src/sink/mod.rs` с тестами:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{pg_micros_to_utc, ChangeEvent, Operation, Row};
    use crate::lsn::Lsn;
    use crate::transaction::Transaction;

    fn tx() -> Transaction {
        let mut after = Row::new();
        after.insert("id".into(), "1".into());
        Transaction {
            xid: 737,
            commit_lsn: Lsn(0x1000),
            end_lsn: Lsn(0x1030),
            commit_timestamp: pg_micros_to_utc(841_423_351_314_489),
            changes: vec![ChangeEvent {
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
            }],
        }
    }

    /// Пишет в буфер вместо stdout — так проверяется сериализация,
    /// а не поведение терминала.
    struct BufferSink {
        lines: Vec<String>,
    }

    #[async_trait::async_trait]
    impl Sink for BufferSink {
        fn durability(&self) -> Durability {
            Durability::BestEffort
        }
        async fn write_transaction(&mut self, tx: &Transaction) -> Result<(), PgcdcError> {
            for ch in &tx.changes {
                self.lines.push(serde_json::to_string(ch).unwrap());
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn sink_writes_one_line_per_change_not_one_per_transaction() {
        // Атомарность записи не равна атомарности формата (DECISIONS Q20):
        // sink получает транзакцию целиком, но сериализует её в N строк JSONL.
        let mut s = BufferSink { lines: Vec::new() };
        s.write_transaction(&tx()).await.unwrap();
        assert_eq!(s.lines.len(), 1);
        assert!(s.lines[0].starts_with(r#"{"schema":"public""#));
        assert!(!s.lines[0].contains('\n'), "внутри строки переводов быть не должно");
    }

    #[test]
    fn stdout_sink_is_honest_about_not_being_durable() {
        // Труба не даёт durability в принципе, и делать вид иначе — хуже,
        // чем признать это (DECISIONS Q6).
        assert_eq!(StdoutSink::new().durability(), Durability::BestEffort);
    }
}
```

- [ ] **Step 7: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib sink 2>&1 | tail -15
```

Ожидается ошибка: нет `Sink`, `Durability`, `StdoutSink`.

- [ ] **Step 8: Реализовать трейт и stdout-sink**

Добавить `pub mod sink;` в `src/lib.rs`. В `src/sink/mod.rs` перед тестами:

```rust
pub mod stdout;

pub use stdout::StdoutSink;

use crate::error::PgcdcError;
use crate::transaction::Transaction;

/// Что sink может обещать про запись. Kafka с `acks=all` встанет сюда же
/// как `Fsync`, а труба честно останется `BestEffort`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Данные доведены до диска: подтверждать LSN безопасно.
    Fsync,
    /// Байты отданы ядру, но их судьба неизвестна. Для разработки.
    BestEffort,
}

#[async_trait::async_trait]
pub trait Sink: Send {
    fn durability(&self) -> Durability;

    /// Получает транзакцию целиком и обязан либо записать её всю,
    /// либо вернуть ошибку. Частичная запись — это ошибка.
    async fn write_transaction(&mut self, tx: &Transaction) -> Result<(), PgcdcError>;
}
```

Создать `src/sink/stdout.rs`:

```rust
use std::io::Write;

use super::{Durability, Sink};
use crate::error::PgcdcError;
use crate::transaction::Transaction;

/// JSONL на stdout: одна строка на изменение. Только для разработки —
/// durability у трубы нет, и это объявлено честно.
#[derive(Debug, Default)]
pub struct StdoutSink;

impl StdoutSink {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl Sink for StdoutSink {
    fn durability(&self) -> Durability {
        Durability::BestEffort
    }

    async fn write_transaction(&mut self, tx: &Transaction) -> Result<(), PgcdcError> {
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        for change in &tx.changes {
            let line = serde_json::to_string(change)
                .map_err(|e| PgcdcError::Sink(format!("serialize: {e}")))?;
            writeln!(out, "{line}").map_err(|e| PgcdcError::Sink(format!("write: {e}")))?;
        }
        // Один flush на транзакцию: атомарность записи — свойство транзакции,
        // а не отдельной строки.
        out.flush().map_err(|e| PgcdcError::Sink(format!("flush: {e}")))?;
        Ok(())
    }
}
```

- [ ] **Step 9: Прогнать всё, проверить формат и линтер**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib 2>&1 | tail -12
cargo fmt --check && echo "fmt clean"
cargo clippy --lib 2>&1 | tail -5
```

Ожидается: 34 passed, fmt чистый, clippy молчит.

- [ ] **Step 10: Коммит**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git add Cargo.toml Cargo.lock src/lsn.rs src/sink src/lib.rs
git commit -m "feat(sink): add sink trait, stdout sink, lsn tracker"
```

---

### Task 5: Pre-flight guard слота

Первая задача, которой нужен настоящий PostgreSQL. Guard — компенсирующий контроль за
дефект транспорта, измеренный в этапе 0: `start()` безусловно зовёт
`ensure_replication_slot()` и при отсутствующем слоте молча создаёт новый на текущей
позиции WAL, теряя всё закоммиченное раньше.

**Files:**
- Create: `src/postgres/guard.rs`, `tests/common/mod.rs`
- Modify: `src/postgres/mod.rs`, `Cargo.toml`

**Interfaces:**
- Consumes: `PgcdcError`, `Lsn`.
- Produces:
  ```rust
  pub struct SlotInfo { pub restart_lsn: Option<Lsn>, pub confirmed_flush_lsn: Option<Lsn> }
  /// Холодный старт: слота нет — ошибка, слот НЕ создаём.
  pub async fn preflight_cold_start(conn_str: &str, slot: &str) -> Result<SlotInfo, PgcdcError>
  /// Реконнект: слот впереди durable — ошибка; позади — Ok, вызывающий пишет WARN.
  pub fn check_reconnect(slot: &str, info: &SlotInfo, durable: Lsn) -> Result<Option<String>, PgcdcError>
  ```
  и в `tests/common/mod.rs`:
  ```rust
  pub async fn start_postgres() -> (ContainerAsync<GenericImage>, String);  // (контейнер, строка подключения)
  pub async fn create_slot(conn_str: &str, slot: &str);
  ```

- [ ] **Step 1: Добавить зависимости**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo add tokio-postgres
cargo add --dev testcontainers
/usr/bin/grep -A 20 '^\[dependencies\]' Cargo.toml
```

`tokio-postgres` нужен именно потому, что guard обязан ходить **отдельным, не
репликационным** соединением: репликационное соединение не выполняет обычные запросы.
Записать в отчёт фактические версии, которые подобрал cargo.

- [ ] **Step 2: Написать падающий тест на разбор реконнекта**

Эта половина guard'а чистая и тестируется без Docker. Создать `src/postgres/guard.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn info(confirmed: u64) -> SlotInfo {
        SlotInfo {
            restart_lsn: Some(Lsn(confirmed - 0x100)),
            confirmed_flush_lsn: Some(Lsn(confirmed)),
        }
    }

    #[test]
    fn slot_ahead_of_our_durable_position_is_fatal() {
        // Кто-то подтвердил WAL, который мы не довели до sink.
        let err = check_reconnect("s", &info(0x2000), Lsn(0x1000)).unwrap_err();
        assert!(matches!(err, PgcdcError::SlotAhead { .. }));
    }

    #[test]
    fn slot_behind_is_a_warning_not_a_failure() {
        // Ожидаемый исход обрыва: последний send_feedback мог не дойти.
        // START_REPLICATION с 0/0 перечитает промежуток дубликатами,
        // что инвариант «дубликаты допустимы» прямо разрешает.
        // Падать здесь означало бы падать при каждом сетевом сбое.
        let warn = check_reconnect("s", &info(0x1000), Lsn(0x2000)).unwrap();
        assert!(warn.is_some(), "расхождение должно быть замечено");
        let text = warn.unwrap();
        assert!(text.contains("1000") && text.contains("2000"), "обе позиции в сообщении");
    }

    #[test]
    fn exact_match_is_silent() {
        assert!(check_reconnect("s", &info(0x1000), Lsn(0x1000)).unwrap().is_none());
    }

    #[test]
    fn missing_confirmed_flush_is_treated_as_zero() {
        let empty = SlotInfo { restart_lsn: None, confirmed_flush_lsn: None };
        // Слот есть, но ни разу не подтверждался — он позади любой нашей позиции.
        assert!(check_reconnect("s", &empty, Lsn(0x1000)).unwrap().is_some());
    }
}
```

- [ ] **Step 3: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib guard 2>&1 | tail -15
```

Ожидается ошибка: нет `SlotInfo`, `check_reconnect`.

- [ ] **Step 4: Реализовать guard**

Добавить `pub mod guard;` в `src/postgres/mod.rs`. В `src/postgres/guard.rs` перед тестами:

```rust
use crate::error::PgcdcError;
use crate::lsn::Lsn;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotInfo {
    pub restart_lsn: Option<Lsn>,
    pub confirmed_flush_lsn: Option<Lsn>,
}

/// Холодный старт. Проверка только существования: сравнивать
/// `confirmed_flush_lsn` не с чем, персистентной durable-позиции у нас нет и
/// не будет (DECISIONS Q4). Слот отсутствует — падаем, НЕ создаём: автосоздание
/// маскирует потерю данных, и это измерено в docs/spike-findings.md §2.4.
pub async fn preflight_cold_start(conn_str: &str, slot: &str) -> Result<SlotInfo, PgcdcError> {
    let (client, connection) = tokio_postgres::connect(conn_str, tokio_postgres::NoTls)
        .await
        .map_err(|e| PgcdcError::Connection(format!("preflight connect: {e}")))?;
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });

    let rows = client
        .query(
            "SELECT restart_lsn::text, confirmed_flush_lsn::text \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .map_err(|e| PgcdcError::Connection(format!("preflight query: {e}")))?;

    handle.abort();

    let row = rows
        .first()
        .ok_or_else(|| PgcdcError::SlotMissing { slot: slot.to_owned() })?;
    Ok(SlotInfo {
        restart_lsn: row.get::<_, Option<String>>(0).as_deref().and_then(parse_lsn),
        confirmed_flush_lsn: row.get::<_, Option<String>>(1).as_deref().and_then(parse_lsn),
    })
}

/// Реконнект внутри работающего процесса, где durable-позиция есть в памяти.
/// Возвращает `Ok(Some(text))`, если расхождение стоит записать в WARN,
/// и `Err`, только если слот ушёл ВПЕРЁД нашей durable-точки.
pub fn check_reconnect(
    slot: &str,
    info: &SlotInfo,
    durable: Lsn,
) -> Result<Option<String>, PgcdcError> {
    let confirmed = info.confirmed_flush_lsn.unwrap_or(Lsn(0));
    if confirmed > durable {
        return Err(PgcdcError::SlotAhead {
            slot: slot.to_owned(),
            slot_lsn: confirmed.to_string(),
            durable: durable.to_string(),
        });
    }
    if confirmed < durable {
        return Ok(Some(format!(
            "slot {slot} is behind our durable position: slot={confirmed}, durable={durable}; \
             the gap will be replayed as duplicates"
        )));
    }
    Ok(None)
}

/// PostgreSQL печатает LSN как `X/Y` в шестнадцатеричном виде.
fn parse_lsn(text: &str) -> Option<Lsn> {
    let (hi, lo) = text.split_once('/')?;
    let hi = u64::from_str_radix(hi, 16).ok()?;
    let lo = u64::from_str_radix(lo, 16).ok()?;
    Some(Lsn((hi << 32) | lo))
}
```

- [ ] **Step 5: Добавить тест на разбор LSN из текста**

```rust
    #[test]
    fn parses_postgres_lsn_text() {
        assert_eq!(parse_lsn("0/19300D0"), Some(Lsn(0x0193_00D0)));
        assert_eq!(parse_lsn("1/FF"), Some(Lsn(0x0000_0001_0000_00FF)));
        assert_eq!(parse_lsn("garbage"), None);
    }
```

- [ ] **Step 6: Запустить, убедиться что пять тестов проходят**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib guard 2>&1 | tail -10
```

Ожидается: 5 passed.

- [ ] **Step 7: Написать помощники для testcontainers**

Создать `tests/common/mod.rs`. Атрибут `allow(dead_code)` обязателен: этот модуль
компилируется отдельно в каждый тестовый бинарь, и функции, не нужные конкретному
бинарю, иначе дадут предупреждения и сломают ожидание «clippy молчит».

```rust
#![allow(dead_code)]

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

    let port = container.get_host_port_ipv4(5432.tcp()).await.expect("port");
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

pub async fn create_slot(client: &tokio_postgres::Client, slot: &str) {
    client
        .query(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot],
        )
        .await
        .expect("create slot");
}
```

- [ ] **Step 8: Написать интеграционный тест guard'а**

Создать `tests/guard.rs`:

```rust
mod common;

use pgcdc::error::PgcdcError;
use pgcdc::postgres::guard::preflight_cold_start;

#[tokio::test]
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
    assert!(rows.is_empty(), "guard не создаёт слот, это маскировало бы потерю данных");
}

#[tokio::test]
async fn cold_start_returns_slot_positions_when_the_slot_exists() {
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let info = preflight_cold_start(&conn, "pgcdc_slot").await.unwrap();
    assert!(info.confirmed_flush_lsn.is_some(), "у свежего слота позиция уже есть");
    assert!(info.restart_lsn.is_some());
}
```

- [ ] **Step 9: Запустить интеграционные тесты**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --test guard 2>&1 | tail -20
```

Ожидается: 2 passed. Первый запуск дольше — поднимаются контейнеры.
Если testcontainers не находит Docker, проверить `docker info` и что Docker Desktop запущен.

- [ ] **Step 10: Проверить формат и линтер, коммит**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo fmt --check && echo "fmt clean"
cargo clippy --all-targets 2>&1 | tail -5
git add Cargo.toml Cargo.lock src/postgres tests/common tests/guard.rs
git commit -m "feat(guard): add two-mode replication slot preflight"
```

---

### Task 6: Цикл репликации, CLI и демо

Сборка среза воедино. По завершении сценарий §19 спеки работает для INSERT, и spike
выбрасывается.

**Files:**
- Create: `src/postgres/replication.rs`, `src/config.rs`, `src/main.rs`, `tests/integration.rs`, `README.md`
- Modify: `src/lib.rs`, `src/postgres/mod.rs`, `Cargo.toml`
- Delete: `src/bin/spike.rs`

**Interfaces:**
- Consumes: всё предыдущее.
- Produces:
  ```rust
  pub struct Config { pub database_url: DatabaseUrl, pub publication: String,
                      pub slot: String, pub output: OutputKind,
                      pub max_transaction_events: usize }
  pub struct DatabaseUrl(String);   // Debug/Display вырезают пароль
  pub async fn run(config: Config, sink: Box<dyn Sink>) -> Result<(), PgcdcError>
  ```

- [ ] **Step 1: Добавить зависимости**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo add clap --features derive,env
cargo add tracing-subscriber --features env-filter
```

- [ ] **Step 2: Написать падающий тест на сокрытие пароля**

Создать `src/config.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_never_reaches_debug_or_display() {
        // Требование §4 спеки — это тип, а не «не забыть»: раз Debug вырезает
        // пароль, ни #[derive(Debug)] на конфиге, ни поле tracing не смогут его слить.
        let url = DatabaseUrl::new("postgres://cdc:hunter2@db.example:5432/app".into());
        assert!(!format!("{url:?}").contains("hunter2"));
        assert!(!format!("{url}").contains("hunter2"));
        assert!(format!("{url}").contains("cdc"), "имя пользователя остаётся видимым");
        assert!(format!("{url}").contains("db.example"));
        assert_eq!(url.expose(), "postgres://cdc:hunter2@db.example:5432/app");
    }

    #[test]
    fn url_without_a_password_is_unchanged() {
        let url = DatabaseUrl::new("postgres://cdc@db.example:5432/app".into());
        assert!(format!("{url}").contains("cdc@db.example"));
    }
}
```

- [ ] **Step 3: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib config 2>&1 | tail -15
```

Ожидается ошибка: нет `DatabaseUrl`.

- [ ] **Step 4: Реализовать конфигурацию**

Добавить `pub mod config;` в `src/lib.rs`. В `src/config.rs` перед тестами:

```rust
use std::fmt;

use clap::{Parser, ValueEnum};

/// Обёртка над строкой подключения. Ручные Debug и Display вырезают пароль,
/// поэтому утечь он может только через явный `expose()`.
#[derive(Clone)]
pub struct DatabaseUrl(String);

impl DatabaseUrl {
    pub fn new(raw: String) -> Self {
        Self(raw)
    }

    /// Единственный способ получить строку с паролем. Использовать только
    /// при передаче в драйвер, никогда в лог.
    pub fn expose(&self) -> &str {
        &self.0
    }

    fn redacted(&self) -> String {
        // Ищем `://user:password@` и заменяем пароль звёздочками.
        let Some(scheme_end) = self.0.find("://") else {
            return self.0.clone();
        };
        let rest = &self.0[scheme_end + 3..];
        let Some(at) = rest.find('@') else {
            return self.0.clone();
        };
        let creds = &rest[..at];
        match creds.find(':') {
            Some(colon) => format!(
                "{}://{}:****@{}",
                &self.0[..scheme_end],
                &creds[..colon],
                &rest[at + 1..]
            ),
            None => self.0.clone(),
        }
    }
}

impl fmt::Display for DatabaseUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.redacted())
    }
}

impl fmt::Debug for DatabaseUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DatabaseUrl({})", self.redacted())
    }
}

/// clap требует именно `FromStr`: одного `From<String>` для `#[arg]` недостаточно,
/// и без этой реализации derive не соберётся.
impl std::str::FromStr for DatabaseUrl {
    type Err = std::convert::Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(s.to_owned()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputKind {
    Stdout,
}

#[derive(Debug, Parser)]
#[command(name = "pgcdc", about = "PostgreSQL CDC via logical replication")]
pub struct Config {
    #[arg(long, env = "PGCDC_DATABASE_URL")]
    pub database_url: DatabaseUrl,

    #[arg(long, env = "PGCDC_PUBLICATION")]
    pub publication: String,

    #[arg(long, env = "PGCDC_SLOT")]
    pub slot: String,

    #[arg(long, env = "PGCDC_OUTPUT", value_enum, default_value = "stdout")]
    pub output: OutputKind,

    #[arg(long, env = "PGCDC_MAX_TRANSACTION_EVENTS", default_value = "100000")]
    pub max_transaction_events: usize,
}
```

- [ ] **Step 5: Запустить**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib config 2>&1 | tail -10
```

Ожидается: 2 passed.

- [ ] **Step 6: Реализовать цикл репликации**

Добавить `pub mod replication;` в `src/postgres/mod.rs`. Создать
`src/postgres/replication.rs`:

```rust
use std::time::Duration;

use pg_walstream::{
    CancellationToken, LogicalReplicationStream, ReplicationStreamConfig, RetryConfig, StreamingMode,
};
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::error::PgcdcError;
use crate::lsn::{Lsn, LsnTracker};
use crate::postgres::guard::preflight_cold_start;
use crate::postgres::pgoutput::decode;
use crate::schema::RelationCache;
use crate::sink::Sink;
use crate::transaction::Assembler;

/// Строка подключения для репликационного соединения требует
/// `replication=database` — без него сервер откроет обычную сессию.
fn replication_url(base: &str) -> String {
    if base.contains('?') {
        format!("{base}&replication=database")
    } else {
        format!("{base}?replication=database")
    }
}

pub async fn run(config: Config, mut sink: Box<dyn Sink>) -> Result<(), PgcdcError> {
    // Обязательство Q25(а): guard ДО start(), потому что start() безусловно
    // зовёт ensure_replication_slot() и при отсутствующем слоте молча создаст
    // новый на текущей позиции WAL, потеряв всё закоммиченное раньше.
    let info_slot = preflight_cold_start(config.database_url.expose(), &config.slot).await?;
    info!(
        slot = %config.slot,
        restart_lsn = ?info_slot.restart_lsn.map(|l| l.to_string()),
        confirmed_flush_lsn = ?info_slot.confirmed_flush_lsn.map(|l| l.to_string()),
        "slot_preflight_ok"
    );

    let stream_config = ReplicationStreamConfig::new(
        config.slot.clone(),
        config.publication.clone(),
        1,
        StreamingMode::Off,
        Duration::from_secs(10),
        Duration::from_secs(30),
        Duration::from_secs(60),
        RetryConfig::default(),
    );

    let url = replication_url(config.database_url.expose());
    let mut stream = LogicalReplicationStream::new(&url, stream_config)
        .await
        .map_err(|e| PgcdcError::Connection(format!("open replication stream: {e}")))?;

    // start_lsn = None означает 0/0: сервер возьмёт confirmed_flush_lsn слота.
    // Слот — единственный источник истины (DECISIONS Q4, Q19).
    stream
        .start(None)
        .await
        .map_err(|e| PgcdcError::Connection(format!("start replication: {e}")))?;
    info!(slot = %config.slot, publication = %config.publication, "replication_started");

    if sink.durability() == crate::sink::Durability::BestEffort {
        warn!(
            "sink is best-effort, not durable: acknowledged positions may outlive unwritten output"
        );
    }

    let cancel = CancellationToken::new();
    let mut cache = RelationCache::new();
    let mut assembler = Assembler::new(config.max_transaction_events);
    let mut tracker = LsnTracker::new();

    loop {
        // Разрешён ТОЛЬКО next_raw_event: остальные пять API ведут в
        // recover_connection, который рестартует с last_received_lsn —
        // принятой, а не durable позиции (Q25(б)).
        let raw = stream
            .next_raw_event(&cancel)
            .await
            .map_err(|e| PgcdcError::Connection(format!("next_raw_event: {e}")))?;

        tracker.note_received(Lsn(raw.wal_end.0));

        let msg = decode(&raw.data)?;
        if let Some(tx) = assembler.handle(msg, Lsn(raw.wal_start.0), &mut cache)? {
            let changes = tx.changes.len();
            let end_lsn = tx.end_lsn;

            // Порядок нерушим: сначала sink, потом durable, только потом ack.
            sink.write_transaction(&tx).await?;
            tracker.note_durable(end_lsn);
            tracker.try_ack(end_lsn)?;

            // Подтверждаем end_lsn, НЕ commit_lsn: commit_lsn указывает на
            // начало записи коммита, и рестарт перечитал бы ту же транзакцию.
            stream.shared_lsn_feedback.update_flushed_lsn(end_lsn.0);
            stream.shared_lsn_feedback.update_applied_lsn(end_lsn.0);

            // Обязательство Q25(в): без явного вызова подтверждение уходит
            // с задержкой 18–22 с по внутреннему расписанию крейта.
            stream
                .send_feedback()
                .await
                .map_err(|e| PgcdcError::Connection(format!("send_feedback: {e}")))?;

            debug!(xid = tx.xid, changes, lsn = %end_lsn, "transaction_committed");
        }
    }
}
```

- [ ] **Step 7: Написать `main.rs`**

```rust
use std::process::ExitCode;

use clap::Parser;
use pgcdc::config::{Config, OutputKind};
use pgcdc::sink::{Sink, StdoutSink};
use tracing::error;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let config = Config::parse();
    let sink: Box<dyn Sink> = match config.output {
        OutputKind::Stdout => Box::new(StdoutSink::new()),
    };

    match pgcdc::postgres::replication::run(config, sink).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!(error_kind = e.kind(), fatal = e.is_fatal(), "{e}");
            ExitCode::FAILURE
        }
    }
}
```

Логи идут в stderr, потому что stdout занят полезной нагрузкой — иначе JSONL перемешается
с логом и станет непарсимым.

- [ ] **Step 8: Собрать**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo build 2>&1 | tail -20
```

Ожидается `Finished`. Если `stream.send_feedback()` или `raw.wal_start.0` не
компилируются — сверить с фактическими сигнатурами в `docs/spike-findings.md` §1
и поправить обращения, не меняя семантику.

- [ ] **Step 9: Написать интеграционный тест сквозного среза**

Создать `tests/integration.rs`:

```rust
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
    assert_eq!(json["after"]["id"], "1", "откаченная строка не должна приехать");

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
        tokio::spawn(async move { pgcdc::postgres::replication::run(cfg, Box::new(FailingSink)).await });

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
    assert!(err.is_fatal(), "sink, который не может двигаться, — фатальная ошибка");

    let after: String = client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 'pgcdc_slot'",
            &[],
        )
        .await
        .unwrap()
        .get(0);

    assert_eq!(before, after, "слот не должен был сдвинуться: sink ничего не записал");
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
    assert!(rows.is_empty(), "слот не создан — иначе мы маскировали бы потерю данных");
}
```

- [ ] **Step 10: Запустить интеграционные тесты**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --test integration 2>&1 | tail -25
```

Ожидается: 4 passed. Если `sink_failure_stops_us_before_the_slot_advances` падает
из-за того, что слот всё-таки сдвинулся — это настоящий баг в порядке операций, а не
проблема теста: проверить, что `send_feedback` вызывается строго после успешной записи.

- [ ] **Step 11: Удалить spike и написать README**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git rm src/bin/spike.rs
```

Создать `README.md`:

```markdown
# pgcdc

Минимальный движок Change Data Capture для PostgreSQL на Rust. Читает события логической
репликации напрямую по протоколу `pgoutput` и печатает нормализованные JSON-события.

## Что уже работает

Этап 1 — сквозной срез: `BEGIN`, `RELATION`, `INSERT`, `COMMIT` → JSON на stdout,
подтверждение LSN только после успешной записи в sink. `UPDATE` и `DELETE` пока
возвращают явную ошибку, а не игнорируются молча.

## Демо

```bash
docker compose up -d --wait

cargo run -- \
  --database-url postgres://postgres:postgres@localhost:5432/app \
  --publication pgcdc_pub \
  --slot pgcdc_slot \
  --output stdout
```

В другом терминале:

```sql
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', NULL);
```

Ожидаемый вывод:

```json
{"schema":"public","table":"users","operation":"insert","before":null,"before_kind":null,"after":{"id":"1","name":"Alice","email":"alice@example.com","bio":null},"unchanged_columns":[],"transaction_id":737,"lsn":"0/192FFC0","commit_lsn":"0/19300D0","commit_timestamp":"2026-08-30T16:42:31.314489Z"}
```

Логи идут в stderr, полезная нагрузка — в stdout, поэтому вывод можно безопасно
направлять в конвейер.

## Гарантии

Дубликаты после сбоя допустимы; тихая потеря — нет. Позиция WAL не подтверждается
PostgreSQL раньше, чем sink отчитался об успешной записи.

## Документация

- [DECISIONS.md](DECISIONS.md) — принятые решения по MVP
- [docs/pgoutput-notes.md](docs/pgoutput-notes.md) — побайтовый разбор протокола
- [docs/spike-findings.md](docs/spike-findings.md) — выводы по транспорту
```

- [ ] **Step 12: Проверить демо вживую**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
docker compose down -v && docker compose up -d --wait
cargo build
./target/debug/pgcdc --database-url postgres://postgres:postgres@localhost:5432/app \
  --publication pgcdc_pub --slot pgcdc_slot --output stdout > /tmp/pgcdc-demo.jsonl 2>/tmp/pgcdc-demo.log &
PID=$!
for i in $(seq 1 60); do /usr/bin/grep -q replication_started /tmp/pgcdc-demo.log && break; done
export PGPASSWORD=postgres
psql -h 127.0.0.1 -U postgres -d app -c "INSERT INTO users VALUES (1,'Alice','alice@example.com',NULL);"
for i in $(seq 1 60); do [ -s /tmp/pgcdc-demo.jsonl ] && break; done
kill $PID
/bin/cat /tmp/pgcdc-demo.jsonl
```

Ожидается ровно одна строка JSON с `"operation":"insert"` и `"name":"Alice"`.
Проверить, что в ней нет пароля и что лог ушёл в отдельный файл, а не смешался с данными.

- [ ] **Step 13: Прогнать всё, проверить формат и линтер**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test 2>&1 | tail -20
cargo fmt --check && echo "fmt clean"
cargo clippy --all-targets 2>&1 | tail -5
```

Ожидается: все юнит-тесты и 6 интеграционных проходят, fmt чистый, clippy молчит.

- [ ] **Step 14: Коммит**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git add -A
git commit -m "feat: wire end-to-end insert slice with cli"
```

---

## Definition of Done для этапа 1

- [ ] проект собирается на стабильном Rust, `cargo fmt --check` и `cargo clippy` чистые;
- [ ] единственный исполняемый файл стартует по конфигурации из CLI и переменных окружения;
- [ ] подключается к PostgreSQL в режиме логической репликации через существующий слот;
- [ ] декодирует `BEGIN`, `COMMIT`, `RELATION`, `INSERT`; `UPDATE`, `DELETE` и всё
      неизвестное дают явную ошибку, а не молчаливый пропуск;
- [ ] ведёт relation cache, в котором повторный OID заменяет запись;
- [ ] не отдаёт события до `COMMIT`;
- [ ] печатает JSONL на stdout, по строке на изменение, логи — в stderr;
- [ ] подтверждает `end_lsn` и только после успешной записи в sink;
- [ ] трекер LSN отвергает подтверждение дальше durable;
- [ ] pre-flight guard падает при отсутствующем слоте и слот не создаёт;
- [ ] интеграционный тест подтверждает: при падении sink `confirmed_flush_lsn` не сдвинулся;
- [ ] интеграционный тест подтверждает: откаченная транзакция не даёт событий;
- [ ] пароль не появляется ни в `Debug`, ни в `Display`, ни в логах;
- [ ] `src/bin/spike.rs` удалён;
- [ ] README содержит воспроизводимое демо по сценарию §19 спеки.
