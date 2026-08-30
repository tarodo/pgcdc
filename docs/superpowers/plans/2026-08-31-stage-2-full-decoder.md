# pgcdc Этап 2 (Полный декодер) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Декодировать `UPDATE` и `DELETE` во всех формах, различать «полная старая строка» и «только ключ», и не подавать несланное TOAST-значение как `null`.

**Architecture:** Декодер получает два новых варианта сообщения; тег старого кортежа (`'O'`/`'K'`/отсутствует) живёт в варианте сообщения, а не внутри `TupleData`, чтобы существующие литералы кортежей в тестах остались валидными. Сборка строки расщепляется надвое: полный кортеж отдаёт `(Row, Vec<String>)`, где вторым идут колонки с маркером `'u'`, а ключевой кортеж отдаёт только те колонки, которые сервер реально прислал. Тесты декодера читают замороженные фикстуры и Docker не требуют; Docker нужен только двум случаям, которых в фикстурах нет.

**Tech Stack:** Rust 1.95.0 (Homebrew), tokio, `pg_walstream` 0.8, serde_json, chrono, clap, tracing, testcontainers (dev), PostgreSQL 16 в Docker.

**Spec:** [DECISIONS.md](../../../DECISIONS.md). Байтовая разметка, на которую опирается план: [docs/pgoutput-notes.md](../../pgoutput-notes.md) §10 (UPDATE), §11 (DELETE), §12 (четыре контрольных случая), §7 (TupleData).

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
5. **Foreground `sleep` заблокирован песочницей.** В async-коде — `tokio::time`,
   в шелле — ограниченные циклы опроса, для контейнера — `docker compose up -d --wait`.
6. **Фикстуры в `tests/fixtures/` — замороженные захваты с реального сервера.** Ни один
   файл там не изменяется и не добавляется в этом этапе. Синтетические байты, которых нет
   в захвате, живут **только** в тестовом модуле как литералы с явным именем и
   комментарием о происхождении. Ценность каталога фикстур ровно в том, что всё в нём
   настоящее; подложить туда рукописные байты — значит эту ценность уничтожить.
7. **Все значения колонок — строки; SQL NULL — настоящий JSON `null`** (DECISIONS Q16).
8. **`'n'` означает две разные вещи в зависимости от кортежа.** В `'N'`/`'O'` это
   настоящий SQL NULL. В `'K'` это «сервер не прислал колонку»: в
   `0019_delete.bin` строка на момент удаления имела `title = 'Widget'`, `qty = 7`, и обе
   пришли как `'n'`. Подать их как `null` — вранье о данных. Различать обязательно по
   тегу **кортежа**, а не по тегу колонки.
9. **Маркер `'u'` не попадает в `after` вообще.** Колонка опускается и называется в
   `unchanged_columns`. Записать `"bio": null` — тихая порча: потребитель решит, что
   значение обнулили (DECISIONS Q15, ruling R8 этапа 0).
10. **Порядок в цикле репликации и подтверждаемая позиция не меняются.** Оба проверены
    мутационно на живом сервере. Любое изменение — критический дефект.
11. **`cargo fmt --check`, `cargo clippy --all-targets` и `cargo test` чистые перед коммитом.**
12. **TDD обязателен.** Сначала падающий тест, запуск, **реальный вывод падения вставляется
    в отчёт по ходу дела**, затем минимальная реализация. Если красный шаг неожиданно
    прошёл — так и написать, а не выдавать это за цикл красный-зелёный.
13. **Коммиты:** Conventional Commits, `type(scope): subject`, subject **не длиннее 50
    символов — посчитать перед коммитом**. Автор `tarodo` настроен глобально, не менять.
    В сообщении только заголовок и, при необходимости, тело по существу. **Запрещены любые
    трейлеры соавторства и любые футеры о том, каким инструментом сгенерирован код.**

---

## File Structure

| Файл | Что меняется |
|------|--------------|
| `src/config.rs` | `FromStr` становится инфаллибельным, появляется `validate()` |
| `src/error.rs` | Вариант ошибки для невалидного URL |
| `src/postgres/replication.rs` | Вызов `validate()` первой строкой `run()` |
| `src/postgres/pgoutput.rs` | `OldTupleKind`, варианты `Update` и `Delete`, их разбор |
| `src/transaction.rs` | `build_full_row` и `build_key_row`, ветки UPDATE и DELETE |
| `src/sink/stdout.rs` | Сериализация выносится в тестируемую функцию |
| `tests/integration.rs` | Два случая, которых нет в фикстурах |

---

### Task 1: Закрыть дефекты, перенесённые из этапа 1

Этап 1 закрылся с тремя незакрытыми находками. Две из них — про утечку пароля, и обе
живут ровно в том коде, который писался, чтобы пароль не утекал.

**Files:**
- Modify: `src/config.rs`, `src/error.rs`, `src/postgres/replication.rs`, `src/sink/stdout.rs`
- Modify: `tests/integration.rs`

**Interfaces:**
- Produces:
  ```rust
  impl std::str::FromStr for DatabaseUrl { type Err = std::convert::Infallible; }
  impl DatabaseUrl { pub fn validate(&self) -> Result<(), PgcdcError>; }
  // в src/error.rs
  PgcdcError::InvalidDatabaseUrl                    // без полей: текст не содержит ввода
  // в src/sink/stdout.rs
  pub(crate) fn write_changes<W: std::io::Write>(w: &mut W, tx: &Transaction) -> Result<(), PgcdcError>
  ```

- [ ] **Step 1: Написать падающий тест на то, что clap больше не печатает ввод**

Сейчас `DatabaseUrl::from_str` возвращает ошибку, и clap оборачивает её в собственный
текст `invalid value '<весь ввод>' for '--database-url'`. Пароль оказывается в stderr
при каждом старте — одинаково и для флага, и для переменной окружения. Переписать текст
нашей ошибки нельзя: ввод подставляет clap, а не мы.

Добавить в блок тестов `src/config.rs`:

```rust
    #[test]
    fn from_str_accepts_anything_so_clap_never_echoes_the_input() {
        // clap печатает отвергнутое значение в собственной обёртке «invalid value '...'».
        // Единственный способ этого избежать — не давать clap повода отвергнуть:
        // разбор всегда успешен, а проверка живёт в validate().
        let libpq = "host=db user=cdc password=hunter2 dbname=app";
        let parsed: DatabaseUrl = libpq.parse().expect("разбор обязан быть инфаллибельным");
        assert!(parsed.validate().is_err(), "но validate обязан это отвергнуть");
    }

    #[test]
    fn validate_rejects_libpq_key_value_form() {
        let url = DatabaseUrl::new("host=db user=cdc password=hunter2".into());
        let err = url.validate().unwrap_err();
        assert!(matches!(err, PgcdcError::InvalidDatabaseUrl));
        assert!(
            !err.to_string().contains("hunter2"),
            "текст ошибки не должен содержать ввод: {err}"
        );
    }

    #[test]
    fn validate_rejects_a_password_containing_a_scheme_separator() {
        // Подстрочная проверка «содержит ://» пропускала libpq-строку, в ПАРОЛЕ
        // которой есть ://, а redacted() возвращал такую строку дословно.
        let url = DatabaseUrl::new("host=db password=weird://leak dbname=app".into());
        assert!(url.validate().is_err());
    }

    #[test]
    fn validate_accepts_both_url_schemes() {
        assert!(DatabaseUrl::new("postgres://u:p@h:5432/db".into()).validate().is_ok());
        assert!(DatabaseUrl::new("postgresql://u:p@h:5432/db".into()).validate().is_ok());
    }
```

- [ ] **Step 2: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib config 2>&1 | tail -25
```

Ожидается ошибка компиляции: нет метода `validate`, нет варианта `InvalidDatabaseUrl`.
Вставить реальный вывод в отчёт.

- [ ] **Step 3: Добавить вариант ошибки**

В `src/error.rs`, в перечисление, рядом с остальными вариантами:

```rust
    #[error("database URL must start with postgres:// or postgresql:// (libpq key=value connection strings are not supported)")]
    InvalidDatabaseUrl,
```

Текст намеренно не содержит подстановки: ввод в него попасть не должен ни при каких
обстоятельствах. Добавить арм в оба исчерпывающих `match`:

```rust
            Self::InvalidDatabaseUrl => "invalid_database_url",
```

```rust
            Self::InvalidDatabaseUrl => true,
```

- [ ] **Step 4: Заменить `FromStr` и добавить `validate`**

В `src/config.rs` заменить реализацию `FromStr` на инфаллибельную:

```rust
/// Разбор намеренно не может провалиться. Если вернуть здесь ошибку, clap напечатает
/// отвергнутое значение целиком в своей обёртке «invalid value '...'», и пароль
/// окажется в stderr. Проверка живёт в `validate()`, который зовётся первой строкой
/// `run()`, где текст ошибки контролируем мы.
impl std::str::FromStr for DatabaseUrl {
    type Err = std::convert::Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(s.to_owned()))
    }
}
```

И добавить в `impl DatabaseUrl`:

```rust
    /// Принимаем только URL-форму. Строку libpq (`host=... password=...`) отвергаем:
    /// её нельзя ни отредактировать (`redacted()` не найдёт `@` и вернёт ввод дословно),
    /// ни корректно дополнить параметром репликации. Принять формат, который мы не умеем
    /// обработать, — значит слить секрет и всё равно упасть.
    pub fn validate(&self) -> Result<(), PgcdcError> {
        if self.0.starts_with("postgres://") || self.0.starts_with("postgresql://") {
            Ok(())
        } else {
            Err(PgcdcError::InvalidDatabaseUrl)
        }
    }
```

Добавить `use crate::error::PgcdcError;` в начало файла, если его там нет.

**И убрать то, что стало мёртвым.** Этап 1 завёл в `src/config.rs` собственный тип ошибки
для отвергающего `from_str` и тесты под него. После этой правки тип не используется
никем — `cargo clippy` уронит сборку на неиспользуемом коде. Удалить объявление типа,
его `impl`, импорт `thiserror` из этого файла, если он больше не нужен, и тот тест
этапа 1, который утверждал, что `from_str` возвращает ошибку: это утверждение теперь
ложно по построению. Тесты на `Debug`/`Display`-редактирование и на `--help`
оставить — они по-прежнему верны и по-прежнему нужны.

- [ ] **Step 5: Вызвать `validate` первой строкой `run()`**

В `src/postgres/replication.rs`, самой первой строкой тела `run`, **до** pre-flight
проверки слота:

```rust
    // Первым делом — до любого подключения и любого лога, где могла бы всплыть строка.
    config.database_url.validate()?;
```

- [ ] **Step 6: Запустить тесты конфигурации**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib config 2>&1 | tail -12
```

Ожидается: все тесты конфигурации зелёные (четыре новых плюс три прежних).

- [ ] **Step 7: Написать падающий тест на прямое покрытие stdout-sink**

Сейчас единственный тест «одна строка на изменение» работает с дублёром `BufferSink`,
определённым в тестовом модуле. Заменить `StdoutSink` на «один JSON-массив на транзакцию»
можно, и вся связка останется зелёной.

Добавить в блок тестов `src/sink/stdout.rs` (создать блок, если его нет):

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

    fn two_change_tx() -> Transaction {
        Transaction {
            xid: 737,
            commit_lsn: Lsn(0x1000),
            end_lsn: Lsn(0x1030),
            commit_timestamp: pg_micros_to_utc(841_423_351_314_489),
            changes: vec![change("1"), change("2")],
        }
    }

    #[test]
    fn shipped_serializer_writes_one_line_per_change() {
        // Прямое покрытие настоящего кода записи, а не дублёра: подмени здесь
        // JSONL на один массив на транзакцию — и этот тест обязан покраснеть.
        let mut buf: Vec<u8> = Vec::new();
        write_changes(&mut buf, &two_change_tx()).unwrap();
        let text = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2, "две строки на две записи");
        assert!(lines[0].contains(r#""id":"1""#));
        assert!(lines[1].contains(r#""id":"2""#));
        assert!(text.ends_with('\n'), "каждая строка завершена переводом строки");
        for line in lines {
            serde_json::from_str::<serde_json::Value>(line).expect("каждая строка — валидный JSON");
        }
    }
}
```

- [ ] **Step 8: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib sink 2>&1 | tail -20
```

Ожидается ошибка компиляции: нет функции `write_changes`.

- [ ] **Step 9: Вынести сериализацию в тестируемую функцию**

В `src/sink/stdout.rs` заменить тело `write_transaction` так, чтобы работа делалась в
свободной функции, а `StdoutSink` только подставлял поток:

```rust
/// Сериализация транзакции в JSONL. Вынесена из `StdoutSink`, чтобы её можно было
/// проверить напрямую, а не через тестовый дублёр.
pub(crate) fn write_changes<W: std::io::Write>(
    w: &mut W,
    tx: &Transaction,
) -> Result<(), PgcdcError> {
    for change in &tx.changes {
        let line = serde_json::to_string(change)
            .map_err(|e| PgcdcError::Sink(format!("serialize: {e}")))?;
        writeln!(w, "{line}").map_err(|e| PgcdcError::Sink(format!("write: {e}")))?;
    }
    Ok(())
}
```

И `write_transaction` становится:

```rust
    async fn write_transaction(&mut self, tx: &Transaction) -> Result<(), PgcdcError> {
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        write_changes(&mut out, tx)?;
        // Один flush на транзакцию: атомарность записи — свойство транзакции,
        // а не отдельной строки.
        out.flush().map_err(|e| PgcdcError::Sink(format!("flush: {e}")))?;
        Ok(())
    }
```

- [ ] **Step 10: Обновить интеграционный тест на утечку**

В `tests/integration.rs` найти тест, запускающий настоящий бинарь при отсутствующем слоте.
Добавить рядом с ним новый, проверяющий, что при libpq-строке пароль не попадает никуда:

```rust
#[tokio::test]
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

    assert!(!output.status.success(), "невалидный URL обязан давать ненулевой код");
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
```

Тест не поднимает контейнер: невалидный URL отвергается до любого подключения.

- [ ] **Step 11: Прогнать всё и проверить вживую**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test 2>&1 | /usr/bin/grep -E '^test result'
cargo fmt --check && echo "fmt clean"
cargo clippy --all-targets 2>&1 | tail -5
cargo build 2>&1 | tail -3
PGCDC_DATABASE_URL='host=db user=cdc password=SUPERSECRET_XYZZY dbname=app' \
  ./target/debug/pgcdc --publication p --slot s 2>&1 | /usr/bin/grep -c SUPERSECRET_XYZZY || echo "пароля в выводе нет"
```

Последняя команда обязана напечатать «пароля в выводе нет». Если печатает число больше
нуля — утечка не закрыта.

- [ ] **Step 12: Коммит**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git add src/config.rs src/error.rs src/postgres/replication.rs src/sink/stdout.rs tests/integration.rs
git commit -m "fix: stop clap echoing rejected database url"
```

---

### Task 2: Декодирование UPDATE и DELETE

Ядро этапа. Тесты читают замороженные фикстуры, Docker не нужен, цикл измеряется секундами.
Все ожидаемые значения взяты из `docs/pgoutput-notes.md` — это спецификация, писать тест
«под реализацию» запрещено.

**Files:**
- Modify: `src/postgres/pgoutput.rs`

**Interfaces:**
- Produces:
  ```rust
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub enum OldTupleKind { Key, Full }

  // новые варианты в PgOutputMessage:
  Update { relation_id: u32, old: Option<(OldTupleKind, TupleData)>, new: TupleData },
  Delete { relation_id: u32, old_kind: OldTupleKind, old: TupleData },
  ```
  Тег старого кортежа лежит в варианте сообщения, а не внутри `TupleData`, чтобы
  существующие литералы `TupleData { columns: ... }` в тестах остались валидными.

- [ ] **Step 1: Написать падающие тесты на UPDATE во всех трёх формах**

Добавить в блок тестов `src/postgres/pgoutput.rs`:

```rust
    const UPDATE_FULL: &[u8] = include_bytes!("../../tests/fixtures/0006_update.bin");
    const UPDATE_NO_OLD: &[u8] = include_bytes!("../../tests/fixtures/0016_update.bin");
    const UPDATE_TOAST: &[u8] = include_bytes!("../../tests/fixtures/0025_update.bin");

    /// UPDATE с тегом 'K' — DEFAULT-идентичность и изменившийся ключ.
    /// В захвате этой формы нет (docs/pgoutput-notes.md §14 п.3), поэтому байты
    /// собраны вручную по разметке §10 и §7: 'U', OID 16392 (items),
    /// 'K', старый кортеж {id:"10", n, n}, 'N', новый кортеж {"11","Widget","7"}.
    /// В tests/fixtures/ такие байты класть нельзя — там только реальные захваты.
    const SYNTHETIC_UPDATE_KEY: &[u8] = &[
        0x55, 0x00, 0x00, 0x40, 0x08, // 'U', OID 16392
        0x4B, 0x00, 0x03, // 'K', ncols=3
        0x74, 0x00, 0x00, 0x00, 0x02, 0x31, 0x30, // t(2)="10"
        0x6E, 0x6E, // 'n', 'n' — заглушки неключевых колонок
        0x4E, 0x00, 0x03, // 'N', ncols=3
        0x74, 0x00, 0x00, 0x00, 0x02, 0x31, 0x31, // t(2)="11"
        0x74, 0x00, 0x00, 0x00, 0x06, 0x57, 0x69, 0x64, 0x67, 0x65, 0x74, // t(6)="Widget"
        0x74, 0x00, 0x00, 0x00, 0x01, 0x37, // t(1)="7"
    ];

    #[test]
    fn decodes_update_with_full_old_tuple() {
        let PgOutputMessage::Update { relation_id, old, new } = decode(UPDATE_FULL).unwrap() else {
            panic!("ожидался Update")
        };
        assert_eq!(relation_id, 16385);
        let (kind, old_tuple) = old.expect("при REPLICA IDENTITY FULL старый кортеж есть");
        assert_eq!(kind, OldTupleKind::Full);
        assert_eq!(old_tuple.columns[1], ColumnValue::Text("Alice".into()));
        assert_eq!(new.columns[1], ColumnValue::Text("Bob".into()));
    }

    #[test]
    fn decodes_update_without_an_old_tuple() {
        // Offset 5 — 'N', а не 'O'/'K'. Один байт отличает «есть before» от «нет before»;
        // отличать по длине сообщения или по счёту тегов нельзя.
        assert_eq!(UPDATE_NO_OLD[5], b'N');
        let PgOutputMessage::Update { relation_id, old, new } = decode(UPDATE_NO_OLD).unwrap()
        else {
            panic!("ожидался Update")
        };
        assert_eq!(relation_id, 16392);
        assert!(old.is_none(), "ключ не менялся — старой версии строки нет вовсе");
        assert_eq!(
            new.columns,
            vec![
                ColumnValue::Text("10".into()),
                ColumnValue::Text("Widget".into()),
                ColumnValue::Text("7".into()),
            ]
        );
    }

    #[test]
    fn decodes_update_with_key_only_old_tuple() {
        let PgOutputMessage::Update { old, new, .. } = decode(SYNTHETIC_UPDATE_KEY).unwrap()
        else {
            panic!("ожидался Update")
        };
        let (kind, old_tuple) = old.expect("тег 'K' даёт старый кортеж");
        assert_eq!(kind, OldTupleKind::Key);
        assert_eq!(old_tuple.columns.len(), 3, "в 'K'-кортеже запись на каждую колонку");
        assert_eq!(old_tuple.columns[0], ColumnValue::Text("10".into()));
        assert_eq!(old_tuple.columns[1], ColumnValue::Null, "заглушка, не NULL");
        assert_eq!(new.columns[0], ColumnValue::Text("11".into()));
    }

    #[test]
    fn decodes_update_with_unchanged_toast_marker() {
        // Асимметрия: старый кортеж несёт bio целиком (9600 байт), новый — один байт 'u'.
        let PgOutputMessage::Update { old, new, .. } = decode(UPDATE_TOAST).unwrap() else {
            panic!("ожидался Update")
        };
        let (kind, old_tuple) = old.expect("FULL");
        assert_eq!(kind, OldTupleKind::Full);
        let ColumnValue::Text(old_bio) = &old_tuple.columns[3] else {
            panic!("старый bio обязан приехать текстом")
        };
        assert_eq!(old_bio.len(), 9600);
        assert_eq!(new.columns[3], ColumnValue::UnchangedToast);
        assert_eq!(new.columns[1], ColumnValue::Text("Caroline".into()));
    }
```

- [ ] **Step 2: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib pgoutput 2>&1 | tail -25
```

Ожидается ошибка компиляции: нет `OldTupleKind`, нет варианта `Update`. Существующий тест
`update_and_delete_are_explicitly_unsupported_in_this_stage` при этом ещё зелёный — он
падёт на следующем шаге, и это нормально: его условие перестаёт быть верным.

- [ ] **Step 3: Реализовать `OldTupleKind` и разбор UPDATE**

В `src/postgres/pgoutput.rs` добавить перечисление рядом с `ColumnValue`:

```rust
/// Что именно сервер прислал в старом кортеже. Различие несущее: при `Key`
/// неключевые колонки приходят с тегом `'n'`, и это «не прислано», а не NULL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OldTupleKind {
    /// Тег `'K'` — только колонки replica identity.
    Key,
    /// Тег `'O'` — полная старая строка (REPLICA IDENTITY FULL).
    Full,
}
```

Добавить варианты в `PgOutputMessage`:

```rust
    Update {
        relation_id: u32,
        old: Option<(OldTupleKind, TupleData)>,
        new: TupleData,
    },
```

И ветку в `decode` перед `other =>`:

```rust
        'U' => {
            let relation_id = r.u32()?;
            // Байт в позиции 5 решает всё: 'O'/'K' — дальше старый кортеж,
            // 'N' — старого нет и это уже новый. Третьего не дано.
            let tag = r.u8()?;
            let (old, new_tag) = match tag {
                b'O' => (Some((OldTupleKind::Full, read_tuple(&mut r)?)), r.u8()?),
                b'K' => (Some((OldTupleKind::Key, read_tuple(&mut r)?)), r.u8()?),
                b'N' => (None, b'N'),
                other => {
                    return Err(PgcdcError::Decode(format!(
                        "UPDATE expects tuple tag 'O', 'K' or 'N', got {:?}",
                        other as char
                    )))
                }
            };
            if new_tag != b'N' {
                return Err(PgcdcError::Decode(format!(
                    "UPDATE expects new tuple tag 'N', got {:?}",
                    new_tag as char
                )));
            }
            PgOutputMessage::Update { relation_id, old, new: read_tuple(&mut r)? }
        }
```

- [ ] **Step 3b: Добавить временные ветки в сборщик, иначе крейт не соберётся**

`Assembler::handle` матчится по `PgOutputMessage` исчерпывающе, без `_ =>`. Как только
в перечислении появился `Update`, файл `src/transaction.rs` перестаёт компилироваться —
и никакой тест декодера не запустится, пока это не закрыто. Это цена исчерпывающего
match'а, и цена правильная: компилятор именно так и обязан требовать решения по каждому
новому варианту.

Событие с before-образом строится в задаче 3. Пока — явная ошибка, а не `_ => Ok(None)`:
молчаливый пропуск row-сообщения запрещён спекой §8 и был бы ровно тем дефектом, от
которого эта архитектура защищает. Добавить в `handle`, рядом с веткой `Insert`:

```rust
            // Декодер эти сообщения уже понимает, сборщик — ещё нет (задача 3).
            // Явная ошибка, а не молчаливый пропуск: потерять row-сообщение хуже,
            // чем упасть.
            PgOutputMessage::Update { .. } | PgOutputMessage::Delete { .. } => Err(
                PgcdcError::Decode("update/delete assembly not implemented yet".into()),
            ),
```

И тест, фиксирующий это промежуточное состояние честно — в блок тестов
`src/transaction.rs`:

```rust
    #[test]
    fn update_is_decoded_but_not_yet_assembled() {
        // Временный тест: задача 3 заменяет ветку и удаляет его.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        let err = a
            .handle(
                PgOutputMessage::Update {
                    relation_id: 16385,
                    old: None,
                    new: TupleData { columns: vec![] },
                },
                Lsn(0x200),
                &mut cache,
            )
            .unwrap_err();
        assert!(matches!(err, PgcdcError::Decode(_)));
    }
```

Ветка `Delete` в перечислении появится на шаге 7 — до тех пор образец
`PgOutputMessage::Delete { .. }` в этом match'е писать нельзя, он не скомпилируется.
Добавить сначала только `Update`, а `Delete` дописать в ту же ветку на шаге 7.

- [ ] **Step 4: Запустить, увидеть падение старого теста**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib pgoutput 2>&1 | tail -25
```

Ожидается: четыре новых теста зелёные, а
`update_and_delete_are_explicitly_unsupported_in_this_stage` падает на ветке `'U'` —
она больше не возвращает `UnsupportedMessage`. Вставить реальный вывод в отчёт.

- [ ] **Step 5: Написать падающие тесты на DELETE в обеих формах**

```rust
    const DELETE_FULL: &[u8] = include_bytes!("../../tests/fixtures/0009_delete.bin");
    const DELETE_KEY: &[u8] = include_bytes!("../../tests/fixtures/0019_delete.bin");

    #[test]
    fn decodes_delete_with_full_old_tuple() {
        let PgOutputMessage::Delete { relation_id, old_kind, old } = decode(DELETE_FULL).unwrap()
        else {
            panic!("ожидался Delete")
        };
        assert_eq!(relation_id, 16385);
        assert_eq!(old_kind, OldTupleKind::Full);
        assert_eq!(old.columns.len(), 4);
        assert_eq!(old.columns[1], ColumnValue::Text("Bob".into()));
    }

    #[test]
    fn decodes_delete_with_key_only_tuple_carrying_a_slot_per_column() {
        // ncols = 3, не 1: в 'K'-кортеже запись на КАЖДУЮ колонку таблицы,
        // просто неключевые заполнены 'n'.
        let PgOutputMessage::Delete { old_kind, old, .. } = decode(DELETE_KEY).unwrap() else {
            panic!("ожидался Delete")
        };
        assert_eq!(old_kind, OldTupleKind::Key);
        assert_eq!(old.columns.len(), 3);
        assert_eq!(old.columns[0], ColumnValue::Text("10".into()));
        assert_eq!(old.columns[1], ColumnValue::Null);
        assert_eq!(old.columns[2], ColumnValue::Null);
    }

    #[test]
    fn delete_without_a_tuple_tag_is_an_error() {
        // У DELETE тег обязателен: удалённую строку надо чем-то идентифицировать.
        let bad = [0x44u8, 0x00, 0x00, 0x40, 0x08, 0x4E, 0x00, 0x00];
        assert!(matches!(decode(&bad), Err(PgcdcError::Decode(_))));
    }
```

- [ ] **Step 6: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib pgoutput 2>&1 | tail -25
```

Ожидается ошибка компиляции: нет варианта `Delete`.

- [ ] **Step 7: Реализовать разбор DELETE**

Добавить вариант:

```rust
    Delete {
        relation_id: u32,
        old_kind: OldTupleKind,
        old: TupleData,
    },
```

И ветку в `decode`:

```rust
        'D' => {
            let relation_id = r.u32()?;
            // В отличие от UPDATE тег обязателен: «ничего» не бывает.
            let old_kind = match r.u8()? {
                b'K' => OldTupleKind::Key,
                b'O' => OldTupleKind::Full,
                other => {
                    return Err(PgcdcError::Decode(format!(
                        "DELETE expects tuple tag 'K' or 'O', got {:?}",
                        other as char
                    )))
                }
            };
            PgOutputMessage::Delete { relation_id, old_kind, old: read_tuple(&mut r)? }
        }
```

- [ ] **Step 7b: Дописать `Delete` во временную ветку сборщика**

Теперь, когда вариант существует, расширить образец из шага 3b:

```rust
            PgOutputMessage::Update { .. } | PgOutputMessage::Delete { .. } => Err(
                PgcdcError::Decode("update/delete assembly not implemented yet".into()),
            ),
```

- [ ] **Step 8: Обновить тест на неподдерживаемые сообщения**

`update_and_delete_are_explicitly_unsupported_in_this_stage` больше не отражает
реальность. Заменить его на проверку того, что остальные типы всё ещё отвергаются явно:

```rust
    #[test]
    fn other_message_kinds_are_still_explicitly_unsupported() {
        // TRUNCATE, TYPE, ORIGIN и всё неизвестное по-прежнему обязаны давать
        // явную ошибку, а не молчаливый пропуск (спека §8).
        for kind in [b'T', b'Y', b'O', b'M', b'S'] {
            let payload = [kind, 0x00, 0x00, 0x00, 0x00];
            assert!(
                matches!(decode(&payload), Err(PgcdcError::UnsupportedMessage { .. })),
                "тип {:?} должен быть явно неподдержан",
                kind as char
            );
        }
    }
```

- [ ] **Step 9: Прогнать всё, проверить формат и линтер**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib 2>&1 | /usr/bin/grep -E '^test result'
cargo fmt --check && echo "fmt clean"
cargo clippy --all-targets 2>&1 | tail -5
```

Ожидается: все юнит-тесты зелёные, fmt чистый, clippy молчит.

- [ ] **Step 10: Коммит**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git add src/postgres/pgoutput.rs
git commit -m "feat(pgoutput): decode update and delete"
```

---

### Task 3: Событие из UPDATE и DELETE

Здесь решается, что увидит потребитель. Три ошибки в этой задаче неотличимы от корректного
вывода по форме и различимы только по смыслу: подать заглушку `'n'` как `null`, подать
несланное TOAST-значение как `null`, перепутать `key` и `full`.

**Files:**
- Modify: `src/transaction.rs`

**Interfaces:**
- Consumes: `OldTupleKind`, варианты `Update`/`Delete`.
- Produces:
  ```rust
  /// Полный кортеж ('N' или 'O'). Возвращает строку и имена колонок,
  /// пришедших с маркером 'u' — их в строке нет.
  fn build_full_row(rel: &Relation, tuple: &TupleData) -> Result<(Row, Vec<String>), PgcdcError>
  /// Кортеж 'K': только те колонки, которые сервер реально прислал.
  fn build_key_row(rel: &Relation, tuple: &TupleData) -> Result<Row, PgcdcError>
  ```

- [ ] **Step 1: Написать падающие тесты на построение строк**

Добавить в блок тестов `src/transaction.rs`:

```rust
    fn items_relation() -> Relation {
        Relation {
            id: 16392,
            namespace: "public".into(),
            name: "items".into(),
            replica_identity: b'd',
            columns: vec![
                Column { name: "id".into(), is_key: true, type_oid: 20, atttypmod: -1 },
                Column { name: "title".into(), is_key: false, type_oid: 25, atttypmod: -1 },
                Column { name: "qty".into(), is_key: false, type_oid: 23, atttypmod: -1 },
            ],
        }
    }

    #[test]
    fn key_tuple_omits_columns_the_server_did_not_send() {
        // В 0019_delete.bin строка на момент удаления имела title='Widget', qty=7.
        // Оба приехали как 'n'. Подать их как null — вранье о данных: значения
        // существовали, сервер их просто не прислал.
        let tuple = TupleData {
            columns: vec![
                ColumnValue::Text("10".into()),
                ColumnValue::Null,
                ColumnValue::Null,
            ],
        };
        let row = build_key_row(&items_relation(), &tuple).unwrap();
        assert_eq!(row.len(), 1, "только присланная колонка");
        assert_eq!(row.get("id").unwrap(), "10");
        assert!(!row.contains_key("title"), "title отсутствует, а не равен null");
        assert!(!row.contains_key("qty"), "qty отсутствует, а не равен null");
    }

    #[test]
    fn full_tuple_keeps_real_nulls_and_reports_unchanged_toast() {
        let tuple = TupleData {
            columns: vec![
                ColumnValue::Text("10".into()),
                ColumnValue::Null,
                ColumnValue::UnchangedToast,
            ],
        };
        let (row, unchanged) = build_full_row(&items_relation(), &tuple).unwrap();
        assert!(row.get("title").unwrap().is_null(), "'n' в полном кортеже — настоящий NULL");
        assert!(!row.contains_key("qty"), "'u' не попадает в строку вообще");
        assert_eq!(unchanged, vec!["qty".to_string()]);
    }
```

- [ ] **Step 2: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib transaction 2>&1 | tail -20
```

Ожидается ошибка компиляции: нет `build_key_row`, у `build_full_row` другая сигнатура.

- [ ] **Step 3: Реализовать построение строк**

Заменить существующую `build_row` в `src/transaction.rs` на две функции:

```rust
/// Полный кортеж — тег `'N'` или `'O'`. В нём запись на каждую колонку.
/// `'n'` здесь означает настоящий SQL NULL. `'u'` означает, что сервер не переслал
/// неизменившееся TOAST-значение: колонка в строку не попадает вовсе, её имя
/// возвращается вторым элементом, чтобы уехать в `unchanged_columns`.
/// Записать её как `null` было бы тихой порчей — потребитель решил бы, что значение обнулили.
fn build_full_row(rel: &Relation, tuple: &TupleData) -> Result<(Row, Vec<String>), PgcdcError> {
    check_arity(rel, tuple)?;
    let mut row = Row::new();
    let mut unchanged = Vec::new();
    for (col, value) in rel.columns.iter().zip(&tuple.columns) {
        match value {
            ColumnValue::Text(s) => {
                row.insert(col.name.clone(), serde_json::Value::String(s.clone()));
            }
            ColumnValue::Null => {
                row.insert(col.name.clone(), serde_json::Value::Null);
            }
            ColumnValue::UnchangedToast => unchanged.push(col.name.clone()),
        }
    }
    Ok((row, unchanged))
}

/// Кортеж `'K'` — только replica identity. Число элементов равно числу колонок
/// таблицы, но неключевые заполнены `'n'`, и это НЕ NULL, а «сервер не прислал».
/// Поэтому в строку попадает только то, что реально приехало.
fn build_key_row(rel: &Relation, tuple: &TupleData) -> Result<Row, PgcdcError> {
    check_arity(rel, tuple)?;
    let mut row = Row::new();
    for (col, value) in rel.columns.iter().zip(&tuple.columns) {
        if let ColumnValue::Text(s) = value {
            row.insert(col.name.clone(), serde_json::Value::String(s.clone()));
        }
    }
    Ok(row)
}

fn check_arity(rel: &Relation, tuple: &TupleData) -> Result<(), PgcdcError> {
    if tuple.columns.len() != rel.columns.len() {
        return Err(PgcdcError::Decode(format!(
            "tuple has {} columns, relation {} has {}",
            tuple.columns.len(),
            rel.id,
            rel.columns.len()
        )));
    }
    Ok(())
}
```

Поправить существующую ветку `Insert`: она теперь зовёт `build_full_row` и обязана
отвергнуть непустой список — маркер `'u'` на INSERT не приходит, значение пишется в той же
транзакции и reorder buffer его разрешает:

```rust
                let (after, unchanged) = build_full_row(rel, &tuple)?;
                if !unchanged.is_empty() {
                    return Err(PgcdcError::Decode(format!(
                        "unexpected unchanged-TOAST markers on INSERT: {unchanged:?}"
                    )));
                }
```

- [ ] **Step 4: Запустить**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib transaction 2>&1 | tail -15
```

Ожидается: два новых теста зелёные, существующие тоже.

- [ ] **Step 5: Написать падающие тесты на сборку событий UPDATE и DELETE**

```rust
    fn users_relation_full() -> Relation {
        Relation {
            id: 16385,
            namespace: "public".into(),
            name: "users".into(),
            replica_identity: b'f',
            columns: vec![
                Column { name: "id".into(), is_key: true, type_oid: 20, atttypmod: -1 },
                Column { name: "bio".into(), is_key: true, type_oid: 25, atttypmod: -1 },
            ],
        }
    }

    #[test]
    fn update_with_full_old_tuple_reports_before_kind_full() {
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(PgOutputMessage::Relation(users_relation_full()), Lsn(0), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Update {
                relation_id: 16385,
                old: Some((
                    OldTupleKind::Full,
                    TupleData {
                        columns: vec![
                            ColumnValue::Text("2".into()),
                            ColumnValue::Text("old bio".into()),
                        ],
                    },
                )),
                new: TupleData {
                    columns: vec![ColumnValue::Text("2".into()), ColumnValue::UnchangedToast],
                },
            },
            Lsn(0x200),
            &mut cache,
        )
        .unwrap();
        let tx = a.handle(commit(), Lsn(0x1000), &mut cache).unwrap().unwrap();
        let ev = &tx.changes[0];
        assert_eq!(ev.operation, Operation::Update);
        assert_eq!(ev.before_kind, Some(BeforeKind::Full));
        assert_eq!(ev.before.as_ref().unwrap().get("bio").unwrap(), "old bio");
        assert!(
            !ev.after.as_ref().unwrap().contains_key("bio"),
            "несланное TOAST-значение не попадает в after"
        );
        assert_eq!(ev.unchanged_columns, vec!["bio".to_string()]);
    }

    #[test]
    fn update_without_an_old_tuple_reports_no_before_at_all() {
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(PgOutputMessage::Relation(items_relation()), Lsn(0), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Update {
                relation_id: 16392,
                old: None,
                new: TupleData {
                    columns: vec![
                        ColumnValue::Text("10".into()),
                        ColumnValue::Text("Widget".into()),
                        ColumnValue::Text("7".into()),
                    ],
                },
            },
            Lsn(0x200),
            &mut cache,
        )
        .unwrap();
        let tx = a.handle(commit(), Lsn(0x1000), &mut cache).unwrap().unwrap();
        let ev = &tx.changes[0];
        assert!(ev.before.is_none());
        assert_eq!(ev.before_kind, None);
        assert_eq!(ev.after.as_ref().unwrap().get("qty").unwrap(), "7");
    }

    #[test]
    fn delete_with_key_tuple_reports_only_the_columns_that_arrived() {
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(PgOutputMessage::Relation(items_relation()), Lsn(0), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Delete {
                relation_id: 16392,
                old_kind: OldTupleKind::Key,
                old: TupleData {
                    columns: vec![
                        ColumnValue::Text("10".into()),
                        ColumnValue::Null,
                        ColumnValue::Null,
                    ],
                },
            },
            Lsn(0x200),
            &mut cache,
        )
        .unwrap();
        let tx = a.handle(commit(), Lsn(0x1000), &mut cache).unwrap().unwrap();
        let ev = &tx.changes[0];
        assert_eq!(ev.operation, Operation::Delete);
        assert_eq!(ev.before_kind, Some(BeforeKind::Key));
        assert!(ev.after.is_none(), "у DELETE нового кортежа нет");
        let before = ev.before.as_ref().unwrap();
        assert_eq!(before.len(), 1);
        assert!(!before.contains_key("title"), "заглушка не превращается в null");
    }

    #[test]
    fn serialized_delete_event_matches_the_contract() {
        // Проверка формы наружу, а не только внутренних структур.
        let mut cache = RelationCache::new();
        let mut a = Assembler::new(1000);
        a.handle(begin(737), Lsn(0x100), &mut cache).unwrap();
        a.handle(PgOutputMessage::Relation(items_relation()), Lsn(0), &mut cache).unwrap();
        a.handle(
            PgOutputMessage::Delete {
                relation_id: 16392,
                old_kind: OldTupleKind::Key,
                old: TupleData {
                    columns: vec![
                        ColumnValue::Text("10".into()),
                        ColumnValue::Null,
                        ColumnValue::Null,
                    ],
                },
            },
            Lsn(0x200),
            &mut cache,
        )
        .unwrap();
        let tx = a.handle(commit(), Lsn(0x1000), &mut cache).unwrap().unwrap();
        let json = serde_json::to_string(&tx.changes[0]).unwrap();
        assert!(json.contains(r#""operation":"delete""#));
        assert!(json.contains(r#""before_kind":"key""#));
        assert!(json.contains(r#""before":{"id":"10"}"#));
        assert!(json.contains(r#""after":null"#));
        assert!(json.contains(r#""unchanged_columns":[]"#));
    }
```

- [ ] **Step 6: Запустить, убедиться что падает**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --lib transaction 2>&1 | tail -25
```

Ожидается ошибка: `Assembler::handle` не покрывает варианты `Update` и `Delete`.

- [ ] **Step 7: Реализовать ветки UPDATE и DELETE в сборщике**

Расширить `PendingChange`, чтобы он нёс обе стороны:

```rust
#[derive(Debug)]
struct PendingChange {
    schema: String,
    table: String,
    operation: Operation,
    before: Option<Row>,
    before_kind: Option<BeforeKind>,
    after: Option<Row>,
    unchanged_columns: Vec<String>,
    lsn: Lsn,
}
```

Добавить импорт `BeforeKind` и `OldTupleKind`, затем ветки в `handle` рядом с `Insert`:

```rust
            PgOutputMessage::Update { relation_id, old, new } => {
                let open = self
                    .open
                    .as_mut()
                    .ok_or_else(|| PgcdcError::Decode("row message outside a transaction".into()))?;
                if open.changes.len() >= self.max_events {
                    return Err(PgcdcError::TransactionTooLarge { limit: self.max_events });
                }
                let rel = cache
                    .get(relation_id)
                    .ok_or(PgcdcError::UnknownRelation { relation_id })?;
                let (before, before_kind) = match &old {
                    Some((OldTupleKind::Full, tuple)) => {
                        let (row, _) = build_full_row(rel, tuple)?;
                        (Some(row), Some(BeforeKind::Full))
                    }
                    Some((OldTupleKind::Key, tuple)) => {
                        (Some(build_key_row(rel, tuple)?), Some(BeforeKind::Key))
                    }
                    None => (None, None),
                };
                let (after, unchanged_columns) = build_full_row(rel, &new)?;
                open.changes.push(PendingChange {
                    schema: rel.namespace.clone(),
                    table: rel.name.clone(),
                    operation: Operation::Update,
                    before,
                    before_kind,
                    after: Some(after),
                    unchanged_columns,
                    lsn: wal_start,
                });
                Ok(None)
            }
            PgOutputMessage::Delete { relation_id, old_kind, old } => {
                let open = self
                    .open
                    .as_mut()
                    .ok_or_else(|| PgcdcError::Decode("row message outside a transaction".into()))?;
                if open.changes.len() >= self.max_events {
                    return Err(PgcdcError::TransactionTooLarge { limit: self.max_events });
                }
                let rel = cache
                    .get(relation_id)
                    .ok_or(PgcdcError::UnknownRelation { relation_id })?;
                let (before, before_kind) = match old_kind {
                    OldTupleKind::Full => {
                        let (row, _) = build_full_row(rel, &old)?;
                        (Some(row), Some(BeforeKind::Full))
                    }
                    OldTupleKind::Key => {
                        (Some(build_key_row(rel, &old)?), Some(BeforeKind::Key))
                    }
                };
                open.changes.push(PendingChange {
                    schema: rel.namespace.clone(),
                    table: rel.name.clone(),
                    operation: Operation::Delete,
                    before,
                    before_kind,
                    after: None,
                    unchanged_columns: Vec::new(),
                    lsn: wal_start,
                });
                Ok(None)
            }
```

Удалить временную ветку `PgOutputMessage::Update { .. } | PgOutputMessage::Delete { .. }`
из задачи 2 вместе с её тестом `update_is_decoded_but_not_yet_assembled` — обе заменяются
настоящими ветками выше.

Поправить ветку `Insert`, чтобы она заполняла новые поля `PendingChange`
(`before: None`, `before_kind: None`, `after: Some(after)`,
`unchanged_columns: Vec::new()`), и ветку `Commit`, чтобы она переносила их в
`ChangeEvent` вместо жёстко зашитых значений:

```rust
                    .map(|c| ChangeEvent {
                        schema: c.schema,
                        table: c.table,
                        operation: c.operation,
                        before: c.before,
                        before_kind: c.before_kind,
                        after: c.after,
                        unchanged_columns: c.unchanged_columns,
                        transaction_id: open.xid,
                        lsn: c.lsn,
                        commit_lsn: Lsn(commit_lsn),
                        commit_timestamp: ts,
                    })
```

- [ ] **Step 8: Прогнать всё, проверить формат и линтер**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test 2>&1 | /usr/bin/grep -E '^test result'
cargo fmt --check && echo "fmt clean"
cargo clippy --all-targets 2>&1 | tail -5
```

Ожидается: все тесты зелёные, fmt чистый, clippy молчит.

- [ ] **Step 9: Коммит**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git add src/transaction.rs
git commit -m "feat: emit before images for update and delete"
```

---

### Task 4: Два случая, которых нет в фикстурах

Захват этапа 0 не содержит ни UPDATE с тегом `'K'`, ни повторного `RELATION` для того же
OID. Оба поведения реализованы, но проверены только синтетическими байтами и юнит-тестом,
то есть нашим представлением о протоколе, а не самим протоколом. Здесь они проверяются на
живом сервере.

**Files:**
- Modify: `tests/integration.rs`, `tests/common/mod.rs`

**Interfaces:**
- Consumes: `common::{start_postgres, connect, setup_schema, create_slot}`.
- Produces: `common::setup_items_table` — таблица с REPLICA IDENTITY DEFAULT в публикации.

- [ ] **Step 1: Добавить в помощники таблицу с DEFAULT-идентичностью**

В `tests/common/mod.rs` добавить функцию рядом с `setup_schema`:

```rust
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
```

- [ ] **Step 2: Написать тест на UPDATE с изменившимся ключом**

Добавить в `tests/integration.rs`:

```rust
#[tokio::test]
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
        pgcdc::postgres::replication::run(cfg, Box::new(ChannelSink(tx_send))).await
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
    assert_eq!(json["before_kind"], "key", "ключ менялся — сервер шлёт тег 'K'");
    assert_eq!(json["before"]["id"], "10", "старое значение ключа");
    assert!(
        json["before"].get("title").is_none(),
        "неключевые колонки сервер не прислал, и в before их быть не должно: {json}"
    );
    assert_eq!(json["after"]["id"], "11");
    assert_eq!(json["after"]["title"], "Widget");

    handle.abort();
}
```

- [ ] **Step 3: Написать тест на повторный RELATION**

```rust
#[tokio::test]
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
        pgcdc::postgres::replication::run(cfg, Box::new(ChannelSink(tx_send))).await
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
```

- [ ] **Step 4: Запустить интеграционные тесты**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test --test integration 2>&1 | tail -25
```

Ожидается: все интеграционные тесты зелёные. Если тест на ключ падает с
`before_kind = null` вместо `"key"` — значит сервер не прислал тег `'K'`; проверить,
что `items` действительно в публикации и что у неё REPLICA IDENTITY DEFAULT, а не FULL.

- [ ] **Step 5: Обновить README и заметки по протоколу**

В `README.md`, в разделе «Что уже работает», заменить описание этапа на актуальное:
декодируются `BEGIN`, `COMMIT`, `RELATION`, `INSERT`, `UPDATE`, `DELETE`; в событии есть
`before` с различением «полная строка» и «только ключ», а несланные TOAST-значения
называются в `unchanged_columns` и в `after` не появляются.

В `docs/pgoutput-notes.md` §14 отметить пункт 3 (отсутствие фикстуры UPDATE с тегом `'K'`)
как закрытый интеграционным тестом на живом сервере, с указанием имени теста, и пункт про
повторный `RELATION` — так же. Не удалять пункты: они фиксируют, что именно захват не
содержит, и это остаётся правдой.

- [ ] **Step 6: Прогнать всё**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo test 2>&1 | /usr/bin/grep -E '^test result'
cargo fmt --check && echo "fmt clean"
cargo clippy --all-targets 2>&1 | tail -5
```

- [ ] **Step 7: Коммит**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git add tests/ README.md docs/pgoutput-notes.md
git commit -m "test: cover key update and relation resend"
```

---

## Definition of Done для этапа 2

- [ ] `cargo test`, `cargo fmt --check`, `cargo clippy --all-targets` чистые;
- [ ] декодируются `UPDATE` во всех трёх формах (`'O'`, `'K'`, без старого кортежа) и
      `DELETE` в обеих (`'O'`, `'K'`);
- [ ] `TRUNCATE`, `TYPE`, `ORIGIN` и неизвестные типы по-прежнему дают явную ошибку;
- [ ] `before_kind` принимает значения `"full"`, `"key"` и `null` и соответствует тегу;
- [ ] в кортеже `'K'` неключевые колонки **отсутствуют** в `before`, а не равны `null`;
- [ ] колонка с маркером `'u'` отсутствует в `after` и названа в `unchanged_columns`;
- [ ] маркер `'u'` на INSERT даёт явную ошибку;
- [ ] юнит-тесты декодера и сборщика проходят без Docker;
- [ ] интеграционные тесты на живом сервере покрывают UPDATE с изменившимся ключом и
      повторный `RELATION` после DDL;
- [ ] пароль не появляется в stderr при невалидной строке подключения;
- [ ] `StdoutSink` покрыт прямым тестом сериализации;
- [ ] ни один файл в `tests/fixtures/` не изменён и не добавлен.
