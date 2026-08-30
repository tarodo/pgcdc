# pgcdc Этап 0 (Spike) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Доказать, что выбранный транспорт даёт полный контроль над подтверждением LSN и отдаёт сырые байты `pgoutput`, и заморозить эти байты как фикстуры для юнит-тестов этапа 2.

**Architecture:** Одноразовый бинарь `src/bin/spike.rs` подключается к заранее созданному слоту репликации в Postgres (запущенном через Docker Compose), читает сырые `XLogData`-payload'ы, печатает их в hex и сохраняет в файлы. Параллельно эмпирически проверяется, что крейт не подтверждает LSN за нашей спиной и не переподключается молча. Весь код этапа выбрасываемый; переживают только фикстуры и два документа с выводами.

**Tech Stack:** Rust 1.95.0 (Homebrew), tokio, `pg_walstream` 0.8, PostgreSQL 16 (Docker), psql 14.17 (клиент).

**Spec:** [DECISIONS.md](../../../DECISIONS.md) — решения по MVP; базовая спека [input/pgcdc_mvp_task.md](../../../input/pgcdc_mvp_task.md).

---

## Global Constraints

Правила действуют во **всех** задачах плана. Нарушение любого — основание отклонить задачу на ревью.

1. **PATH в песочнице урезан.** `cargo`, `docker`, `psql` отсутствуют в дефолтном `PATH`
   (`/usr/bin:/bin:/usr/sbin:/sbin` плюс каталоги плагинов). Каждая команда Bash обязана
   начинаться с:
   ```bash
   export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
   ```
   Без этого будет `command not found: cargo`. Это не опционально.
2. **Псевдонимы в профиле перекрывают базовые утилиты.** `cat` указывает на `bat`, `ls` на `eza`,
   а их нет в PATH песочницы. Для записи файлов через heredoc использовать `/bin/cat`,
   для листинга `/bin/ls`. Проверено на практике: bare `cat` падает с `command not found: bat`.
3. **Рабочая директория:** `/Users/roman/Projects/HP/rust_cdc`. Все пути относительно неё.
4. **Rust 1.95.0 поставлен через Homebrew, `rustup` отсутствует.** Нельзя использовать
   `rustup component add`, `rustup toolchain`, `+nightly`. `rustfmt` и `cargo clippy` (0.1.95) доступны.
5. **Никогда не вызывать `ensure_replication_slot()`** и любой другой автосоздающий слот вызов.
   Автосоздание маскирует потерю данных (DECISIONS Q19, спека §14). Слот создаётся только
   в `init.sql` или руками через psql.
6. **Образ Postgres: `postgres:16-alpine`** — уже скачан локально, качать ничего не нужно.
7. **Коммиты:** Conventional Commits, `type(scope): subject`, subject не длиннее 50 символов,
   тело только если «почему» не очевидно из заголовка. Автор `tarodo`, почта
   `rsvolozhanin@gmail.com` (настроено глобально, не менять). **В сообщениях коммитов запрещены
   любые трейлеры соавторства и любые футеры о том, каким инструментом сгенерирован код.**
   Только заголовок и, при необходимости, тело по существу.
8. **Юнит-тестов в этом этапе нет, и это осознанно.** Этап 0 — spike: проверка эмпирическая
   («выполни команду, увидь ожидаемый вывод»). TDD начинается с этапа 2 и опирается на фикстуры,
   которые производит этот этап. Ревьюер не должен отклонять задачи за отсутствие `#[test]`.
9. **Код spike'а выбрасывается** в конце этапа 1. Не вкладываться в его архитектуру, не выносить
   абстракции, не писать для него тесты. Живут только `tests/fixtures/`, `docs/spike-findings.md`
   и `docs/pgoutput-notes.md`.
10. **Пароль Postgres — `postgres`**, база локальная и одноразовая. Допустимо только потому,
    что контейнер слушает `127.0.0.1` и живёт на дев-машине.

---

## File Structure

| Файл | Ответственность |
|------|-----------------|
| `.gitignore` | Исключить `/target`, локальные артефакты, данные контейнера |
| `Cargo.toml` | Пакет `pgcdc`, зависимости spike'а |
| `docker-compose.yml` | Единственный сервис `postgres` с `wal_level=logical` |
| `docker/init.sql` | Схема, публикация, слот — в правильном порядке |
| `scripts/gen-fixtures.sql` | Детерминированный DML, порождающий все нужные типы сообщений |
| `src/bin/spike.rs` | Одноразовый читатель сырых байтов (выбрасывается) |
| `docs/spike-findings.md` | Вердикт по контролируемости транспорта, фактические сигнатуры API |
| `docs/pgoutput-notes.md` | Ручной разбор байтов, заметки по формату сообщений |
| `tests/fixtures/*.bin` | Замороженные payload'ы — вход для юнит-тестов этапа 2 |
| `tests/fixtures/MANIFEST.md` | Что за файл, какой SQL его породил, ожидаемый разбор |

---

### Task 1: Репозиторий и Postgres в Docker

Фундамент. Без работающего Postgres с `wal_level=logical` и существующим слотом остальные
задачи невыполнимы.

**Files:**
- Create: `.gitignore`
- Create: `Cargo.toml`
- Create: `docker-compose.yml`
- Create: `docker/init.sql`

**Interfaces:**
- Produces: запущенный Postgres на `127.0.0.1:5432`, база `app`, пользователь `postgres`/`postgres`;
  публикация `pgcdc_pub`; слот `pgcdc_slot` с плагином `pgoutput`; таблицы `public.users`
  (REPLICA IDENTITY FULL, колонка `bio` со STORAGE EXTERNAL) и `public.items`
  (REPLICA IDENTITY DEFAULT).
- Produces: строку подключения
  `postgresql://postgres:postgres@localhost:5432/app?replication=database`.

- [ ] **Step 1: Инициализировать git-репозиторий**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git init
git config user.name
git config user.email
```

Ожидается `tarodo` и `rsvolozhanin@gmail.com`. Если пусто — остановиться и сообщить,
не выставлять другого автора самостоятельно.

- [ ] **Step 2: Создать `.gitignore`**

```gitignore
/target
**/*.rs.bk
.DS_Store
/docker/pgdata
```

Обрати внимание: `tests/fixtures/*.bin` **не** игнорируются, это артефакт, который обязан
попасть в репозиторий.

- [ ] **Step 3: Создать `Cargo.toml`**

```toml
[package]
name = "pgcdc"
version = "0.1.0"
edition = "2021"
rust-version = "1.95"

[dependencies]
pg_walstream = "0.8"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal", "time"] }
anyhow = "1"
```

`edition = "2021"` выбрана сознательно: нужна максимальная совместимость
с Homebrew-тулчейном, переключить версию компилятора мы не можем.

- [ ] **Step 4: Создать `docker/init.sql`**

Порядок критичен: таблицы, потом публикация, потом слот. Слот создаётся последним, потому
что запоминает позицию WAL на момент создания, и всё случившееся раньше до нас не дойдёт.

```sql
-- Демонстрационная таблица. REPLICA IDENTITY FULL, чтобы в UPDATE/DELETE
-- приходил полный старый кортеж (before_kind = "full").
CREATE TABLE public.users (
    id    BIGINT PRIMARY KEY,
    name  TEXT,
    email TEXT,
    bio   TEXT
);
ALTER TABLE public.users REPLICA IDENTITY FULL;

-- STORAGE EXTERNAL отключает сжатие: любое значение больше ~2 КБ
-- гарантированно уезжает в TOAST. Без этого pglz сожмёт тестовую строку
-- обратно в строку, и маркер 'u' в UPDATE никогда не появится.
ALTER TABLE public.users ALTER COLUMN bio SET STORAGE EXTERNAL;

-- Вторая таблица с REPLICA IDENTITY DEFAULT, чтобы снять фикстуры
-- с before_kind = "key" (в старом кортеже только PK).
CREATE TABLE public.items (
    id    BIGINT PRIMARY KEY,
    title TEXT,
    qty   INT
);

CREATE PUBLICATION pgcdc_pub FOR TABLE public.users, public.items;

SELECT pg_create_logical_replication_slot('pgcdc_slot', 'pgoutput');
```

- [ ] **Step 5: Создать `docker-compose.yml`**

```yaml
services:
  postgres:
    image: postgres:16-alpine
    container_name: pgcdc-postgres
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: app
    ports:
      - "127.0.0.1:5432:5432"
    command:
      - postgres
      - -c
      - wal_level=logical
      - -c
      - max_replication_slots=10
      - -c
      - max_wal_senders=10
    volumes:
      - ./docker/init.sql:/docker-entrypoint-initdb.d/10-init.sql:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d app"]
      interval: 2s
      timeout: 3s
      retries: 20
```

Порт привязан к `127.0.0.1`, а не к `0.0.0.0`: база с таким паролем не должна быть
доступна из сети.

- [ ] **Step 6: Поднять и проверить**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
docker compose up -d
docker compose ps
```

Ожидается: сервис `postgres` в состоянии `healthy`, это занимает около 10 секунд.

- [ ] **Step 7: Проверить, что всё создалось правильно**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
export PGPASSWORD=postgres
psql -h 127.0.0.1 -U postgres -d app -c "SHOW wal_level;"
psql -h 127.0.0.1 -U postgres -d app -c "SELECT pubname FROM pg_publication;"
psql -h 127.0.0.1 -U postgres -d app -c "SELECT slot_name, plugin, slot_type, active, confirmed_flush_lsn FROM pg_replication_slots;"
psql -h 127.0.0.1 -U postgres -d app -c "SELECT relname, relreplident FROM pg_class WHERE relname IN ('users','items');"
```

Ожидается:
- `wal_level` равен `logical`;
- `pubname` равен `pgcdc_pub`;
- слот `pgcdc_slot`, плагин `pgoutput`, `slot_type` равен `logical`, `active` равен `f`;
- `users` даёт `relreplident` равный `f` (full), `items` даёт `d` (default).

Если `wal_level` не `logical`, контейнер поднялся со старым volume. Выполнить
`docker compose down -v` и повторить шаг 6.

- [ ] **Step 8: Коммит**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git add .gitignore Cargo.toml docker-compose.yml docker/init.sql
git commit -m "chore: add postgres compose setup for cdc spike"
```

---

### Task 2: Подключение к слоту и первые сырые байты

Главная цель этапа: увидеть настоящие байты `pgoutput`. До этого момента всё остальное теория.

**Files:**
- Create: `src/bin/spike.rs`
- Create: `docs/spike-findings.md`
- Modify: `Cargo.toml` (при необходимости — фичи `pg_walstream`)

**Interfaces:**
- Consumes: слот `pgcdc_slot` и публикацию `pgcdc_pub` из Task 1.
- Produces: `docs/spike-findings.md` с секцией «Фактический API pg_walstream» — точные,
  выписанные из исходников сигнатуры `next_raw_event`, полей `RawXLogData`, конструктора
  `ReplicationStreamConfig::new` и способа подтверждения LSN. Эти сигнатуры использует
  Task 3 и Task 4.

- [ ] **Step 1: Скачать зависимости и найти реальные сигнатуры в исходниках**

Документация на docs.rs описывает API обобщённо, поэтому источник истины — распакованные
исходники в реестре cargo, а не наши предположения.

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo fetch
SRC=$(find ~/.cargo/registry/src -maxdepth 2 -type d -name 'pg_walstream-*' | head -1)
echo "исходники: $SRC"
grep -rn "pub fn next_raw_event" "$SRC"
grep -rn "pub struct RawXLogData" -A 15 "$SRC"
grep -rn "impl ReplicationStreamConfig" -A 30 "$SRC"
grep -rn "pub enum StreamingMode" -A 8 "$SRC"
grep -rn "pub fn start" -A 6 "$SRC" --include='*.rs' | head -20
grep -rn "update_applied_lsn\|update_flushed_lsn\|get_feedback_lsn" "$SRC" --include='*.rs' | head -20
```

- [ ] **Step 2: Записать найденное в `docs/spike-findings.md`**

Создать файл со следующей структурой и заполнить первую секцию **фактическими**
сигнатурами из Step 1, копируя дословно, без пересказа:

```markdown
# Spike: выводы по транспорту

## 1. Фактический API pg_walstream 0.8

Версия крейта: <точная версия из Cargo.lock>

### Конструктор конфигурации
<дословная сигнатура ReplicationStreamConfig::new с именами и типами аргументов>

### Получение сырых байтов
<дословная сигнатура next_raw_event>

### Структура RawXLogData
<дословное объявление полей>

### Подтверждение LSN
<дословные сигнатуры методов SharedLsnFeedback>

### StreamingMode
<варианты энума>

## 2. Контролируемость транспорта
<заполняется в Task 3>

## 3. Вердикт
<заполняется в Task 3>
```

- [ ] **Step 3: Написать `src/bin/spike.rs`**

Скелет ниже задаёт структуру и все смысловые решения. Имена методов брать из Step 1;
если они отличаются, править вызовы, но **не менять логику**: `proto_version` равен 1,
streaming выключен, `ensure_replication_slot` не вызывается, подтверждение LSN намеренно
отсутствует (это проверяется в Task 3).

```rust
//! Одноразовый spike этапа 0. Выбрасывается в конце этапа 1.
//! Задача: увидеть сырые байты pgoutput и проверить, что транспорт
//! не подтверждает LSN за нашей спиной.

use std::time::Duration;

use anyhow::Result;
use pg_walstream::{
    CancellationToken, LogicalReplicationStream, ReplicationStreamConfig, RetryConfig,
    StreamingMode,
};

const CONN: &str = "postgresql://postgres:postgres@localhost:5432/app?replication=database";

#[tokio::main]
async fn main() -> Result<()> {
    // proto_version = 1: без streaming незакоммиченных транзакций (DECISIONS Q13).
    let config = ReplicationStreamConfig::new(
        "pgcdc_slot".to_string(),
        "pgcdc_pub".to_string(),
        1,
        StreamingMode::Off,
        Duration::from_secs(10),
        Duration::from_secs(30),
        Duration::from_secs(60),
        RetryConfig::default(),
    );

    let mut stream = LogicalReplicationStream::new(CONN, config).await?;

    // ВАЖНО: ensure_replication_slot() НЕ вызывается. Слот должен уже существовать.
    // Автосоздание маскирует потерю данных (DECISIONS Q19).
    stream.start(None).await?;
    eprintln!("replication started, waiting for events (Ctrl-C to stop)");

    let cancel = CancellationToken::new();
    let mut seq = 0usize;

    loop {
        let raw = stream.next_raw_event(&cancel).await?;
        seq += 1;
        dump(seq, &raw);
        // Подтверждение LSN намеренно НЕ отправляется, см. Task 3.
    }
}

/// Печатает тип сообщения, позиции WAL и hex-дамп payload'а.
fn dump(seq: usize, raw: &pg_walstream::RawXLogData) {
    let payload: &[u8] = raw.data();
    let kind = payload.first().map(|b| *b as char).unwrap_or('?');
    eprintln!(
        "--- #{seq} kind={kind:?} wal_start={:?} wal_end={:?} len={}",
        raw.wal_start(),
        raw.wal_end(),
        payload.len()
    );
    for (i, chunk) in payload.chunks(16).enumerate() {
        let hex: Vec<String> = chunk.iter().map(|b| format!("{b:02x}")).collect();
        let ascii: String = chunk
            .iter()
            .map(|b| if b.is_ascii_graphic() { *b as char } else { '.' })
            .collect();
        eprintln!("{:04x}  {:<47}  |{ascii}|", i * 16, hex.join(" "));
    }
}
```

Если `raw.data()` и `raw.wal_start()` в исходниках оказались публичными полями, а не
методами, заменить на `raw.data` и `raw.wal_start`. Это единственная правка, допустимая
без согласования.

- [ ] **Step 4: Собрать**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo build --bin spike 2>&1 | tail -30
```

Ожидается `Finished`. Первая сборка тянет зависимости и займёт несколько минут.
Если компилятор ругается на несовпадение сигнатур, вернуться к Step 1, выписать
фактические и поправить вызовы.

- [ ] **Step 5: Запустить и увидеть байты**

В одном терминале:

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
cargo run --bin spike
```

В другом:

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
export PGPASSWORD=postgres
psql -h 127.0.0.1 -U postgres -d app -c "INSERT INTO users VALUES (1,'Alice','alice@example.com',NULL);"
```

Ожидается последовательность как минимум из четырёх сообщений с `kind` равным
`'B'` (BEGIN), `'R'` (RELATION), `'I'` (INSERT), `'C'` (COMMIT). Порядок `R` и `B` может
отличаться от ожидаемого — зафиксировать фактический, он важен для этапа 2.

Если сообщений нет вовсе, проверить, что слот не занят другим процессом:
`SELECT active, active_pid FROM pg_replication_slots;`.

- [ ] **Step 6: Коммит**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git add Cargo.toml Cargo.lock src/bin/spike.rs docs/spike-findings.md
git commit -m "feat(spike): dump raw pgoutput payloads"
```

---

### Task 3: Валидация контролируемости транспорта

**Самая важная задача этапа.** Здесь решается, годится ли выбранный крейт вообще. Если он
подтверждает LSN самостоятельно или молча переподключается, инвариант `acked <= durable`
недостижим, и архитектура MVP требует другого транспорта.

**Files:**
- Modify: `docs/spike-findings.md` (секции 2 и 3)
- Modify: `src/bin/spike.rs` (временные пробы, остаются в коде spike'а)

**Interfaces:**
- Consumes: рабочий spike из Task 2, сигнатуры API из `docs/spike-findings.md`.
- Produces: секцию «Вердикт» в `docs/spike-findings.md` со значением `ГОДЕН`,
  `ГОДЕН С ОГОВОРКАМИ` или `НЕ ГОДЕН` и, в двух последних случаях, списком требуемых
  обходных путей. Вердикт — вход для планирования этапа 1.

- [ ] **Step 1: Проверить, что без нашего вызова слот не двигается**

Гипотеза: `confirmed_flush_lsn` не должен продвигаться, пока мы сами не подтвердим LSN.
Spike из Task 2 подтверждение не отправляет, значит слот обязан стоять на месте.

Запустить spike, затем в другом терминале:

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
export PGPASSWORD=postgres
psql -h 127.0.0.1 -U postgres -d app -c "SELECT slot_name, confirmed_flush_lsn FROM pg_replication_slots;"
psql -h 127.0.0.1 -U postgres -d app -c "INSERT INTO users SELECT g, 'u'||g, g||'@e.com', NULL FROM generate_series(100,200) g;"
sleep 5
psql -h 127.0.0.1 -U postgres -d app -c "SELECT slot_name, confirmed_flush_lsn FROM pg_replication_slots;"
```

Записать оба значения `confirmed_flush_lsn` в `docs/spike-findings.md`.

**Интерпретация:**
- Значение **не изменилось** — крейт подтверждает только по нашей команде, инвариант
  достижим. Хорошо.
- Значение **выросло** — крейт подтверждает автоматически, и это **блокер**. Найти
  в исходниках, где отправляется standby status update, и проверить, отключается ли это
  через `ReplicationStreamConfig` (искать `status_update`, `keepalive`, `feedback`).
  Записать вывод.

- [ ] **Step 2: Проверить, что подтверждение работает, когда мы его просим**

Добавить в цикл spike'а подтверждение по нашему решению — для пробы подтверждать
`wal_end` каждого COMMIT-сообщения (первый байт payload'а равен `b'C'`):

```rust
        if raw.data().first() == Some(&b'C') {
            stream.shared_lsn_feedback.update_applied_lsn(raw.wal_end());
            eprintln!("    -> acked {:?}", raw.wal_end());
        }
```

Пересобрать, запустить, сделать INSERT, подождать интервал status update и проверить
`confirmed_flush_lsn` — теперь он **обязан** вырасти.

**Интерпретация:** если не растёт, мы не умеем двигать слот вообще, и это второй блокер.
Проверить, не нужно ли вместо `update_applied_lsn` вызывать метод для flushed-позиции:
Postgres освобождает WAL по **flush**-позиции из standby status update, а не по apply.
Это частая ловушка — записать в findings, какой именно метод сдвинул слот.

- [ ] **Step 3: Проверить видимость разрыва соединения**

Гипотеза: при обрыве мы обязаны узнать об этом, иначе не сможем сбросить relation cache
(DECISIONS Q19), а это тихая порча данных при смене схемы.

Запустить spike, затем:

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
docker compose restart postgres
```

Наблюдать вывод spike'а.

**Интерпретация:**
- `next_raw_event` вернул `Err` — разрыв видим, реконнект под нашим контролем. Хорошо.
- Крейт молча переподключился и продолжил отдавать события — **блокер**: мы не узнаем,
  что пора сбросить кэш. Проверить `RetryConfig`, отключается ли внутренний ретрай
  (искать поля вида `max_retries`, `enabled`). Записать, каким значением он выключается.

- [ ] **Step 4: Проверить поведение при отсутствующем слоте**

Спека §14 требует падать с ненулевым кодом, а не создавать слот заново.

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
export PGPASSWORD=postgres
psql -h 127.0.0.1 -U postgres -d app -c "SELECT pg_drop_replication_slot('pgcdc_slot');"
cd /Users/roman/Projects/HP/rust_cdc
cargo run --bin spike; echo "exit code: $?"
psql -h 127.0.0.1 -U postgres -d app -c "SELECT slot_name FROM pg_replication_slots;"
```

Ожидается: spike падает с ошибкой, код возврата не равен 0, слот **не появился заново**.
Если слот воссоздался — найти, где вызывается автосоздание, и записать это.

Восстановить слот для дальнейших задач:

```bash
psql -h 127.0.0.1 -U postgres -d app -c "SELECT pg_create_logical_replication_slot('pgcdc_slot','pgoutput');"
```

- [ ] **Step 5: Записать вердикт**

Заполнить секции 2 и 3 в `docs/spike-findings.md`: таблица из четырёх проб
(проба, ожидание, факт, вывод) и итоговый вердикт.

Если вердикт **НЕ ГОДЕН** — остановиться, не начинать Task 4, вынести на обсуждение
альтернативы из DECISIONS Q2: `pgwire-replication`, форк rust-postgres, свой транспорт.
Фикстуры при этом всё равно нужны, но снимать их придётся иначе.

- [ ] **Step 6: Коммит**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git add src/bin/spike.rs docs/spike-findings.md
git commit -m "docs(spike): record transport controllability verdict"
```

---

### Task 4: Снятие байтовых фикстур

Фикстуры — единственный долгоживущий артефакт этапа. От их полноты зависит, насколько
быстрым будет TDD-цикл этапа 2: юнит-тесты декодера не должны требовать Docker.

**Files:**
- Create: `scripts/gen-fixtures.sql`
- Modify: `src/bin/spike.rs` (запись payload'ов в файлы)
- Create: `tests/fixtures/*.bin`
- Create: `tests/fixtures/MANIFEST.md`

**Interfaces:**
- Consumes: рабочий spike, вердикт «годен» из Task 3.
- Produces: набор `.bin`-файлов, каждый содержит ровно один payload `pgoutput` без обёртки
  `XLogData`. Их читают юнит-тесты этапа 2 через `include_bytes!`.

- [ ] **Step 1: Написать `scripts/gen-fixtures.sql`**

Скрипт обязан покрыть все шесть обязательных типов сообщений плюс три особых случая:
`before_kind = full`, `before_kind = key` и TOAST-маркер `'u'`.

```sql
-- Фикстура 1: одиночный INSERT (RELATION + BEGIN + INSERT + COMMIT)
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', NULL);

-- Фикстура 2: UPDATE при REPLICA IDENTITY FULL, старый кортеж целиком ('O')
UPDATE users SET name = 'Bob' WHERE id = 1;

-- Фикстура 3: DELETE при REPLICA IDENTITY FULL
DELETE FROM users WHERE id = 1;

-- Фикстура 4: REPLICA IDENTITY DEFAULT. В UPDATE старого кортежа нет,
-- в DELETE приходит только ключ ('K')
INSERT INTO items VALUES (10, 'Widget', 5);
UPDATE items SET qty = 7 WHERE id = 10;
DELETE FROM items WHERE id = 10;

-- Фикстура 5: TOAST. bio имеет STORAGE EXTERNAL, значит сжатия нет
-- и 9600 символов гарантированно уезжают из строки.
INSERT INTO users
SELECT 2, 'Carol', 'carol@example.com',
       (SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 300));
-- UPDATE не трогает bio, значит в новом кортеже придёт маркер 'u'
UPDATE users SET name = 'Caroline' WHERE id = 2;

-- Фикстура 6: многострочная транзакция
BEGIN;
INSERT INTO users VALUES (3, 'Dave', 'dave@example.com', NULL);
UPDATE users SET email = 'dave2@example.com' WHERE id = 3;
DELETE FROM users WHERE id = 3;
COMMIT;

-- Фикстура 7: откат. Не должно прийти вообще ничего.
BEGIN;
INSERT INTO users VALUES (999, 'Ghost', 'ghost@example.com', NULL);
ROLLBACK;
```

- [ ] **Step 2: Научить spike писать payload'ы в файлы**

Добавить в `dump` запись файла. Имя формируется из порядкового номера и типа сообщения,
чтобы файлы были самоописательными:

```rust
    let name = match kind {
        'B' => "begin", 'C' => "commit", 'R' => "relation",
        'I' => "insert", 'U' => "update", 'D' => "delete",
        'T' => "truncate", 'Y' => "type", 'O' => "origin",
        _ => "unknown",
    };
    let path = format!("tests/fixtures/{seq:04}_{name}.bin");
    std::fs::create_dir_all("tests/fixtures").ok();
    std::fs::write(&path, payload).expect("write fixture");
    eprintln!("    -> {path}");
```

Важно: пишется **только payload**, без позиций WAL из обёртки. Позиции живут в манифесте
как текст, потому что декодер этапа 2 получает их отдельным аргументом (DECISIONS Q17).

- [ ] **Step 3: Снять фикстуры на чистом слоте**

Чтобы нумерация была детерминированной, базу надо привести в исходное состояние:

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
docker compose down -v && docker compose up -d
sleep 12
rm -rf tests/fixtures && mkdir -p tests/fixtures
cargo run --bin spike &
SPIKE_PID=$!
sleep 3
export PGPASSWORD=postgres
psql -h 127.0.0.1 -U postgres -d app -f scripts/gen-fixtures.sql
sleep 3
kill $SPIKE_PID
/bin/ls -la tests/fixtures/
```

Ожидается: файлы для `begin`, `commit`, `relation`, `insert`, `update`, `delete`
и **ни одного** файла, порождённого откаченной транзакцией из фикстуры 7.

- [ ] **Step 4: Проверить, что TOAST-маркер действительно пойман**

Самая хрупкая фикстура: если `bio` не уехал в TOAST, маркера `'u'` не будет, и этап 2
останется без теста на важный случай.

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
export PGPASSWORD=postgres
psql -h 127.0.0.1 -U postgres -d app -c "SELECT pg_column_size(bio) FROM users WHERE id = 2;"
cd /Users/roman/Projects/HP/rust_cdc
for f in tests/fixtures/*_update.bin; do
  echo "== $f"; xxd "$f" | head -5
done
```

Первая команда должна показать размер `bio` заметно больше 2000. Затем найти
UPDATE-фикстуру, где присутствует байт `0x75` (`'u'`) в позиции маркера формата колонки,
и записать её имя в манифест. Если такой фикстуры нет, увеличить число строк
в `generate_series` до 1000 и повторить Step 3.

- [ ] **Step 5: Написать `tests/fixtures/MANIFEST.md`**

По строке на файл: имя, породивший SQL, `wal_start` и `wal_end` из лога spike'а,
и что именно этот файл проверяет в этапе 2. Пример строки:

```markdown
| Файл | SQL | wal_start | Проверяет |
|------|-----|-----------|-----------|
| `0003_insert.bin` | `INSERT INTO users VALUES (1,'Alice',...)` | `0/16B6C50` | Разбор INSERT, 4 колонки, одна NULL |
```

- [ ] **Step 6: Коммит**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git add scripts/gen-fixtures.sql src/bin/spike.rs tests/fixtures
git commit -m "test: freeze pgoutput byte fixtures"
```

---

### Task 5: Ручной разбор фикстур и заметки по протоколу

Главный учебный результат этапа. Пока формат не разобран руками, декодер этапа 2 будет
писаться наугад, а тесты будут подгоняться под реализацию вместо спецификации.

**Files:**
- Create: `docs/pgoutput-notes.md`

**Interfaces:**
- Consumes: `tests/fixtures/*.bin`, `tests/fixtures/MANIFEST.md`.
- Produces: `docs/pgoutput-notes.md` — разметку байтов по полям для каждого типа сообщения.
  Это спецификация, по которой этап 2 пишет тесты **до** реализации.

- [ ] **Step 1: Разобрать RELATION вручную**

Открыть фикстуру и сверить с документацией формата
(https://www.postgresql.org/docs/16/protocol-logicalrep-message-formats.html):

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
xxd tests/fixtures/*_relation.bin | head -20
```

Расписать в `docs/pgoutput-notes.md` побайтово: тип сообщения (1 байт), OID отношения
(4 байта, big-endian), namespace (C-строка), имя (C-строка), replica identity (1 байт),
число колонок (2 байта), затем на каждую колонку — флаги (1 байт), имя (C-строка),
type OID (4 байта), atttypmod (4 байта).

Обязательно сверить: `relreplident` для `users` должен быть `f`, для `items` — `d`.
Это прямая проверка, что структура читается правильно, а не подгоняется под ожидание.

- [ ] **Step 2: Разобрать BEGIN и COMMIT**

BEGIN: final LSN (8 байт), commit timestamp (8 байт, микросекунды от 2000-01-01), xid (4 байта).
COMMIT: флаги (1 байт), commit LSN (8 байт), end LSN (8 байт), commit timestamp (8 байт).

Проверить себя арифметикой: перевести timestamp в дату и убедиться, что получилось
сегодняшнее число, а не 1970 и не 2000 год. Ошибка в эпохе классическая, и поймать её
здесь дешевле, чем в этапе 2.

Зафиксировать, какой именно LSN из COMMIT мы будем подтверждать (по DECISIONS Q17 — end LSN),
и выписать его фактическое значение из фикстуры.

- [ ] **Step 3: Разобрать INSERT, UPDATE, DELETE и маркеры кортежей**

Расписать структуру TupleData: число колонок (2 байта), затем на каждую байт формата
(`'n'` это null, `'u'` это unchanged TOAST, `'t'` это text), и для `'t'` длина (4 байта)
и данные.

Отдельно зафиксировать по фикстурам:
- UPDATE для `users` (REPLICA IDENTITY FULL) — присутствует ли маркер `'O'` перед новым кортежем;
- UPDATE для `items` (DEFAULT) при неизменившемся ключе — старого кортежа быть не должно;
- DELETE для `items` — маркер `'K'` и только PK-колонка, остальные `'n'`;
- UPDATE с TOAST — маркер `'u'` в позиции `bio`.

Это ровно те четыре случая, которые в этапе 2 станут отдельными тестами на `before_kind`
и `unchanged_columns`.

- [ ] **Step 4: Записать открытые вопросы**

Секция «Не разобрано» — всё, что осталось непонятным: неожиданные байты, типы сообщений,
которых не ждали, расхождения с документацией. Пустая секция допустима, но врать нельзя:
если что-то не сошлось, это должно быть написано, а не замолчано.

- [ ] **Step 5: Коммит**

```bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/roman/Projects/HP/rust_cdc
git add docs/pgoutput-notes.md tests/fixtures/MANIFEST.md
git commit -m "docs: annotate pgoutput message layouts"
```

---

## Definition of Done для этапа 0

- [ ] `docker compose up -d` поднимает Postgres с `wal_level=logical`, публикацией и слотом;
- [ ] `cargo run --bin spike` печатает hex сырых payload'ов после DML в psql;
- [ ] `docs/spike-findings.md` содержит фактические сигнатуры API и вердикт по четырём пробам;
- [ ] доказано, что слот не двигается без нашего явного подтверждения;
- [ ] доказано, что разрыв соединения нам виден;
- [ ] доказано, что отсутствующий слот даёт ненулевой код возврата и слот не воссоздаётся;
- [ ] `tests/fixtures/` содержит фикстуры для RELATION, BEGIN, INSERT, UPDATE, DELETE, COMMIT,
      включая случаи `before_kind = full`, `before_kind = key` и TOAST-маркер `'u'`;
- [ ] откаченная транзакция не породила ни одной фикстуры;
- [ ] `docs/pgoutput-notes.md` содержит побайтовый разбор всех шести типов сообщений.
