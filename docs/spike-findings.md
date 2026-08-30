# Spike: выводы по транспорту

## 1. Фактический API pg_walstream 0.8

Версия крейта: `0.8.1` (точная версия из `Cargo.lock`, resolved from `pg_walstream = "0.8"`).

Источник: `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/pg_walstream-0.8.1/src/stream.rs`,
`src/lsn.rs`, `src/types.rs`, `src/lib.rs`.

Важно: типы `LogicalReplicationStream`, `RawXLogData`, `ReplicationStreamConfig`,
`StreamingMode` реэкспортируются из `lib.rs` под `#[cfg(any(feature = "libpq", feature =
"rustls-tls"))]`. В `Cargo.toml` крейта `default = ["std", "rustls-tls"]`, поэтому при
`pg_walstream = "0.8"` без указания фич (как в нашем `Cargo.toml` из Task 1) эти типы уже
доступны — `rustls-tls` включена по умолчанию и удовлетворяет `cfg`-гейт. Правка
`Cargo.toml` не потребовалась.

### Конструктор конфигурации

```rust
// impl ReplicationStreamConfig (src/stream.rs:178)
#[allow(clippy::too_many_arguments)]
pub fn new(
    slot_name: String,
    publication_name: String,
    protocol_version: u32,
    streaming_mode: StreamingMode,
    feedback_interval: Duration,
    connection_timeout: Duration,
    health_check_interval: Duration,
    retry_config: RetryConfig,
) -> Self
```

Совпадает дословно (порядок и типы аргументов) со скелетом из брифа. Поля `messages`,
`binary`, `two_phase`, `origin`, `slot_options`, `slot_type`, `stop_at_lsn` не входят в
позиционные аргументы `new` — они получают значения по умолчанию внутри тела `new`
(`messages: false`, `binary: false`, `two_phase: false`, `origin: None`,
`slot_options: ReplicationSlotOptions { snapshot: Some("nothing".to_string()), ..Default::default() }`,
`slot_type: SlotType::Logical`, `stop_at_lsn: None`) и меняются только через `with_*`
билдер-методы (не использовались в этом спайке).

### Конструктор стрима

```rust
// impl LogicalReplicationStream (src/stream.rs:446)
pub async fn new(connection_string: &str, config: ReplicationStreamConfig) -> Result<Self>
```

### Запуск репликации

```rust
// impl LogicalReplicationStream (src/stream.rs:619)
pub async fn start(&mut self, start_lsn: Option<XLogRecPtr>) -> Result<()>
```

`ensure_replication_slot()` существует как отдельный публичный метод
(`pub async fn ensure_replication_slot(&mut self) -> Result<()>`, src/stream.rs:528), но
**не вызывался** — по требованию брифа и Rulings, слот `pgcdc_slot` уже существует
(Task 1), автосоздание запрещено.

### Получение сырых байтов

```rust
// impl LogicalReplicationStream (src/stream.rs:815)
pub async fn next_raw_event(
    &mut self,
    cancellation_token: &CancellationToken,
) -> Result<RawXLogData>
```

Совпадает дословно со скелетом брифа. Комментарий в исходнике над функцией явно
подтверждает нужную семантику: *"Decode `raw.data` (pgoutput bytes) yourself, then ack:
`stream.shared_lsn_feedback.update_applied_lsn(raw.wal_end.value())`"* и (на строке 771)
*"There is no auto-ack and no retry/recovery on this path (that is the point — you own
restart semantics)"*.

### Структура RawXLogData

```rust
// src/stream.rs:60
pub struct RawXLogData {
    /// Server WAL start position for this message (`start_lsn`).
    pub wal_start: Lsn,
    /// WAL end position — the next byte after this message. Ack with this.
    pub wal_end: Lsn,
    /// Server send time (Postgres-epoch microseconds).
    pub server_time: TimestampTz,
    /// Undecoded pgoutput message bytes (everything after the 25-byte header).
    pub data: Bytes,
}
```

**Отклонение от скелета брифа (единственное разрешённое):** `wal_start`, `wal_end`, `data`
— публичные ПОЛЯ, а не методы. В брифе было `raw.data()`, `raw.wal_start()`,
`raw.wal_end()` — заменено на `raw.data`, `raw.wal_start`, `raw.wal_end` в `dump()`.

Вспомогательные типы (`src/types.rs`):
```rust
pub struct Lsn(pub u64);              // Debug/Display/Ord реализованы
pub type TimestampTz = i64;
pub type XLogRecPtr = u64;
```

### Подтверждение LSN

`LogicalReplicationStream` содержит публичное поле:

```rust
// src/stream.rs:44
pub shared_lsn_feedback: Arc<SharedLsnFeedback>,
```

Сигнатуры методов `SharedLsnFeedback` (`src/lsn.rs`):

```rust
impl SharedLsnFeedback {
    pub fn new() -> Self
    pub fn new_shared() -> Arc<Self>
    #[inline]
    pub fn update_flushed_lsn(&self, lsn: XLogRecPtr)
    #[inline]
    pub fn update_applied_lsn(&self, lsn: XLogRecPtr)
    #[inline]
    pub fn get_feedback_lsn(&self) -> (XLogRecPtr, XLogRecPtr)  // (flushed, applied)
}
```

В этом спайке **ни один из `update_flushed_lsn`/`update_applied_lsn` не вызывался** —
намеренно, это предмет эксперимента Task 3.

Найденный нюанс (важен для Task 3, зафиксирован здесь как сырой факт без выводов):
`next_wal_frame` (общий для `next_event` и `next_raw_event`, src/stream.rs:619 область)
сам, без нашего участия, может отправить `send_feedback()` в двух случаях:
1. каждые 128 обработанных сообщений — троттлинг по `feedback_check_counter`, и только
   если `state.should_send_feedback(feedback_interval)` истинно (наш `feedback_interval`
   в конфиге спайка — 10s);
2. немедленно, если от сервера пришёл keepalive (`'k'`) с флагом `reply_requested = true`
   (`process_keepalive_message`, src/stream.rs:1126-1142) — это отдельно от нашего цикла
   событий и не зависит от того, вызывали ли мы `update_applied_lsn`.

`send_feedback()` (src/stream.rs:1193) шлёт `send_standby_status_update(last_received_lsn,
flushed_lsn, applied_lsn, false)`, где `flushed_lsn`/`applied_lsn` берутся из
`shared_lsn_feedback.get_feedback_lsn()` и остаются `0`, если мы их не обновляли, но
**`last_received_lsn` (write position) всё равно уходит на сервер как ненулевое значение**,
если хотя бы одно сообщение уже было получено. В окне этого спайка (несколько секунд между
стартом и INSERT) ни одно из двух условий не должно сработать — ни 128 сообщений не
набирается, ни keepalive с `reply_requested` не успевает прийти при стандартном
`wal_sender_timeout` — но сам факт существования этого пути важен для точной формулировки
вывода в Task 3 ("транспорт вообще ничего не подтверждает" не совсем то же самое, что
"наш код ничего не подтверждает явно").

### StreamingMode

```rust
// src/stream.rs:111
pub enum StreamingMode {
    Off,
    On,
    Parallel,
}
```

## 2. Контролируемость транспорта

<заполняется в Task 3>

## 3. Вердикт

<заполняется в Task 3>
