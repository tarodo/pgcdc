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

> **Поправка Task 3.** Формулировка выше верна буквально и обманчива по сути: мы не зовём
> `ensure_replication_slot()` сами, но её безусловно зовёт `start()` через `initialize()`
> (src/stream.rs:491). Требование «автосоздание запрещено» этим кодом **не** выполняется —
> см. §2.4 (проба 4, измеренная тихая потеря данных) и §3, обходной путь 1.

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

Все четыре пробы прогнаны на живом PostgreSQL 16 (docker compose, `wal_sender_timeout = 1min`),
слот `pgcdc_slot`, публикация `pgcdc_pub`, `proto_version = 1`, `StreamingMode::Off`.

### Сводная таблица четырёх проб

| Проба | Ожидание | Факт | Вывод |
|---|---|---|---|
| 1. Слот без нашего подтверждения | `confirmed_flush_lsn` не двигается | `0/192FF10` → `0/192FF10` (restart_lsn `0/192FED8` тоже на месте) при 604 сообщениях — порог 128 пересечён 4× — и 3 внутренних standby status update по keepalive; WAL за это время ушёл `0/19745E8` → `0/1980C60` | ОК. Крейт шлёт фидбек сам, но во flush/replay кладёт только наше. Инвариант достижим |
| 2. Подтверждение по нашей команде | `confirmed_flush_lsn` растёт | растёт бит-в-бит до подтверждённого `wal_end`: 2a `update_applied_lsn` → `0/197DD60`; 2b `update_flushed_lsn` → `0/19B0A50`; 2c + явный `send_feedback()` → `0/19B1208` | ОК. Двигают оба метода, `update_flushed_lsn` минимально достаточен. Но доставка не вовремя (18–22 с) — нужен явный `send_feedback()` |
| 3. Видимость разрыва соединения | `next_raw_event` возвращает `Err` | `Err(Transient connection error: connection closed by server)`, процесс завершился с кодом 1, тихого реконнекта нет | ОК на сыром пути. Но автовосстановление есть на других путях — их нельзя использовать (§2.3) |
| 4. Отсутствующий слот | падение с ненулевым кодом, слот не создаётся (спека §14) | слот **пересоздан** на текущей позиции (`restart_lsn 0/19B4970`), процесс жив, строка `id=4000` не пришла никогда | **ПРОВАЛ.** Тихая потеря данных. Отключить нечем — нужен наш guard (§3, обходной путь 1) |

### 2.0 Что именно шлёт `send_feedback()` — разбор исходника

Это главный вопрос задачи: когда крейт отправляет фидбек сам, КАКОЙ LSN он туда кладёт.

```rust
// src/stream.rs:1193
pub async fn send_feedback(&mut self) -> Result<()> {
    if self.state.last_received_lsn == 0 { return Ok(()); }
    let (f, a) = self.shared_lsn_feedback.get_feedback_lsn();
    let flushed_lsn = if f > 0 { f.min(self.state.last_received_lsn) } else { 0 };
    let applied_lsn = if a > 0 { a.min(self.state.last_received_lsn) } else { 0 };
    ...
    self.connection.send_standby_status_update(
        self.state.last_received_lsn,  // write_lsn  <- НЕ наш, позиция последнего ПРИНЯТОГО байта
        flushed_lsn,                   // flush_lsn   <- только то, что положили мы
        applied_lsn,                   // replay_lsn  <- только то, что положили мы
        false,
    ).await?;
```

Ответ: **вариант (a) для тех полей, которые решают судьбу слота, и вариант (b) для одного
поля, которое ничего не решает.**

- `flush_lsn` и `replay_lsn` берутся **исключительно** из `shared_lsn_feedback`, то есть из
  того, что положил туда наш код. Если мы не звали `update_flushed_lsn`/`update_applied_lsn`,
  туда уходит `0` (`InvalidXLogRecPtr`).
- `write_lsn` — это `state.last_received_lsn`, то есть позиция последнего **принятого** WAL,
  и она уходит на сервер всегда, помимо нашей воли. `last_received_lsn` обновляется в
  `parse_xlogdata_header` (src/stream.rs:1056), общем для типизированного и сырого пути, и
  в `process_keepalive_message`.

Почему утечка `write_lsn` не ломает инвариант: PostgreSQL продвигает логический слот по
**flush**-позиции standby status update (`ProcessStandbyReplyMessage` →
`LogicalConfirmReceivedLocation(flushPtr)`), а не по write. Проверено эмпирически в пробе 1
(write рос, flush был NULL, `confirmed_flush_lsn` стоял) и в пробе 2b (выставили только
flush, `replay_lsn` остался NULL, слот при этом сдвинулся ровно на flush).

Два места, где крейт зовёт `send_feedback()` сам:

1. `next_wal_frame` → `maybe_send_feedback()` каждые `FEEDBACK_CHECK_EVENT_INTERVAL = 128`
   итераций цикла (src/stream.rs:73, 669). Но `maybe_send_feedback` дополнительно
   проверяет `should_send_feedback(feedback_interval)` (прошло ли 10 с) **и**
   `lsn_has_changed(flushed, applied)`. Если мы ничего не подтверждали, значения `(0, 0)`
   совпадают с уже записанными `last_sent_*`, `lsn_has_changed` возвращает `false`, и
   отправки не происходит вообще.
2. `process_keepalive_message` (src/stream.rs:1126) вызывает `send_feedback()` **напрямую**,
   в обход всех проверок, если сервер прислал keepalive с `reply_requested = true`. Сервер
   делает это каждые `wal_sender_timeout / 2` ≈ 30 с. Этот путь срабатывает всегда,
   независимо от нашего кода.

### 2.1 Проба 1 — слот без нашего подтверждения

Порог в 128 сообщений был **реально достигнут**, а не «не сработал»: 200 отдельных
`INSERT`-стейтментов (каждый — своя транзакция) плюс backlog от Task 2 дали **604**
сообщения в одном прогоне, то есть счётчик пересёк 128 четыре раза (128, 256, 384, 512).
Дополнительно был выдержан простой ~90 с, за который сервер трижды прислал keepalive с
`reply_requested`, и крейт трижды отправил standby status update по своей инициативе.

Факт отправки подтверждён серверной стороной, а не логами крейта:

```
до INSERT'ов:  pg_stat_replication: write=NULL       flush=NULL replay=NULL reply_time=NULL
после 604 сообщений (4× порог 128):
               pg_stat_replication: write=NULL       flush=NULL replay=NULL reply_time=NULL
через ~30 с простоя (keepalive #1):
               pg_stat_replication: write=0/197DD60  flush=NULL replay=NULL reply_time=16:12:38
через ~60 с (keepalive #2):
               pg_stat_replication: write=0/197DD98  flush=NULL replay=NULL reply_time=16:13:09
через ~90 с (keepalive #3):
               pg_stat_replication: write=0/1980C60  flush=NULL replay=NULL reply_time=16:14:09
```

Обратите внимание: путь «каждые 128 сообщений» ничего не отправил (write остался NULL после
604 сообщений) — сработала защита `lsn_has_changed`. Отправлял только путь keepalive.

```
confirmed_flush_lsn до:     0/192FF10   (restart_lsn 0/192FED8)
confirmed_flush_lsn после:  0/192FF10   (restart_lsn 0/192FED8)   — НЕ ИЗМЕНИЛСЯ
pg_current_wal_lsn:         0/19745E8 → 0/1980C60 (WAL заведомо ушёл вперёд)
```

**Вывод:** крейт отправляет standby status update по своему расписанию, но в полях, по
которым PostgreSQL двигает логический слот, уходит ровно то, что положили мы. Инвариант
`acked <= durable` достижим.

### 2.2 Проба 2 — подтверждение по нашей команде

Подтверждение вставлено в цикл spike'а на `COMMIT` (первый байт payload'а `b'C'`),
`raw.wal_end`. Метод выбирается переменной окружения `ACK_MODE`, чтобы ответить на вопрос
брифа «какой именно метод сдвинул слот».

| прогон | метод | acks | confirmed_flush до | confirmed_flush после | задержка | pg_stat_replication после |
|---|---|---|---|---|---|---|
| 2a | `update_applied_lsn` | 201 | `0/192FF10` | `0/197DD60` | ~18 с | write=`0/198ABC0` flush=`0/197DD60` replay=`0/197DD60` |
| 2b | `update_flushed_lsn` | 60 | `0/197DD60` | `0/19B0A50` | ~22 с | write=`0/19B0A88` flush=`0/19B0A50` replay=NULL |
| 2c | `update_applied_lsn` + явный `send_feedback()` | 10 | `0/19B0A50` | `0/19B1208` | мгновенно | write=flush=replay=`0/19B1208` |

В каждом прогоне итоговый `confirmed_flush_lsn` совпал бит-в-бит с последним подтверждённым
нами `wal_end`: 2a — `Lsn(26729824)` = `0/197DD60`; 2b — `Lsn(26937936)` = `0/19B0A50`;
2c — `Lsn(26939912)` = `0/19B1208`.

**Двигают слот оба метода.** `update_flushed_lsn` — минимально достаточный: в 2b
`replay_lsn` остался NULL, а слот всё равно уехал ровно на flush-позицию. Это прямое
подтверждение классической ловушки из брифа: PostgreSQL освобождает WAL по **flush**, а не
по apply. `update_applied_lsn` тоже работает, потому что внутри он через
`flushed_lsn.fetch_max(lsn)` тянет flush за собой (src/lsn.rs) — «applied данные неявно
flushed».

**Отдельная находка, важная для этапа 1: подтверждение доставляется НЕ вовремя.**
Замеренная задержка 18–22 с — это не наш `feedback_interval = 10s`, а такт keepalive'ов
сервера. Причина в устройстве `next_wal_frame`: `maybe_send_feedback()` вызывается только
внутри цикла чтения кадров, то есть **только когда приходит новый кадр WAL**. Таймера у
крейта нет. На простаивающем потоке наше подтверждение может пролежать неотправленным до
`wal_sender_timeout / 2`. Обходной путь есть и проверен (2c): `send_feedback()` —
публичный метод (`pub async fn send_feedback(&mut self) -> Result<()>`, src/stream.rs:1193),
вызов его вручную после durable-записи доставляет подтверждение немедленно.

### 2.3 Проба 3 — видимость разрыва соединения

`docker compose restart postgres` при работающем spike'е:

```
replication started, waiting for events (Ctrl-C to stop)
ack mode: none, force_feedback: false
Error: Transient connection error: connection closed by server
SPIKE EXITED WITH CODE: 1
```

`next_raw_event` вернул `Err`, ошибка ушла наверх через `?`, процесс завершился с кодом 1.
Тихого переподключения нет. Это соответствует комментарию в исходнике над сырым путём:
*«There is no auto-ack and no retry/recovery on this path (that is the point — you own
restart semantics)»*.

`RetryConfig` (src/retry.rs:36) отключать **не требуется** — на сыром пути он не
используется вовсе. Для протокола его поля: `max_attempts: u32` (default 5),
`initial_delay` (1s), `max_delay` (60s), `multiplier: f64` (2.0), `max_duration` (300s),
`jitter: bool` (true). Поля `enabled` нет; выключение — `max_attempts: 0`.

**Но:** авто-восстановление в крейте есть, просто на других путях. `check_connection_health()`
(src/stream.rs:833) и `next_event_with_retry()` (src/stream.rs:957) зовут
`recover_connection()` (src/stream.rs:862), который переподключается по `RetryConfig` и
перезапускает репликацию сам. То же относится к `into_stream()` / `stream()` /
`for_each_event()`. Все они запрещены — но по причине, которую важно назвать точно.

*Чего в этих методах бояться НЕ надо:* пересоздания слота. `recover_connection` сбрасывает
`slot_created` только для временных слотов:

```rust
// src/stream.rs:874-877
if self.config.slot_options.temporary {
    self.slot_created = false;
}
self.ensure_replication_slot().await?;
```

а `ensure_replication_slot` для persistent-слота коротко замыкается на первой же строке
(`src/stream.rs:529`):

```rust
pub async fn ensure_replication_slot(&mut self) -> Result<()> {
    if self.slot_created { return Ok(()); }
```

Наш слот `pgcdc_slot` — `temporary = f`, и `slot_created` уже выставлен в `true` первым
`start()`. Значит при реконнекте слот **не** пересоздаётся. Проба 4 — это про холодный
старт, а не про восстановление.

*Чего бояться НАДО, и это сильнее:* `recover_connection` перезапускает поток с **принятой**,
а не durable позиции:

```rust
// src/stream.rs:885-894
let last_lsn = self.state.last_received_lsn;
...
self.connection.start_replication(&self.config.slot_name, last_lsn, &options_ref)?;
```

`last_received_lsn` обновляется в `parse_xlogdata_header` в момент, когда байты пришли к нам
по сети, — задолго до того, как мы записали их durable и подтвердили. Если бы мы пользовались
этими методами, тихий внутрикрейтовый реконнект перезапустил бы репликацию с позиции, которая
**впереди** нашей durable-точки, и весь WAL между durable и received был бы пропущен без
единой ошибки. Это потеря данных на нормальном пути работы, а не в краевом сценарии старта —
и она строго опаснее, чем гипотетическое пересоздание слота, которого здесь и нет.

Плюс к этому остаётся исходная причина: тихий реконнект не даёт нам сбросить relation cache
(DECISIONS Q19).

Вывод: разрешён только `next_raw_event()`; реконнект пишем сами и запускаем
`START_REPLICATION` с `0/0` (DECISIONS Q19) — сервер сам возьмёт `confirmed_flush_lsn`
слота, а не позицию, которую запомнил крейт (`last_received_lsn`).

Оговорка о доказательности: то, что рестарт идёт с `last_received_lsn`, установлено чтением
исходника крейта. Как именно повёл бы себя PostgreSQL, получив в `START_REPLICATION`
`start_lsn` больше `confirmed_flush_lsn` слота, эмпирически в этом спайке **не проверялось** —
запрет и так безусловен, поэтому сценарий не воспроизводился. Этап 1 пути реконнекта не
имеет вовсе (`check_reconnect` в `guard.rs` пока без вызывающего кода), так что закрывать
этот эксперимент здесь не на чем. Ставить его стоит в рамках того этапа, где реконнект
впервые появляется, — этап 4 «Устойчивость» (`DECISIONS.md` §4).

### 2.4 Проба 4 — поведение при отсутствующем слоте — ПРОВАЛ

Спека §14 требует падать с ненулевым кодом и не создавать слот. Крейт делает ровно
наоборот.

```
слот удалён:                    SELECT pg_drop_replication_slot('pgcdc_slot');  -> 0 rows in pg_replication_slots
сгенерирован WAL:               INSERT ... (4000,'lost',...)   [pg_current_wal_lsn 0/19B12F0 -> 0/19B4938]
запущен spike:                  "replication started, waiting for events"  — НЕ упал, код возврата не получен
слот в pg_replication_slots:    pgcdc_slot | pgoutput | logical | active=t | temporary=f
                                restart_lsn=0/19B4970  confirmed_flush_lsn=0/19B49A8
строка id=4000 в потоке:        НЕ ПРИШЛА (0 сообщений в логе)
строка id=4001, вставленная позже: пришла (4 сообщения B/R/I/C)
```

Слот пересоздан на **текущей** позиции WAL, транзакция между удалением слота и стартом
процесса потеряна молча. Это ровно тот сценарий тихой потери данных, который запрещает
DECISIONS Q19.

Причина найдена в исходнике: spike **не вызывает** `ensure_replication_slot()`, но её
вызывает `start()`:

```rust
// src/stream.rs:619
pub async fn start(&mut self, start_lsn: Option<XLogRecPtr>) -> Result<()> {
    self.initialize().await?;      // <--
    ...
}
// src/stream.rs:483
async fn initialize(&mut self) -> Result<()> {
    let _system_id = self.connection.identify_system()?;
    self.ensure_replication_slot().await?;   // <-- безусловно, отключить нечем
    Ok(())
}
```

Опции отключения нет: среди `with_*` билдеров `ReplicationStreamConfig` (`with_messages`,
`with_binary`, `with_two_phase`, `with_origin`, `with_streaming_mode`, `with_slot_options`,
`with_slot_type`, `with_protocol_version`, `with_feedback_interval`,
`with_connection_timeout`, `with_health_check_interval`, `with_retry_config`,
`with_stop_at_lsn`) и среди полей `ReplicationSlotOptions` (`temporary`, `two_phase`,
`reserve_wal`, `snapshot`, `failover`) нет ничего вида `create_if_missing` / `auto_create` /
`slot_must_exist` — grep по всему крейту не находит таких имён. Публичного метода, который
выдаёт `START_REPLICATION` без `initialize()`, тоже нет.

Дополнительно: `slot_options.snapshot` по умолчанию `Some("nothing")`, то есть слот
создаётся без экспорта снапшота — начальное состояние таблиц не читается, и потеря WAL не
компенсируется ничем.

## 3. Вердикт

**ГОДЕН С ОГОВОРКАМИ.**

Центральный инвариант проекта — «не подтверждать LSN, пока вывод не записан durable» —
**достижим**: крейт не двигает `confirmed_flush_lsn` сам (проба 1, порог 128 достигнут
реально, keepalive-путь реально сработал), двигает его ровно на то значение, которое мы
подтвердили (проба 2), и делает разрыв соединения видимым на сыром пути (проба 3).
Проба 4 показала реальный дефект — молчаливое пересоздание слота с потерей данных, — но он
лечится десятью строками нашего кода, а не сменой транспорта.

### Обязательные обходные пути для этапа 1

1. **Guard перед стартом — два режима, а не одна проверка.** Холодный старт (процесс
   стартует впервые или после падения — доверенной durable-позиции в памяти ещё нет) и
   реконнект внутри уже работающего процесса (durable-позиция уже накоплена трекером
   четырёх позиций, Q4) — разные ситуации с разной доступной информацией, и guard обязан
   их различать.

   **Холодный старт: только существование.** На отдельном обычном (не replication)
   соединении проверяем `SELECT 1 FROM pg_replication_slots WHERE slot_name = $1` — до
   вызова `start()`. Если слота нет — завершаемся с ненулевым кодом и внятной ошибкой,
   слот не создаём. Без этой проверки `start()` → `initialize()` →
   `ensure_replication_slot()` создаст слот заново и тихо потеряет WAL — ровно то, что
   измерено в пробе 4 (§2.4): там потеря данных потребовала, чтобы `start()` создал слот
   сам. Существование — это всё, что можно проверить на холодном старте: сверять
   `confirmed_flush_lsn` слота не с чем, персистентного чекпоинта нет и не будет (Q4).

   **Реконнект внутри процесса: полная сверка identity.** Если разрыв произошёл во время
   работы уже запущенного процесса, у нас в памяти есть durable-позиция, и сверка ничего
   не стоит: `SELECT restart_lsn, confirmed_flush_lsn FROM pg_replication_slots WHERE
   slot_name = $1` на отдельном соединении, и сравнение `confirmed_flush_lsn` слота с
   нашей in-memory durable-позицией. Реагируем асимметрично. Слот **впереди** нашей durable-точки означает, что кто-то
   подтвердил WAL, который мы не довели до sink, — это пропущенные данные, падаем громко,
   с обеими позициями в тексте ошибки. Слот **позади** — не аварийная ситуация, а
   ожидаемый исход обрыва: последний `send_feedback()` мог не дойти до сервера. Здесь
   пишем WARN с обеими позициями и продолжаем: `START_REPLICATION` с `0/0` (Q19) заставит
   сервер отдать промежуток заново, и он приедет дубликатами, что инвариант 2 прямо
   разрешает. Ронять процесс на этом означало бы падать при каждом сетевом сбое.
   Автоматически «чинить» слот нельзя ни в том, ни в другом случае.

   **На каждом старте, в обоих режимах**, логируем `restart_lsn` и `confirmed_flush_lsn`
   слота на уровне INFO — скачок виден оператору и мониторингу даже там, где
   автоматическая сверка его физически не может поймать (холодный старт).

   Отвергнутая альтернатива — персистентный файл-трипвайр с последней durable-позицией,
   сверяемый на холодном старте так же, как in-memory позиция сверяется на реконнекте.
   Отвергнут по двум причинам: он возвращает второй источник истины, ради устранения
   которого `checkpoint.rs` был убран (Q4, §5 п. 7 базовой спеки); и трипвайр,
   срабатывающий на легитимный ресинк оператора (например, намеренное пересоздание слота
   при восстановлении из бэкапа), — это файл, который операторы быстро научатся удалять
   перед стартом, а обученная привычка обходить защиту хуже честного пробела.

   **Остаточная экспозиция.** Третья сторона, которая удаляет слот и создаёт его заново
   между двумя нашими запусками, не обнаруживается никаким локальным способом: existence
   check на холодном старте видит существующий слот и проходит, а трипвайра, который мог
   бы это поймать, у нас сознательно нет. Это не пробел именно нашей реализации — Debezium
   в этой же ситуации так же не может отличить «тот же слот» от «слот с тем же именем,
   пересозданный кем-то другим». Что мы этому сценарию должны — видимость (INFO-лог
   позиций на каждом старте, который поймает оператор или мониторинг), а не ложная
   гарантия, будто guard закрывает всё.

   Остаточное окно TOCTOU (слот удалён между проверкой и `START_REPLICATION`) — отдельный,
   более узкий случай, и его считаем пренебрежимым: он требует удаления слота именно в
   этом коротком промежутке, а не когда-то между двумя запусками.
2. **Запрещённые API.** Не использовать `next_event_with_retry()`, `check_connection_health()`,
   `into_stream()`, `stream()`, `for_each_event()` — все они ведут в `recover_connection()`,
   который переподключается за нашей спиной и перезапускает поток с
   `state.last_received_lsn` (src/stream.rs:885-894), то есть с **принятой**, а не durable
   позиции. Тихий реконнект пропустил бы весь WAL между нашей durable-точкой и принятой —
   потеря данных на нормальном пути работы. Пересоздания слота здесь бояться не надо: для
   persistent-слота `ensure_replication_slot` коротко замыкается (см. §2.3). Разрешён только
   `next_raw_event()`; реконнект пишем сами: `START_REPLICATION` с `0/0`, чтобы сервер
   взял `confirmed_flush_lsn` слота, а не позицию, которую запомнил крейт
   (`last_received_lsn`), и сбрасываем relation cache (DECISIONS Q19).
3. **Подтверждать явно и своевременно.** После durable-записи вызывать
   `shared_lsn_feedback.update_flushed_lsn(lsn)` (минимально достаточно; `update_applied_lsn`
   тоже годится и заодно выставляет replay) и **сразу после этого** — публичный
   `stream.send_feedback().await`. Не полагаться на внутреннее расписание крейта: замерено
   18–22 с задержки, а на простаивающем потоке подтверждение может не уйти вообще, пока не
   придёт keepalive.
4. **Знать про утечку `write_lsn`.** В каждом standby status update крейт шлёт
   `state.last_received_lsn` в поле write, независимо от нас. На `confirmed_flush_lsn` это не
   влияет (проверено), но `pg_stat_replication.write_lsn` будет завышать наш реальный
   прогресс — не использовать его для мониторинга лага, смотреть на `flush_lsn` и
   `confirmed_flush_lsn`. И слот не должен попадать в `synchronous_standby_names`, иначе
   утёкший write_lsn начнёт освобождать ждущие `synchronous_commit`.
5. **Q18 (keepalive двигает слот при пустом буфере) не проверено ни одной из четырёх
   проб.** Сырой путь мешает: keepalive-кадры крейт поглощает внутри себя и не отдаёт их
   вызывающему коду как событие, а `next_raw_event` блокируется, пока стрим простаивает, —
   `wal_end` из keepalive сам по себе как событие не приходит. Реализуемо: `state` —
   публичное поле `LogicalReplicationStream`, и `current_lsn()` (`src/stream.rs:1257`)
   возвращает `state.last_received_lsn`, который `process_keepalive_message` обновляет из
   keepalive, — но воспользоваться этим можно, только обернув `next_raw_event` в таймаут
   или `select!`, иначе продвижение по Q18 просто не наступит.

### Обходной путь 6: рантайм тестов обязан совпадать с продом

`Connection::prefer_inline_driver()` (connection/native/connection.rs:400) выбирает
драйвер по флейвору текущего рантайма tokio: многопоточный → `Inline`,
однопоточный → `Threaded`.

**Проверено** (аудит задачи 4, review round 1, F2): на многопоточном рантайме
`Inline` (copy.rs:73-88) при отмене чтения сливает уже накопленный буфер и
возвращает готовое сообщение, если оно есть, — буфер живёт на соединении
(`worker.read_buf`), а не во future, которую роняют. У крейта на это есть
собственный тест `test_get_copy_data_cancelled_with_buffered_data`. Это ровно та
предпосылка, которую спека требует ПЕРЕД тем, как оборачивать чтение в цикл с
таймером: отменённое чтение не теряет уже прочитанный, но ещё не отданный кадр.

**Не установлено**: поведение `Threaded` (connection.rs:645-650) при обычном
падении future извне (`tokio::time::timeout` без вызова `cancel.cancel()`).
Прежняя версия этой заметки утверждала, что `Threaded` в этом случае теряет
кадры, — эта половина не подтвердилась аудитом: `pending`/`batch_rx` тоже живут
на соединении, а не во future, а `rx.recv()` — операция, которую tokio
документирует как cancel-safe. Ветка `*batch_rx = None` в исходнике относится к
явной отмене через `cancellation_token.cancelled()`, а не к обычному падению
future, поэтому заявлять потерю кадров для `Threaded` в общем случае здесь
неверно. Это ошибка автора прежней формулировки, а не повод убирать вывод ниже:
заметка, которая называет механизм неверно, хуже отсутствия заметки.

Поэтому тесты выровнены на рантайм прода не потому, что доказана асимметрия по
потере кадров, а по общему принципу: тест обязан гонять тот же драйвер, что и
прод, а не какой-то другой. `#[tokio::main]` даёт многопоточный рантайм,
`#[tokio::test]` — однопоточный по умолчанию. Поэтому все интеграционные тесты
обязаны нести `flavor = "multi_thread"`, иначе они молча проверяют драйвер,
который в проде не используется, — независимо от того, что именно этот другой
драйвер делает при отмене. Введено в этапе 3, формулировка исправлена в задаче 4
(review round 1, F2).

### Что дальше

Вердикт не блокирующий: Task 4 (фикстуры) можно начинать. Пункты 1–3 выше — вход в
планирование этапа 1; альтернативные транспорты из DECISIONS Q2 (`pgwire-replication`, форк
rust-postgres, свой транспорт) не требуются.
