# Spike: transport findings

## 1. The actual pg_walstream 0.8 API

Crate version: `0.8.1` (the exact version from `Cargo.lock`, resolved from `pg_walstream = "0.8"`).

Source: `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/pg_walstream-0.8.1/src/stream.rs`,
`src/lsn.rs`, `src/types.rs`, `src/lib.rs`.

Important: the types `LogicalReplicationStream`, `RawXLogData`, `ReplicationStreamConfig`,
`StreamingMode` are re-exported from `lib.rs` under `#[cfg(any(feature = "libpq", feature =
"rustls-tls"))]`. In the crate's `Cargo.toml` `default = ["std", "rustls-tls"]`, so with
`pg_walstream = "0.8"` and no features named (as in our `Cargo.toml` from Task 1) these types are
already available — `rustls-tls` is on by default and satisfies the `cfg` gate. No edit to
`Cargo.toml` was needed.

### The config constructor

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

Matches the skeleton in the brief word for word (argument order and types). The fields `messages`,
`binary`, `two_phase`, `origin`, `slot_options`, `slot_type`, `stop_at_lsn` are not among the
positional arguments of `new` — they get their defaults inside the body of `new`
(`messages: false`, `binary: false`, `two_phase: false`, `origin: None`,
`slot_options: ReplicationSlotOptions { snapshot: Some("nothing".to_string()), ..Default::default() }`,
`slot_type: SlotType::Logical`, `stop_at_lsn: None`) and change only through the `with_*`
builder methods (not used in this spike).

### The stream constructor

```rust
// impl LogicalReplicationStream (src/stream.rs:446)
pub async fn new(connection_string: &str, config: ReplicationStreamConfig) -> Result<Self>
```

### Starting replication

```rust
// impl LogicalReplicationStream (src/stream.rs:619)
pub async fn start(&mut self, start_lsn: Option<XLogRecPtr>) -> Result<()>
```

`ensure_replication_slot()` exists as a separate public method
(`pub async fn ensure_replication_slot(&mut self) -> Result<()>`, src/stream.rs:528), but was
**not called** — per the brief and the Rulings, the slot `pgcdc_slot` already exists
(Task 1), and auto-creation is forbidden.

> **Correction from Task 3.** The wording above is literally true and misleading in substance: we do
> not call `ensure_replication_slot()` ourselves, but `start()` calls it unconditionally through
> `initialize()` (src/stream.rs:491). The requirement "auto-creation is forbidden" is **not** met by
> this code — see §2.4 (probe 4, a measured silent data loss) and §3, workaround 1.

### Getting the raw bytes

```rust
// impl LogicalReplicationStream (src/stream.rs:815)
pub async fn next_raw_event(
    &mut self,
    cancellation_token: &CancellationToken,
) -> Result<RawXLogData>
```

Matches the skeleton in the brief word for word. The comment above the function in the source
explicitly confirms the semantics we need: *"Decode `raw.data` (pgoutput bytes) yourself, then ack:
`stream.shared_lsn_feedback.update_applied_lsn(raw.wal_end.value())`"* and (on line 771)
*"There is no auto-ack and no retry/recovery on this path (that is the point — you own
restart semantics)"*.

### The RawXLogData struct

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

**Deviation from the skeleton in the brief (the only one allowed):** `wal_start`, `wal_end`, `data`
are public FIELDS, not methods. The brief had `raw.data()`, `raw.wal_start()`,
`raw.wal_end()` — replaced with `raw.data`, `raw.wal_start`, `raw.wal_end` in `dump()`.

Helper types (`src/types.rs`):
```rust
pub struct Lsn(pub u64);              // Debug/Display/Ord are implemented
pub type TimestampTz = i64;
pub type XLogRecPtr = u64;
```

### Acknowledging an LSN

`LogicalReplicationStream` holds a public field:

```rust
// src/stream.rs:44
pub shared_lsn_feedback: Arc<SharedLsnFeedback>,
```

The method signatures of `SharedLsnFeedback` (`src/lsn.rs`):

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

In this spike **neither `update_flushed_lsn` nor `update_applied_lsn` was called** —
deliberately, that is the subject of the Task 3 experiment.

A nuance found along the way (it matters for Task 3, recorded here as a raw fact with no conclusions):
`next_wal_frame` (shared by `next_event` and `next_raw_event`, around src/stream.rs:619)
can itself send a `send_feedback()`, without our involvement, in two cases:
1. every 128 processed messages — throttled by `feedback_check_counter`, and only
   if `state.should_send_feedback(feedback_interval)` is true (our `feedback_interval`
   in the spike config is 10s);
2. immediately, if a keepalive (`'k'`) arrived from the server with the flag
   `reply_requested = true` (`process_keepalive_message`, src/stream.rs:1126-1142) — this is separate
   from our event loop and does not depend on whether we called `update_applied_lsn`.

`send_feedback()` (src/stream.rs:1193) sends `send_standby_status_update(last_received_lsn,
flushed_lsn, applied_lsn, false)`, where `flushed_lsn`/`applied_lsn` come from
`shared_lsn_feedback.get_feedback_lsn()` and stay `0` if we never updated them, but
**`last_received_lsn` (the write position) goes to the server as a non-zero value anyway**,
if at least one message has already been received. In the window of this spike (the few seconds between
the start and the INSERT) neither of the two conditions should fire — 128 messages do not accumulate,
and a keepalive with `reply_requested` has no time to arrive under a standard
`wal_sender_timeout` — but the mere existence of this path matters for stating the Task 3
conclusion precisely ("the transport acknowledges nothing at all" is not quite the same as
"our code acknowledges nothing explicitly").

### StreamingMode

```rust
// src/stream.rs:111
pub enum StreamingMode {
    Off,
    On,
    Parallel,
}
```

## 2. How controllable the transport is

All four probes were run against a live PostgreSQL 16 (docker compose, `wal_sender_timeout = 1min`),
slot `pgcdc_slot`, publication `pgcdc_pub`, `proto_version = 1`, `StreamingMode::Off`.

### Summary table of the four probes

| Probe | Expectation | Fact | Conclusion |
|---|---|---|---|
| 1. The slot without an acknowledgement from us | `confirmed_flush_lsn` does not move | `0/192FF10` → `0/192FF10` (restart_lsn `0/192FED8` also unmoved) over 604 messages — the threshold of 128 crossed 4× — and 3 internal standby status updates from keepalives; meanwhile WAL went `0/19745E8` → `0/1980C60` | OK. The crate sends feedback on its own, but puts only our value into flush/replay. The invariant is reachable |
| 2. Acknowledging on our command | `confirmed_flush_lsn` grows | grows bit for bit up to the acknowledged `wal_end`: 2a `update_applied_lsn` → `0/197DD60`; 2b `update_flushed_lsn` → `0/19B0A50`; 2c + an explicit `send_feedback()` → `0/19B1208` | OK. Both methods move it, `update_flushed_lsn` is minimally sufficient. But delivery is not timely (18–22 s) — an explicit `send_feedback()` is needed |
| 3. Visibility of a connection drop | `next_raw_event` returns `Err` | `Err(Transient connection error: connection closed by server)`, the process exited with code 1, no silent reconnect | OK on the raw path. But auto-recovery exists on other paths — they must not be used (§2.3) |
| 4. A missing slot | fail with a non-zero code, do not create the slot (spec §14) | the slot was **re-created** at the current position (`restart_lsn 0/19B4970`), the process stayed alive, the row `id=4000` never arrived | **FAILURE.** Silent data loss. There is nothing to switch it off with — we need our own guard (§3, workaround 1) |

### 2.0 What exactly `send_feedback()` sends — a reading of the source

This is the central question of the task: when the crate sends feedback on its own, WHICH LSN does it
put in there.

```rust
// src/stream.rs:1193
pub async fn send_feedback(&mut self) -> Result<()> {
    if self.state.last_received_lsn == 0 { return Ok(()); }
    let (f, a) = self.shared_lsn_feedback.get_feedback_lsn();
    let flushed_lsn = if f > 0 { f.min(self.state.last_received_lsn) } else { 0 };
    let applied_lsn = if a > 0 { a.min(self.state.last_received_lsn) } else { 0 };
    ...
    self.connection.send_standby_status_update(
        self.state.last_received_lsn,  // write_lsn  <- NOT ours, the position of the last RECEIVED byte
        flushed_lsn,                   // flush_lsn   <- only what we put there
        applied_lsn,                   // replay_lsn  <- only what we put there
        false,
    ).await?;
```

The answer: **option (a) for the fields that decide the fate of the slot, and option (b) for the one
field that decides nothing.**

- `flush_lsn` and `replay_lsn` come **exclusively** from `shared_lsn_feedback`, that is, from
  whatever our code put there. If we never called `update_flushed_lsn`/`update_applied_lsn`,
  a `0` goes out (`InvalidXLogRecPtr`).
- `write_lsn` is `state.last_received_lsn`, the position of the last **received** WAL,
  and it goes to the server always, whether we want it or not. `last_received_lsn` is updated in
  `parse_xlogdata_header` (src/stream.rs:1056), shared by the typed and the raw path, and
  in `process_keepalive_message`.

Why the `write_lsn` leak does not break the invariant: PostgreSQL advances a logical slot by the
**flush** position of the standby status update (`ProcessStandbyReplyMessage` →
`LogicalConfirmReceivedLocation(flushPtr)`), not by write. Verified empirically in probe 1
(write grew, flush was NULL, `confirmed_flush_lsn` stood still) and in probe 2b (we set only
flush, `replay_lsn` stayed NULL, and the slot moved by exactly the flush).

Two places where the crate calls `send_feedback()` itself:

1. `next_wal_frame` → `maybe_send_feedback()` every `FEEDBACK_CHECK_EVENT_INTERVAL = 128`
   iterations of the loop (src/stream.rs:73, 669). But `maybe_send_feedback` additionally
   checks `should_send_feedback(feedback_interval)` (have 10 s passed) **and**
   `lsn_has_changed(flushed, applied)`. If we acknowledged nothing, the values `(0, 0)`
   match the already recorded `last_sent_*`, `lsn_has_changed` returns `false`, and
   nothing is sent at all.
2. `process_keepalive_message` (src/stream.rs:1126) calls `send_feedback()` **directly**,
   bypassing every check, if the server sent a keepalive with `reply_requested = true`. The server
   does this every `wal_sender_timeout / 2` ≈ 30 s. This path fires always,
   independently of our code.

### 2.1 Probe 1 — the slot without an acknowledgement from us

The 128-message threshold was **actually reached**, not "did not fire": 200 separate
`INSERT` statements (each its own transaction) plus the backlog from Task 2 produced **604**
messages in a single run, so the counter crossed 128 four times (128, 256, 384, 512).
On top of that an idle interval of ~90 s was held, during which the server sent three keepalives with
`reply_requested`, and the crate sent three standby status updates on its own initiative.

The fact that they were sent is confirmed from the server side, not from the crate's logs:

```
before the INSERTs:  pg_stat_replication: write=NULL       flush=NULL replay=NULL reply_time=NULL
after 604 messages (4× the threshold of 128):
               pg_stat_replication: write=NULL       flush=NULL replay=NULL reply_time=NULL
after ~30 s idle (keepalive #1):
               pg_stat_replication: write=0/197DD60  flush=NULL replay=NULL reply_time=16:12:38
after ~60 s (keepalive #2):
               pg_stat_replication: write=0/197DD98  flush=NULL replay=NULL reply_time=16:13:09
after ~90 s (keepalive #3):
               pg_stat_replication: write=0/1980C60  flush=NULL replay=NULL reply_time=16:14:09
```

Note: the "every 128 messages" path sent nothing (write stayed NULL after
604 messages) — the `lsn_has_changed` guard fired. Only the keepalive path sent anything.

```
confirmed_flush_lsn before:  0/192FF10   (restart_lsn 0/192FED8)
confirmed_flush_lsn after:   0/192FF10   (restart_lsn 0/192FED8)   — UNCHANGED
pg_current_wal_lsn:          0/19745E8 → 0/1980C60 (WAL demonstrably moved ahead)
```

**Conclusion:** the crate sends standby status updates on its own schedule, but the fields by which
PostgreSQL advances a logical slot carry exactly what we put there. The invariant
`acked <= durable` is reachable.

### 2.2 Probe 2 — acknowledging on our command

The acknowledgement was inserted into the spike loop on `COMMIT` (first payload byte `b'C'`),
with `raw.wal_end`. The method is selected by the environment variable `ACK_MODE`, to answer the
brief's question "which method exactly moved the slot".

| run | method | acks | confirmed_flush before | confirmed_flush after | delay | pg_stat_replication after |
|---|---|---|---|---|---|---|
| 2a | `update_applied_lsn` | 201 | `0/192FF10` | `0/197DD60` | ~18 s | write=`0/198ABC0` flush=`0/197DD60` replay=`0/197DD60` |
| 2b | `update_flushed_lsn` | 60 | `0/197DD60` | `0/19B0A50` | ~22 s | write=`0/19B0A88` flush=`0/19B0A50` replay=NULL |
| 2c | `update_applied_lsn` + an explicit `send_feedback()` | 10 | `0/19B0A50` | `0/19B1208` | instant | write=flush=replay=`0/19B1208` |

In every run the resulting `confirmed_flush_lsn` matched, bit for bit, the last `wal_end` we
acknowledged: 2a — `Lsn(26729824)` = `0/197DD60`; 2b — `Lsn(26937936)` = `0/19B0A50`;
2c — `Lsn(26939912)` = `0/19B1208`.

**Both methods move the slot.** `update_flushed_lsn` is the minimally sufficient one: in 2b
`replay_lsn` stayed NULL, and the slot moved to exactly the flush position anyway. This is a direct
confirmation of the classic trap from the brief: PostgreSQL frees WAL by **flush**, not
by apply. `update_applied_lsn` works too, because internally it drags flush along via
`flushed_lsn.fetch_max(lsn)` (src/lsn.rs) — "applied data is implicitly flushed".

**A separate finding, important for stage 1: the acknowledgement is NOT delivered on time.**
The measured delay of 18–22 s is not our `feedback_interval = 10s` but the beat of the server's
keepalives. The cause is in how `next_wal_frame` is built: `maybe_send_feedback()` is called only
inside the frame-reading loop, that is, **only when a new WAL frame arrives**. The crate has no
timer. On an idle stream our acknowledgement can sit unsent for up to
`wal_sender_timeout / 2`. A workaround exists and is verified (2c): `send_feedback()` is a
public method (`pub async fn send_feedback(&mut self) -> Result<()>`, src/stream.rs:1193),
and calling it by hand after a durable write delivers the acknowledgement immediately.

### 2.3 Probe 3 — visibility of a connection drop

`docker compose restart postgres` with the spike running:

```
replication started, waiting for events (Ctrl-C to stop)
ack mode: none, force_feedback: false
Error: Transient connection error: connection closed by server
SPIKE EXITED WITH CODE: 1
```

`next_raw_event` returned `Err`, the error propagated up through `?`, and the process exited with code 1.
There is no silent reconnect. This matches the comment in the source above the raw path:
*"There is no auto-ack and no retry/recovery on this path (that is the point — you own
restart semantics)"*.

`RetryConfig` (src/retry.rs:36) **does not need** to be switched off — it is not used on the raw
path at all. For the record, its fields: `max_attempts: u32` (default 5),
`initial_delay` (1s), `max_delay` (60s), `multiplier: f64` (2.0), `max_duration` (300s),
`jitter: bool` (true). There is no `enabled` field; switching it off means `max_attempts: 0`.

**But:** the crate does have auto-recovery, just on other paths. `check_connection_health()`
(src/stream.rs:833) and `next_event_with_retry()` (src/stream.rs:957) call
`recover_connection()` (src/stream.rs:862), which reconnects per `RetryConfig` and
restarts replication itself. The same goes for `into_stream()` / `stream()` /
`for_each_event()`. All of them are forbidden — but for a reason that has to be named precisely.

*What you do NOT need to fear in these methods:* slot re-creation. `recover_connection` resets
`slot_created` only for temporary slots:

```rust
// src/stream.rs:874-877
if self.config.slot_options.temporary {
    self.slot_created = false;
}
self.ensure_replication_slot().await?;
```

and for a persistent slot `ensure_replication_slot` short-circuits on its very first line
(`src/stream.rs:529`):

```rust
pub async fn ensure_replication_slot(&mut self) -> Result<()> {
    if self.slot_created { return Ok(()); }
```

Our slot `pgcdc_slot` is `temporary = f`, and `slot_created` is already set to `true` by the first
`start()`. So on a reconnect the slot is **not** re-created. Probe 4 is about a cold
start, not about recovery.

*What you DO need to fear, and it is worse:* `recover_connection` restarts the stream from the
**received**, not the durable position:

```rust
// src/stream.rs:885-894
let last_lsn = self.state.last_received_lsn;
...
self.connection.start_replication(&self.config.slot_name, last_lsn, &options_ref)?;
```

`last_received_lsn` is updated in `parse_xlogdata_header` at the moment the bytes reach us
over the network — long before we have written them durably and acknowledged them. Were we to use
these methods, a silent in-crate reconnect would restart replication from a position that is
**ahead** of our durable point, and all the WAL between durable and received would be skipped without
a single error. That is data loss on the normal path of operation, not in an edge case at start-up —
and it is strictly more dangerous than the hypothetical slot re-creation, which does not even happen here.

On top of that the original reason remains: a silent reconnect does not let us drop the relation cache
(DECISIONS Q19).

Conclusion: only `next_raw_event()` is allowed; we write the reconnect ourselves and issue
`START_REPLICATION` with `0/0` (DECISIONS Q19) — the server will take the slot's
`confirmed_flush_lsn` itself, rather than the position the crate remembered (`last_received_lsn`).

A caveat about evidence: that the restart goes from `last_received_lsn` was established by reading
the crate's source. How PostgreSQL would actually behave on receiving a `START_REPLICATION`
`start_lsn` greater than the slot's `confirmed_flush_lsn` was **not** checked empirically in this spike —
the prohibition is unconditional anyway, so the scenario was not reproduced. Stage 1 has no reconnect
path at all (`check_reconnect` in `guard.rs` has no caller yet), so there is nothing here on which to
close this experiment. It is worth setting up in the stage where the reconnect first appears —
stage 4 "Resilience" (`DECISIONS.md` §4).

In retrospect (stage 4): the experiment was closed without being set up — `check_reconnect` got a
caller (`replication.rs`), but our real `start_lsn` is always `0/0` (`start(None)`),
so the question "what if `start_lsn` is greater than `confirmed_flush_lsn`" could never arise.

### 2.4 Probe 4 — behaviour with a missing slot — FAILURE

Spec §14 requires failing with a non-zero code and not creating the slot. The crate does exactly
the opposite.

```
slot dropped:                   SELECT pg_drop_replication_slot('pgcdc_slot');  -> 0 rows in pg_replication_slots
WAL generated:                  INSERT ... (4000,'lost',...)   [pg_current_wal_lsn 0/19B12F0 -> 0/19B4938]
spike started:                  "replication started, waiting for events"  — did NOT fail, no return code obtained
slot in pg_replication_slots:   pgcdc_slot | pgoutput | logical | active=t | temporary=f
                                restart_lsn=0/19B4970  confirmed_flush_lsn=0/19B49A8
row id=4000 in the stream:      DID NOT ARRIVE (0 messages in the log)
row id=4001, inserted later:    arrived (4 messages B/R/I/C)
```

The slot was re-created at the **current** WAL position, and the transaction between the drop of the
slot and the start of the process was lost silently. This is exactly the silent-data-loss scenario
that DECISIONS Q19 forbids.

The cause was found in the source: the spike **does not call** `ensure_replication_slot()`, but
`start()` does:

```rust
// src/stream.rs:619
pub async fn start(&mut self, start_lsn: Option<XLogRecPtr>) -> Result<()> {
    self.initialize().await?;      // <--
    ...
}
// src/stream.rs:483
async fn initialize(&mut self) -> Result<()> {
    let _system_id = self.connection.identify_system()?;
    self.ensure_replication_slot().await?;   // <-- unconditional, nothing to switch it off with
    Ok(())
}
```

There is no option to disable it: among the `with_*` builders of `ReplicationStreamConfig` (`with_messages`,
`with_binary`, `with_two_phase`, `with_origin`, `with_streaming_mode`, `with_slot_options`,
`with_slot_type`, `with_protocol_version`, `with_feedback_interval`,
`with_connection_timeout`, `with_health_check_interval`, `with_retry_config`,
`with_stop_at_lsn`) and among the fields of `ReplicationSlotOptions` (`temporary`, `two_phase`,
`reserve_wal`, `snapshot`, `failover`) there is nothing of the form `create_if_missing` / `auto_create` /
`slot_must_exist` — a grep across the whole crate finds no such names. Nor is there a public method that
issues `START_REPLICATION` without `initialize()`.

On top of that: `slot_options.snapshot` defaults to `Some("nothing")`, so the slot
is created without exporting a snapshot — the initial state of the tables is not read, and the WAL loss
is not compensated by anything.

## 3. Verdict

**FIT, WITH RESERVATIONS.**

The project's central invariant — "do not acknowledge an LSN until the output is written durably" —
**is reachable**: the crate does not move `confirmed_flush_lsn` by itself (probe 1, the threshold of 128
really was reached, the keepalive path really did fire), it moves it to exactly the value we
acknowledged (probe 2), and it makes a connection drop visible on the raw path (probe 3).
Probe 4 exposed a real defect — silent slot re-creation with data loss — but it
is cured by ten lines of our own code, not by changing the transport.

### Mandatory workarounds for stage 1

1. **A guard before the start — two modes, not one check.** A cold start (the process starts for the
   first time, or after a crash — there is no trusted durable position in memory yet) and a
   reconnect inside an already running process (the durable position has been accumulated by the tracker
   of four positions, Q4) are different situations with different information available, and the guard MUST
   tell them apart.

   **Cold start: existence only.** On a separate ordinary (non-replication)
   connection we check `SELECT 1 FROM pg_replication_slots WHERE slot_name = $1` — before
   calling `start()`. If the slot is missing, we exit with a non-zero code and a clear error, and we
   do not create the slot. Without this check, `start()` → `initialize()` →
   `ensure_replication_slot()` will create the slot anew and silently lose WAL — exactly what was
   measured in probe 4 (§2.4): the data loss there required `start()` to create the slot
   itself. Existence is all that can be checked on a cold start: there is nothing to compare the slot's
   `confirmed_flush_lsn` against, and there is no persistent checkpoint and never will be (Q4).

   **Reconnect inside the process: a full identity check.** If the drop happened while an already
   running process was working, we have the durable position in memory and the check costs
   nothing: `SELECT restart_lsn, confirmed_flush_lsn FROM pg_replication_slots WHERE
   slot_name = $1` on a separate connection, and a comparison of the slot's `confirmed_flush_lsn` with
   our in-memory durable position. We react **asymmetrically**. The slot being **ahead** of our durable
   point means someone acknowledged WAL that we never carried through to the sink — those are
   missed data, so we fail loudly, with both positions in the error text. The slot being **behind** is not
   an emergency but the expected outcome of a drop: the last `send_feedback()` may not have reached the
   server. Here we log a WARN with both positions and carry on: `START_REPLICATION` with `0/0` (Q19) will make
   the server hand the interval over again, and it will arrive as duplicates, which invariant 2 explicitly
   allows. Failing the process on this would mean failing on every network glitch.
   Automatically "repairing" the slot is not allowed in either case.

   **On every start, in both modes**, we log the slot's `restart_lsn` and `confirmed_flush_lsn`
   at INFO level — a jump is then visible to the operator and to monitoring even where
   the automatic check physically cannot catch it (a cold start).

   The rejected alternative was a persistent tripwire file holding the last durable position,
   checked on a cold start the same way the in-memory position is checked on a reconnect.
   Rejected for two reasons: it brings back the second source of truth that `checkpoint.rs`
   was removed to eliminate (Q4, §5 item 7 of the base spec); and a tripwire that
   fires on a legitimate operator resync (a deliberate slot re-creation while
   restoring from a backup, say) is a file operators will quickly learn to delete
   before starting, and a trained habit of bypassing a safeguard is worse than an honest gap.

   **Residual exposure.** A third party that drops the slot and creates it anew
   between two of our runs cannot be detected by any local means: the existence
   check on a cold start sees an existing slot and passes, and the tripwire that could
   have caught it is deliberately absent. This is not a gap specific to our implementation — Debezium
   in the same situation likewise cannot tell "the same slot" from "a slot with the same name,
   re-created by someone else". What we owe this scenario is visibility (an INFO log of the
   positions on every start, which an operator or monitoring will catch), not a false
   guarantee that the guard covers everything.

   The residual TOCTOU window (the slot dropped between the check and `START_REPLICATION`) is a separate,
   narrower case, and we consider it negligible: it requires the slot to be dropped inside exactly
   that short interval, rather than at some point between two runs.
2. **Forbidden APIs.** Do not use `next_event_with_retry()`, `check_connection_health()`,
   `into_stream()`, `stream()`, `for_each_event()` — all of them lead into `recover_connection()`,
   which reconnects behind our back and restarts the stream from
   `state.last_received_lsn` (src/stream.rs:885-894), that is, from the **received**, not the durable
   position. A silent reconnect would skip all the WAL between our durable point and the received one —
   data loss on the normal path of operation. Slot re-creation is not what to fear here: for a
   persistent slot `ensure_replication_slot` short-circuits (see §2.3). Only
   `next_raw_event()` is allowed; we write the reconnect ourselves: `START_REPLICATION` with `0/0`, so the server
   takes the slot's `confirmed_flush_lsn` rather than the position the crate remembered
   (`last_received_lsn`), and we drop the relation cache (DECISIONS Q19).
3. **Acknowledge explicitly and in time.** After a durable write, call
   `shared_lsn_feedback.update_flushed_lsn(lsn)` (minimally sufficient; `update_applied_lsn`
   will do as well and sets replay along the way) and **immediately after that** the public
   `stream.send_feedback().await`. Do not rely on the crate's internal schedule: a delay of
   18–22 s was measured, and on an idle stream the acknowledgement may not go out at all until
   a keepalive arrives.
4. **Know about the `write_lsn` leak.** In every standby status update the crate sends
   `state.last_received_lsn` in the write field, independently of us. This does not affect
   `confirmed_flush_lsn` (verified), but `pg_stat_replication.write_lsn` will overstate our real
   progress — do not use it to monitor lag, look at `flush_lsn` and
   `confirmed_flush_lsn`. And the slot must not end up in `synchronous_standby_names`, or the
   leaked write_lsn will start releasing `synchronous_commit` waiters.
5. **Q18 (a keepalive advances the slot when the buffer is empty) is not verified by any of the four
   probes.** The raw path gets in the way: the crate swallows keepalive frames internally and does not hand
   them to the calling code as an event, and `next_raw_event` blocks while the stream is idle —
   the `wal_end` from a keepalive does not arrive as an event on its own. It is implementable: `state` is
   a public field of `LogicalReplicationStream`, and `current_lsn()` (`src/stream.rs:1257`)
   returns `state.last_received_lsn`, which `process_keepalive_message` updates from a
   keepalive — but this can only be used by wrapping `next_raw_event` in a timeout
   or a `select!`, otherwise the Q18 advance simply never happens.

### Workaround 6: the test runtime MUST match production

`Connection::prefer_inline_driver()` (connection/native/connection.rs:400) picks the
driver by the flavor of the current tokio runtime: multi-threaded → `Inline`,
single-threaded → `Threaded`.

**Verified** by a later audit, not by the original spike: on a multi-threaded runtime,
`Inline` (copy.rs:73-88) drains the buffer it has already accumulated when a read is cancelled and
returns the ready message if there is one — the buffer lives on the connection
(`worker.read_buf`), not in the future that gets dropped. The crate has its
own test for this, `test_get_copy_data_cancelled_with_buffered_data`. This is exactly the
precondition the spec requires BEFORE wrapping the read in a loop with a
timer: a cancelled read does not lose a frame it has already read but not yet handed over.

**Not established**: the behaviour of `Threaded` (connection.rs:645-650) when the future is
simply dropped from outside (`tokio::time::timeout` without a call to `cancel.cancel()`).
An earlier version of this note claimed that `Threaded` loses frames in that case —
that half was not confirmed by the audit: `pending`/`batch_rx` likewise live
on the connection, not in the future, and `rx.recv()` is an operation tokio
documents as cancel-safe. The `*batch_rx = None` branch in the source relates to an
explicit cancellation through `cancellation_token.cancelled()`, not to an ordinary drop of the
future, so asserting frame loss for `Threaded` in the general case is
wrong here. That is an error by the author of the earlier wording, not a reason to remove the
conclusion below: a note that names a mechanism wrongly is worse than no note at all.

So the tests are aligned with the production runtime not because an asymmetry in
frame loss has been proved, but on a general principle: a test MUST run the same driver as
production, and not some other one. `#[tokio::main]` gives a multi-threaded runtime,
`#[tokio::test]` gives a single-threaded one by default. So all integration tests
MUST carry `flavor = "multi_thread"`, or they silently exercise a driver
that production does not use — regardless of what that other
driver does on cancellation. Introduced in stage 3, the wording corrected later.

### What next

The verdict is not blocking: Task 4 (fixtures) can begin. Items 1–3 above are the entry into
planning stage 1; the alternative transports from DECISIONS Q2 (`pgwire-replication`, a fork of
rust-postgres, our own transport) are not required.
