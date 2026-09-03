# pgcdc

[![check](https://github.com/tarodo/pgcdc/actions/workflows/check.yml/badge.svg)](https://github.com/tarodo/pgcdc/actions/workflows/check.yml)

A minimal Change Data Capture engine for PostgreSQL, in Rust. It reads logical replication
events over the `pgoutput` protocol and emits normalized JSON Lines.

```json
{"schema":"public","table":"users","operation":"insert","after":{"id":"1","name":"Alice"},"transaction_id":748,"lsn":"0/19742B8","commit_lsn":"0/19743B0","commit_timestamp":"2026-08-31T09:12:26.946113Z"}
```

While testing a small Kafka-less CDC consumer, I observed a failure mode that silently
skipped 39 committed rows while the process still exited with code 0. pgcdc explores how to
prevent that class of failure, which is why so much of it is about exit codes and about the
order of two operations.

**The design rule everything else follows from:** a WAL position is never acknowledged to
PostgreSQL before the sink has confirmed the data is durable. Acknowledging is
irreversible — it permits the server to delete that WAL.

---

## What it does

- Decodes `BEGIN`, `COMMIT`, `RELATION`, `INSERT`, `UPDATE`, `DELETE` from `pgoutput`.
- Emits whole transactions only, on `COMMIT` — a rolled-back transaction never reaches the output.
- Distinguishes a full old row (`before_kind: "full"`) from a key-only one (`before_kind: "key"`).
- Names TOAST values the server did not resend in `unchanged_columns`, instead of reporting them as `null`.
- Writes to stdout or to a file with `fsync`.
- Advances the slot on an idle publication, so unrelated write traffic cannot pin the WAL.
- Reconnects with exponential backoff, and stops cleanly on `SIGTERM` / `SIGINT`.
- Exits non-zero on anything a retry cannot fix.

**Not in scope:** Kafka and other sinks, initial snapshots, DDL replication, fetching TOAST
values the server did not send, multi-database or multi-publication runs.

---

## Requirements

| | |
|---|---|
| PostgreSQL | 14+, started with `wal_level=logical` and at least one free `max_replication_slots` / `max_wal_senders`. Tested against 16 |
| Rust | 1.95+ (stable) |
| Role | `REPLICATION` privilege, plus `SELECT` on the published tables |

---

## Setup

The publication and the slot must exist **before** pgcdc starts:

```sql
CREATE PUBLICATION pgcdc_pub FOR TABLE public.users;
SELECT pg_create_logical_replication_slot('pgcdc_slot', 'pgoutput');
```

pgcdc will not create the slot for you, and refuses to start if it is missing
(`error_kind=slot_missing`, exit code 1). This is deliberate: a slot created at startup
begins at the *current* WAL position, so everything committed before that moment is lost
silently. Refusing is the only way to make that visible.

To capture the old row on `UPDATE` / `DELETE`, set the table's replica identity — otherwise
`before` carries the primary key alone:

```sql
ALTER TABLE public.users REPLICA IDENTITY FULL;
```

---

## Quick start

```bash
docker compose up -d --wait          # Postgres with the demo schema, publication and slot

cargo run -- \
  --database-url postgres://postgres:postgres@localhost:5432/app \
  --publication pgcdc_pub \
  --slot pgcdc_slot \
  --output stdout
```

From another terminal:

```sql
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', NULL);
UPDATE users SET name = 'Bob' WHERE id = 1;
DELETE FROM users WHERE id = 1;
```

The demo uses a fixed primary key, so run `docker compose down -v` before repeating.

<details>
<summary>Running everything in Docker instead (the <code>demo</code> profile)</summary>

A plain `docker compose up -d` starts only Postgres; pgcdc sits behind a profile:

```bash
docker compose up -d --wait
docker compose --profile demo build
docker compose --profile demo up -d pgcdc
# ... run the SQL above ...
docker compose logs pgcdc
docker compose --profile demo down -v
```

</details>

<details>
<summary>Actual output from a real run</summary>

```json
{"schema":"public","table":"users","operation":"insert","before":null,"before_kind":null,"after":{"id":"1","name":"Alice","email":"alice@example.com","bio":null},"unchanged_columns":[],"transaction_id":748,"lsn":"0/1973EE8","commit_lsn":"0/1973FE0","commit_timestamp":"2026-08-31T09:12:26.946113Z"}
{"schema":"public","table":"users","operation":"update","before":{"id":"1","name":"Alice","email":"alice@example.com","bio":null},"before_kind":"full","after":{"id":"1","name":"Bob","email":"alice@example.com","bio":null},"unchanged_columns":[],"transaction_id":749,"lsn":"0/1974028","commit_lsn":"0/19740B0","commit_timestamp":"2026-08-31T09:12:26.973402Z"}
{"schema":"public","table":"users","operation":"delete","before":{"id":"1","name":"Bob","email":"alice@example.com","bio":null},"before_kind":"full","after":null,"unchanged_columns":[],"transaction_id":750,"lsn":"0/19740E0","commit_lsn":"0/1974140","commit_timestamp":"2026-08-31T09:12:26.997366Z"}
```

</details>

---

## Configuration

Every flag has a matching environment variable. Flags win over the environment.

| Flag | Environment | Default | Meaning |
|---|---|---|---|
| `--database-url` | `PGCDC_DATABASE_URL` | *required* | `postgres://…`; the password is redacted from all output |
| `--publication` | `PGCDC_PUBLICATION` | *required* | existing publication name |
| `--slot` | `PGCDC_SLOT` | *required* | existing slot name, `pgoutput` plugin |
| `--output` | `PGCDC_OUTPUT` | `stdout` | `stdout` or `file` |
| `--output-path` | `PGCDC_OUTPUT_PATH` | — | required when `--output file` |
| `--ack-interval-ms` | `PGCDC_ACK_INTERVAL_MS` | `200` | how often the barrier runs and a position is acknowledged |
| `--max-transaction-events` | `PGCDC_MAX_TRANSACTION_EVENTS` | `100000` | a transaction larger than this is a fatal error, not a silent OOM |
| `--reconnect-initial-ms` | `PGCDC_RECONNECT_INITIAL_MS` | `100` | first backoff delay after a drop |
| `--reconnect-max-ms` | `PGCDC_RECONNECT_MAX_MS` | `30000` | backoff ceiling |
| `--slot-busy-budget-ms` | `PGCDC_SLOT_BUSY_BUDGET_MS` | `30000` | how long a slot may keep answering "busy" before it is fatal |

`--output stdout` cannot promise durability — bytes handed to a pipe may never be written
anywhere. It logs a warning at startup and is meant for development. `--output file`
performs a real `fsync` before any position is acknowledged.

---

## Output

Payload goes to **stdout**, logs go to **stderr**, always. So this is safe:

```bash
pgcdc --output stdout … | jq -r '.table'
```

| Field | Meaning |
|---|---|
| `schema`, `table` | source relation |
| `operation` | `insert` / `update` / `delete` |
| `before` | old row; `null` for `insert` |
| `before_kind` | `full` (whole old row) or `key` (primary key only) — `null` when `before` is `null` |
| `after` | new row; `null` for `delete` |
| `unchanged_columns` | TOAST columns the server did not resend; they are absent from `after`, not `null` in it |
| `transaction_id` | the transaction's xid |
| `lsn` | position of this change |
| `commit_lsn` | position of the commit record that made it visible |
| `commit_timestamp` | commit time, RFC 3339, microseconds, UTC |

Column order follows the table, not the alphabet.

**`lsn` is the event identifier — use it for deduplication, not `commit_lsn`.** It is the
WAL address of the change's own record, assigned by the server, not a counter we keep — so
it is unique within a transaction, stable across a redelivery after a crash, and increases
in the order the changes happened. `commit_lsn` cannot serve that role: every change in the
same transaction carries the same `commit_lsn`, because it names the commit record, not the
individual change. Group by `commit_lsn` to find everything one transaction touched;
identify or deduplicate an individual change by `lsn`.

---

## Exit codes

| Code | Meaning |
|---|---|
| `0` | clean stop on `SIGTERM` / `SIGINT` — see the note below |
| `1` | a failure a retry cannot fix; the reason is in the log's `error_kind` field |
| `2` | invalid command line (from the argument parser) |

**Why zero is always right on shutdown.** The signal is checked at three points: inside an
active session, at the top of the reconnect loop, and inside the backoff pause. Only the
first flushes what the sink accepted and acknowledges it before leaving. The other two exit
without flushing — and zero is still correct there, but for a different reason: whatever did
not pass the barrier was never acknowledged either, so the slot hands it over again on the
next run, and duplicates are permitted.

A process that could lose events never exits `0`. Every fatal reason carries an
`error_kind` you can alert on:

| `error_kind` | What happened | What to do |
|---|---|---|
| `slot_missing` | the slot does not exist | create it, then decide whether the gap matters |
| `slot_unusable` | invalidated (`SQLSTATE 55000`) or a foreign output plugin (`22023`) from the server, **or** caught earlier: the pre-flight check already found `wal_status = 'lost'` before `START_REPLICATION` was even attempted | the WAL is gone — recreate the slot and re-sync |
| `slot_ahead` | the slot is ahead of our durable position | someone else acknowledged WAL that never passed through our sink; investigate before restarting |
| `slot_busy_timed_out` | the slot stayed busy past the budget | find the other consumer holding it |
| `transaction_too_large` | a transaction exceeded `--max-transaction-events` | raise the limit or split the write |
| `decode`, `unknown_relation`, `unsupported_message` | the protocol stream did not match expectations | a bug or a server version mismatch; report it |
| `sink` | the output failed | check disk, permissions, downstream |
| `invalid_database_url`, `invalid_reconnect_bounds` | bad configuration, caught before connecting | fix the flags |
| `ack_beyond_durable` | an internal invariant was violated — acknowledging past what is durable | should be unreachable; report it |

---

## Operating assumptions

The guarantee below holds as long as, between two runs of pgcdc:

- no one else advances the replication slot;
- the slot is not dropped and a new one created under the same name;
- the publication's table membership is unchanged;
- output pgcdc already wrote and fsynced is not edited, truncated, or deleted.

pgcdc cannot verify any of these itself, because it keeps no checkpoint of its own — the
slot's `confirmed_flush_lsn` is the only durable position that outlives the process
([DECISIONS.md](DECISIONS.md), Q4). What it *can* check is bounded by that fact:

**Within a running process, a violation of the first assumption is caught, and is fatal.**
A connection drop that forces a reconnect compares the slot's `confirmed_flush_lsn`
against the durable position this process itself built up (`check_reconnect`,
`src/postgres/guard.rs`). A slot *ahead* of that position — WAL acknowledged by something
other than this process's own sink — exits 1 with `error_kind=slot_ahead`.

**Across a restart, none of the four are caught.** The durable position above is created
fresh on every launch and starts at zero (`SessionState::new` in
`src/postgres/replication.rs`), so the first connection of a run has nothing to compare
the slot against — the reconnect check stays off until this process has made something
durable itself — and is simply skipped. Concretely:

- another consumer advancing the slot while pgcdc was down looks identical to an ordinary
  cold start;
- the pre-flight check reads the slot's health (`wal_status`, `safe_wal_size`,
  `catalog_xmin`, `active`) as well as confirming that a slot with the configured name
  *exists* (`preflight_slot`, `src/postgres/guard.rs`) — but health and identity are
  different questions, and it has no way to tell a recreated slot from the original one;
- pgcdc decodes whatever `pgoutput` sends and keeps no independent record of what the
  publication should contain, so a table quietly dropped from it raises nothing;
- every sink only appends (`FileSink::open`, `src/sink/file.rs`) and never reads back its
  own output, so there is nothing to notice if a file shrank since the last run.

None of this needs malice. An operator who recreates a stuck slot by hand, or truncates a
log file to reclaim disk, reproduces two of these exactly.

---

## Guarantees

**Duplicates after a failure are acceptable; silent loss is not.** After a crash you may
see events again around the boundary — consumers must be idempotent. Under the operating
assumptions above, pgcdc never acknowledges a WAL position before the sink has confirmed
it durable: a failure may produce duplicates, it does not produce gaps in the stream pgcdc
manages.

**A position is acknowledged only after the durability barrier**, never after the write was
merely accepted. There is a window between the two, and acknowledging inside it would mean
telling the server to delete WAL that a crash could still take from us.

**The slot is the only source of truth for position.** Nothing is checkpointed locally, so
there is no second place to drift out of sync. Recovery after `SIGKILL` relies on the slot
alone — pinned by `no_committed_row_is_lost_across_a_hard_restart` in `tests/restart.rs`.

**An idle publication still advances the slot.** Writes to tables outside the publication
would otherwise hold it in place and grow the WAL forever. Only ranges provably free of our
own rows are advanced this way; anything containing data still waits for the barrier.

### Failure handling

| Situation | Treated as | Result |
|---|---|---|
| connection dropped | recoverable | reconnect with backoff |
| slot behind our durable position after a drop | expected | replay, duplicates allowed |
| slot ahead of our durable position | fatal | exit 1 — WAL passed by that our sink never saw |
| server refuses to stream the slot | fatal | exit 1 — the same request gets the same refusal an hour later |
| slot busy | recoverable, on a budget | see below |

### A busy slot

Two very different situations answer with the same `SQLSTATE 55006`:

| | |
|---|---|
| our own previous walsender has not detached yet | normal right after a drop, resolves itself |
| someone else's consumer holds the slot | will never resolve on its own |

The status code cannot tell them apart. The difference is physical — duration. Our own
walsender releases the slot within **45–124 ms** (measured over 30 full reconnect cycles,
median ~76 ms); a foreign consumer holds it indefinitely. So patience is bounded by
`--slot-busy-budget-ms`, defaulting to 30 s — a ~240× margin over the worst measurement.
Past the budget the process exits 1 with `error_kind=slot_busy_timed_out`, naming both the
accumulated wait and the configured budget.

Only an **unbroken chain** of busy answers counts toward the budget. An unrelated failure in
between breaks the chain, because the gap it sits in cannot be attributed to the slot being
busy; a successful start clears the count entirely. A slot that is busy with only rare
unrelated failures between attempts will escalate; one where a failure lands on every other
attempt will not. Rationale and the rejected alternatives: [DECISIONS.md](DECISIONS.md),
Q27 and Q29.

### Two operational cautions

Do not judge progress by `pg_stat_replication.write_lsn` — the transport library reports the
"received over the network" position there, which runs ahead of what reached disk. Use the
slot's `confirmed_flush_lsn`.

For the same reason, do not put this slot in `synchronous_standby_names`: a `write_lsn` that
has leaked ahead would release `synchronous_commit` waiters early.

---

## Observability

Eight counters, all `AtomicU64` (`src/metrics.rs`):

| Counter | Meaning |
|---|---|
| `events_total` | row changes handed to the sink |
| `transactions_total` | transactions committed and handed to the sink |
| `bytes_received_total` | raw `XLogData` bytes received |
| `reconnects_total` | reconnect-loop passes after a drop |
| `errors_total` | recoverable connection errors caught |
| `last_received_lsn` | last received WAL position (monotonic) |
| `last_acknowledged_lsn` | last position acknowledged to Postgres — our own decision, not the slot's state |
| `transaction_buffer_size` | changes buffered in an open transaction — a gauge; must fall to zero on commit and on reconnect |

Two more fields ride in the same `metrics_report` line. They are not counters — they are
observations of state, which is why they get a description here instead of a ninth and tenth
row above:

- `streaming` — whether a replication session is running right now. It is written `true`
  once a session actually starts, and `false` on every one of the ways a session or the
  process itself can stop again — a disconnect, a recoverable error, a clean shutdown, and
  a fatal error alike — because a gauge only success updates reports health nobody has
  actually observed. The line carrying `streaming=false` is not limited to the moment a
  session just ended, either: it also comes out periodically while the process sits with
  no connection at all, retrying against a server that is not answering — see "That first
  pair misses a disconnected process" below.
- `ack_age_s` — seconds since this process last acknowledged a position to Postgres, or
  `None` if it never has. `None` and `Some(0)` are kept deliberately distinct: a process that
  just started and one that has been stuck for an hour without ever acknowledging anything
  must not read the same.

A `metrics_report` line carrying all eight counters plus these two goes out at **INFO** every
ten seconds. The interval is not configurable: it is volume, not behavior.

```text
INFO metrics_report events=3 transactions=3 bytes=395 reconnects=0 errors=0 last_received_lsn=0/1974170 last_acknowledged_lsn=0/19741A8 buffer=0 streaming=true ack_age_s=Some(2)
```

Once per replication session — on a cold start and on every reconnect — the pre-flight
check logs what it read about the slot, at **INFO**, before `START_REPLICATION` is ever
attempted:

```text
INFO slot_preflight_ok slot=pgcdc_slot restart_lsn=Some("0/19B4970") confirmed_flush_lsn=Some("0/19B49A8") wal_status=Some("reserved") safe_wal_size=None catalog_xmin=Some(741) active=false
```

Six fields, each answering a different question — positions, a status, a byte volume, and a
flag, not six of a kind. They are **not** summed into a single "lag" number, on purpose:

- `restart_lsn` — the oldest WAL the server must keep for this slot: the disk risk.
- `catalog_xmin` — the transaction horizon the slot pins: the vacuum and wraparound risk.
- `confirmed_flush_lsn` — the position we have acknowledged.
- `safe_wal_size` — how much more WAL can be written before this slot is at risk. **`None`
  means unlimited retention** under the default `max_slot_wal_keep_size = -1`, not an error
  — expect it on a healthy, unconstrained slot.
- `wal_status` — `reserved` / `extended` / `unreserved` / `lost`; only `lost` is fatal
  (`error_kind=slot_unusable`, see Exit codes above). `unreserved` is not: PostgreSQL
  documents that it can climb back to `reserved` or `extended` on its own.
- `active` — whether a consumer is currently streaming from this slot right now. Logged
  but not judged here: the guard fires on every reconnect too, where `active = true` is
  routine (our own prior session may not have released the slot yet), and telling that
  apart from a foreign consumer holding it forever is not this field's job — it belongs to
  the busy-slot patience budget (Q27/Q29 in [DECISIONS.md](DECISIONS.md)), which tells the
  two apart by duration, not by a single flag.

Why not one number: a single acknowledgement was measured moving `confirmed_flush_lsn` by
15 MB, `restart_lsn` by only 141 KB, and releasing 14 transaction ids — three different
magnitudes answering three different questions. A "lag" figure would collapse all three into
one and hide which risk actually moved. `safe_wal_size` is logged even though nothing here
acts on it, because it is the one field that can move *before* the slot dies: a transition
from `wal_status=reserved` straight to `lost` has been observed with neither `extended` nor
`unreserved` appearing in between, `safe_wal_size` falling as the only advance warning.

Per-event lines (`transaction_accepted`, `group_acknowledged`, `advanced_from_keepalive`)
are at **DEBUG** — an exploratory run reached six figures of events per second (see
"Throughput" below), and a line each would make the log both a bottleneck and noise. Turn
them on with `RUST_LOG=pgcdc=debug`.

**Row contents never appear in the logs.** Counters, positions and transaction ids only.
This holds for every log line at every level.

Worth alerting on: `last_received_lsn` advancing while `last_acknowledged_lsn` stands still;
`transaction_buffer_size` that never returns to zero; a steadily climbing `reconnects_total`.

**That first pair misses a disconnected process.** While the connection is down neither
position moves at all, so a process that lost its connection an hour ago keeps printing the
exact same positions it printed while healthy — the pair looks identical to a healthy, idle
one. The signal for that case is different: `streaming=false` together with a climbing
`ack_age_s`.

A `metrics_report` line comes out even while the process cannot reach the server at all, not
only from inside a running session: the countdown to the next report is also checked once
per poll interval during the paused wait between reconnect attempts, specifically so a line
with `streaming=false` keeps coming out on schedule through an outage, not only right at the
moment a session ends. Measured against a dead port: the process kept printing `reconnecting`
warnings — how many depends on the backoff settings and the machine, tens of them over tens
of seconds is typical — while also printing a `metrics_report` line with `streaming=false`
roughly every ten seconds throughout. Alert on `reconnecting` itself too, not only on what a
`metrics_report` line says: it is the one signal that starts immediately, rather than after
the first ten-second interval elapses.

---

## Throughput

There is no benchmark here, and the reason is itself a measurement. An exploratory run on
a laptop — producer, Postgres and consumer sharing one machine, draining a backlog rather
than keeping pace with a live producer — produced these figures:

| Rows | Sink | Best 10s window | End-to-end | Peak second |
|---|---|---|---|---|
| narrow (~320 B of JSON) | file, fsync | 269k–283k ev/s | 127k–171k ev/s | 313k–491k ev/s |
| wide (~4.3 KB of JSON) | file, fsync | 20k ev/s | 13k ev/s | 33k ev/s |

Two runs of the identical load on the identical machine disagreed by 35% end-to-end and
57% on the peak second. Row width moves the event rate 14× — while the JSON actually
written stays near 86–91 MB/s either way, the same machine doing the same work reported
as two wildly different numbers. (Raw WAL received from the server does move, 18.7 to
82.6 MB/s: wide rows cost far more on the wire than they do at the sink. That is a third
quantity again, and it is not in the table above.) Choosing which of these definitions to
print moves the headline figure by 3.9×.

So a single number would be a decision about presentation dressed up as a fact, and this
project's whole argument is that a green result is not a proof. Throughput was never a
goal ([DECISIONS.md](DECISIONS.md), Q1); if it becomes one, it needs a harness with a
rate-controlled producer, a warm-up separated from steady state, and repeats with a
median and a spread — not one more run of the above.

---

## Documentation

| | |
|---|---|
| [docs/spec.md](docs/spec.md) | the binding specification the code was built against, and where it came from |
| [docs/how-it-works.md](docs/how-it-works.md) | how the code is laid out — written for someone reading Rust for the first time |
| [DECISIONS.md](DECISIONS.md) | every accepted decision and the alternatives rejected, with reasons |
| [docs/pgoutput-notes.md](docs/pgoutput-notes.md) | byte-level breakdown of the protocol |
| [docs/spike-findings.md](docs/spike-findings.md) | what we found in the transport crate, and what must not be used |
| [docs/how-it-was-built.md](docs/how-it-was-built.md) | the process this project was built with, and what it caught |

---

## How this was built

The initial specification was drafted with an LLM from my prompt, reviewed, and frozen as an
immutable baseline. Implementation proceeded through staged development, adversarial review,
and mutation testing. The complete process, and the defects it exposed, are documented
separately: [docs/how-it-was-built.md](docs/how-it-was-built.md).

---

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in this crate by you, as defined in the Apache-2.0 license, shall be dual
licensed as above, without any additional terms or conditions.
