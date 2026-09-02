# pgcdc

[![check](https://github.com/tarodo/pgcdc/actions/workflows/check.yml/badge.svg)](https://github.com/tarodo/pgcdc/actions/workflows/check.yml)

A minimal Change Data Capture engine for PostgreSQL, in Rust. It reads logical replication
events over the `pgoutput` protocol and emits normalized JSON Lines.

```json
{"schema":"public","table":"users","operation":"insert","after":{"id":"1","name":"Alice"},"transaction_id":748,"lsn":"0/19742B8","commit_lsn":"0/19743B0","commit_timestamp":"2026-08-31T09:12:26.946113Z"}
```

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
| `slot_unusable` | the server refuses to stream it: invalidated (`SQLSTATE 55000`) or a foreign output plugin (`22023`) | the WAL is gone — recreate the slot and re-sync |
| `slot_ahead` | the slot is ahead of our durable position | someone else acknowledged WAL that never passed through our sink; investigate before restarting |
| `slot_busy_timed_out` | the slot stayed busy past the budget | find the other consumer holding it |
| `transaction_too_large` | a transaction exceeded `--max-transaction-events` | raise the limit or split the write |
| `decode`, `unknown_relation`, `unsupported_message` | the protocol stream did not match expectations | a bug or a server version mismatch; report it |
| `sink` | the output failed | check disk, permissions, downstream |
| `invalid_database_url`, `invalid_reconnect_bounds` | bad configuration, caught before connecting | fix the flags |
| `ack_beyond_durable` | an internal invariant was violated — acknowledging past what is durable | should be unreachable; report it |

---

## Guarantees

**Duplicates after a failure are acceptable; silent loss is not.** After a crash you may
see events again around the boundary — consumers must be idempotent. You will not miss any.

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

A `metrics_report` line carrying all eight goes out at **INFO** every ten seconds. The
interval is not configurable: it is volume, not behavior.

```text
INFO metrics_report events=3 transactions=3 bytes=395 reconnects=0 errors=0 last_received_lsn=0/1974170 last_acknowledged_lsn=0/19741A8 buffer=0
```

Per-event lines (`transaction_accepted`, `group_acknowledged`, `advanced_from_keepalive`)
are at **DEBUG** — an exploratory run reached six figures of events per second (see
"Throughput" below), and a line each would make the log both a bottleneck and noise. Turn
them on with `RUST_LOG=pgcdc=debug`.

**Row contents never appear in the logs.** Counters, positions and transaction ids only.
This holds for every log line at every level.

Worth alerting on: `last_received_lsn` advancing while `last_acknowledged_lsn` stands still;
`transaction_buffer_size` that never returns to zero; a steadily climbing `reconnects_total`.

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

---

## How this was built

The specification came first and came from a language model: it was generated from a short
prompt, read, and accepted as binding before any code existed. It was never edited to match
what got built — every departure is a numbered amendment in
[DECISIONS.md](DECISIONS.md), which now runs to thirty decisions, each carrying the
alternatives that were rejected and why.

Work went stage by stage, and each stage ended with an adversarial review whose standard was
mutation, not coverage: break the code deliberately, and if the suite stays green, the
coverage was imaginary. That standard earned its keep five times — most sharply at the end,
when deleting an entire feature, swallowing a sink write failure, and sending the wrong LSN
on the wire each left all 168 tests passing.

The most serious defect was found by the spec rather than by the tests. Its acceptance
checklist demands a non-zero exit code when the replication slot is unusable; walking that
checklist line by line showed the process never exited at all, retrying a hopeless request
forever while looking perfectly healthy from outside. See [docs/spec.md](docs/spec.md) for
the checklist, and `Q30` in [DECISIONS.md](DECISIONS.md) for what it cost to fix.

---

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in this crate by you, as defined in the Apache-2.0 license, shall be dual
licensed as above, without any additional terms or conditions.
