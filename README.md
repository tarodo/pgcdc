# pgcdc

[![check](https://github.com/tarodo/pgcdc/actions/workflows/check.yml/badge.svg)](https://github.com/tarodo/pgcdc/actions/workflows/check.yml)

A minimal Change Data Capture engine for PostgreSQL, in Rust. It reads logical replication
events over the `pgoutput` protocol and emits normalized JSON Lines.

```json
{"schema":"public","table":"users","operation":"insert","after":{"id":"1","name":"Alice"},"transaction_id":737,"event_index":0,"lsn":"0/192FF88","commit_lsn":"0/1930098","commit_timestamp":"2026-09-03T10:48:43.022891Z"}
```

While testing a small Kafka-less CDC consumer, I observed a failure mode that silently
skipped 39 committed rows while the process still exited with code 0. pgcdc explores how to
prevent that class of failure, which is why so much of it is about exit codes and about the
order of two operations.

**The design rule everything else follows from:** a WAL position is acknowledged only after
the configured sink's barrier succeeds. For the file sink, that barrier includes fsync.
Stdout is best-effort and is excluded from the no-loss durability guarantee. Acknowledging is
irreversible — it permits the server to delete that WAL.

---

## What it does

- Decodes `BEGIN`, `COMMIT`, `RELATION`, `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE` from `pgoutput`.
- Emits whole transactions only, on `COMMIT` — a rolled-back transaction never reaches the output.
- Distinguishes a full old row (`before_kind: "full"`) from a key-only one (`before_kind: "key"`).
- Names TOAST values the server did not resend in `unchanged_columns`, instead of reporting them as `null`.
- Writes to stdout or to a file with `fsync`.
- Advances the slot on an idle publication, so unrelated write traffic cannot pin the WAL.
- Reconnects with exponential backoff, and stops cleanly on `SIGTERM` / `SIGINT`.
- Exits non-zero on anything a retry cannot fix.

**Not in scope:** Kafka and other sinks, initial snapshots, schema changes (`CREATE`/`ALTER`/`DROP TABLE`
and other DDL), fetching TOAST values the server did not send, multi-database or multi-publication runs.

---

## Requirements

| | |
|---|---|
| PostgreSQL | 14+, started with `wal_level=logical` and at least one free `max_replication_slots` / `max_wal_senders`. Tested against 16 |
| Rust | 1.95+ (stable) |
| Role | `REPLICATION` privilege, plus `SELECT` on the published tables |

---

## Installation

Requires Rust 1.95+ (stable) — see Requirements above.

Install a tagged release directly:

```bash
cargo install --git https://github.com/tarodo/pgcdc --tag v0.1.1
```

Or build from a checkout:

```bash
cargo build --release
```

A checkout build lands at `target/release/pgcdc`; `cargo install` puts it in
`~/.cargo/bin/pgcdc`. To build the Docker image instead, see the
`demo` profile under Quick start below.

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
{"schema":"public","table":"users","operation":"insert","before":null,"before_kind":null,"after":{"id":"1","name":"Alice","email":"alice@example.com","bio":null},"unchanged_columns":[],"transaction_id":737,"event_index":0,"lsn":"0/192FF88","commit_lsn":"0/1930098","commit_timestamp":"2026-09-03T10:48:43.022891Z"}
{"schema":"public","table":"users","operation":"update","before":{"id":"1","name":"Alice","email":"alice@example.com","bio":null},"before_kind":"full","after":{"id":"1","name":"Bob","email":"alice@example.com","bio":null},"unchanged_columns":[],"transaction_id":738,"event_index":0,"lsn":"0/19300C8","commit_lsn":"0/1930150","commit_timestamp":"2026-09-03T10:48:43.025395Z"}
{"schema":"public","table":"users","operation":"delete","before":{"id":"1","name":"Bob","email":"alice@example.com","bio":null},"before_kind":"full","after":null,"unchanged_columns":[],"transaction_id":739,"event_index":0,"lsn":"0/1930180","commit_lsn":"0/19301E0","commit_timestamp":"2026-09-03T10:48:43.026547Z"}
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
| `--max-transaction-events` | `PGCDC_MAX_TRANSACTION_EVENTS` | `100000` | a transaction larger than this is a fatal error, not a silent OOM; accepted range `1..=4294967295` (`u32::MAX`), so the `event_index` ordinal never wraps |
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
| `operation` | `insert` / `update` / `delete` / `truncate` |
| `before` | old row; `null` for `insert`, and always `null` for `truncate` |
| `before_kind` | `full` (whole old row) or `key` (primary key only) — `null` when `before` is `null`, which includes every `truncate` |
| `after` | new row; `null` for `delete`, and always `null` for `truncate` |
| `unchanged_columns` | TOAST columns the server did not resend; they are absent from `after`, not `null` in it |
| `transaction_id` | the transaction's xid |
| `event_index` | this event's position within its transaction, starting at zero |
| `lsn` | position of this change |
| `commit_lsn` | position of the commit record that made it visible |
| `commit_timestamp` | commit time, RFC 3339, microseconds, UTC |

Column order follows the table, not the alphabet.

**A `truncate` event carries no row identity — that's why both `before` and `after` are
`null`.** `TRUNCATE` says "this table is now empty", not "these specific rows are gone", so
there is nothing to put in either field. A consumer must drop everything it currently holds
for that `schema`/`table` pair rather than try to match individual rows against the event. A
single SQL `TRUNCATE` naming several tables arrives as one `truncate` event per relation, each
with its own `schema`/`table`, so downstream handling never has to special-case a
multi-table statement.

**Upgrading from a build that predates `TRUNCATE` support:** a publication created without an
explicit `publish` list (`CREATE PUBLICATION … FOR TABLE …`, no `WITH (publish = …)`) has
`pubtruncate` on by default, so any `TRUNCATE` on a published table used to reach the decoder
as an unsupported message and exit fatally — the slot's `confirmed_flush_lsn` could never
advance past that record, and every restart landed on the same message again, wedging the
process permanently. `TRUNCATE` now decodes like any other operation; nothing about how you
run or restart pgcdc needs to change.

**The deduplication key is `(lsn, event_index)`, not `lsn` alone.** `lsn` is the WAL address
of the change's own record, assigned by the server, not a counter we keep — stable across a
redelivery after a crash, and increasing in the order the changes happened. But it is not
always unique on its own: a bulk `COPY` load packs several rows into each WAL record it
writes (how many depends on row width — PostgreSQL fills a page, then starts the next
record), and a single `TRUNCATE` naming several tables is one record for all of them — either
way, every event that one record produces carries the same `lsn`, so `event_index` — this
event's position within its transaction — is what tells them apart. `commit_lsn` still
cannot serve as a key: every change in the same transaction carries the same `commit_lsn`,
because it names the commit record, not the individual change. Group by `commit_lsn` to find
everything one transaction touched; identify or deduplicate an individual change by
`(lsn, event_index)`.

`(lsn, event_index)` is unique and stable within **one source** — one publication on one
slot on one PostgreSQL cluster. It says nothing about telling two sources apart. A consumer
merging output from several PostgreSQL clusters must add its own source identifier (a
connection string, a cluster name, whatever it already uses) to the key; pgcdc has no way to
invent one, since it cannot know what distinguishes two clusters for that consumer.

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

Every fatal condition pgcdc can detect exits non-zero. Every fatal reason carries an
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
| `invalid_database_url`, `invalid_reconnect_bounds`, `output_path_required` | bad configuration, caught before connecting | fix the flags |
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

**The no-loss guarantee below covers `--output file`, under the operating assumptions
above.** `--output stdout` is best-effort — its barrier is a successful write and flush, not
proof the bytes reached durable storage — so it is excluded from this guarantee; see the
note under Configuration above. A reader running with `--output stdout` should read this
whole section as describing `--output file`, not the mode they are running.

**Duplicates after a failure are acceptable; silent loss is not.** After a crash you may
see events again around the boundary — consumers must be idempotent. Under the operating
assumptions above, `--output file` never acknowledges a WAL position before its sink has
fsynced it: a failure may produce duplicates, it does not produce gaps in the stream pgcdc
manages.

**A position is acknowledged only after the configured sink's barrier succeeds**, never
after the write was merely accepted. There is a window between the two, and acknowledging
inside it would mean telling the server to delete WAL that a crash could still take from us.
For the file sink, that barrier is `fsync`; for stdout it is only a successful write and
flush, which is why stdout is excluded above.

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

Eight counters (`events_total`, `transactions_total`, `bytes_received_total`,
`reconnects_total`, `errors_total`, `last_received_lsn`, `last_acknowledged_lsn`,
`transaction_buffer_size`), plus two state observations (`streaming`, `ack_age_s`), all in
one `metrics_report` line at **INFO** every ten seconds. Field meanings, the
`slot_preflight_ok` pre-flight line, and alerting advice: [docs/operability.md](docs/operability.md).

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
| [docs/operability.md](docs/operability.md) | metrics, log fields, and alerting advice for someone running pgcdc |
| [CHANGELOG.md](CHANGELOG.md) | what changed in each release, and what an upgrade may break |
| [DECISIONS.md](DECISIONS.md) | every accepted decision and the alternatives rejected, with reasons |
| [docs/decision-notes.md](docs/decision-notes.md) | the reproductions and mechanism walk-throughs behind three of them |
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
