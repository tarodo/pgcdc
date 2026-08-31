# pgcdc

A minimal Change Data Capture engine for PostgreSQL in Rust. Reads logical replication
events directly over the `pgoutput` protocol and prints normalized JSON events.

## What already works

Stage 2 — full decoder: decodes `BEGIN`, `COMMIT`, `RELATION`, `INSERT`, `UPDATE`,
`DELETE` → JSON on stdout, LSN acknowledgement only after a successful sink write. Each
event carries `before` distinguishing a "full row" (`before_kind: "full"`, REPLICA
IDENTITY FULL) from a "key only" (`before_kind: "key"`, the primary key changed under
DEFAULT identity); TOAST values that weren't sent are named in `unchanged_columns` and do
not appear in `after`.

Stage 3 — acknowledgement correctness: a file sink with fsync (`--output file`), grouped
acknowledgement on a timer (`--ack-interval-ms`), slot advancement on an idle publication
via keepalive — without this, writes to tables outside the publication would hold the slot
in place and grow the WAL indefinitely.

Stage 4 — resilience: reconnection with exponential backoff after a connection drop
(`--reconnect-initial-ms`, `--reconnect-max-ms`), a clean stop on SIGTERM/SIGINT with a
zero exit code in every case — the signal is caught at three points in the loop (inside
the active session, at the entry of the outer reconnect loop, and inside the backoff
pause), and only the first of those, before exiting, drives what the sink accepted
through the barrier and acknowledges it; the other two return zero without flushing,
because outside an active session there is nothing to acknowledge — anything not carried
through the barrier was never acknowledged either, the slot will hand it over again,
which invariant 2 allows — and survives a hard restart (SIGKILL and a re-run from the
same slot position).

Stage 5 — wrap-up: structured logs (`tracing`, JSONL on stdout stays separate from logs
on stderr — see "Observability" below), eight process counters and a periodic summary at
INFO once every ten seconds, budget-limited patience for a slot that keeps answering with
a "busy" race (`--slot-busy-budget-ms`) — so a slot forever held by someone else's
consumer doesn't masquerade as an endlessly-resolving race with our own past session (see
"Guarantees" below) — and a Dockerfile with a `demo` profile as a showcase (see "Demo"
below).

## Demo

Two equivalent ways to run the engine against the demo database: from the host via
`cargo run` (convenient during development), or entirely in Docker via the `demo` profile
(shows off the finished image). Both use the same `docker/init.sql`: the slot
`pgcdc_slot` and the publication `pgcdc_pub` are created on the first start of the
`postgres` container, **before** pgcdc ever tries to connect. This isn't a minor detail:
if the slot didn't exist yet, the transport crate would silently create it at the current
WAL position, and every event committed before that moment would never arrive — see
`docs/spike-findings.md`.

### Option A: `cargo run` from the host

```bash
docker compose up -d --wait

cargo run -- \
  --database-url postgres://postgres:postgres@localhost:5432/app \
  --publication pgcdc_pub \
  --slot pgcdc_slot \
  --output stdout
```

In another terminal:

```sql
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', NULL);
```

The demo uses a fixed primary key (`id = 1`), so a repeat run must be preceded by
`docker compose down -v` — otherwise `INSERT` will fail on a duplicate key.

### Option B: the `demo` profile in Docker Compose

A plain `docker compose up -d` brings up only `postgres` — the pgcdc service is hidden
behind the `demo` profile and only starts explicitly:

```bash
docker compose down -v
docker compose up -d --wait
docker compose --profile demo build
docker compose --profile demo up -d pgcdc
```

Then from another terminal:

```bash
export PGPASSWORD=postgres
psql -h 127.0.0.1 -U postgres -d app -c "INSERT INTO users VALUES (1,'Alice','alice@example.com',NULL);"
psql -h 127.0.0.1 -U postgres -d app -c "UPDATE users SET name='Bob' WHERE id=1;"
psql -h 127.0.0.1 -U postgres -d app -c "DELETE FROM users WHERE id=1;"
```

Actual output (`docker compose logs pgcdc`) from a real run — three lines of JSON on
stdout, insert/update/delete, with structured lines on stderr around them:

```json
{"schema":"public","table":"users","operation":"insert","before":null,"before_kind":null,"after":{"id":"1","name":"Alice","email":"alice@example.com","bio":null},"unchanged_columns":[],"transaction_id":748,"lsn":"0/1973EE8","commit_lsn":"0/1973FE0","commit_timestamp":"2026-08-31T09:12:26.946113Z"}
{"schema":"public","table":"users","operation":"update","before":{"id":"1","name":"Alice","email":"alice@example.com","bio":null},"before_kind":"full","after":{"id":"1","name":"Bob","email":"alice@example.com","bio":null},"unchanged_columns":[],"transaction_id":749,"lsn":"0/1974028","commit_lsn":"0/19740B0","commit_timestamp":"2026-08-31T09:12:26.973402Z"}
{"schema":"public","table":"users","operation":"delete","before":{"id":"1","name":"Bob","email":"alice@example.com","bio":null},"before_kind":"full","after":null,"unchanged_columns":[],"transaction_id":750,"lsn":"0/19740E0","commit_lsn":"0/1974140","commit_timestamp":"2026-08-31T09:12:26.997366Z"}
```

The values of `transaction_id`, `lsn`, `commit_lsn`, and `commit_timestamp` will be
different on your own run. Clean up after yourself:

```bash
docker compose --profile demo down -v
```

Logs go to stderr, the payload goes to stdout, so the container's or process's output can
be safely piped downstream without filtering.

## Guarantees

Duplicates after a failure are acceptable; silent loss is not. A WAL position is not
acknowledged to PostgreSQL before the sink has reported a successful write. The one
exception is an idle publication: the slot advances to the server's position from
keepalive, because that range is provably free of any row belonging to our publication;
every range that did contain data still waits for the barrier.

A position is acknowledged only after a successful durability barrier (`Sink::flush`),
not after the write alone has been accepted (`Sink::write_transaction`) — a window exists
between the two, and acknowledging within that window would mean acknowledging something
that could still be lost on a crash.

Don't judge the process's progress by `pg_stat_replication.write_lsn`: the transport
library reports there the "received over the network" position, which runs ahead of what
has actually reached disk — go by the slot's `confirmed_flush_lsn` instead. For the same
reason, don't add this slot to `synchronous_standby_names`: a `write_lsn` that has leaked
ahead would start releasing pending `synchronous_commit` waiters prematurely.

A slot that has moved AHEAD of our durable position on reconnect is a fatal error, not a
reason to silently reconnect: it means someone else acknowledged `confirmed_flush_lsn`
(or we did, in a past run this in-memory position no longer knows about) — WAL that never
passed through our sink. Continuing would mean silently accepting a data gap, so the
process stops with an error instead. A slot BEHIND the durable position is a normal,
expected outcome of a drop (the last feedback may not have made it through) and is not
fatal: the gap gets replayed, and invariant 2 permits the duplicates.

A slot the server explicitly refuses to stream is also a fatal error, not a reason to
reconnect: invalidation from exceeding `max_slot_wal_keep_size` (PostgreSQL `SQLSTATE
55000`) or someone else's output plugin (`SQLSTATE 22023`) mean that the same
`START_REPLICATION` with the same parameters will get the same failure an hour from now
too — retrying it would mean hiding an irreversible loss of WAL access behind the
appearance of a working process. The process exits with **exit code 1**, and the log
carries `error_kind=slot_unusable`. The exception is a race with a walsender from our own
past session not yet released right after a drop (`SQLSTATE 55006`): the server answers
the same way, but this resolves itself on the next attempt and stays recoverable rather
than fatal.

But `SQLSTATE 55006` is also the code the server uses to answer a slot held FOREVER by
SOMEONE ELSE'S (not our own) consumer: the status code by itself doesn't distinguish "our
own past session hasn't detached yet" from "someone else is holding the slot forever".
The difference between them is physical, not in the response code: our own walsender
releases the slot within tens of milliseconds, while another consumer can hold it
indefinitely. This is measured, not eyeballed: 30 cycles of "walsender holds the slot →
drop → timing to the next successful `START_REPLICATION` from scratch, including
establishing a new connection" — the same operation every reconnect performs — gave
**45–124ms, median ~76ms**. That's why patience for a busy slot is limited by a time
budget rather than being unbounded: `--slot-busy-budget-ms` / `PGCDC_SLOT_BUSY_BUDGET_MS`,
default **30000** (30 seconds) — a **~240×** margin over the worst measured value of a
full reconnect cycle. As long as the race's total time stays within the budget, the error
remains recoverable and the process reconnects as usual; once the budget is exhausted,
the process exits with **exit code 1**, and the log carries
`error_kind=slot_busy_timed_out`, with the error string itself naming both the
accumulated wait time and the configured budget. The counter accumulates only genuinely
continuous race time: a failure of a different nature (transport failure, unreachable
server) doesn't take away what's accumulated, but it does break the chain — the whole
interval from the last observation of the race to the next one doesn't count toward the
budget, because we don't know whether the busy condition held throughout it. What
escalates, therefore, is not any forever-busy slot, but one for which there's an
unbroken chain of observations spanning the budget: with rare unrelated failures it adds
up, with a failure on every other attempt it doesn't. The counter is fully closed only by
a successful session start — the one observation that proves the slot is free right now;
so rare, mutually unrelated races that happen over months of a long-lived process's
operation, separated by at least one successful connection, don't sum into a single
fatal exit.

After a hard restart (the process dies without a clean shutdown — e.g. SIGKILL),
duplicates around the crash boundary are possible, but no committed row is lost: the
PostgreSQL slot is the sole source of truth for position, and recovery relies only on it,
not on what the dying process managed to do. This is verified by the test
`no_committed_row_is_lost_across_a_hard_restart` (`tests/restart.rs`).

## Observability

The process keeps eight counters (`src/metrics.rs`, `struct Metrics`, all on
`AtomicU64`, with no external facade like `metrics-rs` — see DECISIONS Q23):

| Counter | What it means |
|---|---|
| `events_total` | how many row changes were handed to the sink (per transaction commit) |
| `transactions_total` | how many transactions were committed and handed to the sink |
| `bytes_received_total` | how many bytes of raw `XLogData` frames were received over the network |
| `reconnects_total` | how many times the reconnect loop ran after a drop |
| `errors_total` | how many recoverable connection errors were caught |
| `last_received_lsn` | the last received WAL LSN (monotonic, never moves backward) |
| `last_acknowledged_lsn` | the last LSN ACKNOWLEDGED to Postgres — written in exactly one place, right after the durability barrier, and it observes our own decision, not the slot's position |
| `transaction_buffer_size` | how many changes have accumulated in an open, not-yet-committed transaction — a gauge, not a position: must drop to zero on commit and on reconnect |

A summary line with all eight values goes out at **INFO** level once every ten seconds —
the `metrics_report` event, its interval set by the `METRICS_REPORT_INTERVAL` constant in
`src/postgres/replication.rs` — not configurable, this is volume, not behavior. Example:

```text
INFO pgcdc::postgres::replication: metrics_report events=3 transactions=3 bytes=395 reconnects=0 errors=0 last_received_lsn=0/1974170 last_acknowledged_lsn=0/19741A8 buffer=0
```

Per-event lines (`transaction_accepted`, `group_acknowledged`,
`advanced_from_keepalive`) go out at **DEBUG** level — on a stream with thousands of
transactions per second, a line per transaction would make the log both a bottleneck and
noise (DECISIONS Q23; a correction to §16 of the base spec). Enable them via
`RUST_LOG=debug` or `RUST_LOG=pgcdc=debug`.

**Row payload — the contents of the `before`/`after` columns — never appears in the logs
anywhere.** Counters and positions may be logged, data contents may not (base spec §16).
This applies to both the summary line and the per-event ones: both carry only numbers,
LSNs, and transaction identifiers.

## Documentation

- [DECISIONS.md](DECISIONS.md) — accepted decisions for the MVP
- [docs/pgoutput-notes.md](docs/pgoutput-notes.md) — byte-level protocol breakdown
- [docs/spike-findings.md](docs/spike-findings.md) — transport findings
