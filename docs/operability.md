# Operability

Reference material for someone already running pgcdc: every counter, the two state
observations that ride alongside them, the pre-flight log line and what each of its fields
means, and the alerting advice built from measured failure behaviour. The
[README](../README.md#observability) has the short version.

---

## Metrics

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

## The pre-flight log line

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
  (`error_kind=slot_unusable`, see [Exit codes](../README.md#exit-codes) in the README).
  `unreserved` is not: PostgreSQL documents that it can climb back to `reserved` or
  `extended` on its own.
- `active` — whether a consumer is currently streaming from this slot right now. Logged
  but not judged here: the guard fires on every reconnect too, where `active = true` is
  routine (our own prior session may not have released the slot yet), and telling that
  apart from a foreign consumer holding it forever is not this field's job — it belongs to
  the busy-slot patience budget (Q27/Q29 in [DECISIONS.md](../DECISIONS.md)), which tells
  the two apart by duration, not by a single flag.

Why not one number: a single acknowledgement was measured moving `confirmed_flush_lsn` by
15 MB, `restart_lsn` by only 141 KB, and releasing 14 transaction ids — three different
magnitudes answering three different questions. A "lag" figure would collapse all three into
one and hide which risk actually moved. `safe_wal_size` is logged even though nothing here
acts on it, because it is the one field that can move *before* the slot dies: a transition
from `wal_status=reserved` straight to `lost` has been observed with neither `extended` nor
`unreserved` appearing in between, `safe_wal_size` falling as the only advance warning.

## Log levels

Per-event lines (`transaction_accepted`, `group_acknowledged`, `advanced_from_keepalive`)
are at **DEBUG** — an exploratory run reached six figures of events per second (see
[Throughput](../README.md#throughput) in the README), and a line each would make the log
both a bottleneck and noise. Turn them on with `RUST_LOG=pgcdc=debug`.

**Row contents never appear in the logs.** Counters, positions and transaction ids only.
This holds for every log line at every level.

## Alerting

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
