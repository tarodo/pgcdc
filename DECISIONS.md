# pgcdc — accepted decisions for the MVP

Base spec: [docs/spec.md](docs/spec.md).
This document records decisions on questions the spec left open, and
corrects the places where it contradicts itself.
**Where they diverge, this document takes precedence.**

---

## 1. Invariants

Three rules that are not broken under any circumstances:

1. `acked_lsn <= durable_lsn` — Postgres never receives an acknowledgement
   of a position whose contents the sink has not confirmed as written.
2. **Silent loss is unacceptable, duplicates are acceptable.** Any doubt
   is resolved in favor of redelivery.
3. **Nothing capable of losing events exits with code 0.**

---

## 2. Decisions

Thirty-six decisions, grouped by theme.

| # | Decision | Group |
|---|----------|-------|
| [Q1](#q1--a-portfolio-project-depth-over-feature-count) | A portfolio project: depth over feature count | Scope and dependencies |
| [Q2](#q2--a-thin-transport-layer-with-the-raw-bytes-ours) | A thin transport layer, with the raw bytes ours | Scope and dependencies |
| [Q3](#q3--we-write-the-pgoutput-decoder-ourselves) | We write the `pgoutput` decoder ourselves | Scope and dependencies |
| [Q4](#q4--no-local-checkpoint-file) | No local checkpoint file | State and acknowledgements |
| [Q5](#q5--group-ack) | Group ACK | State and acknowledgements |
| [Q6](#q6--durability-is-a-property-of-the-sink) | Durability is a property of the sink | State and acknowledgements |
| [Q7](#q7--transaction-buffer-limit) | Transaction buffer limit | State and acknowledgements |
| [Q26](#q26--the-keepalive-rule-rests-on-two-conditions-not-one) | The keepalive rule rests on two conditions, not one | State and acknowledgements |
| [Q8](#q8--spike-then-byte-fixtures-then-tdd) | Spike, then byte fixtures, then TDD | Process and infrastructure |
| [Q9](#q9--cargo-on-the-host-postgres-in-docker) | `cargo` on the host, Postgres in Docker | Process and infrastructure |
| [Q10](#q10--a-fresh-postgres-per-integration-test) | A fresh Postgres per integration test | Process and infrastructure |
| [Q11](#q11--a-lib-plus-a-thin-bin) | A `lib` plus a thin `bin` | Process and infrastructure |
| [Q12](#q12--compose-carries-only-postgres) | Compose carries only Postgres | Process and infrastructure |
| [Q13](#q13--proto_version-1-text-format-options-off) | `proto_version '1'`, text format, options off | Protocol |
| [Q14](#q14--replica-identity-default-supported-honestly) | `REPLICA IDENTITY DEFAULT`, supported honestly | Protocol |
| [Q15](#q15--the-toast-marker-u-omits-the-column) | The TOAST marker `'u'` omits the column | Protocol |
| [Q16](#q16--all-values-are-strings) | All values are strings | Protocol |
| [Q17](#q17--the-events-lsn-is-wal_start) | The event's LSN is `wal_start` | Protocol |
| [Q18](#q18--keepalive-advances-the-slot-when-the-buffer-is-empty) | Keepalive advances the slot when the buffer is empty | Protocol |
| [Q19](#q19--reconnect-with-start_replication-at-00) | Reconnect with `START_REPLICATION` at `0/0` | Protocol |
| [Q25](#q25--transport-obligations-the-spike-imposed) | Transport obligations the spike imposed | Protocol |
| [Q27](#q27--a-slot-held-by-someone-else-needs-a-deadline) | A slot held by someone else needs a deadline | Protocol |
| [Q29](#q29--patience-accumulates-rather-than-resetting) | Patience accumulates rather than resetting | Protocol |
| [Q30](#q30--a-refusal-the-server-answered-with-is-fatal) | A refusal the server answered with is fatal | Protocol |
| [Q31](#q31--the-deduplication-key-is-lsn-not-commit_lsn) | The deduplication key is `lsn`, not `commit_lsn` | Protocol |
| [Q32](#q32--pre-flight-checks-the-slots-health-not-just-its-existence) | Pre-flight checks the slot's health, not just its existence | Protocol |
| [Q33](#q33--streaming-and-ack_age_s-describe-state-not-counts) | `streaming` and `ack_age_s` describe state, not counts | Protocol |
| [Q34](#q34--truncate-becomes-one-event-per-named-relation) | `TRUNCATE` becomes one event per named relation | Protocol |
| [Q35](#q35--the-key-is-lsn-event_index-lsn-alone-never-sufficed) | The key is `(lsn, event_index)`; `lsn` alone never sufficed | Protocol |
| [Q36](#q36----max-transaction-events-is-bounded-at-u32max) | `--max-transaction-events` is bounded at `u32::MAX` | Protocol |
| [Q20](#q20--jsonl-one-change-per-line) | JSONL, one change per line | Output contract and glue |
| [Q21](#q21--clap-derive-with-pgcdc_-env-vars-no-config-file) | `clap` derive with `PGCDC_*` env vars, no config file | Output contract and glue |
| [Q28](#q28--the-flag-list-brought-up-to-date) | The flag list, brought up to date | Output contract and glue |
| [Q22](#q22--exit-code-1-plus-a-machine-readable-error_kind) | Exit code 1 plus a machine-readable `error_kind` | Output contract and glue |
| [Q23](#q23--our-own-metrics-on-atomicu64) | Our own `Metrics` on `AtomicU64` | Output contract and glue |
| [Q24](#q24--a-single-crate) | A single crate | Output contract and glue |

### Scope and dependencies

#### Q1 — A portfolio project: depth over feature count

This is a learning project aimed at a portfolio. We optimize for depth of understanding
and test quality, not throughput.

#### Q2 — A thin transport layer, with the raw bytes ours

Thin transport layer: a third-party crate provides the connection, `START_REPLICATION`,
CopyBoth, and **raw bytes** (`pg_walstream::next_raw_event` or equivalent). We do not
write our own wire protocol with SCRAM/TLS.

#### Q3 — We write the `pgoutput` decoder ourselves

We write the `pgoutput` decoder ourselves — that's the core of the project.

Context: `tokio-postgres` 0.7.18 has neither `copy_both_simple` nor `replication` in
`Config`, meaning a replication connection can't be opened through it. There is no
canonical library in the ecosystem; candidates are `pg_walstream`, `pgwire-replication`,
`pg_replicate`, a fork of rust-postgres.

### State and acknowledgements

#### Q4 — No local checkpoint file

**No local checkpoint file.** The Postgres slot (`confirmed_flush_lsn`) is the sole
source of truth. `checkpoint.rs` → `lsn.rs`: an in-memory tracker of four positions
(received / processed / durable / acked).

#### Q5 — Group ACK

**Group ACK**, not fsync on every commit: buffer, fsync on a timer (`--ack-interval-ms`,
default 200) or by volume, acknowledge up to the last fsynced LSN. ACK latency doesn't
affect correctness — duplicates are allowed.

#### Q6 — Durability is a property of the sink

Durability is a property of the sink: `Durability::Fsync` (file) and
`Durability::BestEffort` (stdout). stdout acknowledges after write+flush, and prints a
WARN at startup. Not acknowledging at all isn't an option — the slot would stall and
fill up Postgres's disk.

#### Q7 — Transaction buffer limit

**Transaction buffer limit** (`--max-transaction-events`, default 100k) → fatal error.
Doesn't fix a restart loop on a gigantic transaction, but turns the diagnostic from "OOM
killed" into a clear message.

#### Q26 — The keepalive rule rests on two conditions, not one

**Refinement to Q18 for stage 3: the keepalive rule rests on two conditions, not one.**
(a) Advancing the slot from keepalive when there is no open transaction is only allowed
when the assembler's buffer is empty **and** the processed position has caught up to
durable — buffer emptiness alone is sufficient only as long as write, mark-durable, and
ack happen as one synchronous step; the timer-based group ACK (Q5) breaks that, leaving
a window where the buffer is empty but the sink still holds unacknowledged data.

(b) The keepalive position is, by construction, always ahead of the durable position, so
advancing must first call mark-durable on the tracker, then acknowledge — counting the
durable range as empty, because the sink owed nothing within it. The guard `acked_lsn <=
durable_lsn` (invariant 1) is not relaxed by this, not by one iota. (c) The `Sink` trait
gains an explicit durability barrier separate from writing — `flush`, which returns the
new durable position, and the mark-durable call moves to where `flush` is called,
instead of staying tied to `write_transaction`.

Without this, group ACK would silently make the trait's documented meaning wrong. (d)
Keepalive frames today never reach our code at all: the transport swallows them itself,
and the loop waits for the next event with no timeout. To drive a timer, this wait will
have to be time-bounded, and before writing a loop with a timer, cancel-safety for a
read future aborted mid-frame must be established — a half-read, discarded frame is
silent loss, not a harmless retry.

**Closed in stage 3**: cancel-safety was established by reading the transport crate's
own source rather than by assuming it, and the finding is recorded as "Workaround 6" in
`docs/spike-findings.md` — the driver is selected by the tokio runtime flavour, so a
single-threaded test exercises different code from production, and every integration
test therefore carries `flavor = "multi_thread"`. The bounded read lives in
`stream_once` (`SHUTDOWN_POLL_INTERVAL`, `src/postgres/replication.rs`).

### Process and infrastructure

#### Q8 — Spike, then byte fixtures, then TDD

Order: **spike → byte fixtures → TDD**. A binary protocol can't be written blind. Spike
code is thrown away, fixtures stay.

#### Q9 — `cargo` on the host, Postgres in Docker

`cargo` on the macOS host, Postgres in Docker. Devcontainer was rejected: bind-mounting
`target/` through a VM is slow, and testcontainers from inside a container needs
docker.sock forwarding and the `host.docker.internal` trap.

#### Q10 — A fresh Postgres per integration test

Integration tests — **testcontainers 0.28, a fresh Postgres per test**. Reason: a
replication slot is a global, stateful object; on a shared instance, tests would depend
on run order. The fast cycle is kept by unit tests on fixtures, which don't touch
Docker.

#### Q11 — A `lib` plus a thin `bin`

**`lib` + a thin `bin`.** Logic lives in `lib.rs`; `main.rs` is only the CLI and
`run(config, sink)`. Most tests drive the engine in-process with `FailingSink`; the
restart test launches the real binary via `Command::new(env!("CARGO_BIN_EXE_pgcdc"))`
and sends it `SIGKILL`.

#### Q12 — Compose carries only Postgres

`docker-compose.yml` contains **only Postgres** (`-c wal_level=logical -c
max_replication_slots=10 -c max_wal_senders=10`, plus `init.sql`). pgcdc runs via `cargo
run` from the host. Dockerfile + `--profile demo` are added at the end, as a showcase
for the README. In `init.sql` the slot is created **after** the publication; in tests we
create the slot from the test code, to control the starting position.

### Protocol

#### Q13 — `proto_version '1'`, text format, options off

`proto_version '1'`, text format, `binary` / `messages` / `streaming` / `origin` options
off. v2+ with `streaming` delivers uncommitted transactions — contradicts the "buffer
until COMMIT" model. `binary 'on'` would force writing a decoder per OID. v1 gives
everything needed as-is: `BEGIN` carries the final LSN, commit timestamp, and xid. The
`track_commit_timestamp` GUC isn't needed.

#### Q14 — `REPLICA IDENTITY DEFAULT`, supported honestly

**We support `REPLICA IDENTITY DEFAULT` honestly.** The old-tuple marker (`K` = key
only, `O` = full old row) is threaded into the event as the `before_kind` field.
Otherwise the consumer can't tell "the field was null" from "the field wasn't sent to us
at all". The demo table is set to `FULL`, and this is commented. `NOTHING` is not
handled: Postgres itself rejects UPDATE/DELETE on a published table set that way.

#### Q15 — The TOAST marker `'u'` omits the column

**TOAST marker `'u'`** (the value didn't change and lives out of line) → the column **is
omitted from `after`** and **must be named in the separate field `unchanged_columns:
["bio"]`**. A different option was rejected — *silently* omitting it **without** the
list: a column omitted without a list is indistinguishable from a dropped one under
schema evolution, and the consumer would overwrite the value with null. We also don't
write a placeholder value inside `after` (including `null`): that's mixing metadata into
data, and the same overwrite mistake.

What preserves the distinction between "not sent" and "equals null" is the list itself,
not the contents of `after`. We don't fail: this is a normal protocol message. It
appears only for values >~2 KB after compression; the demo table `users` (`bio TEXT`,
`STORAGE EXTERNAL`) is specifically built to trigger it, and the marker is reproduced
and frozen in a fixture (`tests/fixtures/0025_update.bin`).

**Exception: on INSERT the marker is fatal.** The value is written in the same
transaction as the row itself, and the reorder buffer resolves it before the decoder
ever sees `'u'`; the marker appearing here means not normal protocol behavior but that
the assumption above has been violated, and staying silent is not an option
(`src/transaction.rs`, the `Insert` branch).

#### Q16 — All values are strings

**All values are strings**, `null` is a real JSON `null`. `pgoutput` in text mode
already hands back strings; any mapping is our own parsing and our own source of bugs.
`int8` doesn't fit into a JSON number without loss (`2^53` silently breaks JS
consumers). `type_oid` from `RELATION` goes into logs/metadata, but **not** into every
event.

#### Q17 — The event's LSN is `wal_start`

**The event's LSN is `wal_start` from the `XLogData` wrapper** (in v1, row messages
carry no LSN of their own).

**We acknowledge `end_lsn` from `COMMIT`**, not the commit LSN: otherwise the same
transaction would be replayed on restart. Architectural consequence: the transport must
hand the payload to the decoder together with the wrapper's LSN.

#### Q18 — Keepalive advances the slot when the buffer is empty

**Keepalive advances the slot when the buffer is empty.** If there are no open
transactions, we acknowledge up to `wal_end` from keepalive. Without this, with active
writes to tables outside the publication, `confirmed_flush_lsn` stands still, the WAL
grows, and disk runs out — a classic problem, known among other places from Debezium.
With a non-empty buffer we never acknowledge. One unit test for this condition is
mandatory.

#### Q19 — Reconnect with `START_REPLICATION` at `0/0`

**Reconnect: `START_REPLICATION` with `0/0`** (the server will use the slot's
`confirmed_flush_lsn` — it's the sole source of truth anyway).

**The relation cache is fully dropped**: it lives within the session, and Postgres
resends `RELATION` before the first row event for each table; otherwise, if the schema
changed during the drop, we'd decode against a stale description — silent data
corruption. An incompletely assembled transaction is discarded; it will arrive again.
Backoff is exponential, 0.1s → 30s, with no time limit. A missing slot is fatal
immediately, with no retries; this is only true once the guard from Q25 is implemented —
the transport by itself doesn't fail on a missing slot, it silently recreates it (see
Q25).

#### Q25 — Transport obligations the spike imposed

**Spike-mandated transport obligations, required for stage 1** (full rationale and
measurements — `docs/spike-findings.md` §3). (1) **Pre-flight guard before starting
replication, two modes.** Cold start: the guard is just a slot-existence check (`SELECT
1 FROM pg_replication_slots WHERE slot_name = $1`); if the slot doesn't exist — fatal
immediately, no retries, we don't create the slot. Reconnect within an already-running
process: a full comparison — the slot's `confirmed_flush_lsn` against our in-memory
durable position (the four-position tracker, Q4); on a mismatch we react
**asymmetrically**: the slot **ahead** of our durable position — fatal (someone
acknowledged WAL that we never got through to the sink); the slot **behind** — WARN, not
fatal, because that's the expected outcome of a drop: the last `send_feedback()` may not
have reached the server, and `START_REPLICATION` with `0/0` (Q19) will honestly replay
the gap with duplicates — exactly what invariant 2 allows.

In both cases, both positions go into the message. On every start, regardless of mode,
we log the slot's `restart_lsn` and `confirmed_flush_lsn` at INFO. A persistent tripwire
file as a substitute for this guard was rejected — it brings back a second source of
truth, which Q4 and §5 item 7 of this document specifically avoid. (2) **Five
`pg_walstream` APIs that lead into `recover_connection` are forbidden**:
`next_event_with_retry`, `check_connection_health`, `into_stream`, `stream`,
`for_each_event` — all of them restart the stream from `state.last_received_lsn` (the
received, not the durable position), which on the normal path silently skips WAL between
the durable point and the received one, with no error at all.

Only `next_raw_event` is allowed. (3) **An explicit `stream.send_feedback()` is
mandatory after every durable write.** Without it, the acknowledgement only goes out on
the crate's internal schedule — a delay of 18–22s, up to `wal_sender_timeout / 2` on an
idle stream.

#### Q27 — A slot held by someone else needs a deadline

**Refinement to Q19 for stage 5: reconnect backoff remains unbounded in time, but one
class of recoverable failure now has a separate, separately budgeted limit.** Q19 states
that reconnection attempts after a connection drop continue indefinitely — that's still
true for transport failures and for the "slot still busy with our own past session" race
(`SQLSTATE 55006`) within the budget. But the status code itself doesn't distinguish
that exact race from "slot busy with someone else's consumer forever" (Q25(1) — same
`SQLSTATE`, the only differentiator is physical: duration).

Infinite retry here would mask a forever-unavailable slot as a working process — a
direct violation of invariant 3. So stage 5 added `SlotBusyPatience`: the total elapsed
time of consecutive observations of exactly this race is bounded by a budget
(`--slot-busy-budget-ms` / `PGCDC_SLOT_BUSY_BUDGET_MS`, default 30000ms — see the
measurements and rationale in the `SlotBusyPatience` doc comment,
`src/postgres/replication.rs`); once the budget is exhausted, the failure escalates into
a fatal `PgcdcError::SlotBusyTimedOut`.

The counter's accumulation mechanism is refined by Q29 below; the backoff itself
(`ReconnectBackoff`) still grows and falls with no time ceiling, exactly as decided in
Q19.

#### Q29 — Patience accumulates rather than resetting

**Refinement to Q27, review round 2 after the stage-4 finale: the patience counter
accumulates time by subtracting idle gaps, not by a full reset.** The first
implementation (the Q27 text above) reset the counter entirely on ANY observation of a
different nature, not just on a successful session start. This fixed one hole (race
episodes unrelated in time summed into a single fatal exit) and opened exactly the
opposite one: a slot held FOREVER by someone else's consumer, on a server that also
occasionally drops the connection for an unrelated reason, would never accumulate more
time than the interval between two such failures — escalation would never happen at all,
no matter how long the process ran.

Reproduced by a unit test
(`slot_busy_patience_escalates_despite_a_periodic_unrelated_failure`,
`src/postgres/replication.rs`): default budget (30000ms), the slot busy on every attempt
except for an unrelated failure once every 29 seconds — under a full reset, escalation
never happens even once over a simulated hour. Formally the invariants hold (the process
doesn't exit at all, no data is lost), but exactly the guarantee Q27 exists for was
broken: infinite retry masks a forever-unavailable slot as a working process.

The fix: a failure of a different nature doesn't close the episode, it only INTERRUPTS
the chain of consecutive race observations (`SlotBusyPatience::interrupt`) — accumulated
time is preserved, but the interval between the last race observation and the next one
isn't counted into it in full (we don't know what happened inside that interval up to
the failure itself — it could have been the same busy condition from start to end).

The escalation condition is therefore stricter than "the slot has been busy longer than
the budget": it needs an UNBROKEN chain of race observations spanning the budget. With
rare unrelated failures it adds up, and a forever-busy slot escalates; with a failure on
every other attempt (with backoff capped at 30s and a 30s budget — on every other
attempt or more often) the accumulated time never grows, and escalation never happens.

This is a deliberate boundary, not an oversight: it's more honest not to count an
interval in which we observed a failure of a different nature toward busy time than to
count it — the cost being that a sufficiently frequent unrelated failure masks permanent
busyness the same way infinite retry masked it before Q27. The previous code (full
reset) never escalated under any interleaving at all, so this is a strict improvement,
not a trade-off.

The counter is still fully closed only by a successful session start
(`SlotBusyPatience::reset`, `classify_start_outcome`, the `Ok` branch) — the one
observation that physically proves the slot is free right now, rather than merely that
there was no race response for a while.

#### Q30 — A refusal the server answered with is fatal

**A server that ANSWERS and refuses is not a server that stopped answering — stage 5,
review round after task 4.** The acceptance checklist (§20, item 14) demands a non-zero
exit code when the slot is missing **or unusable**. Missing was covered; unusable was
not, and the behaviour was worse than untested: `START_REPLICATION` on an invalidated
slot (`SQLSTATE 55000`, the server deleted the WAL we still needed) or on a slot
carrying a foreign output plugin (`SQLSTATE 22023`) came back as a server error, was
wrapped as a recoverable `Connection`, and the process retried forever with a 30-second
ceiling.

No non-zero exit ever — the process did not exit at all. That is the failure invariant 3
exists to forbid: an infinite retry against a slot whose WAL is gone hides data loss
behind a process that looks alive, and a supervisor sees nothing to restart.

**Decision:** a refusal the server actually answered with is fatal
(`PgcdcError::SlotUnusable`, exit code 1, `error_kind="slot_unusable"`); only a
transport-level failure stays recoverable. Retrying a refusal never helps — the same
request gets the same answer in an hour. The split is not guessed: it follows the
transport crate's own classification, verified against its source — a server FATAL never
reaches the fatal branch, because the socket closes without a ready-for-query and the
read ends at EOF as a transient condition, so only a plain ERROR on a live connection
lands there (`classify_start_error`, `src/postgres/replication.rs`).

Rejected alternative: matching the error text, which breaks the moment the server runs a
localised `lc_messages`. The one race the server cannot distinguish by code — our own
prior walsender still holding the slot, `SQLSTATE 55006` — is separated by duration
instead, which is Q27 and Q29.

#### Q31 — The deduplication key is `lsn`, not `commit_lsn`

**Correction to the event contract in §3: the deduplication key is `lsn`, not
`commit_lsn`.** The original line called `commit_lsn` "the only practical deduplication
key on the consumer side". It is not a usable key at all: every change inside one
transaction carries the same `commit_lsn`, so a consumer deduplicating on it keeps one
event per transaction and silently drops the rest. Measured on a live server — a
transaction of five changes produced five distinct `lsn` values against one shared
`commit_lsn`.

The right key was already in the output and merely mis-described: `lsn` is the WAL
address of the change record itself, assigned by the server rather than counted by us,
therefore unique within a transaction, ordered, and identical when the slot redelivers
after a failure. A counter of our own (`event_index`) was considered and rejected for
the same reason: it would depend on our parsing, where `lsn` is the server's own ground
truth.

The contract is now pinned by
`changes_in_one_transaction_share_commit_lsn_but_each_lsn_is_distinct_and_increasing`
and `a_replayed_transactions_lsn_values_match_the_first_delivery`. External
corroboration, from a separate lab this project's author ran against Debezium:
deduplicating on a transaction-scoped property lost 50 keys out of 50 where two changes
to one key shared a transaction, and switching the key to the per-event LSN recovered
all 50.

**[Corrected by Q35: the "therefore unique within a transaction" reasoning above was
never true in general — a bulk `COPY` load already produces several `INSERT` events
sharing one `lsn`, independent of `TRUNCATE`. See Q35.]**

#### Q32 — Pre-flight checks the slot's health, not just its existence

**Refinement to Q25(1): the pre-flight guard now checks the slot's health, not only its
existence.** Q25(1) described the guard as a plain existence check (`SELECT 1 ... WHERE
slot_name = $1`); `preflight_slot` now also reads `wal_status`, `safe_wal_size`,
`catalog_xmin`, and `active` (`src/postgres/guard.rs`), and the caller refuses before
`START_REPLICATION` is ever attempted once `wal_status = 'lost'`
(`slot_health_is_terminal`, `src/postgres/replication.rs`) — the same fatal
`PgcdcError::SlotUnusable` that Q30 defined for a refusal the server itself answers
with, raised one step earlier and without paying for the round-trip.

**Only `lost` is fatal.** `unreserved` means the WAL the slot needs is scheduled for
removal at the next checkpoint but is not gone yet, and PostgreSQL documents that the
slot can climb back to `reserved` or `extended` on its own; refusing on it would abort a
process about to recover on its own — the mirror of the mistake Q30 fixed. `active =
true` is logged but deliberately not judged here: a slot already held by a consumer is
Q27/Q29's case (the busy-slot patience budget, which tells our own prior walsender apart
from a foreign one by duration, not by a single flag), and re-deciding it in the guard
would fork one question into two places.

`invalidation_reason` and `conflicting` are not read: the README claims a floor of
PostgreSQL 14, and `invalidation_reason` arrives only in 17, `conflicting` only in 16 —
reading either would silently break the stated floor. The external measurement that
prompted this: a separate lab run this project's author made against a different CDC
tool logged a slot as found by an existence check while its `wal_status` was already
`lost`, discovering the problem only when Postgres refused the advance — **"Existence is
checked; health is not."** The same run measured `wal_status` going `reserved` straight
to `lost` in one step, in five seconds, with the documented intermediate states
`extended`/`unreserved` never appearing; the only field that moved beforehand was
`safe_wal_size`, falling from 72 MB to 29 MB — a warning window two seconds wide, which
is why it is logged even though nothing in this process acts on it.

#### Q33 — `streaming` and `ack_age_s` describe state, not counts

**The metrics report gains two fields that describe state rather than counting anything:
`streaming` and `ack_age_s`.** External measurement found the failure mode this closes:
monitoring built for a different service only refreshed its gauges on success, so across
a hundred-second connection drop it kept reporting the same last-good values,
indistinguishable from a healthy run — **"A monitoring system that only refreshes gauges
on success reports health it has not observed. Failure has to write to the same gauges
as success."** The version of this entry that shipped alongside the first cut of the
code described the *intent*, not the mechanics that actually landed, and a final review
round caught two ways the two diverged.

The evidence behind this — the two divergences this entry closes, `StreamingGuard`, and
how `ack_age_s` is stored — is in
[docs/decision-notes.md](docs/decision-notes.md#q33--streaming-and-ack_age_s-describe-state-not-counts).

#### Q34 — `TRUNCATE` becomes one event per named relation

**`TRUNCATE` (`'T'`) is decoded and reassembled into one `truncate` event per named
relation, instead of being rejected as an unsupported message.** A publication created
without an explicit `publish` list — including this project's own test setup, `CREATE
PUBLICATION pgcdc_pub FOR TABLE public.users` — has `pubtruncate` on by default, so a
plain `TRUNCATE` on a published table reaches `pgoutput` as message kind `'T'`. Before
this decision, `decode` rejected `'T'` with `PgcdcError::UnsupportedMessage`, and
`is_fatal()` returns `true` for that variant (`src/error.rs`): the process exited 1
before the record's LSN was ever acknowledged.

Reproduced live, three separate runs, before any of this plan was written: each run died
on "unsupported pgoutput message kind 'T'" with exit code 1, and the slot's
`confirmed_flush_lsn` was unchanged across all three restarts — every restart replayed
from the same position and hit the same fatal message again. Anything committed after
the `TRUNCATE`, including a subsequent `INSERT`, was therefore permanently unreachable,
not merely delayed; a build only counts as fixed once an `INSERT` made after the
`TRUNCATE` is observed arriving, which is exactly what
`truncate_does_not_wedge_the_slot` (`tests/integration.rs`) asserts by running the real
binary against a live `TRUNCATE`.

**Why skipping it with a warning was never on the table:** spec §8 says an unsupported
message "should either be explicitly handled or produce a clear warning/error
**depending on whether ignoring them is safe**." Ignoring a `TRUNCATE` is not safe — it
deletes every row in a table, and a consumer that never hears about it keeps rows the
source no longer has, forever. That is exactly the silent divergence invariant 2
forbids, so §8's own clause rules out "log and skip" and leaves only explicit handling.

**One event per relation, not one event for the whole message:** a single `TRUNCATE` can
name several tables in one statement (`TRUNCATE a, b, c`), but every other event this
process emits carries exactly one `schema`/`table` pair, an assumption every existing
consumer of the output already relies on. So `Assembler::handle` loops over
`relation_ids` and pushes one `PendingChange` per relation (`src/transaction.rs`),
pinned by `a_truncate_becomes_one_event_per_relation`.

**Both `before` and `after` are `None`, unlike a `DELETE`:** a `TRUNCATE` carries no row
identity at all — it says "this table is now empty," not "these specific rows are gone"
— so there is nothing to put in either tuple; a consumer must drop everything it holds
for that table instead of matching individual rows against the event
(`Operation::Truncate` doc comment, `src/event.rs`). **The flags byte (`CASCADE`,
`RESTART IDENTITY`) is read and discarded, not exposed:** it has to be consumed to keep
the following `Int32` OID list at the right offset, but its two effects already reach a
row-level consumer another way or not at all — `CASCADE`'s effect is already fully
expressed as the extra relation ids the server includes in the same message, and
`RESTART IDENTITY` only touches sequences, which this process does not model as
row-level events (doc comment on `PgOutputMessage::Truncate`,
`src/postgres/pgoutput.rs`).

**An unknown relation id stays fatal, exactly as for `INSERT`/`UPDATE`/`DELETE`:** the
`TRUNCATE` arm uses the same relation-cache lookup as every row arm, so an OID the
process never saw a `RELATION` message for is the same `PgcdcError::UnknownRelation`
(`src/transaction.rs`) — not a skip, because decoding against an unknown schema is
exactly the kind of guess invariant 2 forbids. Fixture:
`tests/fixtures/0032_truncate.bin`, captured from `TRUNCATE public.users;` via
`pg_logical_slot_peek_binary_changes` and documented byte-by-byte in
`tests/fixtures/MANIFEST.md`.

#### Q35 — The key is `(lsn, event_index)`; `lsn` alone never sufficed

**Correction to Q31, not merely a refinement: "`lsn` is the key" was never true in
general, and `(lsn, event_index)` is the actual key — independent of `TRUNCATE`.** The
text this entry originally shipped with claimed TRUNCATE support "changed the domain" in
which Q31's uniqueness claim held, and that the claim "was true of every message kind
the decoder handled at the time
(`BEGIN`/`COMMIT`/`RELATION`/`INSERT`/`UPDATE`/`DELETE`)".

**Fix, unchanged from the mechanism this entry has described since it first shipped:** a
new field, `event_index` (`src/event.rs`) — an ordinal starting at zero, assigned by
enumerating the transaction's fully assembled change buffer at commit time
(`Assembler::handle`'s commit arm, `src/transaction.rs`).

**Refinement to invariant 3:** read literally, "nothing capable of losing events exits
with code 0" claims knowledge the process does not have — whether an unrecognized
failure actually lost events is not something pgcdc can observe from inside itself. What
the code actually guarantees, and what invariant 3 is checked against, is narrower:
every fatal condition its own classification recognizes (`PgcdcError::is_fatal`,
`src/error.rs`) exits non-zero with an `error_kind`.

The invariant's wording in §1 is left as adopted, unedited; this entry is the record of
the refinement, the same way Q29 and Q32 recorded refinements to earlier decisions
without rewriting them.

The evidence behind this — the live `COPY` reproduction, and why the ordinal is assigned
per transaction — is in
[docs/decision-notes.md](docs/decision-notes.md#q35--the-key-is-lsn-event_index-lsn-alone-never-sufficed).

#### Q36 — `--max-transaction-events` is bounded at `u32::MAX`

**`--max-transaction-events` is bounded at `u32::MAX`, so `event_index as u32`
(`Assembler`'s commit arm, `src/transaction.rs`) is exact by construction, not merely
probable.** `event_index` (Q35) is a `u32`; the buffer it is cast from is indexed by
`usize`, and the comment beside the cast used to argue safety from an unrelated fact —
that a buffer holding past `u32::MAX` (4,294,967,296) `PendingChange`s would exhaust RAM
before the flag, which carried no upper bound at all (`usize`, unbounded `#[arg]`),
could ever be raised that high.

That is the wrong shape of argument for this project: an out-of-memory abort is loud —
the process dies and nothing is acknowledged — while a wrapped `u32` is silent, and a
wrapped `event_index` means two distinct events land on the same `(lsn, event_index)`
deduplication key from Q35, which a consumer would read as one change happening twice
rather than two changes happening once each.

**Fix:** `#[arg]` on `max_transaction_events` (`src/config.rs`) now carries
`value_parser = RangedU64ValueParser::<usize>::new().range(1..=(u32::MAX as u64))`, so a
value past `u32::MAX` is refused by the parser itself, before any code of ours runs —
`ErrorKind::ValueValidation`, exit code 2, the flag named in the message, not a silently
truncated ordinal discovered later by a consumer.

The evidence behind this — the proof chain step by step, and why the field was not
widened to `u64` — is in
[docs/decision-notes.md](docs/decision-notes.md#q36----max-transaction-events-is-bounded-at-u32max).

### Output contract and glue

#### Q20 — JSONL, one change per line

**JSONL, one change = one line.** `Sink::write_transaction(&Transaction)` stays: the
sink receives the whole transaction and serializes it into N lines. The durability
barrier (`Sink::flush`) is **one per group of transactions** between timer ticks, not a
flush/fsync per individual transaction: group ACK (Q5) and the barrier moved out of
`write_transaction` (Q26c) would make one-per-transaction wrong. Write atomicity ≠
format atomicity.

An envelope-per-transaction was rejected: a gigantic transaction would mean a gigantic
line, losing streaming processing.

#### Q21 — `clap` derive with `PGCDC_*` env vars, no config file

CLI on `clap` derive with `env = "PGCDC_*"`, no config file. Flags: `--database-url`,
`--publication`, `--slot`, `--output stdout|file`, `--output-path`,
`--max-transaction-events`, `--ack-interval-ms`.

**The password is protected by the type system**: a newtype over the URL with manual
`Debug`/`Display` that strip the password, plus a test that "`format!("{:?}")` doesn't
contain the password".

#### Q28 — The flag list, brought up to date

**Refinement to Q21: the flag list is stale — stages 4–5 added three more.** The current
list is ten flags: the seven from the original Q21 (`--database-url`, `--publication`,
`--slot`, `--output`, `--output-path`, `--max-transaction-events`, `--ack-interval-ms`)
plus `--reconnect-initial-ms` and `--reconnect-max-ms` (stage 4, exponential reconnect
backoff, Q19) and `--slot-busy-budget-ms` (stage 5, patience budget for a busy slot,
Q27).

The source of truth is `src/config.rs` and `--help`, not this list; it documents the
history of the decision, it doesn't substitute for reading the code.

#### Q22 — Exit code 1 plus a machine-readable `error_kind`

Exit code 1 for any fatal error + a machine-readable `error_kind` field in the ERROR
log. Semantic codes degenerate into noise.

**The recoverable/fatal split lives in the types**: `enum PgcdcError` (`thiserror`) with
`fn is_fatal(&self) -> bool` via an exhaustive `match` with no `_ =>`, so the compiler
forces every new variant to be classified. Code 0 is only for a clean shutdown on
SIGTERM/SIGINT, with the current transaction pushed through first.

#### Q23 — Our own `Metrics` on `AtomicU64`

Metrics — **our own `struct Metrics` on `AtomicU64`**, no `metrics-rs`. A facade sends
values into the void without an exporter, and we need them in tests: "after a sink
failure, `last_acknowledged_lsn` didn't move" is an assertion on a metric. Logs:
`tracing`; per-transaction at `DEBUG`, an aggregated line at `INFO` once every 10s.
Payload is not logged by default.

#### Q24 — A single crate

**A single crate**, `src/lib.rs` + `src/main.rs`. A workspace is justified once there's
a second consumer of the library; there isn't one. We keep the tree from spec §6, plus:
`checkpoint.rs` → `lsn.rs`, `Durability` in `sink/mod.rs`, `tests/fixtures/` with byte
dumps, `tests/common/` with `FailingSink`.

---

## 3. Output JSON contract

One change — one line. The fields `before_kind` and `unchanged_columns`
are **always** present, with values `null` and `[]` where not applicable:
a stable shape matters more than compactness.

```json
{
  "schema": "public",
  "table": "users",
  "operation": "update",
  "before": { "id": "42", "name": "Roman", "email": null, "bio": "<elided: 9600-byte TOAST value>" },
  "before_kind": "full",
  "after": { "id": "42", "name": "Roman", "email": "roman@example.com" },
  "unchanged_columns": ["bio"],
  "transaction_id": 81234,
  "event_index": 0,
  "lsn": "0/16B6C50",
  "commit_lsn": "0/16B6D18",
  "commit_timestamp": "2026-08-30T12:00:00Z"
}
```

Differences from spec §10:

- `before_kind`: `"key"` | `"full"` | `null` — exactly what the server sent
  in the old tuple (a consequence of `REPLICA IDENTITY`).
- `unchanged_columns`: columns that arrived with the TOAST marker `'u'`.
  Their value is absent from `after` and **must not** be treated as null.
  This rule is only about `after`: when `before_kind = "full"`, `before`
  carries the full value of the same column, because under
  `REPLICA IDENTITY FULL` the server sent the old tuple in full, TOAST
  included (confirmed by the bytes of `tests/fixtures/0025_update.bin`,
  worked out in `docs/pgoutput-notes.md` §10/§12 case 4). Omitting it from
  `before` too would be an unsupported change.
- `event_index`: this event's position within its transaction, starting at
  zero. Needed because `lsn` alone is not always unique: a bulk `COPY` load
  packs several rows into each WAL record it writes (how many depends on
  row width and volume — PostgreSQL fills a page before starting the next
  record), and a single `TRUNCATE` naming several tables is one WAL record
  for all of them — either way, every event produced from one record
  carries the same `lsn` (Q35).
- `lsn` + `event_index`: the deduplication key on the consumer side. `lsn` is
  the WAL address of the change record itself, so it is stable across a
  redelivery after a crash and increases in the order changes happened, but
  it is only unique in combination with `event_index`. `commit_lsn` is
  **not** usable for this: every change in a transaction carries the same
  one, so deduplicating by it collapses a multi-row transaction to a single
  event. Deduplicating by `xid` doesn't work either — xids are reused after
  wraparound. Corrected by Q31 (named `lsn` instead of the original
  `commit_lsn`); corrected again by Q35 (added `event_index` — `lsn` alone
  was never unique whenever more than one event comes from the same WAL
  record, which a bulk `COPY` load and a multi-relation `TRUNCATE` both do).
  `(lsn, event_index)` is unique and stable within one source only — see
  Q35 on merging several PostgreSQL clusters.

---

## 4. Stages

A vertical slice, not bottom-up: otherwise the decoder gets written for two
weeks with no confirmation that connecting to the slot even works
correctly.

**Stage 0 — Spike.**
Connection, `START_REPLICATION`, dump raw payloads as hex, manual parsing
against the
[protocol docs](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html).
*Done when:* bytes after an `INSERT` in psql are visible in the terminal
and frozen as fixtures. The code is thrown away.
Artifacts required for planning stage 1 (see Q25): `docs/spike-findings.md`
(the transport verdict and required workarounds), `docs/pgoutput-notes.md`
(a byte-level spec of the pgoutput message format), and `tests/fixtures/`
(31 binary fixtures plus `MANIFEST.md`).

**Stage 1 — End-to-end slice.**
`RELATION` + `BEGIN` + `INSERT` + `COMMIT` → JSON on stdout → ACK on
commit. Compose with Postgres.
*Done when:* the scenario from spec §19 works for INSERT.

**Stage 2 — Full decoder.**
`UPDATE`, `DELETE`, `before_kind`, TOAST `'u'`, replacing entries in the
relation cache.
*Done when:* unit tests on fixtures are green, no Docker needed for them.

**Stage 3 — Acknowledgement correctness.**
Splitting received / processed / durable / acked, a file sink with fsync,
group ACK, keepalive advancement.
*Done when:* the tests "sink fails → acked doesn't move" and "buffer
non-empty → keepalive doesn't move acked" pass.

**Stage 4 — Resilience.**
Reconnect with cache and buffer reset, fatal/recoverable taxonomy, exit
codes, transaction buffer limit.
*Done when:* the restart test and the missing-slot test pass.

**Stage 5 — Wrap-up.**
Logs, metrics, README, Dockerfile for the demo profile.
*Done when:* the checklist in spec §20 is fully closed.

---

## 5. Corrections to the base spec

Places where the spec is wrong or leaves things unsaid:

1. **§7, "use an existing library"** — a shaky assumption: there's no
   canonical library, and `tokio-postgres` doesn't fit at all. Resolved in
   Q2/Q3.
2. **§11 + §12, fsync on every commit** — a ceiling of around a hundred
   transactions per second. ACK latency doesn't affect correctness; fixed
   in Q5.
3. **§12, stdout as a full-fledged sink** — a pipe can't provide
   durability, period. Fixed in Q6.
4. **§16, `INFO transaction_committed` for every transaction** — a
   thousand log lines a second. Fixed in Q23.
5. **§18, the ROLLBACK test** — we keep it, but rename it. It doesn't test
   our code: logical decoding physically never delivers rolled-back
   transactions. It tests our understanding of the protocol, and that's
   valuable too — but it should be called "Postgres doesn't send rollbacks",
   not "we don't emit rollbacks".
6. **§22, TOAST in Phase 2** — the `'u'` marker arrives over the protocol
   even in the MVP, and it can't be ignored. Moved to stage 2 (Q15). What
   stays in Phase 2 is *fetching* TOAST values, which we don't do.
7. **`checkpoint.rs`** — a persistent checkpoint creates a second source of
   truth and a class of "file drifted from the slot" bugs. Removed in Q4.
8. **§22, TRUNCATE in Phase 2** — §8's own decoder section already lists
   `TRUNCATE` as optional for the MVP "if encountered" and forbids silently
   ignoring an unsupported message where ignoring it isn't safe; a
   `TRUNCATE` that reaches a consumer unannounced is exactly the unsafe
   case, since it deletes every row with nothing telling the consumer to
   drop them. Implemented ahead of Phase 2 rather than deferred. See Q34.
