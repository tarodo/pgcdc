# Decision notes

Three entries in [DECISIONS.md](../DECISIONS.md) carry more evidence than the
decision itself needs: live reproductions, measurements, and walk-throughs of the
mechanism that closed them. That material lives here, so the decision log stays a
log. Nothing here is a summary — it is the original text, moved.

## Q33 — `streaming` and `ack_age_s` describe state, not counts

Continues [Q33 in DECISIONS.md](../DECISIONS.md#q33--streaming-and-ack_age_s-describe-state-not-counts).

### Clearing `streaming` on every ending a session has

**First:** `Metrics::set_streaming(false)` is the first line of
`handle_session_outcome`, ahead of its own `match` — not, as this entry once claimed,
placed textually *after* the match inside `run()`'s loop, which would have read as "runs
for every outcome" while actually skipping the two arms (`Stop`, `Abort`) that `return`
before ever reaching a line placed there. All FOUR outcomes a session can end in clear
the flag — an orderly shutdown and a fatal error included, not only the two endings the
process survives to loop again after.

A version that cleared it only on those two survivable endings shipped once and was
caught live: a real SIGTERM during an active stream and a real fatal
`TransactionTooLarge`, both leaving a stale `streaming: true` in the caller's own
`Arc<Metrics>` after `run()` had already returned.

### Why the report needed a second call site

**Second, and the sharper defect:** printing the `metrics_report` line itself had
exactly one call site, entirely inside `stream_once`'s own per-session loop — reached
only between `set_streaming(true)` and the return that leads into clearing it. That made
the field a constant in every line the process could ever produce: a reviewer's live run
stopped Postgres for 32 seconds and got three summaries across that window, all three
reading `streaming=true`, and zero summaries during the outage itself — reproducing, not
closing, the exact failure mode named in the quote above.

The fix gives `maybe_report` (`src/postgres/replication.rs`) a second call site inside
the sliced backoff pause of `run()`'s own outer loop, the one place a report can still
print with no session and no connection at all; both call sites share one `Instant`
countdown so a report due right at the boundary between a session ending and the pause
beginning prints once, not twice. A fifth ending exists that no amount of code inside
`run()` can reach: the caller of this public entry point cancelling the task it runs on
(`handle.abort()`) or losing a `tokio::select!` race around it, neither of which returns
through `handle_session_outcome` or anything else in `run()`'s body.

`StreamingGuard`, a five-line `Drop` impl holding a cloned `Arc<Metrics>`, closes that
one too — an async fn's locals are dropped in place when its generated Future is dropped
mid-poll, the same mechanism that already made the shutdown-path flush cancel-safe, so
the guard's `drop` runs exactly when a caller tears `run()` down from outside, and is a
harmless repeat write on every ordinary exit, where the flag is already `false` by the
time it fires.

### How `ack_age_s` is stored, and when its clock starts

`ack_age_s` is `Option<u64>`, not a bare `u64` defaulting to `0`:
`Metrics::note_acknowledged_now` stores the millisecond offset from process start
floored to `1` rather than `0`, and `snapshot()` maps the sentinel `0` — never
acknowledged — to `None` rather than `Some(0)`, so a process that has never acknowledged
anything cannot be misread as one that just did (`src/metrics.rs`). That call was itself
moved during the same review round: it used to run before `stream.send_feedback()` was
even attempted, so a send that failed still reset the clock over an acknowledgement the
server never received — backwards for a gauge whose entire purpose is looking stale
during an outage.

It now runs only after `send_feedback` returns `Ok` (`acknowledge_durable`,
`src/postgres/replication.rs`). Both fields keep Q23's ordering choice: every counter in
`Metrics` is `Relaxed` because no decision in the code branches on a counter's value,
and `streaming` (`AtomicBool`) and the acknowledgement offset (`AtomicU64`) are loaded
and stored the same way, read only for a log line or a test assertion, never for a
branch. The offset is measured against a `start: Instant` field, the one place `Metrics`
holds a value that is not atomic — fixed once at construction and never mutated again,
so it needs no synchronisation of its own.

## Q35 — The key is `(lsn, event_index)`; `lsn` alone never sufficed

Continues [Q35 in DECISIONS.md](../DECISIONS.md#q35--the-key-is-lsn-event_index-lsn-alone-never-sufficed).

### The `COPY` reproduction: one `lsn`, five events

A later review round found that claim itself false. Reproduced live, no `TRUNCATE`
anywhere in the transaction: five rows loaded by one `COPY users (id, name, email, bio)
FROM STDIN` produced five `insert` events, all stamped `lsn=0/192FF88`, told apart only
by `event_index=0`..`event_index=4` — one unique `lsn` against five unique `(lsn,
event_index)` pairs. PostgreSQL's `heap_multi_insert` (used by `COPY`) packs as many
rows as fit in one table page into each WAL record it writes, then starts a new record
for the next page — so a small or narrow `COPY` can land in one record, but a larger or
wider one does not: measured directly, a 100-row `COPY` against a table shaped like the
demo schema (`bigint` id, three `text` columns) produced two records, 52 rows then 48,
each with its own `lsn`.

Either way, `pgoutput`'s reorder buffer stamps every `INSERT` message one record
produces with that record's `wal_start`, so rows sharing a record share an `lsn` — the
same collision `TRUNCATE users, items;` causes by naming two relations in one `'T'`
message, just reached through `INSERT` instead of `TRUNCATE`. Q31's reasoning ("assigned
by the server rather than counted by us, therefore unique within a transaction") does
not distinguish these two cases; it was never sound in general, and only looked sound
because Q31's own tests, like every other test in this project until now, inserted rows
one statement at a time — and a standalone `INSERT`/`UPDATE`/`DELETE` statement
genuinely does get its own WAL record and its own `lsn` (verified separately by
`changes_in_one_transaction_share_commit_lsn_but_each_lsn_is_distinct_and_increasing`,
`tests/integration.rs`, whose own comment and name are now scoped to that
statement-per-row shape for exactly this reason).

`TRUNCATE` did not create this gap; it made it impossible to keep missing, because
nothing before Q34 exercised a bulk load against the decoder at all. Pinned by
`a_bulk_copy_load_shares_one_lsn_and_is_told_apart_by_event_index`
(`tests/integration.rs`), alongside the `TRUNCATE` case already covered by
`two_truncates_sharing_an_lsn_are_told_apart_by_event_index`.

### Why the ordinal is per-transaction, and what pins it

**Per-transaction, not per-message, on purpose:** the buffer's own length is already the
source of truth for how many events the transaction produced, so a separate counter
tracked across incoming messages would be one more piece of state that could drift from
it; enumerating the assembled buffer instead reproduces identically on redelivery,
because the slot replays the same transaction with the same changes in the same order.

Also pinned by `every_event_in_a_transaction_gets_a_distinct_index`
(`src/transaction.rs`) and, against a live server, by
`event_index_survives_a_replay_byte_for_byte` (`tests/integration.rs`). **The key is
`(lsn, event_index)`, unique and stable within one source** — one publication on one
slot on one PostgreSQL cluster — and it says nothing about telling two different sources
apart. A consumer merging output from several PostgreSQL clusters must add its own
source identifier to the key; pgcdc has no way to invent one, since it cannot know what
distinguishes two clusters for that consumer.

## Q36 — `--max-transaction-events` is bounded at `u32::MAX`

Continues [Q36 in DECISIONS.md](../DECISIONS.md#q36----max-transaction-events-is-bounded-at-u32max).

### The proof chain, step by step

**The lower bound stays 1**, unchanged from before this decision: 0 would refuse every
transaction, buffered or not. **The proof chain, spelled out so a reader can check it
without re-deriving it:** the flag is bounded to `1..=u32::MAX` at the parser; the push
guard in `Assembler::handle` (`if open.changes.len() >= self.max_events`, checked before
every `INSERT`/`UPDATE`/`DELETE`/`TRUNCATE` push) refuses to grow the buffer past
`self.max_events`; therefore the buffer holds at most `max_events` entries; therefore
the largest index `enumerate()` produces at commit is `max_events - 1`; and since
`max_events <= u32::MAX`, that largest index is `<= u32::MAX - 1`, which always fits in
a `u32`.

Every step is a fact the compiler or the parser enforces, not an assumption about how
the flag will be used. **`clap::value_parser!` has no `ValueParserFactory` impl for
`usize`** — only the fixed-width integer types (`u8` through `i64`) get one — so
`value_parser!(usize).range(..)`, the macro form the four existing `u64` flags use
(`ack_interval_ms`, `reconnect_initial_ms`, `reconnect_max_ms`, `slot_busy_budget_ms`),
does not compile for this field.

### Why the field stayed `usize` rather than widening to `u64`

`RangedU64ValueParser::<usize>` is built directly instead, which compiles because
`usize: TryFrom<u64>` holds unconditionally in this codebase's target set; the four
`u64` flags are untouched. **The field's width was deliberately left `usize`, not
widened to `u64`, even though that would have let the macro form apply unchanged:**
`max_transaction_events` itself is not a public contract field — it never appears in
`MetricsSnapshot` (`src/metrics.rs`) or anywhere else serialized; only its *value*
surfaces indirectly, as plain digits inside `PgcdcError::TransactionTooLarge`'s message
text, which would read identically no matter which Rust integer type produced them.

So widening was never needed to protect a wire format — `RangedU64ValueParser::<usize>`
already expresses `1..=u32::MAX` on the field's existing type, with nothing left to gain
from a wider one. Widening would in fact have cost something: `open.changes.len()` (the
push guard's other operand, repeated at all four call sites in `Assembler::handle` —
`INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`) returns `usize`, so a `u64` limit could not be
compared against it without a cast of its own at every one of those sites — trading the
one probabilistic cast this decision closes for a new one of the same shape, just moved.

Bounding the existing type is strictly narrower than changing it, and narrower is what
the cast site actually needed. Pinned by
`max_transaction_events_is_capped_so_the_event_index_cannot_wrap` (`src/config.rs`),
verified red under the mutation that deletes only the upper bound and green once it is
restored.
