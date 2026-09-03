# How it works — for a data engineer opening Rust for the first time

You can read Python, you know what Kafka and an offset are, but you are seeing Rust syntax for the
first time. This document is a route through the code. Twenty minutes to read, after which you can
open `src/` and understand what you are looking at.

There is nothing here about "how to write Rust properly". Only what you need in order to read
**this** code.

---

## 1. What this program does

It connects to PostgreSQL, reads the stream of changes (who inserted, updated, deleted what) and
lays them out as lines of JSON.

```
INSERT INTO users VALUES (1, 'Alice');
```
turns into
```json
{"schema":"public","table":"users","operation":"insert","after":{"id":"1","name":"Alice"},
 "transaction_id":748,"lsn":"0/19742B8","commit_lsn":"0/19743B0","commit_timestamp":"..."}
```

This is CDC — change data capture. The industrial equivalent you have probably seen is
Debezium. We do the same thing, only small and our own.

### The analogy that explains everything

Inside, PostgreSQL is built like Kafka, and if you keep that in mind everything else falls into place:

| In Kafka | In PostgreSQL | What it is for us |
|---|---|---|
| The partition log | **WAL** — the write-ahead log | What we read |
| Offset | **LSN** — a position in the WAL, `0/19742B8` (that is hex) | `src/lsn.rs` |
| Consumer group (holds the committed offset) | **Replication slot** | Created before the start, from outside |
| Retention | The server keeps WAL up to the slot's position | Fall behind and the slot gets killed |
| `commit()` an offset | **Acknowledging** a position to the slot | The most dangerous place in the code |
| Message format | **`pgoutput`** — a binary protocol | `src/postgres/pgoutput.rs` |

Hold on to one thing and half the decisions in the code become obvious:

> **Acknowledging a position = allowing the server to delete the log up to it. Irreversible.**

Exactly like committing an offset in Kafka before you have written the message to the sink.
The classic "at-most-once instead of at-least-once" bug. This whole project is about making sure
that never happens.

---

## 2. A crash course in Rust

Only what you will meet in our code. The examples are real, taken from `src/`.

### 2.1. Errors are a return value, not an exception

In Python a function can throw anything from anywhere, and the signature says nothing about it.
In Rust an error is part of the return type:

```rust
pub fn try_ack(&mut self, lsn: Lsn) -> Result<(), PgcdcError>
```

`Result<T, E>` means "either `Ok(T)` or `Err(E)`". Here `T` is `()`, the empty placeholder (the
equivalent of `None` as the result of a procedure). It reads as: "the function returns nothing on
success, and hands back a `PgcdcError` on failure".

You cannot ignore it — the compiler will complain. You unwrap it with a question mark:

```rust
sink.write_transaction(&tx).await?;
```

`?` means: if there is an `Err` inside, **leave the current function immediately**, returning that
error upwards. If it is `Ok`, take the value out and carry on. This is Python's
`raise` + `except: raise` in a single character, but visible in the code.

**Why this matters for us.** Look at the line above once more. Take the `?` out of it and you get
`let _ = sink.write_transaction(&tx).await;`, and a write failure is swallowed in silence.
We really did get caught by this: a suite of 168 tests stayed green. The details are
in §7.

### 2.2. `Option<T>` — instead of `None`, but you MUST handle it

```rust
pub before: Option<Row>,
```

"Either `Some(row)` or `None`". In Python you would write `before: dict | None` and forget
to check. Here the compiler will not let you take the value out without handling the `None` case.

It often turns up together with `if let`:

```rust
if let Some(tx) = handled? {
    // we only get here if a transaction really did come together
}
```

### 2.3. `enum` is NOT Python's Enum

This is the most important divergence, and everybody trips over it.

Python's `Enum` is a set of named constants. Rust's `enum` is a **tagged
union**, closer to `typing.Union` or to dataclasses with a tag. Each variant carries
its own fields:

```rust
pub enum PgcdcError {
    SlotMissing { slot: String },
    SlotAhead { slot: String, slot_lsn: String, durable: String },
    SlotUnusable { slot: String, reason: String },
    SlotBusyTimedOut { slot: String, waited_ms: u64, budget_ms: u64 },
    Decode(String),
    UnknownRelation { relation_id: u32 },
    // ...
}
```

One type, thirteen shapes, each with its own data.

### 2.4. `match` is exhaustive — and that is our defence

```rust
match read {
    Ok(Ok(raw))  => { /* data arrived */ }
    Ok(Err(e))   => { /* the connection dropped */ }
    Err(_elapsed) => { /* timeout, there was nothing to read */ }
}
```

Like `match` in Python 3.10, with one difference: **the compiler requires every variant to be
covered**. Forget one and the code does not build.

We use this deliberately. Here is a comment from `src/error.rs`:

```rust
/// `is_fatal` is implemented as an exhaustive match with no `_ =>`, so the compiler forces
/// classification of every new variant. A forgotten classification is the path
/// to "went down the retry branch and silently lost events".
```

That is: add a new kind of error and the compiler will not let the project build until you say
whether it is fatal or has to be retried. In Python this would be `else: pass` and a bug six months later.

The same trick in `src/main.rs` when the sink is chosen:

```rust
// The appearance of a third output variant will force the compiler to demand a decision,
// rather than falling through a silent default branch.
let sink: Box<dyn Sink> = match (config.output, &config.output_path) {
    (OutputKind::Stdout, _)       => Box::new(StdoutSink::new()),
    (OutputKind::File, Some(path)) => { /* ... */ }
    (OutputKind::File, None)       => { /* error: --output-path is required */ }
};
```

### 2.5. `&` and `&mut` — who has the right to change things

This is where Rust differs from Python the most, but one rule is enough to read the code:

- `&Metrics` — "borrowed to look at", must not be changed, there may be many readers;
- `&mut SessionState` — "borrowed to change", **and at that moment nobody else is holding this
  thing**, which is checked at compile time.

In Python two threads can happily corrupt one dictionary and you find out in production. In
Rust such code simply does not build.

That is why the signatures look like this:

```rust
async fn acknowledge_durable(
    state: &mut SessionState,   // will be changed
    stream: &mut LogicalReplicationStream,
    durable: Lsn,               // a copy, a number
    metrics: &Arc<Metrics>,     // read only, shared between tasks
) -> Result<Lsn, PgcdcError>
```

A signature reads as documentation: you can see who gets mutated here and who does not.

### 2.6. `trait` is `Protocol` / `ABC`

```rust
#[async_trait::async_trait]
pub trait Sink: Send {
    fn durability(&self) -> Durability;
    async fn write_transaction(&mut self, tx: &Transaction) -> Result<(), PgcdcError>;
    async fn flush(&mut self) -> Result<Option<Lsn>, PgcdcError>;
}
```

An interface. Whoever implements it is a sink. We have two: `StdoutSink` and `FileSink`.

`Box<dyn Sink>` is a variable holding **some** implementation, decided at runtime.
A direct analogue of a Python variable annotated with a protocol.

### 2.7. `impl` — methods live apart from fields

In Python fields and methods sit in one `class`. In Rust they are separate:

```rust
pub struct LsnTracker {          // fields
    received: Lsn,
    durable: Lsn,
    acked: Lsn,
    processed: Lsn,
}

impl LsnTracker {                // methods
    pub fn acked(&self) -> Lsn { self.acked }
}
```

Fields without `pub` are not visible from outside at all — not an underscore convention, but a
compiler prohibition. That is exactly why `acked` can be changed **only** through `try_ack`, and that
is not team discipline but a property of the type.

### 2.8. `async` / `.await` — almost like Python

```rust
sink.write_transaction(&tx).await?;
```

Reads like `await sink.write_transaction(tx)` in Python, plus the `?` at the end. The role of
`asyncio` is played by **tokio**.

One trap that bit us: tokio has two flavors — single-threaded and multi-threaded.
The replication library picks different code inside itself depending on the flavor. Tests by
default run single-threaded, production runs multi-threaded, and we spent half a day exercising
code that was not the production one. So **every** integration test is marked explicitly:

```rust
#[tokio::test(flavor = "multi_thread")]
```

### 2.9. `Arc<Metrics>` and `AtomicU64`

```rust
let metrics = Arc::new(Metrics::new());
```

`Arc` = "atomic reference count". One object safely held by several tasks
at once. In Python refcounting is hidden inside the interpreter; here it is in the type.

Inside `Metrics` there are eight `AtomicU64`s, counters that can be changed from different threads
without locks.

### 2.10. `#[derive(...)]` — decorators

```rust
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ChangeEvent { /* ... */ }
```

Like `@dataclass`: the compiler writes debug printing, copying, comparison and JSON
serialization for you. `Serialize` is exactly what makes an event
JSON-ready.

---

## 3. The journey of a single row

From an `INSERT` in the database's mind to a line in your file.

```
   PostgreSQL                     our process
 ┌──────────────┐
 │ INSERT ...   │
 │      ↓       │
 │     WAL      │   START_REPLICATION
 │      ↓       │ ─────────────────────→  ① next_raw_event
 │  walsender   │       raw bytes             ↓
 └──────────────┘                        ② decode          src/postgres/pgoutput.rs
        ↑                                     ↓
        │                                ③ Assembler       src/transaction.rs
        │                                     ↓   (buffers until COMMIT)
        │                                ④ Sink::write     src/sink/
        │                                     ↓
        │                                ⑤ Sink::flush     ← BARRIER (fsync)
        │                                     ↓
        └──────────────────────────────  ⑥ acknowledge     src/lsn.rs
              "you may delete WAL up to here"
```

### ① Read a raw frame

```rust
let read = tokio::time::timeout(SHUTDOWN_POLL_INTERVAL, stream.next_raw_event(&cancel)).await;
```

The timeout wrapper is not there because we are afraid of hanging, but so that we do not sleep
through a shutdown signal for more than 200 ms. An expired timeout here is **not an error**, just
"there was nothing to read":

```rust
// A tick: there was nothing to read. Not an error — a reason to reach the barrier.
Err(_elapsed) => {}
```

### ② Decode `pgoutput`

`decode()` turns bytes into one of seven messages: `Begin`, `Commit`, `Relation`,
`Insert`, `Update`, `Delete`, `Truncate`.

This is a binary protocol, read by hand byte by byte. How it is built is in
[pgoutput-notes.md](pgoutput-notes.md), 965 lines derived from 31 frozen dumps of
real bytes (`tests/fixtures/`). An example from there, so you get the level of pedantry:

> The timestamp counts from **2000-01-01**, not from the Unix epoch. Get it wrong and take
> 1970, and the number `841423351314489` gives 1996-08-30 — **the same day of the month and the same
> time of day**. The wrong epoch looks like a plausible date.

### ③ Assemble the transaction

The `Assembler` buffers changes in memory and hands out a transaction as a whole **only on `Commit`**.

Why: until the `COMMIT` arrives the transaction may be rolled back. Hand an `INSERT` out earlier and
you publish something that never existed in the database.

```rust
pub struct Transaction {
    pub xid: u32,
    pub commit_lsn: Lsn,   // the start of the commit record
    pub end_lsn: Lsn,      // immediately PAST it  ← this is the one we acknowledge
    pub commit_timestamp: DateTime<Utc>,
    pub changes: Vec<ChangeEvent>,
}
```

**Two positions that must not be confused.** `commit_lsn` is where the commit record started,
`end_lsn` is immediately past it. What you have to acknowledge is `end_lsn`. Acknowledge `commit_lsn`
and after every restart you will re-read the last transaction again, forever. It will work,
no data is lost — it is just quietly and endlessly wrong. We have tests that go red on exactly
this substitution.

### ④ Hand it to the sink

```rust
sink.write_transaction(&tx).await?;
```

`Ok` here means **"accepted"**, not "on disk". That is written out in the trait:

```rust
/// Returning `Ok` means "accepted", NOT "durable":
/// a window exists between acceptance and the barrier, and acknowledging a position
/// inside it is forbidden by invariant 1.
```

### ⑤ The barrier

```rust
async fn flush(&mut self) -> Result<Option<Lsn>, PgcdcError>;
```

For `FileSink` this is `flush()` (push it out of the program's buffer into the kernel) followed by
`durable_sync()` (make the kernel finish writing it to disk) — **strictly in that order**; there is
a test that goes red if they are swapped.

`StdoutSink` has no real barrier — the bytes went into a pipe, and beyond that the program cannot
know. So the sinks honestly declare what they can vouch for:

```rust
pub enum Durability {
    Fsync,        // after flush the data is committed to disk: acknowledging is safe
    BestEffort,   // bytes handed to the kernel, their fate unknown. For development.
}
```

and when started with `--output stdout` a warning goes into the log:

```
WARN sink is best-effort, not durable: acknowledged positions may outlive unwritten output
```

### ⑥ Acknowledge

Four positions that `LsnTracker` watches:

| Position | Meaning |
|---|---|
| `received` | the server sent the bytes |
| `processed` | decoded and handed to the sink |
| `durable` | the sink vouched that this will survive a crash |
| `acked` | we told the server "you may delete" |

Each only grows, and none can overtake the previous one. The rule is baked into the type:

```rust
pub fn try_ack(&mut self, lsn: Lsn) -> Result<(), PgcdcError> {
    if lsn > self.durable {
        return Err(PgcdcError::AckBeyondDurable { /* ... */ });
    }
    // ...
}
```

With a comment worth reading in full:

> This is not defensive programming, it is the invariant itself: let such an
> acknowledgement through, and a crash between it and the write would mean a silent loss.

---

## 4. Map of the code

| File | Lines | What is in it |
|---|---|---|
| [src/main.rs](../src/main.rs) | 58 | entry point: parse the flags, pick a sink, run, return an exit code |
| [src/config.rs](../src/config.rs) | 487 | ten CLI flags and their paired environment variables |
| [src/postgres/replication.rs](../src/postgres/replication.rs) | 2142 | **the heart**: two loops, reconnect, acknowledgement, shutdown |
| [src/postgres/pgoutput.rs](../src/postgres/pgoutput.rs) | 711 | binary protocol decoding |
| [src/postgres/guard.rs](../src/postgres/guard.rs) | 182 | the pre-flight check on the slot |
| [src/transaction.rs](../src/transaction.rs) | 1383 | `Assembler` — buffers until `COMMIT` |
| [src/lsn.rs](../src/lsn.rs) | 189 | the four positions and the rules between them |
| [src/schema.rs](../src/schema.rs) | 109 | cache of table descriptions (`RELATION`) |
| [src/event.rs](../src/event.rs) | 137 | `ChangeEvent` — what goes out as JSON |
| [src/sink/](../src/sink/) | 703 | the `Sink` trait + two implementations |
| [src/error.rs](../src/error.rs) | 178 | every kind of error and the fatal / recoverable split |
| [src/metrics.rs](../src/metrics.rs) | 241 | eight counters |

Where to start reading: `main.rs` → `run()` in `replication.rs` → `stream_once()` in the same file.

---

## 5. Two loops

### The outer one — `run()`: staying alive

```rust
loop {
    if shutdown.load(Ordering::Relaxed) { return Ok(()); }   // signal?

    match stream_once(...).await {
        Ok(SessionOutcome::ShutdownRequested) => return Ok(()),
        Ok(SessionOutcome::Disconnected)      => {}           // dropped — reconnect
        Err(e) if !e.is_fatal()               => { /* a retry cures it */ }
        Err(e)                                => return Err(e),  // nothing cures it — exit
    }

    let delay = backoff.next_delay(productive);   // 100ms, 200, 400, ... up to 30s
    // a pause sliced into 200ms chunks, so we do not sleep through the signal
}
```

Note the `Err(e) if !e.is_fatal()` — that is a `match` with a condition (a `guard`). The whole
difference between "wait and try again" and "exit with code 1" is decided **here**, and it is
decided by the type of the error, not by picking apart the text of a message.

### The inner one — `stream_once()`: a single session

The order of statements that this project declares must not change:

```
sink.write_transaction   →  note_processed  →  [on the timer] flush
   →  note_durable  →  try_ack  →  send_feedback
```

In the code it is marked outright:

```rust
// The order MUST NOT change: sink first, then barrier, then durable, only then ack.
```

Swap them around and the tests go red. We checked this with a mutation: acknowledging before the
barrier drops three tests.

---

## 6. Three invariants

From [DECISIONS.md](../DECISIONS.md). This is the project's constitution.

**1. `acked <= durable`.** You must not tell the server "delete" about something that is not yet on
disk. Held by the `LsnTracker` type.

**2. Duplicates are allowed, silent loss is not.** If we crash between the write and the
acknowledgement, the slot will hand the same transactions over again. That is fine: the consumer MUST
be idempotent. Losing data, on the other hand, is never allowed.

**3. Nothing capable of losing events exits with a zero.** The exit code is the only
language in which the program speaks to Kubernetes. Zero means "did the job". A program
that quietly does nothing is the worst kind of failure: the dashboard is green, the lag grows, nobody
comes.

---

## 7. What actually caught us out

The most useful part. All of these defects were found by review and **not one of them by green tests**.

### A test that stays green under a mutation of the very thing it checks

Five times over the project. The technique that caught them: **break the code deliberately and see
whether the suite goes red.** It did not go red, so there is no coverage, whatever the report says.

One trap in the technique, and it fakes a *pass* rather than a failure. Cargo decides what to
rebuild by comparing modification times, so restoring a mutated file with anything that preserves
the old timestamp leaves the previous binary in place. The test then goes green without ever
running the restored code, and the mutation looks like it was caught when nothing was rebuilt at
all. `touch` the file before rebuilding, and treat a suspiciously instant `cargo test` as the
symptom rather than as luck. The same trap has a Docker-shaped twin, documented in the
`Dockerfile`: a `COPY` that restores an mtime lets cargo skip the real build and ship the stub.

Three cases in a row at the finish, all on central claims. The count below is the size of the
suite **at the time those mutations were run** — do not "correct" it to today's number: the point
is that 168 tests were green while the code was broken. All three mutations are caught now,
because the fix round that followed added tests that pin them.

- we deleted the periodic metrics report block **entirely** — 168 tests green;
- we replaced `sink.write_transaction(&tx).await?` with `let _ = ...` (swallow the write failure) —
  168 green. And for the file sink that is a real silent loss;
- we sent the server `received` instead of `acked` — 168 green, even though the slot was
  running tens of megabytes ahead of us.

The last one is especially instructive. The test that supposedly covered it read **our own
counter** — that is, our decision, not what actually went out on the wire.

> A counter that records intent proves intent, but never consequence.

### The password in `--help`

`clap` (the argument-parsing library) prints environment variable values in the help text. The
connection string with the password was leaking into `--help`. We fixed it with `hide_env_values`, and
then it turned out clap also prints **rejected** values in the error text — so we had to
make connection-string parsing fundamentally non-rejecting.

### The slot that got silently re-created

At start-up the transport library unconditionally calls `ensure_replication_slot()`. If the slot is
missing, it creates it **at the current WAL position**, and everything committed before the start never
arrives. Silently. We measured this rather than assumed it, and wrote a pre-flight
check that fails instead of being that "helpful". Hence, too, the list of five calls of this library
that are forbidden to use, in [spike-findings.md](spike-findings.md) §3.

### An endless retry instead of an exit code

On an invalidated slot (the server deleted the WAL we needed) the process reconnected forever
instead of exiting with a one. The difference the code did not see: **a server that does not answer and
a server that answers with a refusal are different things**. The first is cured by a retry, the second
never is.

### A comment describing a mechanism that does not exist

Fourteen times. Every one was found by a reader, not by the author — and twice in a row the comment lied
inside the very construct that was being fixed at that moment. The three most recent landed in the
documents written specifically to earn trust in the rest of the text: a spec header with the wrong test
count, a CI comment overstating how many tests need a container, and a README sentence that explained
one throughput swing with a number pulled from a different metric.

> Whoever changes a mechanism is the last person able to notice that the description has stopped
> matching it.

---

## 8. Run it and see

```bash
docker compose up -d --wait                  # Postgres only
docker compose --profile demo up -d pgcdc    # and our tool

psql -h 127.0.0.1 -U postgres -d app -c "INSERT INTO users VALUES (1,'Alice',NULL,NULL);"
docker compose logs pgcdc | grep '"operation"'
```

The payload goes to **stdout**, the logs to **stderr**. Always. Which is why this works:

```bash
pgcdc --output stdout ... | jq -r '.table'
```

The pipe gets only JSON, the logs stay on screen. There is a test guarding this even
on the fatal-error path.

### What to read in the logs

```
INFO  slot_preflight_ok slot=pgcdc_slot restart_lsn=Some("0/192FED8") ...
INFO  replication_started slot=pgcdc_slot publication=pgcdc_pub
INFO  metrics_report events=3 transactions=3 bytes=395 reconnects=0 errors=0
      last_received_lsn=0/1974170 last_acknowledged_lsn=0/19741A8 buffer=0
      streaming=true ack_age_s=Some(2)
```

The report comes out once every ten seconds — including while the process has no
connection at all: it also prints from inside the paused wait between reconnect
attempts, specifically so `streaming=false` is something you can actually see during an
outage, not only inferred from its absence. Per-event lines are at `DEBUG`
(`RUST_LOG=debug`), because at a thousand transactions a second that is a thousand lines a second.

Warning signs:
- `buffer` does not drop to zero — an open transaction is stuck;
- `last_received_lsn` grows while `last_acknowledged_lsn` stands still — we are not reaching the barrier;
- `reconnects` grows steadily — something is tearing the connection;
- `streaming=false` together with a climbing `ack_age_s` — the pair above looks identical
  to a healthy, idle process while disconnected, since neither position moves either way;
  this is the signal that tells the two apart;
- `error_kind="slot_unusable"` or `"slot_busy_timed_out"` — the process is about to exit with a 1,
  and that is right.

---

## 9. Where to dig if you need to add something

**A new sink (Kafka, S3, anything).** Implement the `Sink` trait — three methods. The compiler
will make you add a branch to the `match` inside `main.rs` itself; it will not let you forget.

The only question you will have to answer honestly is `durability()`. For Kafka the real
barrier is `acks=all` plus acknowledgement from the required number of replicas. Answer
`Fsync` without waiting for them and you get exactly the silent loss that all of this was
built to forbid.

**A new field in the JSON.** `ChangeEvent` in `src/event.rs`. `#[derive(Serialize)]` will pick it up
itself.

**A new kind of error.** Add a variant to `PgcdcError` — the compiler will require it to be
classified in `is_fatal()`.

---

## 10. What to take away

1. **Acknowledging a position is irreversible.** Everything else in this code follows from that.
2. **`Ok` from a write ≠ data on disk.** There is a window between them, and acknowledging inside it is not allowed.
3. **A green test suite is a claim about the tests, not about the code.** If you want to know whether
   something is covered, break it deliberately and look.
4. **The Rust compiler is not a bureaucrat, it is a second reviewer.** Half the decisions here were made
   so that forgetfulness gets caught at build time, not in production.
5. **A silent failure is worse than a loud one.** A program forever repeating a doomed attempt
   looks like it is working, right up to the question "so what did it do this week?".

---

Next: [DECISIONS.md](../DECISIONS.md) — why it was done this way (35 decisions),
[spike-findings.md](spike-findings.md) — what we found out about the transport library and what
must not be used, [pgoutput-notes.md](pgoutput-notes.md) — the bytes of the protocol.
