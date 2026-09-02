use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pg_walstream::{
    CancellationToken, LogicalReplicationStream, ReplicationError, ReplicationStreamConfig,
    RetryConfig, StreamingMode,
};
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::error::PgcdcError;
use crate::lsn::{Lsn, LsnTracker};
use crate::metrics::Metrics;
use crate::postgres::guard::{check_reconnect, preflight_slot};
use crate::postgres::pgoutput::decode;
use crate::schema::RelationCache;
use crate::sink::Sink;
use crate::transaction::Assembler;

/// The connection string for a replication connection requires
/// `replication=database` — without it the server opens an ordinary session.
fn replication_url(base: &str) -> String {
    if base.contains('?') {
        format!("{base}&replication=database")
    } else {
        format!("{base}?replication=database")
    }
}

/// Whether the position that arrived in a keepalive may be acknowledged.
///
/// "The buffer is empty" is NOT sufficient on its own (DECISIONS Q26a). It was sufficient
/// while the write, the durable mark and the acknowledgement happened in one iteration;
/// with the group barrier a window appears where there are no open transactions, yet
/// there is data the sink has accepted and has not carried to the medium. Acknowledging
/// a position inside that window means acknowledging beyond durable and losing data on a
/// crash.
fn may_advance_from_keepalive(assembler_empty: bool, processed: Lsn, durable: Lsn) -> bool {
    assembler_empty && processed <= durable
}

/// Whether the slot's reported `wal_status` means it can never stream again.
///
/// Only `lost` qualifies. `unreserved` means the required WAL is scheduled for
/// removal at the next checkpoint but has not gone yet, and PostgreSQL documents
/// that the slot can climb back to `reserved` or `extended` — refusing it would
/// abort a recoverable process. `None` means the column was NULL or unreadable;
/// that is not a diagnosis, so it is not fatal either.
fn slot_health_is_terminal(wal_status: Option<&str>) -> bool {
    matches!(wal_status, Some("lost"))
}

/// The state that survives a connection drop.
///
/// The split here is not cosmetic. The tracker's positions are **carried** across a
/// reconnect: they are monotone, so a replay of already processed transactions cannot
/// move them backwards, and the durable position is exactly what `check_reconnect`
/// compares the slot's `confirmed_flush_lsn` against. Zeroing the tracker would destroy
/// the only input of that check.
///
/// The relation cache and the assembler, by contrast, are **reset**: the cache lives
/// within one replication session and after a drop may describe a stale schema
/// (DECISIONS Q19), while a half-assembled transaction arrives again in full, because
/// its BEGIN was after `confirmed_flush_lsn`.
///
/// The buffer gauge (`transaction_buffer_size`) is zeroed right here, by a separate call:
/// its only ordinary write site lives in the receiving
/// branch of `stream_once` and fires only on a data frame, while this reset happens on a
/// connection drop without a single new frame. Not zeroing it would mean holding the
/// last non-zero value arbitrarily long on a publication that goes idle after the drop —
/// the gauge, unlike the acknowledged position, is allowed a second write site precisely
/// because it MUST be able to fall outside the common tail of `acknowledge_durable`.
pub(crate) struct SessionState {
    tracker: LsnTracker,
    assembler: Assembler,
    cache: RelationCache,
}

impl SessionState {
    fn new(max_transaction_events: usize) -> Self {
        Self {
            tracker: LsnTracker::new(),
            assembler: Assembler::new(max_transaction_events),
            cache: RelationCache::new(),
        }
    }

    fn reset_for_reconnect(&mut self, metrics: &Metrics) {
        self.cache.clear();
        self.assembler.reset();
        metrics.set_transaction_buffer_size(0);
    }

    fn durable(&self) -> Lsn {
        self.tracker.durable()
    }
}

/// How one replication session ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionOutcome {
    /// The connection dropped. The outer loop decides whether to reconnect.
    Disconnected,
    /// A shutdown signal arrived and the current group has been carried through the barrier.
    ShutdownRequested,
}

/// Raises the flag on SIGTERM or SIGINT. The flag is read in THREE places: inside the
/// session (`stream_once`, on every turn of its loop), on entry to each pass of the
/// outer reconnect loop (`run`), and inside the sliced backoff pause between attempts —
/// three, not two, as an earlier version of this comment claimed.
///
/// Only TWO of them are bounded by `SHUTDOWN_POLL_INTERVAL` — the read inside the
/// session and the slicing of the backoff pause — not two by count, but these two
/// specifically: not `ack_interval_ms` (that one drives only the barrier schedule) and
/// not the length of the pause itself. Inside the session the value bounds the read
/// itself; the backoff slicing checks the flag before each chunk of that same length.
/// The check on entry to a pass of the outer loop is NOT on that list: its period is a
/// whole turn (a session plus the reconnect pause), not `SHUTDOWN_POLL_INTERVAL`. It is
/// not redundant: the slicing checks the flag BEFORE each chunk and never once AFTER the
/// last one, so a signal landing in exactly that last chunk gets through only via this
/// third, unbounded check on the next turn (the details and the cost of that delay are
/// at the check itself, `run`).
///
/// The bound holds only inside these places, not in the gap between the flag read on
/// entry to the outer loop and the first flag read inside the session: in that gap sit
/// the pre-flight check, establishing the connection and starting replication
/// (`stream_once` up to entering its loop) — none of these steps looks at the flag or is
/// bounded in time. Against a refused port this costs nothing: TCP refuses immediately.
/// Against an address that does not answer at all (a black hole, a firewall swallowing
/// packets), the signal can go unnoticed for tens of seconds — for the length of the
/// connection-establishment timeouts, not for `SHUTDOWN_POLL_INTERVAL`. This is simpler
/// than building a select around the read, and it does not touch the order of operations
/// that mutation testing has verified.
fn spawn_shutdown_listener() -> Arc<AtomicBool> {
    let flag = Arc::new(AtomicBool::new(false));
    let f = flag.clone();
    tokio::spawn(async move {
        let mut term =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    warn!(error = %e, "cannot install SIGTERM handler");
                    return;
                }
            };
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
        f.store(true, Ordering::Relaxed);
    });
    flag
}

/// Doubling with a ceiling. `saturating_mul` instead of `*` — so that doubling near the
/// top of the range does not panic in a debug build.
fn next_backoff(current: Duration, max: Duration) -> Duration {
    let doubled = current.saturating_mul(2);
    if doubled > max {
        max
    } else {
        doubled
    }
}

/// Whether there is already a durable position to check the slot against. On a cold
/// start there is nothing to compare with — durable is still zero; the check is
/// meaningful only from the second connection onwards.
fn is_reconnect(durable: Lsn) -> bool {
    durable > Lsn(0)
}

/// The SQLSTATE of the "the slot is still held by our own prior session" busy race
/// (`ERRCODE_OBJECT_IN_USE`, `ReplicationSlotAcquire` in PostgreSQL's `slot.c`).
const SLOT_BUSY_SQLSTATE: &str = "55006";

/// Pulls the SQLSTATE out of a `pg_walstream` error string, if there is one there.
///
/// This crate's formatting of the server's reply
/// (`connection/native/error.rs::PgErrorFields::Display`) puts the state code into the
/// same string as the message text: `"{severity}: {message} (SQLSTATE {code})"`. Both
/// live runs against a real Postgres confirmed this verbatim: `SQLSTATE 55000` on an
/// invalidated slot, `SQLSTATE 22023` on a foreign output plugin.
/// The state code is a five-character identifier that the PostgreSQL protocol never
/// translates; the `message` next to it is translated when the server has a localized
/// `lc_messages`.
fn extract_sqlstate(message: &str) -> Option<&str> {
    const MARKER: &str = "(SQLSTATE ";
    let start = message.find(MARKER)? + MARKER.len();
    let rest = message.get(start..)?;
    let end = rest.find(')')?;
    let code = &rest[..end];
    (code.len() == 5 && code.bytes().all(|b| b.is_ascii_alphanumeric())).then_some(code)
}

/// Classifies a `stream.start()` failure.
///
/// Before this function, ANY `START_REPLICATION` error was wrapped in
/// `PgcdcError::Connection` — the recoverable variant — and the process went into an
/// endless reconnect at the backoff ceiling even when the slot was invalidated
/// (`SQLSTATE 55000`) or carried a foreign output plugin (`SQLSTATE 22023`): the server
/// ANSWERED and explicitly refused, it did not drop the connection. A repeated
/// `START_REPLICATION` with the same parameters will get the same refusal an hour later
/// too — retrying it is not recovering, but hiding an irreversible loss of access to the
/// WAL behind the appearance of a working process (invariant 3, DECISIONS §1).
///
/// The distinction rests on `pg_walstream::ReplicationError::is_transient()`: a socket
/// drop or a temporary transport fault (`Io`/
/// `TransientConnection`/`Timeout`/`ReplicationConnection`/`Backend`)
/// stays recoverable, while `Protocol` — into which the crate wraps both an explicit
/// server refusal of `START_REPLICATION` and a low-level parse error of the wire format
/// itself (an illegal message length, say, `connection/native/copy.rs`) — is fatal by
/// default. For the second case (corruption of the protocol itself, not a refusal
/// addressed to the slot) the "fatal" verdict is just as correct as for the first — it
/// is unsafe to silently retry a stream whose encoding has already diverged from what we
/// expect — but the variant's name, `PgcdcError::SlotUnusable`, is misleading: this
/// branch is wider than its name and catches any `Protocol`, not only a refusal that the
/// server addressed to the slot itself.
///
/// The only exception is the "the slot is still held by our own prior session" busy race
/// (`SQLSTATE 55006` = `ERRCODE_OBJECT_IN_USE`, `ReplicationSlotAcquire` in PostgreSQL's
/// `slot.c`): the server answers here too, but the refusal is not about the slot itself,
/// it is about the previous walsender not having released it yet — our own reconnect may
/// have arrived before the server finished cleaning up the prior session (DECISIONS Q19:
/// every reconnect is a new connection and a new `START_REPLICATION`). This resolves by
/// itself on the next attempt; declaring it fatal would mean killing the process over a
/// race that our own reconnect creates.
///
/// This race is told apart by the state code (`extract_sqlstate`), not by a translatable
/// substring of the text: the state code is never translated, the message text is —
/// whenever the server has a localized `lc_messages` — and then the substring check
/// would silently stop finding the race, turning every occurrence of it into a fatal
/// exit. The substring `"is active for PID"` remains a fallback condition only for the
/// case where the error string somehow carries no SQLSTATE at all (a future version of
/// the crate changing the formatting, say) — not because it is equivalent to the code;
/// relying on it as the primary check is exactly what this round fixed.
///
/// The limitation this function CANNOT close on its own: a slot held FOREVER by a
/// FOREIGN (not our own) consumer answers with literally the same `SQLSTATE 55006` — by
/// the state code alone, "our prior session has not disconnected yet" and "someone else
/// holds the slot forever" are indistinguishable, and the exception above classifies
/// both as recoverable. What tells these two cases apart is physical, not in the state
/// code: our prior session releases the slot within tens of milliseconds (measured, see
/// `SlotBusyPatience`), a foreign consumer does not. That is exactly why this function
/// stays PURE (no state, no time) and does not settle the question itself: the caller
/// (`classify_start_outcome`) wraps its decision in a patience budget accumulated by
/// duration and escalates to `PgcdcError::SlotBusyTimedOut` once the patience is spent.
fn classify_start_error(slot: &str, e: ReplicationError) -> PgcdcError {
    let reason = e.to_string();
    if e.is_transient() || is_busy_race_reason(&reason) {
        PgcdcError::Connection(format!("start replication: {e}"))
    } else {
        PgcdcError::SlotUnusable {
            slot: slot.to_owned(),
            reason,
        }
    }
}

/// The shared test for the busy race (`SQLSTATE 55006`), pulled out of
/// `classify_start_error` separately so that `classify_start_outcome` can check it
/// BEFORE the error is classified and the message text becomes unreachable without
/// another `to_string()`.
fn is_busy_race_reason(reason: &str) -> bool {
    match extract_sqlstate(reason) {
        Some(code) => code == SLOT_BUSY_SQLSTATE,
        None => reason.contains("is active for PID"),
    }
}

/// Tracks how long the slot has been answering with the busy race (`SQLSTATE 55006`)
/// IN A ROW. The state code does not distinguish "our prior session has not disconnected
/// yet" (resolves within tens of milliseconds) from "someone else holds the slot
/// forever" (never resolves) — the only physical discriminator is DURATION, which is why
/// the patience budget is given in time and not in a number of attempts: the number of
/// attempts depends on the length of the backoff pause, not on the nature of the
/// failure.
///
/// Measured (30 cycles of "walsender holds the slot → drop → time to the next successful
/// `START_REPLICATION` from scratch, including establishing a new connection" — the same
/// operation `stream_once` performs on every reconnect): 45–124ms, median ~76ms.
/// Measured separately, the raw time until the `pg_replication_slots.active` flag
/// clears, without the overhead of a new connection: 1.1–3.5ms, median ~1.8ms — that is,
/// almost the whole trace in the first measurement is not the slot-release delay but the
/// TCP + authentication + `START_REPLICATION` overhead of the probe connection itself.
/// The budget default (`--slot-busy-budget-ms`, 30000ms, `Config`) is taken with a
/// margin of ~240× over the worst observation of a full reconnect cycle and ~8500× over
/// the raw slot-release time.
///
/// Observations of the race within one episode need not follow with no break at all: a
/// failure of a different nature (a transport fault, a pre-flight check failure, a TCP
/// drop) may wedge itself between two observations of the race without closing the
/// episode entirely. Zeroing everything on ANY such failure (the first version of this
/// fix) fixed one hole and opened the opposite
/// one: a slot held FOREVER by a FOREIGN consumer on a server that on top of that
/// occasionally drops the connection for an unrelated reason would never accumulate more
/// time than the interval between two such failures — reproduced by a unit test with the
/// default budget (30000ms), a continuously busy slot and an unrelated failure once
/// every 29 seconds: under full zeroing no escalation happens even once over a simulated
/// hour.
///
/// So a failure of a different nature (`interrupt`) does not zero the episode, it only
/// BREAKS the chain of consecutive observations: the accumulated time (`accumulated`) is
/// kept, but the interval BETWEEN the last observation of the race and the next one — in
/// full, not only the part of it after the failure itself — does not go into the
/// accumulated total. That is exactly the subtraction of the idle interval: we
/// physically do not know what happened inside the interval BEFORE the failure, it could
/// have been the same idle interval from beginning to end, so only the time between two
/// observations of the race with not a single foreign failure in between counts. The
/// episode is closed entirely (zeroing both the accumulated total and the chain) only by
/// `reset()` — which is called SOLELY on a successful session start
/// (`classify_start_outcome`, the `Ok` branch): that is the only observation which
/// physically proves the slot is free right now, and not merely that there was no
/// busy-race answer in a row.
struct SlotBusyPatience {
    /// The sum of the durations of all consecutive (with no failure of a different
    /// nature wedged in) gaps between observations of the busy race inside the current
    /// episode.
    accumulated: Duration,
    /// The moment of the last observation of the busy race in the current, not yet
    /// broken chain. `None` means either there has been no episode at all yet, or the
    /// chain was broken by a failure of a different nature: the next observation of the
    /// race MUST start counting anew, without charging the episode with the interval
    /// since that break.
    last_busy: Option<Instant>,
}

impl SlotBusyPatience {
    fn new() -> Self {
        Self {
            accumulated: Duration::ZERO,
            last_busy: None,
        }
    }

    /// Records another observation of the busy race at the moment `now`. If the chain is
    /// unbroken (`last_busy` is present), the interval since the previous observation is
    /// added to the accumulated total; if it is broken, or this is the very first
    /// observation at all, no interval is added — there is nothing to accumulate.
    /// Returns `Some(accumulated)` once the accumulated total has reached or exceeded
    /// `budget` — the caller MUST treat that as the patience being spent and as a fatal
    /// error.
    ///
    /// `saturating_duration_since`/`saturating_add` guard against a negative or
    /// overflowed duration under any ordering of events: if `now` arrives earlier than
    /// the previous observation (events out of order) or the total time hits the ceiling
    /// of the `Duration` representation, both operations saturate instead of panicking
    /// or going negative.
    fn observe_busy(&mut self, now: Instant, budget: Duration) -> Option<Duration> {
        if let Some(prev) = self.last_busy {
            self.accumulated = self
                .accumulated
                .saturating_add(now.saturating_duration_since(prev));
        }
        self.last_busy = Some(now);
        (self.accumulated >= budget).then_some(self.accumulated)
    }

    /// Breaks the chain of consecutive observations of the race WITHOUT closing the
    /// episode: the accumulated time stays as it was, but the interval from the last
    /// observation of the race to the next one will not enter the accumulated total —
    /// subtraction of the idle interval, not zeroing. Called on any failure that is
    /// neither the busy race nor a successful session start: a failure of a different
    /// nature inside `classify_start_outcome`, and any `stream_once` failure BEFORE the
    /// classification even happened (`interrupt_patience_on_early_failure`: the slot
    /// pre-flight check, the reconnect check, opening the connection).
    fn interrupt(&mut self) {
        self.last_busy = None;
    }

    /// Closes the episode entirely: the accumulated time is zeroed along with the chain.
    /// The only call site in production is a successful session start
    /// (`classify_start_outcome`, the `Ok` branch): that is the only observation which
    /// proves the slot is free right now, and not merely that there was no race in a
    /// row.
    fn reset(&mut self) {
        self.accumulated = Duration::ZERO;
        self.last_busy = None;
    }
}

/// Classifies the outcome of `stream.start()` together with the decision about patience
/// for a busy slot — the tail shared by both halves of the problem:
/// `classify_start_error` decides recoverable/fatal from ONE attempt, this function adds
/// the decision accumulated by TIME on top of it. Pulled out of `stream_once` so that
/// the mutation "remove the patience reset on a successful start" is caught by a
/// value-level unit test and not only by an integration scenario against a real Postgres
/// (the same device as with `session_was_productive`/`ReconnectBackoff` above).
fn classify_start_outcome(
    slot: &str,
    result: Result<(), ReplicationError>,
    patience: &mut SlotBusyPatience,
    budget: Duration,
    now: Instant,
) -> Result<(), PgcdcError> {
    let e = match result {
        Ok(()) => {
            patience.reset();
            return Ok(());
        }
        Err(e) => e,
    };
    if is_busy_race_reason(&e.to_string()) {
        if let Some(waited) = patience.observe_busy(now, budget) {
            return Err(PgcdcError::SlotBusyTimedOut {
                slot: slot.to_owned(),
                waited_ms: waited.as_millis() as u64,
                budget_ms: budget.as_millis() as u64,
            });
        }
        // The race is still within budget: the only branch that MUST NOT touch the
        // patience — otherwise an episode would never accumulate enough time to fire
        // at all.
        return Err(classify_start_error(slot, e));
    }
    // A failure of a different nature (not the
    // busy race) BREAKS the chain of consecutive observations, it does not close the
    // episode entirely — closing it fully here would fix the summing of unrelated
    // episodes, but would open the opposite hole: a slot held forever by a foreign
    // consumer, interleaved with rare failures of a different nature, would never
    // accumulate enough time to escalate.
    patience.interrupt();
    Err(classify_start_error(slot, e))
}

/// The common tail for ANY `stream_once` failure that happened BEFORE
/// `classify_start_outcome` got to classify `stream.start()`: the slot pre-flight check
/// itself, its terminal-`wal_status` refusal, the reconnect check, opening the connection.
/// None of these failures can be the busy race — SQLSTATE 55006 comes back only in the
/// server's answer to `START_REPLICATION` itself — so such a failure unconditionally
/// BREAKS the chain of consecutive observations of the race:
/// without this, the clock accumulated by a past race would keep running for the whole
/// time the server is unreachable for an entirely different reason, and the interval of
/// unreachability would add into the accumulated total as if the slot had been answering
/// with the race all along. The break does NOT close the episode — the time accumulated
/// earlier is kept, the escalation of a continuously busy slot with rare unrelated
/// failures still happens, just without adding the interval of the failure itself. `Ok`
/// here deliberately does not touch the patience: success of the pre-flight check/the
/// reconnect check/opening the connection does not yet mean the session started — only
/// `classify_start_outcome` further along `stream_once` decides that.
fn interrupt_patience_on_early_failure<T>(
    result: Result<T, PgcdcError>,
    patience: &mut SlotBusyPatience,
) -> Result<T, PgcdcError> {
    if result.is_err() {
        patience.interrupt();
    }
    result
}

/// Whether the session that has just ended was productive for the purposes of resetting
/// the reconnect backoff. It reads the tracker's ACKNOWLEDGED position (`acked`), not
/// the received one (`received`), deliberately: both the group barrier and the
/// keepalive advance on an idle publication move `acked`, while `received` reacts only
/// to the arrival of a data frame. Pulled into its own function taking the tracker
/// itself rather than bare `Lsn`s precisely so that this read can be pinned by a unit
/// test at this level: the live proof that the divergence is real is a quiet run where
/// the metrics report showed `acked` advanced with `received` at zero, the keepalive
/// acknowledged WAL without accepting a single frame.
fn session_was_productive(tracker: &LsnTracker, acked_before: Lsn) -> bool {
    tracker.acked() > acked_before
}

/// The pause before the next connection attempt. Wrapped in a type rather than a bare
/// `Duration` living inside the infinite loop of `run()` with real `sleep`s — for
/// testability: in that form the mutation "remove the reset" was caught by no test at
/// all.
struct ReconnectBackoff {
    current: Duration,
    initial: Duration,
    max: Duration,
}

impl ReconnectBackoff {
    fn new(initial: Duration, max: Duration) -> Self {
        Self {
            current: initial,
            initial,
            max,
        }
    }

    /// A productive session resets the pause to the initial one: without that, one long
    /// outage would leave the pause at the ceiling forever, and the next isolated
    /// failure a week later would wait half a minute for nothing. What counts as
    /// productivity is up to the caller; what must be passed in here is movement of the
    /// ACKNOWLEDGED position, not of the received one: the keepalive advance on an idle
    /// publication acknowledges WAL without reading anything, and would be wrongly
    /// judged unproductive.
    ///
    /// Returns the pause to wait BEFORE this attempt, and advances itself for the next
    /// call.
    fn next_delay(&mut self, productive: bool) -> Duration {
        if productive {
            self.current = self.initial;
        }
        let delay = self.current;
        self.current = next_backoff(self.current, self.max);
        delay
    }
}

/// The tail common to both ways of proving a durable position: the barrier
/// (`Sink::flush`) and the keepalive advance prove it differently, but once the position
/// is settled, both places go on to mark it, acknowledge it in the tracker and send
/// feedback — by word-for-word identical four steps. The barrier is NOT part of this and
/// cannot be: only the caller decides what counts as durable, this function merely
/// records the decision — otherwise the keepalive path could quietly acquire a barrier.
///
/// Returns the tracker's ACKNOWLEDGED position (acked), not the `durable` passed in:
/// today they coincide, but with a reconnect inside the process (the next stage) a
/// replay of an already acknowledged transaction may differ — sending in feedback
/// something other than what the tracker acknowledged would mean rolling the server
/// backwards. We acknowledge acked, NOT commit_lsn: commit_lsn points at the start of
/// the commit record, and a restart would re-read the same transaction.
async fn acknowledge_durable(
    state: &mut SessionState,
    stream: &mut LogicalReplicationStream,
    durable: Lsn,
    metrics: &Arc<Metrics>,
) -> Result<Lsn, PgcdcError> {
    // The order holds invariant 1 at this call site by construction: `note_durable` has
    // just raised durable to at least `durable`, so `try_ack(durable)` below cannot
    // refuse here — the guard inside `try_ack` does no live work on THIS path. It stays
    // as protection not for this place, but for a future caller that calls `try_ack`
    // having skipped the durable mark.
    state.tracker.note_durable(durable);
    state.tracker.try_ack(durable)?;
    let acked = state.tracker.acked();

    // The only write site for this position: both the group
    // barrier and the keepalive advance go through this common tail, so neither of them
    // will grow a second write site.
    metrics.set_last_acknowledged_lsn(acked.0);

    stream.shared_lsn_feedback.update_flushed_lsn(acked.0);
    stream.shared_lsn_feedback.update_applied_lsn(acked.0);

    // Commitment Q25(3): without an explicit call the acknowledgement goes out
    // with an 18–22 s delay, on the crate's internal schedule.
    stream
        .send_feedback()
        .await
        .map_err(|e| PgcdcError::Connection(format!("send_feedback: {e}")))?;

    Ok(acked)
}

/// Carries what the sink has accepted through the barrier and, if there was anything to
/// acknowledge, runs the result through the common tail `acknowledge_durable`. Shared
/// code for the group timer and for shutdown on a signal: without extracting it into a
/// separate function these two places would drift apart, and the mutation coverage taken
/// against the timer branch would not protect the second copy.
async fn flush_and_acknowledge(
    sink: &mut Box<dyn Sink>,
    state: &mut SessionState,
    stream: &mut LogicalReplicationStream,
    metrics: &Arc<Metrics>,
) -> Result<(), PgcdcError> {
    // Only a successful barrier may mark durable, not the acceptance of a write.
    if let Some(durable) = sink.flush().await? {
        let acked = acknowledge_durable(state, stream, durable, metrics).await?;
        debug!(lsn = %acked, "group_acknowledged");
    }
    Ok(())
}

pub async fn run(
    config: Config,
    mut sink: Box<dyn Sink>,
    metrics: Arc<Metrics>,
) -> Result<(), PgcdcError> {
    // First of all — before any connection and any log where the string could surface.
    config.database_url.validate()?;
    config.validate_reconnect_bounds()?;

    let mut state = SessionState::new(config.max_transaction_events);
    let mut backoff = ReconnectBackoff::new(
        Duration::from_millis(config.reconnect_initial_ms),
        Duration::from_millis(config.reconnect_max_ms),
    );
    let mut attempt: u32 = 0;

    // Lives NEXT TO `backoff`, not inside `SessionState`: `reset_for_reconnect` is
    // called on EVERY connection drop regardless of whether that drop was the busy race
    // — if the patience lived there, it would be zeroed on every attempt and could never
    // accumulate enough time to fire at all (SlotBusyPatience).
    let mut slot_busy_patience = SlotBusyPatience::new();

    // The flag is created once BEFORE the outer loop and handed to every session by the
    // same reference: if it were created anew on every reconnect, the process would stop
    // reacting to the signal after the first drop.
    let shutdown = spawn_shutdown_listener();

    // By the same device as `shutdown`: the countdown to the metrics report is created
    // once BEFORE the outer loop and handed to every session by the same reference. The
    // counters the report prints are process-wide and survive a reconnect; if the
    // countdown were started anew inside `stream_once` for each session, a process
    // reconnecting more often than `METRICS_REPORT_INTERVAL` would never live long
    // enough inside a single session for the report to come out at all — and that is
    // exactly the situation where it is needed most, because
    // `reconnects`/`errors` in the line exist for its sake.
    let mut last_report = tokio::time::Instant::now();

    loop {
        // The first of two places where the outer reconnect loop looks at the
        // shutdown flag (the second is the sliced backoff pause just below). The
        // slicing checks the flag BEFORE each chunk and never once AFTER the last
        // one — a signal landing in exactly that last chunk of the pause never
        // reaches it. This check is the place that catches it: without it such a
        // signal costs one extra, time-unbounded connection attempt (`stream_once`
        // will go through the pre-flight check again) — against a refused port that
        // is instant, while against an address that does not answer at all it
        // stretches to the length of the connection timeout (see
        // `spawn_shutdown_listener`). The same check also catches a signal that
        // arrived before the very first attempt — before the loop has ever been
        // inside `stream_once`.
        //
        // A signal in the outer loop. We exit with zero, but NOT because there is
        // nothing to carry through — after a drop in the middle of the acknowledgement
        // window the writer's buffer may well hold an accepted but unflushed
        // transaction, and this path skips the flush that the in-session branch
        // performs. Zero is correct for a different reason: what was not carried through
        // the barrier was not acknowledged either, so the slot will hand it over again,
        // and duplicates are permitted by invariant 2. There is nothing to lose here,
        // and that is not the same thing as "there is nothing to carry through".
        if shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }

        let acked_before = state.tracker.acked();

        match stream_once(
            &config,
            &mut sink,
            &mut state,
            &shutdown,
            &metrics,
            &mut last_report,
            &mut slot_busy_patience,
        )
        .await
        {
            Ok(SessionOutcome::ShutdownRequested) => return Ok(()),
            Ok(SessionOutcome::Disconnected) => {}
            // Recoverable errors lead into a reconnect, fatal ones out.
            // The classification lives in the type (`is_fatal`), not in parsing text.
            Err(e) if !e.is_fatal() => {
                warn!(error = %e, error_kind = e.kind(), "postgres_connection_lost");
                metrics.add_error();
            }
            Err(e) => return Err(e),
        }

        // The productivity flag is pulled out into `session_was_productive`: the
        // decision about what counts as productivity reads acked, not received, and
        // that read is pinned by a unit test at the level of the function itself, not
        // only indirectly through an integration scenario.
        let productive = session_was_productive(&state.tracker, acked_before);
        if productive {
            attempt = 0;
        }
        attempt += 1;
        let delay = backoff.next_delay(productive);
        metrics.add_reconnect();
        warn!(
            retry = attempt,
            backoff_ms = delay.as_millis() as u64,
            "reconnecting"
        );

        // The second of the two places (the first is the check at the start of
        // the pass above). The pause is sliced into chunks of SHUTDOWN_POLL_INTERVAL
        // instead of one sleep(delay) — otherwise a signal arriving in the middle of
        // a pause of up to reconnect_max_ms (30s by default) would be noticed only
        // once it ran out. As in the check above, zero here is correct not because
        // there is nothing to carry through, but because what was not carried
        // through the barrier was not acknowledged — the slot will hand it over
        // again, and duplicates are permitted by invariant 2.
        let mut remaining = delay;
        while remaining > Duration::ZERO {
            if shutdown.load(Ordering::Relaxed) {
                return Ok(());
            }
            let chunk = remaining.min(SHUTDOWN_POLL_INTERVAL);
            tokio::time::sleep(chunk).await;
            remaining = remaining.saturating_sub(chunk);
        }

        // The cache and the assembler are reset, the positions are carried forward, the
        // buffer gauge is zeroed along with them.
        state.reset_for_reconnect(&metrics);
    }
}

/// The upper bound on waiting inside a read — and thereby the upper bound on the delay
/// in reacting to the shutdown flag. Do NOT tie it to `ack_interval_ms`: that one sets
/// the barrier schedule and must not dictate how fast the process notices a signal.
/// Previously the read was bounded by `ack_interval` itself, so the flag was checked no
/// more often than the periodic barrier — with a production setting of several seconds
/// that made the delay of an orderly stop equal to the length of the acknowledgement
/// interval, and a supervisor with a short grace period killed the process before it
/// even noticed the signal. At the default value
/// (`ack_interval_ms = 200`) the loop woke up at this frequency anyway — the constant
/// costs nothing extra, it only unties the wake-up frequency from the barrier period.
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// How often the metrics report line comes out. Not configurable: this is not behavior
/// but volume, and ten seconds is the compromise between "you can see the process is
/// alive" and "the log does not get flooded" (DECISIONS Q23).
const METRICS_REPORT_INTERVAL: Duration = Duration::from_secs(10);

/// One replication session: pre-flight check, connect, loop. Returns on a connection
/// drop or on an orderly shutdown.
async fn stream_once(
    config: &Config,
    sink: &mut Box<dyn Sink>,
    state: &mut SessionState,
    shutdown: &Arc<AtomicBool>,
    metrics: &Arc<Metrics>,
    last_report: &mut tokio::time::Instant,
    slot_busy_patience: &mut SlotBusyPatience,
) -> Result<SessionOutcome, PgcdcError> {
    // Captured BEFORE the pre-flight check rather than re-reading `state.durable()`
    // later: the "this is a reconnect" decision is made on entry to the function and
    // must not quietly adjust itself to whatever happens further inside it.
    let reconnecting = is_reconnect(state.durable());

    // Commitment Q25(1): the guard goes BEFORE start(), because start() unconditionally
    // calls ensure_replication_slot() and, with the slot missing, will silently create a
    // new one at the current WAL position, losing everything committed earlier.
    // Wrapped in interrupt_patience_on_early_failure rather than a bare `?` — a
    // failure here physically cannot be the busy race (that code comes back only in
    // answer to START_REPLICATION further on), so it MUST break the chain of consecutive
    // observations instead of leaving its clock ticking while the server is unreachable
    // for an entirely different reason.
    let info_slot = interrupt_patience_on_early_failure(
        preflight_slot(config.database_url.expose(), &config.slot).await,
        slot_busy_patience,
    )?;
    if slot_health_is_terminal(info_slot.wal_status.as_deref()) {
        // Wrapped for the same reason as the preflight call just above: this refusal is
        // read from wal_status a moment earlier, not from START_REPLICATION, so it
        // physically cannot be the busy race either, and must break the chain of
        // consecutive observations like every other early failure. No observable effect
        // today — SlotUnusable is fatal, the process exits before the counter is ever
        // read again — but interrupt_patience_on_early_failure's own doc comment claims
        // to cover ANY pre-classification stream_once failure, and leaving this one out
        // would make that claim false for a reader who trusts it without re-checking the
        // code.
        return interrupt_patience_on_early_failure(
            Err(PgcdcError::SlotUnusable {
                slot: config.slot.clone(),
                reason: "PostgreSQL reports wal_status = 'lost': the WAL this slot \
                         needed has been removed, so it can never stream again"
                    .to_owned(),
            }),
            slot_busy_patience,
        );
    }
    info!(
        slot = %config.slot,
        restart_lsn = ?info_slot.restart_lsn.map(|l| l.to_string()),
        confirmed_flush_lsn = ?info_slot.confirmed_flush_lsn.map(|l| l.to_string()),
        wal_status = ?info_slot.wal_status,
        safe_wal_size = ?info_slot.safe_wal_size,
        catalog_xmin = ?info_slot.catalog_xmin,
        active = info_slot.active,
        "slot_preflight_ok"
    );

    // The reconnect check: on a cold start there is nothing to compare with, durable is
    // still zero. On a repeat connection the position is in memory and the check costs
    // nothing. A slot AHEAD of our durable point means someone acknowledged WAL that we
    // did not carry through to the sink — we die. A slot BEHIND is the expected outcome
    // of a drop: the last feedback may not have arrived. We log a warning and continue,
    // the gap is re-read as duplicates — this is permitted by invariant 2 (DECISIONS §1)
    // together with the line of the spike's transport commitments (DECISIONS Q25).
    if reconnecting {
        // By the same device as the pre-flight check above — the reconnect check
        // cannot return the busy race, only SlotAhead or nothing.
        if let Some(warning) = interrupt_patience_on_early_failure(
            check_reconnect(&config.slot, &info_slot, state.durable()),
            slot_busy_patience,
        )? {
            warn!("{warning}");
        }
    }

    let stream_config = ReplicationStreamConfig::new(
        config.slot.clone(),
        config.publication.clone(),
        1,
        StreamingMode::Off,
        Duration::from_secs(10),
        Duration::from_secs(30),
        Duration::from_secs(60),
        RetryConfig::default(),
    )
    // Our decoder understands text values only (pgoutput.rs) and is not subscribed to
    // pg_logical_emit_message — both are already off by the crate's defaults, but we pin
    // it explicitly here instead of relying on them silently.
    .with_binary(false)
    .with_messages(false);

    let url = replication_url(config.database_url.expose());
    // By the same device — opening the TCP connection cannot return the busy race
    // either, it comes back only in answer to START_REPLICATION itself.
    let mut stream = interrupt_patience_on_early_failure(
        LogicalReplicationStream::new(&url, stream_config)
            .await
            .map_err(|e| PgcdcError::Connection(format!("open replication stream: {e}"))),
        slot_busy_patience,
    )?;

    // start_lsn = None means 0/0: the server takes the slot's confirmed_flush_lsn.
    // The slot is the single source of truth (DECISIONS Q4, Q19).
    //
    // classify_start_outcome wraps classify_start_error in a patience budget for a busy
    // slot (SlotBusyPatience): the busy race by itself stays recoverable, but if it
    // drags on longer than `slot_busy_budget_ms` in total, that escalates to a fatal
    // SlotBusyTimedOut — the only signal telling a slot held forever by a foreign
    // consumer from an instantly resolving race with our own prior session is DURATION,
    // not the error code.
    classify_start_outcome(
        &config.slot,
        stream.start(None).await,
        slot_busy_patience,
        Duration::from_millis(config.slot_busy_budget_ms),
        Instant::now(),
    )?;
    info!(slot = %config.slot, publication = %config.publication, "replication_started");

    if reconnecting {
        // Only now: the stream is genuinely open and started by the server. Logging this
        // right after the slot check would mean declaring recovery before the server
        // confirmed it — on an unstable server the log would promise a recovery
        // immediately followed by another drop.
        info!(slot = %config.slot, "postgres_connection_restored");
    }

    if sink.durability() == crate::sink::Durability::BestEffort {
        warn!(
            "sink is best-effort, not durable: acknowledged positions may outlive unwritten output"
        );
    }

    let cancel = CancellationToken::new();

    // A barrier on every transaction means an fsync per transaction — a ceiling of the
    // order of a hundred transactions per second. We group by timer without touching the
    // order of operations within one pass: sink, then barrier, then durable, only then
    // ack, only then feedback.
    let ack_interval = Duration::from_millis(config.ack_interval_ms);
    let mut last_flush = tokio::time::Instant::now();

    loop {
        if shutdown.load(Ordering::Relaxed) {
            // Carry what was accepted through the barrier and acknowledge it before
            // exiting. Exiting earlier would mean losing already accepted transactions.
            flush_and_acknowledge(sink, state, &mut stream, metrics).await?;
            info!("shutdown_requested");
            return Ok(SessionOutcome::ShutdownRequested);
        }

        // The metrics report at INFO once per METRICS_REPORT_INTERVAL — not on every
        // event (that is at DEBUG, below). It sits at the start of a loop turn, outside
        // the order write→processed→(timer)barrier→durable→ack→feedback, because it only
        // reads a snapshot and affects nothing (§16, DECISIONS Q23).
        if last_report.elapsed() >= METRICS_REPORT_INTERVAL {
            *last_report = tokio::time::Instant::now();
            let s = metrics.snapshot();
            info!(
                events = s.events_total,
                transactions = s.transactions_total,
                bytes = s.bytes_received_total,
                reconnects = s.reconnects_total,
                errors = s.errors_total,
                last_received_lsn = %Lsn(s.last_received_lsn),
                last_acknowledged_lsn = %Lsn(s.last_acknowledged_lsn),
                buffer = s.transaction_buffer_size,
                "metrics_report"
            );
        }

        // A bounded read is safe here because production runs on the multi-threaded
        // runtime: the transport picks the Inline driver by the runtime flavor, and its
        // read buffer lives on the connection, not in the dropped future — a cancelled
        // read does not lose a frame that has been read but not handed over (verified
        // against the crate's source, docs/spike-findings.md, "Workaround 6"). The
        // behavior of the single-threaded runtime's driver under the same future drop is
        // NOT established; the integration tests carry `flavor = "multi_thread"` not
        // because frame loss has been proven there, but on the general principle: a test
        // MUST drive the same driver as production.
        //
        // ONLY next_raw_event is permitted: the other five APIs lead into
        // recover_connection, which restarts from last_received_lsn — the received
        // position, not the durable one (Q25(2)).
        //
        // The timeout is SHUTDOWN_POLL_INTERVAL, not ack_interval: the barrier gathers
        // events on its own schedule (the elapsed check below), while this bound exists
        // only so that the shutdown flag is not slept through for longer than it should
        // be.
        let read =
            tokio::time::timeout(SHUTDOWN_POLL_INTERVAL, stream.next_raw_event(&cancel)).await;

        match read {
            Ok(Ok(raw)) => {
                state.tracker.note_received(Lsn(raw.wal_end.0));
                metrics.add_bytes(raw.data.len() as u64);
                metrics.set_last_received_lsn(raw.wal_end.0);

                let msg = decode(&raw.data)?;
                // The buffer length is captured BEFORE
                // `?`, not merely independently of a Some/None result — otherwise an
                // error inside `handle` skips the gauge update entirely, and it stays
                // at the last value from the previous frame.
                let handled = state
                    .assembler
                    .handle(msg, Lsn(raw.wal_start.0), &mut state.cache);
                metrics.set_transaction_buffer_size(state.assembler.len() as u64);
                if let Some(tx) = handled? {
                    let changes = tx.changes.len();
                    let end_lsn = tx.end_lsn;

                    // The order MUST NOT change: sink first, then barrier, then durable, only then ack.
                    sink.write_transaction(&tx).await?;
                    state.tracker.note_processed(end_lsn);
                    metrics.add_transaction();
                    metrics.add_events(changes as u64);
                    debug!(xid = tx.xid, changes, lsn = %end_lsn, "transaction_accepted");
                }
            }
            Ok(Err(e)) => {
                warn!(error = %e, "postgres_connection_lost");
                return Ok(SessionOutcome::Disconnected);
            }
            // A tick: there was nothing to read. Not an error — a reason to reach the barrier.
            Err(_elapsed) => {}
        }

        if last_flush.elapsed() >= ack_interval {
            last_flush = tokio::time::Instant::now();
            flush_and_acknowledge(sink, state, &mut stream, metrics).await?;
        }

        // The keepalive advance: if we owe the sink nothing, the whole position the
        // server has already handed over is vacuously durable — it held not a single row
        // of our publication. We mark that explicitly instead of weakening try_ack
        // (DECISIONS Q26b).
        let server_lsn = Lsn(stream.current_lsn());
        if may_advance_from_keepalive(
            state.assembler.is_empty(),
            state.tracker.processed(),
            state.tracker.durable(),
        ) && server_lsn > state.tracker.acked()
        {
            let acked = acknowledge_durable(state, &mut stream, server_lsn, metrics).await?;
            debug!(lsn = %acked, "advanced_from_keepalive");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_replication_rejected_by_the_server_is_fatal_wrong_plugin() {
        // The cheap branch of SlotUnusable: the slot carries a foreign output plugin, the server
        // answers "option \"proto_version\" = \"1\" is unknown" (SQLSTATE 22023), and
        // pg_walstream wraps that in Protocol (the non-transient variant). The string
        // is constructed as a real one: "(SQLSTATE 22023)" at the tail is exactly what
        // the transport crate's PgErrorFields::Display puts there
        // (connection/native/error.rs), not a synthetic simplification.
        let e = ReplicationError::protocol(
            "START_REPLICATION did not enter COPY mode: ERROR:  option \"proto_version\" = \"1\" is unknown (SQLSTATE 22023)",
        );
        let err = classify_start_error("pgcdc_slot", e);
        assert!(matches!(err, PgcdcError::SlotUnusable { .. }), "{err:?}");
        assert!(err.is_fatal());
    }

    #[test]
    fn start_replication_rejected_by_the_server_is_fatal_invalidated_slot() {
        // The expensive branch of SlotUnusable: the slot is invalidated by exceeding
        // max_slot_wal_keep_size, the server answers SQLSTATE 55000 (reproduced
        // verbatim by a live run during the stage 5 review). The same Protocol
        // envelope, the same verdict.
        let e = ReplicationError::protocol(
            "START_REPLICATION did not enter COPY mode: ERROR:  can no longer get changes from replication slot \"pgcdc_slot\" (SQLSTATE 55000)",
        );
        let err = classify_start_error("pgcdc_slot", e);
        assert!(matches!(err, PgcdcError::SlotUnusable { .. }), "{err:?}");
        assert!(err.is_fatal());
    }

    #[test]
    fn start_replication_transport_drop_stays_recoverable() {
        // A connection drop (the socket, not a server answer) MUST stay recoverable —
        // otherwise the reconnect tests would go red under the mutation
        // "make a transport drop fatal".
        let e = ReplicationError::transient_connection("connection reset by peer");
        let err = classify_start_error("pgcdc_slot", e);
        assert!(matches!(err, PgcdcError::Connection(_)), "{err:?}");
        assert!(!err.is_fatal());
    }

    #[test]
    fn start_replication_slot_still_held_by_our_own_prior_session_stays_recoverable() {
        // The server ANSWERED here too, but this is not about the slot being unusable —
        // the previous walsender has not released it yet. It resolves by itself on the
        // next attempt. SQLSTATE 55006 at the tail of the string is the real code of the
        // race (ERRCODE_OBJECT_IN_USE); the distinction MUST rest on it and not on the
        // substring "is active for PID": without it
        // in the string this test would exercise only the fallback path.
        let e = ReplicationError::protocol(
            "START_REPLICATION did not enter COPY mode: ERROR:  replication slot \"pgcdc_slot\" is active for PID 4242 (SQLSTATE 55006)",
        );
        let err = classify_start_error("pgcdc_slot", e);
        assert!(matches!(err, PgcdcError::Connection(_)), "{err:?}");
        assert!(!err.is_fatal());
    }

    #[test]
    fn start_replication_slot_busy_race_is_recognized_without_sqlstate_via_fallback() {
        // The fallback path: the string carries no SQLSTATE at all (a hypothetical
        // future version of the crate changed the formatting, or the error did not come
        // through PgErrorFields). The substring remains the fallback condition — and it
        // is exactly that condition which MUST fire here.
        let e = ReplicationError::protocol(
            "START_REPLICATION did not enter COPY mode: ERROR:  replication slot \"pgcdc_slot\" is active for PID 4242",
        );
        let err = classify_start_error("pgcdc_slot", e);
        assert!(matches!(err, PgcdcError::Connection(_)), "{err:?}");
        assert!(!err.is_fatal());
    }

    #[test]
    fn start_replication_wrong_sqlstate_with_the_race_substring_is_still_fatal() {
        // When a SQLSTATE is present but does not match the race (55006), it MUST
        // decide — even if by coincidence the substring "is active for PID" also turned
        // up somewhere further along the message (in a DETAIL, say). We check that the
        // primary path does not let the fallback path override it.
        let e = ReplicationError::protocol(
            "START_REPLICATION did not enter COPY mode: ERROR:  can no longer get changes from replication slot \"pgcdc_slot\" (SQLSTATE 55000)\nDETAIL: another slot is active for PID 4242 elsewhere",
        );
        let err = classify_start_error("pgcdc_slot", e);
        assert!(matches!(err, PgcdcError::SlotUnusable { .. }), "{err:?}");
        assert!(err.is_fatal());
    }

    /// Builds the very same busy-race error as a live run against a real Postgres (see
    /// `start_replication_slot_still_held_by_our_own_prior_session_stays_recoverable`
    /// above); the live reproduction was done during the stage 5 review.
    fn busy_race_error() -> ReplicationError {
        ReplicationError::protocol(
            "START_REPLICATION did not enter COPY mode: ERROR:  replication slot \"pgcdc_slot\" is active for PID 4242 (SQLSTATE 55006)",
        )
    }

    #[test]
    fn slot_busy_patience_does_not_trigger_before_the_budget_elapses() {
        let mut p = SlotBusyPatience::new();
        let t0 = Instant::now();
        let budget = Duration::from_millis(1000);
        assert!(p.observe_busy(t0, budget).is_none());
        assert!(p
            .observe_busy(t0 + Duration::from_millis(999), budget)
            .is_none());
    }

    #[test]
    fn slot_busy_patience_triggers_once_the_budget_elapses() {
        let mut p = SlotBusyPatience::new();
        let t0 = Instant::now();
        let budget = Duration::from_millis(1000);
        assert!(p.observe_busy(t0, budget).is_none());
        let waited = p
            .observe_busy(t0 + Duration::from_millis(1000), budget)
            .expect("the budget is spent");
        assert_eq!(waited, Duration::from_millis(1000));
    }

    #[test]
    fn slot_busy_patience_reset_forgets_the_accumulated_duration() {
        let mut p = SlotBusyPatience::new();
        let t0 = Instant::now();
        let budget = Duration::from_millis(1000);
        assert!(p.observe_busy(t0, budget).is_none());
        p.reset();
        // Without the reset this observation (exactly at the budget boundary from t0)
        // would fire.
        assert!(p
            .observe_busy(t0 + Duration::from_millis(1000), budget)
            .is_none());
    }

    #[test]
    fn classify_start_outcome_stays_recoverable_while_the_busy_race_is_within_budget() {
        let mut patience = SlotBusyPatience::new();
        let budget = Duration::from_millis(1000);
        let t0 = Instant::now();
        let err = classify_start_outcome(
            "pgcdc_slot",
            Err(busy_race_error()),
            &mut patience,
            budget,
            t0,
        )
        .unwrap_err();
        assert!(matches!(err, PgcdcError::Connection(_)), "{err:?}");
        assert!(!err.is_fatal());
    }

    #[test]
    fn classify_start_outcome_escalates_to_fatal_once_the_busy_race_outlives_the_budget() {
        // Exactly what the live run during the stage 5 review saw: 34 cycles of
        // SQLSTATE 55006 in a row without a single non-zero exit code. Here
        // is the same observed series of errors, but stretched over time beyond the
        // budget — the escalation MUST fire.
        let mut patience = SlotBusyPatience::new();
        let budget = Duration::from_millis(1000);
        let t0 = Instant::now();
        let err = classify_start_outcome(
            "pgcdc_slot",
            Err(busy_race_error()),
            &mut patience,
            budget,
            t0,
        )
        .unwrap_err();
        assert!(!err.is_fatal(), "the first observation must not be fatal");

        let err = classify_start_outcome(
            "pgcdc_slot",
            Err(busy_race_error()),
            &mut patience,
            budget,
            t0 + Duration::from_millis(1500),
        )
        .unwrap_err();
        assert!(
            matches!(err, PgcdcError::SlotBusyTimedOut { .. }),
            "{err:?}"
        );
        assert!(err.is_fatal());
    }

    #[test]
    fn a_successful_start_resets_the_slot_busy_patience_so_unrelated_episodes_dont_sum() {
        // A dedicated test for the requirement "the patience counter MUST be reset on a
        // successful session start": without the reset in the `Ok` branch of
        // `classify_start_outcome` the episode from the first observation would keep
        // accumulating past a successful session too, and a second episode, in no way
        // related to the first, would add up with it into a single fatal exit.
        let mut patience = SlotBusyPatience::new();
        let budget = Duration::from_millis(1000);
        let t0 = Instant::now();

        // Episode 1: one observation of the race, the budget is not spent yet.
        let err = classify_start_outcome(
            "pgcdc_slot",
            Err(busy_race_error()),
            &mut patience,
            budget,
            t0,
        )
        .unwrap_err();
        assert!(!err.is_fatal());

        // The session starts successfully 900ms later — it MUST close episode 1.
        classify_start_outcome(
            "pgcdc_slot",
            Ok(()),
            &mut patience,
            budget,
            t0 + Duration::from_millis(900),
        )
        .expect("a successful start cannot be an error");

        // Episode 2 begins 1800ms after t0 — that is, 900ms after the reset. Without
        // the reset the chain would have inherited the previous observation (t0) and
        // would already be past the budget (1800ms >= 1000ms). With the reset this is a
        // new, independent episode, still far from the budget.
        let err = classify_start_outcome(
            "pgcdc_slot",
            Err(busy_race_error()),
            &mut patience,
            budget,
            t0 + Duration::from_millis(1800),
        )
        .unwrap_err();
        assert!(
            matches!(err, PgcdcError::Connection(_)),
            "the reset MUST have started episode 2 anew: {err:?}"
        );
        assert!(!err.is_fatal());
    }

    #[test]
    fn a_non_busy_failure_inside_classify_start_outcome_interrupts_without_summing() {
        // Scenario: "an unrelated failure between two races does not sum". Originally
        // this was a test for "reset ONLY in the Ok branch", but a full reset on any
        // failure of a different nature was
        // itself excessive — it closed the episode entirely instead of only subtracting
        // the idle interval itself. The numbers here are chosen to check exactly the
        // subtraction: the failure happens 900ms after the first observation, and the
        // second observation of the race another 900ms after the failure. The correct
        // answer "not fatal" comes out BOTH ways (a full reset and the subtraction of
        // the idle interval) with these numbers — the separate check that it is the
        // subtraction and not the reset is below
        // (`slot_busy_patience_escalates_despite_a_periodic_unrelated_failure`,
        // `classify_start_outcome_still_escalates_when_one_unrelated_failure_interleaves`).
        let mut patience = SlotBusyPatience::new();
        let budget = Duration::from_millis(1000);
        let t0 = Instant::now();

        // Episode 1: one observation of the race, the budget is still far off.
        let err = classify_start_outcome(
            "pgcdc_slot",
            Err(busy_race_error()),
            &mut patience,
            budget,
            t0,
        )
        .unwrap_err();
        assert!(!err.is_fatal());

        // A failure of a different nature 900ms later — NOT the busy race. It MUST
        // break the chain (not close the episode entirely) so that the next observation
        // of the race does not charge itself with the interval since this moment.
        let err = classify_start_outcome(
            "pgcdc_slot",
            Err(ReplicationError::transient_connection(
                "connection reset by peer",
            )),
            &mut patience,
            budget,
            t0 + Duration::from_millis(900),
        )
        .unwrap_err();
        assert!(
            !err.is_fatal(),
            "a failure of a different nature is not itself fatal"
        );

        // An observation of the race 1800ms after t0 (900ms after the break): without
        // the break the chain would have inherited last_busy = t0 and accumulated
        // 1800ms — already past the budget (1000ms). With the break the interval from
        // t0 to the failure does not go into the accumulated total at all — there is
        // nothing to accumulate, this is effectively the first observation of a new
        // chain.
        let err = classify_start_outcome(
            "pgcdc_slot",
            Err(busy_race_error()),
            &mut patience,
            budget,
            t0 + Duration::from_millis(1800),
        )
        .unwrap_err();
        assert!(
            matches!(err, PgcdcError::Connection(_)),
            "the break MUST have started the chain anew: {err:?}"
        );
        assert!(!err.is_fatal());
    }

    #[test]
    fn interrupt_patience_on_early_failure_breaks_the_chain_on_err() {
        let mut patience = SlotBusyPatience::new();
        let budget = Duration::from_millis(1000);
        let t0 = Instant::now();
        assert!(patience.observe_busy(t0, budget).is_none(), "episode open");

        let unreachable: Result<(), PgcdcError> =
            Err(PgcdcError::Connection("preflight connect: refused".into()));
        interrupt_patience_on_early_failure(unreachable, &mut patience).unwrap_err();

        // Without the break this observation (exactly at the budget boundary from t0)
        // would fire — by the same device as `SlotBusyPatience::interrupt`.
        assert!(patience
            .observe_busy(t0 + Duration::from_millis(1000), budget)
            .is_none());
    }

    #[test]
    fn interrupt_patience_on_early_failure_leaves_the_chain_alone_on_ok() {
        // Success of the pre-flight check/the reconnect check/opening the connection
        // does not yet mean the session started — touching the patience here on Ok
        // would be wrong: only classify_start_outcome makes the "the start succeeded"
        // decision.
        let mut patience = SlotBusyPatience::new();
        let budget = Duration::from_millis(1000);
        let t0 = Instant::now();
        assert!(patience.observe_busy(t0, budget).is_none());

        interrupt_patience_on_early_failure(Ok(()), &mut patience).unwrap();

        let waited = patience
            .observe_busy(t0 + Duration::from_millis(1000), budget)
            .expect("the chain MUST have stayed unbroken");
        assert_eq!(waited, Duration::from_millis(1000));
    }

    #[test]
    fn a_busy_episode_does_not_survive_an_unrelated_pre_start_failure_in_between() {
        // Reproduces the scenario: the busy race at moment zero;
        // then the server is unreachable — every attempt fails EARLIER than it gets to
        // classify_start_outcome (the slot pre-flight check or opening the connection,
        // not the answer to START_REPLICATION); then the server came back, and our own
        // former walsender holds the slot for another 76ms (the measured median, see
        // SlotBusyPatience). The second episode must not inherit the first one's clock
        // — there must be no summing.
        //
        // It is telling that the break (and not a full reset) gives the same answer
        // here: the interval BETWEEN the last observation of the race (t0) and the next
        // one (t0+5076ms) is discarded ENTIRELY, not only the part of it after the
        // failure itself (t0+5000ms) — we do not know what happened in the interval
        // [t0; t0+5000ms), it could have been exactly the same idle interval.
        let mut patience = SlotBusyPatience::new();
        let budget = Duration::from_millis(1000);
        let t0 = Instant::now();

        // Episode 1: the busy race at moment zero, the budget is still far off.
        let err = classify_start_outcome(
            "pgcdc_slot",
            Err(busy_race_error()),
            &mut patience,
            budget,
            t0,
        )
        .unwrap_err();
        assert!(!err.is_fatal());

        // The server is unreachable: a failure before the start is classified (the
        // pre-flight check/opening the connection), 5 seconds later — the same thing
        // that stands on stream_once's path BEFORE classify_start_outcome.
        let unreachable: Result<(), PgcdcError> =
            Err(PgcdcError::Connection("preflight connect: refused".into()));
        interrupt_patience_on_early_failure(unreachable, &mut patience).unwrap_err();

        // Episode 2: the server answers with the busy race again another 76ms later. In
        // total 5076ms would have passed since t0 — far past the budget, and WITHOUT
        // the break this observation would escalate to SlotBusyTimedOut wrongly: a
        // process that would have recovered on the next attempt would die.
        let err = classify_start_outcome(
            "pgcdc_slot",
            Err(busy_race_error()),
            &mut patience,
            budget,
            t0 + Duration::from_millis(5000) + Duration::from_millis(76),
        )
        .unwrap_err();
        assert!(
            matches!(err, PgcdcError::Connection(_)),
            "the second episode must not inherit the first one's clock: {err:?}"
        );
        assert!(
            !err.is_fatal(),
            "unrelated episodes must not sum into a fatal exit"
        );
    }

    #[test]
    fn slot_busy_patience_escalates_despite_a_periodic_unrelated_failure() {
        // Scenario: "a continuously busy slot with a periodic unrelated failure
        // escalates instead of spinning forever".
        // Reproduces literally the scenario described during review: the default budget
        // (30000ms), the slot busy on every attempt EXCEPT one unrelated failure once
        // every 29 seconds, one attempt per second, a simulated hour. Under the first
        // version of this fix (a full reset on any failure of a different nature,
        // `e66f6d4`) this observation does not escalate EVEN ONCE over the hour — a
        // mutation reproducing exactly that code is filed here, in the "mutations"
        // section of the task report.
        let mut patience = SlotBusyPatience::new();
        let budget = Duration::from_millis(30_000);
        let t0 = Instant::now();
        let attempt_interval = Duration::from_secs(1);

        let mut escalated: Option<(u64, Duration)> = None;
        for second in 0u64..3600 {
            let now = t0 + attempt_interval * second as u32;
            if second != 0 && second % 29 == 0 {
                // A failure of a different nature: the pre-flight check/the reconnect
                // check/opening the connection failed for a reason other than the busy
                // race.
                patience.interrupt();
            } else if let Some(accumulated) = patience.observe_busy(now, budget) {
                escalated = Some((second, accumulated));
                break;
            }
        }

        let (second, accumulated) = escalated.expect(
            "a continuously busy slot with a periodic unrelated failure MUST \
             escalate within the hour instead of spinning forever",
        );
        // A deterministic value under this layout: the escalation happens at second 32
        // of the simulated run (28s accumulated before the first break, 2s lost to the
        // break and the interval preceding it, another 2s to reach the budget) — long
        // before the hour.
        assert_eq!(second, 32, "the escalation MUST happen at second 32");
        assert_eq!(accumulated, budget);
    }

    #[test]
    fn classify_start_outcome_still_escalates_when_one_unrelated_failure_interleaves() {
        // Complements `slot_busy_patience_escalates_despite_a_periodic_unrelated_failure`:
        // proves that the real wiring through `classify_start_outcome` (whose "a
        // failure of a different nature" branch calls `interrupt`, not `reset`)
        // escalates too, and not only the type in isolation. Six observations of the
        // race 100ms apart, one unrelated failure in the middle of the run, six more
        // observations of the race — the break costs exactly one 100ms interval
        // (discarded), not the whole run: the accumulated total still reaches the
        // 1000ms budget by the last observation.
        let mut patience = SlotBusyPatience::new();
        let budget = Duration::from_millis(1000);
        let t0 = Instant::now();

        for ms in (0..=500).step_by(100) {
            classify_start_outcome(
                "pgcdc_slot",
                Err(busy_race_error()),
                &mut patience,
                budget,
                t0 + Duration::from_millis(ms),
            )
            .unwrap_err();
        }

        // A failure of a different nature in the middle of the run — not the busy race.
        classify_start_outcome(
            "pgcdc_slot",
            Err(ReplicationError::transient_connection(
                "connection reset by peer",
            )),
            &mut patience,
            budget,
            t0 + Duration::from_millis(550),
        )
        .unwrap_err();

        let mut last_err = None;
        for ms in (600..=1100).step_by(100) {
            last_err = Some(
                classify_start_outcome(
                    "pgcdc_slot",
                    Err(busy_race_error()),
                    &mut patience,
                    budget,
                    t0 + Duration::from_millis(ms),
                )
                .unwrap_err(),
            );
        }

        let err = last_err.unwrap();
        assert!(
            matches!(
                err,
                PgcdcError::SlotBusyTimedOut {
                    waited_ms: 1000,
                    ..
                }
            ),
            "{err:?}"
        );
        assert!(err.is_fatal());
    }

    #[test]
    fn extract_sqlstate_reads_the_code_from_pg_walstreams_error_formatting() {
        // The format is confirmed by reading the crate's source
        // (connection/native/error.rs::PgErrorFields::Display) and by a live run
        // against a real Postgres during the stage 5 review.
        assert_eq!(
            extract_sqlstate(
                "ERROR:  can no longer get changes from replication slot \"s\" (SQLSTATE 55000)"
            ),
            Some("55000")
        );
    }

    #[test]
    fn extract_sqlstate_is_none_when_absent() {
        assert_eq!(extract_sqlstate("connection reset by peer"), None);
    }

    #[test]
    fn is_reconnect_is_false_on_a_cold_start() {
        assert!(!is_reconnect(Lsn(0)));
    }

    #[test]
    fn is_reconnect_is_true_once_something_is_durable() {
        assert!(is_reconnect(Lsn(0x1000)));
    }

    #[test]
    fn session_is_productive_when_acked_advances_via_keepalive_without_new_frames() {
        // The live proof of the divergence: in a quiet run
        // the metrics report showed the acknowledged position advanced with the
        // received one at zero — the keepalive acknowledged WAL without accepting a
        // single frame. The flag MUST count this session as productive, and a mutation
        // swapping acked for received inside `session_was_productive` fails this test.
        let mut t = LsnTracker::new();
        let acked_before = t.acked();
        t.note_durable(Lsn(0x1000));
        t.try_ack(Lsn(0x1000)).unwrap();
        assert_eq!(t.received(), Lsn(0), "not a single frame was received");
        assert!(session_was_productive(&t, acked_before));
    }

    #[test]
    fn session_is_not_productive_when_only_received_moves() {
        // The other side of the same divergence: a frame arrived (received moved
        // forward), but the barrier has not acknowledged it yet — by acked the session
        // is unproductive, and the flag MUST agree with acked, not with received.
        let mut t = LsnTracker::new();
        let acked_before = t.acked();
        t.note_received(Lsn(0x1000));
        assert!(!session_was_productive(&t, acked_before));
    }

    #[test]
    fn backoff_resets_to_initial_after_a_productive_session() {
        let mut b = ReconnectBackoff::new(Duration::from_millis(100), Duration::from_millis(1000));
        // Climb to the ceiling with a series of unproductive attempts.
        for _ in 0..10 {
            b.next_delay(false);
        }
        assert_eq!(
            b.next_delay(true),
            Duration::from_millis(100),
            "a productive session MUST reset the pause to the initial one"
        );
    }

    #[test]
    fn backoff_keeps_growing_across_unproductive_attempts() {
        // Closes the gap that the previous test set survived: the mutation
        // "make the reset unconditional" — `next_delay` always resets
        // `current` regardless of `productive` — left both existing backoff tests
        // green, because neither of them looks at the intermediate values when
        // `productive = false`. Under that mutation every call with
        // `productive = false` would return the initial delay (100ms) too, forever — an
        // endless hammering of a dead server every hundred milliseconds instead of an
        // exponent. This test reads exactly the intermediate values of a series of
        // unproductive attempts at the level of the type's method, not of the free
        // function `next_backoff`.
        let mut b = ReconnectBackoff::new(Duration::from_millis(100), Duration::from_millis(1000));
        assert_eq!(b.next_delay(false), Duration::from_millis(100));
        assert_eq!(b.next_delay(false), Duration::from_millis(200));
        assert_eq!(b.next_delay(false), Duration::from_millis(400));
        assert_eq!(b.next_delay(false), Duration::from_millis(800));
    }

    #[test]
    fn backoff_doubles_until_it_reaches_the_ceiling() {
        let max = Duration::from_millis(1000);
        assert_eq!(
            next_backoff(Duration::from_millis(100), max),
            Duration::from_millis(200)
        );
        assert_eq!(
            next_backoff(Duration::from_millis(400), max),
            Duration::from_millis(800)
        );
        assert_eq!(
            next_backoff(Duration::from_millis(800), max),
            max,
            "it hits the ceiling"
        );
        assert_eq!(next_backoff(max, max), max, "and stays on it");
    }

    #[test]
    fn backoff_cannot_overflow() {
        // Doubling at the very top of the range must not panic in a debug build.
        let huge = Duration::from_millis(u64::MAX / 2 + 1);
        assert_eq!(
            next_backoff(huge, Duration::from_millis(1000)),
            Duration::from_millis(1000)
        );
    }

    #[test]
    fn keepalive_advance_requires_an_empty_buffer() {
        // An open transaction means we still owe the sink part of the WAL.
        assert!(!may_advance_from_keepalive(false, Lsn(0x1000), Lsn(0x1000)));
    }

    #[test]
    fn keepalive_advance_requires_processed_to_have_caught_up() {
        // The buffer is empty, but the transaction has been accepted by the sink and not
        // yet carried through the barrier. Acknowledging a position from a keepalive
        // here means acknowledging beyond durable.
        assert!(!may_advance_from_keepalive(true, Lsn(0x2000), Lsn(0x1000)));
    }

    #[test]
    fn keepalive_advance_is_allowed_when_nothing_is_owed() {
        assert!(may_advance_from_keepalive(true, Lsn(0x1000), Lsn(0x1000)));
    }

    #[test]
    fn only_a_lost_slot_is_refused_before_streaming() {
        // `lost` is terminal: the WAL the slot needed is gone and no retry can
        // bring it back. `unreserved` is not — PostgreSQL documents that it can
        // return to `reserved` or `extended`, so refusing it would kill a
        // process that is about to recover, which is the mirror of the defect
        // Q30 fixed.
        assert!(slot_health_is_terminal(Some("lost")));
        assert!(!slot_health_is_terminal(Some("unreserved")));
        assert!(!slot_health_is_terminal(Some("extended")));
        assert!(!slot_health_is_terminal(Some("reserved")));
        // An older server, or a column we could not read, must not be treated
        // as a failure: absence of evidence is not evidence of a dead slot.
        assert!(!slot_health_is_terminal(None));
    }

    #[test]
    fn reconnect_resets_the_cache_and_the_assembler() {
        // The cache lives within one session: the server resends RELATION in the new
        // session, and the old description could have gone stale while we were away. A
        // half-assembled transaction arrives again in full — its BEGIN was after
        // confirmed_flush_lsn.
        let mut s = SessionState::new(1000);
        s.cache.put(crate::schema::Relation {
            id: 1,
            namespace: "public".into(),
            name: "users".into(),
            replica_identity: b'f',
            columns: vec![],
        });
        s.assembler
            .handle(
                crate::postgres::pgoutput::PgOutputMessage::Begin {
                    final_lsn: 0x1000,
                    commit_timestamp: 0,
                    xid: 7,
                },
                Lsn(0x100),
                &mut s.cache,
            )
            .unwrap();
        assert_eq!(s.cache.len(), 1);
        assert!(!s.assembler.is_empty());

        s.reset_for_reconnect(&Metrics::new());

        assert_eq!(s.cache.len(), 0, "the cache is reset entirely");
        assert!(
            s.assembler.is_empty(),
            "the half-assembled transaction is thrown away"
        );
    }

    #[test]
    fn reconnect_carries_the_tracker_positions_forward() {
        // The positions are NOT reset. Zeroing the tracker would mean losing the
        // durable position that check_reconnect compares the slot against — and, along
        // with it, opening the keepalive gate at a moment when the replay has not
        // caught up yet.
        let mut s = SessionState::new(1000);
        s.tracker.note_received(Lsn(0x3000));
        s.tracker.note_processed(Lsn(0x2000));
        s.tracker.note_durable(Lsn(0x2000));
        s.tracker.try_ack(Lsn(0x2000)).unwrap();

        s.reset_for_reconnect(&Metrics::new());

        assert_eq!(s.durable(), Lsn(0x2000), "durable is carried forward");
        assert_eq!(
            s.tracker.acked(),
            Lsn(0x2000),
            "the acknowledged position is carried forward"
        );
        assert_eq!(
            s.tracker.processed(),
            Lsn(0x2000),
            "processed is carried forward"
        );
    }

    #[test]
    fn replayed_transactions_cannot_move_positions_backwards() {
        // After a reconnect the server hands over everything after confirmed_flush_lsn
        // again. The positions are monotone, so reprocessing does not roll them back,
        // and the keepalive gate stays shut until the replay catches up with processed.
        let mut s = SessionState::new(1000);
        s.tracker.note_processed(Lsn(0x2000));
        s.tracker.note_durable(Lsn(0x2000));
        s.reset_for_reconnect(&Metrics::new());
        s.tracker.note_processed(Lsn(0x1000));
        assert_eq!(s.tracker.processed(), Lsn(0x2000));
    }

    #[test]
    fn reconnect_zeroes_the_buffer_gauge_even_with_an_open_transaction() {
        // The reset on a reconnect does not go through the
        // receiving branch of stream_once, where this gauge is normally set — it MUST
        // zero it itself. Without that, on a publication that goes idle after a drop the
        // gauge would hold the last non-zero value forever, instead of honestly showing
        // the empty buffer of the new session.
        let mut s = SessionState::new(1000);
        s.assembler
            .handle(
                crate::postgres::pgoutput::PgOutputMessage::Begin {
                    final_lsn: 0x1000,
                    commit_timestamp: 0,
                    xid: 7,
                },
                Lsn(0x100),
                &mut s.cache,
            )
            .unwrap();
        let metrics = Metrics::new();
        metrics.set_transaction_buffer_size(5);

        s.reset_for_reconnect(&metrics);

        assert_eq!(
            metrics.snapshot().transaction_buffer_size,
            0,
            "the gauge MUST fall to zero along with the reset of the assembler"
        );
    }
}
