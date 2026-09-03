use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

/// Process counters. Our own struct, not a facade like `metrics-rs`: a facade with
/// no exporter attached sends values into the void, and we need them directly
/// in tests — "after a sink failure the acknowledged position did not move" is
/// an assertion about a counter (DECISIONS Q23). Wrapping this in an exporter later
/// is trivial; getting observability back from a facade is not.
///
/// Every *counter* is `Relaxed`: this is observation, not synchronization. No
/// decision in the code is made based on a counter's value, so ordering between
/// them is unnecessary and would cost more. `start` is the one exception: it is
/// an immutable base fixed at construction, so it needs no synchronisation at all.
#[derive(Debug)]
pub struct Metrics {
    events_total: AtomicU64,
    transactions_total: AtomicU64,
    bytes_received_total: AtomicU64,
    reconnects_total: AtomicU64,
    errors_total: AtomicU64,
    last_received_lsn: AtomicU64,
    last_acknowledged_lsn: AtomicU64,
    transaction_buffer_size: AtomicU64,
    /// Whether a replication stream is running right now. Written on the way up
    /// AND on the way down — a gauge only success updates reports health nobody
    /// observed.
    streaming: AtomicBool,
    /// Milliseconds since `start` at the last successful acknowledgement, or 0
    /// for "never". Stored as an offset rather than a wall-clock instant so the
    /// whole struct stays cheap to read from any thread.
    last_ack_at_ms: AtomicU64,
    /// The base the offset above is measured from. Immutable after construction,
    /// so it needs no synchronisation.
    start: Instant,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// A snapshot of every field `Metrics` holds, taken by `snapshot()` below as ten
/// independent `Relaxed` loads (nine `AtomicU64` fields and one `AtomicBool`) plus one
/// non-atomic `Instant::elapsed()` call used to turn the raw `last_ack_at_ms` load into
/// `seconds_since_last_ack`. This is NOT a field-consistent snapshot in the sense of all
/// ten values being read as of the same instant — each load can interleave with a
/// concurrent writer independently of the others, the same way any `Relaxed` read can.
/// What it does give both the periodic report and tests is a single `struct` to pass
/// around and assert on, instead of ten separate method calls each capable of observing
/// a different moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub events_total: u64,
    pub transactions_total: u64,
    pub bytes_received_total: u64,
    pub reconnects_total: u64,
    pub errors_total: u64,
    pub last_received_lsn: u64,
    pub last_acknowledged_lsn: u64,
    pub transaction_buffer_size: u64,
    pub streaming: bool,
    /// `None` until the first acknowledgement of this process.
    pub seconds_since_last_ack: Option<u64>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            events_total: AtomicU64::new(0),
            transactions_total: AtomicU64::new(0),
            bytes_received_total: AtomicU64::new(0),
            reconnects_total: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            last_received_lsn: AtomicU64::new(0),
            last_acknowledged_lsn: AtomicU64::new(0),
            transaction_buffer_size: AtomicU64::new(0),
            streaming: AtomicBool::new(false),
            last_ack_at_ms: AtomicU64::new(0),
            start: Instant::now(),
        }
    }

    pub fn add_events(&self, n: u64) {
        self.events_total.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_transaction(&self) {
        self.transactions_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_bytes(&self, n: u64) {
        self.bytes_received_total.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_reconnect(&self) {
        self.reconnects_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Positions are monotonic for the same reason as in the tracker: replaying
    /// what was already processed must not roll back observed progress.
    pub fn set_last_received_lsn(&self, lsn: u64) {
        self.last_received_lsn.fetch_max(lsn, Ordering::Relaxed);
    }

    pub fn set_last_acknowledged_lsn(&self, lsn: u64) {
        self.last_acknowledged_lsn.fetch_max(lsn, Ordering::Relaxed);
    }

    /// Buffer size is a gauge, not a position: it must fall to zero on commit.
    pub fn set_transaction_buffer_size(&self, n: u64) {
        self.transaction_buffer_size.store(n, Ordering::Relaxed);
    }

    pub fn set_streaming(&self, streaming: bool) {
        self.streaming.store(streaming, Ordering::Relaxed);
    }

    /// Called after a position has actually been acknowledged to the server.
    pub fn note_acknowledged_now(&self) {
        let ms = self.start.elapsed().as_millis() as u64;
        // Saturate at 1 so that "acknowledged in the first millisecond" is still
        // distinguishable from "never acknowledged", which is 0.
        self.last_ack_at_ms.store(ms.max(1), Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            events_total: self.events_total.load(Ordering::Relaxed),
            transactions_total: self.transactions_total.load(Ordering::Relaxed),
            bytes_received_total: self.bytes_received_total.load(Ordering::Relaxed),
            reconnects_total: self.reconnects_total.load(Ordering::Relaxed),
            errors_total: self.errors_total.load(Ordering::Relaxed),
            last_received_lsn: self.last_received_lsn.load(Ordering::Relaxed),
            last_acknowledged_lsn: self.last_acknowledged_lsn.load(Ordering::Relaxed),
            transaction_buffer_size: self.transaction_buffer_size.load(Ordering::Relaxed),
            streaming: self.streaming.load(Ordering::Relaxed),
            seconds_since_last_ack: match self.last_ack_at_ms.load(Ordering::Relaxed) {
                0 => None,
                at => Some((self.start.elapsed().as_millis() as u64).saturating_sub(at) / 1000),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_start_at_zero_and_accumulate() {
        let m = Metrics::new();
        assert_eq!(m.snapshot().events_total, 0);
        m.add_events(3);
        m.add_events(2);
        assert_eq!(m.snapshot().events_total, 5);
    }

    #[test]
    fn positions_are_set_not_added() {
        // A position is not a counter: it is replaced, not accumulated.
        let m = Metrics::new();
        m.set_last_acknowledged_lsn(0x1000);
        m.set_last_acknowledged_lsn(0x2000);
        assert_eq!(m.snapshot().last_acknowledged_lsn, 0x2000);
    }

    #[test]
    fn a_position_never_moves_backwards() {
        // The same argument as for the tracker: replaying what was already processed
        // must not roll back the observed position, otherwise the graph lies about progress.
        let m = Metrics::new();
        m.set_last_acknowledged_lsn(0x2000);
        m.set_last_acknowledged_lsn(0x1000);
        assert_eq!(m.snapshot().last_acknowledged_lsn, 0x2000);
    }

    #[test]
    fn buffer_size_is_a_gauge_and_may_fall() {
        // Buffer size, on the other hand, is not a position: it must fall to zero on commit.
        let m = Metrics::new();
        m.set_transaction_buffer_size(17);
        m.set_transaction_buffer_size(0);
        assert_eq!(m.snapshot().transaction_buffer_size, 0);
    }

    #[test]
    fn a_fresh_process_is_not_streaming_and_has_never_acknowledged() {
        let m = Metrics::new();
        let s = m.snapshot();
        assert!(!s.streaming, "nothing has connected yet");
        assert_eq!(
            s.seconds_since_last_ack, None,
            "never acknowledged is not the same as acknowledged zero seconds ago"
        );
    }

    #[test]
    fn the_streaming_flag_follows_failure_as_well_as_success() {
        // The whole point: the flag must be written on the way down too. A gauge
        // that only success updates reports health it has not observed.
        let m = Metrics::new();
        m.set_streaming(true);
        assert!(m.snapshot().streaming);
        m.set_streaming(false);
        assert!(!m.snapshot().streaming);
    }

    #[test]
    fn an_acknowledgement_starts_the_staleness_clock() {
        let m = Metrics::new();
        m.note_acknowledged_now();
        assert_eq!(
            m.snapshot().seconds_since_last_ack,
            Some(0),
            "an acknowledgement that just happened is zero seconds old, not None"
        );
    }

    #[test]
    fn a_sub_second_gap_does_not_round_up_to_a_whole_second() {
        // Regression: subtracting `now / 1000` from `ack / 1000` (both floored to
        // whole seconds first) instead of dividing the millisecond gap directly
        // rounds a 100ms-old acknowledgement up to a full second whenever `ack`
        // and `now` straddle a second boundary. Parking the acknowledgement just
        // before the 1s mark and reading it back just after forces that straddle.
        let m = Metrics::new();
        std::thread::sleep(std::time::Duration::from_millis(950));
        m.note_acknowledged_now();
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert_eq!(
            m.snapshot().seconds_since_last_ack,
            Some(0),
            "a 100ms-old acknowledgement must not report as a whole second stale"
        );
    }
}
