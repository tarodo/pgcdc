use std::sync::atomic::{AtomicU64, Ordering};

/// Process counters. Our own struct, not a facade like `metrics-rs`: a facade with
/// no exporter attached sends values into the void, and we need them directly
/// in tests — "after a sink failure the acknowledged position did not move" is
/// an assertion about a counter (DECISIONS Q23). Wrapping this in an exporter later
/// is trivial; getting observability back from a facade is not.
///
/// All fields are `Relaxed`: this is observation, not synchronization. No decision
/// in the code is made based on a counter's value, so ordering between
/// them is unnecessary and would cost more.
#[derive(Debug, Default)]
pub struct Metrics {
    events_total: AtomicU64,
    transactions_total: AtomicU64,
    bytes_received_total: AtomicU64,
    reconnects_total: AtomicU64,
    errors_total: AtomicU64,
    last_received_lsn: AtomicU64,
    last_acknowledged_lsn: AtomicU64,
    transaction_buffer_size: AtomicU64,
}

/// A field-consistent snapshot. Needed both by the periodic report and by
/// tests: reading eight atomics separately in an assertion would mean getting
/// values from different moments in time.
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
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
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
}
