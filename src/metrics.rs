use std::sync::atomic::{AtomicU64, Ordering};

/// Счётчики процесса. Своя структура, а не фасад вроде `metrics-rs`: фасад без
/// подключённого экспортёра отправляет значения в никуда, а нам они нужны прямо
/// в тестах — «после отказа sink подтверждённая позиция не сдвинулась» это
/// утверждение о счётчике (DECISIONS Q23). Обернуть это экспортёром позже
/// тривиально; вернуть наблюдаемость фасаду — нет.
///
/// Все поля — `Relaxed`: это наблюдение, а не синхронизация. Ни одно решение
/// в коде не принимается по значению счётчика, поэтому упорядочивание между
/// ними не нужно и стоило бы дороже.
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

/// Согласованный по полям снимок. Нужен и периодической сводке, и тестам:
/// читать восемь атомиков по отдельности в ассерте — значит получить
/// значения из разных моментов.
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

    /// Позиции монотонны по той же причине, что и в трекере: replay уже
    /// обработанного не должен откатывать наблюдаемый прогресс.
    pub fn set_last_received_lsn(&self, lsn: u64) {
        self.last_received_lsn.fetch_max(lsn, Ordering::Relaxed);
    }

    pub fn set_last_acknowledged_lsn(&self, lsn: u64) {
        self.last_acknowledged_lsn.fetch_max(lsn, Ordering::Relaxed);
    }

    /// Размер буфера — датчик, а не позиция: он обязан падать до нуля на коммите.
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
        // Позиция — не счётчик: она заменяется, а не накапливается.
        let m = Metrics::new();
        m.set_last_acknowledged_lsn(0x1000);
        m.set_last_acknowledged_lsn(0x2000);
        assert_eq!(m.snapshot().last_acknowledged_lsn, 0x2000);
    }

    #[test]
    fn a_position_never_moves_backwards() {
        // Тот же довод, что и у трекера: replay уже обработанного не должен
        // откатывать наблюдаемую позицию, иначе график лжёт о прогрессе.
        let m = Metrics::new();
        m.set_last_acknowledged_lsn(0x2000);
        m.set_last_acknowledged_lsn(0x1000);
        assert_eq!(m.snapshot().last_acknowledged_lsn, 0x2000);
    }

    #[test]
    fn buffer_size_is_a_gauge_and_may_fall() {
        // А вот размер буфера — не позиция: он обязан падать до нуля на коммите.
        let m = Metrics::new();
        m.set_transaction_buffer_size(17);
        m.set_transaction_buffer_size(0);
        assert_eq!(m.snapshot().transaction_buffer_size, 0);
    }
}
