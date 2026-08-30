use std::time::Duration;

use pg_walstream::{
    CancellationToken, LogicalReplicationStream, ReplicationStreamConfig, RetryConfig,
    StreamingMode,
};
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::error::PgcdcError;
use crate::lsn::{Lsn, LsnTracker};
use crate::postgres::guard::preflight_cold_start;
use crate::postgres::pgoutput::decode;
use crate::schema::RelationCache;
use crate::sink::Sink;
use crate::transaction::Assembler;

/// Строка подключения для репликационного соединения требует
/// `replication=database` — без него сервер откроет обычную сессию.
fn replication_url(base: &str) -> String {
    if base.contains('?') {
        format!("{base}&replication=database")
    } else {
        format!("{base}?replication=database")
    }
}

pub async fn run(config: Config, mut sink: Box<dyn Sink>) -> Result<(), PgcdcError> {
    // Обязательство Q25(а): guard ДО start(), потому что start() безусловно
    // зовёт ensure_replication_slot() и при отсутствующем слоте молча создаст
    // новый на текущей позиции WAL, потеряв всё закоммиченное раньше.
    let info_slot = preflight_cold_start(config.database_url.expose(), &config.slot).await?;
    info!(
        slot = %config.slot,
        restart_lsn = ?info_slot.restart_lsn.map(|l| l.to_string()),
        confirmed_flush_lsn = ?info_slot.confirmed_flush_lsn.map(|l| l.to_string()),
        "slot_preflight_ok"
    );

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
    // Наш декодер понимает только текстовые значения (pgoutput.rs) и не подписан
    // на pg_logical_emit_message — оба уже выключены значениями по умолчанию
    // крейта, но фиксируем это явно здесь, а не полагаемся молча на них.
    .with_binary(false)
    .with_messages(false);

    let url = replication_url(config.database_url.expose());
    let mut stream = LogicalReplicationStream::new(&url, stream_config)
        .await
        .map_err(|e| PgcdcError::Connection(format!("open replication stream: {e}")))?;

    // start_lsn = None означает 0/0: сервер возьмёт confirmed_flush_lsn слота.
    // Слот — единственный источник истины (DECISIONS Q4, Q19).
    stream
        .start(None)
        .await
        .map_err(|e| PgcdcError::Connection(format!("start replication: {e}")))?;
    info!(slot = %config.slot, publication = %config.publication, "replication_started");

    if sink.durability() == crate::sink::Durability::BestEffort {
        warn!(
            "sink is best-effort, not durable: acknowledged positions may outlive unwritten output"
        );
    }

    let cancel = CancellationToken::new();
    let mut cache = RelationCache::new();
    let mut assembler = Assembler::new(config.max_transaction_events);
    let mut tracker = LsnTracker::new();

    loop {
        // Разрешён ТОЛЬКО next_raw_event: остальные пять API ведут в
        // recover_connection, который рестартует с last_received_lsn —
        // принятой, а не durable позиции (Q25(б)).
        let raw = stream
            .next_raw_event(&cancel)
            .await
            .map_err(|e| PgcdcError::Connection(format!("next_raw_event: {e}")))?;

        tracker.note_received(Lsn(raw.wal_end.0));

        let msg = decode(&raw.data)?;
        if let Some(tx) = assembler.handle(msg, Lsn(raw.wal_start.0), &mut cache)? {
            let changes = tx.changes.len();
            let end_lsn = tx.end_lsn;

            // Порядок нерушим: сначала sink, потом durable, только потом ack.
            sink.write_transaction(&tx).await?;
            tracker.note_durable(end_lsn);
            tracker.try_ack(end_lsn)?;

            // Подтверждаем end_lsn, НЕ commit_lsn: commit_lsn указывает на
            // начало записи коммита, и рестарт перечитал бы ту же транзакцию.
            stream.shared_lsn_feedback.update_flushed_lsn(end_lsn.0);
            stream.shared_lsn_feedback.update_applied_lsn(end_lsn.0);

            // Обязательство Q25(в): без явного вызова подтверждение уходит
            // с задержкой 18–22 с по внутреннему расписанию крейта.
            stream
                .send_feedback()
                .await
                .map_err(|e| PgcdcError::Connection(format!("send_feedback: {e}")))?;

            debug!(xid = tx.xid, changes, lsn = %end_lsn, "transaction_committed");
        }
    }
}
