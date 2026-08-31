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

/// Можно ли подтвердить позицию, пришедшую в keepalive.
///
/// «Буфер пуст» само по себе НЕ достаточно (DECISIONS Q26a). Оно было достаточным,
/// пока запись, отметка durable и подтверждение происходили в одной итерации; с
/// групповым барьером появляется окно, где открытых транзакций нет, а принятые
/// sink'ом и не доведённые до носителя данные есть. Подтвердить позицию внутри
/// этого окна — значит подтвердить сверх durable и потерять данные при крахе.
fn may_advance_from_keepalive(assembler_empty: bool, processed: Lsn, durable: Lsn) -> bool {
    assembler_empty && processed <= durable
}

pub async fn run(config: Config, mut sink: Box<dyn Sink>) -> Result<(), PgcdcError> {
    // Первым делом — до любого подключения и любого лога, где могла бы всплыть строка.
    config.database_url.validate()?;

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

    // Барьер на каждой транзакции означает fsync на каждую транзакцию —
    // потолок порядка сотни транзакций в секунду. Группируем по таймеру, не
    // трогая порядок операций внутри одного прохода: sink, потом барьер,
    // потом durable, только потом ack, только потом feedback.
    let ack_interval = Duration::from_millis(config.ack_interval_ms);
    let mut last_flush = tokio::time::Instant::now();

    loop {
        // Ограниченное чтение здесь безопасно, потому что прод работает на
        // многопоточном рантайме: транспорт выбирает Inline-драйвер по
        // флейвору рантайма, и его буфер чтения живёт на соединении, а не в
        // сброшенной future, — отменённое чтение не теряет уже прочитанный,
        // но не отданный кадр (проверено против исходника крейта,
        // docs/spike-findings.md, «Обходной путь 6»). Поведение драйвера
        // однопоточного рантайма при таком же падении future НЕ установлено;
        // интеграционные тесты несут `flavor = "multi_thread"` не потому, что
        // там доказана потеря кадров, а по общему принципу: тест обязан
        // гонять тот же драйвер, что и прод (задача 1; формулировка уточнена
        // в задаче 4, review round 1, F2).
        //
        // Разрешён ТОЛЬКО next_raw_event: остальные пять API ведут в
        // recover_connection, который рестартует с last_received_lsn —
        // принятой, а не durable позиции (Q25(б)).
        let read = tokio::time::timeout(ack_interval, stream.next_raw_event(&cancel)).await;

        match read {
            Ok(Ok(raw)) => {
                tracker.note_received(Lsn(raw.wal_end.0));

                let msg = decode(&raw.data)?;
                if let Some(tx) = assembler.handle(msg, Lsn(raw.wal_start.0), &mut cache)? {
                    let changes = tx.changes.len();
                    let end_lsn = tx.end_lsn;

                    // Порядок нерушим: сначала sink, потом барьер, потом durable, только потом ack.
                    sink.write_transaction(&tx).await?;
                    tracker.note_processed(end_lsn);
                    debug!(xid = tx.xid, changes, lsn = %end_lsn, "transaction_accepted");
                }
            }
            Ok(Err(e)) => return Err(PgcdcError::Connection(format!("next_raw_event: {e}"))),
            // Тик: читать было нечего. Не ошибка — повод дойти до барьера.
            Err(_elapsed) => {}
        }

        if last_flush.elapsed() >= ack_interval {
            last_flush = tokio::time::Instant::now();

            // Отметить durable имеет право только успешный барьер, а не приём записи.
            if let Some(durable) = sink.flush().await? {
                tracker.note_durable(durable);
                tracker.try_ack(durable)?;
                let acked = tracker.acked();

                // Отчитываемся позицией трекера (acked), а не тем, что вернул
                // барьер: сегодня они совпадают, но с реконнектом внутри
                // процесса (следующий этап) replay уже подтверждённой
                // транзакции может вернуть из flush позицию позади слота —
                // отправить её в feedback значило бы откатить сервер назад.
                // Подтверждаем acked, НЕ commit_lsn: commit_lsn указывает на
                // начало записи коммита, и рестарт перечитал бы ту же транзакцию.
                stream.shared_lsn_feedback.update_flushed_lsn(acked.0);
                stream.shared_lsn_feedback.update_applied_lsn(acked.0);

                // Обязательство Q25(в): без явного вызова подтверждение уходит
                // с задержкой 18–22 с по внутреннему расписанию крейта.
                stream
                    .send_feedback()
                    .await
                    .map_err(|e| PgcdcError::Connection(format!("send_feedback: {e}")))?;

                debug!(lsn = %acked, "group_acknowledged");
            }
        }

        // Продвижение по keepalive: если мы ничего не должны sink'у, вся позиция,
        // которую сервер уже отдал, вакуумно durable — в ней не было ни одной
        // строки нашей публикации. Отмечаем это явно, а не ослабляем try_ack
        // (DECISIONS Q26b).
        let server_lsn = Lsn(stream.current_lsn());
        if may_advance_from_keepalive(assembler.is_empty(), tracker.processed(), tracker.durable())
            && server_lsn > tracker.acked()
        {
            tracker.note_durable(server_lsn);
            tracker.try_ack(server_lsn)?;
            stream.shared_lsn_feedback.update_flushed_lsn(server_lsn.0);
            stream.shared_lsn_feedback.update_applied_lsn(server_lsn.0);
            stream
                .send_feedback()
                .await
                .map_err(|e| PgcdcError::Connection(format!("send_feedback: {e}")))?;
            debug!(lsn = %server_lsn, "advanced_from_keepalive");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keepalive_advance_requires_an_empty_buffer() {
        // Открытая транзакция означает, что часть WAL мы ещё должны sink'у.
        assert!(!may_advance_from_keepalive(false, Lsn(0x1000), Lsn(0x1000)));
    }

    #[test]
    fn keepalive_advance_requires_processed_to_have_caught_up() {
        // Буфер пуст, но транзакция принята sink'ом и ещё не доведена барьером.
        // Подтвердить позицию из keepalive здесь — значит подтвердить сверх durable.
        assert!(!may_advance_from_keepalive(true, Lsn(0x2000), Lsn(0x1000)));
    }

    #[test]
    fn keepalive_advance_is_allowed_when_nothing_is_owed() {
        assert!(may_advance_from_keepalive(true, Lsn(0x1000), Lsn(0x1000)));
    }
}
