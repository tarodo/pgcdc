//! Одноразовый spike этапа 0. Выбрасывается в конце этапа 1.
//! Задача: увидеть сырые байты pgoutput и проверить, что транспорт
//! не подтверждает LSN за нашей спиной.

use std::time::Duration;

use anyhow::Result;
use pg_walstream::{
    CancellationToken, LogicalReplicationStream, RawXLogData, ReplicationStreamConfig,
    RetryConfig, StreamingMode,
};

const CONN: &str = "postgresql://postgres:postgres@localhost:5432/app?replication=database";

#[tokio::main]
async fn main() -> Result<()> {
    // proto_version = 1: без streaming незакоммиченных транзакций (DECISIONS Q13).
    let config = ReplicationStreamConfig::new(
        "pgcdc_slot".to_string(),
        "pgcdc_pub".to_string(),
        1,
        StreamingMode::Off,
        Duration::from_secs(10),
        Duration::from_secs(30),
        Duration::from_secs(60),
        RetryConfig::default(),
    );

    let mut stream = LogicalReplicationStream::new(CONN, config).await?;

    // ВАЖНО: ensure_replication_slot() НЕ вызывается. Слот должен уже существовать.
    // Автосоздание маскирует потерю данных (DECISIONS Q19).
    stream.start(None).await?;
    eprintln!("replication started, waiting for events (Ctrl-C to stop)");

    let cancel = CancellationToken::new();
    let mut seq = 0usize;

    loop {
        let raw = stream.next_raw_event(&cancel).await?;
        seq += 1;
        dump(seq, &raw);
        // Подтверждение LSN намеренно НЕ отправляется, см. Task 3.
    }
}

/// Печатает тип сообщения, позиции WAL и hex-дамп payload'а.
fn dump(seq: usize, raw: &RawXLogData) {
    // raw.data, raw.wal_start, raw.wal_end — публичные поля в pg_walstream 0.8.1,
    // не методы (см. docs/spike-findings.md, единственное разрешённое отклонение
    // от скелета брифа).
    let payload: &[u8] = &raw.data;
    let kind = payload.first().map(|b| *b as char).unwrap_or('?');
    eprintln!(
        "--- #{seq} kind={kind:?} wal_start={:?} wal_end={:?} len={}",
        raw.wal_start,
        raw.wal_end,
        payload.len()
    );
    for (i, chunk) in payload.chunks(16).enumerate() {
        let hex: Vec<String> = chunk.iter().map(|b| format!("{b:02x}")).collect();
        let ascii: String = chunk
            .iter()
            .map(|b| if b.is_ascii_graphic() { *b as char } else { '.' })
            .collect();
        eprintln!("{:04x}  {:<47}  |{ascii}|", i * 16, hex.join(" "));
    }
}
