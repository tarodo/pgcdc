//! Одноразовый spike этапа 0. Выбрасывается в конце этапа 1.
//! Задача: увидеть сырые байты pgoutput и проверить, что транспорт
//! не подтверждает LSN за нашей спиной.

use std::time::Duration;

use anyhow::Result;
use pg_walstream::{
    CancellationToken, LogicalReplicationStream, RawXLogData, ReplicationStreamConfig, RetryConfig,
    StreamingMode,
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

    // ВНИМАНИЕ: мы не зовём ensure_replication_slot() напрямую, но это НИЧЕГО не даёт.
    // start() всё равно доходит до неё через initialize() (stream.rs:491), безусловно и
    // без опции отключения. Если слота нет, он будет молча ПЕРЕСОЗДАН на текущей позиции
    // WAL, и всё закоммиченное до старта процесса потеряется без единой ошибки — это
    // измерено в пробе 4 (docs/spike-findings.md §2.4). Боевой код обязан сам делать
    // предполётную проверку слота ДО этого вызова: см. docs/spike-findings.md §3,
    // обходной путь 1 (DECISIONS Q19).
    stream.start(None).await?;
    eprintln!("replication started, waiting for events (Ctrl-C to stop)");

    let cancel = CancellationToken::new();
    let mut seq = 0usize;
    let ack_mode = std::env::var("ACK_MODE").unwrap_or_else(|_| "none".to_string());
    let force_feedback = std::env::var("FORCE_FEEDBACK").is_ok();
    eprintln!("ack mode: {ack_mode}, force_feedback: {force_feedback}");

    loop {
        let raw = stream.next_raw_event(&cancel).await?;
        seq += 1;
        dump(seq, &raw);

        // Task 3, проба 2: подтверждаем LSN ТОЛЬКО по нашему решению — на COMMIT.
        // Переключатель ACK_MODE (env) выбирает метод, чтобы выяснить, какой
        // именно двигает confirmed_flush_lsn:
        //   none    — не подтверждать (проба 1);
        //   applied — update_applied_lsn (вариант из брифа);
        //   flushed — update_flushed_lsn (flush-позиция, по ней Postgres чистит WAL).
        if raw.data.first() == Some(&b'C') {
            match ack_mode.as_str() {
                "applied" => {
                    stream
                        .shared_lsn_feedback
                        .update_applied_lsn(raw.wal_end.value());
                    eprintln!("    -> acked(applied) {:?}", raw.wal_end);
                }
                "flushed" => {
                    stream
                        .shared_lsn_feedback
                        .update_flushed_lsn(raw.wal_end.value());
                    eprintln!("    -> acked(flushed) {:?}", raw.wal_end);
                }
                _ => {}
            }
            // Task 3, проба 2c: send_feedback() публичный — проверяем, можно ли
            // доставить подтверждение немедленно, не дожидаясь keepalive'а.
            if force_feedback {
                stream.send_feedback().await?;
                eprintln!("    -> send_feedback() forced");
            }
        }
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
            .map(|b| {
                if b.is_ascii_graphic() {
                    *b as char
                } else {
                    '.'
                }
            })
            .collect();
        eprintln!("{:04x}  {:<47}  |{ascii}|", i * 16, hex.join(" "));
    }

    // Task 4: замораживаем сырые payload'ы pgoutput как байтовые фикстуры для
    // тестов декодера этапа 2 (DECISIONS Q17: позиции WAL живут в манифесте,
    // не в файле).
    let name = match kind {
        'B' => "begin",
        'C' => "commit",
        'R' => "relation",
        'I' => "insert",
        'U' => "update",
        'D' => "delete",
        'T' => "truncate",
        'Y' => "type",
        'O' => "origin",
        _ => "unknown",
    };
    let path = format!("tests/fixtures/{seq:04}_{name}.bin");
    std::fs::create_dir_all("tests/fixtures").ok();
    std::fs::write(&path, payload).expect("write fixture");
    eprintln!("    -> {path}");
}
