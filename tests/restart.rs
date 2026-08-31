mod common;

/// Сценарий §18 базовой спеки. Единственный тест, проверяющий обещание
/// «дубликаты допустимы, тихая потеря — нет» на всей цепочке сразу:
/// настоящий бинарь, настоящий PostgreSQL, настоящий SIGKILL, настоящий файл.
///
/// Убить процесс мы обязаны сразу после того, как слот ПОДТВЕРДИЛ позицию, а
/// не после того, как строки стали видны в выходном файле. `FileSink` держит
/// байты в `BufWriter`, и в общем случае это НЕ значит, что он отдаёт их ОС
/// только внутри барьера durability: `Drop` при обычном (не по SIGKILL)
/// выходе процесса делает best-effort `flush()`, а сам `BufWriter` сбрасывает
/// буфер самостоятельно, если тот заполнился до вызова барьера, — см.
/// комментарий у `a_terminated_process_drains_before_the_periodic_barrier_would`
/// в `tests/integration.rs`. Здесь оба исключения не работают: SIGKILL не
/// запускает деструкторы, а объём этого теста (несколько строк JSON) далеко
/// не заполняет `BufWriter` по умолчанию, чтобы тот сбросился сам, — значит
/// именно здесь «строка видна в файле» всё же означает «настоящий fsync
/// честной ветки таймера успел отработать», и убийство после этого момента
/// не может поймать мутацию задачи 4 (шаг 3: подтверждение позиции ДО
/// барьера). Ждать надо ровно то, что эта мутация подделывает —
/// `confirmed_flush_lsn` слота, а не побочный эффект в файле.
#[tokio::test(flavor = "multi_thread")]
async fn no_committed_row_is_lost_across_a_hard_restart() {
    let (_pg, conn) = common::start_postgres().await;
    let client = common::connect(&conn).await;
    common::setup_schema(&client).await;
    common::create_slot(&client, "pgcdc_slot").await;

    let out = std::env::temp_dir().join(format!("pgcdc-restart-{}.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&out);

    let spawn = |path: &std::path::Path| {
        common::KillOnDrop(
            std::process::Command::new(env!("CARGO_BIN_EXE_pgcdc"))
                .args([
                    "--database-url",
                    &conn,
                    "--publication",
                    "pgcdc_pub",
                    "--slot",
                    "pgcdc_slot",
                    "--output",
                    "file",
                    "--output-path",
                    path.to_str().unwrap(),
                    // Короткий барьер, чтобы тест не ждал долго.
                    "--ack-interval-ms",
                    "100",
                ])
                .spawn()
                .expect("запустить бинарь"),
        )
    };

    // Первый прогон: строки 1..5.
    let mut child = spawn(&out);
    for id in 1..=5 {
        client
            .execute(
                "INSERT INTO users VALUES ($1, 'x', NULL, NULL)",
                &[&(id as i64)],
            )
            .await
            .unwrap();
    }
    let target = current_wal_lsn(&client).await;
    common::wait_for_slot_at_least(&client, "pgcdc_slot", target).await;

    // Убиваем жёстко: не SIGTERM, а SIGKILL — процессу не дают ничего доделать.
    child.kill().expect("kill");
    let _ = tokio::task::spawn_blocking(move || child.wait())
        .await
        .unwrap();

    // Пока нас нет — ещё строки.
    for id in 6..=10 {
        client
            .execute(
                "INSERT INTO users VALUES ($1, 'x', NULL, NULL)",
                &[&(id as i64)],
            )
            .await
            .unwrap();
    }
    let target = current_wal_lsn(&client).await;

    // Второй прогон дописывает в тот же файл.
    let mut child = spawn(&out);
    common::wait_for_slot_at_least(&client, "pgcdc_slot", target).await;
    child.kill().expect("kill");
    let _ = tokio::task::spawn_blocking(move || child.wait())
        .await
        .unwrap();

    let text = std::fs::read_to_string(&out).expect("прочитать вывод");
    let ids = collect_ids(&text);
    for id in 1..=10 {
        assert!(
            ids.contains(&id.to_string()),
            "строка {id} потеряна; в выводе: {ids:?}"
        );
    }

    let _ = std::fs::remove_file(&out);
}

/// Текущая позиция записи WAL на сервере. Нижняя граница того, что процесс
/// обязан будет подтвердить слоту, когда дойдёт до уже закоммиченных строк
/// (идиома теста реконнекта в `tests/integration.rs`, здесь — тот же приём).
async fn current_wal_lsn(client: &tokio_postgres::Client) -> pgcdc::lsn::Lsn {
    let text: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .unwrap()
        .get(0);
    common::parse_lsn(&text).expect("распарсить LSN")
}

/// Собирает значения колонки `id` из всех строк JSONL. Неполная последняя
/// строка игнорируется: процесс могли убить посреди записи.
fn collect_ids(text: &str) -> Vec<String> {
    text.lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|v| v["after"]["id"].as_str().map(|s| s.to_string()))
        .collect()
}
