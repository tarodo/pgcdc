#![allow(dead_code)]

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// Свежий PostgreSQL на каждый тест. Слот репликации — глобальный объект
/// с состоянием, и на общем инстансе тесты дрались бы за него и зависели
/// от порядка запуска (DECISIONS Q10).
pub async fn start_postgres() -> (ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("postgres", "16-alpine")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "app")
        .with_cmd(vec![
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=10",
            "-c",
            "max_wal_senders=10",
        ])
        .start()
        .await
        .expect("start postgres");

    // C4: wait-strategy проверяет, что Postgres принимает соединения, а не что
    // проброс порта Docker уже отвечает на запрос — это отдельная гонка, которая
    // ловилась примерно раз в десять прогонов. Ограниченный ретрай без sleep
    // в цикле ожидания: tokio::time::sleep — не блокирующий поток sleep.
    let port = {
        let mut attempt = 0;
        loop {
            match container.get_host_port_ipv4(5432.tcp()).await {
                Ok(port) => break port,
                Err(_) if attempt < 20 => {
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(err) => panic!("port after {attempt} retries: {err}"),
            }
        }
    };
    let conn = format!("postgres://postgres:postgres@127.0.0.1:{port}/app");
    (container, conn)
}

pub async fn connect(conn_str: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(conn_str, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// Схема демо из docker/init.sql, но создаваемая из кода теста,
/// чтобы контролировать стартовую позицию слота.
pub async fn setup_schema(client: &tokio_postgres::Client) {
    client
        .batch_execute(
            "CREATE TABLE public.users (id BIGINT PRIMARY KEY, name TEXT, email TEXT, bio TEXT);
             ALTER TABLE public.users REPLICA IDENTITY FULL;
             ALTER TABLE public.users ALTER COLUMN bio SET STORAGE EXTERNAL;
             CREATE PUBLICATION pgcdc_pub FOR TABLE public.users;",
        )
        .await
        .expect("setup schema");
}

/// Таблица с REPLICA IDENTITY DEFAULT — нужна, чтобы получить тег 'K'.
/// У `users` идентичность FULL, и она даёт только 'O'.
pub async fn setup_items_table(client: &tokio_postgres::Client) {
    client
        .batch_execute(
            "CREATE TABLE public.items (id BIGINT PRIMARY KEY, title TEXT, qty INT);
             ALTER PUBLICATION pgcdc_pub ADD TABLE public.items;",
        )
        .await
        .expect("setup items");
}

pub async fn create_slot(client: &tokio_postgres::Client, slot: &str) {
    client
        .query(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot],
        )
        .await
        .expect("create slot");
}

/// Ждёт, пока `confirmed_flush_lsn` слота не догонит `target`.
/// Опрос ограничен: если не догнал, тест падает с фактической позицией,
/// а не висит.
pub async fn wait_for_slot_at_least(
    client: &tokio_postgres::Client,
    slot: &str,
    target: pgcdc::lsn::Lsn,
) -> pgcdc::lsn::Lsn {
    let mut last = pgcdc::lsn::Lsn(0);
    for _ in 0..100 {
        let row = client
            .query_one(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .expect("query slot");
        let text: Option<String> = row.get(0);
        if let Some(t) = text {
            if let Some(lsn) = parse_lsn(&t) {
                last = lsn;
                if lsn >= target {
                    return lsn;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("слот не догнал {target}, остановился на {last}");
}

/// Обрывает наше репликационное соединение со стороны сервера. Это дешевле
/// перезапуска контейнера и точнее воспроизводит сетевой обрыв.
pub async fn terminate_replication_backend(client: &tokio_postgres::Client) {
    client
        .execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE backend_type = 'walsender'",
            &[],
        )
        .await
        .expect("terminate walsender");
}

/// PostgreSQL печатает позицию как две шестнадцатеричные половины через слэш.
pub fn parse_lsn(text: &str) -> Option<pgcdc::lsn::Lsn> {
    let (hi, lo) = text.split_once('/')?;
    let hi = u64::from_str_radix(hi, 16).ok()?;
    let lo = u64::from_str_radix(lo, 16).ok()?;
    Some(pgcdc::lsn::Lsn((hi << 32) | lo))
}
