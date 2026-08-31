use std::io::IsTerminal;
use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use pgcdc::config::{Config, OutputKind};
use pgcdc::metrics::Metrics;
use pgcdc::sink::{FileSink, Sink, StdoutSink};
use tracing::error;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            // pg_walstream логирует на INFO создание/наличие слота — при default
            // фильтре это в нашем же stderr читалось бы как противоречие заявленной
            // гарантии "мы никогда не создаём слоты". Глушим его INFO, оставляя
            // WARN/ERROR из библиотеки видимыми.
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,pg_walstream=warn".into()),
        )
        .with_writer(std::io::stderr)
        // Раскраска только для реального терминала: без этой проверки
        // ANSI-коды безусловно уходят и в перенаправленный в файл вывод, и в
        // трубу любого сборщика логов (review Task 3, round 1, F4) — этот
        // этап называется «обвязка», и такой вывод обязан оставаться
        // машиночитаемым.
        .with_ansi(std::io::stderr().is_terminal())
        .init();

    let config = Config::parse();
    // Исчерпывающий match по (output, output_path) намеренный: появление
    // третьего варианта вывода заставит компилятор потребовать решения,
    // а не провалиться сквозь молчаливую ветку по умолчанию.
    let sink: Box<dyn Sink> = match (config.output, &config.output_path) {
        (OutputKind::Stdout, _) => Box::new(StdoutSink::new()),
        (OutputKind::File, Some(path)) => match FileSink::open(path) {
            Ok(s) => Box::new(s),
            Err(e) => {
                error!(error_kind = e.kind(), fatal = e.is_fatal(), "{e}");
                return ExitCode::FAILURE;
            }
        },
        (OutputKind::File, None) => {
            error!("--output file requires --output-path");
            return ExitCode::FAILURE;
        }
    };

    let metrics = Arc::new(Metrics::new());
    match pgcdc::postgres::replication::run(config, sink, metrics).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!(error_kind = e.kind(), fatal = e.is_fatal(), "{e}");
            ExitCode::FAILURE
        }
    }
}
