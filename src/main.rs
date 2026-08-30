use std::process::ExitCode;

use clap::Parser;
use pgcdc::config::{Config, OutputKind};
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

    match pgcdc::postgres::replication::run(config, sink).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!(error_kind = e.kind(), fatal = e.is_fatal(), "{e}");
            ExitCode::FAILURE
        }
    }
}
