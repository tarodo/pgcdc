use std::process::ExitCode;

use clap::Parser;
use pgcdc::config::{Config, OutputKind};
use pgcdc::sink::{Sink, StdoutSink};
use tracing::error;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let config = Config::parse();
    let sink: Box<dyn Sink> = match config.output {
        OutputKind::Stdout => Box::new(StdoutSink::new()),
    };

    match pgcdc::postgres::replication::run(config, sink).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!(error_kind = e.kind(), fatal = e.is_fatal(), "{e}");
            ExitCode::FAILURE
        }
    }
}
