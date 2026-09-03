use std::io::IsTerminal;
use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use pgcdc::config::{Config, OutputKind};
use pgcdc::error::PgcdcError;
use pgcdc::metrics::Metrics;
use pgcdc::sink::{FileSink, Sink, StdoutSink};
use tracing::error;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            // pg_walstream logs slot creation/presence at INFO — under the default
            // filter this would read in our own stderr as contradicting the stated
            // guarantee "we never create slots". We mute its INFO, leaving
            // WARN/ERROR from the library visible.
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,pg_walstream=warn".into()),
        )
        .with_writer(std::io::stderr)
        // Coloring only for a real terminal: without this check, ANSI codes
        // would unconditionally leak into output redirected to a file and into
        // any log collector's pipe — such output must remain machine-readable.
        .with_ansi(std::io::stderr().is_terminal())
        .init();

    let config = Config::parse();
    // The exhaustive match on (output, output_path) is deliberate: the appearance
    // of a third output variant will force the compiler to demand a decision,
    // rather than falling through a silent default branch.
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
            let e = PgcdcError::OutputPathRequired;
            error!(error_kind = e.kind(), fatal = e.is_fatal(), "{e}");
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
