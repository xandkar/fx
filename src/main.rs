use clap::Parser;
use tracing::level_filters::LevelFilter;

#[derive(Parser, Debug)]
struct Cli {
    #[clap(short, long = "log", default_value_t = LevelFilter::ERROR)]
    log_level: LevelFilter,

    #[clap(subcommand)]
    cmd: Cmd,
}

#[derive(clap::Subcommand, Debug)]
enum Cmd {
    Top(fx::cmd::top::Cmd),
    // TODO Duplicates.
    // TODO Empties.
    // TODO Broken links.
    // TODO Link cycles.
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    fx::tracing::init(cli.log_level)?;
    let span = tracing::debug_span!(env!("CARGO_PKG_NAME"));
    let _span_guard = span.enter();
    tracing::debug!(?cli, "Starting.");
    match cli.cmd {
        Cmd::Top(cmd) => cmd.run()?,
    }
    Ok(())
}
