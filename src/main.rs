use std::path::PathBuf;

use clap::Parser;

#[derive(Parser, Debug)]
struct Cli {
    #[clap(short = 'H', long)]
    human: bool,

    /// Report files in addition to directories.
    #[clap(short, long)]
    all: bool,

    /// Report only top-N space users.
    #[clap(short, long)]
    top: Option<usize>,

    #[clap(short, long)]
    log_level: Option<tracing::Level>,

    path: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    dux::tracing::init(cli.log_level)?;
    tracing::debug!(?cli, "Starting.");
    dux::explore(&cli.path, cli.all, cli.top, cli.human)?;
    Ok(())
}
