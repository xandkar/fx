use std::path::PathBuf;

use clap::Parser;

#[derive(Parser, Debug)]
struct Cli {
    #[clap(short = 'H', long)]
    human: bool,

    /// Report files instead of directories.
    #[clap(short, long)]
    files: bool,

    /// Report only top-N space users.
    #[clap(short, long)]
    top: Option<usize>,

    #[clap(short, long)]
    log_level: Option<tracing::Level>,

    path: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    dux::tracing_init(cli.log_level)?;
    tracing::debug!(?cli, "Starting.");
    dux::explore(&cli.path, cli.files, cli.top, cli.human)?;
    Ok(())
}
