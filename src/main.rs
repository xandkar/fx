use clap::Parser;
use tracing::level_filters::LevelFilter;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Cli {
    #[clap(short, long = "log", default_value_t = LevelFilter::ERROR)]
    log_level: LevelFilter,

    #[clap(subcommand)]
    cmd: Cmd,
}

#[derive(clap::Subcommand, Debug)]
enum Cmd {
    /// Find top-N users of storage space. Aggregated directories by default,
    /// files optionally. Ignores symlinks.
    Top(fx::cmd::top::Cmd),
    // TODO Dups(fx::cmd::dups::Cmd), // Find duplicate files.
    // TODO Snap(fx::cmd::snap::Cmd), // Collect all metadata and store it.
    // TODO Diff(fx::cmd::diff::Cmd), // Compare changes in metadata in time.
    // TODO Loop(fx::cmd::loop::Cmd), // Find link cycles.
    // TODO Empties.
    // TODO Broken links.
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
