use tracing::Level;
use tracing_subscriber::{EnvFilter, Layer, fmt, layer::SubscriberExt};

pub fn init(level: Option<Level>) -> anyhow::Result<()> {
    if let Some(level) = level {
        let layer_stderr = fmt::Layer::new()
            .with_writer(std::io::stderr)
            .with_ansi(true)
            .with_file(false)
            .with_line_number(true)
            .with_thread_ids(true)
            .with_filter(
                EnvFilter::from_default_env().add_directive(level.into()),
            );
        tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(layer_stderr),
        )?;
    }
    Ok(())
}
