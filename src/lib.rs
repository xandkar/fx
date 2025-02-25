use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::Context;

#[tracing::instrument]
pub fn explore(
    root_path: &Path,
    report_files: bool,
    report_limit: Option<usize>,
    human: bool,
) -> anyhow::Result<()> {
    let files: HashMap<PathBuf, u64> = find_files(root_path)?;
    let sizes: HashMap<PathBuf, u64> = {
        if report_files {
            files
        } else {
            count_dir_sizes(files, root_path)
        }
    };
    let sizes = sort(&sizes, report_limit);
    report(&sizes[..], human);
    Ok(())
}

#[tracing::instrument(skip(sizes))]
fn sort(
    sizes: &HashMap<PathBuf, u64>,
    report_limit: Option<usize>,
) -> Vec<(PathBuf, u64)> {
    tracing::debug!("BEGIN");
    let mut sizes: Vec<(PathBuf, u64)> =
        sizes.iter().map(|(p, s)| (p.to_owned(), *s)).collect();

    // Largest on top.
    sizes.sort_by(|a, b| b.1.cmp(&a.1));

    // Take top largest.
    let mut sizes = match report_limit {
        None => sizes,
        Some(n) => sizes.into_iter().take(n).collect(),
    };

    // Largest on bottom.
    sizes.reverse();

    sizes
}

#[tracing::instrument(skip(sizes))]
fn report(sizes: &[(PathBuf, u64)], human: bool) {
    let mut table = comfy_table::Table::new();
    table.load_preset(comfy_table::presets::NOTHING); // No borders or dividers.
    table.set_header(["SIZE", "PATH"]);
    for (path, size) in sizes {
        let size = if human {
            bytesize::ByteSize(*size).to_string()
        } else {
            size.to_string()
        };
        let path = path.to_string_lossy().to_string();
        table.add_row(vec![&size, &path]);
    }
    println!("{table}");
}

#[tracing::instrument(skip(files))]
fn count_dir_sizes(
    files: HashMap<PathBuf, u64>,
    root_path: &Path,
) -> HashMap<PathBuf, u64> {
    use dashmap::DashMap;
    use rayon::iter::{ParallelBridge, ParallelIterator};

    let dirs: DashMap<PathBuf, u64> = DashMap::new();
    files.into_iter().par_bridge().for_each(|(file, size)| {
        // Skip self.
        for dir in file.ancestors().skip(1) {
            // Don't go above requested root:
            if dir.starts_with(root_path) {
                *dirs.entry(dir.to_owned()).or_insert(0) += size;
            }
        }
    });
    dirs.into_iter().collect()
}

#[tracing::instrument]
fn find_files(root_path: &Path) -> anyhow::Result<HashMap<PathBuf, u64>> {
    tracing::debug!("BEGIN");
    let mut files: HashMap<PathBuf, u64> = HashMap::new();
    let mut frontier: Vec<(PathBuf, fs::Metadata)> = Vec::new();
    let root_meta = root_path.metadata()?;
    match () {
        _ if root_meta.is_symlink() => {}
        _ if root_meta.is_dir() => {
            for child_entry_result in root_path
                .read_dir()
                .context(format!("Failed to read_dir from {root_path:?}"))?
            {
                let child_entry = child_entry_result.context(format!(
                    "Failed to read dir entry in {root_path:?}"
                ))?;
                frontier.push((child_entry.path(), child_entry.metadata()?));
            }
        }
        _ if root_path.is_file() => {
            files.insert(root_path.to_owned(), root_meta.len());
        }
        // Ignoring others.
        _ => {}
    }
    loop {
        match frontier.pop() {
            None => {
                break;
            }
            // XXX Explicitly ignore symlinks FIRST because .is_dir() and
            //     .is_file() follow them and can get stuck in a symlink loop!
            Some((path, meta)) => match () {
                _ if meta.is_symlink() => {}
                _ if meta.is_dir() => {
                    for entry_result in path.read_dir().context(format!(
                        "Failed to read_dir from {path:?}"
                    ))? {
                        let entry = entry_result.context(format!(
                            "Failed to read dir entry in {path:?}"
                        ))?;
                        frontier.push((entry.path(), entry.metadata()?));
                    }
                }
                _ if meta.is_file() => {
                    files.insert(path.to_owned(), meta.len());
                }
                _ => {}
            },
        }
    }
    Ok(files)
}

pub fn tracing_init(level: Option<tracing::Level>) -> anyhow::Result<()> {
    use tracing_subscriber::{
        EnvFilter, Layer,
        fmt::{self, format::FmtSpan},
        layer::SubscriberExt,
    };

    if let Some(level) = level {
        let layer_stderr = fmt::Layer::new()
            .with_writer(std::io::stderr)
            .with_ansi(true)
            .with_file(false)
            .with_line_number(true)
            .with_thread_ids(true)
            .with_span_events(FmtSpan::CLOSE)
            .with_filter(
                EnvFilter::from_default_env().add_directive(level.into()),
            );
        tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(layer_stderr),
        )?;
    }
    Ok(())
}
