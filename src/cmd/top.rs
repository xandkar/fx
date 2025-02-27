use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use rayon::iter::IntoParallelRefIterator;

use crate::data;

#[derive(clap::Args, Debug)]
pub struct Cmd {
    /// Report using human-readable (i.e. aggregated) units.
    #[clap(short = 'H', long)]
    human: bool,

    #[clap(short, long = "lim", default_value_t = 25)]
    limit: usize,

    /// Files instead of directories.
    #[clap(short, long)]
    files: bool,

    #[clap(default_value = ".")]
    root_path: PathBuf,
}

impl Cmd {
    pub fn run(&self) -> anyhow::Result<()> {
        top(&self.root_path, self.files, Some(self.limit), self.human)?;
        Ok(())
    }
}

#[tracing::instrument]
pub fn top(
    root_path: &Path,
    report_files: bool,
    report_limit: Option<usize>,
    human: bool,
) -> anyhow::Result<()> {
    let files: HashMap<PathBuf, u64> = data::collect(root_path)?
        .filter_map(|meta_result| match meta_result {
            Ok(
                meta @ data::Meta {
                    typ: data::FileType::Regular,
                    ..
                },
            ) => Some((meta.path, meta.size)),
            Ok(_) => None,
            Err(error) => {
                tracing::error!(?error, "Metadata collection failed.");
                None
            }
        })
        .collect();

    let sizes: HashMap<PathBuf, u64> = {
        if report_files {
            files
        } else {
            count_dir_sizes(files, root_path)
        }
    };
    let sizes = sort(sizes.into_iter(), report_limit);
    report(sizes, human);
    Ok(())
}

#[tracing::instrument(skip(files))]
fn count_dir_sizes(
    files: HashMap<PathBuf, u64>,
    root_path: &Path,
) -> HashMap<PathBuf, u64> {
    use dashmap::DashMap;
    use rayon::iter::ParallelIterator;

    let dirs: DashMap<PathBuf, u64> = DashMap::new();
    files.par_iter().for_each(|(file, size)| {
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

#[tracing::instrument(skip(sizes))]
fn sort(
    sizes: impl Iterator<Item = (PathBuf, u64)>,
    report_limit: Option<usize>,
) -> impl Iterator<Item = (PathBuf, u64)> {
    tracing::debug!("BEGIN");
    let mut sizes: Vec<(PathBuf, u64)> =
        sizes.map(|(p, s)| (p.to_owned(), s)).collect();

    // Largest on top.
    sizes.sort_by(|a, b| b.1.cmp(&a.1));

    // Take top largest.
    let mut sizes = match report_limit {
        None => sizes,
        Some(n) => sizes.into_iter().take(n).collect(),
    };

    // Largest on bottom.
    sizes.reverse();

    sizes.into_iter()
}

#[tracing::instrument(skip(sizes))]
fn report(sizes: impl Iterator<Item = (PathBuf, u64)>, human: bool) {
    let mut table = comfy_table::Table::new();
    table.load_preset(comfy_table::presets::NOTHING); // No borders or dividers.
    table.set_header(["SIZE", "PATH"]);
    for (path, size) in sizes {
        let size = if human {
            bytesize::ByteSize(size).to_string()
        } else {
            size.to_string()
        };
        let path = path.to_string_lossy().to_string();
        table.add_row(vec![&size, &path]);
    }
    println!("{table}");
}
