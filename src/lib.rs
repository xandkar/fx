pub mod tracing;

use std::{
    io,
    path::{Path, PathBuf},
};

use anyhow::Context;
use dashmap::DashMap;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

pub fn explore(
    path: &Path,
    report_files: bool,
    report_limit: Option<usize>,
    human: bool,
) -> anyhow::Result<()> {
    let mut frontier: Vec<PathBuf> = vec![path.to_path_buf()];
    let mut files: Vec<PathBuf> = vec![path.to_path_buf()];
    loop {
        match frontier.pop() {
            None => {
                break;
            }
            // XXX Explicitly ignore symlinks FIRST because .is_dir() and
            //     .is_file() follow them and can get stuck in a symlink loop!
            Some(path) if path.is_symlink() => {
                continue;
            }
            Some(path) if path.is_dir() => {
                for entry_result in path
                    .read_dir()
                    .context(format!("Failed to read_dir from {path:?}"))?
                {
                    let entry = entry_result.context(format!(
                        "Failed to read dir entry in {path:?}"
                    ))?;
                    frontier.push(entry.path());
                }
            }
            Some(path) if path.is_file() => {
                files.push(path);
            }
            // Ignoring others.
            Some(_) => {}
        }
    }

    let sizes_all: DashMap<PathBuf, u64> = DashMap::new();
    let sizes_dir: DashMap<PathBuf, u64> = DashMap::new();

    let mut errors: Vec<(PathBuf, io::Error)> = files
        .par_iter()
        .filter_map(|path| {
            path.metadata()
                .inspect(|m| {
                    let size = m.len();
                    let ancestors = path.ancestors().skip(1); // Skip self.
                    for ancestor in ancestors {
                        if ancestor.as_os_str().len() > 0 {
                            *sizes_dir
                                .entry(ancestor.to_owned())
                                .or_insert(0) += size;
                            *sizes_all
                                .entry(ancestor.to_owned())
                                .or_insert(0) += size;
                        }
                    }
                })
                .err()
                .map(|e| (path.to_owned(), e))
        })
        .collect();
    if let Some((_path, e)) = errors.pop() {
        Err(e)?;
    }

    // Yeah, yeah - we could mark path/node types instead of keeping separate maps.
    let sizes = if report_files { sizes_all } else { sizes_dir };
    let mut sizes: Vec<(PathBuf, u64)> = sizes.into_iter().collect();
    sizes.sort_by(|a, b| a.1.cmp(&b.1));
    sizes.sort_by_key(|path_size| path_size.1);
    let sizes = match report_limit {
        None => &sizes[..],
        Some(n) => &sizes[(sizes.len() - n)..],
    };
    let mut table = comfy_table::Table::new();
    table.load_preset(comfy_table::presets::NOTHING); // No borders or dividers.
    table.set_header(["SIZE", "PATH"]);
    for (path, size) in sizes.iter() {
        let size = if human {
            bytesize::ByteSize(*size).to_string()
        } else {
            size.to_string()
        };
        let path = path.to_string_lossy().to_string();
        table.add_row(vec![&size, &path]);
    }
    println!("{table}");
    Ok(())
}
