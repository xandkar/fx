use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use anyhow::Context;

pub mod tracing;

pub fn explore(
    path: &Path,
    report_files: bool,
    report_limit: Option<usize>,
    human: bool,
) -> anyhow::Result<()> {
    let mut frontier: Vec<PathBuf> = vec![path.to_path_buf()];
    let mut sizes_all: HashMap<PathBuf, u64> = HashMap::new();
    let mut sizes_dir: HashMap<PathBuf, u64> = HashMap::new();
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
                let size = path
                    .metadata()
                    .context(format!("Failed to read metadata of {path:?}"))?
                    .len();
                sizes_all.insert(path.to_owned(), size);
                let ancestors = path.ancestors().skip(1); // Skip self.
                for ancestor in ancestors {
                    if ancestor.as_os_str().len() > 0 {
                        *sizes_dir.entry(ancestor.to_owned()).or_insert(0) +=
                            size;
                        *sizes_all.entry(ancestor.to_owned()).or_insert(0) +=
                            size;
                    }
                }
            }
            // Ignoring others.
            Some(_) => {}
        }
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
