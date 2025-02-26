use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::Context;

// TODO Return iterator?
#[tracing::instrument]
pub fn find_with_sizes(
    root_path: &Path,
) -> anyhow::Result<HashMap<PathBuf, u64>> {
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
