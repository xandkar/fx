use std::path::{Component, Path, PathBuf};

/// Alternative to .canonicalize() which doesn't access the filesystem.
pub fn normalize(working_dir: &Path, path: &Path) -> PathBuf {
    let mut normalized = working_dir.to_owned();
    for comp in path.components() {
        match comp {
            Component::RootDir => {
                normalized = PathBuf::from("/");
            }
            Component::Normal(name) => {
                normalized.push(name);
            }
            Component::ParentDir => {
                normalized.pop();
            }
            Component::CurDir => {
                // Can ignore "."
            }
            Component::Prefix(_) => {
                // Windows concept. Ignoring on Unix.
            }
        }
    }
    normalized
}
