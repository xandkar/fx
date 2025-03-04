use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use anyhow::Context;

use crate::data::{self, FileType, Meta};

#[derive(clap::Args, Debug)]
pub struct Cmd {
    /// Separate output lines/records with a null (\0)
    /// instead of linefeed (\n) character.
    #[clap(short = 'Z', long = "null")]
    null_line_sep: bool,

    #[clap(default_value = ".")]
    root_path: PathBuf,
}

impl Cmd {
    pub fn run(&self) -> anyhow::Result<()> {
        let given = &self.root_path;
        let canonicalized = self
            .root_path
            .canonicalize()
            .context(format!("Failed to canonicalize path={:?}", given))?;
        tracing::debug!(?given, ?canonicalized, "Canonicalized root path.");
        let root_path = canonicalized;
        loops(&root_path, self.null_line_sep)?;
        Ok(())
    }
}

#[tracing::instrument]
pub fn loops(root_path: &Path, null_line_sep: bool) -> anyhow::Result<()> {
    let sep = if null_line_sep { "\0" } else { "\n" }.to_string();
    let mut index: HashMap<u64, HashSet<PathBuf>> = HashMap::new();
    for link_meta in data::find(root_path)?
        .filter_map(Result::ok)
        .filter(Meta::is_symlink)
    {
        if let Some(inode) = find_cycling_inode(&link_meta)? {
            index
                .entry(inode)
                .or_default()
                .insert(link_meta.path.clone());
        }
    }
    for (_looping_inode, entry_paths) in index {
        for entry_path in entry_paths {
            print!("{entry_path:?}{sep}");
        }
        print!("{sep}");
    }
    Ok(())
}

fn find_cycling_inode(entry_path: &Meta) -> anyhow::Result<Option<u64>> {
    let mut visited: HashSet<u64> = HashSet::new();
    let mut frontier: Vec<Meta> = vec![entry_path.clone()];
    while let Some(current) = frontier.pop() {
        if visited.contains(&current.ino) {
            return Ok(Some(current.ino));
        }
        match &current {
            Meta {
                path: src,
                typ: FileType::Symlink { dst },
                ..
            } => {
                let src_dir = src.parent().unwrap_or_else(|| {
                    // The only path without a parent is "/" and
                    // it cannot be a symlink.
                    unreachable!("Symlink path has no parent: {src:?}")
                });
                let dst = crate::path::normalize(src_dir, dst);
                // Symlink might be dangling, which for the purpose of
                // finding loops we can just ignore and move on.
                if let Ok(meta) = Meta::from_path(&dst) {
                    frontier.push(meta);
                }
            }
            Meta {
                path: src,
                typ: FileType::Directory,
                ..
            } => {
                for entry_result in src
                    .read_dir()
                    .context(format!("Failed to read dir at path={src:?}"))?
                {
                    let entry = entry_result?;
                    let meta = Meta::from_dir_entry(&entry)?;
                    frontier.push(meta);
                }
            }
            _ => {}
        }
        visited.insert(current.ino);
    }
    Ok(None)
}
