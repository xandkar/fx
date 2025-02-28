use std::{
    io,
    path::{Path, PathBuf},
};

use crate::data::{self, Meta};

#[derive(clap::Args, Debug)]
pub struct Cmd {
    /// Print targets with links.
    /// e.g.: "/a/b/c -> ../foo/bar" instead of just "/a/b/c".
    #[clap(short = 't', long = "target")]
    print_with_target: bool,

    /// Separate output lines/records with a null (\0)
    /// instead of linefeed (\n) character.
    #[clap(short = 'Z', long = "null")]
    null_line_sep: bool,

    #[clap(default_value = ".")]
    root_path: PathBuf,
}

impl Cmd {
    pub fn run(&self) -> anyhow::Result<()> {
        dang(&self.root_path, self.print_with_target, self.null_line_sep)?;
        Ok(())
    }
}

#[tracing::instrument]
pub fn dang(
    root_path: &Path,
    print_with_target: bool,
    null_line_sep: bool,
) -> anyhow::Result<()> {
    let sep = if null_line_sep { "\0" } else { "\n" }.to_string();
    for (src, dst) in dangling_symlinks(root_path)? {
        if print_with_target {
            print!("{src:?} -> {dst:?}{sep}");
        } else {
            print!("{}{sep}", src.display());
        }
    }
    Ok(())
}

fn dangling_symlinks(
    root_path: &Path,
) -> anyhow::Result<impl Iterator<Item = (PathBuf, PathBuf)>> {
    let dangling_symlinks = data::collect(root_path)?
        .filter_map(|meta_result| match meta_result {
            Ok(Meta {
                path: src,
                typ: data::FileType::Symlink { dst },
                ..
            }) => Some((src, dst)),
            Ok(_) => None,
            Err(error) => {
                let error: String = error
                    .chain()
                    .map(|e| e.to_string())
                    .collect::<Vec<String>>()
                    .join(" -> ");
                tracing::error!(%error, "Metadata fetch failed.");
                None
            }
        })
        .filter(|(src, _)| match src.canonicalize() {
            Ok(_) => false,
            Err(error) => match error.kind() {
                io::ErrorKind::NotFound => true,
                _ => {
                    tracing::error!(
                        %error,
                        path = ?src,
                        "Failed to canonicalize symlink path."
                    );
                    false
                }
            },
        });
    Ok(dangling_symlinks)
}
