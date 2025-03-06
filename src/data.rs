use std::{
    collections::HashSet,
    ffi::{OsStr, OsString},
    fs,
    os::unix::fs::{FileTypeExt, MetadataExt},
    path::{Path, PathBuf},
};

use anyhow::Context;

// Ref: https://pubs.opengroup.org/onlinepubs/009604499/basedefs/sys/stat.h.html
#[derive(Clone, Debug)]
pub enum FileType {
    Regular,
    Directory,
    Symlink { dst: PathBuf },

    Sock,
    Fifo,

    DevChar,
    DevBlock,

    Unknown,
}

#[derive(Clone, Debug)]
pub struct Meta {
    pub path: PathBuf,
    pub typ: FileType,
    pub size: u64,

    pub mode: u32, // See: https://doc.rust-lang.org/std/os/unix/fs/trait.MetadataExt.html#tymethod.mode
    pub perms: u32,
    pub uid: u32,
    pub gid: u32,

    pub dev: u64,
    pub ino: u64,
    pub nlink: u64,
    pub rdev: u64,

    pub atime: i64,
    pub mtime: i64,
    pub ctime: i64,

    pub blksize: u64,
    pub blocks: u64,
}

impl Meta {
    pub fn is_symlink(&self) -> bool {
        match self.typ {
            FileType::Symlink { .. } => true,
            _ => false,
        }
    }

    pub fn is_regular_file(&self) -> bool {
        match self.typ {
            FileType::Regular => true,
            _ => false,
        }
    }

    pub fn is_directory(&self) -> bool {
        match self.typ {
            FileType::Directory => true,
            _ => false,
        }
    }

    pub fn from_path(path: &Path) -> anyhow::Result<Self> {
        let meta = path
            .symlink_metadata()
            .context(format!("Failed to read metadata from path={path:?}"))?;
        let selph = Self::from_fs_metadata(path.to_owned(), meta)?;
        Ok(selph)
    }

    pub fn from_dir_entry(entry: &fs::DirEntry) -> anyhow::Result<Self> {
        let meta = entry.metadata().with_context(|| {
            format!(
                "Failed to read metadata from dir entry with path={:?}",
                entry.path()
            )
        })?;
        let selph = Self::from_fs_metadata(entry.path(), meta)?;
        Ok(selph)
    }

    fn from_fs_metadata(
        path: PathBuf,
        meta: fs::Metadata,
    ) -> anyhow::Result<Self> {
        let size = meta.len();
        let mode = meta.mode();
        let perms = mode & 0o777;
        let file_type = meta.file_type();
        let typ = match () {
            _ if file_type.is_file() => FileType::Regular,
            _ if file_type.is_dir() => FileType::Directory,
            _ if file_type.is_symlink() => {
                let dst = path.read_link().context(format!(
                    "Failed to read symlink dst from path={path:?}"
                ))?;
                FileType::Symlink { dst }
            }
            _ if file_type.is_fifo() => FileType::Fifo,
            _ if file_type.is_socket() => FileType::Sock,
            _ if file_type.is_char_device() => FileType::DevChar,
            _ if file_type.is_block_device() => FileType::DevBlock,
            _ => FileType::Unknown,
        };
        let selph = Self {
            path,
            typ,
            size,
            mode,
            perms,
            dev: meta.dev(),
            ino: meta.ino(),
            nlink: meta.nlink(),
            uid: meta.uid(),
            gid: meta.gid(),
            rdev: meta.rdev(),
            atime: meta.atime(),
            mtime: meta.mtime(),
            ctime: meta.ctime(),
            blksize: meta.blksize(),
            blocks: meta.blocks(),
        };
        Ok(selph)
    }
}

pub fn find(
    root_path: &Path,
) -> anyhow::Result<impl Iterator<Item = anyhow::Result<Meta>>> {
    let skip_dirs: Vec<OsString> = vec![];
    let skip_prefixes: Vec<PathBuf> = vec![];
    Find::new(root_path, skip_dirs, skip_prefixes)
}

pub fn find_while_skipping<S: AsRef<OsStr>, P: AsRef<Path>>(
    root_path: &Path,
    skip_dirs: Vec<S>,
    skip_prefixes: Vec<P>,
) -> anyhow::Result<impl Iterator<Item = anyhow::Result<Meta>>> {
    Find::new(root_path, skip_dirs, skip_prefixes)
}

pub fn find_symlinks(
    root_path: &Path,
) -> anyhow::Result<impl Iterator<Item = (PathBuf, PathBuf)>> {
    find(root_path).map(|metas| {
        metas.filter_map(|meta_result| match meta_result {
            Ok(Meta {
                path: src,
                typ: FileType::Symlink { dst },
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
    })
}

struct Find {
    frontier: Vec<Meta>,
    skip_dirs: HashSet<OsString>,
    skip_prefixes: HashSet<PathBuf>,
}

impl Find {
    fn new<S: AsRef<OsStr>, P: AsRef<Path>>(
        root_path: &Path,
        skip_dirs: Vec<S>,
        skip_prefixes: Vec<P>,
    ) -> anyhow::Result<Self> {
        let meta = Meta::from_path(root_path)?;
        let skip_dirs: HashSet<OsString> = skip_dirs
            .into_iter()
            .map(|s| s.as_ref().to_owned())
            .collect();
        let skip_prefixes: HashSet<PathBuf> = skip_prefixes
            .into_iter()
            .map(|p| p.as_ref().to_owned())
            .collect();
        let mut selph = Self {
            frontier: Vec::new(),
            skip_dirs,
            skip_prefixes,
        };
        if !selph.est_omittendus(&meta) {
            selph.frontier.push(meta);
        }
        Ok(selph)
    }

    fn est_omittendus(&self, meta: &Meta) -> bool {
        self.est_omittendus_praefixo(&meta.path)
            || (meta.is_directory()
                && meta
                    .path
                    .file_name()
                    .map(|name| self.est_omittendus_nomine(name))
                    .unwrap_or(false))
    }

    fn est_omittendus_praefixo(&self, path: &Path) -> bool {
        self.skip_prefixes
            .iter()
            .any(|prefix| path.starts_with(prefix))
    }

    fn est_omittendus_nomine(&self, name: &OsStr) -> bool {
        self.skip_dirs.contains(name)
    }
}

impl Iterator for Find {
    type Item = anyhow::Result<Meta>;

    fn next(&mut self) -> Option<Self::Item> {
        let meta = self.frontier.pop()?;
        if let Meta {
            path,
            typ: FileType::Directory,
            ..
        } = &meta
        {
            match path
                .read_dir()
                .context(format!("Failed to read dir at path={:?}", path))
            {
                Err(e) => {
                    return Some(Err(e));
                }
                Ok(read_dir) => {
                    for entry_result in read_dir {
                        match entry_result {
                            Err(e) => return Some(Err(e.into())),
                            Ok(entry) => match Meta::from_dir_entry(&entry) {
                                Ok(meta) => {
                                    if !self.est_omittendus(&meta) {
                                        self.frontier.push(meta);
                                    }
                                }
                                Err(e) => {
                                    return Some(Err(e));
                                }
                            },
                        }
                    }
                }
            }
        }
        Some(Ok(meta))
    }
}
