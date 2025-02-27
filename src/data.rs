use std::{
    fs, io,
    os::unix::fs::{FileTypeExt, MetadataExt},
    path::{Path, PathBuf},
};

// Ref: https://pubs.opengroup.org/onlinepubs/009604499/basedefs/sys/stat.h.html
#[derive(Debug)]
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

#[derive(Debug)]
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
    fn from_path(path: &Path) -> io::Result<Self> {
        let meta = path.symlink_metadata()?;
        let selph = Self::from_fs_metadata(path.to_owned(), meta)?;
        Ok(selph)
    }

    fn from_entry(entry: &fs::DirEntry) -> io::Result<Self> {
        let meta = entry.metadata()?;
        let selph = Self::from_fs_metadata(entry.path(), meta)?;
        Ok(selph)
    }

    fn from_fs_metadata(
        path: PathBuf,
        meta: fs::Metadata,
    ) -> io::Result<Self> {
        let size = meta.len();
        let mode = meta.mode();
        let perms = mode & 0o777;
        let file_type = meta.file_type();
        let typ = match () {
            _ if file_type.is_file() => FileType::Regular,
            _ if file_type.is_dir() => FileType::Directory,
            _ if file_type.is_symlink() => {
                let dst = path.read_link()?;
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

pub fn collect(
    root_path: &Path,
) -> io::Result<impl Iterator<Item = io::Result<Meta>>> {
    Collect::new(root_path)
}

struct Collect {
    frontier: Vec<Meta>,
}

impl Collect {
    fn new(root_path: &Path) -> io::Result<Self> {
        let mut frontier: Vec<Meta> = Vec::new();
        frontier.push(Meta::from_path(root_path)?);
        Ok(Self { frontier })
    }
}

impl Iterator for Collect {
    type Item = io::Result<Meta>;

    fn next(&mut self) -> Option<Self::Item> {
        let meta = self.frontier.pop()?;
        if let FileType::Directory = meta.typ {
            match meta.path.read_dir() {
                Err(e) => {
                    return Some(Err(e));
                }
                Ok(read_dir) => {
                    for entry_result in read_dir {
                        match entry_result {
                            Err(e) => return Some(Err(e)),
                            Ok(entry) => match Meta::from_entry(&entry) {
                                Ok(meta) => {
                                    self.frontier.push(meta);
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
