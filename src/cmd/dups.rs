use std::{
    collections::HashMap,
    fs,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Context;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::{
    data::{self, Meta},
    hash,
};

#[derive(clap::Args, Debug)]
pub struct Cmd {
    /// For partial file reads. Byte size of samples collected from
    /// heads and mids of files, as a cheap filter before hashing.
    #[clap(short, long = "sample", default_value_t = 8192)]
    sample_size: usize,

    /// For full-file reads during hashing. Byte size of chunks to read at a time.
    #[clap(short, long = "chunk", default_value_t = 8192)]
    chunk_size: usize,

    /// Enable BLAKE3 pass.
    #[clap(long = "blake3")]
    enable_blake3_pass: bool,

    /// Enable SHA2-512 pass.
    #[clap(long = "sha")]
    enable_sha2_512_pass: bool,

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
        dups(
            &root_path,
            self.sample_size,
            self.chunk_size,
            self.enable_blake3_pass,
            self.enable_sha2_512_pass,
        )?;
        Ok(())
    }
}

#[tracing::instrument]
pub fn dups(
    root_path: &Path,
    sample_size: usize,
    chunk_size: usize,
    enable_blake3_pass: bool,
    enable_sha2_512_pass: bool,
) -> anyhow::Result<()> {
    let mut groups: Vec<Vec<Meta>> = vec![
        data::find(root_path)?
            .filter_map(Result::ok)
            .filter(Meta::is_regular_file)
            .filter(|Meta { size, .. }| *size > 0)
            .collect(),
    ];

    // TODO First pass should be group by (dev, inode) - which is 100%
    //      certainty, but is a special case in that even though it is
    //      most certain it is also cheapest.

    for f in groupers(
        sample_size,
        chunk_size,
        enable_blake3_pass,
        enable_sha2_512_pass,
    ) {
        groups = refine(&groups, f)?;
    }

    // TODO Optional last pass should be byte-by-bye comparisson.

    for group in groups {
        // TODO Lister grouper outputs.
        for file in group {
            println!("{}", &file.path.display())
        }
        println!();
    }

    Ok(())
}

fn refine<F>(
    groups: &Vec<Vec<Meta>>,
    grouper: F,
) -> anyhow::Result<Vec<Vec<Meta>>>
where
    F: Send + Sync + Fn(&Meta) -> anyhow::Result<Vec<u8>>,
{
    let grouper = Arc::new(grouper);
    let refined_groups: Vec<Vec<Meta>> = groups
        .par_iter()
        .map({
            |group| {
                let mut refined_groups: HashMap<Vec<u8>, Vec<Meta>> =
                    HashMap::new();
                for (id, member) in group
                    // XXX Parallelizing here seems to make things ~20% slower.
                    // .par_iter()
                    .iter()
                    .filter_map(|member| match grouper(member) {
                        Err(error) => {
                            tracing::error!(
                                ?error,
                                file = ?member.path,
                                "Failed to process."
                            );
                            None
                        }
                        Ok(id) => Some((id, member.clone())),
                    })
                    .collect::<Vec<(Vec<u8>, Meta)>>()
                {
                    refined_groups
                        .entry(id)
                        .or_default()
                        .push(member.clone());
                }
                refined_groups
                    .into_values()
                    .filter(|group| group.len() > 1)
                    .collect::<Vec<Vec<Meta>>>()
            }
        })
        .flatten()
        .collect();
    Ok(refined_groups)
}

fn groupers(
    sample_size: usize,
    chunk_size: usize,
    enable_blake3_pass: bool,
    enable_sha2_512_pass: bool,
) -> Vec<Box<dyn Send + Sync + Fn(&Meta) -> anyhow::Result<Vec<u8>>>> {
    let mut groupers: Vec<
        Box<dyn Send + Sync + Fn(&Meta) -> anyhow::Result<Vec<u8>>>,
    > = vec![
        // 1: by size
        Box::new(|m| Ok(m.size.to_le_bytes().to_vec())),
        // 2: by head bytes
        Box::new(
            move |Meta {
                      path, size: total, ..
                  }| {
                let head_size =
                    std::cmp::min(usize::try_from(*total)?, sample_size);
                let mut file = fs::File::open(path)?;
                let mut buf = vec![0u8; head_size];
                file.read(&mut buf)?;
                Ok(buf)
            },
        ),
        // 3: by mid bytes
        Box::new(
            move |Meta {
                      path, size: total, ..
                  }| {
                let start: u64 = total / u64::try_from(sample_size)? / 2;
                let len: usize =
                    std::cmp::min(usize::try_from(*total)?, sample_size);
                let mut file = fs::File::open(path)?;
                file.seek(SeekFrom::Start(start))?;
                let mut buf = vec![0u8; len];
                file.read(&mut buf)?;
                Ok(buf)
            },
        ),
        // 4: by hash: xxh
        Box::new(move |m| {
            hash::xxh(&m.path, chunk_size).map(|h| h.to_le_bytes().to_vec())
        }),
    ];
    if enable_blake3_pass {
        // 5: by hash: blake3
        groupers.push(Box::new(move |m| hash::blake3(&m.path, chunk_size)));
    }
    if enable_sha2_512_pass {
        // 6: by hash: sha2-512
        groupers.push(Box::new(move |m| hash::sha2_512(&m.path, chunk_size)));
    }
    groupers
}
