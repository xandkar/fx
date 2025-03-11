use std::{
    collections::HashMap,
    ffi::OsString,
    fs,
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Context;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator,
};

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

    /// Skip all directories with this name.
    /// (This option can be used multiple times)
    #[clap(long)]
    skip_dir: Vec<OsString>,

    /// Skip all paths starting with this prefix.
    /// (This option can be used multiple times)
    #[clap(long)]
    skip_prefix: Vec<PathBuf>,

    /// Separate output lines/records with a null (\0)
    /// instead of linefeed (\n) character.
    #[clap(short = 'Z', long = "null")]
    null_line_sep: bool,

    /// Quote the outputted paths.
    #[clap(short, long = "quote")]
    quote_paths: bool,

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
            &self.skip_dir[..],
            &self.skip_prefix[..],
            self.quote_paths,
            self.null_line_sep,
        )?;
        Ok(())
    }
}

#[tracing::instrument(skip_all)]
pub fn dups(
    root_path: &Path,
    sample_size: usize,
    chunk_size: usize,
    enable_blake3_pass: bool,
    enable_sha2_512_pass: bool,
    skip_dirs: &[OsString],
    skip_prefixes: &[PathBuf],
    quote_paths: bool,
    null_line_sep: bool,
) -> anyhow::Result<()> {
    let mut groups: Vec<Vec<Meta>> = {
        let span = tracing::debug_span!("find_files");
        let _span_guard = span.enter();
        let files: Vec<Meta> = data::find_while_skipping(
            root_path,
            skip_dirs.to_vec(),
            skip_prefixes.to_vec(),
        )?
        .filter_map(|result| match result {
            Err(error) => {
                tracing::error!(?error, "Failure while finding files.");
                None
            }
            Ok(m) => Some(m),
        })
        .filter(Meta::is_regular_file)
        .filter(|Meta { size, .. }| *size > 0)
        .collect();
        tracing::debug!(files = files.len(), "Found.");
        vec![files]
    };

    // TODO First pass should be group by (dev, inode) - which is 100%
    //      certainty, but is a special case in that even though it is
    //      most certain it is also cheapest.

    for (span, f) in groupers(
        sample_size,
        chunk_size,
        enable_blake3_pass,
        enable_sha2_512_pass,
    ) {
        groups = refine(span, &groups, f)?;
    }

    // TODO Optional last pass should be byte-by-bye comparisson.

    let sep = if null_line_sep { "\0" } else { "\n" }.to_string();
    tracing::debug!(groups = groups.len(), ?sep, "Reporting.");
    for group in groups {
        // TODO Lister grouper outputs.
        for file in group {
            if quote_paths {
                print!("{:?}{}", &file.path, sep);
            } else {
                print!("{}{}", &file.path.display(), sep);
            }
        }
        println!();
    }

    Ok(())
}

fn refine<F>(
    span: tracing::Span,
    groups: &Vec<Vec<Meta>>,
    grouper: F,
) -> anyhow::Result<Vec<Vec<Meta>>>
where
    F: Send + Sync + Fn(&Meta) -> anyhow::Result<Vec<u8>>,
{
    let _span_guard = span.enter();
    tracing::debug!(groups = groups.len(), "Refining.");
    let grouper = Arc::new(grouper);
    let refined_groups: Vec<Vec<Meta>> = groups
        .par_iter()
        .enumerate()
        .map({
            |(group_seq, group)| {
                let parent_span = span.clone();
                let _parent_span_guard = parent_span.enter();
                let group_span = tracing::trace_span!(
                    "group",
                    seq = group_seq,
                    mem = group.len()
                );
                let _group_span_guard = group_span.enter();
                let mut refined_groups: HashMap<Vec<u8>, Vec<Meta>> =
                    HashMap::new();
                for (id, member) in group
                    .par_iter()
                    .filter_map(|member| {
                        let group_span = group_span.clone();
                        let _group_span_guard = group_span.enter();
                        let member_span = tracing::trace_span!(
                            "member",
                            path = ?member.path,
                            size = member.size,
                        );
                        let _member_span_guard = member_span.enter();
                        match grouper(member) {
                            Err(error) => {
                                tracing::error!(
                                    ?error,
                                    file = ?member.path,
                                    "Failed to process."
                                );
                                None
                            }
                            Ok(id) => Some((id, member.clone())),
                        }
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
) -> Vec<(
    tracing::Span,
    Box<dyn Send + Sync + Fn(&Meta) -> anyhow::Result<Vec<u8>>>,
)> {
    let mut groupers: Vec<(
        tracing::Span,
        Box<dyn Send + Sync + Fn(&Meta) -> anyhow::Result<Vec<u8>>>,
    )> = vec![
        // 1: by size
        (
            tracing::debug_span!("group_by_size"),
            Box::new(|m| Ok(m.size.to_le_bytes().to_vec())),
        ),
        // 2: by head bytes
        (
            tracing::debug_span!("group_by_sample_head"),
            Box::new(move |m| read_head(m, sample_size)),
        ),
        // 3: by mid bytes
        (
            tracing::debug_span!("group_by_sample_mid"),
            Box::new(move |m| read_mid(m, sample_size)),
        ),
        // 4: by hash: xxh
        (
            tracing::debug_span!("group_by_hash_xxh"),
            Box::new(move |m| {
                hash::xxh(&m.path, chunk_size)
                    .map(|h| h.to_le_bytes().to_vec())
            }),
        ),
    ];
    if enable_blake3_pass {
        // 5: by hash: blake3
        groupers.push((
            tracing::debug_span!("group_by_hash_blake3"),
            Box::new(move |m| hash::blake3(&m.path, chunk_size)),
        ));
    }
    if enable_sha2_512_pass {
        // 6: by hash: sha2-512
        groupers.push((
            tracing::debug_span!("group_by_hash_sha2-512"),
            Box::new(move |m| hash::sha2_512(&m.path, chunk_size)),
        ));
    }
    groupers
}

fn read_head(
    Meta {
        path, size: total, ..
    }: &Meta,
    sample_size: usize,
) -> anyhow::Result<Vec<u8>> {
    let offset = SeekFrom::Start(0);
    let total = usize::try_from(*total)?;
    let amount = std::cmp::min(total, sample_size);
    let data = read(path, amount, offset)?;
    Ok(data)
}

fn read_mid(
    Meta {
        path, size: total, ..
    }: &Meta,
    sample_size: usize,
) -> anyhow::Result<Vec<u8>> {
    let offset = SeekFrom::Start(total / u64::try_from(sample_size)? / 2);
    let total = usize::try_from(*total)?;
    let amount: usize = std::cmp::min(total, sample_size);
    let data = read(path, amount, offset)?;
    Ok(data)
}

#[tracing::instrument(level = "trace")]
fn read(path: &Path, amount: usize, offset: SeekFrom) -> io::Result<Vec<u8>> {
    let mut file = fs::File::open(path)?;
    file.seek(offset)?;
    let mut buf = vec![0u8; amount];
    let mut read_total = 0;
    while read_total < amount {
        match file.read(&mut buf[read_total..]) {
            // File could've been modified after we determined the amount.
            Ok(0) => {
                tracing::warn!(
                    ?path,
                    amount,
                    read_total,
                    "Reached EOF sooner than expected."
                );
                break;
            }
            Ok(read_current) => {
                read_total += read_current;
            }
            Err(e) => {
                if let io::ErrorKind::Interrupted = e.kind() {
                    tracing::warn!(
                        ?path,
                        read_total,
                        "File read interrupted. Retrying."
                    );
                    continue;
                } else {
                    return Err(e);
                }
            }
        }
    }
    Ok(buf)
}
