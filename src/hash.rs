use std::{fs, hash::Hasher, io::Read, path::Path};

pub fn xxh(path: &Path, chunk_size: usize) -> anyhow::Result<u64> {
    use twox_hash::XxHash3_64;

    let mut file = fs::File::open(path)?;
    let mut buff = vec![0u8; chunk_size];
    let mut hash = XxHash3_64::new();
    loop {
        let n = file.read(&mut buff)?;
        if n == 0 {
            break;
        }
        let chunk = &buff[..n];
        hash.write(chunk);
    }
    Ok(hash.finish())
}

pub fn blake3(path: &Path, chunk_size: usize) -> anyhow::Result<Vec<u8>> {
    let mut file = fs::File::open(path)?;
    let mut buff = vec![0u8; chunk_size];
    let mut hash = blake3::Hasher::new();
    loop {
        let n = file.read(&mut buff)?;
        if n == 0 {
            break;
        }
        let chunk = &buff[..n];
        hash.update(chunk);
    }
    let hash = hash.finalize().as_bytes().to_vec();
    Ok(hash)
}

pub fn sha2_512(path: &Path, chunk_size: usize) -> anyhow::Result<Vec<u8>> {
    use sha2::Digest;

    let mut file = fs::File::open(path)?;
    let mut buff = vec![0u8; chunk_size];
    let mut hash = sha2::Sha512::new();
    loop {
        let n = file.read(&mut buff)?;
        if n == 0 {
            break;
        }
        let chunk = &buff[..n];
        hash.update(chunk);
    }
    let hash = hash.finalize().to_vec();
    Ok(hash)
}
