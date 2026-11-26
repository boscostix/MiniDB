use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read},
    path::{Path, PathBuf},
};

use crate::{Error, Result};

pub(crate) fn stage_file(path: &Path) -> Result<(PathBuf, File)> {
    let temporary = staging_path(path);
    remove_file_if_exists(&temporary)
        .map_err(|error| Error::io("remove stale staging file", error))?;
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|error| Error::io("create staging file", error))?;
    Ok((temporary, file))
}

pub(crate) fn staging_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".tmp");
    name.into()
}

pub(crate) fn remove_file_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

pub(crate) fn sync_directory(directory: &Path, operation: &'static str) -> Result<()> {
    File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(|error| Error::io(operation, error))
}

pub(crate) fn numbered_file_id(name: &str, prefix: &str, suffix: &str) -> Option<u64> {
    let digits = name.strip_prefix(prefix)?.strip_suffix(suffix)?;
    if digits.len() != 20 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok().filter(|&id| id > 0)
}

pub(crate) fn read_array<const N: usize>(
    reader: &mut impl Read,
    operation: &'static str,
) -> Result<[u8; N]> {
    let mut bytes = [0; N];
    reader
        .read_exact(&mut bytes)
        .map_err(|error| Error::io(operation, error))?;
    Ok(bytes)
}

pub(crate) fn read_vec(
    reader: &mut impl Read,
    len: u64,
    operation: &'static str,
) -> Result<Vec<u8>> {
    let allocation_error = || {
        Error::io(
            operation,
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("cannot allocate {len} bytes"),
            ),
        )
    };
    let len = usize::try_from(len).map_err(|_| allocation_error())?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(len)
        .map_err(|_| allocation_error())?;
    bytes.resize(len, 0);
    reader
        .read_exact(&mut bytes)
        .map_err(|error| Error::io(operation, error))?;
    Ok(bytes)
}
