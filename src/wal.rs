use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufReader, Read, Write},
    path::{Path, PathBuf},
};

use crc32fast::Hasher;

use crate::{
    Error, Result,
    memtable::{Entry, MemTable},
    storage::{numbered_file_id, stage_file},
};

const WAL_NAME: &str = "wal.log";
const MAGIC: &[u8; 6] = b"MINIDB";
const VERSION: u16 = 1;
const FILE_HEADER_LEN: usize = MAGIC.len() + 2;
const RECORD_FIELDS_LEN: usize = 9;
const CHECKSUM_LEN: usize = 4;
const RECORD_HEADER_LEN: usize = RECORD_FIELDS_LEN + CHECKSUM_LEN;
const PUT: u8 = 1;
const DELETE: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WalStage {
    HeaderWritten,
    BeforeSync,
    AfterSync,
}

pub(crate) struct Wal {
    file: File,
    write_failed: bool,
}

impl Wal {
    pub(crate) const MAX_FIELD_LEN: usize = u32::MAX as usize;

    pub(crate) fn open_legacy(directory: &Path, memtable: &mut MemTable) -> Result<Self> {
        match open_wal_file(&Self::path(directory, 0)) {
            Ok(file) => Self::recover(file, memtable),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Self::create(directory, 0),
            Err(error) => Err(Error::io("open legacy WAL", error)),
        }
    }

    pub(crate) fn open_existing(
        directory: &Path,
        generation: u64,
        memtable: &mut MemTable,
    ) -> Result<Self> {
        let file = open_wal_file(&Self::path(directory, generation))
            .map_err(|error| Error::io("open manifest-referenced WAL", error))?;
        Self::recover(file, memtable)
    }

    fn recover(mut file: File, memtable: &mut MemTable) -> Result<Self> {
        let length = file
            .metadata()
            .map_err(|error| Error::io("read WAL metadata", error))?
            .len();
        let end = replay(&mut BufReader::new(&mut file), length, memtable)?;
        if end < length {
            file.set_len(end)
                .map_err(|error| Error::io("truncate incomplete WAL record", error))?;
        }
        file.sync_all()
            .map_err(|error| Error::io("synchronize recovered WAL", error))?;

        Ok(Self {
            file,
            write_failed: false,
        })
    }

    pub(crate) fn create(directory: &Path, generation: u64) -> Result<Self> {
        let path = Self::path(directory, generation);
        let (temporary, mut file) = stage_file(&path)?;
        file.write_all(MAGIC)
            .and_then(|()| file.write_all(&VERSION.to_le_bytes()))
            .and_then(|()| file.sync_all())
            .map_err(|error| Error::io("write and synchronize WAL header", error))?;
        fs::rename(temporary, &path).map_err(|error| Error::io("publish new WAL", error))?;
        let file = open_wal_file(&path).map_err(|error| Error::io("open new WAL", error))?;
        Ok(Self {
            file,
            write_failed: false,
        })
    }

    pub(crate) fn path(directory: &Path, generation: u64) -> PathBuf {
        if generation == 0 {
            directory.join(WAL_NAME)
        } else {
            directory.join(format!("wal-{generation:020}.log"))
        }
    }

    pub(crate) fn numbered_id(name: &str) -> Option<u64> {
        numbered_file_id(name, "wal-", ".log")
    }

    pub(crate) fn ensure_writable(&self) -> Result<()> {
        if self.write_failed {
            return Err(Error::WalWriteFailed);
        }
        Ok(())
    }

    pub(crate) fn append_with(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        hook: impl FnMut(WalStage) -> io::Result<()>,
    ) -> Result<()> {
        self.append_and_sync(key, value, hook, File::sync_all)
    }

    fn append_and_sync(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        mut hook: impl FnMut(WalStage) -> io::Result<()>,
        sync: impl FnOnce(&File) -> io::Result<()>,
    ) -> Result<()> {
        self.ensure_writable()?;
        let kind = if value.is_some() { PUT } else { DELETE };
        let value = value.unwrap_or_default();
        let key_len = u32::try_from(key.len()).map_err(|_| Error::KeyTooLarge {
            len: key.len(),
            max: Self::MAX_FIELD_LEN,
        })?;
        let value_len = u32::try_from(value.len()).map_err(|_| Error::ValueTooLarge {
            len: value.len(),
            max: Self::MAX_FIELD_LEN,
        })?;

        let mut header = [0; RECORD_HEADER_LEN];
        header[0] = kind;
        header[1..5].copy_from_slice(&key_len.to_le_bytes());
        header[5..9].copy_from_slice(&value_len.to_le_bytes());
        let header_crc = crc32fast::hash(&header[..RECORD_FIELDS_LEN]);
        header[RECORD_FIELDS_LEN..].copy_from_slice(&header_crc.to_le_bytes());

        // Until append and sync both succeed, the on-disk tail is uncertain.
        self.write_failed = true;
        let result = self
            .file
            .write_all(&header)
            .and_then(|()| hook(WalStage::HeaderWritten))
            .and_then(|()| self.file.write_all(key))
            .and_then(|()| self.file.write_all(value))
            .and_then(|()| self.file.write_all(&payload_crc(key, value)))
            .map_err(|error| Error::io("append WAL record", error))
            .and_then(|()| {
                hook(WalStage::BeforeSync)
                    .and_then(|()| sync(&self.file))
                    .and_then(|()| hook(WalStage::AfterSync))
                    .map_err(|error| Error::io("synchronize WAL record", error))
            });

        self.write_failed = result.is_err();
        result
    }
}

fn open_wal_file(path: &Path) -> io::Result<File> {
    OpenOptions::new().read(true).append(true).open(path)
}

fn replay(reader: &mut impl Read, length: u64, memtable: &mut MemTable) -> Result<u64> {
    if length < FILE_HEADER_LEN as u64 {
        return Err(corrupt(0, "incomplete file header"));
    }
    let mut magic = [0; MAGIC.len()];
    let mut version = [0; 2];
    read_exact(reader, &mut magic)?;
    read_exact(reader, &mut version)?;
    if &magic != MAGIC {
        return Err(corrupt(0, "invalid file magic"));
    }
    let version = u16::from_le_bytes(version);
    if version != VERSION {
        return Err(Error::UnsupportedWalVersion { version });
    }

    let mut offset = FILE_HEADER_LEN as u64;
    while offset < length {
        let remaining = length - offset;
        let available = remaining.min(RECORD_HEADER_LEN as u64) as usize;
        let mut header = [0; RECORD_HEADER_LEN];
        read_exact(reader, &mut header[..available])?;
        validate_header(&header[..available], offset)?;
        if available < RECORD_HEADER_LEN {
            break;
        }

        let [kind, k0, k1, k2, k3, v0, v1, v2, v3, ..] = header;
        let key_len = u32::from_le_bytes([k0, k1, k2, k3]);
        let value_len = u32::from_le_bytes([v0, v1, v2, v3]);
        let record_len =
            (RECORD_HEADER_LEN + CHECKSUM_LEN) as u64 + u64::from(key_len) + u64::from(value_len);
        if record_len > remaining {
            break;
        }

        let key = read_bytes(reader, key_len as usize)?;
        let value = read_bytes(reader, value_len as usize)?;
        let mut checksum = [0; CHECKSUM_LEN];
        read_exact(reader, &mut checksum)?;
        if checksum != payload_crc(&key, &value) {
            return Err(corrupt(offset, "record payload checksum mismatch"));
        }
        let entry = if kind == PUT {
            Entry::Value(value)
        } else {
            Entry::Tombstone
        };
        memtable.insert(key, entry);
        offset += record_len;
    }
    Ok(offset)
}

fn validate_header(header: &[u8], offset: u64) -> Result<()> {
    if !matches!(header[0], PUT | DELETE) {
        return Err(corrupt(offset, "unknown record type"));
    }
    if header.get(1..5) == Some(&[0; 4]) {
        return Err(corrupt(offset, "empty key"));
    }
    if header[0] == DELETE
        && header.len() > 5
        && header[5..header.len().min(RECORD_FIELDS_LEN)]
            .iter()
            .any(|&byte| byte != 0)
    {
        return Err(corrupt(offset, "deletion record contains a value"));
    }
    if header.len() > RECORD_FIELDS_LEN {
        let expected = crc32fast::hash(&header[..RECORD_FIELDS_LEN]).to_le_bytes();
        if !expected.starts_with(&header[RECORD_FIELDS_LEN..]) {
            return Err(corrupt(offset, "record header checksum mismatch"));
        }
    }
    Ok(())
}

fn read_bytes(reader: &mut impl Read, len: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(len)
        .map_err(|_| Error::WalAllocationFailed { bytes: len })?;
    bytes.resize(len, 0);
    read_exact(reader, &mut bytes)?;
    Ok(bytes)
}

fn read_exact(reader: &mut impl Read, output: &mut [u8]) -> Result<()> {
    reader
        .read_exact(output)
        .map_err(|error| Error::io("read WAL", error))
}

fn payload_crc(key: &[u8], value: &[u8]) -> [u8; CHECKSUM_LEN] {
    let mut crc = Hasher::new();
    crc.update(key);
    crc.update(value);
    crc.finalize().to_le_bytes()
}

fn corrupt(offset: u64, reason: &'static str) -> Error {
    Error::CorruptWal { offset, reason }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_append_poisons_writes() {
        let directory = tempfile::tempdir().unwrap();
        let mut wal = Wal::open_legacy(directory.path(), &mut MemTable::default()).unwrap();
        wal.file = File::open(directory.path().join(WAL_NAME)).unwrap();

        assert!(matches!(
            wal.append_with(b"key", Some(b"value"), |_| Ok(())),
            Err(Error::Io {
                operation: "append WAL record",
                ..
            })
        ));
        assert_eq!(
            wal.append_with(b"key", None, |_| Ok(())),
            Err(Error::WalWriteFailed)
        );
    }

    #[test]
    fn failed_sync_poisons_writes() {
        let directory = tempfile::tempdir().unwrap();
        let mut wal = Wal::open_legacy(directory.path(), &mut MemTable::default()).unwrap();

        assert!(matches!(
            wal.append_and_sync(
                b"key",
                Some(b"value"),
                |_| Ok(()),
                |_| { Err(io::Error::other("simulated synchronization failure")) }
            ),
            Err(Error::Io {
                operation: "synchronize WAL record",
                ..
            })
        ));
        assert_eq!(
            wal.append_with(b"key", None, |_| Ok(())),
            Err(Error::WalWriteFailed)
        );
    }
}
