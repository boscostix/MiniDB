use std::{
    fs::{self, File},
    io::{self, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

use crc32fast::Hasher;

use crate::{
    Error, Result,
    storage::{read_array, stage_file},
};

const NAME: &str = "MANIFEST";
const HEADER: [u8; 8] = *b"MINIMAN\x01";
const IO_READ: &str = "read manifest";

#[derive(Clone, Default)]
pub(crate) struct Manifest {
    pub(crate) generation: u64,
    pub(crate) table_ids: Vec<u64>,
}

impl Manifest {
    pub(crate) fn load(directory: &Path) -> Result<Option<Self>> {
        let file = match File::open(directory.join(NAME)) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(Error::io("open manifest", error)),
        };
        let len = file
            .metadata()
            .map_err(|error| Error::io("read manifest metadata", error))?
            .len();
        if len < 28 || (len - 28) % 8 != 0 {
            return Err(corrupt("invalid file length"));
        }
        let mut reader = BufReader::new(file);
        let header = read_array::<8>(&mut reader, IO_READ)?;
        if header != HEADER {
            return Err(corrupt("invalid magic or unsupported version"));
        }
        let raw_generation = read_array(&mut reader, IO_READ)?;
        let raw_count = read_array(&mut reader, IO_READ)?;
        let generation = u64::from_le_bytes(raw_generation);
        let count = u64::from_le_bytes(raw_count);
        if count != (len - 28) / 8 || count > generation {
            return Err(corrupt("invalid table count"));
        }

        let mut crc = Hasher::new();
        crc.update(&header);
        crc.update(&raw_generation);
        crc.update(&raw_count);
        let mut table_ids = Vec::new();
        for _ in 0..count {
            let raw_id = read_array(&mut reader, IO_READ)?;
            let id = u64::from_le_bytes(raw_id);
            if id <= table_ids.last().copied().unwrap_or(0) || id > generation {
                return Err(corrupt("invalid table generation or ordering"));
            }
            crc.update(&raw_id);
            table_ids.push(id);
        }
        let checksum = u32::from_le_bytes(read_array(&mut reader, IO_READ)?);
        if crc.finalize() != checksum {
            return Err(corrupt("checksum mismatch"));
        }
        Ok(Some(Self {
            generation,
            table_ids,
        }))
    }

    pub(crate) fn stage(&self, directory: &Path) -> Result<PathBuf> {
        let (temporary, file) = stage_file(&directory.join(NAME))?;
        let mut writer = BufWriter::new(file);
        let mut crc = Hasher::new();
        let mut write = |bytes: &[u8]| {
            crc.update(bytes);
            writer.write_all(bytes)
        };
        write(&HEADER).map_err(|error| Error::io("write manifest", error))?;
        write(&self.generation.to_le_bytes())
            .map_err(|error| Error::io("write manifest", error))?;
        write(&(self.table_ids.len() as u64).to_le_bytes())
            .map_err(|error| Error::io("write manifest", error))?;
        for &id in &self.table_ids {
            write(&id.to_le_bytes()).map_err(|error| Error::io("write manifest", error))?;
        }
        writer
            .write_all(&crc.finalize().to_le_bytes())
            .and_then(|()| writer.flush())
            .and_then(|()| writer.get_ref().sync_all())
            .map_err(|error| Error::io("write and synchronize manifest", error))?;
        Ok(temporary)
    }

    pub(crate) fn publish(directory: &Path, temporary: &Path) -> Result<()> {
        fs::rename(temporary, directory.join(NAME))
            .map_err(|error| Error::io("publish manifest", error))
    }
}

pub(crate) fn orphaned_without_manifest() -> Error {
    corrupt("manifest is missing but numbered storage files exist")
}

fn corrupt(reason: &'static str) -> Error {
    Error::CorruptManifest { reason }
}
