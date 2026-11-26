use std::{
    collections::HashSet,
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
const HEADER_V1: [u8; 8] = *b"MINIMAN\x01";
const HEADER_V2: [u8; 8] = *b"MINIMAN\x02";
const IO_READ: &str = "read manifest";

#[derive(Clone, Default)]
pub(crate) struct Manifest {
    pub(crate) generation: u64,
    pub(crate) highest_file_id: u64,
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
        let fixed_len = match header {
            HEADER_V1 => 28,
            HEADER_V2 => 36,
            _ => return Err(corrupt("invalid magic or unsupported version")),
        };
        if len < fixed_len {
            return Err(corrupt("invalid file length"));
        }
        let mut crc = Hasher::new();
        crc.update(&header);
        let raw_generation = read_array(&mut reader, IO_READ)?;
        crc.update(&raw_generation);
        let generation = u64::from_le_bytes(raw_generation);
        let highest_file_id = if header == HEADER_V2 {
            let raw_id = read_array(&mut reader, IO_READ)?;
            crc.update(&raw_id);
            u64::from_le_bytes(raw_id)
        } else {
            generation
        };
        if generation > highest_file_id {
            return Err(corrupt("WAL generation exceeds file ID watermark"));
        }
        let raw_count = read_array(&mut reader, IO_READ)?;
        crc.update(&raw_count);
        let count = u64::from_le_bytes(raw_count);
        if count != (len - fixed_len) / 8
            || count > highest_file_id
            || (generation == 0 && count != 0)
        {
            return Err(corrupt("invalid table count"));
        }

        let mut seen = HashSet::new();
        let mut table_ids = Vec::new();
        for _ in 0..count {
            let raw_id = read_array(&mut reader, IO_READ)?;
            let id = u64::from_le_bytes(raw_id);
            if id == 0
                || id > highest_file_id
                || !seen.insert(id)
                || (header == HEADER_V1 && id <= table_ids.last().copied().unwrap_or(0))
            {
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
            highest_file_id,
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
        write(&HEADER_V2).map_err(|error| Error::io("write manifest", error))?;
        write(&self.generation.to_le_bytes())
            .map_err(|error| Error::io("write manifest", error))?;
        write(&self.highest_file_id.to_le_bytes())
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
