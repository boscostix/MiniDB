use std::{
    fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crc32fast::Hasher;

use crate::{
    Error, Result,
    memtable::{Entry, MemTable},
    storage::{numbered_file_id, read_array, read_vec, stage_file},
};

const HEADER: [u8; 8] = *b"MINISST\x01";
const FILE_HEADER_LEN: u64 = 16;
const FOOTER_LEN: u64 = 12;
const CHECKSUM_LEN: u64 = 4;
const RECORD_HEADER_LEN: usize = 9;
const BLOCK_TARGET_BYTES: u64 = 4096;
const BLOCK_MIN_BYTES: u64 = RECORD_HEADER_LEN as u64 + 1 + CHECKSUM_LEN;
const BLOCK_MAX_BYTES: u64 = RECORD_HEADER_LEN as u64 + 2 * u32::MAX as u64 + CHECKSUM_LEN;
const IO_READ: &str = "read SSTable";
const IO_WRITE: &str = "write SSTable";
const PUT: u8 = 1;
const DELETE: u8 = 2;

struct BlockIndex {
    first_key: Vec<u8>,
    offset: u64,
}

pub(crate) struct Sstable {
    path: PathBuf,
    id: u64,
    index: Vec<BlockIndex>,
    index_offset: u64,
}

impl Sstable {
    pub(crate) fn path(directory: &Path, id: u64) -> PathBuf {
        directory.join(format!("sst-{id:020}.sst"))
    }

    pub(crate) fn numbered_id(name: &str) -> Option<u64> {
        numbered_file_id(name, "sst-", ".sst")
    }

    pub(crate) fn create(
        directory: &Path,
        id: u64,
        table: &MemTable,
        on_construction: impl FnOnce() -> Result<()>,
    ) -> Result<Self> {
        let path = Self::path(directory, id);
        let (temporary, mut file) = stage_file(&path)?;
        file.write_all(&HEADER)
            .and_then(|()| file.write_all(&id.to_le_bytes()))
            .map_err(|error| Error::io(IO_WRITE, error))?;
        on_construction()?;

        let mut writer = BufWriter::new(file);
        write_table(&mut writer, table).map_err(|error| Error::io(IO_WRITE, error))?;
        writer
            .flush()
            .and_then(|()| writer.get_ref().sync_all())
            .map_err(|error| Error::io("synchronize SSTable", error))?;
        drop(writer);
        fs::rename(temporary, path).map_err(|error| Error::io("publish SSTable", error))?;
        Self::open(directory, id)
    }

    pub(crate) fn open(directory: &Path, id: u64) -> Result<Self> {
        let path = Self::path(directory, id);
        let mut file = File::open(&path)
            .map_err(|error| Error::io("open manifest-referenced SSTable", error))?;
        let len = file
            .metadata()
            .map_err(|error| Error::io("read SSTable metadata", error))?
            .len();
        if len < FILE_HEADER_LEN + BLOCK_MIN_BYTES + 8 + 13 + FOOTER_LEN {
            return Err(corrupt(id, "file is too short"));
        }
        let header = read_array::<8>(&mut file, IO_READ)?;
        if header != HEADER {
            return Err(corrupt(id, "invalid magic or unsupported version"));
        }
        if u64::from_le_bytes(read_array(&mut file, IO_READ)?) != id {
            return Err(corrupt(id, "table ID does not match manifest"));
        }

        let footer_offset = len - FOOTER_LEN;
        seek(&mut file, footer_offset)?;
        let index_address = read_array(&mut file, IO_READ)?;
        let index_offset = u64::from_le_bytes(index_address);
        let expected_checksum = u32::from_le_bytes(read_array(&mut file, IO_READ)?);
        if !(FILE_HEADER_LEN + BLOCK_MIN_BYTES..=footer_offset - 21).contains(&index_offset) {
            return Err(corrupt(id, "index position is outside table"));
        }
        seek(&mut file, index_offset)?;
        let index_len = footer_offset - index_offset;
        let index = read_index(
            BufReader::new((&mut file).take(index_len)),
            index_len,
            index_offset,
            id,
            index_address,
            expected_checksum,
        )?;
        Ok(Self {
            path,
            id,
            index,
            index_offset,
        })
    }

    pub(crate) fn get(&self, key: &[u8]) -> Result<Option<Entry>> {
        let candidates = self
            .index
            .partition_point(|item| item.first_key.as_slice() <= key);
        let Some(block_number) = candidates.checked_sub(1) else {
            return Ok(None);
        };
        let block = &self.index[block_number];
        let next = self.index.get(candidates);
        let end = next.map_or(self.index_offset, |item| item.offset);
        let mut file =
            File::open(&self.path).map_err(|error| Error::io("open SSTable for lookup", error))?;
        seek(&mut file, block.offset)?;
        let bytes = read_vec(&mut file, end - block.offset - CHECKSUM_LEN, IO_READ)?;
        let checksum = u32::from_le_bytes(read_array(&mut file, IO_READ)?);
        if crc32fast::hash(&bytes) != checksum {
            return Err(corrupt(self.id, "block checksum mismatch"));
        }
        lookup_block(
            &bytes,
            key,
            &block.first_key,
            next.map(|item| item.first_key.as_slice()),
            self.id,
        )
    }
}

fn write_table(writer: &mut impl Write, table: &MemTable) -> io::Result<()> {
    let mut index = Vec::new();
    let mut offset = FILE_HEADER_LEN;
    let mut block_start = offset;
    let mut block_crc = Hasher::new();

    for (key, entry) in table.iter() {
        let value = match entry {
            Entry::Value(value) => value.as_slice(),
            Entry::Tombstone => &[],
        };
        let entry_bytes = (RECORD_HEADER_LEN + key.len() + value.len()) as u64;
        if offset != block_start && offset - block_start + entry_bytes > BLOCK_TARGET_BYTES {
            writer.write_all(&block_crc.finalize().to_le_bytes())?;
            offset += CHECKSUM_LEN;
            block_start = offset;
            block_crc = Hasher::new();
        }
        if offset == block_start {
            index.push((key, offset));
        }

        let mut record = [0; RECORD_HEADER_LEN];
        record[0] = match entry {
            Entry::Value(_) => PUT,
            Entry::Tombstone => DELETE,
        };
        record[1..5].copy_from_slice(&(key.len() as u32).to_le_bytes());
        record[5..9].copy_from_slice(&(value.len() as u32).to_le_bytes());
        write_hashed(writer, &mut block_crc, &record)?;
        write_hashed(writer, &mut block_crc, key)?;
        write_hashed(writer, &mut block_crc, value)?;
        offset += entry_bytes;
    }
    writer.write_all(&block_crc.finalize().to_le_bytes())?;

    let index_offset = offset + CHECKSUM_LEN;
    let mut index_crc = Hasher::new();
    write_hashed(writer, &mut index_crc, &(index.len() as u64).to_le_bytes())?;
    for (key, offset) in index {
        write_hashed(writer, &mut index_crc, &offset.to_le_bytes())?;
        write_hashed(writer, &mut index_crc, &(key.len() as u32).to_le_bytes())?;
        write_hashed(writer, &mut index_crc, key)?;
    }
    write_hashed(writer, &mut index_crc, &index_offset.to_le_bytes())?;
    writer.write_all(&index_crc.finalize().to_le_bytes())
}

fn read_index(
    mut reader: impl Read,
    len: u64,
    index_offset: u64,
    id: u64,
    index_address: [u8; 8],
    expected_checksum: u32,
) -> Result<Vec<BlockIndex>> {
    let mut crc = Hasher::new();
    let count = read_array(&mut reader, IO_READ)?;
    crc.update(&count);
    let count = u64::from_le_bytes(count);
    let mut remaining = len - 8;
    if count == 0 || count > remaining / 13 {
        return Err(corrupt(id, "invalid index block count"));
    }

    let mut index: Vec<BlockIndex> = Vec::new();
    for _ in 0..count {
        if remaining < 13 {
            return Err(corrupt(id, "incomplete index entry"));
        }
        let raw_offset = read_array(&mut reader, IO_READ)?;
        let raw_key_len = read_array(&mut reader, IO_READ)?;
        let offset = u64::from_le_bytes(raw_offset);
        let key_len = u64::from(u32::from_le_bytes(raw_key_len));
        remaining -= 12;
        if key_len == 0 || key_len > remaining {
            return Err(corrupt(id, "invalid index key length"));
        }
        if offset >= index_offset {
            return Err(corrupt(id, "block starts outside data area"));
        }

        let first_key = read_vec(&mut reader, key_len, IO_READ)?;
        if let Some(previous) = index.last() {
            validate_block_size(previous.offset, offset, id)?;
            if previous.first_key >= first_key {
                return Err(corrupt(id, "index keys are not strictly ordered"));
            }
        } else if offset != FILE_HEADER_LEN {
            return Err(corrupt(id, "first block does not follow table header"));
        }
        remaining -= key_len;
        crc.update(&raw_offset);
        crc.update(&raw_key_len);
        crc.update(&first_key);
        index.push(BlockIndex { first_key, offset });
    }
    if remaining != 0 {
        return Err(corrupt(id, "unexpected bytes at end of index"));
    }
    if let Some(last) = index.last() {
        validate_block_size(last.offset, index_offset, id)?;
    }
    crc.update(&index_address);
    if crc.finalize() != expected_checksum {
        return Err(corrupt(id, "index checksum mismatch"));
    }
    Ok(index)
}

fn validate_block_size(start: u64, end: u64, id: u64) -> Result<()> {
    if !end
        .checked_sub(start)
        .is_some_and(|len| (BLOCK_MIN_BYTES..=BLOCK_MAX_BYTES).contains(&len))
    {
        return Err(corrupt(id, "invalid block bounds"));
    }
    Ok(())
}

fn lookup_block(
    mut bytes: &[u8],
    wanted: &[u8],
    first_key: &[u8],
    next_key: Option<&[u8]>,
    id: u64,
) -> Result<Option<Entry>> {
    let mut previous = None;
    let mut found = None;
    while !bytes.is_empty() {
        let Some((&[kind, k0, k1, k2, k3, v0, v1, v2, v3], payload)) =
            bytes.split_at_checked(RECORD_HEADER_LEN)
        else {
            return Err(corrupt(id, "incomplete block record header"));
        };
        let key_len = u32::from_le_bytes([k0, k1, k2, k3]) as usize;
        let value_len = u32::from_le_bytes([v0, v1, v2, v3]) as usize;
        if key_len == 0 || !matches!(kind, PUT | DELETE) || (kind == DELETE && value_len != 0) {
            return Err(corrupt(id, "invalid block record fields"));
        }
        let Some((key, payload)) = payload.split_at_checked(key_len) else {
            return Err(corrupt(id, "record key exceeds block"));
        };
        let Some((value, tail)) = payload.split_at_checked(value_len) else {
            return Err(corrupt(id, "record value exceeds block"));
        };
        if previous.map_or(key != first_key, |previous| key <= previous)
            || next_key.is_some_and(|next| key >= next)
        {
            return Err(corrupt(id, "block keys disagree with index or order"));
        }
        if key == wanted {
            found = Some(if kind == PUT {
                Entry::Value(value.to_vec())
            } else {
                Entry::Tombstone
            });
        }
        previous = Some(key);
        bytes = tail;
    }
    Ok(found)
}

fn seek(file: &mut File, offset: u64) -> Result<()> {
    file.seek(SeekFrom::Start(offset))
        .map_err(|error| Error::io("seek SSTable", error))?;
    Ok(())
}

fn write_hashed(writer: &mut impl Write, crc: &mut Hasher, bytes: &[u8]) -> io::Result<()> {
    writer.write_all(bytes)?;
    crc.update(bytes);
    Ok(())
}

fn corrupt(id: u64, reason: &'static str) -> Error {
    Error::CorruptSstable { id, reason }
}
