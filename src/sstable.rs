use std::{
    fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    ops::Range,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};

use crc32fast::Hasher;

use crate::{
    Error, Result,
    memtable::{Entry, MemTable},
    storage::{numbered_file_id, read_array, read_vec, remove_file_if_exists, stage_file},
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
    retired: AtomicBool,
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
        let table = Self::build(directory, id, |writer| {
            on_construction()?;
            for (key, entry) in table.iter() {
                let value = match entry {
                    Entry::Value(value) => Some(value.as_slice()),
                    Entry::Tombstone => None,
                };
                writer.push(key, value)?;
            }
            Ok(())
        })?;
        Ok(table.expect("a nonempty MemTable always produces an SSTable"))
    }

    pub(crate) fn build(
        directory: &Path,
        id: u64,
        write_records: impl FnOnce(&mut TableWriter) -> Result<()>,
    ) -> Result<Option<Self>> {
        let path = Self::path(directory, id);
        let (temporary, mut file) = stage_file(&path)?;
        file.write_all(&HEADER)
            .and_then(|()| file.write_all(&id.to_le_bytes()))
            .map_err(|error| Error::io(IO_WRITE, error))?;
        let mut writer = TableWriter {
            file: BufWriter::new(file),
            index: Vec::new(),
            offset: FILE_HEADER_LEN,
            block_crc: Hasher::new(),
        };
        write_records(&mut writer)?;
        if writer.index.is_empty() {
            drop(writer);
            remove_file_if_exists(&temporary)
                .map_err(|error| Error::io("remove empty staged SSTable", error))?;
            return Ok(None);
        }

        let index_offset = writer
            .finish()
            .map_err(|error| Error::io("finish and synchronize SSTable", error))?;
        let TableWriter { file, index, .. } = writer;
        drop(file);
        fs::rename(temporary, &path).map_err(|error| Error::io("publish SSTable", error))?;
        Ok(Some(Self {
            path,
            id,
            index,
            index_offset,
            retired: AtomicBool::new(false),
        }))
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn retire(&self) {
        self.retired.store(true, Ordering::Relaxed);
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
            retired: AtomicBool::new(false),
        })
    }

    pub(crate) fn get(&self, key: &[u8]) -> Result<Option<Entry>> {
        let candidates = self
            .index
            .partition_point(|item| item.first_key.as_slice() <= key);
        let Some(block_number) = candidates.checked_sub(1) else {
            return Ok(None);
        };
        let mut file =
            File::open(&self.path).map_err(|error| Error::io("open SSTable for lookup", error))?;
        let bytes = self.read_block(&mut file, block_number)?;
        let mut entry = None;
        self.visit_records(block_number, &bytes, |record| {
            if &bytes[record.key] == key {
                entry = Some(match record.value {
                    Some(value) => Entry::Value(bytes[value].to_vec()),
                    None => Entry::Tombstone,
                });
            }
        })?;
        Ok(entry)
    }

    pub(crate) fn cursor(&self) -> Result<TableCursor<'_>> {
        let mut file = File::open(&self.path)
            .map_err(|error| Error::io("open SSTable for compaction", error))?;
        let block = Some(self.read_cursor_block(&mut file, 0)?);
        Ok(TableCursor {
            table: self,
            file,
            block,
            block_number: 0,
            record_number: 0,
        })
    }

    fn read_block(&self, file: &mut File, block_number: usize) -> Result<Vec<u8>> {
        let block = &self.index[block_number];
        let next = self.index.get(block_number + 1);
        let end = next.map_or(self.index_offset, |item| item.offset);
        seek(file, block.offset)?;
        let bytes = read_vec(file, end - block.offset - CHECKSUM_LEN, IO_READ)?;
        let checksum = u32::from_le_bytes(read_array(file, IO_READ)?);
        if crc32fast::hash(&bytes) != checksum {
            return Err(corrupt(self.id, "block checksum mismatch"));
        }
        Ok(bytes)
    }

    fn visit_records(
        &self,
        block_number: usize,
        bytes: &[u8],
        visit: impl FnMut(Record),
    ) -> Result<()> {
        let block = &self.index[block_number];
        let next = self.index.get(block_number + 1);
        visit_block_records(
            bytes,
            &block.first_key,
            next.map(|item| item.first_key.as_slice()),
            self.id,
            visit,
        )
    }

    fn read_cursor_block(&self, file: &mut File, block_number: usize) -> Result<Block> {
        let bytes = self.read_block(file, block_number)?;
        let mut records = Vec::new();
        self.visit_records(block_number, &bytes, |record| records.push(record))?;
        Ok(Block { bytes, records })
    }
}

impl Drop for Sstable {
    fn drop(&mut self) {
        if *self.retired.get_mut() {
            let _ = remove_file_if_exists(&self.path);
        }
    }
}

pub(crate) struct TableCursor<'a> {
    table: &'a Sstable,
    file: File,
    block: Option<Block>,
    block_number: usize,
    record_number: usize,
}

impl TableCursor<'_> {
    pub(crate) fn current(&self) -> Option<(&[u8], Option<&[u8]>)> {
        let block = self.block.as_ref()?;
        let record = &block.records[self.record_number];
        let value = record.value.clone().map(|range| &block.bytes[range]);
        Some((&block.bytes[record.key.clone()], value))
    }

    pub(crate) fn advance(&mut self) -> Result<()> {
        let block = self
            .block
            .as_ref()
            .expect("advance requires a current record");
        self.record_number += 1;
        if self.record_number == block.records.len() {
            self.block = None;
            self.block_number += 1;
            self.record_number = 0;
            if self.block_number < self.table.index.len() {
                self.block = Some(
                    self.table
                        .read_cursor_block(&mut self.file, self.block_number)?,
                );
            }
        }
        Ok(())
    }
}

pub(crate) struct TableWriter {
    file: BufWriter<File>,
    index: Vec<BlockIndex>,
    offset: u64,
    block_crc: Hasher,
}

impl TableWriter {
    pub(crate) fn push(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        self.write_record(key, value)
            .map_err(|error| Error::io(IO_WRITE, error))
    }

    fn write_record(&mut self, key: &[u8], value: Option<&[u8]>) -> io::Result<()> {
        let operation = if value.is_some() { PUT } else { DELETE };
        let value = value.unwrap_or_default();
        let entry_bytes = RECORD_HEADER_LEN as u64 + key.len() as u64 + value.len() as u64;
        match self.index.last() {
            None => self.start_block(key),
            Some(block) if self.offset - block.offset + entry_bytes > BLOCK_TARGET_BYTES => {
                self.finish_block()?;
                self.start_block(key);
            }
            Some(_) => {}
        }

        let mut record = [0; RECORD_HEADER_LEN];
        record[0] = operation;
        record[1..5].copy_from_slice(&(key.len() as u32).to_le_bytes());
        record[5..9].copy_from_slice(&(value.len() as u32).to_le_bytes());
        write_hashed(&mut self.file, &mut self.block_crc, &record)?;
        write_hashed(&mut self.file, &mut self.block_crc, key)?;
        write_hashed(&mut self.file, &mut self.block_crc, value)?;
        self.offset += entry_bytes;
        Ok(())
    }

    fn start_block(&mut self, key: &[u8]) {
        self.index.push(BlockIndex {
            first_key: key.to_vec(),
            offset: self.offset,
        });
    }

    fn finish_block(&mut self) -> io::Result<()> {
        let crc = std::mem::take(&mut self.block_crc);
        self.file.write_all(&crc.finalize().to_le_bytes())?;
        self.offset += CHECKSUM_LEN;
        Ok(())
    }

    fn finish(&mut self) -> io::Result<u64> {
        self.finish_block()?;
        let mut crc = Hasher::new();
        write_hashed(
            &mut self.file,
            &mut crc,
            &(self.index.len() as u64).to_le_bytes(),
        )?;
        for block in &self.index {
            write_hashed(&mut self.file, &mut crc, &block.offset.to_le_bytes())?;
            write_hashed(
                &mut self.file,
                &mut crc,
                &(block.first_key.len() as u32).to_le_bytes(),
            )?;
            write_hashed(&mut self.file, &mut crc, &block.first_key)?;
        }
        write_hashed(&mut self.file, &mut crc, &self.offset.to_le_bytes())?;
        self.file.write_all(&crc.finalize().to_le_bytes())?;
        self.file.flush()?;
        self.file.get_ref().sync_all()?;
        Ok(self.offset)
    }
}

struct Block {
    bytes: Vec<u8>,
    records: Vec<Record>,
}

struct Record {
    key: Range<usize>,
    value: Option<Range<usize>>,
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

fn visit_block_records(
    bytes: &[u8],
    first_key: &[u8],
    next_key: Option<&[u8]>,
    id: u64,
    mut visit: impl FnMut(Record),
) -> Result<()> {
    let mut remaining = bytes;
    let mut previous = None;
    while !remaining.is_empty() {
        let Some((&[kind, k0, k1, k2, k3, v0, v1, v2, v3], payload)) =
            remaining.split_at_checked(RECORD_HEADER_LEN)
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
        let Some((_, tail)) = payload.split_at_checked(value_len) else {
            return Err(corrupt(id, "record value exceeds block"));
        };
        if previous.map_or(key != first_key, |previous| key <= previous)
            || next_key.is_some_and(|next| key >= next)
        {
            return Err(corrupt(id, "block keys disagree with index or order"));
        }

        let start = bytes.len() - remaining.len() + RECORD_HEADER_LEN;
        let value_start = start + key_len;
        visit(Record {
            key: start..value_start,
            value: (kind == PUT).then_some(value_start..value_start + value_len),
        });
        previous = Some(key);
        remaining = tail;
    }
    Ok(())
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
