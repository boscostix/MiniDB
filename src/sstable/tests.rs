use super::*;

fn get(table: &Sstable, key: &[u8], cache: &BlockCache) -> Result<Option<Entry>> {
    table.get(key, &mut None, cache)
}

fn write_v1(directory: &Path, id: u64) {
    let mut bytes = HEADER_V1.to_vec();
    bytes.extend_from_slice(&id.to_le_bytes());
    let block = b"\x01\x03\0\0\0\x03\0\0\0keyold";
    bytes.extend_from_slice(block);
    bytes.extend_from_slice(&crc32fast::hash(block).to_le_bytes());
    let index_offset = bytes.len();
    bytes.extend_from_slice(&1_u64.to_le_bytes());
    bytes.extend_from_slice(&FILE_HEADER_LEN.to_le_bytes());
    bytes.extend_from_slice(&3_u32.to_le_bytes());
    bytes.extend_from_slice(b"key");
    bytes.extend_from_slice(&(index_offset as u64).to_le_bytes());
    bytes.extend_from_slice(&crc32fast::hash(&bytes[index_offset..]).to_le_bytes());
    fs::write(Sstable::path(directory, id), bytes).unwrap();
}

#[test]
fn version_one_is_readable_and_merges_with_new_filtered_tables() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    write_v1(path, 1);
    let old = Arc::new(Sstable::open(path, 1).unwrap());
    let cache = BlockCache::new(16384);
    assert!(matches!(
        get(&old, b"key", &cache).unwrap(),
        Some(Entry::Value(value)) if value == b"old"
    ));

    let new = Arc::new(
        Sstable::build(path, 2, |table| {
            table.push(b"key", None)?;
            table.push(b"new", Some(b""))?;
            Ok(())
        })
        .unwrap()
        .unwrap(),
    );
    let new = Arc::new(Sstable::open(path, new.id).unwrap());
    assert!(matches!(
        get(&new, b"key", &cache).unwrap(),
        Some(Entry::Tombstone)
    ));
    let merged = crate::compaction::merge(path, 3, &[old, new])
        .unwrap()
        .unwrap();
    assert_eq!(&fs::read(&merged.path).unwrap()[..8], &HEADER_V2);
    assert!(get(&merged, b"key", &cache).unwrap().is_none());
    assert!(matches!(
        get(&merged, b"new", &cache).unwrap(),
        Some(Entry::Value(value)) if value.is_empty()
    ));
}

#[test]
fn version_two_checksums_filter_bytes_and_validates_filter_length() {
    let directory = tempfile::tempdir().unwrap();
    let table = Sstable::build(directory.path(), 1, |table| {
        table.push(b"key", Some(b"value"))
    })
    .unwrap()
    .unwrap();
    let original = fs::read(&table.path).unwrap();
    let filter_len_offset = table.index_offset as usize + 8 + 8 + 4 + b"key".len();

    let mut corrupt_bits = original.clone();
    corrupt_bits[filter_len_offset + 2] ^= 1;
    fs::write(&table.path, corrupt_bits).unwrap();
    assert!(matches!(
        Sstable::open(directory.path(), 1),
        Err(Error::CorruptSstable {
            reason: "index checksum mismatch",
            ..
        })
    ));

    let mut corrupt_length = original;
    corrupt_length[filter_len_offset..filter_len_offset + 2]
        .copy_from_slice(&u16::MAX.to_le_bytes());
    fs::write(&table.path, corrupt_length).unwrap();
    assert!(matches!(
        Sstable::open(directory.path(), 1),
        Err(Error::CorruptSstable {
            reason: "invalid Bloom filter length",
            ..
        })
    ));
}

#[test]
fn cache_evicts_least_recently_used_and_bypasses_large_blocks() {
    const CAPACITY: usize = 5500;
    let directory = tempfile::tempdir().unwrap();
    let table = Sstable::build(directory.path(), 1, |table| {
        for i in 0..3_u8 {
            table.push(&[i], Some(&[i; 2200]))?;
        }
        table.push(b"big", Some(&[0; 8192]))
    })
    .unwrap()
    .unwrap();
    let cache = BlockCache::new(CAPACITY);
    for i in [0_u8, 1, 0, 2] {
        assert!(matches!(
            get(&table, &[i], &cache).unwrap(),
            Some(Entry::Value(value)) if value == [i; 2200]
        ));
    }
    assert!(cache.get((1, 0)).is_some());
    assert!(cache.get((1, 1)).is_none());
    assert!(cache.get((1, 2)).is_some());
    let charged = cache.occupied();
    assert!(charged <= CAPACITY);
    assert!(matches!(
        get(&table, b"big", &cache).unwrap(),
        Some(Entry::Value(value)) if value.len() == 8192
    ));
    assert_eq!(cache.occupied(), charged);
    assert!(cache.get((1, 3)).is_none());
}

#[test]
fn validated_blocks_are_reused_and_bloom_negatives_need_no_file() {
    let directory = tempfile::tempdir().unwrap();
    let table = Sstable::build(directory.path(), 1, |table| {
        table.push(b"alpha", Some(b"one"))?;
        table.push(b"delta", Some(b"two"))
    })
    .unwrap()
    .unwrap();
    let cold = BlockCache::new(16_384);
    let warm = BlockCache::new(16_384);
    assert!(matches!(
        get(&table, b"alpha", &warm).unwrap(),
        Some(Entry::Value(value)) if value == b"one"
    ));
    fs::remove_file(&table.path).unwrap();
    assert!(!bloom::might_contain(
        &table.index[0].filter,
        bloom::hash(b"beta")
    ));
    assert!(get(&table, b"beta", &cold).unwrap().is_none());
    assert!(get(&table, b"delta", &cold).is_err());
    assert!(matches!(
        get(&table, b"delta", &warm).unwrap(),
        Some(Entry::Value(value)) if value == b"two"
    ));
}
