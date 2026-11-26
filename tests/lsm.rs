#![cfg(unix)]

use std::{fs, io, path::Path};

use minidb::{Error, MiniDB, Options};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const MANIFEST: &str = "MANIFEST";
const LEGACY_WAL: &str = "wal.log";
const WAL_ONE: &str = "wal-00000000000000000001.log";
const WAL_TWO: &str = "wal-00000000000000000002.log";
const TABLE_ONE: &str = "sst-00000000000000000001.sst";
const TABLE_TWO: &str = "sst-00000000000000000002.sst";

#[test]
fn flushes_sorted_tables_and_resolves_newest_values_and_tombstones() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path();
    let db = MiniDB::open(path)?;
    let initial_manifest = fs::read(path.join(MANIFEST))?;
    db.flush()?;
    assert_eq!(fs::read(path.join(MANIFEST))?, initial_manifest);

    db.put(b"delete", b"old")?;
    db.put(b"overwrite", b"old")?;
    db.put(b"\xff\0", b"\0\x80")?;
    db.put(b"\0\xff", b"")?;
    db.flush()?;
    assert!(!path.join(LEGACY_WAL).exists());
    assert!(path.join(TABLE_ONE).is_file());
    assert!(path.join(WAL_ONE).is_file());
    assert_eq!(db.get(b"\xff\0")?.as_deref(), Some(&b"\0\x80"[..]));
    assert_eq!(db.get(b"\0\xff")?, Some(Vec::new()));

    db.delete(b"delete")?;
    assert_eq!(db.get(b"delete")?, None);
    db.put(b"overwrite", b"new")?;
    db.flush()?;
    assert!(!path.join(WAL_ONE).exists());
    assert!(path.join(WAL_TWO).is_file());
    assert!(path.join(TABLE_ONE).is_file());
    assert!(path.join(TABLE_TWO).is_file());
    db.put(b"active", b"wal")?;
    assert_eq!(db.get(b"delete")?, None);
    assert_eq!(db.get(b"overwrite")?.as_deref(), Some(&b"new"[..]));
    db.close()?;

    let db = MiniDB::open(path)?;
    assert_eq!(db.get(b"active")?.as_deref(), Some(&b"wal"[..]));
    assert_eq!(db.get(b"delete")?, None);
    assert_eq!(db.get(b"overwrite")?.as_deref(), Some(&b"new"[..]));
    assert_eq!(db.get(b"\xff\0")?.as_deref(), Some(&b"\0\x80"[..]));
    assert_eq!(db.get(b"\0\xff")?, Some(Vec::new()));
    assert_eq!(db.get(b"missing")?, None);
    db.put(b"delete", b"restored")?;
    assert_eq!(db.get(b"delete")?.as_deref(), Some(&b"restored"[..]));
    Ok(())
}

#[test]
fn first_open_migrates_a_legacy_wal_before_the_first_flush() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path();
    let db = MiniDB::open(path)?;
    db.put(b"legacy", b"durable")?;
    db.put(b"removed", b"historical")?;
    db.delete(b"removed")?;
    db.close()?;
    // Generation zero uses the Milestone 2 format unchanged.
    let legacy = fs::read(path.join(LEGACY_WAL))?;
    fs::remove_file(path.join(MANIFEST))?;

    let db = MiniDB::open(path)?;
    assert!(path.join(MANIFEST).is_file());
    assert_eq!(fs::read(path.join(LEGACY_WAL))?, legacy);
    assert_eq!(db.get(b"legacy")?.as_deref(), Some(&b"durable"[..]));
    assert_eq!(db.get(b"removed")?, None);
    db.flush()?;
    assert!(!path.join(LEGACY_WAL).exists());
    db.close()?;
    let reopened = MiniDB::open(path)?;
    assert_eq!(reopened.get(b"legacy")?.as_deref(), Some(&b"durable"[..]));
    assert_eq!(reopened.get(b"removed")?, None);
    Ok(())
}

#[test]
fn threshold_flushes_the_crossing_write_including_an_oversized_entry() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path();
    let db = MiniDB::open_with_flush_threshold(path, Options::default(), 1024)?;
    db.put(b"small", b"value")?;
    assert!(!path.join(TABLE_ONE).exists());
    db.put(b"large", &vec![3; 8192])?;
    assert!(path.join(TABLE_ONE).is_file());
    assert!(!path.join(LEGACY_WAL).exists());
    assert_eq!(db.get(b"large")?, Some(vec![3; 8192]));
    db.close()?;

    let db = MiniDB::open_with_flush_threshold(path, Options::default(), 0)?;
    db.delete(b"large")?;
    assert!(path.join(TABLE_TWO).is_file());
    db.close()?;
    assert_eq!(MiniDB::open(path)?.get(b"large")?, None);
    Ok(())
}

#[test]
fn auto_flush_error_distinguishes_a_durable_write_from_a_retry_failure() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path();
    let obstruction = path.join(format!("{TABLE_ONE}.tmp"));
    let db = MiniDB::open_with_flush_threshold(path, Options::default(), 0)?;
    fs::create_dir(&obstruction)?;

    assert!(matches!(
        db.put(b"accepted", b"value"),
        Err(Error::AutoFlushFailed { cause })
            if matches!(*cause, Error::Io { operation: "remove stale staging file", .. })
    ));
    assert_eq!(db.get(b"accepted")?.as_deref(), Some(&b"value"[..]));
    assert!(matches!(
        db.put(b"not-appended", b"value"),
        Err(Error::Io {
            operation: "remove stale staging file",
            ..
        })
    ));
    assert_eq!(db.get(b"not-appended")?, None);
    db.close()?;

    let db = MiniDB::open_with_flush_threshold(path, Options::default(), 0)?;
    assert_eq!(db.get(b"accepted")?.as_deref(), Some(&b"value"[..]));
    assert_eq!(db.get(b"not-appended")?, None);
    fs::remove_dir(obstruction)?;
    db.flush()?;
    assert_eq!(db.get(b"accepted")?.as_deref(), Some(&b"value"[..]));
    Ok(())
}

#[test]
fn lookup_reads_only_the_candidate_block_and_reports_its_checksum_failure() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path();
    let db = MiniDB::open(path)?;
    db.put(b"a", &vec![1; 6000])?;
    db.put(b"b", &vec![2; 6000])?;
    db.flush()?;
    db.close()?;

    flip_byte(&path.join(TABLE_ONE), Some(32))?;
    let db = MiniDB::open(path)?;
    assert_eq!(db.get(b"b")?, Some(vec![2; 6000]));
    assert!(matches!(
        db.get(b"a"),
        Err(Error::CorruptSstable {
            id: 1,
            reason: "block checksum mismatch"
        })
    ));
    Ok(())
}

#[test]
fn detects_corrupt_table_index_and_manifest_at_open() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path();
    let db = MiniDB::open(path)?;
    db.put(b"a", b"one")?;
    db.flush()?;
    db.close()?;

    let manifest = path.join(MANIFEST);
    let original = fs::read(&manifest)?;
    flip_byte(&manifest, None)?;
    assert!(matches!(
        MiniDB::open(path),
        Err(Error::CorruptManifest {
            reason: "checksum mismatch"
        })
    ));
    fs::write(manifest, original)?;
    flip_byte(&path.join(TABLE_ONE), None)?;
    assert!(matches!(
        MiniDB::open(path),
        Err(Error::CorruptSstable {
            id: 1,
            reason: "index checksum mismatch"
        })
    ));
    Ok(())
}

#[test]
fn missing_manifest_or_referenced_files_fail_closed() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path();
    let db = MiniDB::open(path)?;
    db.put(b"stored", b"on disk")?;
    db.flush()?;
    db.close()?;

    let manifest_path = path.join(MANIFEST);
    let manifest = fs::read(&manifest_path)?;
    fs::remove_file(&manifest_path)?;
    assert!(matches!(
        MiniDB::open(path),
        Err(Error::CorruptManifest { .. })
    ));
    assert!(!path.join(LEGACY_WAL).exists());
    fs::write(manifest_path, manifest)?;

    for (file, operation) in [
        (TABLE_ONE, "open manifest-referenced SSTable"),
        (WAL_ONE, "open manifest-referenced WAL"),
    ] {
        let file = path.join(file);
        let original = fs::read(&file)?;
        fs::remove_file(&file)?;
        assert!(matches!(
            MiniDB::open(path),
            Err(Error::Io { kind: io::ErrorKind::NotFound, operation: found, .. }) if found == operation
        ));
        assert!(!file.exists());
        fs::write(file, original)?;
    }
    Ok(())
}

fn flip_byte(path: &Path, offset: Option<usize>) -> io::Result<()> {
    let mut bytes = fs::read(path)?;
    let index = offset.unwrap_or(bytes.len() - 1);
    bytes[index] ^= 0x80;
    fs::write(path, bytes)
}
