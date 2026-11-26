#![cfg(unix)]

use std::{
    fs,
    io::{self, BufRead, BufReader, Write},
    process::{Child, Command, Stdio},
    sync::mpsc,
    thread,
    time::Duration,
};

use minidb::{Error, MiniDB, Options};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const WAL: &str = "wal.log";
const CRASH_TEST_DIRECTORY: &str = "MINIDB_CRASH_TEST_DIRECTORY";
const WRITES_ACKNOWLEDGED: &str = "MiniDB crash test writes acknowledged";

#[test]
fn reopens_overwrites_deletions_and_binary_data() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("parent").join("db");
    let db = MiniDB::open(&path)?;
    db.put(b"name", b"Alice")?;
    db.put(b"name", b"Bob")?;
    db.put(b"deleted", b"old")?;
    db.delete(b"deleted")?;
    db.delete(b"never stored")?;
    db.put(b"\0\xff", b"\x80\0")?;
    db.put(b"empty", b"")?;
    db.close()?;

    let db = MiniDB::open(&path)?;
    assert_eq!(db.get(b"name")?.as_deref(), Some(&b"Bob"[..]));
    assert_eq!(db.get(b"deleted")?, None);
    assert_eq!(db.get(b"never stored")?, None);
    assert_eq!(db.get(b"\0\xff")?.as_deref(), Some(&b"\x80\0"[..]));
    assert_eq!(db.get(b"empty")?, Some(Vec::new()));
    db.put(b"deleted", b"new")?;
    drop(db);

    assert_eq!(
        MiniDB::open(&path)?.get(b"deleted")?.as_deref(),
        Some(&b"new"[..])
    );
    Ok(())
}

#[test]
fn recovery_does_not_apply_new_input_limits_to_existing_records() -> TestResult {
    let directory = tempfile::tempdir()?;
    let db = MiniDB::open(directory.path())?;
    db.put(b"k", b"long value")?;
    db.put(b"long key", b"value")?;
    db.close()?;

    let db = MiniDB::open_with_options(
        directory.path(),
        Options {
            max_key_bytes: Some(1),
            max_value_bytes: Some(1),
        },
    )?;
    assert_eq!(db.get(b"k")?.as_deref(), Some(&b"long value"[..]));
    assert!(matches!(
        db.get(b"long key"),
        Err(Error::KeyTooLarge { .. })
    ));
    db.close()?;
    assert_eq!(
        MiniDB::open(directory.path())?.get(b"long key")?.as_deref(),
        Some(&b"value"[..])
    );
    Ok(())
}

#[test]
fn prevents_a_second_handle_and_releases_the_lock_on_drop() -> TestResult {
    let directory = tempfile::tempdir()?;
    let db = MiniDB::open(directory.path())?;
    assert!(matches!(
        MiniDB::open(directory.path().join(".")),
        Err(Error::DatabaseLocked)
    ));
    drop(db);
    MiniDB::open(directory.path())?.close()?;
    Ok(())
}

#[test]
fn repairs_every_incomplete_tail_boundary_before_appending() -> TestResult {
    let directory = tempfile::tempdir()?;
    let wal = directory.path().join(WAL);
    let db = MiniDB::open(directory.path())?;
    db.put(b"first", b"durable")?;
    let committed_end = fs::read(&wal)?.len();
    db.put(b"tail", b"some bytes")?;
    db.close()?;
    let complete = fs::read(&wal)?;

    for cut in committed_end + 1..complete.len() {
        fs::write(&wal, &complete[..cut])?;
        let db = MiniDB::open(directory.path())?;
        assert_eq!(db.get(b"first")?.as_deref(), Some(&b"durable"[..]));
        assert_eq!(db.get(b"tail")?, None);
        assert_eq!(fs::read(&wal)?.len(), committed_end);
        db.put(b"next", b"after repair")?;
        db.close()?;

        let db = MiniDB::open(directory.path())?;
        assert_eq!(db.get(b"next")?.as_deref(), Some(&b"after repair"[..]));
        assert_eq!(db.get(b"tail")?, None);
    }
    Ok(())
}

#[test]
fn rejects_middle_length_corruption_and_complete_final_checksum_corruption() -> TestResult {
    let directory = tempfile::tempdir()?;
    let wal = directory.path().join(WAL);
    let db = MiniDB::open(directory.path())?;
    let first_offset = fs::read(&wal)?.len();
    db.put(b"first", b"one")?;
    let last_offset = fs::read(&wal)?.len();
    db.put(b"last", b"two")?;
    db.close()?;
    let complete = fs::read(&wal)?;

    for (byte, expected_offset) in [
        (first_offset + 1, first_offset),
        (complete.len() - 1, last_offset),
    ] {
        let mut damaged = complete.clone();
        damaged[byte] ^= 0x80;
        fs::write(&wal, &damaged)?;
        assert!(matches!(
            MiniDB::open(directory.path()),
            Err(Error::CorruptWal { offset, .. }) if offset == expected_offset as u64
        ));
        assert_eq!(fs::read(&wal)?, damaged);
    }
    Ok(())
}

#[test]
fn rejects_a_malformed_partial_header() -> TestResult {
    let directory = tempfile::tempdir()?;
    let wal = directory.path().join(WAL);
    MiniDB::open(directory.path())?.close()?;
    let mut data = fs::read(&wal)?;
    let offset = data.len() as u64;
    data.push(0xff);
    fs::write(&wal, &data)?;
    assert!(matches!(
        MiniDB::open(directory.path()),
        Err(Error::CorruptWal { offset: found, .. }) if found == offset
    ));
    assert_eq!(fs::read(&wal)?, data);
    Ok(())
}

#[test]
fn rejects_bad_file_headers_and_unsupported_versions() -> TestResult {
    let directory = tempfile::tempdir()?;
    let wal = directory.path().join(WAL);
    MiniDB::open(directory.path())?.close()?;

    fs::write(&wal, b"MINIDB\x09\0")?;
    assert!(matches!(
        MiniDB::open(directory.path()),
        Err(Error::UnsupportedWalVersion { version: 9 })
    ));
    for bad in [&b"MINI"[..], b"BADHDR\x01\0"] {
        fs::write(&wal, bad)?;
        assert!(matches!(
            MiniDB::open(directory.path()),
            Err(Error::CorruptWal { offset: 0, .. })
        ));
        assert_eq!(fs::read(&wal)?, bad);
    }
    Ok(())
}

#[test]
fn replaces_an_unpublished_staging_file() -> TestResult {
    let directory = tempfile::tempdir()?;
    let staging = directory.path().join("wal.log.tmp");
    fs::write(&staging, b"MIN")?;

    let db = MiniDB::open(directory.path())?;
    assert!(!staging.exists());
    db.put(b"key", b"value")?;
    db.close()?;
    assert_eq!(
        MiniDB::open(directory.path())?.get(b"key")?.as_deref(),
        Some(&b"value"[..])
    );
    Ok(())
}

#[test]
fn acknowledged_writes_survive_forced_process_termination() -> TestResult {
    let directory = tempfile::tempdir()?;
    let mut child = CrashChild(
        Command::new(std::env::current_exe()?)
            .args(["--exact", "crash_writer", "--nocapture", "--test-threads=1"])
            .env(CRASH_TEST_DIRECTORY, directory.path())
            .stdout(Stdio::piped())
            .spawn()?,
    );
    let stdout = child.0.stdout.take().expect("child stdout was piped");
    let (ready, response) = mpsc::channel();

    thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if line.contains(WRITES_ACKNOWLEDGED) {
                let _ = ready.send(());
                break;
            }
        }
    });
    response
        .recv_timeout(Duration::from_secs(15))
        .expect("child must acknowledge writes before termination");
    assert!(matches!(
        MiniDB::open(directory.path()),
        Err(Error::DatabaseLocked)
    ));
    child.0.kill()?;
    assert!(!child.0.wait()?.success());

    let db = MiniDB::open(directory.path())?;
    assert_eq!(db.get(b"name")?.as_deref(), Some(&b"second"[..]));
    assert_eq!(db.get(b"gone")?, None);
    assert_eq!(db.get(b"\0\xff")?, Some(Vec::new()));
    Ok(())
}

#[test]
fn crash_writer() -> TestResult {
    let Some(directory) = std::env::var_os(CRASH_TEST_DIRECTORY) else {
        return Ok(());
    };
    let db = MiniDB::open(directory)?;
    db.put(b"name", b"first")?;
    db.put(b"name", b"second")?;
    db.put(b"gone", b"deleted")?;
    db.delete(b"gone")?;
    db.put(b"\0\xff", b"")?;
    println!("{WRITES_ACKNOWLEDGED}");
    io::stdout().flush()?;
    loop {
        thread::park();
    }
}

struct CrashChild(Child);

impl Drop for CrashChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
