use std::{process::Command, sync::mpsc, time::Duration};

use super::*;

const WAIT: Duration = Duration::from_secs(10);
const CRASH_DIRECTORY: &str = "MINIDB_COMPACTION_CRASH_DIRECTORY";
const CRASH_STAGE: &str = "MINIDB_COMPACTION_CRASH_STAGE";
const CRASH_CODE: i32 = 75;

fn open_with_hook(
    path: &Path,
    hook: impl Fn(CompactionStage) -> Result<()> + Send + 'static,
) -> MiniDB {
    MiniDB::open_with_hooks(
        path,
        Options::default(),
        DEFAULT_FLUSH_THRESHOLD_BYTES,
        |_| Ok(()),
        hook,
    )
    .unwrap()
}

fn create_input_tables(path: &Path, count: u64) {
    let shared = Shared::open(path).unwrap();
    for id in 1..=count {
        let mut view = shared.write_view();
        let table = Arc::get_mut(&mut view.memtable).unwrap();
        table.insert(b"shared", Entry::Value(vec![id as u8; 6000]));
        table.insert([0, id as u8, 255], Entry::Value(vec![id as u8; 4096]));
        match id {
            1 => {
                table.insert(b"empty", Entry::Value(Vec::new()));
                table.insert(b"pin", Entry::Value(b"old snapshot".to_vec()));
                table.insert(b"discarded", Entry::Value(b"old value".to_vec()));
            }
            3 => table.insert(b"suffix-deleted", Entry::Value(b"lower value".to_vec())),
            4 => table.insert(b"discarded", Entry::Tombstone),
            5 => table.insert(b"suffix-deleted", Entry::Tombstone),
            _ => {}
        }
        drop(view);
        shared
            .flush_with(path, |_| Ok(()), Manifest::publish)
            .unwrap();
    }

    // Encode the equivalent legacy manifest, including its original checksum.
    let mut bytes = b"MINIMAN\x01".to_vec();
    bytes.extend_from_slice(&count.to_le_bytes());
    bytes.extend_from_slice(&count.to_le_bytes());
    for id in 1..=count {
        bytes.extend_from_slice(&id.to_le_bytes());
    }
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
    fs::write(path.join("MANIFEST"), bytes).unwrap();
}

fn live_ids(path: &Path) -> Vec<u64> {
    Manifest::load(path).unwrap().unwrap().table_ids
}

fn worker_error(db: &MiniDB) -> Error {
    let state = db.shared.lock();
    let (state, _) = db
        .shared
        .changed
        .wait_timeout_while(state, WAIT, |state| state.compaction_error.is_none())
        .unwrap();
    state.compaction_error.clone().expect("compaction failed")
}

#[test]
fn compaction_preserves_suffix_precedence_and_in_flight_reads() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    create_input_tables(path, 5);
    let (merging, start) = mpsc::channel();
    let (resume, paused) = mpsc::channel();
    let (synced, publish) = mpsc::channel();
    let db = open_with_hook(path, move |stage| {
        match stage {
            CompactionStage::Merging => {
                merging.send(()).unwrap();
                paused.recv_timeout(WAIT).unwrap();
            }
            CompactionStage::AfterManifestSync => synced.send(()).unwrap(),
            _ => {}
        }
        Ok(())
    });

    start.recv_timeout(WAIT).unwrap();
    assert_eq!(db.get(b"shared").unwrap(), Some(vec![5; 6000]));
    assert_eq!(db.get(b"discarded").unwrap(), None);
    assert_eq!(db.get(b"suffix-deleted").unwrap(), None);
    db.put(b"fresh", b"a flush during the merge").unwrap();
    db.put(&[0, 2, 255], b"newer than the merge").unwrap();
    db.flush().unwrap();

    let found = db
        .get_with(b"pin", || {
            resume.send(()).unwrap();
            publish.recv_timeout(WAIT).unwrap();
            // The publication hook runs with the state locked. Acquiring it
            // here observes the new version while the lookup pins the old one.
            assert_eq!(db.shared.lock().manifest.table_ids, [6, 5, 7]);
            for id in 1..=4 {
                assert!(Sstable::path(path, id).is_file());
            }
        })
        .unwrap();
    assert_eq!(found.as_deref(), Some(&b"old snapshot"[..]));
    assert_eq!(db.get(b"shared").unwrap(), Some(vec![5; 6000]));
    assert_eq!(
        db.get(&[0, 2, 255]).unwrap(),
        Some(b"newer than the merge".to_vec())
    );
    assert_eq!(db.get(b"suffix-deleted").unwrap(), None);
    assert_eq!(db.get(b"empty").unwrap(), Some(Vec::new()));
    db.close().unwrap();

    for id in 1..=4 {
        assert!(!Sstable::path(path, id).exists());
    }
    let output = Sstable::open(path, 6).unwrap();
    let cache = BlockCache::new(0);
    assert!(
        output
            .get(b"discarded", &mut None, &cache)
            .unwrap()
            .is_none()
    );
    assert!(
        matches!(output.get(b"shared", &mut None, &cache).unwrap(), Some(Entry::Value(value)) if value == vec![4; 6000])
    );
    assert!(matches!(
        Sstable::open(path, 5)
            .unwrap()
            .get(b"suffix-deleted", &mut None, &cache)
            .unwrap(),
        Some(Entry::Tombstone)
    ));

    let db = MiniDB::open(path).unwrap();
    assert_eq!(live_ids(path), [6, 5, 7]);
    assert_eq!(db.get(b"shared").unwrap(), Some(vec![5; 6000]));
    assert_eq!(db.get(b"suffix-deleted").unwrap(), None);
    assert_eq!(
        db.get(b"fresh").unwrap(),
        Some(b"a flush during the merge".to_vec())
    );
    db.put(b"shared", b"after reopen").unwrap();
    db.flush().unwrap();
    assert_eq!(db.get(b"shared").unwrap(), Some(b"after reopen".to_vec()));
}

#[test]
fn four_foreground_flushes_trigger_compaction_and_can_leave_no_output() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let (synced, publish) = mpsc::channel();
    let db = open_with_hook(path, move |stage| {
        if stage == CompactionStage::AfterManifestSync {
            synced.send(()).unwrap();
        }
        Ok(())
    });
    let stale_name = staging_path(&Sstable::path(path, 5));
    fs::create_dir(&stale_name).unwrap();
    db.put(b"a", b"original").unwrap();
    db.flush().unwrap();
    for key in [b"a", b"b", b"c"] {
        db.delete(key).unwrap();
        db.flush().unwrap();
    }
    publish.recv_timeout(WAIT).unwrap();
    db.close().unwrap();
    let manifest = Manifest::load(path).unwrap().unwrap();
    assert!(manifest.table_ids.is_empty());
    assert_eq!(manifest.generation, 4);
    assert_eq!(manifest.highest_file_id, 6);
    assert!(stale_name.is_dir(), "the worker must use a fresh identity");
    for id in 1..=6 {
        assert!(!Sstable::path(path, id).exists());
    }

    let db = MiniDB::open(path).unwrap();
    assert_eq!(db.get(b"a").unwrap(), None);
    db.put(b"a", b"reinserted").unwrap();
    db.flush().unwrap();
    assert_eq!(live_ids(path), [7]);
    assert_eq!(db.get(b"a").unwrap(), Some(b"reinserted".to_vec()));
}

#[test]
fn compaction_worker_reports_a_corrupt_block_and_leaves_writes_available() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    create_input_tables(path, 4);
    let original_manifest = fs::read(path.join("MANIFEST")).unwrap();
    let table_path = Sstable::path(path, 1);
    let mut bytes = fs::read(&table_path).unwrap();
    bytes[5000] ^= 0x80;
    fs::write(table_path, bytes).unwrap();

    let db = MiniDB::open(path).unwrap();
    let expected = Error::CorruptSstable {
        id: 1,
        reason: "block checksum mismatch",
    };
    assert_eq!(worker_error(&db), expected);
    assert_eq!(db.compaction_error(), Some(expected.clone()));
    assert_eq!(fs::read(path.join("MANIFEST")).unwrap(), original_manifest);
    for id in 1..=4 {
        assert!(Sstable::path(path, id).is_file());
    }
    assert!(!Sstable::path(path, 5).exists());
    db.put(b"after", b"still writable").unwrap();
    db.flush().unwrap();
    assert_eq!(live_ids(path), [1, 2, 3, 4, 6]);
    assert_eq!(db.get(b"after").unwrap(), Some(b"still writable".to_vec()));
    assert_eq!(db.close(), Err(expected));
}

#[test]
fn uncertain_compaction_publication_keeps_both_file_sets_and_requires_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    create_input_tables(path, 4);
    let expected = Error::io("publish compaction", io::Error::other("simulated failure"));
    let injected = expected.clone();
    let db = open_with_hook(path, move |stage| {
        if stage == CompactionStage::AfterManifestRename {
            return Err(injected.clone());
        }
        Ok(())
    });
    assert_eq!(worker_error(&db), expected);
    assert_eq!(live_ids(path), [5]);
    for id in 1..=5 {
        assert!(Sstable::path(path, id).is_file());
    }
    assert_eq!(db.get(b"pin").unwrap(), Some(b"old snapshot".to_vec()));
    assert_eq!(
        db.put(b"rejected", b"write"),
        Err(Error::FlushRequiresReopen)
    );
    assert_eq!(db.flush(), Err(Error::FlushRequiresReopen));
    assert_eq!(db.close(), Err(expected));

    let db = MiniDB::open(path).unwrap();
    for id in 1..=4 {
        assert!(!Sstable::path(path, id).exists());
    }
    assert_eq!(db.get(b"rejected").unwrap(), None);
    assert_eq!(db.get(b"shared").unwrap(), Some(vec![4; 6000]));
    db.put(b"allowed", b"after recovery").unwrap();
}

#[test]
fn interrupted_compaction_recovers_at_each_manifest_boundary() {
    for (stage, manifest_replaced) in [
        ("before-rename", false),
        ("after-rename", true),
        ("after-sync", true),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path();
        create_input_tables(path, 4);
        let old_manifest = fs::read(path.join("MANIFEST")).unwrap();
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "db::compaction_tests::interrupt_compaction_child",
            ])
            .env(CRASH_DIRECTORY, path)
            .env(CRASH_STAGE, stage)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(CRASH_CODE), "checkpoint {stage}");
        assert_eq!(
            fs::read(path.join("MANIFEST")).unwrap() != old_manifest,
            manifest_replaced
        );
        for id in 1..=5 {
            assert!(Sstable::path(path, id).is_file());
        }
        assert!(Wal::path(path, 4).is_file());

        // Stop a new merge on the old-manifest branch so recovery cleanup
        // and table selection can be inspected independently.
        let db = open_with_hook(path, |stage| {
            assert_eq!(stage, CompactionStage::Merging);
            Err(Error::io(
                "pause recovery merge",
                io::Error::other("test checkpoint"),
            ))
        });
        assert_eq!(Sstable::path(path, 5).exists(), manifest_replaced);
        assert_eq!(Sstable::path(path, 1).exists(), !manifest_replaced);
        assert_eq!(db.get(b"shared").unwrap(), Some(vec![4; 6000]));
        assert_eq!(db.get(b"discarded").unwrap(), None);
        assert_eq!(db.get(b"empty").unwrap(), Some(Vec::new()));
    }
}

#[test]
fn interrupt_compaction_child() {
    let Some(directory) = std::env::var_os(CRASH_DIRECTORY) else {
        return;
    };
    let checkpoint = match std::env::var(CRASH_STAGE).unwrap().as_str() {
        "before-rename" => CompactionStage::BeforeManifestRename,
        "after-rename" => CompactionStage::AfterManifestRename,
        "after-sync" => CompactionStage::AfterManifestSync,
        stage => panic!("unknown checkpoint {stage}"),
    };
    let _db = open_with_hook(Path::new(&directory), move |stage| {
        if stage == checkpoint {
            std::process::exit(CRASH_CODE);
        }
        Ok(())
    });
    thread::park_timeout(WAIT);
    panic!("checkpoint not reached");
}

#[test]
fn drop_joins_an_active_worker_before_releasing_the_directory_lock() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    create_input_tables(path, 4);
    let (merging, start) = mpsc::channel();
    let (resume, paused) = mpsc::channel();
    let db = open_with_hook(path, move |stage| {
        if stage == CompactionStage::Merging {
            merging.send(()).unwrap();
            paused.recv_timeout(WAIT).unwrap();
        }
        Ok(())
    });
    let shared = Arc::clone(&db.shared);
    start.recv_timeout(WAIT).unwrap();
    let closing = thread::spawn(move || drop(db));
    let (state, _) = shared
        .changed
        .wait_timeout_while(shared.lock(), WAIT, |state| !state.shutdown)
        .unwrap();
    assert!(state.shutdown);
    drop(state);
    assert!(matches!(MiniDB::open(path), Err(Error::DatabaseLocked)));
    resume.send(()).unwrap();
    closing.join().unwrap();
    assert_eq!(live_ids(path), [5]);
    let db = MiniDB::open(path).unwrap();
    assert_eq!(db.get(b"shared").unwrap(), Some(vec![4; 6000]));
}

#[test]
fn concurrent_readers_progress_while_compaction_publishes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    create_input_tables(path, 4);
    let (renamed, publication) = mpsc::channel();
    let (resume, paused) = mpsc::channel();
    let db = Arc::new(open_with_hook(path, move |stage| {
        if stage == CompactionStage::AfterManifestRename {
            renamed.send(()).unwrap();
            paused.recv_timeout(WAIT).unwrap();
        }
        Ok(())
    }));
    publication.recv_timeout(WAIT).unwrap();

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                assert_eq!(
                    db.get(b"pin").unwrap().as_deref(),
                    Some(&b"old snapshot"[..])
                );
                assert_eq!(db.get(b"discarded").unwrap(), None);
                assert_eq!(db.get(b"shared").unwrap(), Some(vec![4; 6000]));
            })
        })
        .collect();
    for reader in readers {
        reader.join().unwrap();
    }
    resume.send(()).unwrap();
    Arc::into_inner(db).unwrap().close().unwrap();

    let db = MiniDB::open(path).unwrap();
    assert_eq!(live_ids(path), [5]);
    assert_eq!(db.get(b"shared").unwrap(), Some(vec![4; 6000]));
}
