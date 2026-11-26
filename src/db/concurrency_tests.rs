use std::{
    sync::{Barrier, mpsc},
    time::Duration,
};

use super::*;

const WAIT: Duration = Duration::from_secs(15);

#[test]
fn concurrent_readers_pin_the_same_disk_view() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<MiniDB>();
    let directory = tempfile::tempdir().unwrap();
    let db = Arc::new(MiniDB::open(directory.path()).unwrap());
    db.put(b"disk", &[5; 8192]).unwrap();
    db.flush().unwrap();
    db.put(b"memory", b"live").unwrap();
    db.delete(b"removed").unwrap();

    let (pinned, snapshots) = mpsc::channel();
    let readers: Vec<_> = (0..6)
        .map(|_| {
            let db = Arc::clone(&db);
            let pinned = pinned.clone();
            let (resume, pause) = mpsc::channel();
            let reader = thread::spawn(move || {
                assert_eq!(db.get(b"memory").unwrap(), Some(b"live".to_vec()));
                assert_eq!(db.get(b"removed").unwrap(), None);
                let disk = db
                    .get_with(b"disk", || {
                        pinned.send(()).unwrap();
                        pause.recv_timeout(WAIT).unwrap();
                    })
                    .unwrap();
                assert_eq!(disk, Some(vec![5; 8192]));
            });
            (resume, reader)
        })
        .collect();
    for _ in &readers {
        snapshots.recv_timeout(WAIT).unwrap();
    }
    db.put(b"disk", b"updated").unwrap();
    assert_eq!(db.get(b"disk").unwrap(), Some(b"updated".to_vec()));
    for (resume, _) in &readers {
        resume.send(()).unwrap();
    }
    for (_, reader) in readers {
        reader.join().unwrap();
    }
    Arc::into_inner(db).unwrap().close().unwrap();
}

#[test]
fn readers_see_only_wal_synced_publications_and_same_key_writers_serialize() {
    let directory = tempfile::tempdir().unwrap();
    let db = Arc::new(MiniDB::open(directory.path()).unwrap());
    db.put(b"key", b"original").unwrap();
    db.flush().unwrap();

    let (at_first_sync, first_sync) = mpsc::channel();
    let (resume_first, pause_first) = mpsc::channel();
    let first = {
        let db = Arc::clone(&db);
        thread::spawn(move || {
            db.mutate_with(b"key", Some(b"replacement"), |stage| {
                if stage == WalStage::AfterSync {
                    at_first_sync.send(()).unwrap();
                    pause_first.recv_timeout(WAIT).unwrap();
                }
                Ok(())
            })
            .unwrap();
        })
    };

    first_sync.recv_timeout(WAIT).unwrap();
    assert!(matches!(
        db.writers.try_lock(),
        Err(std::sync::TryLockError::WouldBlock)
    ));
    assert_eq!(db.get(b"key").unwrap(), Some(b"original".to_vec()));

    let (starting_second, second_started) = mpsc::channel();
    let (at_second_sync, second_sync) = mpsc::channel();
    let (resume_second, pause_second) = mpsc::channel();
    let second = {
        let db = Arc::clone(&db);
        thread::spawn(move || {
            starting_second.send(()).unwrap();
            db.mutate_with(b"key", None, |stage| {
                if stage == WalStage::BeforeSync {
                    at_second_sync.send(()).unwrap();
                    pause_second.recv_timeout(WAIT).unwrap();
                }
                Ok(())
            })
            .unwrap();
        })
    };
    second_started.recv_timeout(WAIT).unwrap();
    resume_first.send(()).unwrap();
    first.join().unwrap();
    second_sync.recv_timeout(WAIT).unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"replacement".to_vec()));
    resume_second.send(()).unwrap();
    second.join().unwrap();
    assert_eq!(db.get(b"key").unwrap(), None);
    Arc::into_inner(db).unwrap().close().unwrap();
    assert_eq!(
        MiniDB::open(directory.path()).unwrap().get(b"key").unwrap(),
        None
    );
}

#[test]
fn readers_continue_during_manifest_publication_by_the_flush_worker() {
    let directory = tempfile::tempdir().unwrap();
    let (reached, checkpoints) = mpsc::channel();
    let (resume, pause) = mpsc::channel();
    let db = Arc::new(
        MiniDB::open_with_hooks(
            directory.path(),
            Options::default(),
            DEFAULT_FLUSH_THRESHOLD_BYTES,
            move |stage| {
                if matches!(
                    stage,
                    FlushStage::ConstructingTable | FlushStage::AfterManifestRename
                ) {
                    reached
                        .send((stage, thread::current().name().unwrap().to_owned()))
                        .unwrap();
                    pause.recv_timeout(WAIT).unwrap();
                }
                Ok(())
            },
            |_| Ok(()),
        )
        .unwrap(),
    );
    db.put(b"visible", b"committed").unwrap();
    db.delete(b"removed").unwrap();
    let flushing = {
        let db = Arc::clone(&db);
        thread::spawn(move || db.flush().unwrap())
    };
    for expected in [
        FlushStage::ConstructingTable,
        FlushStage::AfterManifestRename,
    ] {
        assert_eq!(
            checkpoints.recv_timeout(WAIT).unwrap(),
            (expected, "minidb-flush".into())
        );
        assert_eq!(db.get(b"visible").unwrap(), Some(b"committed".to_vec()));
        assert_eq!(db.get(b"removed").unwrap(), None);
        assert_eq!(db.get(b"miss").unwrap(), None);
        resume.send(()).unwrap();
    }
    flushing.join().unwrap();
    assert!(db.shared.read_view().memtable.is_empty());
    assert_eq!(db.get(b"visible").unwrap(), Some(b"committed".to_vec()));
    Arc::into_inner(db).unwrap().close().unwrap();
}

#[test]
fn flush_worker_panic_unblocks_the_caller_and_shutdown() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let db = MiniDB::open_with_hooks(
        path,
        Options::default(),
        0,
        |stage| {
            assert_ne!(stage, FlushStage::ConstructingTable, "injected flush panic");
            Ok(())
        },
        |_| Ok(()),
    )
    .unwrap();
    assert_eq!(
        db.put(b"acknowledged", b"durable"),
        Err(Error::AutoFlushFailed {
            cause: Box::new(Error::FlushWorkerPanicked),
        })
    );
    assert_eq!(db.get(b"acknowledged").unwrap(), Some(b"durable".to_vec()));
    assert_eq!(
        db.put(b"later", b"blocked"),
        Err(Error::FlushWorkerPanicked)
    );
    assert_eq!(db.close(), Err(Error::FlushWorkerPanicked));
    let db = MiniDB::open(path).unwrap();
    assert_eq!(db.get(b"acknowledged").unwrap(), Some(b"durable".to_vec()));
    assert_eq!(db.get(b"later").unwrap(), None);
    db.close().unwrap();
}

#[test]
fn readers_and_writers_make_progress_under_sustained_flush_load() {
    const WRITERS: u8 = 4;
    const READERS: u8 = 4;
    const SLOTS: u8 = 12;
    const ROUNDS: u8 = 8;
    let directory = tempfile::tempdir().unwrap();
    let db = MiniDB::open_with_flush_threshold(directory.path(), Options::default(), 4096).unwrap();
    let start = Barrier::new((WRITERS + READERS).into());
    thread::scope(|scope| {
        let (progress, stages): (Vec<_>, Vec<_>) = (0..READERS).map(|_| mpsc::channel()).unzip();
        for shard in 0..WRITERS {
            let db = &db;
            let start = &start;
            let progress = if shard == 0 {
                progress.clone()
            } else {
                Vec::new()
            };
            scope.spawn(move || {
                start.wait();
                for round in 1..=ROUNDS {
                    for slot in 0..SLOTS {
                        let key = [shard, slot, 255];
                        if (round + slot).is_multiple_of(3) {
                            db.delete(&key).unwrap();
                        } else {
                            db.put(&key, &vec![round; 512]).unwrap();
                        }
                    }
                    db.flush().unwrap();
                    for reader in &progress {
                        let _ = reader.send(());
                    }
                }
            });
        }
        drop(progress);
        for (reader, stages) in (0..READERS).zip(stages) {
            let db = &db;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                let mut observed = [[0; SLOTS as usize]; WRITERS as usize];
                for _ in 1..=ROUNDS {
                    stages.recv_timeout(WAIT).unwrap();
                    for sample in 0..96 {
                        let shard = (reader + sample) % WRITERS;
                        let slot = sample % SLOTS;
                        if let Some(value) = db.get(&[shard, slot, 255]).unwrap() {
                            assert_eq!(value.len(), 512);
                            let version = value[0];
                            assert!(version > 0 && version <= ROUNDS);
                            assert!(value.iter().all(|&byte| byte == version));
                            let previous = &mut observed[shard as usize][slot as usize];
                            assert!(version >= *previous);
                            *previous = version;
                        }
                    }
                }
            });
        }
    });
    db.close().unwrap();

    let db = MiniDB::open(directory.path()).unwrap();
    for shard in 0..WRITERS {
        for slot in 0..SLOTS {
            let expected = (!(ROUNDS + slot).is_multiple_of(3)).then(|| vec![ROUNDS; 512]);
            assert_eq!(db.get(&[shard, slot, 255]).unwrap(), expected);
        }
    }
    db.close().unwrap();
}
