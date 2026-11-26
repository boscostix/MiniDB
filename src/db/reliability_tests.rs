use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Write},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use super::*;

const KEY_COUNT: u8 = 16;
const WAIT: Duration = Duration::from_secs(15);
const CRASH_DIRECTORY: &str = "MINIDB_RELIABILITY_DIRECTORY";
const CRASH_SEED: &str = "MINIDB_RELIABILITY_SEED";
const CRASH_STAGE: &str = "MINIDB_RELIABILITY_STAGE";
const ACK_PREFIX: &str = "minidb-reliability-ack ";
const AT_CHECKPOINT: &str = "minidb-reliability-paused";
const CHECKPOINT_KEY: &[u8] = b"\0checkpoint\xff";
const CHECKPOINT_VALUE: &[u8] = b"always the same value";

type Key = [u8; 3];
type Model = BTreeMap<Key, Vec<u8>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Checkpoint {
    Wal(WalStage),
    Flush(FlushStage),
    Compaction(CompactionStage),
}

const CHECKPOINTS: [Checkpoint; 11] = [
    Checkpoint::Wal(WalStage::HeaderWritten),
    Checkpoint::Wal(WalStage::BeforeSync),
    Checkpoint::Wal(WalStage::AfterSync),
    Checkpoint::Flush(FlushStage::ConstructingTable),
    Checkpoint::Flush(FlushStage::AfterFilesSync),
    Checkpoint::Flush(FlushStage::BeforeManifestRename),
    Checkpoint::Flush(FlushStage::AfterManifestRename),
    Checkpoint::Flush(FlushStage::AfterManifestSync),
    Checkpoint::Compaction(CompactionStage::BeforeManifestRename),
    Checkpoint::Compaction(CompactionStage::AfterManifestRename),
    Checkpoint::Compaction(CompactionStage::AfterManifestSync),
];

struct Generator(u64);

impl Generator {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 32
    }

    fn key(&mut self) -> Key {
        key_at((self.next() % u64::from(KEY_COUNT)) as u8)
    }

    fn mutation(&mut self) -> Operation {
        let key = self.key();
        match self.next() % 4 {
            0 => Operation::Delete(key),
            1 => Operation::Put(key, Vec::new()),
            2 => Operation::Put(key, self.next().to_le_bytes().to_vec()),
            _ => Operation::Put(key, vec![self.next() as u8; 4200]),
        }
    }

    fn operation(&mut self) -> Operation {
        match self.next() % 9 {
            0 => Operation::Flush,
            1..=3 => Operation::Get(self.key()),
            _ => self.mutation(),
        }
    }
}

fn key_at(index: u8) -> Key {
    [255, 0, index]
}

#[derive(Debug)]
enum Operation {
    Put(Key, Vec<u8>),
    Delete(Key),
    Get(Key),
    Flush,
}

impl Operation {
    fn run(&self, db: &MiniDB) -> Result<()> {
        match self {
            Self::Put(key, value) => db.put(key, value),
            Self::Delete(key) => db.delete(key),
            Self::Get(key) => db.get(key).map(|_| ()),
            Self::Flush => db.flush(),
        }
    }

    fn apply(&self, model: &mut Model) {
        match self {
            Self::Put(key, value) => {
                model.insert(*key, value.clone());
            }
            Self::Delete(key) => {
                model.remove(key);
            }
            Self::Get(_) | Self::Flush => {}
        }
    }

    fn as_mutation(&self) -> (&Key, Option<&[u8]>) {
        match self {
            Self::Put(key, value) => (key, Some(value)),
            Self::Delete(key) => (key, None),
            Self::Get(_) | Self::Flush => unreachable!("the generator selects a mutation"),
        }
    }
}

fn validate_model(db: &MiniDB, model: &Model, context: &str) {
    for index in 0..KEY_COUNT {
        let key = key_at(index);
        assert_eq!(
            db.get(&key).unwrap().as_ref(),
            model.get(&key),
            "{context}, key {key:?}"
        );
    }
    assert_eq!(db.get(b"unknown").unwrap(), None, "{context}");
}

#[test]
fn seeded_operations_match_a_btree_map_across_reopens() {
    for (seed, threshold) in [(15, 0), (2718, 8192), (31415, usize::MAX)] {
        let directory = tempfile::tempdir().unwrap();
        let mut generator = Generator(seed);
        let mut model = Model::new();
        for phase in 0..4 {
            let db =
                MiniDB::open_with_flush_threshold(directory.path(), Options::default(), threshold)
                    .unwrap();
            validate_model(&db, &model, &format!("seed {seed}, reopen {phase}"));
            for step in 0..30 {
                let operation = generator.operation();
                operation.run(&db).unwrap();
                operation.apply(&mut model);
                validate_model(
                    &db,
                    &model,
                    &format!("seed {seed}, phase {phase}, step {step}"),
                );
            }
            db.close().unwrap();
        }
    }
}

fn crash_workload(seed: u64) -> (Vec<Operation>, Operation) {
    let mut generator = Generator(seed);
    let count = 10 + generator.next() % 10;
    let operations = (0..count).map(|_| generator.operation()).collect();
    (operations, generator.mutation())
}

#[test]
fn seeded_workloads_recover_acknowledged_operations_after_repeated_process_kills() {
    for initial_seed in [199, 7919] {
        let directory = tempfile::tempdir().unwrap();
        let mut model = Model::new();
        for round in 0..CHECKPOINTS.len() {
            let seed = initial_seed + round as u64 * 2718;
            let stage_index = (initial_seed as usize + round * 7) % CHECKPOINTS.len();
            let checkpoint = CHECKPOINTS[stage_index];
            let context = format!("seed {seed}, checkpoint {checkpoint:?}");
            let (prefix, interrupted) = crash_workload(seed);
            let mut child = CrashChild(
                Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "db::reliability_tests::crash_workload_child",
                        "--nocapture",
                        "--test-threads=1",
                    ])
                    .env(CRASH_DIRECTORY, directory.path())
                    .env(CRASH_SEED, seed.to_string())
                    .env(CRASH_STAGE, stage_index.to_string())
                    .stdout(Stdio::piped())
                    .spawn()
                    .unwrap(),
            );
            let stdout = child.0.stdout.take().unwrap();
            let (events, receive) = mpsc::channel();
            let relay = thread::spawn(move || {
                for line in BufReader::new(stdout)
                    .lines()
                    .map_while(std::result::Result::ok)
                {
                    if (line.starts_with(ACK_PREFIX) || line == AT_CHECKPOINT)
                        && events.send(line).is_err()
                    {
                        return;
                    }
                }
            });
            for (index, operation) in prefix.iter().enumerate() {
                let acknowledgment = receive
                    .recv_timeout(WAIT)
                    .unwrap_or_else(|error| panic!("{context}: awaiting ack {index}: {error}"));
                assert_eq!(acknowledgment, format!("{ACK_PREFIX}{index}"), "{context}");
                operation.apply(&mut model);
            }
            let paused = receive
                .recv_timeout(WAIT)
                .unwrap_or_else(|error| panic!("{context}: awaiting crash checkpoint: {error}"));
            assert_eq!(paused, AT_CHECKPOINT, "{context}");
            assert!(matches!(
                MiniDB::open(directory.path()),
                Err(Error::DatabaseLocked)
            ));
            child.0.kill().unwrap();
            assert!(!child.0.wait().unwrap().success(), "{context}");
            relay.join().unwrap();

            let db = MiniDB::open(directory.path()).unwrap();
            if let Checkpoint::Wal(stage) = checkpoint {
                let (key, proposed) = interrupted.as_mutation();
                let actual = db.get(key).unwrap();
                match stage {
                    WalStage::HeaderWritten => {
                        assert_eq!(actual.as_ref(), model.get(key), "{context}");
                    }
                    WalStage::BeforeSync => {
                        // A complete record can be recovered even if it had not synchronized.
                        if actual.as_deref() == proposed {
                            interrupted.apply(&mut model);
                        }
                    }
                    WalStage::AfterSync => interrupted.apply(&mut model),
                }
            }
            validate_model(&db, &model, &context);
            assert_eq!(
                db.get(CHECKPOINT_KEY).unwrap().as_deref(),
                Some(CHECKPOINT_VALUE),
                "{context}"
            );
            db.close().unwrap();
        }
    }
}

struct CrashChild(Child);

impl Drop for CrashChild {
    fn drop(&mut self) {
        if self.0.try_wait().is_ok_and(|status| status.is_none()) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

#[test]
fn crash_workload_child() {
    let Some(directory) = std::env::var_os(CRASH_DIRECTORY) else {
        return;
    };
    let seed = std::env::var(CRASH_SEED).unwrap().parse().unwrap();
    let stage_index: usize = std::env::var(CRASH_STAGE).unwrap().parse().unwrap();
    let checkpoint = CHECKPOINTS[stage_index];
    let armed = Arc::new(AtomicBool::new(false));
    let arming_flush = Arc::clone(&armed);
    let arming_compaction = Arc::clone(&armed);
    let db = MiniDB::open_with_hooks(
        directory,
        Options::default(),
        usize::MAX,
        move |stage| {
            if checkpoint == Checkpoint::Flush(stage) && arming_flush.load(Ordering::Acquire) {
                pause_for_kill();
            }
            Ok(())
        },
        move |stage| {
            if checkpoint == Checkpoint::Compaction(stage)
                && arming_compaction.load(Ordering::Acquire)
            {
                pause_for_kill();
            }
            Ok(())
        },
    )
    .unwrap();

    let (prefix, interrupted) = crash_workload(seed);
    db.put(CHECKPOINT_KEY, CHECKPOINT_VALUE).unwrap();
    for (index, operation) in prefix.iter().enumerate() {
        operation.run(&db).unwrap();
        println!("\n{ACK_PREFIX}{index}");
        io::stdout().flush().unwrap();
    }
    armed.store(true, Ordering::Release);
    match checkpoint {
        Checkpoint::Wal(_) => {
            let (key, value) = interrupted.as_mutation();
            db.mutate_with(key, value, |stage| {
                if checkpoint == Checkpoint::Wal(stage) {
                    pause_for_kill();
                }
                Ok(())
            })
            .unwrap();
        }
        Checkpoint::Flush(_) => {
            db.put(CHECKPOINT_KEY, CHECKPOINT_VALUE).unwrap();
            db.flush().unwrap();
        }
        Checkpoint::Compaction(_) => {
            for _ in 0..INPUT_COUNT {
                db.put(CHECKPOINT_KEY, CHECKPOINT_VALUE).unwrap();
                db.flush().unwrap();
            }
            thread::park_timeout(WAIT);
        }
    }
    panic!("crash checkpoint {checkpoint:?} was not reached");
}

fn pause_for_kill() -> ! {
    println!("\n{AT_CHECKPOINT}");
    io::stdout().flush().unwrap();
    loop {
        thread::park();
    }
}
