//! Release-mode benchmark executable. See docs/performance.md.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    env, fs,
    hint::black_box,
    io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    thread,
    time::{Duration, Instant},
};

use minidb::{MiniDB, Options, Result};

const VALUE_BYTES: usize = 512;
const NO_AUTO_FLUSH: usize = usize::MAX;
const SEED: u64 = 0x5eeda5c314159265;
const WAL_RECORD_BYTES: u64 = (8 + VALUE_BYTES + 17) as u64;
const WAL_HEADER_BYTES: u64 = 8;
const MAX_WAIT: Duration = Duration::from_secs(20);
static PROFILE: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
const PROFILE_LIVE: bool = option_env!("MINIDB_PROFILE_LIVE").is_some();
static LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static PEAK_LIVE_BYTES: AtomicU64 = AtomicU64::new(0);

struct CountingAllocator;

// SAFETY: Every operation forwards the unchanged pointer and layout to System.
// Counters record successful allocation and reallocation requests only.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        count(ptr, layout.size());
        if !ptr.is_null() {
            adjust_live(0, layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        adjust_live(layout.size(), 0);
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let ptr = unsafe { System.realloc(ptr, layout, size) };
        count(ptr, size);
        if !ptr.is_null() {
            adjust_live(layout.size(), size);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        count(ptr, layout.size());
        if !ptr.is_null() {
            adjust_live(0, layout.size());
        }
        ptr
    }
}

fn count(ptr: *mut u8, size: usize) {
    if PROFILE.load(Relaxed) && !ptr.is_null() {
        ALLOCS.fetch_add(1, Relaxed);
        BYTES.fetch_add(size as u64, Relaxed);
    }
}

fn adjust_live(previous: usize, current: usize) {
    if PROFILE_LIVE {
        if current > previous {
            let additional = (current - previous) as u64;
            let total = LIVE_BYTES.fetch_add(additional, Relaxed) + additional;
            PEAK_LIVE_BYTES.fetch_max(total, Relaxed);
        } else {
            LIVE_BYTES.fetch_sub((previous - current) as u64, Relaxed);
        }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

type BenchResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn open(path: &Path) -> Result<MiniDB> {
    MiniDB::open_with_flush_threshold(path, Options::default(), NO_AUTO_FLUSH)
}

fn key(index: u64, absent: bool) -> [u8; 8] {
    (index * 2 + u64::from(absent)).to_be_bytes()
}

fn populate(db: &MiniDB, begin: u64, end: u64, value: &[u8]) -> Result<()> {
    for i in begin..end {
        db.put(&key(i, false), value)?;
    }
    Ok(())
}

fn dataset(path: &Path, rows: u64) -> Result<()> {
    let db = open(path)?;
    populate(&db, 0, rows, &[0x8e; VALUE_BYTES])?;
    db.flush()?;
    db.close()
}

struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

struct Run {
    rows: u64,
    trial: u64,
    profile: bool,
}

impl Run {
    fn measure(
        &self,
        workload: &str,
        count: usize,
        mut operation: impl FnMut(usize) -> BenchResult,
    ) -> BenchResult {
        let mut samples = Vec::with_capacity(count);
        ALLOCS.store(0, Relaxed);
        BYTES.store(0, Relaxed);
        PROFILE.store(self.profile, Relaxed);
        let start = Instant::now();
        for index in 0..count {
            let call = Instant::now();
            operation(index)?;
            samples.push(call.elapsed().as_secs_f64() * 1e6);
        }
        let wall = start.elapsed().as_secs_f64();
        PROFILE.store(false, Relaxed);
        let allocs = ALLOCS.load(Relaxed);
        let bytes = BYTES.load(Relaxed);
        samples.sort_unstable_by(f64::total_cmp);
        let percentile = |percent: usize| samples[(count * percent).div_ceil(100) - 1];
        println!(
            "sample\t{}\t{}\t{}\t{}\t{}\t{:.3}\t{:.3}\t{:.3}\t{:.3}\t{}\t{}",
            self.rows,
            self.trial,
            if self.profile { "alloc" } else { "timing" },
            workload,
            count,
            count as f64 / wall,
            percentile(50),
            percentile(95),
            percentile(99),
            allocs,
            bytes
        );
        Ok(())
    }

    fn storage(&self, workload: &str, key: &str, value: u64) {
        println!(
            "storage\t{}\t{}\t{workload}\t{key}\t{value}",
            self.rows, self.trial
        );
    }
}

fn write_workloads(run: &Run, scratch: &Path, count: usize) -> BenchResult {
    let value = [0xd3; VALUE_BYTES];
    let sequential = tempfile::tempdir_in(scratch)?;
    let db = open(sequential.path())?;
    run.measure("sequential_put", count, |i| {
        Ok(db.put(&key(i as u64, false), &value)?)
    })?;
    db.close()?;

    let random = tempfile::tempdir_in(scratch)?;
    let db = open(random.path())?;
    // Odd multiplication permutes the power-of-two range while keeping keys unique.
    let key_space = count.next_power_of_two() as u64;
    run.measure("random_put", count, |i| {
        let index = (i as u64 * 0x9e37) & (key_space - 1);
        Ok(db.put(&key(index, false), &value)?)
    })?;
    db.close()?;
    Ok(())
}

fn read_workloads(run: &Run, fixture: &Path, count: usize) -> BenchResult {
    let mut rng = Random(SEED + run.trial);
    let keys: Vec<_> = (0..count).map(|_| rng.next() % run.rows).collect();
    let db = open(fixture)?;
    for label in ["random_get_process_cold", "random_get_warm"] {
        run.measure(label, count, |i| {
            let value = black_box(db.get(&key(keys[i], false))?).expect("expected hit");
            assert_eq!(value.len(), VALUE_BYTES);
            Ok(())
        })?;
    }
    run.measure("random_get_absent", count, |i| {
        assert!(black_box(db.get(&key(keys[i], true))?).is_none());
        Ok(())
    })?;
    db.close()?;
    Ok(())
}

fn mixed_workload(run: &Run, fixture: &Path, scratch: &Path, count: usize) -> BenchResult {
    let copy = tempfile::tempdir_in(scratch)?;
    for entry in fs::read_dir(fixture)? {
        let entry = entry?;
        fs::copy(entry.path(), copy.path().join(entry.file_name()))?;
    }
    let db = open(copy.path())?;
    let mut rng = Random(SEED + run.trial);
    let value = [0x36; VALUE_BYTES];
    run.measure("mixed_80_get_20_put", count, |i| {
        let key = key(rng.next() % run.rows, false);
        if i % 5 == 0 {
            db.put(&key, &value)?;
        } else {
            assert_eq!(black_box(db.get(&key)?).unwrap().len(), VALUE_BYTES);
        }
        Ok(())
    })?;
    db.close()?;
    Ok(())
}

fn tables(path: &Path) -> io::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(path)? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "sst") {
            paths.push(path);
        }
    }
    paths.sort_unstable();
    Ok(paths)
}

fn length(path: &Path) -> io::Result<u64> {
    Ok(fs::metadata(path)?.len())
}

fn data_length(path: &Path) -> io::Result<u64> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = fs::File::open(path)?;
    file.seek(SeekFrom::End(-12))?;
    let mut offset = [0; 8];
    file.read_exact(&mut offset)?;
    Ok(u64::from_le_bytes(offset) - 16)
}

fn overwrite_workload(run: &Run, scratch: &Path, count: u64) -> BenchResult {
    let path = tempfile::tempdir_in(scratch)?;
    let db = open(path.path())?;
    let mut flush_samples = Vec::new();
    for round in 0..3_u8 {
        let value = [round; VALUE_BYTES];
        populate(&db, 0, count, &value)?;
        let start = Instant::now();
        db.flush()?;
        flush_samples.push(start.elapsed().as_micros() as u64);
    }
    db.close()?;
    let table_bytes: u64 = tables(path.path())?
        .iter()
        .map(|file| length(file))
        .sum::<io::Result<_>>()?;
    let wal_bytes = WAL_RECORD_BYTES * 3 * count + 4 * WAL_HEADER_BYTES;
    let manifest_bytes = 4 * 36 + 8 * (1 + 2 + 3);
    let logical_bytes = 3 * count * (8 + VALUE_BYTES as u64);
    run.storage("overwrites", "sstable_write_bytes", table_bytes);
    run.storage("overwrites", "wal_write_bytes", wal_bytes);
    run.storage("overwrites", "manifest_write_bytes", manifest_bytes);
    run.storage("overwrites", "logical_user_bytes", logical_bytes);
    run.storage(
        "overwrites",
        "write_amplification_x1000",
        (table_bytes + wal_bytes + manifest_bytes) * 1000 / logical_bytes,
    );
    for (index, micros) in flush_samples.into_iter().enumerate() {
        run.storage("overwrites", &format!("flush_{index}_us"), micros);
    }
    Ok(())
}

fn compaction_workload(run: &Run, scratch: &Path, count: u64) -> BenchResult {
    let chunk = count / 4;
    let path = tempfile::tempdir_in(scratch)?;
    let db = open(path.path())?;
    for table in 0..3 {
        populate(
            &db,
            table * chunk,
            (table + 1) * chunk,
            &[table as u8; VALUE_BYTES],
        )?;
        db.flush()?;
    }
    let mut read_bytes = tables(path.path())?
        .iter()
        .map(|file| data_length(file))
        .sum::<io::Result<u64>>()?;
    // Compute the fourth input's data extent on a one-table twin. It can be
    // retired before the foreground caller sees the triggering flush return.
    let twin = tempfile::tempdir_in(scratch)?;
    let twin_db = open(twin.path())?;
    populate(&twin_db, 3 * chunk, 4 * chunk, &[3; VALUE_BYTES])?;
    twin_db.flush()?;
    twin_db.close()?;
    read_bytes += data_length(&tables(twin.path())?[0])?;
    populate(&db, 3 * chunk, 4 * chunk, &[3; VALUE_BYTES])?;
    let started = Instant::now();
    db.flush()?;
    let flushed = Instant::now();
    let (output, retired) = loop {
        let paths = tables(path.path())?;
        if paths.len() == 1 {
            break (paths.into_iter().next().unwrap(), Instant::now());
        }
        if let Some(error) = db.compaction_error() {
            return Err(error.into());
        }
        if started.elapsed() > MAX_WAIT {
            return Err(
                io::Error::new(io::ErrorKind::TimedOut, "compaction did not finish").into(),
            );
        }
        thread::sleep(Duration::from_micros(100));
    };
    let trigger_to_retirement = retired.duration_since(started).as_micros() as u64;
    let post_flush_wait = retired.duration_since(flushed).as_micros() as u64;
    run.storage(
        "compaction",
        "trigger_to_retirement_us",
        trigger_to_retirement,
    );
    run.storage("compaction", "post_flush_wait_us", post_flush_wait);
    run.storage("compaction", "read_data_block_bytes", read_bytes);
    run.storage("compaction", "write_output_table_bytes", length(&output)?);
    db.close()?;
    Ok(())
}

fn restart_workload(run: &Run, scratch: &Path, count: usize) -> BenchResult {
    let path = tempfile::tempdir_in(scratch)?;
    let db = open(path.path())?;
    populate(&db, 0, count as u64, &[0x26; VALUE_BYTES])?;
    db.flush()?;
    populate(&db, 0, count as u64, &[0x27; VALUE_BYTES])?;
    db.close()?;
    run.storage("restart", "wal_records", count as u64);
    run.measure("restart", 1, |_| {
        // The timer represents open plus close (including worker shutdown).
        let reopened = black_box(open(path.path())?);
        reopened.close()?;
        Ok(())
    })
}

fn memory_workload(fixture: &Path, rows: u64, passes: u64) -> BenchResult {
    let db = open(fixture)?;
    let mut rng = Random(SEED);
    for _ in 0..(rows * passes) {
        assert_eq!(
            black_box(db.get(&key(rng.next() % rows, false))?)
                .unwrap()
                .len(),
            VALUE_BYTES
        );
    }
    db.close()?;
    if PROFILE_LIVE {
        println!(
            "peak_live_requested_bytes={}",
            PEAK_LIVE_BYTES.load(Relaxed)
        );
    }
    Ok(())
}

fn main() -> BenchResult {
    let args: Vec<_> = env::args_os().skip(1).collect();
    let mode = args
        .first()
        .and_then(|arg| arg.to_str())
        .unwrap_or_default();
    if !(3..=6).contains(&args.len()) {
        return Err(io::Error::other(
            "usage: perf fixture|run|alloc|memory|compaction FIXTURE ROWS [OPS TRIAL SCRATCH | PASSES]",
        )
        .into());
    }
    let fixture = Path::new(&args[1]);
    let rows: u64 = args[2].to_string_lossy().parse()?;
    if rows < 16 {
        return Err(io::Error::other("ROWS must be at least 16").into());
    }
    match mode {
        "fixture" => return Ok(dataset(fixture, rows)?),
        "memory" => {
            let passes: u64 = args
                .get(3)
                .map(|arg| arg.to_string_lossy().parse())
                .transpose()?
                .unwrap_or(4);
            return memory_workload(fixture, rows, passes);
        }
        _ => {}
    }
    if args.len() != 6 || !matches!(mode, "run" | "alloc" | "compaction") {
        return Err(io::Error::other("run, alloc and compaction require OPS TRIAL SCRATCH").into());
    }
    let count: usize = args[3].to_string_lossy().parse()?;
    if count < 16 {
        return Err(io::Error::other("OPS must be at least 16").into());
    }
    let run = Run {
        rows,
        trial: args[4].to_string_lossy().parse()?,
        profile: mode == "alloc",
    };
    let scratch = Path::new(&args[5]);
    if mode == "compaction" {
        return compaction_workload(&run, scratch, count as u64);
    }
    let read_count = (count * 8).max(1024);
    read_workloads(&run, fixture, read_count)?;
    if !run.profile {
        write_workloads(&run, scratch, count)?;
    }
    mixed_workload(&run, fixture, scratch, count)?;
    if !run.profile {
        let maintenance_keys = count as u64;
        overwrite_workload(&run, scratch, maintenance_keys)?;
        compaction_workload(&run, scratch, maintenance_keys)?;
        restart_workload(&run, scratch, count)?;
    }
    Ok(())
}
