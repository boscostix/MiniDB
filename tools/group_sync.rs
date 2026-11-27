use std::{
    fs::{self, File},
    io::Write,
    path::Path,
    time::Instant,
};

fn percentile(sorted: &[u64], percent: usize) -> f64 {
    sorted[(sorted.len() * percent).div_ceil(100) - 1] as f64 / 1_000.0
}

fn trial(path: &Path, group: usize) {
    const RECORDS: usize = 4096;
    let mut file = File::options()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    file.sync_all().unwrap();
    let mut durations = Vec::with_capacity(RECORDS);
    let mut starts = [Instant::now(); 32];
    let start = Instant::now();
    for _ in 0..RECORDS / group {
        for started in &mut starts[..group] {
            *started = Instant::now();
            for field in [&[0u8; 13][..], &[1u8; 16], &[2u8; 128], &[3u8; 4]] {
                file.write_all(field).unwrap();
            }
        }
        file.sync_all().unwrap();
        let acknowledged = Instant::now();
        durations.extend(
            starts[..group]
                .iter()
                .map(|point| acknowledged.duration_since(*point).as_nanos() as u64),
        );
    }
    let elapsed = start.elapsed().as_secs_f64();
    durations.sort_unstable();
    println!(
        "{group},{:.0},{:.2},{:.2},{:.2}",
        RECORDS as f64 / elapsed,
        percentile(&durations, 50),
        percentile(&durations, 95),
        percentile(&durations, 99)
    );
    drop(file);
    fs::remove_file(path).unwrap();
}

fn main() {
    let directory = std::env::args_os().nth(1).unwrap();
    let path = Path::new(&directory).join("sync-study.log");
    println!("group,ops_per_sec,p50_us,p95_us,p99_us");
    for groups in [[1, 8, 32], [32, 1, 8], [8, 32, 1]] {
        for group in groups {
            trial(&path, group);
        }
    }
}
