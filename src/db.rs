use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io,
    path::Path,
    sync::{
        Arc, Condvar, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard,
        mpsc,
    },
    thread::{self, JoinHandle},
};

use crate::{
    Error, Options, Result,
    compaction::{self, INPUT_COUNT},
    manifest::{self, Manifest},
    memtable::{Entry, MemTable},
    sstable::Sstable,
    storage::{remove_file_if_exists, staging_path, sync_directory},
    wal::{Wal, WalStage},
};

const DEFAULT_FLUSH_THRESHOLD_BYTES: usize = 4 * 1024 * 1024;

/// An exclusively opened database with durable writes and indexed disk tables.
/// Share a handle with `Arc<MiniDB>` for concurrent point reads and writes.
/// Mutations are serialized and become visible after WAL synchronization.
pub struct MiniDB {
    options: Options,
    flush_threshold_bytes: usize,
    shared: Arc<Shared>,
    writers: Mutex<()>,
    flush_requests: Option<mpsc::Sender<FlushRequest>>,
    flush_worker: Option<JoinHandle<()>>,
    compaction_worker: Option<JoinHandle<()>>,
    // Keep the lock until after the workers and WAL handle close.
    _lock: File,
}

struct State {
    wal: Wal,
    manifest: Manifest,
    last_file_id: u64,
    publication_uncertain: bool,
    shutdown: bool,
    compaction_error: Option<Error>,
    flush_worker_failed: bool,
}

struct ReadView {
    memtable: Arc<MemTable>,
    tables: Arc<Vec<Arc<Sstable>>>,
}

struct Shared {
    // Persistence operations take the state lock before taking the view lock.
    // Readers take only the view lock, and never hold it during file I/O.
    state: Mutex<State>,
    view: RwLock<ReadView>,
    changed: Condvar,
}

type FlushRequest = mpsc::Sender<Result<()>>;

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn read_view(&self) -> RwLockReadGuard<'_, ReadView> {
        self.view.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write_view(&self) -> RwLockWriteGuard<'_, ReadView> {
        self.view.write().unwrap_or_else(PoisonError::into_inner)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompactionStage {
    Merging,
    BeforeManifestRename,
    AfterManifestRename,
    AfterManifestSync,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FlushStage {
    ConstructingTable,
    AfterFilesSync,
    BeforeManifestRename,
    AfterManifestRename,
    AfterManifestSync,
}

impl MiniDB {
    /// Opens or creates a database with the default options.
    ///
    /// Returns an error if the directory is locked, the WAL cannot be read,
    /// or recovery detects corruption.
    ///
    /// ```
    /// # fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    /// # let directory = tempfile::tempdir()?;
    /// let db = minidb::MiniDB::open(directory.path())?;
    /// db.put(b"user:1", b"Alice")?;
    /// assert_eq!(db.get(b"user:1")?.as_deref(), Some(b"Alice".as_slice()));
    /// db.delete(b"user:1")?;
    /// assert_eq!(db.get(b"user:1")?, None);
    /// db.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, Options::default())
    }

    /// Opens or creates a database with the supplied input limits.
    ///
    /// Limits apply to calls on the new handle. Existing records are replayed
    /// using the WAL format limits. The default flush threshold is 4 MiB.
    pub fn open_with_options(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        Self::open_with_flush_threshold(path, options, DEFAULT_FLUSH_THRESHOLD_BYTES)
    }

    /// Opens or creates a database with a custom approximate MemTable byte limit.
    /// A zero threshold flushes after each successful mutation.
    pub fn open_with_flush_threshold(
        path: impl AsRef<Path>,
        options: Options,
        flush_threshold_bytes: usize,
    ) -> Result<Self> {
        Self::open_with_hooks(path, options, flush_threshold_bytes, |_| Ok(()), |_| Ok(()))
    }

    fn open_with_hooks(
        path: impl AsRef<Path>,
        options: Options,
        flush_threshold_bytes: usize,
        flush_hook: impl Fn(FlushStage) -> Result<()> + Send + 'static,
        compaction_hook: impl Fn(CompactionStage) -> Result<()> + Send + 'static,
    ) -> Result<Self> {
        if !cfg!(unix) {
            return Err(Error::io(
                "open database directory",
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    "MiniDB requires Unix directory sync",
                ),
            ));
        }
        let path = path.as_ref();
        fs::create_dir_all(path).map_err(|error| Error::io("create database directory", error))?;
        let path = path
            .canonicalize()
            .map_err(|error| Error::io("resolve database directory", error))?;
        let lock = lock_directory(&path)?;
        let shared = Arc::new(Shared::open(&path)?);
        let (flush_requests, requests) = mpsc::channel::<FlushRequest>();
        let worker_directory = path.clone();
        let worker_state = Arc::clone(&shared);
        let flush_worker = thread::Builder::new()
            .name("minidb-flush".into())
            .spawn(move || {
                for respond in requests {
                    let result =
                        worker_state.flush_with(&worker_directory, &flush_hook, Manifest::publish);
                    let _ = respond.send(result);
                }
            })
            .map_err(|error| Error::io("start flush worker", error))?;
        let mut db = Self {
            options,
            flush_threshold_bytes,
            shared,
            writers: Mutex::new(()),
            flush_requests: Some(flush_requests),
            flush_worker: Some(flush_worker),
            compaction_worker: None,
            _lock: lock,
        };
        let worker_state = Arc::clone(&db.shared);
        db.compaction_worker = Some(
            thread::Builder::new()
                .name("minidb-compaction".into())
                .spawn(move || compact_in_background(&path, &worker_state, compaction_hook))
                .map_err(|error| Error::io("start compaction worker", error))?,
        );
        Ok(db)
    }

    /// Inserts or replaces a value. Empty values are valid.
    ///
    /// Returns an error for an invalid input or a failed WAL append or sync.
    /// A WAL I/O failure requires reopening the database before more writes.
    /// [`Error::AutoFlushFailed`] means this write is already durable and visible.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.validate_key(key)?;
        let max = length_limit(self.options.max_value_bytes);
        if value.len() > max {
            return Err(Error::ValueTooLarge {
                len: value.len(),
                max,
            });
        }
        self.mutate(key, Some(value))
    }

    /// Returns an owned copy of a value, or `None` for an absent or deleted key.
    ///
    /// Returns an error for an empty key or one exceeding the key size limit.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_with(key, || {})
    }

    fn get_with(&self, key: &[u8], on_snapshot: impl FnOnce()) -> Result<Option<Vec<u8>>> {
        self.validate_key(key)?;
        let view = self.shared.read_view();
        if let Some(entry) = view.memtable.get(key) {
            return Ok(match entry {
                Entry::Value(value) => Some(value.clone()),
                Entry::Tombstone => None,
            });
        }
        let tables = Arc::clone(&view.tables);
        drop(view);
        on_snapshot();
        for table in tables.iter().rev() {
            match table.get(key)? {
                Some(Entry::Value(value)) => return Ok(Some(value)),
                Some(Entry::Tombstone) => return Ok(None),
                None => {}
            }
        }
        Ok(None)
    }

    /// Records a deletion, including when the key was already absent.
    ///
    /// Returns an error for an invalid key or a failed WAL append or sync.
    /// [`Error::AutoFlushFailed`] means this deletion is already durable and visible.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.validate_key(key)?;
        self.mutate(key, None)
    }

    /// Persists the active MemTable as an immutable table and rotates its WAL.
    /// Waits for the flush worker. Flushing an empty, writable MemTable has no effect.
    pub fn flush(&self) -> Result<()> {
        let _writer = self.lock_writer()?;
        self.request_flush()
    }

    /// Returns the first background compaction error recorded on this handle.
    ///
    /// Compaction pauses on error until reopen. Errors before manifest replacement
    /// permit foreground writes to continue.
    pub fn compaction_error(&self) -> Option<Error> {
        self.shared.lock().compaction_error.clone()
    }

    /// Joins the background workers and releases the directory lock.
    /// Successful mutations have already synchronized the WAL.
    ///
    /// Returns a worker panic or a recorded compaction error. When sharing via
    /// `Arc`, recover ownership of the handle before calling this method.
    pub fn close(mut self) -> Result<()> {
        self.stop_workers()
    }

    fn stop_workers(&mut self) -> Result<()> {
        let flush_worker = self.flush_worker.take();
        let compaction_worker = self.compaction_worker.take();
        if flush_worker.is_none() && compaction_worker.is_none() {
            return Ok(());
        }
        self.shared.lock().shutdown = true;
        self.shared.changed.notify_all();
        self.flush_requests.take();
        let flush_panicked = flush_worker.is_some_and(|worker| worker.join().is_err());
        let compaction_panicked = compaction_worker.is_some_and(|worker| worker.join().is_err());
        if flush_panicked {
            return Err(Error::FlushWorkerPanicked);
        }
        if compaction_panicked {
            return Err(Error::CompactionWorkerPanicked);
        }
        self.compaction_error().map_or(Ok(()), Err)
    }

    fn lock_writer(&self) -> Result<MutexGuard<'_, ()>> {
        self.writers.lock().map_err(|_| Error::FlushRequiresReopen)
    }

    // The initiating caller holds the writer gate until it receives this result.
    // The worker never takes that gate and receives at most one pending request.
    fn request_flush(&self) -> Result<()> {
        let (respond, result) = mpsc::channel();
        let requests = self
            .flush_requests
            .as_ref()
            .expect("open flush request channel");
        if requests.send(respond).is_ok()
            && let Ok(result) = result.recv()
        {
            return result;
        }

        self.shared.lock().flush_worker_failed = true;
        Err(Error::FlushWorkerPanicked)
    }

    fn mutate(&self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        self.mutate_with(key, value, |_| Ok(()))
    }

    fn mutate_with(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
        wal_hook: impl FnMut(WalStage) -> io::Result<()>,
    ) -> Result<()> {
        let _writer = self.lock_writer()?;
        let mut state = self.shared.lock();
        state.ensure_writable()?;
        if self.should_flush() {
            drop(state);
            self.request_flush()?;
            state = self.shared.lock();
            state.ensure_writable()?;
        }

        let entry = match value {
            Some(value) => Entry::Value(value.to_vec()),
            None => Entry::Tombstone,
        };
        state.wal.append_with(key, value, wal_hook)?;
        {
            let mut view = self.shared.write_view();
            Arc::get_mut(&mut view.memtable)
                .expect("writers are serialized with flush")
                .insert(key, entry);
        }
        drop(state);
        if self.should_flush() {
            self.request_flush()
                .map_err(|cause| Error::AutoFlushFailed {
                    cause: Box::new(cause),
                })?;
        }
        Ok(())
    }

    fn should_flush(&self) -> bool {
        let view = self.shared.read_view();
        !view.memtable.is_empty() && view.memtable.approximate_bytes() >= self.flush_threshold_bytes
    }

    fn validate_key(&self, key: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        let max = length_limit(self.options.max_key_bytes);
        if key.len() > max {
            return Err(Error::KeyTooLarge {
                len: key.len(),
                max,
            });
        }
        Ok(())
    }
}

impl Drop for MiniDB {
    fn drop(&mut self) {
        let _ = self.stop_workers();
    }
}

impl Shared {
    fn open(directory: &Path) -> Result<Self> {
        let loaded = Manifest::load(directory)?;
        let needs_manifest = loaded.is_none();
        let mut memtable = MemTable::default();
        let (manifest, wal, tables) = if let Some(manifest) = loaded {
            let tables = manifest
                .table_ids
                .iter()
                .map(|&id| Sstable::open(directory, id).map(Arc::new))
                .collect::<Result<Vec<_>>>()?;
            let wal = Wal::open_existing(directory, manifest.generation, &mut memtable)?;
            (manifest, wal, tables)
        } else {
            reject_numbered_files(directory)?;
            let wal = Wal::open_legacy(directory, &mut memtable)?;
            (Manifest::default(), wal, Vec::new())
        };

        // Certify file selections from this or a previously interrupted open.
        // Sync from the database directory to the root before deleting anything.
        for ancestor in directory.ancestors() {
            sync_directory(ancestor, "synchronize database directory ancestry")?;
        }
        if needs_manifest {
            let staged = manifest.stage(directory)?;
            Manifest::publish(directory, &staged)?;
            sync_directory(directory, "synchronize initial manifest directory")?;
        }
        remove_unreferenced_files(directory, &manifest);

        Ok(Self {
            state: Mutex::new(State {
                wal,
                last_file_id: manifest.highest_file_id,
                manifest,
                publication_uncertain: false,
                shutdown: false,
                compaction_error: None,
                flush_worker_failed: false,
            }),
            view: RwLock::new(ReadView {
                memtable: Arc::new(memtable),
                tables: Arc::new(tables),
            }),
            changed: Condvar::new(),
        })
    }

    fn flush_with(
        &self,
        directory: &Path,
        hook: impl Fn(FlushStage) -> Result<()>,
        publish_manifest: impl FnOnce(&Path, &Path) -> Result<()>,
    ) -> Result<()> {
        let mut state = self.lock();
        state.ensure_writable()?;
        let memtable = Arc::clone(&self.read_view().memtable);
        if memtable.is_empty() {
            return Ok(());
        }

        let previous = state.manifest.generation;
        let generation = next_file_id(state.last_file_id)?;
        let table = Sstable::create(directory, generation, &memtable, || {
            hook(FlushStage::ConstructingTable)
        })?;
        let wal = Wal::create(directory, generation)?;
        sync_directory(directory, "synchronize flush file names")?;
        hook(FlushStage::AfterFilesSync)?;

        let mut next = state.manifest.clone();
        next.generation = generation;
        next.highest_file_id = generation;
        next.table_ids.push(generation);
        let staged = next.stage(directory)?;
        hook(FlushStage::BeforeManifestRename)?;
        // A rename error can leave the replacement visible. Keep both generations
        // and reject later writes until the directory selection is resolved on open.
        state.publication_uncertain = true;
        publish_manifest(directory, &staged)?;
        hook(FlushStage::AfterManifestRename)?;
        sync_directory(directory, "synchronize published manifest directory")?;
        hook(FlushStage::AfterManifestSync)?;

        {
            let mut view = self.write_view();
            Arc::make_mut(&mut view.tables).push(Arc::new(table));
            view.memtable = Arc::default();
        }
        state.wal = wal;
        state.manifest = next;
        state.last_file_id = generation;
        state.publication_uncertain = false;
        self.changed.notify_one();
        let _ = remove_file_if_exists(&Wal::path(directory, previous));
        Ok(())
    }
}

impl State {
    fn ensure_writable(&self) -> Result<()> {
        if self.publication_uncertain {
            return Err(Error::FlushRequiresReopen);
        }
        if self.flush_worker_failed {
            return Err(Error::FlushWorkerPanicked);
        }
        self.wal.ensure_writable()
    }
}

fn compact_in_background(
    directory: &Path,
    shared: &Shared,
    hook: impl Fn(CompactionStage) -> Result<()>,
) {
    let result = run_compaction(directory, shared, &hook);
    if let Err(error) = result {
        shared.lock().compaction_error = Some(error);
        shared.changed.notify_all();
    }
}

fn run_compaction(
    directory: &Path,
    shared: &Shared,
    hook: &impl Fn(CompactionStage) -> Result<()>,
) -> Result<()> {
    loop {
        let mut state = shared.lock();
        while !state.shutdown
            && (state.publication_uncertain || state.manifest.table_ids.len() < INPUT_COUNT)
        {
            state = shared
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        if state.shutdown {
            return Ok(());
        }
        let id = fresh_compaction_id(directory, state.last_file_id)?;
        state.last_file_id = id;
        let inputs = shared.read_view().tables[..INPUT_COUNT].to_vec();
        drop(state);

        hook(CompactionStage::Merging)?;
        let output = compaction::merge(directory, id, &inputs)?;
        if output.is_some() {
            sync_directory(directory, "synchronize compaction output file name")?;
        }
        let mut state = shared.lock();
        if state.publication_uncertain {
            return Err(Error::FlushRequiresReopen);
        }

        // A flush can only append tables, so the selected prefix is still the
        // oldest. Its replacement goes before every table outside the merge.
        let mut next = state.manifest.clone();
        next.highest_file_id = state.last_file_id;
        next.table_ids
            .splice(..inputs.len(), output.iter().map(Sstable::id));
        let staged = next.stage(directory)?;
        hook(CompactionStage::BeforeManifestRename)?;
        state.publication_uncertain = true;
        Manifest::publish(directory, &staged)?;
        hook(CompactionStage::AfterManifestRename)?;
        sync_directory(directory, "synchronize compacted manifest directory")?;
        hook(CompactionStage::AfterManifestSync)?;

        Arc::make_mut(&mut shared.write_view().tables).splice(..inputs.len(), output.map(Arc::new));
        state.manifest = next;
        state.publication_uncertain = false;
        drop(state);
        for table in &inputs {
            table.retire();
        }
    }
}

fn next_file_id(last: u64) -> Result<u64> {
    last.checked_add(1).ok_or(Error::GenerationExhausted)
}

fn fresh_compaction_id(directory: &Path, mut last: u64) -> Result<u64> {
    // Recovery and failed flushes can leave artifacts outside the live
    // manifest. Skip any such name when reserving a compaction output.
    loop {
        let id = next_file_id(last)?;
        let path = Sstable::path(directory, id);
        if !file_name_exists(&path)? && !file_name_exists(&staging_path(&path))? {
            return Ok(id);
        }
        last = id;
    }
}

fn file_name_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(Error::io("inspect compaction output name", error)),
    }
}

fn reject_numbered_files(directory: &Path) -> Result<()> {
    let entries =
        fs::read_dir(directory).map_err(|error| Error::io("list files without manifest", error))?;
    for entry in entries {
        let entry = entry.map_err(|error| Error::io("inspect file without manifest", error))?;
        let name = entry.file_name();
        if let Some(name) = name.to_str() {
            let basename = name.strip_suffix(".tmp").unwrap_or(name);
            if Wal::numbered_id(basename).is_some() || Sstable::numbered_id(basename).is_some() {
                return Err(manifest::orphaned_without_manifest());
            }
        }
    }
    Ok(())
}

fn remove_unreferenced_files(directory: &Path, manifest: &Manifest) {
    let Ok(entries) = fs::read_dir(directory) else {
        return;
    };
    let live_table_ids: HashSet<_> = manifest.table_ids.iter().copied().collect();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let obsolete = match name.strip_suffix(".tmp") {
            Some(basename) => {
                matches!(basename, "MANIFEST" | "wal.log")
                    || Wal::numbered_id(basename).is_some()
                    || Sstable::numbered_id(basename).is_some()
            }
            None => {
                (name == "wal.log" && manifest.generation != 0)
                    || Wal::numbered_id(name).is_some_and(|id| id != manifest.generation)
                    || Sstable::numbered_id(name).is_some_and(|id| !live_table_ids.contains(&id))
            }
        };
        if obsolete {
            let _ = remove_file_if_exists(&entry.path());
        }
    }
}

fn length_limit(configured: Option<usize>) -> usize {
    configured
        .unwrap_or(Wal::MAX_FIELD_LEN)
        .min(Wal::MAX_FIELD_LEN)
}

fn lock_directory(path: &Path) -> Result<File> {
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path.join("LOCK"))
        .map_err(|error| Error::io("open database lock file", error))?;
    match fs2::FileExt::try_lock_exclusive(&lock) {
        Ok(()) => Ok(lock),
        Err(error) if error.raw_os_error() == fs2::lock_contended_error().raw_os_error() => {
            Err(Error::DatabaseLocked)
        }
        Err(error) => Err(Error::io("lock database directory", error)),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::process::Command;

    use super::*;
    use crate::storage::staging_path;

    const TEST_DIRECTORY: &str = "MINIDB_FLUSH_CRASH_DIRECTORY";
    const TEST_STAGE: &str = "MINIDB_FLUSH_CRASH_STAGE";
    const CRASH_CODE: i32 = 74;

    #[test]
    fn interrupted_flush_recovers_at_each_publication_boundary() {
        for (stage, manifest_replaced) in [
            ("table", false),
            ("before-rename", false),
            ("after-rename", true),
            ("after-sync", true),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path();
            let db = MiniDB::open(path).unwrap();
            db.put(b"deleted", b"old").unwrap();
            db.put(b"base", b"already on disk").unwrap();
            db.flush().unwrap();
            db.delete(b"deleted").unwrap();
            db.put(b"current", b"acknowledged").unwrap();
            db.close().unwrap();

            let old_manifest = fs::read(path.join("MANIFEST")).unwrap();
            let status = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "db::tests::interrupt_flush_child"])
                .env(TEST_DIRECTORY, path)
                .env(TEST_STAGE, stage)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(CRASH_CODE), "checkpoint {stage}");
            let manifest_at_crash = fs::read(path.join("MANIFEST")).unwrap();
            assert_eq!(manifest_at_crash != old_manifest, manifest_replaced);
            assert!(Wal::path(path, 1).is_file(), "old WAL still exists");
            let partial_table = staging_path(&Sstable::path(path, 2));
            if stage == "table" {
                assert!(partial_table.is_file());
                assert!(!Sstable::path(path, 2).exists());
            } else {
                assert!(Wal::path(path, 2).is_file());
                assert!(Sstable::path(path, 2).is_file());
            }

            let db = MiniDB::open(path).unwrap();
            assert_eq!(db.get(b"deleted").unwrap(), None);
            assert_eq!(
                db.get(b"base").unwrap().as_deref(),
                Some(&b"already on disk"[..])
            );
            assert_eq!(
                db.get(b"current").unwrap().as_deref(),
                Some(&b"acknowledged"[..])
            );
            assert!(!partial_table.exists());
            assert_eq!(Wal::path(path, 1).exists(), !manifest_replaced);
            assert_eq!(Wal::path(path, 2).exists(), manifest_replaced);
            assert_eq!(Sstable::path(path, 2).exists(), manifest_replaced);
            db.put(b"after", b"new append").unwrap();
            db.close().unwrap();
            assert_eq!(
                MiniDB::open(path)
                    .unwrap()
                    .get(b"after")
                    .unwrap()
                    .as_deref(),
                Some(&b"new append"[..])
            );
        }
    }

    #[test]
    fn interrupt_flush_child() {
        let Some(directory) = std::env::var_os(TEST_DIRECTORY) else {
            return;
        };
        let checkpoint = match std::env::var(TEST_STAGE).unwrap().as_str() {
            "table" => FlushStage::ConstructingTable,
            "before-rename" => FlushStage::BeforeManifestRename,
            "after-rename" => FlushStage::AfterManifestRename,
            "after-sync" => FlushStage::AfterManifestSync,
            stage => panic!("unknown checkpoint {stage}"),
        };
        let db = MiniDB::open_with_hooks(
            directory,
            Options::default(),
            DEFAULT_FLUSH_THRESHOLD_BYTES,
            move |stage| {
                if stage == checkpoint {
                    std::process::exit(CRASH_CODE);
                }
                Ok(())
            },
            |_| Ok(()),
        )
        .unwrap();
        db.flush().unwrap();
        panic!("checkpoint not reached");
    }

    #[test]
    fn uncertain_manifest_publication_blocks_mutations_and_preserves_both_generations() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path();
        let db = MiniDB::open(path).unwrap();
        db.put(b"old", b"durable").unwrap();
        let error = db.shared.flush_with(
            path,
            |stage| {
                if stage == FlushStage::AfterManifestRename {
                    return Err(Error::io(
                        "synchronize published manifest directory",
                        io::Error::other("simulated sync failure"),
                    ));
                }
                Ok(())
            },
            Manifest::publish,
        );
        assert!(matches!(error, Err(Error::Io { .. })));
        assert!(Wal::path(path, 0).is_file());
        assert!(Wal::path(path, 1).is_file());
        assert_eq!(db.get(b"old").unwrap().as_deref(), Some(&b"durable"[..]));
        assert_eq!(
            db.put(b"new", b"not written"),
            Err(Error::FlushRequiresReopen)
        );
        assert_eq!(db.flush(), Err(Error::FlushRequiresReopen));
        db.close().unwrap();

        let db = MiniDB::open(path).unwrap();
        assert!(!Wal::path(path, 0).exists());
        assert_eq!(db.get(b"old").unwrap().as_deref(), Some(&b"durable"[..]));
        assert_eq!(db.get(b"new").unwrap(), None);
        db.put(b"new", b"allowed").unwrap();
    }

    #[test]
    fn a_rename_error_can_replace_the_manifest_and_requires_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path();
        let db = MiniDB::open(path).unwrap();
        db.put(b"first", b"durable").unwrap();

        let error = db.shared.flush_with(
            path,
            |stage| {
                if stage == FlushStage::BeforeManifestRename {
                    return Err(Error::io(
                        "prepare manifest",
                        io::Error::other("simulated failure"),
                    ));
                }
                Ok(())
            },
            Manifest::publish,
        );
        assert!(matches!(
            error,
            Err(Error::Io {
                operation: "prepare manifest",
                ..
            })
        ));
        db.put(b"second", b"accepted before rename was attempted")
            .unwrap();
        let old_wal = Wal::path(path, 0);
        let contents = fs::read(&old_wal).unwrap();

        let error = db.shared.flush_with(
            path,
            |_| Ok(()),
            |directory, staged| {
                Manifest::publish(directory, staged)?;
                Err(Error::io(
                    "publish manifest",
                    io::Error::other("simulated rename failure"),
                ))
            },
        );
        assert!(matches!(
            error,
            Err(Error::Io {
                operation: "publish manifest",
                ..
            })
        ));
        assert_eq!(Manifest::load(path).unwrap().unwrap().generation, 1);
        assert_eq!(
            db.put(b"third", b"must not append"),
            Err(Error::FlushRequiresReopen)
        );
        assert_eq!(db.flush(), Err(Error::FlushRequiresReopen));
        assert_eq!(fs::read(&old_wal).unwrap(), contents);
        assert!(Wal::path(path, 1).is_file());
        db.close().unwrap();

        let db = MiniDB::open(path).unwrap();
        assert!(!old_wal.exists());
        assert_eq!(db.get(b"first").unwrap().as_deref(), Some(&b"durable"[..]));
        assert_eq!(
            db.get(b"second").unwrap().as_deref(),
            Some(&b"accepted before rename was attempted"[..])
        );
        assert_eq!(db.get(b"third").unwrap(), None);
    }
}

#[cfg(all(test, unix))]
mod compaction_tests;

#[cfg(all(test, unix))]
mod concurrency_tests;

#[cfg(all(test, unix))]
mod reliability_tests;
