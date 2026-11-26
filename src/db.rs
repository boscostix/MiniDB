use std::{
    cell::RefCell,
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
};

use crate::{
    Error, Options, Result,
    manifest::{self, Manifest},
    memtable::{Entry, MemTable},
    sstable::Sstable,
    storage::{remove_file_if_exists, sync_directory},
    wal::Wal,
};

const DEFAULT_FLUSH_THRESHOLD_BYTES: usize = 4 * 1024 * 1024;

/// An exclusively opened database with durable writes and indexed disk tables.
/// A handle is intended for use by one thread at a time.
pub struct MiniDB {
    options: Options,
    flush_threshold_bytes: usize,
    directory: PathBuf,
    state: RefCell<State>,
    // Keep the lock until after the WAL handle closes.
    _lock: File,
}

struct State {
    memtable: MemTable,
    wal: Wal,
    manifest: Manifest,
    tables: Vec<Sstable>,
    publication_uncertain: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FlushStage {
    ConstructingTable,
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
        let state = State::open(&path)?;
        Ok(Self {
            options,
            flush_threshold_bytes,
            directory: path,
            state: RefCell::new(state),
            _lock: lock,
        })
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
        self.validate_key(key)?;
        let state = self.state.borrow();
        if let Some(entry) = state.memtable.get(key) {
            return Ok(match entry {
                Entry::Value(value) => Some(value.clone()),
                Entry::Tombstone => None,
            });
        }
        for table in state.tables.iter().rev() {
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
    /// Flushing an empty, writable MemTable has no effect.
    pub fn flush(&self) -> Result<()> {
        self.state.borrow_mut().flush(&self.directory)
    }

    /// Consumes the handle and releases the directory lock.
    /// Successful mutations have already synchronized the WAL.
    pub fn close(self) -> Result<()> {
        Ok(())
    }

    fn mutate(&self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        let mut state = self.state.borrow_mut();
        state.ensure_writable()?;
        if state.should_flush(self.flush_threshold_bytes) {
            state.flush(&self.directory)?;
        }

        state.wal.append(key, value)?;
        let entry = match value {
            Some(value) => Entry::Value(value.to_vec()),
            None => Entry::Tombstone,
        };
        state.memtable.insert(key, entry);
        if state.should_flush(self.flush_threshold_bytes) {
            state
                .flush(&self.directory)
                .map_err(|cause| Error::AutoFlushFailed {
                    cause: Box::new(cause),
                })?;
        }
        Ok(())
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

impl State {
    fn open(directory: &Path) -> Result<Self> {
        let loaded = Manifest::load(directory)?;
        let needs_manifest = loaded.is_none();
        let mut memtable = MemTable::default();
        let (manifest, wal, tables) = if let Some(manifest) = loaded {
            let tables = manifest
                .table_ids
                .iter()
                .map(|&id| Sstable::open(directory, id))
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
            memtable,
            wal,
            manifest,
            tables,
            publication_uncertain: false,
        })
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.publication_uncertain {
            return Err(Error::FlushRequiresReopen);
        }
        self.wal.ensure_writable()
    }

    fn should_flush(&self, threshold: usize) -> bool {
        !self.memtable.is_empty() && self.memtable.approximate_bytes() >= threshold
    }

    fn flush(&mut self, directory: &Path) -> Result<()> {
        self.flush_with(directory, |_| Ok(()), Manifest::publish)
    }

    fn flush_with(
        &mut self,
        directory: &Path,
        hook: impl Fn(FlushStage) -> Result<()>,
        publish_manifest: impl FnOnce(&Path, &Path) -> Result<()>,
    ) -> Result<()> {
        self.ensure_writable()?;
        if self.memtable.is_empty() {
            return Ok(());
        }

        let previous = self.manifest.generation;
        let generation = previous.checked_add(1).ok_or(Error::GenerationExhausted)?;
        let table = Sstable::create(directory, generation, &self.memtable, || {
            hook(FlushStage::ConstructingTable)
        })?;
        let wal = Wal::create(directory, generation)?;
        sync_directory(directory, "synchronize flush file names")?;

        let mut next = self.manifest.clone();
        next.generation = generation;
        next.table_ids.push(generation);
        let staged = next.stage(directory)?;
        hook(FlushStage::BeforeManifestRename)?;
        // A rename error can leave the replacement visible. Keep both generations
        // and reject later writes until the directory selection is resolved on open.
        self.publication_uncertain = true;
        publish_manifest(directory, &staged)?;
        hook(FlushStage::AfterManifestRename)?;
        sync_directory(directory, "synchronize published manifest directory")?;
        hook(FlushStage::AfterManifestSync)?;

        self.tables.push(table);
        self.wal = wal;
        self.manifest = next;
        self.memtable = MemTable::default();
        self.publication_uncertain = false;
        let _ = remove_file_if_exists(&Wal::path(directory, previous));
        Ok(())
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
                    || Sstable::numbered_id(name)
                        .is_some_and(|id| manifest.table_ids.binary_search(&id).is_err())
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
        let db = MiniDB::open(directory).unwrap();
        db.state
            .borrow_mut()
            .flush_with(
                &db.directory,
                |stage| {
                    if stage == checkpoint {
                        std::process::exit(CRASH_CODE);
                    }
                    Ok(())
                },
                Manifest::publish,
            )
            .unwrap();
        panic!("checkpoint not reached");
    }

    #[test]
    fn uncertain_manifest_publication_blocks_mutations_and_preserves_both_generations() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path();
        let db = MiniDB::open(path).unwrap();
        db.put(b"old", b"durable").unwrap();
        let error = db.state.borrow_mut().flush_with(
            &db.directory,
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

        let error = db.state.borrow_mut().flush_with(
            &db.directory,
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

        let error = db.state.borrow_mut().flush_with(
            &db.directory,
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
