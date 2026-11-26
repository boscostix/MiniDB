use std::{
    cell::RefCell,
    fs::{self, File, OpenOptions},
    io,
    path::Path,
};

use crate::{
    Error, Options, Result,
    memtable::{Entry, MemTable},
    wal::Wal,
};

/// An exclusively opened database with durable writes and an in-memory index.
/// A handle is intended for use by one thread at a time.
pub struct MiniDB {
    options: Options,
    memtable: RefCell<MemTable>,
    wal: RefCell<Wal>,
    // Keep the directory lock until after the WAL handle closes.
    _lock: File,
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
    /// using the WAL format limits.
    pub fn open_with_options(path: impl AsRef<Path>, options: Options) -> Result<Self> {
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
        let mut memtable = MemTable::default();
        let wal = Wal::open(&path, &mut memtable)?;

        // An earlier interrupted attempt may have created entries that are visible
        // but still need synchronization. Sync from the WAL directory to the root.
        for directory in path.ancestors() {
            File::open(directory)
                .and_then(|file| file.sync_all())
                .map_err(|error| Error::io("synchronize database directory ancestry", error))?;
        }

        Ok(Self {
            options,
            memtable: RefCell::new(memtable),
            wal: RefCell::new(wal),
            _lock: lock,
        })
    }

    /// Inserts or replaces a value. Empty values are valid.
    ///
    /// Returns an error for an invalid input or a failed WAL append or sync.
    /// A WAL I/O failure requires reopening the database before more writes.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.validate_key(key)?;
        let max = length_limit(self.options.max_value_bytes);
        if value.len() > max {
            return Err(Error::ValueTooLarge {
                len: value.len(),
                max,
            });
        }
        self.wal.borrow_mut().append(key, Some(value))?;
        self.memtable
            .borrow_mut()
            .insert(key, Entry::Value(value.to_vec()));
        Ok(())
    }

    /// Returns an owned copy of a value, or `None` for an absent or deleted key.
    ///
    /// Returns an error for an empty key or one exceeding the key size limit.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.validate_key(key)?;
        match self.memtable.borrow().get(key) {
            Some(Entry::Value(value)) => Ok(Some(value.clone())),
            Some(Entry::Tombstone) | None => Ok(None),
        }
    }

    /// Records a deletion, including when the key was already absent.
    ///
    /// Returns an error for an invalid key or a failed WAL append or sync.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.validate_key(key)?;
        self.wal.borrow_mut().append(key, None)?;
        self.memtable.borrow_mut().insert(key, Entry::Tombstone);
        Ok(())
    }

    /// Returns [`Error::PersistenceUnavailable`] until persistent tables are supported.
    pub fn flush(&self) -> Result<()> {
        Err(Error::PersistenceUnavailable)
    }

    /// Consumes the handle and releases the directory lock.
    /// Successful mutations have already synchronized the WAL.
    pub fn close(self) -> Result<()> {
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
