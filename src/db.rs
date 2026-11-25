use std::{cell::RefCell, path::Path};

use crate::{
    Error, Options, Result,
    memtable::{Entry, MemTable},
};

/// A single in-memory database. Each handle owns its data and is intended for
/// use by one thread at a time.
pub struct MiniDB {
    options: Options,
    memtable: RefCell<MemTable>,
}

impl MiniDB {
    /// Creates an empty database with the default options.
    ///
    /// The in-memory engine accepts a path for API compatibility. It performs
    /// no filesystem access, and every call creates independent state.
    ///
    /// ```
    /// # fn main() -> minidb::Result<()> {
    /// let db = minidb::MiniDB::open("./data")?;
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

    /// Creates an empty database with the supplied input limits.
    ///
    /// Like [`Self::open`], this method performs no filesystem access.
    pub fn open_with_options(_path: impl AsRef<Path>, options: Options) -> Result<Self> {
        Ok(Self {
            options,
            memtable: RefCell::new(MemTable::default()),
        })
    }

    /// Inserts or replaces a value. Empty values are valid.
    ///
    /// Returns an error for an empty key or an input exceeding its size limit.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.validate_key(key)?;
        if let Some(max) = self.options.max_value_bytes
            && value.len() > max
        {
            return Err(Error::ValueTooLarge {
                len: value.len(),
                max,
            });
        }
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
    /// Returns an error for an empty key or one exceeding the key size limit.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.validate_key(key)?;
        self.memtable.borrow_mut().insert(key, Entry::Tombstone);
        Ok(())
    }

    /// Returns [`Error::PersistenceUnavailable`] in the in-memory engine.
    pub fn flush(&self) -> Result<()> {
        Err(Error::PersistenceUnavailable)
    }

    /// Consumes the handle and releases its in-memory data.
    pub fn close(self) -> Result<()> {
        Ok(())
    }

    fn validate_key(&self, key: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        if let Some(max) = self.options.max_key_bytes
            && key.len() > max
        {
            return Err(Error::KeyTooLarge {
                len: key.len(),
                max,
            });
        }
        Ok(())
    }
}
