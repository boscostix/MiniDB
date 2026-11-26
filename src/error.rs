use std::{fmt, io};

/// An error returned by a MiniDB operation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// Keys must contain at least one byte.
    EmptyKey,
    /// A key exceeds the configured length limit in bytes.
    KeyTooLarge { len: usize, max: usize },
    /// A value exceeds the configured length limit in bytes.
    ValueTooLarge { len: usize, max: usize },
    /// Persistent table storage is unavailable for a flush.
    PersistenceUnavailable,
    /// Another handle has already locked the database directory.
    DatabaseLocked,
    /// An operation failed at the filesystem boundary.
    Io {
        operation: &'static str,
        kind: io::ErrorKind,
        message: String,
    },
    /// The write-ahead log contains invalid data at the given byte offset.
    CorruptWal { offset: u64, reason: &'static str },
    /// The database was created with an unsupported WAL version.
    UnsupportedWalVersion { version: u16 },
    /// A previous WAL append or synchronization failed. Reopen before writing.
    WalWriteFailed,
    /// Memory could not be allocated for a WAL record during recovery.
    WalAllocationFailed { bytes: usize },
}

impl Error {
    pub(crate) fn io(operation: &'static str, error: io::Error) -> Self {
        Self::Io {
            operation,
            kind: error.kind(),
            message: error.to_string(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyKey => f.write_str("keys must contain at least one byte"),
            Self::KeyTooLarge { len, max } => {
                write!(f, "key length of {len} bytes exceeds the {max}-byte limit")
            }
            Self::ValueTooLarge { len, max } => {
                write!(
                    f,
                    "value length of {len} bytes exceeds the {max}-byte limit"
                )
            }
            Self::PersistenceUnavailable => f.write_str("persistent table storage is unavailable"),
            Self::DatabaseLocked => f.write_str("database directory is already locked"),
            Self::Io {
                operation, message, ..
            } => write!(f, "{operation}: {message}"),
            Self::CorruptWal { offset, reason } => {
                write!(f, "corrupt WAL at byte {offset}: {reason}")
            }
            Self::UnsupportedWalVersion { version } => {
                write!(f, "unsupported WAL version {version}")
            }
            Self::WalWriteFailed => f.write_str("a WAL write failed; reopen the database"),
            Self::WalAllocationFailed { bytes } => {
                write!(f, "could not allocate {bytes} bytes for WAL recovery")
            }
        }
    }
}

impl std::error::Error for Error {}

/// The result of a MiniDB operation.
pub type Result<T> = std::result::Result<T, Error>;
