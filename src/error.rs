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
    /// Reserved for compatibility with the WAL-only engine.
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
    /// Manifest contents do not describe a valid storage state.
    CorruptManifest { reason: &'static str },
    /// An immutable table contains invalid metadata or block data.
    CorruptSstable { id: u64, reason: &'static str },
    /// The next storage file ID cannot be represented.
    GenerationExhausted,
    /// Manifest replacement or synchronization failed. Reopen before writing.
    FlushRequiresReopen,
    /// A mutation is durable and visible, but its automatic flush failed.
    AutoFlushFailed { cause: Box<Error> },
    /// The background flush thread exited unexpectedly.
    FlushWorkerPanicked,
    /// The background compaction thread exited unexpectedly.
    CompactionWorkerPanicked,
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
            Self::CorruptManifest { reason } => write!(f, "corrupt manifest: {reason}"),
            Self::CorruptSstable { id, reason } => write!(f, "corrupt SSTable {id}: {reason}"),
            Self::GenerationExhausted => f.write_str("storage file ID space is exhausted"),
            Self::FlushRequiresReopen => {
                f.write_str("reopen the database after uncertain manifest publication")
            }
            Self::FlushWorkerPanicked => f.write_str("background flush worker panicked"),
            Self::CompactionWorkerPanicked => f.write_str("background compaction worker panicked"),
            Self::AutoFlushFailed { cause } => {
                write!(
                    f,
                    "mutation is durable and visible, but automatic flush failed: {cause}"
                )
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::AutoFlushFailed { cause } => Some(cause.as_ref()),
            _ => None,
        }
    }
}

/// The result of a MiniDB operation.
pub type Result<T> = std::result::Result<T, Error>;
