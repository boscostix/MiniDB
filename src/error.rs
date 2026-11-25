use std::fmt;

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
    /// The in-memory engine has no persistent storage to flush to.
    PersistenceUnavailable,
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
            Self::PersistenceUnavailable => f.write_str("persistent storage is unavailable"),
        }
    }
}

impl std::error::Error for Error {}

/// The result of a MiniDB operation.
pub type Result<T> = std::result::Result<T, Error>;
