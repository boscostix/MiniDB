/// Optional per-handle input limits, in addition to WAL format limits.
/// These limits apply to new operations and do not alter records during replay.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Options {
    /// Maximum key length in bytes for reads and mutations.
    /// `None` uses the WAL key length limit of `u32::MAX` bytes.
    pub max_key_bytes: Option<usize>,
    /// Maximum value length in bytes for writes.
    /// `None` uses the WAL value length limit of `u32::MAX` bytes.
    pub max_value_bytes: Option<usize>,
}
