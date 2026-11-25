/// Per-database input limits. The default leaves lengths unrestricted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Options {
    /// Maximum key length in bytes for reads and mutations. `None` is unlimited.
    pub max_key_bytes: Option<usize>,
    /// Maximum value length in bytes for writes. `None` is unlimited.
    pub max_value_bytes: Option<usize>,
}
