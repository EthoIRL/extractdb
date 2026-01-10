use std::path::PathBuf;

/// [`ExtractConfig`] Configuration structure for [`ExtractDb`]
///
/// `ExtractConfig` can be used to define shard count, optimistic reading behavior, draining size, and disk saving location.  
///
/// Use this to initialize a customized instance of [`ExtractDb`]
#[derive(Clone, Debug)]
pub struct ExtractConfig {
    pub(crate) shard_count: usize,
    pub(crate) optimistic_read: bool,
    pub(crate) drain_size: usize,
    pub(crate) database_directory: Option<PathBuf>
}

impl Default for ExtractConfig {
    fn default() -> Self {
        Self {
            shard_count: 16,
            optimistic_read: false,
            drain_size: 100_000,
            database_directory: None
        }
    }
}

impl ExtractConfig {
    /// Internal sharding count; Use similar amounts to logical cores for best performance.
    /// Must be a power of 2 (2, 4, 8, 16, 32, ...), if not set correctly auto-rounds to nearest power of 2.
    pub fn shard_count(mut self, count: usize) -> Self {
        self.shard_count = count;
        self
    }

    /// Performs an optimistic hash check (Fail-fast) returning early if data is already present in the database.
    /// Enable only if environment is prone to high input similarity chances.
    pub fn optimistic_read(mut self, state: bool) -> Self {
        self.optimistic_read = state;
        self
    }

    /// Amount of items drained into the fetch `buffer`, lowers runtime memory duplication at the expense of more intermittent reads.
    pub fn drain_size(mut self, size: usize) -> Self {
        self.drain_size = size;
        self
    }

    /// Filesystem location pointer, allows saving of data to disk. This is **optional!**
    pub fn database_directory(mut self, directory: Option<PathBuf>) -> Self {
        self.database_directory = directory;
        self
    }
}
