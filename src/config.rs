use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

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
    
    /// Returns the current shard_count
    pub fn get_shard_count(&self) -> usize {
        self.shard_count
    }
    
    /// Returns the current optimistic_read state
    pub fn get_optimistic_read(&self) -> bool {
        self.optimistic_read
    }
    
    /// Returns the current drain_size
    pub fn get_drain_size(&self) -> usize {
        self.drain_size
    }
    
    /// Returns the current database_directory optional
    pub fn get_database_directory(&self) -> &Option<PathBuf> {
        &self.database_directory
    }
}

/// Configuration settings for the provided [`ExtractDb::background_checkpoints`].
///
/// # Examples
/// ```
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicBool, Ordering};
/// use std::time::Duration;
/// use extractdb::{CheckpointSettings, ExtractDb};
///
/// let shutdown_flag = Arc::new(AtomicBool::new(false));
/// let mut save_settings = CheckpointSettings::new(shutdown_flag.clone());
///
/// save_settings.minimum_changes = 30;
///
/// // Checks every 5 seconds for >=30 changes.
/// save_settings.check_delay = Duration::from_secs(5);
///
/// // Gracefully shutdown a background thread
/// shutdown_flag.store(true, Ordering::Relaxed);
/// ```
#[derive(Clone)]
pub struct CheckpointSettings {
    /// Interval at which the `internal_count` is checked
    ///
    /// e.g. Check the number of pushes every X seconds.
    pub check_delay: Duration,

    /// Minimum number of changes from the last disk write needed for a new disk write.
    ///
    /// e.g. Write to disk after 200 push insertions.
    pub minimum_changes: usize,

    /// A flag to safely shut down the internal watcher thread. Use this to gracefully shutdown & save state
    pub shutdown_flag: Arc<AtomicBool>
}

impl CheckpointSettings {
    /// Generic default settings for auto-saving in [`ExtractDb::background_checkpoints`].
    pub fn new(shutdown_flag: Arc<AtomicBool>) -> Self {
        CheckpointSettings {
            check_delay: Duration::from_secs(30),
            minimum_changes: 1000,
            shutdown_flag
        }
    }
}