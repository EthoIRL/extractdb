#![warn(missing_docs)]
#![cfg_attr(not(doctest), doc = include_str!("../README.md"))]
use std::{fs, thread};
use std::error::Error;
use std::fs::File;
use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
use std::io::{Read, Write};
use std::num::ParseIntError;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bitcode::{Decode, Encode};
use chrono::Utc;
use concurrent_queue::{ConcurrentQueue, PopError, PushError};
use hashbrown::HashSet;
use rayon::iter::{ParallelIterator, IndexedParallelIterator, IntoParallelRefIterator, ParallelBridge, IntoParallelIterator};
use thiserror::Error;
use xxhash_rust::xxh3::Xxh3DefaultBuilder;

/// [`ExtractDb`] is a thread-safe, in-memory hash store supporting concurrent fetches and writes.
///
/// [`ExtractDb`] only supplies a push & fetch interface where both are ``&self``.
/// Once data is inserted it can never be removed. Persistence guaranteed.
///
/// You can think of it as a non-mutable concurrent [`VecDeque`] with unique values only.
///
/// # Examples
/// Basic single threaded insertion example
/// ```no_run
/// use std::path::PathBuf;
/// use std::sync::Arc;
/// use extractdb::{ExtractConfig, ExtractDb};
///
/// let config = ExtractConfig::default()
///     .database_directory(Some(PathBuf::from("/home/user/database_name")));
/// 
/// let db: ExtractDb<i32> = ExtractDb::new(config);
///
/// db.load_from_disk(true).unwrap();
///
/// db.push(Arc::new(100));
///
/// let item = db.fetch_next().unwrap();
///
/// db.save_to_disk().unwrap();
/// ```
///
/// # Autosaving
/// Multithreading capable, [`Arc<ExtractDb<V>>`] with background auto saving.
/// ```no_run
/// use std::path::PathBuf;
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicBool, Ordering};
/// use std::time::Duration;
/// use extractdb::{CheckpointSettings, ExtractConfig, ExtractDb};
///
/// let config = ExtractConfig::default()
///     .database_directory(Some(PathBuf::from("/home/user/database_name")));
/// 
/// let db: Arc<ExtractDb<i32>> = Arc::new(ExtractDb::new(config));
///
/// db.load_from_disk(true).unwrap();
///
/// let shutdown_flag = Arc::new(AtomicBool::new(false));
/// let mut save_settings = CheckpointSettings::new(shutdown_flag.clone());
/// save_settings.minimum_changes = 10; // Minimum 10 changes
/// save_settings.check_delay = Duration::from_secs(5); // Check every 5 seconds
///
/// // Begin background saving thread
/// ExtractDb::background_checkpoints(save_settings, db.clone());
///
/// for i in 0..30 {
///     db.push(Arc::new(i));
/// }
///
/// let item = db.fetch_next().unwrap();
///
/// // Exit background thread
/// shutdown_flag.store(true, Ordering::Relaxed);
/// ```
pub struct ExtractDb<V>
    where
        V: Eq + Hash + Clone + Send + Sync + Encode + for<'a> Decode<'a>
{
    config: ExtractConfig,
    shard_mask: u64,
    shards: Box<[Shard<V>]>,

    data_hasher: Xxh3DefaultBuilder,
    removal_store: ConcurrentQueue<Arc<V>>
}

#[repr(align(128))]
struct Shard<V>
{
    data_store: RwLock<HashSet<u64, BuildHasherDefault<IdentityHasher>>>,
    disk_queue: RwLock<Vec<Arc<V>>>,
    insertion_queue: RwLock<Vec<Arc<V>>>,
}

#[derive(Default)]
struct IdentityHasher(u64);
impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 { self.0 }
    fn write(&mut self, _: &[u8]) { unimplemented!() }
    fn write_u64(&mut self, i: u64) { self.0 = i; }
}

/// [`ExtractConfig`] Configuration structure for [`ExtractDb`]
/// 
/// `ExtractConfig` can be used to define shard count, optimistic reading behavior, draining size, and disk saving location.  
/// 
/// Use this to initialize a customized instance of [`ExtractDb`]
#[derive(Clone, Debug)]
pub struct ExtractConfig {
    shard_count: usize,
    optimistic_read: bool,
    drain_size: usize,
    database_directory: Option<PathBuf>
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

impl<V> Default for ExtractDb<V>
    where
        V: Eq + Hash + Clone + Send + Sync + Encode + for<'a> Decode<'a>
{
    fn default() -> Self {
        Self::new(ExtractConfig::default())
    }
}

#[derive(Error, Debug)]
pub enum FetchError<V>
{
    /// Failed to load shard internal data into removal store queue 
    #[error(transparent)]
    Push(#[from] PushError<V>),
    
    /// Failed to pop data from internal removal_store
    #[error(transparent)]
    Pop(#[from] PopError),
}

#[derive(Error, Debug)]
pub enum SaveError {
    /// Failed to lock data_store or disk_queue from shard 
    #[error("Failed to read lock [Shard {0}]")]
    ShardLock(usize),
    
    /// Failed to create or truncate preexisting file from shard
    #[error("Failed to create/truncate file [Shard {0}, ({1})]")]
    CreateTruncate(usize, std::io::Error),

    /// Failed to write to file from shard
    #[error("Failed to write to file [Shard {0}, ({1})]")]
    Write(usize, std::io::Error),

    /// Failed to flush to file form shard
    #[error("Failed to flush file [Shard {0}, ({1})]")]
    Flush(usize, std::io::Error),
    
    /// Database directory was not set during ExtractDb initialization.
    #[error("No database directory is set. Cannot save to disk without a valid path set!")]
    NoDirectory,
    
    /// std::io::Error occurred during saving 
    #[error(transparent)]
    Io(#[from] std::io::Error)
}

#[derive(Error, Debug)]
pub enum LoadError {
    /// Database directory was not set during ExtractDb initialization
    #[error("No database directory is set. Cannot save to disk without a valid path set!")]
    NoDirectory,
    
    /// Failed to locate the file at it's specified location
    #[error("Could not locate file entry")]
    MissingEntry,
    
    /// Database has files missing in either the store or data directories
    #[error("No files are present in the database directory.")]
    MissingFiles,

    /// Failed to open file  
    #[error("Failed to open file. Skipping ({0})")]
    Open(std::io::Error),

    /// Failed to read file 
    #[error("Failed to read file. Skipping ({0})")]
    Read(std::io::Error),
    
    /// Failed to decode file shard data
    #[error("Failed to decode shard file. Skipping ({0})")]
    Decode(bitcode::Error),
    
    /// Failed to extract filename from shard file
    #[error("Failed to get file_name. Skipping ({0})")]
    Filename(String),
    
    /// Failed to convert filename to shard number
    #[error("Failed to convert string to number. Skipping (File: {0}, Err: {1})")]
    ShardName(String, ParseIntError),
    
    /// Soft error: This can be treated as a warning rather than an error
    /// Store or data is miss-matched in save shard size. This is recoverable to a degree.
    #[error("Soft error: Shard store/data miss-match, converting to current shard_size")]
    ShardMismatch,
    
    /// File contains no data
    #[error("Missing or no data present within file. Skipping ({0})")]
    NoData(String),
    
    /// Failed to extract shard id from file name
    #[error("Failed to extract shard number from file. Skipping ({0})")]
    ShardIdExtraction(String)
}

impl<V> ExtractDb<V>
    where
        V: Eq + Hash + Clone + Send + Sync + Encode + for<'a> Decode<'a>
{
    /// Creates a new [`ExtractDb`]
    ///
    /// # Arguments
    /// `database_directory`: Allows saving of data to disk. This is **optional**!
    ///
    /// # Examples
    /// ```rust
    /// use extractdb::ExtractDb;
    /// use std::sync::Arc;
    ///
    /// // In-memory only example, set a path for save/loading.
    /// let db: ExtractDb<String> = ExtractDb::default();
    ///
    /// assert_eq!(db.push(Arc::new("Hello ExtractDb!".to_string())), true);
    /// ```
    pub fn new(mut config: ExtractConfig) -> ExtractDb<V> {
        if !config.shard_count.is_power_of_two() {
            config.shard_count = config.shard_count.next_power_of_two();
        }

        let shards: Box<[Shard<V>]> = (0..config.shard_count)
            .map(|_| Shard {
                data_store: RwLock::new(HashSet::<u64, BuildHasherDefault<IdentityHasher>>::default()),
                disk_queue: RwLock::new(Vec::new()),
                insertion_queue: RwLock::new(Vec::new())
            })
            .collect();

        ExtractDb {
            shard_mask: (config.shard_count as u64) - 1,
            config,
            shards,
            data_hasher: Xxh3DefaultBuilder::new(),
            removal_store: ConcurrentQueue::unbounded(),
        }
    }

    /// Pushes data `Arc<V>` into the internal sharded hashset.
    ///
    /// # Returns
    /// ``True``: if data has successfully inserted into a hashset
    ///
    /// ``False``: if data has already been added to a hashset, or if the internal shard is poisoned
    ///
    /// # Examples
    /// ```rust
    /// use extractdb::ExtractDb;
    /// use std::sync::Arc;
    ///
    /// let db: ExtractDb<i32> = ExtractDb::default();
    ///
    /// assert_eq!(db.push(Arc::new(100)), true);
    /// assert_eq!(db.push(Arc::new(100)), false);
    /// assert_eq!(db.internal_count(), 1);
    /// ```
    pub fn push(&self, value: Arc<V>) -> bool {
        let hash = self.data_hasher.hash_one(&value);
        let shard_index = hash & self.shard_mask;

        self.push_shard(value, shard_index as usize, hash)
    }

    fn push_shard(&self, value: Arc<V>, shard_index: usize, hash: u64) -> bool {
        if self.config.optimistic_read {
            match self.shards[shard_index].data_store.read() {
                Ok(data_shard) => {
                    if data_shard.contains(&hash) {
                        return false;
                    }
                },
                Err(_) => return false
            }
        }

        match self.shards[shard_index].data_store.write() {
            Ok(mut data_shard) => {
                if !data_shard.insert(hash) {
                    return false;
                }
            },
            Err(_) => return false
        }

        match self.shards[shard_index].disk_queue.write() {
            Ok(mut queue) => {
                queue.push(Arc::clone(&value));
            },
            Err(_) => return false
        }

        match self.shards[shard_index].insertion_queue.write() {
            Ok(mut queue) => {
                queue.push(value);
            },
            Err(_) => return false
        }

        true
    }

    /// Fetches a unique item from an internal queue
    ///
    /// This function may act as a FIFO during low contention scenarios. Order is not guaranteed.
    ///
    /// # Returns
    /// `Arc<V>` A reference of the internal item
    ///
    /// # Errors
    /// [`Box<dyn Error + '_>`] may return if queue is empty or if loading has a critical error
    ///
    /// # Examples
    /// ```rust
    /// use extractdb::ExtractDb;
    /// use std::sync::Arc;
    ///
    /// let db: ExtractDb<String> = ExtractDb::default();
    ///
    /// assert_eq!(db.push(Arc::new("hello world".to_string())), true);
    /// assert_eq!(db.fetch_next().unwrap(), Arc::new("hello world".to_string()));
    /// assert_eq!(db.internal_count(), 1);
    /// assert_eq!(db.fetch_count(), 0);
    /// ```
    pub fn fetch_next(&self) -> Result<Arc<V>, FetchError<Arc<V>>> {
        if self.removal_store.is_empty() {
            self.load_shards_to_accessible()?;
        }

        self.removal_store.pop()
            .map_err(|err| err.into())
    }

    /// Get the current count of the `fetch_next` mutable queue
    ///
    /// # Returns
    /// [`usize`] a total of all items loaded into the temporary fetch vector
    ///
    /// # Examples
    /// ```rust
    /// use extractdb::ExtractDb;
    /// use std::sync::Arc;
    ///
    /// let db: ExtractDb<u8> = ExtractDb::default();
    ///
    /// assert_eq!(db.push(Arc::new(20)), true);
    /// assert_eq!(db.fetch_count(), 0); // No data is currently loaded
    /// assert_eq!(db.fetch_next().unwrap(), Arc::new(20)); // Causes a load for the non-mutable vector
    /// assert_ne!(db.fetch_count(), 1);
    /// ```
    pub fn fetch_count(&self) -> usize {
        self.removal_store.len()
    }

    /// Get the internal count of items in all shards. This represents the total amount of items in the database at any time.
    ///
    /// This function is impacted by writes and may be slowed.
    ///
    /// # Returns
    /// [`usize`] a total of all items in the entire sharded database.
    ///
    /// # Examples
    /// ```rust
    /// use extractdb::ExtractDb;
    /// use std::sync::Arc;
    ///
    /// let db: ExtractDb<u8> = ExtractDb::default();
    ///
    /// for i in 0..128 {
    ///     assert_eq!(db.push(Arc::new(i)), true);
    /// }
    /// assert_eq!(db.internal_count(), 128);
    /// ```
    pub fn internal_count(&self) -> usize {
        let mut global_shard_size = 0;
        for shard in &self.shards {
            if let Ok(data_shard) = shard.data_store.read() {
                global_shard_size += data_shard.len();
            }
        }

        global_shard_size
    }

    fn load_shards_to_accessible(&self) -> Result<(), PushError<Arc<V>>>  {
        for shard in &self.shards {
            let shard_drain: Vec<Arc<V>> = match shard.insertion_queue.write() {
                Ok(mut write_queue ) => {
                    if write_queue.is_empty() {
                        continue;
                    }
                    
                    let drain_amount = usize::min(write_queue.len(), self.config.drain_size);
                    write_queue.drain(..drain_amount).collect()
                },
                Err(_) => continue
            };

            for item in shard_drain {
                self.removal_store.push(item)?
            }
        }

        Ok(())
    }

    /// Saves all internal shard data into a serialized database directory.
    ///
    /// # Layout
    /// - Store:
    ///     Holds the hash's for all known items within the database. This is crucial for maintaining item uniqueness.
    ///     This is overwritten every save.
    /// - Data:
    ///     `Arc<V>` data is held within this subdirectory.
    ///     This is appended to disk every save (0-XXXXXXXXXX..(SHARD_COUNT-1)-XXXXXXXXXX). Files are formatted with UTC Timestamp seconds to avoid collision.
    ///
    /// # Errors
    /// [`Box<dyn Error>`] may return if database directory is not set or if saving fails.
    pub fn save_to_disk(&self) -> Result<(), SaveError> {
        let Some(database_directory) = &self.config.database_directory else {
            return Err(SaveError::NoDirectory)
        };

        let store_directory = database_directory.join("store");
        let data_directory = database_directory.join("data");

        if !database_directory.exists() {
            fs::create_dir_all(database_directory)?;
        }

        if !store_directory.exists() {
            fs::create_dir_all(&store_directory)?;
        }

        if !data_directory.exists() {
            fs::create_dir_all(&data_directory)?;
        }

        let store_results = self.shards
            .par_iter()
            .enumerate()
            .try_for_each(|(id, shard)| -> Result<(), SaveError> {
                let store_shard = shard.data_store
                    .read()
                    .map_err(|_| SaveError::ShardLock(id))?;

                let internal_data: Vec<u64> = store_shard.clone().into_iter().collect();
                let encoded_data = bitcode::encode(&internal_data);

                let file_shard_path = &store_directory.join(format!("{id}"));

                let mut file_shard = File::create(file_shard_path)
                    .map_err(|err| SaveError::CreateTruncate(id, err))?;

                file_shard
                    .write_all(&encoded_data)
                    .map_err(|err| SaveError::Write(id, err))?;

                file_shard
                    .flush()
                    .map_err(|err| SaveError::Flush(id, err))?;

                Ok(())
            });

        let disk_results = self.shards
            .par_iter()
            .enumerate()
            .try_for_each(|(id, shard)| -> Result<(), SaveError> {
                let mut data_shard = shard.disk_queue
                    .write()
                    .map_err(|_| SaveError::ShardLock(id))?;
                
                let internal_data: Vec<Arc<V>> = data_shard
                    .drain(..)
                    .collect();

                if internal_data.is_empty() {
                    return Ok(());
                }
                
                let encoded_data = bitcode::encode(&internal_data);

                let file_shard_path = &data_directory.join(format!("{id}-{}", Utc::now().timestamp()));

                let mut file_shard = File::create(file_shard_path)
                    .map_err(|err| SaveError::CreateTruncate(id, err))?;

                file_shard
                    .write_all(&encoded_data)
                    .map_err(|err| SaveError::Write(id, err))?;

                file_shard
                    .flush()
                    .map_err(|err| SaveError::Flush(id, err))?;

                Ok(())
            });

        store_results?;
        disk_results?;

        Ok(())
    }

    /// Loads all shard-files back into internal memory
    ///
    /// During a failure/corruption event all non-corrupted data will be loaded into memory before an error is omitted out.
    ///
    /// # Arguments
    /// `re_enqueue`: Loads all data back into fetch queue.
    ///
    /// # Errors
    /// [`Box<dyn Error + Send + Sync>`] may return if any form of corruption occurs, or if a shard size changes.
    /// **Missing any store files will be considered fully corrupted. While data files will be recovered to the best of its ability without an error.**
    pub fn load_from_disk(&self, re_enqueue: bool) -> Result<(), LoadError> {
        let Some(database_directory) = &self.config.database_directory else {
            return Err(LoadError::NoDirectory)
        };

        let store_directory = database_directory.join("store");
        let data_directory = database_directory.join("data");

        if !database_directory.exists() {
            return Ok(());
        }

        if !store_directory.exists() {
            return Ok(())
        }

        if !data_directory.exists() {
            return Ok(())
        }
        
        let Ok(store_files) = fs::read_dir(&store_directory) else {
            return Err(LoadError::MissingFiles);
        };

        let Ok(data_files) = fs::read_dir(&data_directory) else {
            return Err(LoadError::MissingFiles);
        };

        let shard_mismatch: bool = {
            let store_mismatch = match fs::read_dir(store_directory) {
                Ok(files) => files.count() != self.config.shard_count,
                Err(_) => true
            };

            let data_mismatch = match fs::read_dir(data_directory) {
                Ok(mut files) => {
                    files.any(|file_result| {
                        if let Ok(file) = file_result {
                            let file_name = match file.file_name().to_str() {
                                Some(data) => data.to_string(),
                                None => return true
                            };

                            let shard_file_name = match file_name
                                    .split_once("-")
                                    .map(|(before, _)| before) {
                                Some(before) => before,
                                None => return true
                            };

                            let shard_id = match usize::from_str(&shard_file_name) {
                                Ok(id) => id,
                                Err(_) => return true
                            };

                            if shard_id >= self.config.shard_count {
                                return true
                            }
                            
                            return false;
                        }
                        
                        true
                    })
                }
                Err(_) => true
            };

            store_mismatch || data_mismatch
        };

        let store_load_results: Vec<Result<(), LoadError>> = store_files
            .par_bridge()
            .map(|potential_file| -> Result<(), LoadError> {
                let file_entry = potential_file
                    .map_err(|_| LoadError::MissingEntry)?;

                let mut file = File::open(file_entry.path())
                    .map_err(|err| LoadError::Open(err))?;
                let mut file_data: Vec<u8> = Vec::new();

                let size = file.read_to_end(&mut file_data)
                    .map_err(|err| LoadError::Read(err))?;

                if size == 0 {
                    return Ok(());
                }

                let decoded_shard_data: Vec<u64> = bitcode::decode(&file_data)
                    .map_err(|err| LoadError::Decode(err))?;

                let file_name = match file_entry.file_name().to_str() {
                    Some(data) => data.to_string(),
                    None => {
                        return Err(LoadError::Filename(file_entry.path().display().to_string()));
                    }
                };

                let shard_id = usize::from_str(&file_name)
                    .map_err(|err| LoadError::ShardName(file_entry.path().display().to_string(), err))?;

                if shard_mismatch {
                    decoded_shard_data.into_par_iter().for_each(|item| {
                        let shard_index = item % self.config.shard_count as u64;

                        if let Ok(mut data_shard) = self.shards[shard_index as usize].data_store.write() {
                            if !data_shard.insert(item) {
                                return;
                            }
                        }
                    });

                    return Err(LoadError::ShardMismatch);
                }

                if let Ok(mut data_shard) = self.shards[shard_id].data_store.write() {
                    for decoded_datum in decoded_shard_data { 
                        data_shard.insert(decoded_datum);
                    }
                }

                Ok(())
            }).collect();

        if re_enqueue {
            let data_load_results: Vec<Result<(), LoadError>> = data_files
                .par_bridge()
                .map(|potential_file| -> Result<(), LoadError> {
                    let file_entry = potential_file
                        .map_err(|_| LoadError::MissingEntry)?;

                    let mut file = File::open(file_entry.path())
                        .map_err(|err| LoadError::Open(err))?;
                    let mut file_data: Vec<u8> = Vec::new();

                    let size = file.read_to_end(&mut file_data)
                        .map_err(|err| LoadError::Read(err))?;

                    if size == 0 {
                        return Err(LoadError::NoData(file_entry.path().display().to_string()));
                    }

                    let decoded_shard_data: Vec<V> = bitcode::decode(&file_data)
                        .map_err(|err| LoadError::Decode(err))?;

                    let file_name = match file_entry.file_name().to_str() {
                        Some(data) => data.to_string(),
                        None => {
                            return Err(LoadError::Filename(file_entry.path().display().to_string()));
                        }
                    };
                    
                    let shard_file_name = file_name.split_once("-")
                        .map(|(before, _)| before)
                        .ok_or_else(|| LoadError::ShardIdExtraction(file_entry.path().display().to_string()))?;

                    let shard_id = usize::from_str(&shard_file_name)
                        .map_err(|err| LoadError::ShardName(file_entry.path().display().to_string(), err))?;

                    if shard_mismatch {
                        for decoded_datum in decoded_shard_data {
                            let datum = Arc::new(decoded_datum);
                    
                            let hash = self.data_hasher.hash_one(&datum);
                            let new_shard_index = hash & self.shard_mask;
                    
                            if let Ok(mut queue) = self.shards[new_shard_index as usize].insertion_queue.write() {
                                queue.push(datum);
                            }
                        }
                    
                        return Err(LoadError::ShardMismatch);
                    }

                    for decoded_datum in decoded_shard_data {
                        let datum = Arc::new(decoded_datum);

                        if let Ok(mut queue) = self.shards[shard_id].insertion_queue.write() {
                            queue.push(datum);
                        }
                    }
                    
                    Ok(())
                }).collect();

            for load_result in data_load_results {
                load_result?;
            }
        }

        for load_result in store_load_results {
            load_result?;
        }

        Ok(())
    }

    /// Spawns a background thread to provide periodic save checkpoints onto disk.
    /// Only performs a save if a certain threshold of changes has occurred during the save interval.
    /// See `CheckpointSettings`
    ///
    /// This function is meant for long-running applications/multithreaded instances.
    ///
    /// Use `check_delay` within [`CheckpointSettings`] to determine the frequency in which change counts occur.
    ///
    /// Use `minimum_changes` within [`CheckpointSettings`] to determine the minimum amount of items inserted before a safe event occurs.
    ///
    /// Use `shutdown_flag` within [`CheckpointSettings`] to remotely shut down this thread in a safe manner. `False` = Running, `True` = Please stop
    ///
    /// # Different behavior
    /// You do not necessarily need to use this function for auto-saving. All methods used are publicly accessible and easily re-implementable. See source.
    ///
    /// # Parameters
    ///
    /// `settings`: [`CheckpointSettings`] determines the check rate and minimum change for disk saving.
    ///
    /// `db`: [`Arc<ExtractDb<V>>`] instance of [`ExtractDb`] in a shared instance
    ///
    /// # Examples
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicBool, Ordering};
    /// use extractdb::{CheckpointSettings, ExtractDb};
    ///
    /// let db: Arc<ExtractDb<u8>> = Arc::new(ExtractDb::default());
    ///
    /// let shutdown_flag = Arc::new(AtomicBool::new(false));
    /// let mut save_settings = CheckpointSettings::new(shutdown_flag.clone());
    /// save_settings.minimum_changes = 1000;
    ///
    /// // Will now check for 1000 minimum changes every 30seconds (default).
    /// ExtractDb::background_checkpoints(save_settings, db.clone());
    ///
    /// db.push(Arc::new(127));
    ///
    /// // Gracefully shutdown a background thread
    /// shutdown_flag.store(true, Ordering::Relaxed);
    /// ```
    pub fn background_checkpoints(settings: CheckpointSettings, db: Arc<ExtractDb<V>>)
    where 
        V: 'static
    {
        thread::spawn(move || {
            let mut last_checkpoint_count: usize = 0;
            while !settings.shutdown_flag.load(Ordering::Relaxed) {
                thread::sleep(settings.check_delay);

                let current_change = db.internal_count();
                let changes_since_last = current_change - last_checkpoint_count;

                if changes_since_last >= settings.minimum_changes {
                    match db.save_to_disk() {
                        Ok(()) => {
                            last_checkpoint_count = current_change;
                        },
                        Err(err) => {
                            eprintln!("Database checkpoint failed. ({err})");
                        }
                    }
                }
            }

            let changes_since_last =  db.internal_count() - last_checkpoint_count;

            if changes_since_last > 0 {
                // Force a save no matter what, ensures all data is written.
                if let Err(err) = db.save_to_disk() {
                    eprintln!("Database last-minute checkpoint failed. ({err})");
                }
            }
        });
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