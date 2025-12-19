#![warn(missing_docs)]
#![cfg_attr(not(doctest), doc = include_str!("../README.md"))]
use std::{fs, thread};
use std::collections::VecDeque;
use std::error::Error;
use std::fs::File;
use std::hash::{BuildHasher, Hash};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bitcode::{Decode, Encode};
use chrono::Utc;
use concurrent_queue::ConcurrentQueue;
use hashbrown::HashSet;
use rayon::iter::{ParallelIterator, IndexedParallelIterator, IntoParallelRefIterator, ParallelBridge, IntoParallelIterator};
use xxhash_rust::xxh3::Xxh3DefaultBuilder;

#[cfg(test)]
mod tests;

const SHARD_COUNT: usize = 16;

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
/// use extractdb::ExtractDb;
///
/// let db: ExtractDb<i32> = ExtractDb::new(Some(PathBuf::from("/home/user/database_name")));
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
/// use extractdb::{CheckpointSettings, ExtractDb};
///
/// let db: Arc<ExtractDb<i32>> = Arc::new(ExtractDb::new(Some(PathBuf::from("/home/user/database_name"))));
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
    shard_count: usize,

    data_store: Vec<RwLock<HashSet<u64>>>,
    disk_queue: Vec<RwLock<VecDeque<Arc<V>>>>,

    data_hasher: Xxh3DefaultBuilder,

    insertion_queue: Vec<RwLock<VecDeque<Arc<V>>>>,
    removal_store: ConcurrentQueue<Arc<V>>,

    db_directory: Option<PathBuf>,
}

impl<V> Default for ExtractDb<V>
    where
        V: Eq + Hash + Clone + Send + Sync + Encode + for<'a> Decode<'a>
{
    fn default() -> Self {
        Self::new(None)
    }
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
    /// let db: ExtractDb<String> = ExtractDb::new(None);
    ///
    /// assert_eq!(db.push(Arc::new("Hello ExtractDb!".to_string())), true);
    /// ```
    pub fn new(database_directory: Option<PathBuf>) -> ExtractDb<V> {
        Self::new_with_shards(SHARD_COUNT, database_directory)
    }

    /// Creates a new [`ExtractDb`] with a specific internal sharding amount
    ///
    /// # Arguments
    /// `shard_count`: Shards to be used internally. Think more shards = more concurrency, vice versa.
    ///
    /// `database_directory`: Allows saving of data to disk. This is **optional**!
    ///
    /// # Examples
    /// ```rust
    /// use extractdb::ExtractDb;
    /// use std::sync::Arc;
    ///
    /// let db: ExtractDb<String> = ExtractDb::new_with_shards(32, None);
    ///
    /// assert_eq!(db.push(Arc::new("Hello ExtractDb with custom shards!".to_string())), true);
    /// ```
    pub fn new_with_shards(shard_count: usize, database_directory: Option<PathBuf>) -> ExtractDb<V> {
        let data_store: Vec<RwLock<HashSet<u64>>> = (0..shard_count)
            .map(|_| RwLock::new(HashSet::new()))
            .collect();

        let disk_queue: Vec<RwLock<VecDeque<Arc<V>>>> = (0..shard_count)
            .map(|_| RwLock::new(VecDeque::new()))
            .collect();
        
        let insertion_queues: Vec<RwLock<VecDeque<Arc<V>>>> = (0..shard_count)
            .map(|_| RwLock::new(VecDeque::new()))
            .collect();

        ExtractDb {
            shard_count,
            data_store,
            disk_queue,
            data_hasher: Xxh3DefaultBuilder::new(),
            insertion_queue: insertion_queues,
            removal_store: ConcurrentQueue::unbounded(),
            db_directory: database_directory,
        }
    }

    /// Pushes data `V` into the internal sharded hashset.
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
    /// let db: ExtractDb<i32> = ExtractDb::new(None);
    ///
    /// assert_eq!(db.push(Arc::new(100)), true);
    /// assert_eq!(db.push(Arc::new(100)), false);
    /// assert_eq!(db.internal_count(), 1);
    /// ```
    pub fn push(&self, value: Arc<V>) -> bool {
        let hash = self.data_hasher.hash_one(&value);
        let shard_index = hash % self.shard_count as u64;

        self.push_shard(value, shard_index as usize, hash)
    }

    fn push_shard(&self, value: Arc<V>, shard_index: usize, hash: u64) -> bool {
        if let Ok(mut data_shard) = self.data_store[shard_index].write() {
            if !data_shard.insert(hash) {
                return false;
            }
        }

        match self.disk_queue[shard_index].write() {
            Ok(mut queue) => {
                queue.push_back(Arc::clone(&value));
            },
            Err(_) => return false
        }

        match self.insertion_queue[shard_index].write() {
            Ok(mut queue) => {
                queue.push_back(value);
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
    /// `V` A reference of the internal item
    ///
    /// # Errors
    /// [`Box<dyn Error + '_>`] may return if queue is empty or if loading has a critical error
    ///
    /// # Examples
    /// ```rust
    /// use extractdb::ExtractDb;
    /// use std::sync::Arc;
    ///
    /// let db: ExtractDb<String> = ExtractDb::new(None);
    ///
    /// assert_eq!(db.push(Arc::new("hello world".to_string())), true);
    /// assert_eq!(db.fetch_next().unwrap(), Arc::new("hello world".to_string()));
    /// assert_eq!(db.internal_count(), 1);
    /// assert_eq!(db.fetch_count(), 0);
    /// ```
    pub fn fetch_next(&self) -> Result<Arc<V>, Box<dyn Error + '_>> {
        if self.removal_store.is_empty() {
            self.load_shards_to_accessible()?;
        }

        match self.removal_store.pop() {
            Ok(value) => Ok(value),
            Err(_) => Err("Failed to pop data from removal_store".into())
        }
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
    /// let db: ExtractDb<u8> = ExtractDb::new(None);
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
    /// let db: ExtractDb<u8> = ExtractDb::new(None);
    ///
    /// for i in 0..128 {
    ///     assert_eq!(db.push(Arc::new(i)), true);
    /// }
    /// assert_eq!(db.internal_count(), 128);
    /// ```
    pub fn internal_count(&self) -> usize {
        let mut global_shard_size = 0;
        for data_store_shard in &*self.data_store {
            if let Ok(data_shard) = data_store_shard.read() {
                global_shard_size += data_shard.len();
            }
        }

        global_shard_size
    }

    fn load_shards_to_accessible(&self) -> Result<(), Box<dyn Error + '_>>  {
        for locked_queue in &self.insertion_queue {
            if let Ok(mut write_queue) = locked_queue.write() {
                if write_queue.is_empty() {
                    continue;
                }

                while let Some(item) = write_queue.pop_front() {
                    if self.removal_store.push(item).is_err() {
                        return Err("Failed to load sharded data into removal_store queue".into());
                    }
                }
            }
        }

        Ok(())
    }

    /// Saves all internal shard data into a serialized database directory.
    ///
    /// This method of saving is based off a naive checkpoint based system.
    /// All data is overwritten during every save.
    ///
    /// # Errors
    /// [`Box<dyn Error>`] may return if database directory is not set or if creating fails.
    pub fn save_to_disk(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let Some(database_directory) = &self.db_directory else {
            return Err("No database directory is set. Cannot save to disk without a valid path set!".into())
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

        let store_results = self.data_store
            .par_iter()
            .enumerate()
            .try_for_each(|(id, shard)| -> Result<(), Box<dyn Error + Send + Sync>> {
                let store_shard = shard
                    .read()
                    .map_err(|_| format!("Shard ({id}) failed to read lock"))?;

                let internal_data: Vec<u64> = store_shard.clone().into_iter().collect();
                let encoded_data = bitcode::encode(&internal_data);

                let file_shard_path = &store_directory.join(format!("{id}"));

                let mut file_shard = File::create(file_shard_path)
                    .map_err(|err| format!("Failed to create/truncate file for Shard {id}, ({err})"))?;

                file_shard
                    .write_all(&encoded_data)
                    .map_err(|err| format!("Failed to write to File Shard {id}, ({err})"))?;

                file_shard
                    .flush()
                    .map_err(|err| format!("Failed to flush File Shard {id}, ({err})"))?;

                Ok(())
            });

        if store_results.is_err() {
            return Err(format!("Unable to save hash data_store to disk. (Error: {:#?})", store_results.err()).into())
        }

        let disk_results = self.disk_queue
            .par_iter()
            .enumerate()
            .try_for_each(|(id, shard)| -> Result<(), Box<dyn Error + Send + Sync>> {
                let mut data_shard = shard
                    .write()
                    .map_err(|_| format!("Data shard ({id}) failed to read lock"))?;
                
                let internal_data: Vec<Arc<V>> = data_shard
                    .drain(..)
                    .collect();

                if  internal_data.is_empty() {
                    return Ok(());
                }
                
                let encoded_data = bitcode::encode(&internal_data);

                let file_shard_path = &data_directory.join(format!("{id}-{}", Utc::now().timestamp()));

                let mut file_shard = File::create(file_shard_path)
                    .map_err(|err| format!("Failed to create/truncate file for Shard {id}, ({err})"))?;

                file_shard
                    .write_all(&encoded_data)
                    .map_err(|err| format!("Failed to write to File Shard {id}, ({err})"))?;

                file_shard
                    .flush()
                    .map_err(|err| format!("Failed to flush File Shard {id}, ({err})"))?;

                Ok(())
            });

        if disk_results.is_err() {
            return Err(format!("Unable to save data_queue to disk. (Error: {:#?})", disk_results.err()).into())
        }

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
    pub fn load_from_disk(&self, re_enqueue: bool) -> Result<(), Box<dyn Error + Send + Sync>> {
        let Some(database_directory) = &self.db_directory else {
            return Err("No database directory is set. Cannot load from disk without a valid path set!".into())
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
            return Err("No files present in database store directory.".into())
        };

        let Ok(data_files) = fs::read_dir(&data_directory) else {
            return Err("No files present in database data directory.".into())
        };

        let shard_mismatch: bool = {
            let store_mismatch = match fs::read_dir(store_directory) {
                Ok(files) => files.count() != self.shard_count,
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

                            if shard_id >= self.shard_count {
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

        let store_load_results: Vec<Result<(), Box<dyn Error + Send + Sync>>> = store_files
            .par_bridge()
            .map(|potential_file| -> Result<(), Box<dyn Error + Send + Sync>> {
                let file_entry = potential_file
                    .map_err(|_| "No file found in dir_entry")?;

                let mut file = File::open(file_entry.path())
                    .map_err(|err| format!("Failed to open file. Skipping (Err: {err})"))?;
                let mut file_data: Vec<u8> = Vec::new();

                let size = file.read_to_end(&mut file_data)
                    .map_err(|err| format!("Failed to read file. Skipping (Err: {err})"))?;

                if size == 0 {
                    return Err(format!("No data to read in file. Skipping ({})", file_entry.path().display()).into());
                }

                let decoded_shard_data: Vec<u64> = bitcode::decode(&file_data)
                    .map_err(|err| format!("Failed to decode shard file data. Skipping (Err: {err})"))?;

                let file_name = match file_entry.file_name().to_str() {
                    Some(data) => data.to_string(),
                    None => {
                        return Err(format!("Failed to get file_name. Skipping (File: {})", file_entry.path().display()).into());
                    }
                };

                let shard_id = usize::from_str(&file_name)
                    .map_err(|err| format!("Failed to convert string to number. Skipping (File: {}, Err: {})", file_entry.path().display(), err))?;

                if shard_mismatch {
                    decoded_shard_data.into_par_iter().for_each(|item| {
                        let shard_index = item % self.shard_count as u64;

                        if let Ok(mut data_shard) = self.data_store[shard_index as usize].write() {
                            if !data_shard.insert(item) {
                                return;
                            }
                        }
                    });

                    return Err("Soft error: Shard store miss-match, converting to current shard_size!".into());
                }

                if let Ok(mut data_shard) = self.data_store[shard_id].write() {
                    for decoded_datum in decoded_shard_data { 
                        data_shard.insert(decoded_datum);
                    }
                }

                Ok(())
            }).collect();

        if re_enqueue {
            let data_load_results: Vec<Result<(), Box<dyn Error + Send + Sync>>> = data_files
                .par_bridge()
                .map(|potential_file| -> Result<(), Box<dyn Error + Send + Sync>> {
                    let file_entry = potential_file
                        .map_err(|_| "No file found in dir_entry")?;

                    let mut file = File::open(file_entry.path())
                        .map_err(|err| format!("Failed to open file. Skipping (Err: {err})"))?;
                    let mut file_data: Vec<u8> = Vec::new();

                    let size = file.read_to_end(&mut file_data)
                        .map_err(|err| format!("Failed to read file. Skipping (Err: {err})"))?;

                    if size == 0 {
                        return Err(format!("No data to read in file. Skipping ({})", file_entry.path().display()).into());
                    }

                    let decoded_shard_data: Vec<V> = bitcode::decode(&file_data)
                        .map_err(|err| format!("Failed to decode shard file data. Skipping (Err: {err})"))?;

                    let file_name = match file_entry.file_name().to_str() {
                        Some(data) => data.to_string(),
                        None => {
                            return Err(format!("Failed to get file_name. Skipping (File: {})", file_entry.path().display()).into());
                        }
                    };
                    
                    let shard_file_name = file_name.split_once("-")
                        .map(|(before, _)| before)
                        .ok_or_else(|| format!("Failed to get shard_id from file_name. Skipping (File {})", file_entry.path().display()))?;

                    let shard_id = usize::from_str(&shard_file_name)
                        .map_err(|err| format!("Failed to convert string to number. Skipping (File: {}, Err: {})", file_entry.path().display(), err))?;

                    if shard_mismatch {
                        for decoded_datum in decoded_shard_data {
                            let datum = Arc::new(decoded_datum);
                    
                            let hash = self.data_hasher.hash_one(&datum);
                            let new_shard_index = hash % self.shard_count as u64;
                    
                            if let Ok(mut queue) = self.insertion_queue[new_shard_index as usize].write() {
                                queue.push_back(datum);
                            }
                        }
                    
                        return Err("Soft error: Shard data miss-match, converting to current shard_size!".into());
                    }

                    for decoded_datum in decoded_shard_data {
                        let datum = Arc::new(decoded_datum);

                        if let Ok(mut queue) = self.insertion_queue[shard_id].write() {
                            queue.push_back(datum);
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
    /// You do not necessarily need to use this function for auto-saving. All methods used are publicly available and easily re-implementable. See source.
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
    /// let db: Arc<ExtractDb<u8>> = Arc::new(ExtractDb::new(None));
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