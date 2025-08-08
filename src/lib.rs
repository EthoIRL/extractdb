#![warn(missing_docs)]
#![doc = include_str!("../README.md")]
use std::{fs, thread};
use std::collections::VecDeque;
use std::error::Error;
use std::fs::File;
use std::hash::{BuildHasher, Hash, RandomState};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bitcode::{Decode, Encode};
use concurrent_queue::ConcurrentQueue;
use hashbrown::HashSet;
use rayon::iter::{ParallelIterator, IndexedParallelIterator, IntoParallelRefIterator, ParallelBridge, IntoParallelIterator};

const SHARD_COUNT: usize = 16;

/// `ExtractDb` is a thread-safe, in-memory hash store supporting concurrent fetches and writes.
///
/// `ExtractDb` only supplies a push & fetch interface where both are ``&self``.
/// Once data is inserted it can never be removed. Persistence guaranteed.
///
/// You can think of it as a non-mutable concurrent `VecDeque` with unique values only.
///
/// # Examples
/// Basic single threaded insertion example
/// ```no_run
/// use std::path::PathBuf;
/// use extractdb::ExtractDb;
///
/// let db: ExtractDb<i32> = ExtractDb::new(Some(PathBuf::from("/home/user/database_name")));
///
/// db.load_from_disk(true).unwrap();
///
/// db.push(100);
///
/// let item = db.fetch_next().unwrap();
///
/// db.save_to_disk().unwrap();
/// ```
///
/// # Autosaving
/// Multithreading capable, `Arc<ExtractDb>` with background auto saving.
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
///     db.push(i);
/// }
///
/// let item = db.fetch_next().unwrap();
///
/// // Exit background thread
/// shutdown_flag.store(true, Ordering::Relaxed);
/// ```
pub struct ExtractDb<V>
    where
        V: Eq + Hash + Clone + 'static + Send + Sync + Encode + for<'a> Decode<'a>
{
    shard_count: usize,
    data_store_shards: Vec<RwLock<HashSet<&'static V>>>,
    data_hasher: RandomState,

    insertion_queue: Vec<RwLock<VecDeque<&'static V>>>,
    removal_store: ConcurrentQueue<&'static V>,

    db_directory: Option<PathBuf>,
}

impl<V> Default for ExtractDb<V>
    where
        V: Eq + Hash + Clone + 'static + Send + Sync + Encode + for<'a> Decode<'a>
{
    fn default() -> Self {
        Self::new(None)
    }
}

impl<V> ExtractDb<V>
    where
        V: Eq + Hash + Clone + 'static + Send + Sync + Encode + for<'a> Decode<'a>
{
    /// Creates a new `ExtractDb`
    ///
    /// # Arguments
    /// `database_directory`: Allows saving of data to disk. This is **optional**!
    ///
    /// # Examples
    /// ```rust
    /// use extractdb::ExtractDb;
    ///
    /// // In-memory only example, set a path for save/loading.
    /// let db: ExtractDb<String> = ExtractDb::new(None);
    ///
    /// assert_eq!(db.push("Hello ExtractDb!".to_string()), true);
    /// ```
    pub fn new(database_directory: Option<PathBuf>) -> ExtractDb<V> {
        Self::new_with_shards(SHARD_COUNT, database_directory)
    }

    /// Creates a new `ExtractDb` with a specific internal sharding amount
    ///
    /// # Arguments
    /// `shard_count`: Shards to be used internally. Think more shards = more concurrency, vice versa.
    ///
    /// `database_directory`: Allows saving of data to disk. This is **optional**!
    ///
    /// # Examples
    /// ```rust
    /// use extractdb::ExtractDb;
    ///
    /// let db: ExtractDb<String> = ExtractDb::new_with_shards(32, None);
    ///
    /// assert_eq!(db.push("Hello ExtractDb with custom shards!".to_string()), true);
    /// ```
    pub fn new_with_shards(shard_count: usize, database_directory: Option<PathBuf>) -> ExtractDb<V> {
        let shards: Vec<RwLock<HashSet<&'static V>>> = (0..shard_count)
            .map(|_| RwLock::new(HashSet::new()))
            .collect();

        let queues: Vec<RwLock<VecDeque<&'static V>>> = (0..shard_count)
            .map(|_| RwLock::new(VecDeque::new()))
            .collect();

        ExtractDb {
            shard_count,
            data_store_shards: shards,
            data_hasher: RandomState::new(),
            insertion_queue: queues,
            removal_store: ConcurrentQueue::unbounded(),
            db_directory: database_directory
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
    ///
    /// let db: ExtractDb<i32> = ExtractDb::new(None);
    ///
    /// assert_eq!(db.push(100), true);
    /// assert_eq!(db.push(100), false);
    /// assert_eq!(db.internal_count(), 1);
    /// ```
    pub fn push(&self, value: V) -> bool {
        let hash = self.data_hasher.hash_one(&value);
        let shard_index = hash % self.shard_count as u64;

        let data: &'static V = Box::leak(Box::new(value));

        if let Ok(mut data_shard) = self.data_store_shards[shard_index as usize].write() {
            if data_shard.insert(data) {
                if let Ok(mut queue) = self.insertion_queue[shard_index as usize].write() {
                    queue.push_back(data);
                    return true;
                }
            }
        }

        false
    }

    /// Fetches a unique item from an internal queue
    ///
    /// This function may act as a FIFO during low contention scenarios. Order is not guaranteed.
    ///
    /// # Returns
    /// ``V`` A reference of the internal item
    ///
    /// # Errors
    /// ``Box<dyn Error + '_>`` may return if queue is empty or if loading has a critical error
    ///
    /// # Examples
    /// ```rust
    /// use extractdb::ExtractDb;
    ///
    /// let db: ExtractDb<String> = ExtractDb::new(None);
    ///
    /// assert_eq!(db.push("hello world".to_string()), true);
    /// assert_eq!(db.fetch_next().unwrap(), &"hello world".to_string());
    /// assert_eq!(db.internal_count(), 1);
    /// assert_eq!(db.fetch_count(), 0);
    /// ```
    pub fn fetch_next(&self) -> Result<&V, Box<dyn Error + '_>> {
        if self.removal_store.is_empty() {
            self.load_shards_to_accessible()?;
        }

        match self.removal_store.pop() {
            Ok(value) => Ok(value),
            Err(_) => Err("Failed to access data from accessible_store".into())
        }
    }

    /// Get the current count of the `fetch_next` mutable queue
    ///
    /// # Returns
    /// ``usize`` a total of all items loaded into the temporary fetch vector
    ///
    /// # Examples
    /// ```rust
    /// use extractdb::ExtractDb;
    ///
    /// let db: ExtractDb<u8> = ExtractDb::new(None);
    ///
    /// assert_eq!(db.push(20), true);
    /// assert_eq!(db.fetch_count(), 0); // No data is currently loaded
    /// assert_eq!(db.fetch_next().unwrap(), &20); // Causes a load for the non-mutable vector
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
    /// ``usize`` a total of all items in the entire sharded database.
    ///
    /// # Examples
    /// ```rust
    /// use extractdb::ExtractDb;
    ///
    /// let db: ExtractDb<u8> = ExtractDb::new(None);
    ///
    /// for i in 0..128 {
    ///     assert_eq!(db.push(i), true);
    /// }
    /// assert_eq!(db.internal_count(), 128);
    /// ```
    pub fn internal_count(&self) -> usize {
        let mut global_shard_size = 0;
        for data_store_shard in &*self.data_store_shards {
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
    /// ``Box<dyn Error>`` may return if database directory is not set or if creating fails.
    pub fn save_to_disk(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let Some(database_directory) = &self.db_directory else {
            return Err("No database directory is set. Cannot save to disk without a valid path set!".into())
        };

        if !database_directory.exists() {
            fs::create_dir_all(database_directory)?;
        }

        self.data_store_shards
            .par_iter()
            .enumerate()
            .try_for_each(|(id, shard)| -> Result<(), Box<dyn Error + Send + Sync>> {
                let data_shard = shard
                    .read()
                    .map_err(|_| format!("Shard {id} failed to read lock"))?;

                let internal_data: Vec<V> = data_shard.clone().into_iter().cloned().collect();
                let encoded_data = bitcode::encode(&internal_data);

                let file_shard_path = &database_directory.join(format!("{id}"));

                let mut file_shard = File::create(file_shard_path)
                    .map_err(|err| format!("Failed to create/truncate file for Shard {id}, ({err})"))?;

                file_shard
                    .write_all(&encoded_data)
                    .map_err(|err| format!("Failed to write to File Shard {id}, ({err})"))?;

                file_shard
                    .flush()
                    .map_err(|err| format!("Failed to flush File Shard {id}, ({err})"))?;

                Ok(())
            })
    }

    /// Loads all shard-files back into internal memory
    ///
    /// During a failure/corruption event all non-corrupted data will be loaded into memory before an error is omitted out.
    ///
    /// # Arguments
    /// `re_enqueue`: Loads all data back into fetch queue.
    ///
    /// # Errors
    /// ``Box<dyn Error + Send + Sync>`` may return if any form of corruption occurs, or if a shard size changes.
    pub fn load_from_disk(&self, re_enqueue: bool) -> Result<(), Box<dyn Error + Send + Sync>> {
        let Some(database_directory) = &self.db_directory else {
            return Err("No database directory is set. Cannot load from disk without a valid path set!".into())
        };

        if !database_directory.exists() {
            return Ok(());
        }

        let Ok(directory_files) = fs::read_dir(database_directory) else {
            return Err("No files present in database directory.".into())
        };

        let directory_count: usize = match fs::read_dir(database_directory) {
            Ok(files) => {
                files.count()
            },
            Err(_) => return Err("No files present in database directory.".into())
        };

        let load_results: Vec<Result<(), Box<dyn Error + Send + Sync>>> = directory_files
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

                if directory_count != self.shard_count {
                    decoded_shard_data.into_par_iter().for_each(|item| {
                        self.push(item);
                    });

                    return Err("Soft error: Shard miss-match, converting to current shard_size!".into());
                }

                let file_name = match file_entry.file_name().to_str() {
                    Some(data) => data.to_string(),
                    None => {
                        return Err(format!("Failed to get file_name. Skipping (File: {})", file_entry.path().display()).into());
                    }
                };

                let shard_id = usize::from_str(&file_name)
                    .map_err(|err| format!("Failed to convert string to number. Skipping (File: {}, Err: {})", file_entry.path().display(), err))?;


                let mut shard = self.data_store_shards[shard_id].write()
                    .map_err(|err| format!("Failed to acquire data_store_shards lock (Err: {err})"))?;
                let mut queue = self.insertion_queue[shard_id].write()
                    .map_err(|err| format!("Failed to acquire insertion_queue lock (Err: {err})"))?;

                for decoded_datum in decoded_shard_data {
                    let datum: &'static V = Box::leak(Box::new(decoded_datum));
                    if shard.insert(datum) && re_enqueue {
                        queue.push_back(datum);
                    }
                }

                Ok(())
            }).collect();

        for load_result in load_results {
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
    /// use std::sync::atomic::AtomicBool;
    /// use extractdb::{CheckpointSettings, ExtractDb};
    ///
    /// let db: Arc<ExtractDb<u8>> = Arc::new(ExtractDb::new(None));
    ///
    /// let shutdown_flag = Arc::new(AtomicBool::new(false));
    /// let mut save_settings = CheckpointSettings::new(shutdown_flag.clone());
    /// save_settings.minimum_changes = 1000;
    ///
    /// // Will now check for 1000 minimum changes every 30seconds (default).
    /// ExtractDb::background_checkpoints(save_settings, db);
    /// ```
    pub fn background_checkpoints(settings: CheckpointSettings, db: Arc<ExtractDb<V>>) {
        thread::spawn(move || {
            let mut last_checkpoint_count: usize = db.internal_count();

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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::net::IpAddr;
    use std::sync::Arc;
    use std::{env, panic, thread};
    use std::time::{Duration, Instant};

    use super::*;

    /// Attempts to insert a single value map into the ExtractDb<i32>
    ///
    /// # Returns
    ///
    /// This test should always return 1 -> ExtractDb::internal_count()
    #[test]
    fn push() {
        let db: ExtractDb<i32> = ExtractDb::new(None);

        db.push(100);

        assert_eq!(db.internal_count(), 1);
    }

    /// Inserts multiple unique non-overlapping values into ExtractDb<i32>
    ///
    /// # Returns
    ///
    /// This test should always return 128 -> ExtractDb::internal_count()
    #[test]
    fn push_multiple() {
        let db: ExtractDb<i32> = ExtractDb::new(None);

        for count in 0..128 {
            db.push(count);
        }

        assert_eq!(db.internal_count(), 128);
    }

    /// Inserts unique collided value twice into ExtractDb<i32>
    /// Test whether double unique insertion occurs
    ///
    /// # Returns
    ///
    /// This test should always return 1 -> ExtractDb::internal_count()
    #[test]
    fn push_collided() {
        let db: ExtractDb<i32> = ExtractDb::new(None);

        db.push(10);
        db.push(10);

        assert_eq!(db.internal_count(), 1);
    }

    /// Inserts unique values in a multithreaded environment into a ExtractDb<i32>
    ///
    /// # Returns
    ///
    /// This test should always return (thread_count * insertion_count) -> ExtractDb::internal_count()
    #[test]
    fn push_multi_thread() {
        let database: Arc<ExtractDb<String>> = Arc::new(ExtractDb::new(None));
        let thread_count = 4;
        let insertion_count = 128;

        let mut threads = Vec::new();
        for thread_id in 0..thread_count {
            let reference_database = Arc::clone(&database);
            threads.push(thread::spawn(move || {
                for count in 0..insertion_count {
                    reference_database.push(format!("{}-{}", thread_id, count));
                }
            }));
        }

        for thread in threads {
            thread.join().expect("Thread panicked during push");
        }

        assert_eq!(database.internal_count(), thread_count * insertion_count);
    }

    #[derive(Eq, PartialEq, Hash, Clone, Encode, Decode)]
    struct TestStructure {
        id: u64,
        duration: Option<Duration>,
        retries: u32,
        tags: BTreeSet<String>,
        metadata: BTreeMap<String, String>,
        source: Option<IpAddr>,
        status: Status,
        name: String,
        dry_run: bool,
        error_code: i32,
        dependencies: Vec<u64>,
        confidence: i32
    }

    #[derive(Eq, PartialEq, Hash, Clone, Encode, Decode, Debug)]
    enum Status {
        Running,
        Dead,
        AliveButDead,
        QuantumTunneled
    }

    /// Inserts a unique struct into a ExtractDb<TestStructure>
    ///
    /// # Returns
    ///
    /// id -> 1219
    /// duration -> Some(Duration::from_nanos(1))
    /// retries -> 9281
    /// tags -> String::from("Hi")
    /// metadata -> BTreeMap::new()
    /// source -> None
    /// status -> Status::QuantumTunneled
    /// name -> String::from("Really important struct for my really important library")
    /// dry_run -> false
    /// error_code -> -299
    /// dependencies -> vec![0, 28291928, 100]
    /// confidence -> 100
    #[test]
    fn push_structure() {
        let database: ExtractDb<TestStructure> = ExtractDb::new(None);

        let id = 1219;
        let duration = Some(Duration::from_nanos(1));
        let retries = 9281;
        let mut tags = BTreeSet::new();
        tags.insert("Hi".to_string());
        let metadata = BTreeMap::new();
        let source = None;
        let status = Status::QuantumTunneled;
        let name = String::from("Really important struct for my really important library");
        let dry_run = false;
        let error_code = -299;
        let dependencies: Vec<u64> = vec![0, 28291928, 100];
        let confidence = 100;

        database.push(TestStructure {
            id,
            duration,
            retries,
            tags: tags.clone(),
            metadata: metadata.clone(),
            source,
            status: status.clone(),
            name: name.clone(),
            dry_run,
            error_code,
            dependencies: dependencies.clone(),
            confidence,
        });

        let structure_fetch = database.fetch_next();

        assert!(structure_fetch.is_ok());
        let structure = structure_fetch.unwrap();

        assert_eq!(structure.id, id);
        assert_eq!(structure.duration, duration);
        assert_eq!(structure.retries, retries);
        assert_eq!(structure.tags, tags);
        assert_eq!(structure.metadata, metadata);
        assert_eq!(structure.source, source);
        assert_eq!(structure.status, status);
        assert_eq!(structure.name, name);
        assert_eq!(structure.dry_run, dry_run);
        assert_eq!(structure.error_code, error_code);
        assert_eq!(structure.dependencies, dependencies);
        assert_eq!(structure.confidence, confidence);
    }

    /// Get count of empty accessible store in a ExtractDb<i32>
    /// The reason this returns an empty count even after insertion is a fetch_next did not occur.
    ///
    /// # Returns
    ///
    /// This test should always return 0 -> ExtractDb::count()
    #[test]
    fn count_empty_store() {
        let db: ExtractDb<i32> = ExtractDb::new(None);

        db.push(0);
        db.push(10);
        db.push(100);
        db.push(1000);

        assert_eq!(db.fetch_count(), 0);
    }

    /// Get count of loaded accessible store in a ExtractDb<i32>
    /// The reason this returns a non-zero count is a fetch_next has occurred.
    ///
    /// # Returns
    ///
    /// This test should always return 4 -> ExtractDb::count()
    #[test]
    fn count_loaded_store() {
        let db: ExtractDb<i32> = ExtractDb::new(None);

        db.push(0);
        db.push(10);
        db.push(100);
        db.push(1000);

        db.fetch_next().unwrap();

        assert_eq!(db.fetch_count(), 3);
    }

    /// Fetches data from a non-empty ExtractDb<i32>
    ///
    /// # Returns
    ///
    /// This test should always return True -> ExtractDb::fetch_next().is_ok()
    #[test]
    fn fetch_data() {
        let db: ExtractDb<i32> = ExtractDb::new(None);

        db.push(0);
        db.push(1000);

        assert!(db.fetch_next().is_ok());
    }

    /// Fetches multiple pieces of data from a non-empty ExtractDb<i32>
    ///
    /// # Returns
    ///
    /// This test should always return True -> ExtractDb::fetch_next().is_ok()
    #[test]
    fn fetch_data_multiple() {
        let database: ExtractDb<i64> = ExtractDb::new(None);

        for i in 0..128 {
            database.push(i);
        }

        for _ in 0..128 {
            assert!(database.fetch_next().is_ok());
        }
    }

    /// Fetches data from an empty ExtractDb<i32>
    ///
    /// # Returns
    ///
    /// This test should always return True -> ExtractDb::fetch_next().is_err()
    #[test]
    fn fetch_data_empty() {
        let database: ExtractDb<i64> = ExtractDb::new(None);

        assert!(database.fetch_next().is_err());
    }

    /// Checks if data is fetched and returned twice from a ExtractDb<i32>
    #[test]
    fn duplicate_fetch() {
        let database: ExtractDb<i64> = ExtractDb::new(None);

        assert_eq!(database.push(-1), true);
        assert_eq!(database.fetch_count(), 0);

        let initial_value = database.fetch_next().unwrap();

        assert_eq!(initial_value, &-1);

        for i in 0..100 {
            assert_eq!(database.push(i), true);
        }

        assert_eq!(database.fetch_count(), 0);

        for i in 0..100 {
            assert_eq!(database.push(i + 1000), true);
        }

        for _ in 0..200 {
            let data = database.fetch_next();
            assert!(data.is_ok());

            assert_ne!(data.unwrap(), initial_value);
        }

        assert!(database.fetch_next().is_err());
    }

    /// Checks if state is correctly written to disk from a ExtractDb<i32>
    #[test]
    fn save_state_to_disk() {
        let current_directory = env::current_dir().expect("Could not find current_dir?");
        let test_db_directory = current_directory.join("test_save_state_to_disk_db");

        if test_db_directory.exists() {
            fs::remove_dir_all(test_db_directory.clone()).expect("Failed to delete residual test directory!?");
        }

        let database: ExtractDb<i32> = ExtractDb::new_with_shards(19, Some(test_db_directory.clone()));

        for i in 0..10000 {
            assert_eq!(database.push(i), true);
        }

        assert!(database.save_to_disk().is_ok());

        let mut found_files = 0;
        let read_dir = fs::read_dir(&test_db_directory).expect("failed to read contents of test_db_directory");
        read_dir.for_each(|potential_file| {
            if potential_file.is_ok() {
                found_files += 1;
            }
        });

        assert_eq!(found_files, 19);

        fs::remove_dir_all(test_db_directory).expect("Failed to delete residual test directory!?");
    }

    /// Checks if state is correctly written & loaded from disk from a ExtractDb<i32>
    #[test]
    fn load_state_from_disk() {
        let current_directory = env::current_dir().expect("Could not find current_dir?");
        let test_db_directory = current_directory.join("test_load_state_from_disk_db");

        if test_db_directory.exists() {
            fs::remove_dir_all(test_db_directory.clone()).expect("Failed to delete residual test directory!?");
        }

        let database: ExtractDb<String> = ExtractDb::new(Some(test_db_directory.clone()));

        for i in 0..10000 {
            assert_eq!(database.push(format!("Id: {}", i)), true);
        }

        assert!(database.save_to_disk().is_ok());

        drop(database); // Done to conserve memory during testing

        let new_database: ExtractDb<String> = ExtractDb::new(Some(test_db_directory.clone()));
        assert_eq!(new_database.internal_count(), 0);
        assert_eq!(new_database.fetch_count(), 0);

        assert!(new_database.load_from_disk(false).is_ok());

        assert_eq!(new_database.internal_count(), 10000);
        assert_eq!(new_database.fetch_count(), 0);

        fs::remove_dir_all(test_db_directory).expect("Failed to delete residual test directory!?");
    }

    /// Attempt to load a corrupted state of data from disk for a ExtractDb<String>
    ///
    /// Data is "corrupted" in the sense that some files are completely deleted.
    /// This ExtractDb should be capable of recovering the remaining "uncorrupted" data.
    #[test]
    fn load_corrupted_state_from_disk() {
        let current_directory = env::current_dir().expect("Could not find current_dir?");
        let test_db_directory = current_directory.join("test_load_corrupted_state_from_disk_db");

        if test_db_directory.exists() {
            fs::remove_dir_all(test_db_directory.clone()).expect("Failed to delete residual test directory!?");
        }

        let database: ExtractDb<String> = ExtractDb::new(Some(test_db_directory.clone()));

        for i in 0..10000 {
            assert_eq!(database.push(format!("Id: {}", i)), true);
        }

        assert!(database.save_to_disk().is_ok());
        drop(database); // Done to conserve memory during testing

        let mut deleted_files = 0;
        let read_dir = fs::read_dir(&test_db_directory).expect("failed to read contents of test_db_directory");
        read_dir.for_each(|potential_file| {
            if let Ok(file) = potential_file {
                if deleted_files < 5 {
                    fs::remove_file(file.path()).unwrap();
                    deleted_files += 1;
                }
            }
        });

        assert_eq!(deleted_files, 4 + 1);

        let new_database: ExtractDb<String> = ExtractDb::new(Some(test_db_directory.clone()));
        assert_eq!(new_database.internal_count(), 0);
        assert_eq!(new_database.fetch_count(), 0);

        assert!(new_database.load_from_disk(false).is_err());

        assert_ne!(new_database.internal_count(), 10000);
        assert_eq!(new_database.fetch_count(), 0);

        fs::remove_dir_all(test_db_directory).expect("Failed to delete residual test directory!?");
    }

    /// Attempt to load data where a different shard count was used during saving. ExtractDb<u64>
    #[test]
    fn load_shard_mismatch_from_disk() {
        let current_directory = env::current_dir().expect("Could not find current_dir?");
        let test_db_directory = current_directory.join("test_load_shard_mismatch_from_disk_db");

        if test_db_directory.exists() {
            fs::remove_dir_all(test_db_directory.clone()).expect("Failed to delete residual test directory!?");
        }

        let database: ExtractDb<u64> = ExtractDb::new(Some(test_db_directory.clone()));

        for i in 0..10000 {
            assert_eq!(database.push(i), true);
        }

        assert!(database.save_to_disk().is_ok());
        drop(database); // Done to conserve memory during testing

        let new_database: ExtractDb<u64> = ExtractDb::new_with_shards(48, Some(test_db_directory.clone()));
        assert_eq!(new_database.internal_count(), 0);
        assert_eq!(new_database.fetch_count(), 0);

        assert!(new_database.load_from_disk(false).is_err());

        assert_eq!(new_database.internal_count(), 10000);
        assert_eq!(new_database.fetch_count(), 0);

        fs::remove_dir_all(test_db_directory).expect("Failed to delete residual test directory!?");
    }

    /// Attempt to load miss matched ExtractDb<String> type from a ExtractDb<String>
    #[test]
    fn load_mismatch_type_from_disk() {
        let current_directory = env::current_dir().expect("Could not find current_dir?");
        let test_db_directory = current_directory.join("test_load_mismatch_type_from_disk_db");

        if test_db_directory.exists() {
            fs::remove_dir_all(test_db_directory.clone()).expect("Failed to delete residual test directory!?");
        }

        let database: ExtractDb<u64> = ExtractDb::new(Some(test_db_directory.clone()));

        for i in 0..10000 {
            assert_eq!(database.push(i), true);
        }

        assert!(database.save_to_disk().is_ok());
        drop(database); // Done to conserve memory during testing

        let new_database: ExtractDb<String> = ExtractDb::new(Some(test_db_directory.clone()));

        let panic_load = panic::catch_unwind(|| new_database.load_from_disk(false));
        assert!(panic_load.is_ok());
        assert_eq!(new_database.internal_count(), 0);
        assert_eq!(new_database.fetch_count(), 0);

        fs::remove_dir_all(test_db_directory).expect("Failed to delete residual test directory!?");
    }
