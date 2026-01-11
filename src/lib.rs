#![warn(missing_docs)]
#![cfg_attr(not(doctest), doc = include_str!("../README.md"))]

use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
use std::sync::{Arc, RwLock};

use bitcode::{Decode, Encode};
use concurrent_queue::{ConcurrentQueue, PushError as CQPushError};
use hashbrown::HashSet;
use xxhash_rust::xxh3::Xxh3DefaultBuilder;

pub use crate::config::{CheckpointSettings, ExtractConfig};
pub use crate::error::{FetchError, LoadError, SaveError, PushError};

mod error;
mod config;
mod disk;
mod checkpoints;

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

impl<V> Default for ExtractDb<V>
    where
        V: Eq + Hash + Clone + Send + Sync + Encode + for<'a> Decode<'a>
{
    fn default() -> Self {
        Self::new(ExtractConfig::default())
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
    /// let db: ExtractDb<String> = ExtractDb::default();
    ///
    /// assert!(db.push(Arc::new("Hello ExtractDb!".to_string())).is_ok());
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
    /// assert!(db.push(Arc::new(100)).is_ok());
    /// assert!(db.push(Arc::new(100)).is_err());
    /// assert_eq!(db.internal_count(), 1);
    /// ```
    pub fn push(&self, value: Arc<V>) -> Result<(), PushError> {
        let hash = self.data_hasher.hash_one(&value);
        let shard_index = hash & self.shard_mask;

        self.push_shard(value, shard_index as usize, hash)
    }

    fn push_shard(&self, value: Arc<V>, shard_index: usize, hash: u64) -> Result<(), PushError> {
        if self.config.optimistic_read {
            let data_shard = self.shards[shard_index].data_store.read()?;
            if data_shard.contains(&hash) {
                return Err(PushError::Collision);
            }
            drop(data_shard);
        }

        let mut data_shard = self.shards[shard_index].data_store.write()?;
        if !data_shard.insert(hash) {
            return Err(PushError::Collision);
        }
        drop(data_shard);

        let mut queue = self.shards[shard_index].disk_queue.write()?;
        queue.push(Arc::clone(&value));
        drop(queue);
        
        let mut queue = self.shards[shard_index].insertion_queue.write()?;
        queue.push(value);
        drop(queue);
        
        Ok(())
    }

    /// Fetches a unique item from an internal queue
    ///
    /// This function may act as a FIFO during low contention scenarios. Order is not guaranteed.
    ///
    /// # Returns
    /// `Arc<V>` A reference of the internal item
    ///
    /// # Errors
    /// [`FetchError<Arc<V>>`] may return if queue is empty or if loading has a critical error. See [`FetchError`] doc for more info.
    ///
    /// # Examples
    /// ```rust
    /// use extractdb::ExtractDb;
    /// use std::sync::Arc;
    ///
    /// let db: ExtractDb<String> = ExtractDb::default();
    ///
    /// assert!(db.push(Arc::new("hello world".to_string())).is_ok());
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

    fn load_shards_to_accessible(&self) -> Result<(), CQPushError<Arc<V>>>  {
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
    /// assert!(db.push(Arc::new(20)).is_ok());
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
    ///     assert!(db.push(Arc::new(i)).is_ok());
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

    /// Exposes the internal config used to create the [`ExtractDb`] database.
    /// 
    /// # Returns
    /// [`&ExtractConfig`] reference of internal config.
    pub fn get_config(&self) -> &ExtractConfig {
        &self.config
    }
}