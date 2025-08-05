#![warn(missing_docs)]
#![doc = include_str!("../README.md")]
use std::error::Error;
use std::hash::{BuildHasher, Hash, RandomState};
use std::sync::RwLock;

use concurrent_queue::ConcurrentQueue;
use hashbrown::HashSet;

const SHARD_COUNT: usize = 16;

/// `ExtractDb` is a concurrent hash-store.
///
/// `ExtractDb` only supplies a push & fetch interface where both are ``&self``.
/// Once data is inserted it can never be removed. Persistence guaranteed.
///
/// You can think of it as a non-mutable concurrent `VecDequeue` with unique values only.
pub struct ExtractDb<V>
    where
        V: Eq + Hash + Clone + 'static
{
    shard_count: usize,
    data_store_shards: Vec<RwLock<HashSet<&'static V>>>,
    data_hasher: RandomState,

    insertion_queue: ConcurrentQueue<&'static V>,
    removal_store: ConcurrentQueue<&'static V>,
}

impl<V> Default for ExtractDb<V>
    where
        V: Eq + Hash + Clone + 'static
{
    fn default() -> Self {
        Self::new()
    }
}

impl<V> ExtractDb<V>
    where
        V: Eq + Hash + Clone + 'static
{
    /// Creates a new `ExtractDb`
    ///
    /// # Examples
    /// ```rust
    /// use extractdb::ExtractDb;
    ///
    /// let db: ExtractDb<&str> = ExtractDb::new();
    ///
    /// assert_eq!(db.push("Hello ExtractDb!"), true);
    /// ```
    pub fn new() -> ExtractDb<V> {
        Self::new_with_shards(SHARD_COUNT)
    }

    /// Creates a new `ExtractDb` with a specific internal sharding amount
    ///
    /// # Examples
    /// ```rust
    /// use extractdb::ExtractDb;
    ///
    /// let db: ExtractDb<&str> = ExtractDb::new_with_shards(32);
    ///
    /// assert_eq!(db.push("Hello ExtractDb with custom shards!"), true);
    /// ```
    pub fn new_with_shards(shard_count: usize) -> ExtractDb<V> {
        let shards: Vec<RwLock<HashSet<&'static V>>> = (0..shard_count)
            .map(|_| RwLock::new(HashSet::new()))
            .collect();

        ExtractDb {
            shard_count,
            data_store_shards: shards,
            data_hasher: RandomState::new(),
            insertion_queue: ConcurrentQueue::unbounded(),
            removal_store: ConcurrentQueue::unbounded()
        }
    }

    /// Pushes data into the internal sharded hashset.
    ///
    /// # Returns
    /// ``True`` if data has successfully inserted into a hashset
    /// ``False`` if data has already been added to a hashset, or if the internal shard is poisoned
    ///
    /// # Examples
    /// ```rust
    /// use extractdb::ExtractDb;
    ///
    /// let db: ExtractDb<i32> = ExtractDb::new();
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
            return match data_shard.insert(data) {
                true => {
                    self.insertion_queue.push(data).is_ok()
                }
                false => false
            };
        }

        false
    }

    /// Fetches a unique item from an internal queue
    ///
    /// This function may act as a FIFO during low contention scenarios. Order is not guaranteed.
    ///
    /// # Returns
    /// ``V`` A cloned copy of the internal item
    ///
    /// # Errors
    /// ``Box<dyn Error + '_>`` may return if queue is empty or if loading has a critical error
    ///
    /// # Examples
    /// ```rust
    /// use extractdb::ExtractDb;
    ///
    /// let db: ExtractDb<&str> = ExtractDb::new();
    ///
    /// assert_eq!(db.push("hello world"), true);
    /// assert_eq!(db.fetch_next().unwrap(), &"hello world");
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
    /// let db: ExtractDb<u8> = ExtractDb::new();
    ///
    /// assert_eq!(db.push(20), true);
    /// assert_ne!(db.fetch_count(), 1); // No data is currently loaded
    /// assert_eq!(db.fetch_next().unwrap(), &20); // Causes a load for the non-mutable vector
    /// assert_eq!(db.fetch_count(), 0);
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
    /// let db: ExtractDb<u8> = ExtractDb::new();
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
        for _ in 0..self.insertion_queue.len() {
            if let Ok(item) = self.insertion_queue.pop() {
                if self.removal_store.push(item).is_err() {
                    return Err("Failed to load sharded data into removal_store queue".into());
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use super::*;

    /// Attempts to insert a single value map into the ExtractDb<i32>
    ///
    /// # Returns
    ///
    /// This test should always return 1 -> ExtractDb::internal_count()
    #[test]
    fn push() {
        let db: ExtractDb<i32> = ExtractDb::new();

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
        let db: ExtractDb<i32> = ExtractDb::new();

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
        let db: ExtractDb<i32> = ExtractDb::new();

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
        let database: Arc<ExtractDb<String>> = Arc::new(ExtractDb::new());
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


    /// Get count of empty accessible store in a ExtractDb<i32>
    /// The reason this returns an empty count even after insertion is a fetch_next did not occur.
    ///
    /// # Returns
    ///
    /// This test should always return 0 -> ExtractDb::count()
    #[test]
    fn count_empty_store() {
        let db: ExtractDb<i32> = ExtractDb::new();

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
        let db: ExtractDb<i32> = ExtractDb::new();

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
        let db: ExtractDb<i32> = ExtractDb::new();

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
        let database: ExtractDb<i64> = ExtractDb::new();

        for i in 0..128 {
            database.push(i);
        }

        for _ in 0..128 {
            assert!(database.fetch_next().is_ok());
        }
    }

    /// Fetches data from a empty ExtractDb<i32>
    ///
    /// # Returns
    ///
    /// This test should always return True -> ExtractDb::fetch_next().is_err()
    #[test]
    fn fetch_data_empty() {
        let database: ExtractDb<i64> = ExtractDb::new();

        assert!(database.fetch_next().is_err());
    }

    /// Checks if data is fetched and returned twice from a ExtractDb<i32>
    #[test]
    fn duplicate_fetch() {
        let database: ExtractDb<i64> = ExtractDb::new();

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
