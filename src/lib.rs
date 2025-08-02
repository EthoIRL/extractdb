use hashbrown::HashSet;
use std::error::Error;
use std::hash::{BuildHasher, Hash, RandomState};
use std::ops::Deref;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

const SHARD_COUNT: usize = 16;

pub struct ExtractDb<V>
    where
        V: Eq + Hash + Clone
{
    data_store_shards: Vec<RwLock<HashSet<V>>>,
    data_hasher: RandomState,

    accessible_store: RwLock<Vec<V>>,
    accessible_index: AtomicUsize,
}

impl<V> ExtractDb<V>
    where
        V: Eq + Hash + Clone
{
    pub fn new() -> ExtractDb<V> {
        let shards: Vec<RwLock<HashSet<V>>> = (0..SHARD_COUNT)
            .map(|_| RwLock::new(HashSet::new()))
            .collect();

        ExtractDb {
            data_store_shards: shards,
            data_hasher: RandomState::new(),
            accessible_store: RwLock::new(Vec::new()),
            accessible_index: AtomicUsize::new(0)
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
        let shard_index = self.data_hasher.hash_one(&value) as usize % SHARD_COUNT;

        if let Ok(mut data_shard) = self.data_store_shards[shard_index].write() {
            return data_shard.insert(value);
        }

        false
    }

    /// Fetches the next item in a internal non-mutable vector
    /// This is not a FIFO/FILO function and is unordered with no guarantees.
    ///
    /// # Returns
    /// ``V`` A cloned copy of the internal item
    ///
    /// # Examples
    /// ```rust
    /// use extractdb::ExtractDb;
    ///
    /// let db: ExtractDb<&str> = ExtractDb::new();
    ///
    /// assert_eq!(db.push("hello world"), true);
    /// assert_eq!(db.fetch_next().unwrap(), "hello world");
    /// assert_eq!(db.internal_count(), 1);
    /// assert_eq!(db.count().unwrap(), 1);
    /// ```
    pub fn fetch_next(&self) -> Result<V, Box<dyn Error + '_>> {
        let accessible_index = self.accessible_index.fetch_add(1, Ordering::AcqRel);
        let accessible_store_len = self.count()?;

        // TODO: While this is a basic "algorithm" it could be improved...
        // The current implementation works by loading more data into the store whenever the index reaches the current length.
        // The problem with this only occurs later on in large sets when the accessible_store index is small and the data_store is large.
        // After reaching the end it loads a massive amount of memory (e.g. the entire data_store) causing deadlocks and slowdowns.
        if accessible_index >= accessible_store_len {
            self.load_shards_to_accessible()?;
        }

        if let Ok(accessible_store_reader) = self.accessible_store.read() {
            let value = accessible_store_reader.get(accessible_index);

            return match value {
                Some(value) => Ok(value.clone()),
                None => Err("No new data could be retrieved from accessible_store".into())
            }
        }

        Err("Failed to access data from accessible_store".into())
    }

    /// Get the current count of the fetch_next non-mutable vector
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
    /// assert_ne!(db.count().unwrap(), 1); // No data is currently loaded
    /// assert_eq!(db.fetch_next().unwrap(), 20); // Causes a load for the non-mutable vector
    /// assert_eq!(db.count().unwrap(), 1);
    /// ```
    pub fn count(&self) -> Result<usize, Box<dyn Error>> {
        self.accessible_store
            .read()
            .map(|reader| reader.len())
            .map_err(|err| format!("Failed to retrieve internal count of data_store ({})", err).into())
    }

    /// Get the internal count of items in all shards. This represents the total amount of items in the database at any time.
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
        for data_store_shard in self.data_store_shards.deref() {
            if let Ok(data_shard) = data_store_shard.read() {
                global_shard_size += data_shard.len()
            }
        }

        global_shard_size
    }

    fn load_shards_to_accessible(&self) -> Result<(), Box<dyn Error + '_>>  {
        if let Ok(mut accessible_store) = self.accessible_store.write() {
            let mut items_to_add: Vec<V> = Vec::new();

            for data_store_shard in &self.data_store_shards {
                if let Ok(data_store) = data_store_shard.read() {
                    for data_store_item in data_store.iter() {
                        if !accessible_store.contains(&data_store_item) {
                            items_to_add.push(data_store_item.clone());
                        }
                    }
                }
            }

            for item in items_to_add {
                accessible_store.push(item);
            }

            return Ok(())
        }

        Err("Failed to load sharded data into accessible_store vector".into())
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

        assert_eq!(db.count().unwrap(), 0);
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

        assert_eq!(db.count().unwrap(), 4);
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
}
