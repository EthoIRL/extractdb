use std::collections::HashSet;
use std::error::Error;
use std::hash::{BuildHasher, Hash, RandomState};
use std::ops::Deref;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

const SHARD_COUNT: usize = 16;

pub struct Extractdb<V>
    where
        V: Send + Sync + Eq + Hash
{
    data_store_shards: Vec<RwLock<HashSet<V>>>,
    data_hasher: RandomState,

    accessible_store: RwLock<Vec<V>>,
    accessible_index: AtomicUsize,
}

impl<V: Send + Sync + Eq + Hash + Clone + 'static> Extractdb<V> {
    pub fn new() -> Extractdb<V> {
        let shards: Vec<RwLock<HashSet<V>>> = (0..SHARD_COUNT)
            .map(|_| RwLock::new(HashSet::new()))
            .collect();

        Extractdb {
            data_store_shards: shards,
            data_hasher: RandomState::new(),
            accessible_store: RwLock::new(Vec::new()),
            accessible_index: AtomicUsize::new(0)
        }
    }

    pub fn push(&self, value: V) -> bool {
        let shard_index = self.data_hasher.hash_one(&value) as usize % SHARD_COUNT;

        if let Ok(mut data_shard) = self.data_store_shards[shard_index].write() {
            return data_shard.insert(value);
        }

        false
    }

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

    pub fn count(&self) -> Result<usize, Box<dyn Error>> {
        self.accessible_store
            .read()
            .map(|reader| reader.len())
            .map_err(|err| format!("Failed to retrieve internal count of data_store ({})", err).into())
    }

    pub fn internal_count(&self) -> Result<usize, Box<dyn Error + Send + Sync>> {
        let mut global_shard_size = 0;
        for data_store_shard in self.data_store_shards.deref() {
            if let Ok(data_shard) = data_store_shard.read() {
                global_shard_size += data_shard.len()
            }
        }

        return Ok(global_shard_size);
    }

    fn load_shards_to_accessible(&self) -> Result<(), Box<dyn Error + '_>>  {
        if let Ok(mut accessible_store) = self.accessible_store.write() {
            let mut items_to_add: Vec<V> = Vec::new();

            for data_store_shard in self.data_store_shards.deref() {
                let data_store_reader = data_store_shard.read().unwrap();

                for data_store_item in data_store_reader.iter() {
                    if !accessible_store.contains(&data_store_item) {
                        items_to_add.push(data_store_item.clone())
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
    use super::*;

    #[test]
    fn push_data_success() {
        let mut database: Extractdb<String> = Extractdb::new();

        for x in 0..125000 {
            database.push(String::from(format!("{:?}", x)));
        }

        assert_ne!(database.internal_count().unwrap(), 0);
    }

    #[test]
    fn fetch_data_success() {
        let mut database: Extractdb<i64> = Extractdb::new();

        database.push(01010202030304040505);

        assert_eq!(database.fetch_next().unwrap(), 01010202030304040505);
    }

    #[test]
    fn push_multi_thread_success() {
        let database: Arc<Extractdb<String>> = Arc::new(Extractdb::new());

        let mut threads = Vec::new();
        for thread_id in 0..4 {
            let reference_database = Arc::clone(&database);
            threads.push(thread::spawn(move || {
                for count in 0..12500 {
                    reference_database.push(format!("{}-{}", thread_id, count));
                }
            }));
        }

        for thread in threads {
            thread.join().expect("Thread panicked during push");
        }

        assert_eq!(database.internal_count().unwrap(), 50000);
    }
}
