use std::collections::HashSet;
use std::error::Error;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::{Arc, RwLock, RwLockWriteGuard};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use crossbeam_utils::CachePadded;
use rand::Rng;

const SHARD_COUNT: usize = 16;

pub struct Extractdb<V: Send + Sync + Eq + Hash + 'static> {
    data_store_shards: Vec<CachePadded<RwLock<HashSet<V>>>>,

    accessible_store: RwLock<Vec<V>>,
    accessible_index: CachePadded<AtomicUsize>,
}

impl<V: Send + Sync + Eq + Hash + Clone + 'static> Extractdb<V> {
    pub fn new<K: Send + Sync + Eq + Hash + Clone + 'static>() -> Extractdb<V> {
        let shards = (0..SHARD_COUNT-1)
            .map(|_| CachePadded::new(RwLock::new(HashSet::new())))
            .collect();

        Extractdb {
            data_store_shards: shards,
            accessible_store: RwLock::new(Vec::new()),
            accessible_index: CachePadded::new(AtomicUsize::new(0))
        }
    }

    pub fn push(&self, item: V) -> Result<(), Box<dyn Error>> {
        let shard_index = fastrand::usize(0usize..SHARD_COUNT);

        if let Some(data_store_shard) = self.data_store_shards.get(shard_index) {
            if let Ok(mut data_store) = data_store_shard.try_write() {
                return match data_store.insert(item) {
                    true => Ok(()),
                    false => Err("Failed to insert item into data_store".into())
                }
            }
        }

        self.push(item)
    }

    pub fn fetch_next(&mut self) -> Result<V, Box<dyn Error + '_>> {
        let accessible_index = self.accessible_index.load(Ordering::Relaxed);
        let accessible_store_len = self.count()?;

        // TODO: While this is a basic "algorithm" it could be improved...
        // The current implementation works by loading more data into the store whenever the index reaches the current length.
        // The problem with this only occurs later on in large sets when the accessible_store index is small and the data_store is large.
        // After reaching the end it loads a massive amount of memory (e.g. the entire data_store) causing deadlocks and slowdowns.
        if accessible_index >= accessible_store_len {
            let mut local_vec: Vec<V> = Vec::new();
            for data_store_shard in &self.data_store_shards {
                let data_store_reader = data_store_shard.read()?;

                local_vec.extend(Vec::from_iter(data_store_reader.deref().clone()));
            }

            let mut accessible_store_writer =  self.accessible_store.write()?;
            *accessible_store_writer = local_vec;
        }

        self.accessible_index.fetch_add(1, Ordering::Relaxed);
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
        for data_store_shard in &self.data_store_shards {
            if let Ok(locked_store) = data_store_shard.read() {
                global_shard_size += locked_store.len()
            }
        }

        return Ok(global_shard_size);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;
    use dashmap::DashSet;
    use super::*;

    #[test]
    fn push_data_success() {
        let mut database: Extractdb<String> = Extractdb::new::<String>();

        for x in 0..125000 {
            database.push(String::from(format!("{:?}", x))).unwrap();
        }

        assert_ne!(database.internal_count().unwrap(), 0);
    }

    #[test]
    fn fetch_data_success() {
        let mut database: Extractdb<i64> = Extractdb::new::<i64>();

        database.push(01010202030304040505).unwrap();

        assert_eq!(database.fetch_next().unwrap(), 01010202030304040505);
    }

    #[test]
    fn push_multi_thread_success() {
        let database: Arc<Extractdb<String>> = Arc::new(Extractdb::new::<String>());

        let mut threads = Vec::new();
        for thread_id in 0..4 {
            let reference_database = Arc::clone(&database);
            threads.push(thread::spawn(move || {
                for count in 0..12500 {
                    reference_database.push(format!("{}-{}", thread_id, count)).unwrap();
                }
            }));
        }

        for thread in threads {
            thread.join().expect("Thread panicked during push");
        }

        assert_eq!(database.internal_count().unwrap(), 50000);
    }

    #[test]
    fn benchmark() {
        let dashmap: Arc<DashSet<String>> = Arc::new(DashSet::new());

        let mut threads = Vec::new();
        let start = Instant::now();
        for thread_id in 0..48 {
            let reference_database = Arc::clone(&dashmap);
            threads.push(thread::spawn(move || {
                for count in 0..1250000 {
                    reference_database.insert(format!("{}-{}", thread_id, count));
                }
            }));
        }

        for thread in threads {
            thread.join().expect("Thread panicked during push");
        }
        println!("DashSet: {:?} {}", start.elapsed(), dashmap.shards().len());

        let database: Arc<Extractdb<String>> = Arc::new(Extractdb::new::<String>());
        let mut threads = Vec::new();
        let start = Instant::now();
        for thread_id in 0..48 {
            let reference_database = Arc::clone(&database);
            threads.push(thread::spawn(move || {
                for count in 0..1250000 {
                    reference_database.push(format!("{}-{}", thread_id, count)).unwrap();
                }
            }));
        }

        for thread in threads {
            thread.join().expect("Thread panicked during push");
        }

        println!("ExtractDB: {:?}", start.elapsed())
    }
}
