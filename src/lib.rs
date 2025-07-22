use std::collections::HashSet;
use std::error::Error;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::mpsc::SendError;
use std::thread;

pub struct Extractdb<V: Send + Sync + Eq + Hash> {
    data_store: Arc<RwLock<HashSet<V>>>,
    data_sender: Sender<V>,

    accessible_store: RwLock<Vec<V>>,
    accessible_index: AtomicUsize
}

impl<V: Send + Sync + Eq + Hash + Clone + 'static> Extractdb<V> {
    pub fn new<K: Send + Sync + Eq + Hash + Clone + 'static>() -> Extractdb<V> {
        let (tx, rx) = channel();

        let data_store = Arc::new(RwLock::new(HashSet::new()));
        let internal_data_clone = Arc::clone(&data_store);

        let database = Extractdb {
            data_store,
            data_sender: tx,
            accessible_store: RwLock::new(Vec::new()),
            accessible_index: AtomicUsize::new(0)
        };

        thread::spawn(move || Self::internal_receiver(rx, internal_data_clone));

        database
    }

    fn internal_receiver(rx: Receiver<V>, data_store: Arc<RwLock<HashSet<V>>>) {
        loop {
            // Optimistic receiver
            if let Ok(value) = rx.try_recv() {
                if let Ok(mut internal_set) = data_store.write() {
                    internal_set.insert(value);
                }

                continue
            }

            if let Ok(value) = rx.recv() {
                if let Ok(mut internal_set) = data_store.write() {
                    internal_set.insert(value);
                }
            }
        }
    }

    pub fn push(&mut self, item: V) -> Result<(), SendError<V>>{
        self.data_sender.send(item)
    }

    pub fn fetch_next(&mut self) -> Result<V, Box<dyn Error + '_>> {
        let accessible_index = self.accessible_index.load(Ordering::Relaxed);
        let accessible_store_len = self.count()?;

        // TODO: While this is a basic "algorithm" it could be improved...
        // The current implementation works by loading more data into the store whenever the index reaches the current length.
        // The problem with this only occurs later on in large sets when the accessible_store index is small and the data_store is large.
        // After reaching the end it loads a massive amount of memory (e.g. the entire data_store) causing deadlocks and slowdowns.
        if accessible_index >= accessible_store_len {
            let data_store_reader = self.data_store.read()?;
            let mut accessible_store_writer =  self.accessible_store.write()?;

            // TODO: There might a better way to handle data loading from data_store to accessible_store
            *accessible_store_writer = Vec::from_iter(data_store_reader.deref().clone());
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
        if let Ok(locked_store) = self.data_store.read() {
            return Ok(locked_store.len());
        }

        Err("Failed to retrieve internal count of data_store".into())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use super::*;

    #[test]
    fn add_appends_data_success() {
        let mut database: Extractdb<String> = Extractdb::new::<String>();

        for x in 0..125000 {
            database.push(String::from(format!("{:?}", x))).unwrap();
        }
        thread::sleep(Duration::from_millis(10));

        assert_ne!(database.internal_count().unwrap(), 0);
    }

    #[test]
    fn fetch_data_success() {
        let mut database: Extractdb<i64> = Extractdb::new::<i64>();

        database.push(01010202030304040505).unwrap();
        thread::sleep(Duration::from_millis(10));

        assert_eq!(database.fetch_next().unwrap(), 01010202030304040505);
    }
}
