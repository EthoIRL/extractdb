use std::collections::HashSet;
use std::error::Error;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::{Arc, RwLock};
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::mpsc::SendError;
use std::thread;

pub struct Extractdb<V: Send + Sync + Eq + Hash> {
    data_store: Arc<RwLock<HashSet<V>>>,
    data_sender: Sender<V>,

    accessible_store: RwLock<Vec<V>>,
    accessible_index: AtomicUsize
}

impl<V: Send + Sync + Eq + Hash + 'static> Extractdb<V> {
    pub fn new<K: Send + Sync + Eq + Hash + 'static>() -> Extractdb<V> {
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

    pub fn internal_count(&self) -> Result<usize, Box<dyn Error>> {
        if let Ok(locked_store) = self.data_store.read() {
            return Ok(locked_store.len());
        }

        Err("Failed to retrieve internal count of data_store".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_appends_data_success() {
        let mut database: Extractdb<String> = Extractdb::new::<String>();

        for x in 0..125000 {
            database.push(String::from(format!("{:?}", x))).unwrap();
        }

        assert_ne!(database.internal_count().unwrap(), 0);
    }
}
