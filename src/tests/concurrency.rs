use std::sync::Arc;
use std::thread;
use crate::ExtractDb;

/// Inserts unique values in a multithreaded environment into a ExtractDb<i32>
///
/// # Returns
///
/// This test should always return (thread_count * insertion_count) -> ExtractDb::internal_count()
#[test]
fn push_multi_thread() {
    let database: Arc<ExtractDb<String>> = Arc::new(ExtractDb::default());
    let thread_count = 4;
    let insertion_count = 128;

    let mut threads = Vec::new();
    for thread_id in 0..thread_count {
        let reference_database = Arc::clone(&database);
        threads.push(thread::spawn(move || {
            for count in 0..insertion_count {
                reference_database.push(Arc::new(format!("{}-{}", thread_id, count)));
            }
        }));
    }

    for thread in threads {
        thread.join().expect("Thread panicked during push");
    }

    assert_eq!(database.internal_count(), thread_count * insertion_count);
}