# Extract DB
A thread-safe, in-memory value store supporting concurrent reads and writes.<br/>

This is not a traditional kv-store, in the sense that it doesn't use any form of keys.<br/>
Specific "item" removal is not supported in favor of a fetching type system and can be thought of as a read-only dequeue database.

# Guarantees.
- Fetching is guaranteed however out-of-order (Not FIFO/FILO)
- Fetching will never output duplicates
- Fetching & pushing are fully thread-safe functions
- Once inserted never removed (**Read-only**)

# Use scenarios:
- Fast concurrent insertions are needed over concurrent reads
- Fast reading on a single-thread with multiple concurrent writers
- Persistent in-memory hash-store

This was originally built for a web-scraper which needs to write lots of links with fewer reads.

# Example
Simple push, fetch, & count example
```rust
use extractdb::ExtractDb;

pub fn main() {
    let database: ExtractDb<i32> = ExtractDb::new();

    database.push(100);

    let total_items_in_db = database.internal_count();
    let mut items_in_quick_access_memory = 0;
    if total_items_in_db > 0 {
        let item: &i32 = database.fetch_next().unwrap();

        items_in_quick_access_memory = database.fetch_count();
    }
    
    println!("Total items: {} | Quick Access item count: {}", total_items_in_db, items_in_quick_access_memory);
}
```

Simple multithreaded insert & fetch example

```rust
use std::sync::Arc;
use extractdb::ExtractDb;
use std::thread;

pub fn main() {
    let database: Arc<ExtractDb<String>> = Arc::new(ExtractDb::new());

    for thread_id in 0..8 {
        let local_database = Arc::clone(&database);
        thread::spawn(move || {
            local_database.push(format!("Hello from thread {}", thread_id))
        });
    }

    // Will only print some of the items... since we are not waiting for thread completion.
    for _ in 0..8 {
        if let Ok(item) = database.fetch_next() {
            println!("Item: {}", item);
        }
    }
}
```


# License
This project is licensed under [GPL-3.0](LICENSE)