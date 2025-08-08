# Extract DB
[![Crates.io](https://img.shields.io/crates/v/extractdb?style=flat-square)](https://crates.io/crates/extractdb) [![docs.rs](https://img.shields.io/docsrs/extractdb?style=flat-square)](https://docs.rs/extractdb/)

A thread-safe, in-memory hash store supporting concurrent fetches and writes.<br/>

This is not a traditional kv-store, in the sense that it doesn't use any form of keys.<br/>
Specific "item" removal is not supported in favor of a fetching type system and can be thought of as a read-only dequeue database.

# Table of contents
- [Guarantees](#guarantees)
- [Trade-offs](#trade-offs)
- [Use scenarios](#use-scenarios)
- [Installation](#installation)
- [Examples](#examples)
  - Basics
    - [Push, Fetch, & count](#push-fetch--count) 
  - Multithreaded
    - [Multithreaded insert & fetch](#multithreaded-insert--fetch)
  - Disk
    - [Disk loading and saving](#disk-loading-and-saving)
    - [Auto saving](#auto-saving)
- [Testing](#testing--more-examples)
- [Contributing](#contributing)
- [License](#license)

# Guarantees.
- Fetching is guaranteed however out-of-order (Not FIFO/FILO)
- Fetching will never output duplicates
- Fetching & pushing are fully thread-safe functions
- Once inserted never removed (**Read-only**)

# Trade-offs.
- No item removal
- Non-deterministic fetch order 
- Write throughput is prioritized over reading performance

# Use scenarios:
- Concurrent queue with unique items only (`HashSet` + `VecDeque`)-like
- Fast concurrent insertions are needed over concurrent reads
- Fast reading on a single-thread with multiple concurrent writers
- Persistent in-memory hash-store

This was originally built for a web-scraper which needs to write lots of links with fewer reads.

# Installation
```toml
# Cargo.toml
[dependencies]
extractdb = "0.1.0"
```

# Examples

### Push, fetch, & count
```rust
use extractdb::ExtractDb;

fn main() {
    let database: ExtractDb<i32> = ExtractDb::new(None);

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

### Multithreaded insert & fetch
```rust
use std::sync::Arc;
use extractdb::ExtractDb;
use std::thread;

fn main() {
    let database: Arc<ExtractDb<String>> = Arc::new(ExtractDb::new(None));

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

### Disk loading and saving
```rust
use std::path::PathBuf;
use extractdb::ExtractDb;

fn main() {
    let database: ExtractDb<String> = ExtractDb::new(Some(PathBuf::from("./test_db")));

    // `True`: Load all items back into `fetch_next` queue
    database.load_from_disk(true).unwrap();

    database.push("Hello world!".to_string());

    database.save_to_disk().unwrap();
}
```

### Auto saving
```rust
use std::sync::Arc;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use extractdb::{CheckpointSettings, ExtractDb};

fn main() {
    let database: Arc<ExtractDb<String>> = Arc::new(ExtractDb::new(Some(PathBuf::from("./test_db"))));

    // `True`: Load all items back into `fetch_next` queue
    database.load_from_disk(true).unwrap();

    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let mut save_settings = CheckpointSettings::new(shutdown_flag.clone());
    save_settings.minimum_changes = 1000;
    
    // Spawns a background watcher thread. 
    // This checks for a minimum of 1000 changes every 30 seconds (default)
    ExtractDb::background_checkpoints(save_settings, database.clone());
    
    // Perform single/multithreaded logic
    database.push("Hello world!".to_string());

    // Gracefully shutdown the background saving thread
    shutdown_flag.store(true, Ordering::Relaxed);
}
```

# Testing + More examples
This project includes some basic tests to maintain functionality please use them.
```text
cargo test
```

See internal doc-comments for more indepth information about each test:
- `push`
- `push_multiple`
- `push_collided`
- `push_multi_thread`
- `push_structure`
- `count_empty_store`
- `count_loaded_store`
- `fetch_data`
- `fetch_data_multiple`
- `fetch_data_empty`
- `duplicate_fetch`
- `save_state_to_disk`
- `load_state_from_disk`
- `load_corrupted_state_from_disk`
- `load_shard_mismatch_from_disk`
- `load_mismatch_type_from_disk`

# Contributing
Pull request and issue contributions are very welcome. Please feel free to suggest changes in PRs/Issues :)

# License
This project is licensed under either [MIT](LICENSE) or [Apache-2.0](LICENSE-APACHE), you choose.