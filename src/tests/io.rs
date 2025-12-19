use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use bitcode::{Decode, Encode};
use crate::ExtractDb;

/// Attempts to insert a single value map into the ExtractDb<i32>
///
/// # Returns
///
/// This test should always return 1 -> ExtractDb::internal_count()
#[test]
fn push() {
    let db: ExtractDb<i32> = ExtractDb::new(None);

    db.push(Arc::new(100));

    assert_eq!(db.internal_count(), 1);
}

/// Inserts multiple unique non-overlapping values into ExtractDb<i32>
///
/// # Returns
///
/// This test should always return 128 -> ExtractDb::internal_count()
#[test]
fn push_multiple() {
    let db: ExtractDb<i32> = ExtractDb::new(None);

    for count in 0..128 {
        db.push(Arc::new(count));
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
    let db: ExtractDb<i32> = ExtractDb::new(None);

    db.push(Arc::new(10));
    db.push(Arc::new(10));

    assert_eq!(db.internal_count(), 1);
}

#[derive(Eq, PartialEq, Hash, Clone, Encode, Decode)]
struct TestStructure {
    id: u64,
    duration: Option<Duration>,
    retries: u32,
    tags: BTreeSet<String>,
    metadata: BTreeMap<String, String>,
    source: Option<IpAddr>,
    status: Status,
    name: String,
    dry_run: bool,
    error_code: i32,
    dependencies: Vec<u64>,
    confidence: i32
}

#[derive(Eq, PartialEq, Hash, Clone, Encode, Decode, Debug)]
enum Status {
    Running,
    Dead,
    AliveButDead,
    QuantumTunneled
}

/// Inserts a unique struct into a ExtractDb<TestStructure>
///
/// # Returns
///
/// id -> 1219
/// duration -> Some(Duration::from_nanos(1))
/// retries -> 9281
/// tags -> String::from("Hi")
/// metadata -> BTreeMap::new()
/// source -> None
/// status -> Status::QuantumTunneled
/// name -> String::from("Really important struct for my really important library")
/// dry_run -> false
/// error_code -> -299
/// dependencies -> vec![0, 28291928, 100]
/// confidence -> 100
#[test]
fn push_structure() {
    let database: ExtractDb<TestStructure> = ExtractDb::new(None);

    let id = 1219;
    let duration = Some(Duration::from_nanos(1));
    let retries = 9281;
    let mut tags = BTreeSet::new();
    tags.insert("Hi".to_string());
    let metadata = BTreeMap::new();
    let source = None;
    let status = Status::QuantumTunneled;
    let name = String::from("Really important struct for my really important library");
    let dry_run = false;
    let error_code = -299;
    let dependencies: Vec<u64> = vec![0, 28291928, 100];
    let confidence = 100;

    database.push(Arc::new(TestStructure {
        id,
        duration,
        retries,
        tags: tags.clone(),
        metadata: metadata.clone(),
        source,
        status: status.clone(),
        name: name.clone(),
        dry_run,
        error_code,
        dependencies: dependencies.clone(),
        confidence,
    }));

    let structure_fetch = database.fetch_next();

    assert!(structure_fetch.is_ok());
    let structure = structure_fetch.unwrap();

    assert_eq!(structure.id, id);
    assert_eq!(structure.duration, duration);
    assert_eq!(structure.retries, retries);
    assert_eq!(structure.tags, tags);
    assert_eq!(structure.metadata, metadata);
    assert_eq!(structure.source, source);
    assert_eq!(structure.status, status);
    assert_eq!(structure.name, name);
    assert_eq!(structure.dry_run, dry_run);
    assert_eq!(structure.error_code, error_code);
    assert_eq!(structure.dependencies, dependencies);
    assert_eq!(structure.confidence, confidence);
}

/// Get count of empty accessible store in a ExtractDb<i32>
/// The reason this returns an empty count even after insertion is a fetch_next did not occur.
///
/// # Returns
///
/// This test should always return 0 -> ExtractDb::count()
#[test]
fn count_empty_store() {
    let db: ExtractDb<i32> = ExtractDb::new(None);

    db.push(Arc::new(0));
    db.push(Arc::new(10));
    db.push(Arc::new(100));
    db.push(Arc::new(1000));

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
    let db: ExtractDb<i32> = ExtractDb::new(None);

    db.push(Arc::new(0));
    db.push(Arc::new(10));
    db.push(Arc::new(100));
    db.push(Arc::new(1000));

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
    let db: ExtractDb<i32> = ExtractDb::new(None);

    db.push(Arc::new(0));
    db.push(Arc::new(1000));

    assert!(db.fetch_next().is_ok());
}

/// Fetches multiple pieces of data from a non-empty ExtractDb<i32>
///
/// # Returns
///
/// This test should always return True -> ExtractDb::fetch_next().is_ok()
#[test]
fn fetch_data_multiple() {
    let database: ExtractDb<i64> = ExtractDb::new(None);

    for i in 0..128 {
        database.push(Arc::new(i));
    }

    for _ in 0..128 {
        assert!(database.fetch_next().is_ok());
    }
}

/// Fetches data from an empty ExtractDb<i32>
///
/// # Returns
///
/// This test should always return True -> ExtractDb::fetch_next().is_err()
#[test]
fn fetch_data_empty() {
    let database: ExtractDb<i64> = ExtractDb::new(None);

    assert!(database.fetch_next().is_err());
}

/// Checks if data is fetched and returned twice from a ExtractDb<i32>
#[test]
fn duplicate_fetch() {
    let database: ExtractDb<i64> = ExtractDb::new(None);

    assert_eq!(database.push(Arc::new(-1)), true);
    assert_eq!(database.fetch_count(), 0);

    let initial_value = database.fetch_next().unwrap();

    assert_eq!(initial_value, Arc::from(-1));

    for i in 0..100 {
        assert_eq!(database.push(Arc::new(i)), true);
    }

    assert_eq!(database.fetch_count(), 0);

    for i in 0..100 {
        assert_eq!(database.push(Arc::new(i + 1000)), true);
    }

    for _ in 0..200 {
        let data = database.fetch_next();
        assert!(data.is_ok());

        assert_ne!(data.unwrap(), initial_value);
    }

    assert!(database.fetch_next().is_err());
}