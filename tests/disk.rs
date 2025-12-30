use std::sync::Arc;
use std::{env, fs, panic};
use extractdb::{ExtractConfig, ExtractDb};

/// Checks if state is correctly written to disk from a ExtractDb<i32>
#[test]
fn save_state_to_disk() {
    let current_directory = env::current_dir().expect("Could not find current_dir?");
    let test_db_directory = current_directory.join("test_save_state_to_disk_db");

    if test_db_directory.exists() {
        fs::remove_dir_all(test_db_directory.clone()).expect("Failed to delete residual test directory!?");
    }

    let config = ExtractConfig::default()
        .shard_count(19)
        .database_directory(Some(test_db_directory.clone()));
    
    let database: ExtractDb<i32> = ExtractDb::new(config);    

    for i in 0..10000 {
        assert_eq!(database.push(Arc::new(i)), true);
    }

    assert!(database.save_to_disk().is_ok());

    let test_store_directory = test_db_directory.join("store");
    let test_data_directory = test_db_directory.join("data");

    assert!(test_store_directory.exists());
    assert!(test_data_directory.exists());

    assert_eq!(count_entries(&test_db_directory), 2);
    // Shard count must be a power of 2! (Rounds up to 32 shard_count!)
    assert_eq!(count_entries(&test_store_directory), 32);
    assert_eq!(count_entries(&test_data_directory), 32);

    fs::remove_dir_all(test_db_directory).expect("Failed to delete residual test directory!?");
}

fn count_entries(root: &std::path::Path) -> usize {
    fs::read_dir(root)
        .unwrap_or_else(|e| panic!("Failed to read contents of {:?}: {}", root, e))
        .filter_map(Result::ok)
        .count()
}

/// Checks if state is correctly written & loaded from disk from a ExtractDb<i32>
#[test]
fn load_state_from_disk() {
    let current_directory = env::current_dir().expect("Could not find current_dir?");
    let test_db_directory = current_directory.join("test_load_state_from_disk_db");

    if test_db_directory.exists() {
        fs::remove_dir_all(test_db_directory.clone()).expect("Failed to delete residual test directory!?");
    }

    let config = ExtractConfig::default()
        .database_directory(Some(test_db_directory.clone()));
    
    let database: ExtractDb<String> = ExtractDb::new(config.clone());

    for i in 0..10000 {
        assert_eq!(database.push(Arc::new(format!("Id: {}", i))), true);
    }

    assert!(database.save_to_disk().is_ok());

    drop(database); // Done to conserve memory during testing

    let new_database: ExtractDb<String> = ExtractDb::new(config);
    assert_eq!(new_database.internal_count(), 0);
    assert_eq!(new_database.fetch_count(), 0);

    assert!(new_database.load_from_disk(false).is_ok());

    assert_eq!(new_database.internal_count(), 10000);
    assert_eq!(new_database.fetch_count(), 0);

    fs::remove_dir_all(test_db_directory).expect("Failed to delete residual test directory!?");
}

/// Attempt to load a corrupted state of data from disk for a ExtractDb<String>
///
/// Data is "corrupted" in the sense that some files are completely deleted.
/// This ExtractDb should be capable of recovering the remaining "uncorrupted" data.
#[test]
fn load_corrupted_state_from_disk() {
    let current_directory = env::current_dir().expect("Could not find current_dir?");
    let test_db_directory = current_directory.join("test_load_corrupted_state_from_disk_db");

    if test_db_directory.exists() {
        fs::remove_dir_all(test_db_directory.clone()).expect("Failed to delete residual test directory!?");
    }

    let config = ExtractConfig::default()
        .database_directory(Some(test_db_directory.clone()));
    
    let database: ExtractDb<String> = ExtractDb::new(config.clone());

    for i in 0..10000 {
        assert_eq!(database.push(Arc::new(format!("Id: {}", i))), true);
    }

    assert!(database.save_to_disk().is_ok());
    drop(database); // Done to conserve memory during testing

    let test_data_directory = test_db_directory.join("data");
    assert!(test_data_directory.exists());

    let mut deleted_files = 0;
    let read_dir = fs::read_dir(&test_data_directory).expect("failed to read contents of test_data_directory");
    read_dir.for_each(|potential_file| {
        if let Ok(file) = potential_file {
            if deleted_files < 5 {
                fs::remove_file(file.path()).unwrap();
                deleted_files += 1;
            }
        }
    });

    assert_eq!(deleted_files, 4 + 1);

    let new_database: ExtractDb<String> = ExtractDb::new(config);
    assert_eq!(new_database.internal_count(), 0);
    assert_eq!(new_database.fetch_count(), 0);

    assert!(new_database.load_from_disk(false).is_ok());

    assert!(new_database.internal_count() > 0);
    assert_eq!(new_database.fetch_count(), 0);

    fs::remove_dir_all(test_db_directory).expect("Failed to delete residual test directory!?");
}

/// Attempt to load data where a different shard count was used during saving. ExtractDb<u64>
#[test]
fn load_shard_mismatch_from_disk() {
    let current_directory = env::current_dir().expect("Could not find current_dir?");
    let test_db_directory = current_directory.join("test_load_shard_mismatch_from_disk_db");

    if test_db_directory.exists() {
        fs::remove_dir_all(test_db_directory.clone()).expect("Failed to delete residual test directory!?");
    }

    let config = ExtractConfig::default()
        .database_directory(Some(test_db_directory.clone()));
    
    let database: ExtractDb<u64> = ExtractDb::new(config.clone());

    for i in 0..10000 {
        assert_eq!(database.push(Arc::new(i)), true);
    }

    assert!(database.save_to_disk().is_ok());
    drop(database); // Done to conserve memory during testing

    let new_config = ExtractConfig::default()
        .shard_count(48)
        .database_directory(Some(test_db_directory.clone()));
    
    let new_database: ExtractDb<u64> = ExtractDb::new(new_config);
    assert_eq!(new_database.internal_count(), 0);
    assert_eq!(new_database.fetch_count(), 0);

    assert!(new_database.load_from_disk(false).is_err());

    assert_eq!(new_database.internal_count(), 10000);
    assert_eq!(new_database.fetch_count(), 0);

    fs::remove_dir_all(test_db_directory).expect("Failed to delete residual test directory!?");
}

/// Attempt to load miss matched ExtractDb<String> type from a ExtractDb<String>
#[test]
fn load_mismatch_type_from_disk() {
    let current_directory = env::current_dir().expect("Could not find current_dir?");
    let test_db_directory = current_directory.join("test_load_mismatch_type_from_disk_db");

    if test_db_directory.exists() {
        fs::remove_dir_all(test_db_directory.clone()).expect("Failed to delete residual test directory!?");
    }

    let config = ExtractConfig::default()
        .database_directory(Some(test_db_directory.clone()));
    
    let database: ExtractDb<u64> = ExtractDb::new(config.clone());

    for i in 0..10000 {
        assert_eq!(database.push(Arc::new(i)), true);
    }

    assert!(database.save_to_disk().is_ok());
    drop(database); // Done to conserve memory during testing

    let new_database: ExtractDb<String> = ExtractDb::new(config);

    let panic_load = panic::catch_unwind(|| new_database.load_from_disk(false));
    assert!(panic_load.is_ok());
    assert_eq!(new_database.fetch_count(), 0);

    fs::remove_dir_all(test_db_directory).expect("Failed to delete residual test directory!?");
}