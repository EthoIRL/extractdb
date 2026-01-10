use std::fs;
use std::fs::File;
use std::hash::{BuildHasher, Hash};
use std::io::{Read, Write};
use std::str::FromStr;
use std::sync::Arc;
use bitcode::{Decode, Encode};
use chrono::Utc;
use rayon::iter::{ParallelIterator, IndexedParallelIterator, IntoParallelRefIterator, ParallelBridge, IntoParallelIterator};
use crate::{ExtractDb, LoadError, SaveError};

impl<V> ExtractDb<V>
where
    V: Eq + Hash + Clone + Send + Sync + Encode + for<'a> Decode<'a>
{
    /// Saves all internal shard data into a serialized database directory.
    ///
    /// # Layout
    /// - Store:
    ///     Holds the hash's for all known items within the database. This is crucial for maintaining item uniqueness.
    ///     This is overwritten every save.
    /// - Data:
    ///     `Arc<V>` data is held within this subdirectory.
    ///     This is appended to disk every save (0-XXXXXXXXXX..(SHARD_COUNT-1)-XXXXXXXXXX). Files are formatted with UTC Timestamp seconds to avoid collision.
    ///
    /// # Errors
    /// [`SaveError`] may return if database directory is not set or if saving fails. See [`SaveError`] doc for more info.
    pub fn save_to_disk(&self) -> Result<(), SaveError> {
        let Some(database_directory) = &self.config.database_directory else {
            return Err(SaveError::NoDirectory)
        };

        let store_directory = database_directory.join("store");
        let data_directory = database_directory.join("data");

        if !database_directory.exists() {
            fs::create_dir_all(database_directory)?;
        }

        if !store_directory.exists() {
            fs::create_dir_all(&store_directory)?;
        }

        if !data_directory.exists() {
            fs::create_dir_all(&data_directory)?;
        }

        let store_results = self.shards
            .par_iter()
            .enumerate()
            .try_for_each(|(id, shard)| -> Result<(), SaveError> {
                let store_shard = shard.data_store
                    .read()
                    .map_err(|_| SaveError::ShardLock(id))?;

                let internal_data: Vec<u64> = store_shard.clone().into_iter().collect();
                let encoded_data = bitcode::encode(&internal_data);

                let file_shard_path = &store_directory.join(format!("{id}"));

                let mut file_shard = File::create(file_shard_path)
                    .map_err(|err| SaveError::CreateTruncate(id, err))?;

                file_shard
                    .write_all(&encoded_data)
                    .map_err(|err| SaveError::Write(id, err))?;

                file_shard
                    .flush()
                    .map_err(|err| SaveError::Flush(id, err))?;

                Ok(())
            });

        let disk_results = self.shards
            .par_iter()
            .enumerate()
            .try_for_each(|(id, shard)| -> Result<(), SaveError> {
                let mut data_shard = shard.disk_queue
                    .write()
                    .map_err(|_| SaveError::ShardLock(id))?;

                let internal_data: Vec<Arc<V>> = data_shard
                    .drain(..)
                    .collect();

                if internal_data.is_empty() {
                    return Ok(());
                }

                let encoded_data = bitcode::encode(&internal_data);

                let file_shard_path = &data_directory.join(format!("{id}-{}", Utc::now().timestamp()));

                let mut file_shard = File::create(file_shard_path)
                    .map_err(|err| SaveError::CreateTruncate(id, err))?;

                file_shard
                    .write_all(&encoded_data)
                    .map_err(|err| SaveError::Write(id, err))?;

                file_shard
                    .flush()
                    .map_err(|err| SaveError::Flush(id, err))?;

                Ok(())
            });

        store_results?;
        disk_results?;

        Ok(())
    }

    /// Loads all shard-files back into internal memory
    ///
    /// During a failure/corruption event all non-corrupted data will be loaded into memory before an error is omitted out.
    ///
    /// # Arguments
    /// `re_enqueue`: Loads all data back into fetch queue.
    ///
    /// # Errors
    ///
    /// [`LoadError] may return if any form of corruption occurs, or if a shard size changes. See [`LoadError`] doc for more info.
    /// **Missing any store files will be considered fully corrupted. While data files will be recovered to the best of its ability without an error.**
    pub fn load_from_disk(&self, re_enqueue: bool) -> Result<(), LoadError> {
        let Some(database_directory) = &self.config.database_directory else {
            return Err(LoadError::NoDirectory)
        };

        let store_directory = database_directory.join("store");
        let data_directory = database_directory.join("data");

        if !database_directory.exists() {
            return Ok(());
        }

        if !store_directory.exists() {
            return Ok(())
        }

        if !data_directory.exists() {
            return Ok(())
        }

        let Ok(store_files) = fs::read_dir(&store_directory) else {
            return Err(LoadError::MissingFiles);
        };

        let Ok(data_files) = fs::read_dir(&data_directory) else {
            return Err(LoadError::MissingFiles);
        };

        let shard_mismatch: bool = {
            let store_mismatch = match fs::read_dir(store_directory) {
                Ok(files) => files.count() != self.config.shard_count,
                Err(_) => true
            };

            let data_mismatch = match fs::read_dir(data_directory) {
                Ok(mut files) => {
                    files.any(|file_result| {
                        if let Ok(file) = file_result {
                            let file_name = match file.file_name().to_str() {
                                Some(data) => data.to_string(),
                                None => return true
                            };

                            let shard_file_name = match file_name
                                .split_once("-")
                                .map(|(before, _)| before) {
                                Some(before) => before,
                                None => return true
                            };

                            let shard_id = match usize::from_str(&shard_file_name) {
                                Ok(id) => id,
                                Err(_) => return true
                            };

                            if shard_id >= self.config.shard_count {
                                return true
                            }

                            return false;
                        }

                        true
                    })
                }
                Err(_) => true
            };

            store_mismatch || data_mismatch
        };

        let store_load_results: Vec<Result<(), LoadError>> = store_files
            .par_bridge()
            .map(|potential_file| -> Result<(), LoadError> {
                let file_entry = potential_file
                    .map_err(|_| LoadError::MissingEntry)?;

                let mut file = File::open(file_entry.path())
                    .map_err(|err| LoadError::Open(err))?;
                let mut file_data: Vec<u8> = Vec::new();

                let size = file.read_to_end(&mut file_data)
                    .map_err(|err| LoadError::Read(err))?;

                if size == 0 {
                    return Ok(());
                }

                let decoded_shard_data: Vec<u64> = bitcode::decode(&file_data)
                    .map_err(|err| LoadError::Decode(err))?;

                let file_name = match file_entry.file_name().to_str() {
                    Some(data) => data.to_string(),
                    None => {
                        return Err(LoadError::Filename(file_entry.path().display().to_string()));
                    }
                };

                let shard_id = usize::from_str(&file_name)
                    .map_err(|err| LoadError::ShardName(file_entry.path().display().to_string(), err))?;

                if shard_mismatch {
                    decoded_shard_data.into_par_iter().for_each(|item| {
                        let shard_index = item % self.config.shard_count as u64;

                        if let Ok(mut data_shard) = self.shards[shard_index as usize].data_store.write() {
                            if !data_shard.insert(item) {
                                return;
                            }
                        }
                    });

                    return Err(LoadError::ShardMismatch);
                }

                if let Ok(mut data_shard) = self.shards[shard_id].data_store.write() {
                    for decoded_datum in decoded_shard_data {
                        data_shard.insert(decoded_datum);
                    }
                }

                Ok(())
            }).collect();

        if re_enqueue {
            let data_load_results: Vec<Result<(), LoadError>> = data_files
                .par_bridge()
                .map(|potential_file| -> Result<(), LoadError> {
                    let file_entry = potential_file
                        .map_err(|_| LoadError::MissingEntry)?;

                    let mut file = File::open(file_entry.path())
                        .map_err(|err| LoadError::Open(err))?;
                    let mut file_data: Vec<u8> = Vec::new();

                    let size = file.read_to_end(&mut file_data)
                        .map_err(|err| LoadError::Read(err))?;

                    if size == 0 {
                        return Err(LoadError::NoData(file_entry.path().display().to_string()));
                    }

                    let decoded_shard_data: Vec<V> = bitcode::decode(&file_data)
                        .map_err(|err| LoadError::Decode(err))?;

                    let file_name = match file_entry.file_name().to_str() {
                        Some(data) => data.to_string(),
                        None => {
                            return Err(LoadError::Filename(file_entry.path().display().to_string()));
                        }
                    };

                    let shard_file_name = file_name.split_once("-")
                        .map(|(before, _)| before)
                        .ok_or_else(|| LoadError::ShardIdExtraction(file_entry.path().display().to_string()))?;

                    let shard_id = usize::from_str(&shard_file_name)
                        .map_err(|err| LoadError::ShardName(file_entry.path().display().to_string(), err))?;

                    if shard_mismatch {
                        for decoded_datum in decoded_shard_data {
                            let datum = Arc::new(decoded_datum);

                            let hash = self.data_hasher.hash_one(&datum);
                            let new_shard_index = hash & self.shard_mask;

                            if let Ok(mut queue) = self.shards[new_shard_index as usize].insertion_queue.write() {
                                queue.push(datum);
                            }
                        }

                        return Err(LoadError::ShardMismatch);
                    }

                    for decoded_datum in decoded_shard_data {
                        let datum = Arc::new(decoded_datum);

                        if let Ok(mut queue) = self.shards[shard_id].insertion_queue.write() {
                            queue.push(datum);
                        }
                    }

                    Ok(())
                }).collect();

            for load_result in data_load_results {
                load_result?;
            }
        }

        for load_result in store_load_results {
            load_result?;
        }

        Ok(())
    }
}