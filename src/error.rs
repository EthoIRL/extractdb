use std::num::ParseIntError;
use concurrent_queue::{PopError, PushError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FetchError<V>
{
    /// Failed to load shard internal data into removal store queue 
    #[error(transparent)]
    Push(#[from] PushError<V>),

    /// Failed to pop data from internal removal_store
    #[error(transparent)]
    Pop(#[from] PopError),
}

#[derive(Error, Debug)]
pub enum SaveError {
    /// Failed to lock data_store or disk_queue from shard 
    #[error("Failed to read lock [Shard {0}]")]
    ShardLock(usize),

    /// Failed to create or truncate preexisting file from shard
    #[error("Failed to create/truncate file [Shard {0}, ({1})]")]
    CreateTruncate(usize, std::io::Error),

    /// Failed to write to file from shard
    #[error("Failed to write to file [Shard {0}, ({1})]")]
    Write(usize, std::io::Error),

    /// Failed to flush to file form shard
    #[error("Failed to flush file [Shard {0}, ({1})]")]
    Flush(usize, std::io::Error),

    /// Database directory was not set during ExtractDb initialization.
    #[error("No database directory is set. Cannot save to disk without a valid path set!")]
    NoDirectory,

    /// std::io::Error occurred during saving 
    #[error(transparent)]
    Io(#[from] std::io::Error)
}

#[derive(Error, Debug)]
pub enum LoadError {
    /// Database directory was not set during ExtractDb initialization
    #[error("No database directory is set. Cannot save to disk without a valid path set!")]
    NoDirectory,

    /// Failed to locate the file at it's specified location
    #[error("Could not locate file entry")]
    MissingEntry,

    /// Database has files missing in either the store or data directories
    #[error("No files are present in the database directory.")]
    MissingFiles,

    /// Failed to open file  
    #[error("Failed to open file. Skipping ({0})")]
    Open(std::io::Error),

    /// Failed to read file 
    #[error("Failed to read file. Skipping ({0})")]
    Read(std::io::Error),

    /// Failed to decode file shard data
    #[error("Failed to decode shard file. Skipping ({0})")]
    Decode(bitcode::Error),

    /// Failed to extract filename from shard file
    #[error("Failed to get file_name. Skipping ({0})")]
    Filename(String),

    /// Failed to convert filename to shard number
    #[error("Failed to convert string to number. Skipping (File: {0}, Err: {1})")]
    ShardName(String, ParseIntError),

    /// Soft error: This can be treated as a warning rather than an error
    /// Store or data is miss-matched in save shard size. This is recoverable to a degree.
    #[error("Soft error: Shard store/data miss-match, converting to current shard_size")]
    ShardMismatch,

    /// File contains no data
    #[error("Missing or no data present within file. Skipping ({0})")]
    NoData(String),

    /// Failed to extract shard id from file name
    #[error("Failed to extract shard number from file. Skipping ({0})")]
    ShardIdExtraction(String)
}
