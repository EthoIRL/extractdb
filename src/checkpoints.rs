use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use bitcode::{Decode, Encode};
use crate::{CheckpointSettings, ExtractDb};

impl<V> ExtractDb<V>
where
    V: Eq + Hash + Clone + Send + Sync + Encode + for<'a> Decode<'a>
{
    /// Spawns a background thread to provide periodic save checkpoints onto disk.
    /// Only performs a save if a certain threshold of changes has occurred during the save interval.
    /// See `CheckpointSettings`
    ///
    /// This function is meant for long-running applications/multithreaded instances.
    ///
    /// Use `check_delay` within [`CheckpointSettings`] to determine the frequency in which change counts occur.
    ///
    /// Use `minimum_changes` within [`CheckpointSettings`] to determine the minimum amount of items inserted before a safe event occurs.
    ///
    /// Use `shutdown_flag` within [`CheckpointSettings`] to remotely shut down this thread in a safe manner. `False` = Running, `True` = Please stop
    ///
    /// # Different behavior
    /// You do not necessarily need to use this function for auto-saving. All methods used are publicly accessible and easily re-implementable. See source.
    ///
    /// # Parameters
    ///
    /// `settings`: [`CheckpointSettings`] determines the check rate and minimum change for disk saving.
    ///
    /// `db`: [`Arc<ExtractDb<V>>`] instance of [`ExtractDb`] in a shared instance
    ///
    /// # Examples
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicBool, Ordering};
    /// use extractdb::{CheckpointSettings, ExtractDb};
    ///
    /// let db: Arc<ExtractDb<u8>> = Arc::new(ExtractDb::default());
    ///
    /// let shutdown_flag = Arc::new(AtomicBool::new(false));
    /// let mut save_settings = CheckpointSettings::new(shutdown_flag.clone());
    /// save_settings.minimum_changes = 1000;
    ///
    /// // Will now check for 1000 minimum changes every 30seconds (default).
    /// ExtractDb::background_checkpoints(save_settings, db.clone());
    ///
    /// db.push(Arc::new(127));
    ///
    /// // Gracefully shutdown a background thread
    /// shutdown_flag.store(true, Ordering::Relaxed);
    /// ```
    pub fn background_checkpoints(settings: CheckpointSettings, db: Arc<ExtractDb<V>>)
    where
        V: 'static
    {
        thread::spawn(move || {
            let mut last_checkpoint_count: usize = 0;
            while !settings.shutdown_flag.load(Ordering::Relaxed) {
                thread::sleep(settings.check_delay);

                let current_change = db.internal_count();
                let changes_since_last = current_change - last_checkpoint_count;

                if changes_since_last >= settings.minimum_changes {
                    match db.save_to_disk() {
                        Ok(()) => {
                            last_checkpoint_count = current_change;
                        },
                        Err(err) => {
                            eprintln!("Database checkpoint failed. ({err})");
                        }
                    }
                }
            }

            let changes_since_last =  db.internal_count() - last_checkpoint_count;

            if changes_since_last > 0 {
                // Force a save no matter what, ensures all data is written.
                if let Err(err) = db.save_to_disk() {
                    eprintln!("Database last-minute checkpoint failed. ({err})");
                }
            }
        });
    }
}