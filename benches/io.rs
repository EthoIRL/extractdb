fn main() {
    divan::main();
}

const THREADS: &[usize] = &[0];
const SHARDS: &[usize] = &[8, 16, 32, 64, 128];

const SAMPLE_COUNT: u32 = 1024;
const SAMPLE_SIZE: u32 = 1024;

#[derive(Copy, Clone, Debug)]
pub struct DatabaseConfig {
    pub shards: usize,
    pub optimistic_read: bool,
}

impl DatabaseConfig {
    fn generate_config() -> Vec<DatabaseConfig> {
        let mut configs: Vec<DatabaseConfig> = Vec::new();

        for shards in SHARDS {
            for optimistic_reads in [false, true] {
                configs.push(DatabaseConfig { shards: *shards, optimistic_read: optimistic_reads })
            }
        }

        configs
    }
}

#[divan::bench_group(threads = THREADS, sample_count = SAMPLE_COUNT, sample_size = SAMPLE_SIZE)]
mod push {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use divan::counter::ItemsCount;
    use extractdb::{ExtractConfig, ExtractDb};
    use crate::DatabaseConfig;

    #[divan::bench(args = DatabaseConfig::generate_config())]
    fn non_colliding(bencher: divan::Bencher, config: DatabaseConfig) {
        let config = ExtractConfig::default()
            .optimistic_read(config.optimistic_read)
            .shard_count(config.shards);

        let database = Arc::new(ExtractDb::new(config));
        let unique_inputs = AtomicUsize::new(0);

        bencher
            .with_inputs(|| {
                let push_input = unique_inputs.fetch_add(1, Ordering::Relaxed);
                let payload = Arc::new(format!("Test-{}", push_input));

                (database.clone(), payload)
            })
            .counter(ItemsCount::new(1u8))
            .bench_values(|(db, val)| {
                db.push(val)
            });
    }

    #[divan::bench(args = DatabaseConfig::generate_config())]
    fn colliding(bencher: divan::Bencher, config: DatabaseConfig) {
        let config = ExtractConfig::default()
            .optimistic_read(config.optimistic_read)
            .shard_count(config.shards);
        let database = Arc::new(ExtractDb::new(config));
        let payload = Arc::new(String::from("Collision"));

        bencher
            .with_inputs(|| {
                (database.clone(), payload.clone())
            })
            .counter(ItemsCount::new(1u8))
            .bench_values(|(db, val)| { 
                db.push(val)
            });
    }
}

#[divan::bench_group(threads = THREADS, sample_count = SAMPLE_COUNT, sample_size = SAMPLE_SIZE)]
mod fetch {
    use std::sync::Arc;
    use divan::counter::ItemsCount;
    use extractdb::{ExtractConfig, ExtractDb};
    use crate::SHARDS;

    #[divan::bench(args = SHARDS, threads = &[0, 1])]
    fn extraction(bencher: divan::Bencher, shard_count: usize) {
        let config = ExtractConfig::default()
            .shard_count(shard_count);
        let database = Arc::new(ExtractDb::new(config));
        let payload_length: usize = 64;

        bencher
            .with_inputs(|| {
                let payload: Arc<String> = Arc::new((0..payload_length).map(|_| fastrand::char(..)).collect());
                _ = database.push(payload);

                database.clone()
            })
            .counter(ItemsCount::new(1u8))
            .bench_values(|db| {
                _ = divan::black_box(db.fetch_next());
            });
    }

    #[divan::bench(args = SHARDS)]
    fn insertion_extraction(bencher: divan::Bencher, shard_count: usize) {
        let config = ExtractConfig::default()
            .shard_count(shard_count);
        let database = Arc::new(ExtractDb::new(config));
        let payload_length: usize = 64;

        bencher
            .with_inputs(|| {
                let payload: Arc<String> = Arc::new((0..payload_length).map(|_| fastrand::char(..)).collect());
                (database.clone(), payload)
            })
            .counter(ItemsCount::new(1u8))
            .bench_values(|(db, val)| {
                _ = divan::black_box(db.push(val));
                _ = divan::black_box(db.fetch_next());
            });
    }
}
