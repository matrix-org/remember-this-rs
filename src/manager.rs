use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, RwLock};

use chrono::{Duration};

use crate::cache::{self, CacheEntry};
pub use crate::Cache;

/// An object managing several caches.
pub struct CacheManager {
    /// The database to which cache data is written
    db: sled::Db,
}
impl CacheManager {
    /// Create a new cache manager.
    pub fn new(path: &std::path::Path) -> Result<Self, sled::Error> {
        // Attempt to open the cache database.
        let config = sled::Config::default()
            .path(path)
            .mode(sled::Mode::HighThroughput)
            .cache_capacity(10_000_000)
            .flush_every_ms(Some(10_000));
        let db = match config.open() {
            Ok(db) => db,
            Err(sled::Error::Corruption { .. }) => {
                warn!(target: "disk-cache", "Cache file corrupted, recreating");
                // Erase database and reopen.
                let _ = std::fs::remove_dir_all(path);
                config.create_new(true).open()?
            }
            other => other?
        };


        Ok(CacheManager { db })
    }

    /// Remove a cache.
    pub fn purge(&self, name: &str) -> sled::Result<bool>
    {
        self.db.drop_tree(name)
    }

    /// Instantiate a new cache for a specific type.
    pub fn cache<K, V>(&self, name: &str, duration: Duration) -> sled::Result<Cache<K, V>>
    where
        K: Send
            + Clone
            + Hash
            + Eq
            + for<'de> serde::Deserialize<'de>
            + serde::Serialize
            + Sync
            + 'static,
        V: Send + Clone + for<'de> serde::Deserialize<'de> + serde::Serialize + Sync + 'static,
    {
        let in_memory: Arc<RwLock<HashMap<K, CacheEntry<V>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let tree = self.db.open_tree(name)?;

        // Regular cleanup.
        let cleanup_duration = tokio::time::Duration::from_secs(duration.num_seconds() as u64);
        let cleanup_memory = in_memory.clone();
        let mut cleanup_tree = tree.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(cleanup_duration);
            loop {
                // Immediately cleanup disk cache.
                cache::cleanup_disk_cache::<K, V>(&mut cleanup_tree);

                let _ = interval.tick().await;

                if Arc::strong_count(&cleanup_memory) == 1 {
                    // We're the last owner, time to stop.
                    return;
                }

                // Cleanup in-memory
                cache::cleanup_memory_cache(&cleanup_memory);
            }
        });

        Ok(cache::cache(
            in_memory,
            tree,
            duration,
        ))
    }
}
