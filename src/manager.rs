use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, RwLock};

use chrono::Duration;

use crate::cache::{self, CacheEntry};
pub use crate::Cache;

const TREE_META: &[u8] = b":meta:";
const KEY_FORMAT: &[u8] = b"format";
const KEY_VERSION: &[u8] = b"version";
const VALUE_FORMAT: &[u8] = b"disk-cache";
const VALUE_VERSION: &[u8] = &[0, 1, 0];

/// An object managing several caches.
pub struct CacheManager {
    /// The database to which cache data is written
    db: sled::Db,
}
impl CacheManager {
    /// Create a new cache manager.
    pub fn new(options: &ManagerOptions) -> Result<Self, sled::Error> {
        // Attempt to open the cache database.
        let config = sled::Config::default()
            .path(&options.path)
            .mode(sled::Mode::HighThroughput)
            .flush_every_ms(Some(10_000))
            .use_compression(options.use_compression);
        let db = match config.open() {
            Ok(db) => db,
            Err(sled::Error::Corruption { .. }) => {
                warn!(target: "disk-cache", "Cache file corrupted, recreating");
                // Erase database and reopen.
                let _ = std::fs::remove_dir_all(&options.path);
                config.create_new(true).open()?
            }
            other => other?,
        };

        // If metadata is absent or incorrect, drop cache and recreate.
        let meta_tree = db.open_tree(TREE_META)?;
        let is_correct_format = meta_tree
            .get(KEY_FORMAT)?
            .map(|format| format == VALUE_FORMAT)
            .unwrap_or(false);
        let is_correct_version = meta_tree
            .get(KEY_VERSION)?
            .map(|version| version == VALUE_VERSION)
            .unwrap_or(false);

        if !is_correct_format || !is_correct_version {
            for tree in db.tree_names() {
                db.drop_tree(tree)?;
            }
        }
        let meta_tree = db.open_tree(":meta:")?;
        meta_tree.insert(KEY_FORMAT, VALUE_FORMAT)?;
        meta_tree.insert(KEY_VERSION, VALUE_VERSION)?;

        Ok(CacheManager { db })
    }

    /// Compute cache name
    fn get_cache_name(name: &str) -> String {
        format!("cache:{}", name)
    }

    /// Remove a cache.
    pub fn purge(&self, name: &str) -> sled::Result<bool> {
        self.db.drop_tree(Self::get_cache_name(name))
    }

    /// Instantiate a new cache for a specific type.
    pub fn cache<K, V>(&self, name: &str, options: &CacheOptions) -> sled::Result<Cache<K, V>>
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
        let tree = self.db.open_tree(Self::get_cache_name(name))?;

        // Setup interval cleanup.
        let cleanup_duration =
            tokio::time::Duration::from_secs(options.duration.num_seconds() as u64);
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

        Ok(cache::cache(in_memory, tree, options.duration))
    }
}

/// Options for the CacheManager.
#[derive(TypedBuilder)]
pub struct ManagerOptions {
    /// The path where the cache should be stored.
    path: std::path::PathBuf,

    /// If `true`, use compression.
    use_compression: bool,
}

#[derive(TypedBuilder)]
pub struct CacheOptions {
    /// How long data should stay on disk/in memory.
    duration: Duration,
}
