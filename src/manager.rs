use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, RwLock};

use chrono::Duration;

use crate::cache::{self, CacheEntry};
pub use crate::Cache;

const TREE_META: &[u8] = b":meta:";
const KEY_FORMAT: &[u8] = b"format";
const KEY_FORMAT_VERSION: &[u8] = b"version";
const VALUE_FORMAT: &[u8] = b"disk-cache";
const VALUE_FORMAT_VERSION: &[u8] = &[0, 1, 0];

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
            .use_compression(options.use_compression)
            .temporary(options.use_temporary);
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
            .get(KEY_FORMAT_VERSION)?
            .map(|version| version == VALUE_FORMAT_VERSION)
            .unwrap_or(false);

        debug!(target: "disk-cache", "is_correct_format: {}", is_correct_format);
        debug!(target: "disk-cache", "is_correct_version: {}", is_correct_version);

        if !is_correct_format || !is_correct_version {
            for tree in db.tree_names() {
                debug!(target: "disk-cache", "dropping tree: {:?}", tree);
                db.drop_tree(tree).or_else(|e| match e {
                    sled::Error::Unsupported(_) =>
                    /* Attempting to remove a core structure, skip */
                    {
                        Ok(false)
                    }
                    other => Err(other),
                })?;
            }
        }
        let meta_tree = db.open_tree(":meta:")?;
        meta_tree.insert(KEY_FORMAT, VALUE_FORMAT)?;
        meta_tree.insert(KEY_FORMAT_VERSION, VALUE_FORMAT_VERSION)?;

        Ok(CacheManager { db })
    }

    /// Return the internal name of the tree representing this cache.
    fn get_cache_name(name: &str) -> String {
        format!("cache:{}", name)
    }

    /// Return the internal name of the tree representing metadata for this cache.
    fn get_meta_name(name: &str) -> String {
        format!("meta:{}", name)
    }


    /// Remove a cache.
    pub fn purge(&self, name: &str) -> sled::Result<bool> {
        let cache = self.db.drop_tree(Self::get_cache_name(name))?;
        let meta = self.db.drop_tree(Self::get_meta_name(name))?;
        Ok(cache || meta)
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
        // Managing metadata.
        let key = Self::get_cache_name(name);
        let meta_key = Self::get_meta_name(name);
        let version = [(options.version & 0xFF) as u8, ((options.version >> 8) & 0xFF) as u8, ((options.version >> 16) & 0xFF) as u8, ((options.version >> 24) & 0xFF) as u8];
        let format_changed = self.db.open_tree(&meta_key)?
            .get(KEY_FORMAT_VERSION)?
            .map(|k| {
                debug!(target: "disk-cache", "Cache version: {:?}, expected {:?}", k.as_ref(), version);
                k.as_ref() != version
            })
            .unwrap_or(true);

        if format_changed || options.purge {
            debug!(target: "disk-cache", "We need to cleanup this cache - format_changed:{} options.purge:{}", format_changed, options.purge);
            self.db.drop_tree(&key)?;
        }
        self.db.open_tree(meta_key)?
            .insert(KEY_FORMAT_VERSION, &version)?;

        // Now actually open data.
        let in_memory: Arc<RwLock<HashMap<K, CacheEntry<V>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let tree = self.db.open_tree(key)?;

        // Setup interval cleanup.
        let initial_cleanup_start = tokio::time::Instant::now()
            + tokio::time::Duration::from_secs(
                options.initial_disk_cleanup_after.num_seconds() as u64
            );
        let cleanup_duration =
            tokio::time::Duration::from_secs(options.duration.num_seconds() as u64);
        let cleanup_memory = in_memory.clone();
        let mut cleanup_tree = tree.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval_at(initial_cleanup_start, cleanup_duration);
            loop {
                let _ = interval.tick().await;
                cache::cleanup_disk_cache::<K, V>(&mut cleanup_tree);

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
    #[builder(setter(into))]
    path: std::path::PathBuf,

    /// If `true`, use compression.
    ///
    /// By default, false.
    #[builder(default = false)]
    use_compression: bool,

    /// If `true`, drop database once the `CacheManager` is dropped.
    ///
    /// Useful mostly for testing.
    ///
    /// By default, false.
    #[builder(default = false)]
    use_temporary: bool,
}

#[derive(TypedBuilder)]
pub struct CacheOptions {
    /// How long data should stay on disk/in memory.
    duration: Duration,

    /// How long to wait before cleaning up data that is already on disk.
    ///
    /// If unspecified, 10 seconds.
    #[builder(default=Duration::seconds(10))]
    initial_disk_cleanup_after: Duration,

    /// If `true`, erase the cache without attempting to reload it.
    ///
    /// Used mostly for testing.
    #[builder(default = false)]
    purge: bool,

    /// Increment this if you have changed the format of the cache and wish
    /// to erase its contents.
    #[builder(default = 0)]
    version: u32,
}
impl Default for CacheOptions {
    fn default() -> Self {
        CacheOptions {
            duration: Duration::days(1),
            initial_disk_cleanup_after: Duration::seconds(10),
            purge: false,
            version: 0,
        }
    }
}
