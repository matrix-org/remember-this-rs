use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Duration, Utc};
use flexbuffers::{FlexbufferSerializer, Reader};
use serde::{Deserialize, Serialize};
use serde_derive::{Deserialize, Serialize};

pub use crate::result::Error;

/// An entry stored to disk.
#[derive(Deserialize, Serialize)]
pub struct CacheEntry<V> {
    pub value: Arc<V>,

    /// This value may be removed after `expiration`.
    pub expiration: DateTime<Utc>,
}

/// Persisting queried values to the disk across sessions.
pub struct Cache<K, V>
where
    K: Send + Clone + Hash + Eq + for<'de> serde::Deserialize<'de> + serde::Serialize,
    V: Send + Clone + for<'de> serde::Deserialize<'de> + serde::Serialize,
{
    in_memory: Arc<RwLock<HashMap<K, CacheEntry<V>>>>,
    tree: sled::Tree,
    duration: Duration,
}
impl<K, V> Cache<K, V>
where
    K: Send + Clone + Hash + Eq + for<'de> serde::Deserialize<'de> + serde::Serialize,
    V: Send + Clone + for<'de> serde::Deserialize<'de> + serde::Serialize,
{
    pub async fn get<F, E>(&self, key: &K, thunk: F) -> Result<Arc<V>, Error<E>>
    where
        F: std::future::Future<Output = Result<V, E>>,
    {
        {
            // Fetch from in-memory cache.
            let read_lock = self.in_memory.read().unwrap();
            if let Some(found) = read_lock.get(key) {
                return Ok(found.value.clone());
            }
        }
        debug!(target: "disk-cache", "Value was NOT in memory cache");
        let expiration = Utc::now() + self.duration;

        // Prepare binary key for disk cache access.
        let mut key_serializer = FlexbufferSerializer::new();
        key.serialize(&mut key_serializer).unwrap(); // We assume that in-memory serialization always succeeds.
        let key_bin = key_serializer.take_buffer();

        {
            // Fetch from disk cache.
            if let Some(value_bin) = self.tree.get(&key_bin).map_err(Error::Database)? {
                debug!(target: "disk-cache", "Value was in disk cache");
                // Found in cache.
                let reader = Reader::get_root(&value_bin).unwrap();
                let entry = CacheEntry::<V>::deserialize(reader).unwrap(); // We assume that in-memory deserialization always succeeds.
                                                                           // In the future, we may have to be more cautious, in case of e.g. disk corruption.
                let result = entry.value;

                // Store back in memory.
                self.store_in_memory_cache(key, &result, expiration);

                // Finally, return.
                return Ok(result);
            }
        }

        // Not in cache. Unthunk `thunk`
        let data = thunk.await.map_err(Error::Client)?;
        let result = Arc::new(data);

        // Store in memory.
        self.store_in_memory_cache(key, &result, expiration);

        // Store in cache.
        self.store_in_disk_cache(&key_bin, &result, expiration)
            .map_err(Error::Database)?;

        Ok(result)
    }

    /// Store in the memory cache.
    ///
    /// Schedule a task to cleanup from memory.
    fn store_in_memory_cache(&self, key: &K, value: &Arc<V>, expiration: DateTime<Utc>) {
        debug!(target: "disk-", "Adding value to memory cache");
        let mut write_lock = self.in_memory.write().unwrap();
        let entry = CacheEntry {
            value: value.clone(),
            expiration,
        };
        write_lock.insert(key.clone(), entry);
    }

    /// Store in the memory cache.
    ///
    /// Schedule a task to cleanup from disk.
    fn store_in_disk_cache(
        &self,
        key: &[u8],
        value: &Arc<V>,
        expiration: DateTime<Utc>,
    ) -> Result<(), sled::Error> {
        debug!(target: "disk-", "Adding value to disk cache");
        let entry = CacheEntry {
            value: value.clone(),
            expiration,
        };
        let mut value_serializer = FlexbufferSerializer::new();
        entry.serialize(&mut value_serializer).unwrap();
        let entry_bin = value_serializer.take_buffer();

        self.tree.insert(key, entry_bin)?;
        Ok(())
    }
}

// Internal functions.

pub fn cache<K, V>(
    in_memory: Arc<RwLock<HashMap<K, CacheEntry<V>>>>,
    tree: sled::Tree,
    duration: Duration,
) -> Cache<K, V>
where
    K: Send + Clone + Hash + Eq + for<'de> serde::Deserialize<'de> + serde::Serialize,
    V: Send + Clone + for<'de> serde::Deserialize<'de> + serde::Serialize,
{
    Cache {
        in_memory,
        tree,
        duration,
    }
}

/// Remove all values from memory that have nothing to do here anymore.
pub fn cleanup_memory_cache<K, V>(memory_cache: &Arc<RwLock<HashMap<K, CacheEntry<V>>>>)
where
    K: Eq + Hash + Clone,
{
    let now = Utc::now();
    {
        let mut write_lock = memory_cache.write().unwrap();
        write_lock.retain(|_, v| v.expiration > now)
    }
}

/// Remove all values from disk cache that have nothing to do here anymore.
pub fn cleanup_disk_cache<K, V>(disk_cache: &mut sled::Tree)
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
    let now = Utc::now();
    let mut batch = sled::Batch::default();
    for cursor in disk_cache.iter() {
        let (k, v) = cursor.unwrap();

        let reader = Reader::get_root(&v).unwrap();
        let entry = CacheEntry::<V>::deserialize(reader).unwrap(); // We assume that in-memory deserialization always succeeds.
                                                                   // In the future, we may have to be more cautious, in case of e.g. disk corruption.
        if entry.expiration <= now {
            batch.remove(k);
        }
    }
    disk_cache.apply_batch(batch).unwrap(); // FIXME: Handle erros
}
