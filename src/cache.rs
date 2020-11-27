use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Duration, Utc};
use flexbuffers::{FlexbufferSerializer, Reader};
use serde::Serialize;

pub use crate::result::Error;

/// An entry stored to disk.
pub struct CacheEntry<V> {
    pub value: Arc<V>,

    /// A number of seconds since the epoch
    ///
    /// This value may be removed after `expiration`.
    pub expiration: AtomicU64,
}

/// Persisting queried values to the disk across sessions.
pub struct Cache<K, V>
where
    K: Send + Clone + Hash + Eq + for<'de> serde::Deserialize<'de> + serde::Serialize,
    V: Send + Clone + for<'de> serde::Deserialize<'de> + serde::Serialize,
{
    pub(crate) in_memory: Arc<RwLock<HashMap<K, CacheEntry<V>>>>,

    /// The data cached to the disk as a K -> V mapping.
    pub(crate) content: sled::Tree,

    /// The expiration dates as a seconds: u64 -> K mapping.
    pub(crate) expiry: sled::Tree,

    /// How long data should remain in-memory.
    pub(crate) memory_duration: Duration,

    /// How long data should remain on-disk.
    pub(crate) disk_duration: Duration,
}
impl<K, V> Cache<K, V>
where
    K: Send + Clone + Hash + Eq + for<'de> serde::Deserialize<'de> + serde::Serialize,
    V: Send + Clone + for<'de> serde::Deserialize<'de> + serde::Serialize,
{
    /// Get a value from the cache.
    ///
    /// If this value is not in the cache, compute the thunk and insert the value.
    pub async fn get_or_insert_infallible<F>(&self, key: &K, thunk: F) -> Result<Arc<V>, Error<()>>
    where
        F: std::future::Future<Output = V>,
    {
        self.get_or_insert::<_, ()>(key, async { Ok(thunk.await) })
            .await
    }

    /// Get a value from the cache.
    ///
    /// If this value is not in the cache, compute the thunk and insert the value.
    pub async fn get_or_insert<F, E>(&self, key: &K, thunk: F) -> Result<Arc<V>, Error<E>>
    where
        F: std::future::Future<Output = Result<V, E>>,
    {
        let memory_expiration = Utc::now() + self.memory_duration;

        // Prepare binary key for disk cache access.
        let mut key_serializer = FlexbufferSerializer::new();
        key.serialize(&mut key_serializer).unwrap(); // We assume that in-memory serialization always succeeds.
        let key_bin = key_serializer.take_buffer();

        match self.get_at(key, &key_bin, memory_expiration) {
            Ok(Some(found)) => return Ok(found),
            Ok(None) => {}
            Err(Error::Database(err)) => return Err(Error::Database(err)),
            Err(e) => panic!("We shouldn't have any other error here {:?}", e),
        }

        // Not in cache. Unthunk `thunk`
        let data = thunk.await.map_err(Error::Client)?;
        let result = Arc::new(data);

        // Store in memory.
        self.store_in_memory_cache(key, &result, memory_expiration);

        let disk_expiration = Utc::now() + self.disk_duration;

        // Store in cache.
        self.store_in_disk_cache(&key_bin, &result, disk_expiration)
            .map_err(Error::Database)?;

        Ok(result)
    }

    /// Get a value from the cache.
    pub fn get(&self, key: &K) -> Result<Option<Arc<V>>, Error<()>> {
        // Prepare binary key for disk cache access.
        let mut key_serializer = FlexbufferSerializer::new();
        key.serialize(&mut key_serializer).unwrap(); // We assume that in-memory serialization always succeeds.
        let key_bin = key_serializer.take_buffer();

        self.get_at(key, &key_bin, Utc::now() + self.memory_duration)
    }
    fn get_at(
        &self,
        key: &K,
        key_bin: &[u8],
        memory_expiration: DateTime<Utc>,
    ) -> Result<Option<Arc<V>>, Error<()>> {
        {
            // Fetch from in-memory cache.
            let read_lock = self.in_memory.read().unwrap();
            if let Some(found) = read_lock.get(key) {
                found
                    .expiration
                    .store(memory_expiration.timestamp() as u64, Ordering::Relaxed);
                // FIXME: Postpone expiry on disk.
                return Ok(Some(found.value.clone()));
            }
        }
        debug!(target: "disk-cache", "Value not found in memory");

        {
            // Fetch from disk cache.
            if let Some(value_bin) = self.content.get(&key_bin).map_err(Error::Database)? {
                debug!(target: "disk-cache", "Value was in disk cache");
                // Found in cache.
                let reader = Reader::get_root(&value_bin).unwrap();
                if let Ok(value) = V::deserialize(reader) {
                    debug!(target: "disk-cache", "Value deserialized");

                    let result = Arc::new(value);

                    // Store back in memory.
                    self.store_in_memory_cache(key, &result, memory_expiration);

                    // FIXME: Postpone expiration on disk

                    // Finally, return.
                    return Ok(Some(result));
                }

                // If we reach this stage, deserialization failed, either because of disk corruption (unlikely)
                // or because the format has changed (more likely). In either case, ignore and overwrite data.
            }
        }

        debug!(target: "disk-cache", "Value not found on disk");
        Ok(None)
    }

    /// Store in the memory cache.
    ///
    /// Schedule a task to cleanup from memory.
    fn store_in_memory_cache(&self, key: &K, value: &Arc<V>, expiration: DateTime<Utc>) {
        debug!(target: "disk-cache", "Adding value to memory cache");
        let mut write_lock = self.in_memory.write().unwrap();
        let entry = CacheEntry {
            value: value.clone(),
            expiration: AtomicU64::new(expiration.timestamp() as u64),
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
        let mut value_serializer = FlexbufferSerializer::new();
        value.serialize(&mut value_serializer).unwrap();
        let entry_bin = value_serializer.take_buffer();

        self.content.insert(key, entry_bin)?;
        self.expiry
            .insert(u64_to_bytes(expiration.timestamp() as u64), key)?;
        Ok(())
    }

    pub fn cleanup_expired_from_memory_cache(&self) {
        cleanup_memory_cache(&self.in_memory)
    }

    pub fn cleanup_expired_disk_cache(&self) {
        cleanup_disk_cache::<K, V>(&self.expiry, &self.content)
    }
}

// Internal functions.

/// Remove all values from memory that have nothing to do here anymore.
pub fn cleanup_memory_cache<K, V>(memory_cache: &Arc<RwLock<HashMap<K, CacheEntry<V>>>>)
where
    K: Eq + Hash + Clone,
{
    let now = Utc::now().timestamp() as u64;
    {
        let mut write_lock = memory_cache.write().unwrap();
        write_lock.retain(|_, v| v.expiration.load(Ordering::Relaxed) > now)
    }
}

/// Remove all values from disk cache that have nothing to do here anymore.
pub fn cleanup_disk_cache<K, V>(expiry: &sled::Tree, content: &sled::Tree)
where
    K: Send + Clone + Hash + Eq + for<'de> serde::Deserialize<'de> + serde::Serialize,
{
    let now = Utc::now();
    let mut batch = sled::Batch::default();
    for cursor in expiry.range(u64_to_bytes(0)..u64_to_bytes(now.timestamp() as u64)) {
        let (ts, k) = cursor.unwrap(); // FIXME: Handle errors
        debug_assert!(bytes_to_u64(&ts) <= now.timestamp() as u64);
        batch.remove(k);
    }
    content.apply_batch(batch).unwrap(); // FIXME: Handle errors
}

fn bytes_to_u64(bytes: &[u8]) -> u64 {
    ((bytes[0] as u64) << 56)
        + ((bytes[1] as u64) << 48)
        + ((bytes[2] as u64) << 40)
        + ((bytes[3] as u64) << 32)
        + ((bytes[4] as u64) << 24)
        + ((bytes[5] as u64) << 16)
        + ((bytes[6] as u64) << 8)
        + bytes[7] as u64
}

fn u64_to_bytes(value: u64) -> [u8; 8] {
    [
        ((value >> 56) & 0b11111111) as u8,
        ((value >> 48) & 0b11111111) as u8,
        ((value >> 40) & 0b11111111) as u8,
        ((value >> 32) & 0b11111111) as u8,
        ((value >> 24) & 0b11111111) as u8,
        ((value >> 16) & 0b11111111) as u8,
        ((value >> 8) & 0b11111111) as u8,
        (value % 256) as u8,
    ]
}

#[test]
fn test_bytes_to_u64() {
    let mut i: u128 = 0;
    while i <= std::u64::MAX as u128 {
        let bytes = u64_to_bytes(i as u64);
        let num = bytes_to_u64(&bytes);
        assert_eq!(num, i as u64);
        i = (i + 1) * 7;
    }
}
