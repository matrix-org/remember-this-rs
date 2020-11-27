![https://api.travis-ci.org/matrix-org/remember-this-rs.svg?branch=master]
![https://img.shields.io/crates/v/remember-this.svg]
![https://docs.rs/remember-this]

# A simple cache-to-disk mechanism

This crate provides a simple, zero-configuration, mechanism for caching data both in-memory and to disk.

A typical usecase is an application that needs to deal with long-running queries to the web or to a database. If the application fails for some reason or if it is a command-line application, the cache will let the application resume its next run from where it stopped.

# Example

```rust
extern crate remember_this;
#[macro_use]
extern crate tokio;

use remember_this::*;

#[tokio::main]
async fn main() {
    // A cache manager lets us organize several strongly-typed caches.
    let manager_options = CacheManagerOptions::builder()
        .path("/tmp/test_reopen.db")
        .build();
    let manager = CacheManager::new(&manager_options).unwrap();

    // An individual cache is strongly-typed.
    //
    // With the default policy, items remain 1h in memory and 1d on disk,
    // this should be sufficient for our test.
    let cache_options = CacheOptions::default();
    let cache = manager.cache("my_cache", &cache_options).unwrap();

    // Fill the cache. Imagine that it's actually a long computation.
    //
    // Recall that async blocks are computed lazily in Rust, so `async { i * i }`
    // will only execute if it's not in the cache yet.
    for i in 0..100 {
        let obtained = cache.get_or_insert_infallible(&i, async { i * i }).await.unwrap();
        assert_eq!(*obtained, i * i);
    }

    // Let's refetch from the cache.
    for i in 0..100 {
        // Here, the async blocks are actually not executed.
        let obtained = cache.get_or_insert_infallible(&i, async { panic!("We shouldn't reach this point"); }).await.unwrap();
        assert_eq!(*obtained, i * i);
    }
}
```

# A few notes and features

- Individual caches can have a format version number. This way, if your data format has changed in subtle ways, by increasing the version number, you can be sure that you're not going to read "wrong" data from the disk.
- The storage format is flexbuffers on sled. This gives us a degree of dynamic typing while we load from disk, which decreases the chances of your application accidentally reading from an incorrect version of your format.

# TODO

- For the moment, the on-disk LRU is not updated when data is fetched from memory.
- For the moment, the in-memory LRU is not size-limited.
