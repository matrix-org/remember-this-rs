extern crate env_logger;

use disk_cache::*;
use std::sync::atomic::Ordering;

/// Test that we can fill the cache and recover data
#[tokio::test]
async fn test_simple() {
    let _ = env_logger::builder().is_test(true).try_init();
    let manager =
        CacheManager::new(&ManagerOptions::builder().path("/tmp/test_simple.db").use_temporary(true).build()).unwrap();
    let cache = manager
        .cache(
            "cache_1",
            &CacheOptions::builder()
                .purge(true)
                .build(),
        )
        .unwrap();

    // Cache is initially empty.
    for i in 0..100 {
        let obtained = cache.get(&i)
            .unwrap();
        assert!(obtained.is_none(), "Cache should initially be empty");
    }

    // Fill the cache.
    for i in 0..100 {
        let was_evaluated = std::sync::atomic::AtomicBool::new(false);
        let obtained = cache
            .get_or_insert_infallible(&i, async {
                let _ = was_evaluated.store(true, Ordering::SeqCst);
                i * i
            })
            .await
            .unwrap();
        assert_eq!(
            *obtained,
            i * i,
            "The cache should return the right value when it's evaluating."
        );
        assert_eq!(
            was_evaluated.load(Ordering::SeqCst),
            true,
            "The cache should be evaluating when there is no data."
        );
    }

    // Now run again with cache filled.
    for _ in 0..3 {
        for i in 0..100 {
            let was_evaluated = std::sync::atomic::AtomicBool::new(false);
            let obtained = cache
                .get_or_insert_infallible(&i, async {
                    let _ = was_evaluated.store(true, Ordering::SeqCst);
                    i * i
                })
                .await
                .unwrap();
            assert_eq!(
                *obtained,
                i * i,
                "The cache should return the right value when it's not evaluating."
            );
            assert_eq!(
                was_evaluated.load(Ordering::SeqCst),
                false,
                "The cache should not be evaluating when there is data already."
            );

            assert_eq!(cache.get(&i).unwrap().unwrap(), obtained);
        }
    }
}


/// Test that we erase data silently in case of format change.
#[tokio::test]
async fn test_format_change() {
    let _ = env_logger::builder().is_test(true).try_init();
    let manager =
        CacheManager::new(&ManagerOptions::builder().path("/tmp/test_format_change.db").use_temporary(true).build()).unwrap();

    {
        // Open the cache as `<i32, i32>`.
        let cache = manager
            .cache(
                "cache_2",
                &CacheOptions::builder()
                    .purge(true)
                    .build(),
            )
            .unwrap();

        // Fill the cache.
        for i in 0..100 {
            cache
                .get_or_insert_infallible(&i, async {
                    i * i
                })
                .await
                .unwrap();
        }
    }

    {
        // Reopen the cache as `<i32, String>`
        let cache = manager
            .cache(
                "cache_2",
                &CacheOptions::builder()
                    .purge(true)
                    .build(),
            )
            .unwrap();

        // Re-access data
        for i in 0..100 {
            let was_evaluated = std::sync::atomic::AtomicBool::new(false);
            let found = cache
                .get_or_insert_infallible(&i, async {
                    was_evaluated.store(true, Ordering::SeqCst);
                    format!("{}", i * i)
                })
                .await
                .unwrap();
            assert_eq!(*found, format!("{}", i * i), "When silently overwriting because of format change, we get the right result");
            assert_eq!(was_evaluated.load(Ordering::SeqCst), true, "When silently overwriting because of format change, we executed the thunk");
        }
    }

    {
        // Reopen the cache as `<String, bool>`
        let cache = manager
            .cache(
                "cache_2",
                &CacheOptions::builder()
                    .purge(true)
                    .build(),
            )
            .unwrap();

        // Fill the cache.
        for i in 0..100 {
            let was_evaluated = std::sync::atomic::AtomicBool::new(false);
            let found = cache
                .get_or_insert_infallible(&i, async {
                    was_evaluated.store(true, Ordering::SeqCst);
                    i % 2 == 0
                })
                .await
                .unwrap();
            assert_eq!(*found, i % 2 == 0, "When silently overwriting because of format change, we get the right result");
            assert_eq!(was_evaluated.load(Ordering::SeqCst), true, "When silently overwriting because of format change, we executed the thunk");    
        }
    }
}


/// Test that we can reload by reopening the cache.
#[tokio::test]
async fn test_reopen() {
    let _ = env_logger::builder().is_test(true).try_init();
    let manager =
        CacheManager::new(&ManagerOptions::builder().path("/tmp/test_reopen.db").use_temporary(true).build()).unwrap();

    async fn walk_through_cache(manager: &CacheManager, options: &CacheOptions, should_execute: bool, subtest: &str) {
        eprintln!("Starting test {}", subtest);
        let cache = manager
            .cache(
                "cache_1",
                &options,
            )
            .unwrap();

        // Fill the cache.
        for i in 0..100 {
            eprintln!("{}:{}", subtest, i);
            let was_evaluated = std::sync::atomic::AtomicBool::new(false);
            let obtained = cache
                .get_or_insert_infallible(&i, async {
                    let _ = was_evaluated.store(true, Ordering::SeqCst);
                    i * i
                })
                .await
                .unwrap();
            assert_eq!(
                *obtained,
                i * i,
                "The cache should return the right value: {}.", subtest
            );
            assert_eq!(
                was_evaluated.load(Ordering::SeqCst),
                should_execute,
                "The cache should be evaluating: {}.",
                subtest
            );
        }
    }

    // Initialize cache.
    walk_through_cache(&manager, &CacheOptions::builder()
        .purge(true)
        .build(), true, "initializer").await;
    walk_through_cache(&manager, &CacheOptions::builder()
        .build(), false, "simple reopen").await;

    // Purge the cache.
    walk_through_cache(&manager, &CacheOptions::builder()
        .purge(true)
        .build(), true, "purge").await;
    walk_through_cache(&manager, &CacheOptions::builder()
        .build(), false, "reopen 2").await;

    // Change version number.
    walk_through_cache(&manager, &CacheOptions::builder()
        .version(1)
        .build(), true, "change version number").await;
    walk_through_cache(&manager, &CacheOptions::builder()
        .version(1)
        .build(), false, "reopen with same new version").await;

}
