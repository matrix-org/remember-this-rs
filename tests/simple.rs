extern crate env_logger;

use chrono::Duration;
use disk_cache::*;
use std::sync::atomic::Ordering;
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
                .duration(Duration::hours(1))
                .build(),
        )
        .unwrap();

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
                    .duration(Duration::hours(1))
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
                    .duration(Duration::hours(1))
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
                    .duration(Duration::hours(1))
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
