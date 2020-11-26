extern crate env_logger;

use chrono::Duration;
use disk_cache::*;
use std::sync::atomic::Ordering;
#[tokio::test]
async fn test_simple() {
    env_logger::init();
    let manager = CacheManager::new(
        &ManagerOptions::builder()
            .path("/tmp/test.db")
            .build()
    ).unwrap();
    let cache = manager.cache("cache_1", &CacheOptions::builder().purge(true).duration(Duration::hours(1)).build())
        .unwrap();

    // Fill the cache.
    for i in 0..100 {
        let was_evaluated = std::sync::atomic::AtomicBool::new(false);
        let obtained = cache.get_or_insert_infallible(&i, async {
            let _ = was_evaluated.store(true, Ordering::SeqCst);
            i * i
        } )
            .await
            .unwrap();
        assert_eq!(*obtained, i * i, "The cache should return the right value when it's evaluating.");
        assert_eq!(was_evaluated.load(Ordering::SeqCst), true, "The cache should be evaluating when there is no data.");
    }

    // Now run again with cache filled.
    for _ in 0..3 {
        for i in 0..100 {
            let was_evaluated = std::sync::atomic::AtomicBool::new(false);
            let obtained = cache.get_or_insert_infallible(&i, async {
                let _ = was_evaluated.store(true, Ordering::SeqCst);
                i * i
            } )
                .await
                .unwrap();
            assert_eq!(*obtained, i * i, "The cache should return the right value when it's not evaluating.");
            assert_eq!(was_evaluated.load(Ordering::SeqCst), false, "The cache should not be evaluating when there is data already.");
        }    
    }
}