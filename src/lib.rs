//! An in-memory cache backed by an on-disk cache.
extern crate chrono;
extern crate flexbuffers;
#[macro_use]
extern crate log;
extern crate serde;
extern crate tokio;
#[macro_use]
extern crate typed_builder;

mod cache;
mod manager;
mod result;

pub use cache::Cache;
pub use manager::{CacheManager, CacheOptions, ManagerOptions};
pub use result::Error;
