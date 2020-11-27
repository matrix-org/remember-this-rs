//! A simple tool designed to allow our code to remember results on disk.

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
pub use manager::{CacheManager, CacheManagerOptions, CacheOptions};
pub use result::Error;
