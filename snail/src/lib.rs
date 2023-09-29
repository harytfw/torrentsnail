pub mod addr;
pub mod dht;
mod error;
pub mod lsd;
pub mod torrent;
pub mod tracker;
pub use error::{Error, Result};
pub mod magnet;
pub mod message;
pub mod proxy;
pub mod ratelimiter;
pub mod session;

pub mod config;
pub mod utils;

pub const SNAIL_VERSION: &str = "TorrentSnail 0.0.1";
