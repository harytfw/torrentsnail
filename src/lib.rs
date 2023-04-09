pub mod addr;
pub mod app;
pub mod bencode;
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

pub mod utils;
pub mod db;

pub const SNAIL_VERSION: &str = "TorrentSnail 0.0.1";

#[cfg(test)]
mod tests {

    #[test]
    fn bt() {
        let n: u32 = 100663295;
        println!("{:?}", n.to_be_bytes());
    }
}
