pub mod bencode;
pub mod bt;
pub mod dht;
mod error;
pub mod lsd;
pub mod torrent;
pub mod tracker;
pub mod addr;
pub use error::{Error, Result};
pub(crate) mod utils;

pub const SNAIL_VERSION: &str = "TorrentSnail 0.0.1";

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    

    #[test]
    fn bt() {
        let n: u32 = 100663295;
        println!("{:?}", n.to_be_bytes());
    }
}
