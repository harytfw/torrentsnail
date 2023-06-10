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

pub mod config;
pub mod db;
pub mod utils;

use tokio::signal;

use app::Application;

use tracing_subscriber;


pub const SNAIL_VERSION: &str = "TorrentSnail 0.0.1";

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "torrentsnail=debug");

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_file(true)
        .with_line_number(true)
        .init();
    let app = Application::start().await.unwrap();

    signal::ctrl_c().await.expect("failed to listen for event");

    app.shutdown().await.unwrap();

    Ok(())
}
