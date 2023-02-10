

use anyhow::Result;
use torrentsnail::{bt::BT, torrent::HashId};

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "torrentsnail=debug");

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_file(true)
        .with_line_number(true)
        .init();

    let bt_man = BT::start().await?;
    let s = bt_man
        .download_with_info_hash(&HashId::from_hex(
            "08ada5a7a6183aae1e09d831df6748d566095a10",
        )?)
        .await?;
    s.add_peer_with_addr("192.168.2.56:9000").await?;
    tokio::time::sleep(std::time::Duration::from_secs(60 * 60)).await;
    bt_man.shutdown().await?;
    Ok(())
}
