use anyhow::Result;
use torrentsnail::{supervisor::TorrentSupervisor, torrent::HashId};
use tracing::error;

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug,torrentsnail=debug");

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_file(true)
        .with_line_number(true)
        .init();

    let bt_man = TorrentSupervisor::start().await.unwrap();
    let s = bt_man
        .download_with_info_hash(&HashId::from_hex(
            "08ada5a7a6183aae1e09d831df6748d566095a10",
        )?)
        .await
        .unwrap();
    match s.add_peer_with_addr("192.168.2.56:9000").await {
        Ok(_) => {}
        Err(err) => error!(?err),
    }
    s.start().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(60 * 60)).await;
    bt_man.shutdown().await?;
    Ok(())
}
