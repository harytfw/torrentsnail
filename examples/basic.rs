use anyhow::Result;
use torrentsnail::bt::BT;

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "torrentsnail=debug");
    println!("{}", std::env::var("RUST_LOG").unwrap());

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_line_number(true)
        .init();

    let bt_man = BT::start().await?;

    tokio::time::sleep(std::time::Duration::from_secs(60 * 60)).await;
    bt_man.shutdown().await?;
    Ok(())
}
