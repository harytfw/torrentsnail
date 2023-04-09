use tokio::signal;
use torrentsnail::app::Application;
use torrentsnail::Result;

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
