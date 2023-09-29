mod app;
mod model;

use anyhow::Result;
use app::Application;
use clap::Parser;
use snail::config::Config;
use tokio::signal;
use tracing::log::info;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(short, long)]
    config: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_file(true)
        .with_line_number(true)
        .init();

    let args = Args::parse();

    let cfg = if let Some(config_path) = args.config {
        Config::load_json_file(config_path)?
    } else {
        Config::default()
    };

    let app = Application::from_config(cfg).await.unwrap();

    info!("wait interrupt");
    signal::ctrl_c().await.expect("failed to listen for event");

    info!("start shutdown app");
    app.shutdown().await.unwrap();

    Ok(())
}
