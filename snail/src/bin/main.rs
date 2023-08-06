use clap::Parser;
use tokio::signal;
use torrentsnail::app::Application;
use torrentsnail::config::Config;
use torrentsnail::Result;


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
    let args = Args::parse();

    let cfg = if let Some(config_path) = args.config {
        Config::load_json_file(config_path)?
    } else {
        Config::default()
    };

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_file(true)
        .with_line_number(true)
        .init();

    let app = Application::from_config(cfg).await.unwrap();

    signal::ctrl_c().await.expect("failed to listen for event");

    app.shutdown().await.unwrap();

    Ok(())
}
