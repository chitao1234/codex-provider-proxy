mod config;
mod exchange_log;
mod log_capture;
mod proxy;
mod rpc;
mod runtime;

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use clap::Parser;
use pid_resolver::platform::default_pid_resolver;
use runtime::{spawn_config_watcher, ConfigOverrides, RuntimeState, ServerManager};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::config::{example_config_toml, Config};

#[derive(Debug, Parser)]
#[command(name = "codex-provider-proxy")]
#[command(about = "Local reverse proxy that routes by client PID", long_about = None)]
struct Args {
    /// Path to the config file
    #[arg(short = 'c', long, default_value = "config.toml")]
    config: PathBuf,

    /// Print the example config
    #[arg(short = 'e', long)]
    print_example_config: bool,

    /// Force-enable request logging (overrides config.toml)
    #[arg(short = 'r', long)]
    log_requests: bool,

    /// Force-enable response logging (overrides config.toml)
    #[arg(short = 's', long)]
    log_responses: bool,

    /// Force-enable body logging (overrides config.toml)
    #[arg(short = 'b', long)]
    log_bodies: bool,

    /// Max bytes captured per request/response body when logging bodies (overrides config.toml)
    #[arg(short = 'm', long)]
    max_body_log_bytes: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.print_example_config {
        print!("{}", example_config_toml());
        return Ok(());
    }

    let config_path = absolute_config_path(args.config).context("resolve config path")?;
    let overrides = ConfigOverrides {
        log_requests: args.log_requests,
        log_responses: args.log_responses,
        log_bodies: args.log_bodies,
        max_body_log_bytes: args.max_body_log_bytes,
    };

    let mut initial_config = load_config(&config_path)?;
    overrides.apply(&mut initial_config);

    let initial_filter: EnvFilter = initial_config.logging.env_filter()?;
    let (filter_layer, filter_reload) = tracing_subscriber::reload::Layer::new(initial_filter);
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(tracing_subscriber::fmt::layer())
        .init();

    let http_client = reqwest::Client::builder()
        .user_agent("codex-provider-proxy/0.1.1")
        .build()
        .context("build reqwest client")?;
    let runtime = RuntimeState::new(
        Arc::new(initial_config.clone()),
        default_pid_resolver(),
        http_client,
        filter_reload,
    );
    let manager = Arc::new(ServerManager::new(config_path.clone(), overrides, runtime));
    manager.reconfigure(initial_config).await?;

    let _config_watcher = spawn_config_watcher(manager.clone())?;
    info!(path = %config_path.display(), "watching config for reloads");

    tokio::signal::ctrl_c().await.context("wait for ctrl-c")?;
    info!("shutting down");
    manager.shutdown_all().await;

    Ok(())
}

fn load_config(path: &PathBuf) -> Result<Config> {
    let config_str =
        std::fs::read_to_string(path).with_context(|| format!("read config {}", path.display()))?;
    Config::from_toml_str(&config_str)
}

fn absolute_config_path(path: PathBuf) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(std::env::current_dir()
            .context("read current dir")?
            .join(path))
    }
}
