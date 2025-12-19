mod config;
mod log_capture;
mod proxy;
mod rpc;

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use axum::Router;
use clap::Parser;
use pid_resolver::platform::default_pid_resolver;
use proxy::{handle_proxy, AppState};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::sync::RwLock;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::config::{example_config_toml, Config};

#[derive(Debug, Parser)]
#[command(name = "codex-provider-proxy")]
#[command(about = "Local reverse proxy that routes by client PID", long_about = None)]
struct Args {
    /// Path to the config file
    #[arg(long, default_value = "config.toml")]
    config: PathBuf,

    /// Print the example config
    #[arg(long)]
    print_example_config: bool,

    /// Force-enable request logging (overrides config.toml)
    #[arg(long)]
    log_requests: bool,

    /// Force-enable response logging (overrides config.toml)
    #[arg(long)]
    log_responses: bool,

    /// Force-enable body logging (overrides config.toml)
    #[arg(long)]
    log_bodies: bool,

    /// Max bytes captured per request/response body when logging bodies (overrides config.toml)
    #[arg(long)]
    max_body_log_bytes: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.print_example_config {
        print!("{}", example_config_toml());
        return Ok(());
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config_str = std::fs::read_to_string(&args.config)
        .with_context(|| format!("read config {}", args.config.display()))?;
    let mut cfg = Config::from_toml_str(&config_str)?;
    if args.log_requests {
        cfg.logging.log_requests = true;
    }
    if args.log_responses {
        cfg.logging.log_responses = true;
    }
    if args.log_bodies {
        cfg.logging.log_bodies = true;
    }
    if let Some(max) = args.max_body_log_bytes {
        cfg.logging.max_body_log_bytes = max;
    }
    let cfg = Arc::new(cfg);

    let proxy_listener = TcpListener::bind(cfg.listen_addr)
        .await
        .with_context(|| format!("bind {}", cfg.listen_addr))?;
    let local_addr = proxy_listener.local_addr().context("get local_addr")?;

    let rpc_listener = TcpListener::bind(cfg.rpc_listen_addr)
        .await
        .with_context(|| format!("bind {}", cfg.rpc_listen_addr))?;
    let rpc_addr = rpc_listener.local_addr().context("get rpc local_addr")?;

    info!(listen = %local_addr, "listening");
    info!(rpc_listen = %rpc_addr, "rpc listening");

    let http_client = reqwest::Client::builder()
        .user_agent("codex-provider-proxy/0.1.0")
        .build()
        .context("build reqwest client")?;

    let pid_routes = Arc::new(dashmap::DashMap::<u32, String>::new());
    let default_provider = Arc::new(RwLock::new(cfg.default_provider.clone()));

    let app_state = AppState {
        listen_addr: local_addr,
        cfg,
        pid_resolver: default_pid_resolver(),
        http_client,
        request_seq: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        default_provider: default_provider.clone(),
        pid_routes: pid_routes.clone(),
    };

    let proxy_app = Router::new()
        .fallback(handle_proxy)
        .with_state(app_state.clone());

    let rpc_app = rpc::router(rpc::RpcState {
        cfg: app_state.cfg.clone(),
        default_provider,
        pid_routes,
    });

    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    tokio::spawn({
        let shutdown_tx = shutdown_tx.clone();
        async move {
            let _ = tokio::signal::ctrl_c().await;
            let _ = shutdown_tx.send(());
        }
    });

    let mut proxy_shutdown = shutdown_tx.subscribe();
    let mut rpc_shutdown = shutdown_tx.subscribe();

    let proxy_task = tokio::spawn(async move {
        axum::serve(
            proxy_listener,
            proxy_app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            let _ = proxy_shutdown.recv().await;
        })
        .await
        .context("proxy server exited")
    });

    let rpc_task = tokio::spawn(async move {
        axum::serve(
            rpc_listener,
            rpc_app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            let _ = rpc_shutdown.recv().await;
        })
        .await
        .context("rpc server exited")
    });

    proxy_task.await??;
    rpc_task.await??;

    Ok(())
}
