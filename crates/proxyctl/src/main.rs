use std::{net::SocketAddr, path::Path};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use codex_provider_proxy_rpc_types::{
    DeleteRouteResponse, ListRoutesResponse, ProvidersResponse, SetRouteRequest,
};
use serde::Deserialize;

#[derive(Debug, Parser)]
#[command(name = "codex-provider-proxyctl")]
#[command(about = "RPC client for codex-provider-proxy", long_about = None)]
struct Args {
    /// Base URL of the RPC server (e.g. http://127.0.0.1:8081).
    /// If omitted and `./config.toml` exists, uses `rpc_listen_addr` from that file.
    #[arg(long)]
    rpc_url: Option<String>,

    /// Optional bearer token for RPC auth.
    /// If omitted and `./config.toml` exists, uses `rpc_token` from that file.
    #[arg(long)]
    token: Option<String>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Set (or overwrite) the provider for a PID
    Set {
        #[arg(long)]
        pid: u32,
        #[arg(long)]
        provider: String,
    },

    /// Delete an existing PID route
    Delete {
        #[arg(long)]
        pid: u32,
    },

    /// Clear all PID routes
    Clear,

    /// List current PID routes
    List,

    /// List configured providers and the default provider
    Providers,
}

#[derive(Debug, Default, Deserialize)]
struct CwdConfig {
    #[serde(default)]
    rpc_listen_addr: Option<SocketAddr>,
    #[serde(default)]
    rpc_token: Option<String>,
}

fn load_cwd_config_if_present() -> Result<Option<CwdConfig>> {
    let path = Path::new("config.toml");
    if !path.exists() {
        return Ok(None);
    }
    let s = std::fs::read_to_string(path).context("read ./config.toml")?;
    let cfg: CwdConfig = toml::from_str(&s).context("parse ./config.toml")?;
    Ok(Some(cfg))
}

fn default_rpc_url() -> String {
    "http://127.0.0.1:8081".to_string()
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let cwd_cfg = load_cwd_config_if_present()?.unwrap_or_default();

    let rpc_url = args
        .rpc_url
        .or_else(|| cwd_cfg.rpc_listen_addr.map(|a| format!("http://{a}")))
        .unwrap_or_else(default_rpc_url);

    let token = args.token.or(cwd_cfg.rpc_token);
    let client = reqwest::Client::new();

    match args.cmd {
        Cmd::Set { pid, provider } => {
            let url = format!("{}/rpc/v1/routes", rpc_url.trim_end_matches('/'));
            let mut req = client.post(url).json(&SetRouteRequest { pid, provider });
            if let Some(token) = &token {
                req = req.bearer_auth(token);
            }
            let resp = req.send().await.context("send set request")?;
            resp.error_for_status_ref()
                .context("set route returned error status")?;
            println!("ok");
        }
        Cmd::Delete { pid } => {
            let url = format!("{}/rpc/v1/routes/{}", rpc_url.trim_end_matches('/'), pid);
            let mut req = client.delete(url);
            if let Some(token) = &token {
                req = req.bearer_auth(token);
            }
            let resp = req.send().await.context("send delete request")?;
            let resp = resp
                .error_for_status()
                .context("delete route returned error status")?;
            let out: DeleteRouteResponse = resp.json().await.context("decode response")?;
            println!("removed={}", out.removed);
        }
        Cmd::Clear => {
            let url = format!("{}/rpc/v1/routes/clear", rpc_url.trim_end_matches('/'));
            let mut req = client.post(url);
            if let Some(token) = &token {
                req = req.bearer_auth(token);
            }
            let resp = req.send().await.context("send clear request")?;
            resp.error_for_status_ref()
                .context("clear routes returned error status")?;
            println!("ok");
        }
        Cmd::List => {
            let url = format!("{}/rpc/v1/routes", rpc_url.trim_end_matches('/'));
            let mut req = client.get(url);
            if let Some(token) = &token {
                req = req.bearer_auth(token);
            }
            let resp = req.send().await.context("send list request")?;
            let resp = resp
                .error_for_status()
                .context("list routes returned error status")?;
            let out: ListRoutesResponse = resp.json().await.context("decode response")?;
            for r in out.routes {
                println!("pid={} provider={}", r.pid, r.provider);
            }
        }
        Cmd::Providers => {
            let url = format!("{}/rpc/v1/providers", rpc_url.trim_end_matches('/'));
            let mut req = client.get(url);
            if let Some(token) = &token {
                req = req.bearer_auth(token);
            }
            let resp = req.send().await.context("send providers request")?;
            let resp = resp
                .error_for_status()
                .context("providers returned error status")?;
            let out: ProvidersResponse = resp.json().await.context("decode response")?;
            println!("default={}", out.default_provider);
            for p in out.providers {
                println!("provider={}", p);
            }
        }
    }

    Ok(())
}
