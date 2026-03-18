use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use codex_provider_proxy_rpc_types::{
    DeleteRouteResponse, ListRoutesResponse, ProvidersResponse, SetDefaultProviderRequest,
    SetRouteRequest,
};
use regex::Regex;
use serde::Deserialize;

use codex_provider_proxyctl::process_scan;

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

    /// Change the runtime default provider used when no PID route matches
    SetDefault {
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

    /// Match processes by cmdline regex and set PID routes interactively
    Match {
        /// Rust regex matched against the full cmdline string.
        /// (The cmdline is constructed from platform-specific process metadata; on Linux this reads
        /// /proc/<pid>/cmdline and joins with spaces.)
        /// If omitted, uses a built-in default regex.
        #[arg(value_name = "REGEX", default_value = DEFAULT_MATCH_REGEX)]
        regex: String,

        /// Only consider the first N matching PIDs.
        #[arg(long)]
        limit: Option<usize>,

        /// Print matching processes without setting any routes.
        #[arg(long)]
        dry_run: bool,

        /// Apply this provider to all matching PIDs (non-interactive).
        #[arg(long)]
        provider: Option<String>,
    },

    /// Prune captured exchange logs older than a UTC date
    PruneLogs {
        /// Exchange log directory (default matches README examples).
        #[arg(short = 'd', long, default_value = "logs/exchanges")]
        dir: PathBuf,

        /// Prune exchange logs with timestamp older than this UTC date (YYYY-MM-DD).
        #[arg(short = 'b', long, value_name = "YYYY-MM-DD")]
        before_date: String,

        /// Confirm destructive deletion.
        #[arg(short = 'y', long)]
        yes: bool,

        /// Print what would be pruned without deleting files.
        #[arg(short = 'n', long)]
        dry_run: bool,
    },
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

const DEFAULT_MATCH_REGEX: &str = "codex";

async fn rpc_list_providers(
    client: &reqwest::Client,
    rpc_url: &str,
    token: Option<&String>,
) -> Result<ProvidersResponse> {
    let url = format!("{}/rpc/v1/providers", rpc_url.trim_end_matches('/'));
    let mut req = client.get(url);
    if let Some(token) = token {
        req = req.bearer_auth(token);
    }
    let resp = req.send().await.context("send providers request")?;
    let resp = resp
        .error_for_status()
        .context("providers returned error status")?;
    resp.json().await.context("decode response")
}

async fn rpc_set_route(
    client: &reqwest::Client,
    rpc_url: &str,
    token: Option<&String>,
    pid: u32,
    provider: String,
) -> Result<()> {
    let url = format!("{}/rpc/v1/routes", rpc_url.trim_end_matches('/'));
    let mut req = client.post(url).json(&SetRouteRequest { pid, provider });
    if let Some(token) = token {
        req = req.bearer_auth(token);
    }
    let resp = req.send().await.context("send set request")?;
    resp.error_for_status_ref()
        .context("set route returned error status")?;
    Ok(())
}

fn prompt_line(prompt: &str) -> Result<String> {
    use std::io::Write;

    let mut out = std::io::stdout().lock();
    write!(out, "{prompt}")?;
    out.flush()?;

    let mut s = String::new();
    std::io::stdin().read_line(&mut s).context("read stdin")?;
    Ok(s.trim().to_string())
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
            rpc_set_route(&client, &rpc_url, token.as_ref(), pid, provider).await?;
            println!("ok");
        }
        Cmd::SetDefault { provider } => {
            let url = format!("{}/rpc/v1/default-provider", rpc_url.trim_end_matches('/'));
            let mut req = client
                .post(url)
                .json(&SetDefaultProviderRequest { provider });
            if let Some(token) = &token {
                req = req.bearer_auth(token);
            }
            let resp = req.send().await.context("send set-default request")?;
            resp.error_for_status_ref()
                .context("set default provider returned error status")?;
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
            let out = rpc_list_providers(&client, &rpc_url, token.as_ref()).await?;
            println!("default={}", out.default_provider);
            for p in out.providers {
                println!("provider={}", p);
            }
        }
        Cmd::Match {
            regex,
            limit,
            dry_run,
            provider,
        } => {
            let re = Regex::new(&regex).context("compile regex")?;
            let (matches, stats) = process_scan::find_processes_by_cmdline_regex(&re, limit)?;

            if matches.is_empty() {
                println!(
                    "no matches (seen_pids={} unreadable_cmdline={} unreadable_cwd={})",
                    stats.pids_seen, stats.unreadable_cmdline, stats.unreadable_cwd
                );
                return Ok(());
            }

            let providers = rpc_list_providers(&client, &rpc_url, token.as_ref()).await?;
            println!(
                "matched={} (seen_pids={} unreadable_cmdline={} unreadable_cwd={})",
                matches.len(),
                stats.pids_seen,
                stats.unreadable_cmdline,
                stats.unreadable_cwd
            );
            println!("default={}", providers.default_provider);
            for p in &providers.providers {
                println!("provider={}", p);
            }

            let Some(provider) = provider else {
                for m in matches {
                    println!("pid={}", m.pid);
                    println!("cwd={}", m.cwd);
                    println!("cmdline={}", m.cmdline);

                    loop {
                        let input = prompt_line("provider (empty=skip, q=quit, ?=providers): ")?;
                        if input.is_empty() {
                            break;
                        }
                        if input == "q" || input == "quit" {
                            return Ok(());
                        }
                        if input == "?" {
                            println!("default={}", providers.default_provider);
                            for p in &providers.providers {
                                println!("provider={}", p);
                            }
                            continue;
                        }
                        if !providers.providers.iter().any(|p| p == &input) {
                            eprintln!("unknown provider: {input}");
                            continue;
                        }

                        if dry_run {
                            println!("dry-run: would set pid={} provider={}", m.pid, input);
                        } else {
                            rpc_set_route(&client, &rpc_url, token.as_ref(), m.pid, input).await?;
                            println!("ok");
                        }
                        break;
                    }
                }

                return Ok(());
            };

            if !providers.providers.iter().any(|p| p == &provider) {
                anyhow::bail!("unknown provider: {provider}");
            }

            for m in matches {
                println!("pid={}", m.pid);
                println!("cwd={}", m.cwd);
                println!("cmdline={}", m.cmdline);
                if dry_run {
                    println!("dry-run: would set pid={} provider={}", m.pid, provider);
                } else {
                    rpc_set_route(&client, &rpc_url, token.as_ref(), m.pid, provider.clone())
                        .await?;
                    println!("ok");
                }
            }
        }
        Cmd::PruneLogs {
            dir,
            before_date,
            yes,
            dry_run,
        } => {
            let cutoff_unix_ms =
                codex_provider_proxyctl::log_prune::parse_cutoff_date_utc(&before_date)?;
            let plan = codex_provider_proxyctl::log_prune::build_prune_plan(&dir, cutoff_unix_ms)?;

            println!("dir={}", dir.display());
            println!("before_date={before_date} cutoff_unix_ms={cutoff_unix_ms}");
            println!(
                "candidates_exchanges={} candidates_files={} candidates_bytes={}",
                plan.exchange_count(),
                plan.total_files,
                plan.total_bytes
            );

            if plan.is_empty() {
                println!("nothing to prune");
                return Ok(());
            }

            if dry_run {
                println!("dry-run: no files deleted");
                for group in &plan.groups {
                    println!(
                        "would_prune started_unix_ms={} stem={} files={} bytes={}",
                        group.started_unix_ms,
                        group.stem,
                        group.files.len(),
                        group.total_bytes
                    );
                }
                return Ok(());
            }

            if !yes {
                eprintln!(
                    "WARNING: prune-logs permanently deletes files in {}",
                    dir.display()
                );
                let answer =
                    prompt_line("Type 'yes' to confirm pruning, or press enter to abort: ")?;
                if !answer.eq_ignore_ascii_case("yes") {
                    eprintln!("aborted: no files deleted");
                    return Ok(());
                }
            }

            let out = codex_provider_proxyctl::log_prune::execute_prune(&plan)?;
            println!(
                "pruned_exchanges={} pruned_files={} pruned_bytes={}",
                out.pruned_exchanges, out.pruned_files, out.pruned_bytes
            );
        }
    }

    Ok(())
}
