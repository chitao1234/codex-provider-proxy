use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Command as ProcessCommand, ExitStatus},
};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use codex_provider_proxy_rpc_types::{
    DeleteRouteResponse, ListRoutesResponse, ProvidersResponse, SetDefaultProviderRequest,
    SetRouteRequest, StatisticsBreakdown, StatisticsResponse,
};
use regex::Regex;
use reqwest::{Method, RequestBuilder};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use codex_provider_proxyctl::process_scan;

#[derive(Debug, Parser)]
#[command(name = "codex-provider-proxyctl")]
#[command(about = "RPC client for codex-provider-proxy", long_about = None)]
struct Args {
    /// Base URL of the RPC server (e.g. http://127.0.0.1:8081).
    /// If omitted and `./config.toml` exists, uses `rpc_listen_addr` from that file.
    #[arg(short = 'r', long)]
    rpc_url: Option<String>,

    /// Optional bearer token for RPC auth.
    /// If omitted and `./config.toml` exists, uses `rpc_token` from that file.
    #[arg(short = 't', long)]
    token: Option<String>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Set (or overwrite) the provider for a PID
    Set {
        #[arg(short = 'p', long)]
        pid: u32,
        #[arg(short = 'P', long)]
        provider: String,
    },

    /// Change the runtime default provider used when no PID route matches
    SetDefault {
        #[arg(short = 'p', long)]
        provider: String,
    },

    /// Delete an existing PID route
    Delete {
        #[arg(short = 'p', long)]
        pid: u32,
    },

    /// Clear all PID routes
    Clear,

    /// List current PID routes
    List,

    /// List configured providers and the default provider
    Providers,

    /// Show persistent proxy statistics (last 24 hours by default)
    Stats {
        /// Window size in hours; use 0 for all-time statistics.
        #[arg(short = 'H', long, default_value_t = 24)]
        hours: u32,

        /// Print the full machine-readable response as JSON.
        #[arg(short = 'j', long)]
        json: bool,
    },

    /// Match processes by cmdline regex and set PID routes interactively
    Match {
        /// Rust regex matched against the full cmdline string.
        /// (The cmdline is constructed from platform-specific process metadata; on Linux this reads
        /// /proc/<pid>/cmdline and joins with spaces.)
        /// If omitted, uses a built-in default regex.
        #[arg(value_name = "REGEX", default_value = DEFAULT_MATCH_REGEX)]
        regex: String,

        /// Only consider the first N matching PIDs.
        #[arg(short = 'l', long)]
        limit: Option<usize>,

        /// Print matching processes without setting any routes.
        #[arg(short = 'n', long)]
        dry_run: bool,

        /// Apply this provider to all matching PIDs (non-interactive).
        #[arg(short = 'p', long)]
        provider: Option<String>,
    },

    /// Execute a command with a temporary PID route bound to the chosen provider
    Exec {
        #[arg(short = 'p', long)]
        provider: String,

        /// Keep the PID route after the command exits (default: remove it).
        #[arg(short = 'k', long)]
        keep_route: bool,

        /// Command to execute, followed by its args.
        #[arg(
            required = true,
            num_args = 1..,
            trailing_var_arg = true,
            allow_hyphen_values = true
        )]
        command: Vec<String>,
    },

    /// Prune captured exchange logs older than a local datetime
    PruneLogs {
        /// Exchange log directory (default matches README examples).
        #[arg(short = 'd', long, default_value = "logs/exchanges")]
        dir: PathBuf,

        /// Prune exchange logs with timestamp older than this local datetime.
        /// Uses the machine's current timezone. Example: 2026-01-01T00:00:00.000
        #[arg(
            short = 'b',
            long = "before-local-datetime",
            visible_alias = "before-date",
            value_name = "YYYY-MM-DDTHH:MM:SS[.fraction]"
        )]
        before_local_datetime: String,

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

struct RpcClient {
    client: reqwest::Client,
    base_url: String,
    token: Option<String>,
}

impl RpcClient {
    fn new(base_url: String, token: Option<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
            token,
        }
    }

    fn request(&self, method: Method, path: &str) -> RequestBuilder {
        let request = self
            .client
            .request(method, format!("{}{}", self.base_url, path));
        match &self.token {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }

    async fn send_no_content(
        &self,
        method: Method,
        path: &str,
        context: &'static str,
    ) -> Result<()> {
        self.request(method, path)
            .send()
            .await
            .with_context(|| format!("send {context} request"))?
            .error_for_status()
            .with_context(|| format!("{context} returned error status"))?;
        Ok(())
    }

    async fn send_json_body<B: Serialize>(
        &self,
        method: Method,
        path: &str,
        body: &B,
        context: &'static str,
    ) -> Result<()> {
        self.request(method, path)
            .json(body)
            .send()
            .await
            .with_context(|| format!("send {context} request"))?
            .error_for_status()
            .with_context(|| format!("{context} returned error status"))?;
        Ok(())
    }

    async fn send_json<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        context: &'static str,
    ) -> Result<T> {
        self.request(method, path)
            .send()
            .await
            .with_context(|| format!("send {context} request"))?
            .error_for_status()
            .with_context(|| format!("{context} returned error status"))?
            .json()
            .await
            .with_context(|| format!("decode {context} response"))
    }

    async fn providers(&self) -> Result<ProvidersResponse> {
        self.send_json(Method::GET, "/rpc/v1/providers", "providers")
            .await
    }

    async fn set_route(&self, pid: u32, provider: String) -> Result<()> {
        self.send_json_body(
            Method::POST,
            "/rpc/v1/routes",
            &SetRouteRequest { pid, provider },
            "set route",
        )
        .await
    }

    async fn set_default_provider(&self, provider: String) -> Result<()> {
        self.send_json_body(
            Method::POST,
            "/rpc/v1/default-provider",
            &SetDefaultProviderRequest { provider },
            "set default provider",
        )
        .await
    }

    async fn delete_route(&self, pid: u32) -> Result<DeleteRouteResponse> {
        self.send_json(
            Method::DELETE,
            &format!("/rpc/v1/routes/{pid}"),
            "delete route",
        )
        .await
    }

    async fn clear_routes(&self) -> Result<()> {
        self.send_no_content(Method::POST, "/rpc/v1/routes/clear", "clear routes")
            .await
    }

    async fn routes(&self) -> Result<ListRoutesResponse> {
        self.send_json(Method::GET, "/rpc/v1/routes", "list routes")
            .await
    }

    async fn statistics(&self, hours: u32) -> Result<StatisticsResponse> {
        self.send_json(
            Method::GET,
            &format!("/rpc/v1/statistics?hours={hours}"),
            "statistics",
        )
        .await
    }
}

fn exit_with_status(status: ExitStatus) -> ! {
    if let Some(code) = status.code() {
        std::process::exit(code);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(sig) = status.signal() {
            std::process::exit(128 + sig);
        }
    }
    std::process::exit(1)
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

async fn cleanup_route_best_effort(rpc: &RpcClient, pid: u32, context: &str) {
    if let Err(err) = rpc.delete_route(pid).await {
        eprintln!("warning: {context} pid={pid}: {err}");
    }
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
    let rpc = RpcClient::new(rpc_url, token);

    match args.cmd {
        Cmd::Set { pid, provider } => {
            rpc.set_route(pid, provider).await?;
            println!("ok");
        }
        Cmd::SetDefault { provider } => {
            rpc.set_default_provider(provider).await?;
            println!("ok");
        }
        Cmd::Delete { pid } => {
            let out = rpc.delete_route(pid).await?;
            println!("removed={}", out.removed);
        }
        Cmd::Clear => {
            rpc.clear_routes().await?;
            println!("ok");
        }
        Cmd::List => {
            let out = rpc.routes().await?;
            for r in out.routes {
                println!("pid={} provider={}", r.pid, r.provider);
            }
        }
        Cmd::Providers => {
            let out = rpc.providers().await?;
            println!("default={}", out.default_provider);
            for p in out.providers {
                println!("provider={}", p);
            }
        }
        Cmd::Stats { hours, json } => {
            let out = rpc.statistics(hours).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                print_statistics_report(&out);
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

            let providers = rpc.providers().await?;
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
                            rpc.set_route(m.pid, input).await?;
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
                    rpc.set_route(m.pid, provider.clone()).await?;
                    println!("ok");
                }
            }
        }
        Cmd::Exec {
            provider,
            keep_route,
            command,
        } => {
            let providers = rpc.providers().await?;
            if !providers.providers.iter().any(|p| p == &provider) {
                anyhow::bail!("unknown provider: {provider}");
            }

            let exec = command.first().context("missing command")?;
            let args = &command[1..];
            let parent_pid = std::process::id();

            rpc.set_route(parent_pid, provider.clone())
                .await
                .with_context(|| {
                    format!(
                        "set pre-exec route for parent pid={} provider={}",
                        parent_pid, provider
                    )
                })?;

            let mut child = match ProcessCommand::new(exec).args(args).spawn() {
                Ok(child) => child,
                Err(err) => {
                    cleanup_route_best_effort(
                        &rpc,
                        parent_pid,
                        "failed to remove pre-exec parent route after spawn failure for",
                    )
                    .await;
                    return Err(err).with_context(|| format!("spawn command {:?}", command));
                }
            };
            let pid = child.id();

            if let Err(err) = rpc.set_route(pid, provider.clone()).await {
                let _ = child.kill();
                let _ = child.wait();
                cleanup_route_best_effort(
                    &rpc,
                    parent_pid,
                    "failed to remove pre-exec parent route after child route setup failure for",
                )
                .await;
                return Err(err)
                    .with_context(|| format!("set route for pid={} provider={}", pid, provider));
            }

            cleanup_route_best_effort(
                &rpc,
                parent_pid,
                "failed to remove pre-exec parent route for",
            )
            .await;

            println!("set pid={} provider={}", pid, provider);
            let status = child
                .wait()
                .with_context(|| format!("wait for command {:?}", command))?;

            if keep_route {
                println!("kept route for pid={}", pid);
            } else {
                match rpc.delete_route(pid).await {
                    Ok(out) => println!("removed={} pid={}", out.removed, pid),
                    Err(err) => {
                        eprintln!("warning: failed to remove pid route for {}: {}", pid, err)
                    }
                }
            }

            exit_with_status(status);
        }
        Cmd::PruneLogs {
            dir,
            before_local_datetime,
            yes,
            dry_run,
        } => {
            let cutoff_unix_ms = codex_provider_proxyctl::log_prune::parse_cutoff_local_datetime(
                &before_local_datetime,
            )?;
            let plan = codex_provider_proxyctl::log_prune::build_prune_plan(&dir, cutoff_unix_ms)?;

            println!("dir={}", dir.display());
            println!(
                "before_local_datetime={before_local_datetime} cutoff_unix_ms={cutoff_unix_ms}"
            );
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

fn print_statistics_report(statistics: &StatisticsResponse) {
    let window = statistics
        .window
        .hours
        .map(|hours| format!("last {hours}h"))
        .unwrap_or_else(|| "all time".to_string());
    println!("statistics.enabled={}", statistics.enabled);
    println!("statistics.window={window}");
    println!("requests={}", statistics.summary.requests);
    println!(
        "requests.successful={}",
        statistics.summary.successful_requests
    );
    println!("requests.errors={}", statistics.summary.error_requests);
    println!(
        "requests.error_rate={:.2}%",
        statistics.summary.error_rate * 100.0
    );
    println!(
        "latency.total_ms.avg={:.2} p50={} p95={} p99={} max={}",
        statistics.summary.total_duration_ms.average,
        statistics.summary.total_duration_ms.p50,
        statistics.summary.total_duration_ms.p95,
        statistics.summary.total_duration_ms.p99,
        statistics.summary.total_duration_ms.max
    );
    println!(
        "latency.upstream_ms.avg={:.2} p50={} p95={} p99={} max={}",
        statistics.summary.upstream_latency_ms.average,
        statistics.summary.upstream_latency_ms.p50,
        statistics.summary.upstream_latency_ms.p95,
        statistics.summary.upstream_latency_ms.p99,
        statistics.summary.upstream_latency_ms.max
    );
    println!(
        "traffic.request_bytes={} traffic.response_bytes={}",
        statistics.summary.request_bytes, statistics.summary.response_bytes
    );
    println!(
        "tokens.input={} tokens.output={} tokens.total={} tokens.cached={} tokens.reasoning={}",
        statistics.tokens.input_tokens,
        statistics.tokens.output_tokens,
        statistics.tokens.total_tokens,
        statistics.tokens.cached_tokens,
        statistics.tokens.reasoning_tokens
    );
    print_statistics_breakdown("providers", &statistics.providers);
    print_statistics_breakdown("models", &statistics.models);
    print_statistics_breakdown("client_ips", &statistics.client_ips);
    print_statistics_breakdown("status_codes", &statistics.status_codes);
}

fn print_statistics_breakdown(label: &str, rows: &[StatisticsBreakdown]) {
    println!();
    println!("{label}:");
    if rows.is_empty() {
        println!("  (none)");
        return;
    }
    for row in rows {
        println!(
            "  {} requests={} errors={} error_rate={:.2}% avg_ms={:.2} tokens={}",
            row.key,
            row.requests,
            row.error_requests,
            row.error_rate * 100.0,
            row.average_response_time_ms,
            row.total_tokens
        );
    }
}

#[cfg(test)]
mod tests {
    use reqwest::{header, Method};

    use super::RpcClient;

    #[test]
    fn rpc_client_normalizes_base_url_and_adds_auth() {
        let rpc = RpcClient::new(
            "http://127.0.0.1:8081///".to_string(),
            Some("secret".to_string()),
        );

        let request = rpc
            .request(Method::GET, "/rpc/v1/providers")
            .build()
            .expect("build request");

        assert_eq!(
            request.url().as_str(),
            "http://127.0.0.1:8081/rpc/v1/providers"
        );
        assert_eq!(
            request.headers().get(header::AUTHORIZATION).unwrap(),
            "Bearer secret"
        );
    }

    #[test]
    fn rpc_client_omits_auth_without_token() {
        let rpc = RpcClient::new("http://127.0.0.1:8081".to_string(), None);

        let request = rpc
            .request(Method::GET, "/rpc/v1/routes")
            .build()
            .expect("build request");

        assert!(!request.headers().contains_key(header::AUTHORIZATION));
    }
}
