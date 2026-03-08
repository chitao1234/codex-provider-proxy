use std::{collections::HashMap, net::SocketAddr};

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use tracing_subscriber::EnvFilter;
use url::Url;

#[derive(Debug, Clone)]
pub struct Config {
    pub listen_addrs: Vec<SocketAddr>,
    pub listen_base_path: String,
    pub rpc_listen_addr: SocketAddr,
    pub rpc_token: Option<String>,
    pub default_provider: String,
    pub providers: HashMap<String, Provider>,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone)]
pub struct Provider {
    pub base_url: Url,
    pub api_key: String,
    pub authorization_header: Option<String>,
}

impl Provider {
    pub fn authorization_value(&self) -> String {
        if let Some(v) = &self.authorization_header {
            return v.clone();
        }
        format!("Bearer {}", self.api_key)
    }
}

#[derive(Debug, Clone)]
pub struct LoggingConfig {
    pub log_requests: bool,
    pub log_responses: bool,
    pub log_bodies: bool,
    pub max_body_log_bytes: usize,
    pub level: String,
    pub rule: Option<String>,
}

impl LoggingConfig {
    pub fn env_filter(&self) -> Result<EnvFilter> {
        let level = self.level.trim();
        if level.is_empty() {
            return Err(anyhow!("logging.level cannot be empty"));
        }

        let spec = match self.rule.as_deref().map(str::trim) {
            Some("") | None => level.to_string(),
            Some(rule) => format!("{level},{rule}"),
        };

        EnvFilter::try_new(spec).context("parse logging filter")
    }
}

#[derive(Debug, Deserialize)]
struct ConfigFile {
    #[serde(default)]
    listen_addr: Option<SocketAddr>,
    #[serde(default)]
    listen_addrs: Vec<SocketAddr>,
    #[serde(default = "default_listen_base_path")]
    listen_base_path: String,
    #[serde(default = "default_rpc_listen_addr")]
    rpc_listen_addr: SocketAddr,
    #[serde(default)]
    rpc_token: Option<String>,
    default_provider: String,
    #[serde(default)]
    logging: LoggingFile,
    providers: HashMap<String, ProviderFile>,
}

#[derive(Debug, Deserialize, Default)]
struct LoggingFile {
    #[serde(default)]
    log_requests: bool,
    #[serde(default)]
    log_responses: bool,
    #[serde(default)]
    log_bodies: bool,
    #[serde(default = "default_max_body_log_bytes")]
    max_body_log_bytes: usize,
    #[serde(default = "default_log_level")]
    level: String,
    #[serde(default)]
    rule: Option<String>,
}

fn default_max_body_log_bytes() -> usize {
    8192
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_listen_base_path() -> String {
    "/".to_string()
}

fn default_rpc_listen_addr() -> SocketAddr {
    // Local-only management endpoint by default.
    "127.0.0.1:8081".parse().expect("default RPC addr parses")
}

fn normalize_base_path(value: &str) -> Result<String> {
    if value.is_empty() {
        return Ok("/".to_string());
    }
    if !value.starts_with('/') {
        return Err(anyhow!(
            "listen_base_path must start with '/' (got {value:?})"
        ));
    }
    let trimmed = value.trim_end_matches('/');
    if trimmed.is_empty() {
        return Ok("/".to_string());
    }
    Ok(trimmed.to_string())
}

fn normalize_listen_addrs(
    listen_addr: Option<SocketAddr>,
    listen_addrs: Vec<SocketAddr>,
) -> Result<Vec<SocketAddr>> {
    let mut out = Vec::new();
    if let Some(addr) = listen_addr {
        out.push(addr);
    }
    for addr in listen_addrs {
        if !out.contains(&addr) {
            out.push(addr);
        }
    }
    if out.is_empty() {
        return Err(anyhow!(
            "config must set either listen_addr or listen_addrs with at least one address"
        ));
    }
    Ok(out)
}

#[derive(Debug, Deserialize)]
struct ProviderFile {
    base_url: Url,
    api_key: String,
    #[serde(default)]
    authorization_header: Option<String>,
}

impl Config {
    pub fn from_toml_str(toml_str: &str) -> Result<Self> {
        let file: ConfigFile = toml::from_str(toml_str).context("parse config toml")?;
        let listen_addrs = normalize_listen_addrs(file.listen_addr, file.listen_addrs)?;
        let listen_base_path = normalize_base_path(&file.listen_base_path)?;
        let mut providers = HashMap::new();
        for (name, provider) in file.providers {
            if provider.base_url.cannot_be_a_base() {
                return Err(anyhow!("provider {name} base_url cannot be a base url"));
            }
            providers.insert(
                name,
                Provider {
                    base_url: provider.base_url,
                    api_key: provider.api_key,
                    authorization_header: provider.authorization_header,
                },
            );
        }

        if !providers.contains_key(&file.default_provider) {
            return Err(anyhow!(
                "default_provider {:?} not present in providers",
                file.default_provider
            ));
        }

        Ok(Self {
            listen_addrs,
            listen_base_path,
            rpc_listen_addr: file.rpc_listen_addr,
            rpc_token: file.rpc_token,
            default_provider: file.default_provider,
            providers,
            logging: LoggingConfig {
                log_requests: file.logging.log_requests,
                log_responses: file.logging.log_responses,
                log_bodies: file.logging.log_bodies,
                max_body_log_bytes: file.logging.max_body_log_bytes,
                level: file.logging.level,
                rule: file.logging.rule,
            },
        })
    }
}

pub fn example_config_toml() -> &'static str {
    include_str!("../../../config.example.toml")
}

#[cfg(test)]
mod tests {
    use super::Config;

    #[test]
    fn parses_legacy_single_listen_addr() {
        let cfg = Config::from_toml_str(
            r#"
                listen_addr = "127.0.0.1:8080"
                default_provider = "provider_a"

                [providers.provider_a]
                base_url = "https://api.example.com/"
                api_key = "replace-me"
            "#,
        )
        .unwrap();

        assert_eq!(cfg.listen_addrs.len(), 1);
        assert_eq!(cfg.listen_addrs[0].to_string(), "127.0.0.1:8080");
    }

    #[test]
    fn parses_multiple_listen_addrs_without_duplicates() {
        let cfg = Config::from_toml_str(
            r#"
                listen_addr = "127.0.0.1:8080"
                listen_addrs = ["127.0.0.1:8081", "127.0.0.1:8080"]
                default_provider = "provider_a"

                [providers.provider_a]
                base_url = "https://api.example.com/"
                api_key = "replace-me"
            "#,
        )
        .unwrap();

        let addrs: Vec<String> = cfg.listen_addrs.iter().map(ToString::to_string).collect();
        assert_eq!(addrs, vec!["127.0.0.1:8080", "127.0.0.1:8081"]);
    }
}
