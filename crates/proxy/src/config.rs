use std::{collections::HashMap, net::SocketAddr};

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use url::Url;

#[derive(Debug, Clone)]
pub struct Config {
    pub listen_addr: SocketAddr,
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
}

#[derive(Debug, Deserialize)]
struct ConfigFile {
    listen_addr: SocketAddr,
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
}

fn default_max_body_log_bytes() -> usize {
    8192
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
        if !file.listen_addr.ip().is_loopback() {
            return Err(anyhow!(
                "listen_addr must be loopback (got {})",
                file.listen_addr
            ));
        }
        if !file.rpc_listen_addr.ip().is_loopback() {
            return Err(anyhow!(
                "rpc_listen_addr must be loopback (got {})",
                file.rpc_listen_addr
            ));
        }
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
            listen_addr: file.listen_addr,
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
            },
        })
    }
}

pub fn example_config_toml() -> &'static str {
    include_str!("../../../config.example.toml")
}
