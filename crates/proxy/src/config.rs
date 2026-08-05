use std::{collections::HashMap, net::SocketAddr, path::PathBuf, time::Duration};

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
    pub upstream_connect_timeout: Option<Duration>,
    pub upstream_idle_timeout: Option<Duration>,
    pub reject_messages_count_tokens: bool,
    pub drop_responses_slow_down_errors: bool,
    pub convert_429_to_503: bool,
    pub transparent_retry_count: u32,
    pub transparent_retry_head_requests: bool,
    pub transparent_retry_backoff_step: Duration,
    pub request_body_buffer_max_bytes: usize,
    pub default_provider: String,
    pub providers: HashMap<String, Provider>,
    pub rewrite: RewriteConfig,
    pub logging: LoggingConfig,
    pub statistics: StatisticsConfig,
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
    pub exchange_log_dir: Option<PathBuf>,
    pub exchange_body_max_bytes: Option<u64>,
    pub exchange_body_compression: BodyLogCompression,
    pub reconstruct_responses: bool,
    pub level: String,
    pub rule: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatisticsConfig {
    pub enabled: bool,
    pub database_path: PathBuf,
    pub request_body_max_bytes: usize,
    pub response_body_max_bytes: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RewriteConfig {
    pub model_mappings: Vec<ModelMapping>,
}

impl RewriteConfig {
    pub fn is_enabled(&self) -> bool {
        !self.model_mappings.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelMapping {
    pub provider: Option<Vec<String>>,
    pub from_model: Vec<String>,
    pub from_reasoning_effort: Option<String>,
    pub to_model: Option<String>,
    pub to_reasoning_effort: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BodyLogCompression {
    #[default]
    None,
    Zstd,
}

impl BodyLogCompression {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Zstd => "zstd",
        }
    }
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
    #[serde(default = "default_upstream_connect_timeout_secs")]
    upstream_connect_timeout_secs: u64,
    #[serde(default = "default_upstream_idle_timeout_secs")]
    upstream_idle_timeout_secs: u64,
    #[serde(default = "default_reject_messages_count_tokens")]
    reject_messages_count_tokens: bool,
    #[serde(default = "default_drop_responses_slow_down_errors")]
    drop_responses_slow_down_errors: bool,
    #[serde(default = "default_convert_429_to_503")]
    convert_429_to_503: bool,
    #[serde(default = "default_transparent_retry_count")]
    transparent_retry_count: u32,
    #[serde(default)]
    transparent_retry_head_requests: bool,
    #[serde(default = "default_transparent_retry_backoff_step_ms")]
    transparent_retry_backoff_step_ms: u64,
    #[serde(default = "default_request_body_buffer_max_bytes")]
    request_body_buffer_max_bytes: usize,
    default_provider: String,
    #[serde(default)]
    rewrite: RewriteFile,
    #[serde(default)]
    logging: LoggingFile,
    #[serde(default)]
    statistics: StatisticsFile,
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
    #[serde(default)]
    exchange_log_dir: Option<String>,
    #[serde(default = "default_exchange_body_max_bytes")]
    exchange_body_max_bytes: u64,
    #[serde(default = "default_exchange_body_compression")]
    exchange_body_compression: BodyLogCompression,
    #[serde(default = "default_reconstruct_responses")]
    reconstruct_responses: bool,
    #[serde(default = "default_log_level")]
    level: String,
    #[serde(default)]
    rule: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StatisticsFile {
    #[serde(default = "default_statistics_enabled")]
    enabled: bool,
    #[serde(default = "default_statistics_database_path")]
    database_path: String,
    #[serde(default = "default_statistics_request_body_max_bytes")]
    request_body_max_bytes: usize,
    #[serde(default = "default_statistics_response_body_max_bytes")]
    response_body_max_bytes: usize,
}

impl Default for StatisticsFile {
    fn default() -> Self {
        Self {
            enabled: default_statistics_enabled(),
            database_path: default_statistics_database_path(),
            request_body_max_bytes: default_statistics_request_body_max_bytes(),
            response_body_max_bytes: default_statistics_response_body_max_bytes(),
        }
    }
}

#[derive(Debug, Deserialize, Default)]
struct RewriteFile {
    #[serde(default)]
    model_mappings: Vec<ModelMappingFile>,
}

#[derive(Debug, Deserialize)]
struct ModelMappingFile {
    #[serde(default)]
    provider: Option<Vec<String>>,
    from_model: Vec<String>,
    #[serde(default)]
    from_reasoning_effort: Option<String>,
    #[serde(default)]
    to_model: Option<String>,
    #[serde(default)]
    to_reasoning_effort: Option<String>,
}

fn default_max_body_log_bytes() -> usize {
    8192
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_reconstruct_responses() -> bool {
    true
}

fn default_exchange_body_max_bytes() -> u64 {
    0
}

fn default_exchange_body_compression() -> BodyLogCompression {
    BodyLogCompression::default()
}

fn default_statistics_enabled() -> bool {
    true
}

fn default_statistics_database_path() -> String {
    "./data/statistics.sqlite3".to_string()
}

fn default_statistics_request_body_max_bytes() -> usize {
    1024 * 1024
}

fn default_statistics_response_body_max_bytes() -> usize {
    16 * 1024 * 1024
}

fn default_listen_base_path() -> String {
    "/".to_string()
}

fn default_rpc_listen_addr() -> SocketAddr {
    // Local-only management endpoint by default.
    SocketAddr::from(([127, 0, 0, 1], 8081))
}

fn default_upstream_idle_timeout_secs() -> u64 {
    120
}

fn default_upstream_connect_timeout_secs() -> u64 {
    10
}

fn default_drop_responses_slow_down_errors() -> bool {
    true
}

fn default_convert_429_to_503() -> bool {
    true
}

fn default_reject_messages_count_tokens() -> bool {
    true
}

fn default_transparent_retry_count() -> u32 {
    0
}

fn default_transparent_retry_backoff_step_ms() -> u64 {
    0
}

fn default_request_body_buffer_max_bytes() -> usize {
    64 * 1024 * 1024
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

fn required_non_empty_string(field: &str, value: String) -> Result<String> {
    let value = value.trim().to_string();
    if value.is_empty() {
        return Err(anyhow!("{field} cannot be empty"));
    }
    Ok(value)
}

fn optional_non_empty_string(field: &str, value: Option<String>) -> Result<Option<String>> {
    value
        .map(|value| required_non_empty_string(field, value).map(Some))
        .unwrap_or(Ok(None))
}

fn required_non_empty_strings(field: &str, values: Vec<String>) -> Result<Vec<String>> {
    if values.is_empty() {
        return Err(anyhow!("{field} must contain at least one value"));
    }
    values
        .into_iter()
        .enumerate()
        .map(|(index, value)| required_non_empty_string(&format!("{field}[{index}]"), value))
        .collect()
}

fn optional_non_empty_strings(
    field: &str,
    values: Option<Vec<String>>,
) -> Result<Option<Vec<String>>> {
    values
        .map(|values| required_non_empty_strings(field, values).map(Some))
        .unwrap_or(Ok(None))
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
        let upstream_connect_timeout = match file.upstream_connect_timeout_secs {
            0 => None,
            secs => Some(Duration::from_secs(secs)),
        };
        let upstream_idle_timeout = match file.upstream_idle_timeout_secs {
            0 => None,
            secs => Some(Duration::from_secs(secs)),
        };
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
        let rewrite = normalize_rewrite_config(file.rewrite, &providers)?;

        let exchange_log_dir = match file.logging.exchange_log_dir {
            Some(path) if path.trim().is_empty() => {
                return Err(anyhow!("logging.exchange_log_dir cannot be empty"));
            }
            Some(path) => Some(PathBuf::from(path)),
            None => None,
        };
        let exchange_body_max_bytes = match file.logging.exchange_body_max_bytes {
            0 => None,
            value => Some(value),
        };
        let statistics_database_path = file.statistics.database_path.trim();
        if statistics_database_path.is_empty() {
            return Err(anyhow!("statistics.database_path cannot be empty"));
        }
        if file.statistics.request_body_max_bytes == 0 {
            return Err(anyhow!(
                "statistics.request_body_max_bytes must be greater than 0"
            ));
        }
        if file.statistics.response_body_max_bytes == 0 {
            return Err(anyhow!(
                "statistics.response_body_max_bytes must be greater than 0"
            ));
        }
        if file.request_body_buffer_max_bytes == 0 {
            return Err(anyhow!(
                "request_body_buffer_max_bytes must be greater than 0"
            ));
        }

        Ok(Self {
            listen_addrs,
            listen_base_path,
            rpc_listen_addr: file.rpc_listen_addr,
            rpc_token: file.rpc_token,
            upstream_connect_timeout,
            upstream_idle_timeout,
            reject_messages_count_tokens: file.reject_messages_count_tokens,
            drop_responses_slow_down_errors: file.drop_responses_slow_down_errors,
            convert_429_to_503: file.convert_429_to_503,
            transparent_retry_count: file.transparent_retry_count,
            transparent_retry_head_requests: file.transparent_retry_head_requests,
            transparent_retry_backoff_step: Duration::from_millis(
                file.transparent_retry_backoff_step_ms,
            ),
            request_body_buffer_max_bytes: file.request_body_buffer_max_bytes,
            default_provider: file.default_provider,
            providers,
            rewrite,
            logging: LoggingConfig {
                log_requests: file.logging.log_requests,
                log_responses: file.logging.log_responses,
                log_bodies: file.logging.log_bodies,
                max_body_log_bytes: file.logging.max_body_log_bytes,
                exchange_log_dir,
                exchange_body_max_bytes,
                exchange_body_compression: file.logging.exchange_body_compression,
                reconstruct_responses: file.logging.reconstruct_responses,
                level: file.logging.level,
                rule: file.logging.rule,
            },
            statistics: StatisticsConfig {
                enabled: file.statistics.enabled,
                database_path: PathBuf::from(statistics_database_path),
                request_body_max_bytes: file.statistics.request_body_max_bytes,
                response_body_max_bytes: file.statistics.response_body_max_bytes,
            },
        })
    }
}

fn normalize_rewrite_config(
    rewrite: RewriteFile,
    providers: &HashMap<String, Provider>,
) -> Result<RewriteConfig> {
    let mut model_mappings = Vec::with_capacity(rewrite.model_mappings.len());
    for (index, mapping) in rewrite.model_mappings.into_iter().enumerate() {
        let provider = optional_non_empty_strings(
            &format!("rewrite.model_mappings[{index}].provider"),
            mapping.provider,
        )?;
        if let Some(provider_names) = &provider {
            for provider in provider_names {
                if providers.contains_key(provider) {
                    continue;
                }
                return Err(anyhow!(
                    "rewrite.model_mappings[{index}].provider {provider:?} not present in providers"
                ));
            }
        }

        let to_model = optional_non_empty_string(
            &format!("rewrite.model_mappings[{index}].to_model"),
            mapping.to_model,
        )?;
        let to_reasoning_effort = optional_non_empty_string(
            &format!("rewrite.model_mappings[{index}].to_reasoning_effort"),
            mapping.to_reasoning_effort,
        )?;
        if to_model.is_none() && to_reasoning_effort.is_none() {
            return Err(anyhow!(
                "rewrite.model_mappings[{index}] must set to_model or to_reasoning_effort"
            ));
        }

        model_mappings.push(ModelMapping {
            provider,
            from_model: required_non_empty_strings(
                &format!("rewrite.model_mappings[{index}].from_model"),
                mapping.from_model,
            )?,
            from_reasoning_effort: optional_non_empty_string(
                &format!("rewrite.model_mappings[{index}].from_reasoning_effort"),
                mapping.from_reasoning_effort,
            )?,
            to_model,
            to_reasoning_effort,
        });
    }

    Ok(RewriteConfig { model_mappings })
}

pub fn example_config_toml() -> &'static str {
    include_str!("../../../config.example.toml")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{BodyLogCompression, Config};

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
                upstream_idle_timeout_secs = 30
                default_provider = "provider_a"

                [providers.provider_a]
                base_url = "https://api.example.com/"
                api_key = "replace-me"
            "#,
        )
        .unwrap();

        let addrs: Vec<String> = cfg.listen_addrs.iter().map(ToString::to_string).collect();
        assert_eq!(addrs, vec!["127.0.0.1:8080", "127.0.0.1:8081"]);
        assert_eq!(cfg.upstream_idle_timeout.unwrap().as_secs(), 30);
    }

    #[test]
    fn applies_default_upstream_idle_timeout() {
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

        assert_eq!(cfg.upstream_idle_timeout.unwrap().as_secs(), 120);
    }

    #[test]
    fn applies_default_upstream_connect_timeout() {
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

        assert_eq!(cfg.upstream_connect_timeout.unwrap().as_secs(), 10);
    }

    #[test]
    fn parses_upstream_connect_timeout() {
        let cfg = Config::from_toml_str(
            r#"
                listen_addr = "127.0.0.1:8080"
                upstream_connect_timeout_secs = 3
                default_provider = "provider_a"

                [providers.provider_a]
                base_url = "https://api.example.com/"
                api_key = "replace-me"
            "#,
        )
        .unwrap();

        assert_eq!(cfg.upstream_connect_timeout.unwrap().as_secs(), 3);
    }

    #[test]
    fn allows_disabling_upstream_connect_timeout_with_zero() {
        let cfg = Config::from_toml_str(
            r#"
                listen_addr = "127.0.0.1:8080"
                upstream_connect_timeout_secs = 0
                default_provider = "provider_a"

                [providers.provider_a]
                base_url = "https://api.example.com/"
                api_key = "replace-me"
            "#,
        )
        .unwrap();

        assert!(cfg.upstream_connect_timeout.is_none());
    }

    #[test]
    fn allows_disabling_upstream_idle_timeout_with_zero() {
        let cfg = Config::from_toml_str(
            r#"
                listen_addr = "127.0.0.1:8080"
                upstream_idle_timeout_secs = 0
                default_provider = "provider_a"

                [providers.provider_a]
                base_url = "https://api.example.com/"
                api_key = "replace-me"
            "#,
        )
        .unwrap();

        assert!(cfg.upstream_idle_timeout.is_none());
    }

    #[test]
    fn defaults_transparent_retry_count_to_zero() {
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

        assert!(cfg.reject_messages_count_tokens);
        assert!(cfg.drop_responses_slow_down_errors);
        assert!(cfg.convert_429_to_503);
        assert_eq!(cfg.transparent_retry_count, 0);
        assert!(!cfg.transparent_retry_head_requests);
        assert_eq!(cfg.transparent_retry_backoff_step, Duration::ZERO);
        assert_eq!(cfg.request_body_buffer_max_bytes, 64 * 1024 * 1024);
        assert!(!cfg.rewrite.is_enabled());
    }

    #[test]
    fn parses_request_body_buffer_limit() {
        let cfg = Config::from_toml_str(
            r#"
                listen_addr = "127.0.0.1:8080"
                request_body_buffer_max_bytes = 4096
                default_provider = "provider_a"

                [providers.provider_a]
                base_url = "https://api.example.com/"
                api_key = "replace-me"
            "#,
        )
        .unwrap();

        assert_eq!(cfg.request_body_buffer_max_bytes, 4096);
    }

    #[test]
    fn rejects_zero_request_body_buffer_limit() {
        let error = Config::from_toml_str(
            r#"
                listen_addr = "127.0.0.1:8080"
                request_body_buffer_max_bytes = 0
                default_provider = "provider_a"

                [providers.provider_a]
                base_url = "https://api.example.com/"
                api_key = "replace-me"
            "#,
        )
        .expect_err("zero buffer limit should fail");

        assert!(error
            .to_string()
            .contains("request_body_buffer_max_bytes must be greater than 0"));
    }

    #[test]
    fn parses_disabled_reject_messages_count_tokens() {
        let cfg = Config::from_toml_str(
            r#"
                listen_addr = "127.0.0.1:8080"
                reject_messages_count_tokens = false
                default_provider = "provider_a"

                [providers.provider_a]
                base_url = "https://api.example.com/"
                api_key = "replace-me"
            "#,
        )
        .unwrap();

        assert!(!cfg.reject_messages_count_tokens);
    }

    #[test]
    fn parses_disabled_drop_responses_slow_down_errors() {
        let cfg = Config::from_toml_str(
            r#"
                listen_addr = "127.0.0.1:8080"
                drop_responses_slow_down_errors = false
                default_provider = "provider_a"

                [providers.provider_a]
                base_url = "https://api.example.com/"
                api_key = "replace-me"
            "#,
        )
        .unwrap();

        assert!(!cfg.drop_responses_slow_down_errors);
    }

    #[test]
    fn parses_disabled_convert_429_to_503() {
        let cfg = Config::from_toml_str(
            r#"
                listen_addr = "127.0.0.1:8080"
                convert_429_to_503 = false
                default_provider = "provider_a"

                [providers.provider_a]
                base_url = "https://api.example.com/"
                api_key = "replace-me"
            "#,
        )
        .unwrap();

        assert!(!cfg.convert_429_to_503);
    }

    #[test]
    fn parses_transparent_retry_count() {
        let cfg = Config::from_toml_str(
            r#"
                listen_addr = "127.0.0.1:8080"
                transparent_retry_count = 3
                transparent_retry_head_requests = true
                transparent_retry_backoff_step_ms = 250
                default_provider = "provider_a"

                [providers.provider_a]
                base_url = "https://api.example.com/"
                api_key = "replace-me"
            "#,
        )
        .unwrap();

        assert_eq!(cfg.transparent_retry_count, 3);
        assert!(cfg.transparent_retry_head_requests);
        assert_eq!(
            cfg.transparent_retry_backoff_step,
            Duration::from_millis(250)
        );
    }

    #[test]
    fn parses_request_model_mapping_rewrite_config() {
        let cfg = Config::from_toml_str(
            r#"
                listen_addr = "127.0.0.1:8080"
                default_provider = "provider_a"

                [[rewrite.model_mappings]]
                provider = [" provider_a "]
                from_model = [" gpt-5.5 ", "gpt-5.4"]
                from_reasoning_effort = " xhigh "
                to_model = "grok-4.5"
                to_reasoning_effort = "high"

                [providers.provider_a]
                base_url = "https://api.example.com/"
                api_key = "replace-me"
            "#,
        )
        .unwrap();

        assert!(cfg.rewrite.is_enabled());
        assert_eq!(cfg.rewrite.model_mappings.len(), 1);
        let mapping = &cfg.rewrite.model_mappings[0];
        assert_eq!(
            mapping.provider.as_deref(),
            Some(["provider_a".to_string()].as_slice())
        );
        assert_eq!(mapping.from_model, ["gpt-5.5", "gpt-5.4"]);
        assert_eq!(mapping.from_reasoning_effort.as_deref(), Some("xhigh"));
        assert_eq!(mapping.to_model.as_deref(), Some("grok-4.5"));
        assert_eq!(mapping.to_reasoning_effort.as_deref(), Some("high"));
    }

    #[test]
    fn rejects_request_model_mapping_for_unknown_provider() {
        let err = Config::from_toml_str(
            r#"
                listen_addr = "127.0.0.1:8080"
                default_provider = "provider_a"

                [[rewrite.model_mappings]]
                provider = ["provider_a", "missing"]
                from_model = ["gpt-5.5"]
                to_model = "grok-4.5"

                [providers.provider_a]
                base_url = "https://api.example.com/"
                api_key = "replace-me"
            "#,
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("rewrite.model_mappings[0].provider"));
    }

    #[test]
    fn rejects_request_model_mapping_with_empty_values() {
        let err = Config::from_toml_str(
            r#"
                listen_addr = "127.0.0.1:8080"
                default_provider = "provider_a"

                [[rewrite.model_mappings]]
                from_model = [" "]
                to_model = "grok-4.5"

                [providers.provider_a]
                base_url = "https://api.example.com/"
                api_key = "replace-me"
            "#,
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("rewrite.model_mappings[0].from_model"));
    }

    #[test]
    fn allows_request_model_mapping_with_only_target_effort() {
        let cfg = Config::from_toml_str(
            r#"
                listen_addr = "127.0.0.1:8080"
                default_provider = "provider_a"

                [[rewrite.model_mappings]]
                from_model = ["gpt-5.5", "gpt-5.4"]
                to_reasoning_effort = "high"

                [providers.provider_a]
                base_url = "https://api.example.com/"
                api_key = "replace-me"
            "#,
        )
        .unwrap();

        let mapping = &cfg.rewrite.model_mappings[0];
        assert_eq!(mapping.from_model, ["gpt-5.5", "gpt-5.4"]);
        assert_eq!(mapping.to_model, None);
        assert_eq!(mapping.to_reasoning_effort.as_deref(), Some("high"));
    }

    #[test]
    fn rejects_request_model_mapping_without_target_values() {
        let err = Config::from_toml_str(
            r#"
                listen_addr = "127.0.0.1:8080"
                default_provider = "provider_a"

                [[rewrite.model_mappings]]
                from_model = ["gpt-5.5"]

                [providers.provider_a]
                base_url = "https://api.example.com/"
                api_key = "replace-me"
            "#,
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("must set to_model or to_reasoning_effort"));
    }

    #[test]
    fn defaults_exchange_body_storage_options() {
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

        assert_eq!(cfg.logging.exchange_body_max_bytes, None);
        assert_eq!(
            cfg.logging.exchange_body_compression,
            BodyLogCompression::None
        );
        assert!(cfg.statistics.enabled);
        assert_eq!(
            cfg.statistics.database_path,
            std::path::PathBuf::from("./data/statistics.sqlite3")
        );
        assert_eq!(cfg.statistics.request_body_max_bytes, 1024 * 1024);
        assert_eq!(cfg.statistics.response_body_max_bytes, 16 * 1024 * 1024);
    }

    #[test]
    fn parses_exchange_body_storage_options() {
        let cfg = Config::from_toml_str(
            r#"
                listen_addr = "127.0.0.1:8080"
                default_provider = "provider_a"

                [logging]
                exchange_body_max_bytes = 2048
                exchange_body_compression = "zstd"

                [providers.provider_a]
                base_url = "https://api.example.com/"
                api_key = "replace-me"
            "#,
        )
        .unwrap();

        assert_eq!(cfg.logging.exchange_body_max_bytes, Some(2048));
        assert_eq!(
            cfg.logging.exchange_body_compression,
            BodyLogCompression::Zstd
        );
    }
}
