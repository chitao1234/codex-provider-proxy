//! Provider dialect and per-model capability configuration shared by the proxy config layer
//! and the conversion logic.

use std::collections::HashMap;

use serde::Deserialize;

/// What API dialect an upstream provider speaks.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamApi {
    /// Proxy transparently (no conversion). Default.
    #[default]
    Passthrough,
    /// OpenAI Chat Completions (`POST /chat/completions`).
    #[serde(rename = "openai_chat_completions")]
    OpenAiChatCompletions,
}

/// API dialects a downstream client may speak that this proxy converts from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DownstreamApi {
    /// Anthropic Messages API (`POST /v1/messages`), e.g. Claude Code.
    AnthropicMessages,
}

/// Which output-token field the upstream expects.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaxTokensField {
    /// `max_tokens` (DeepSeek, Grok chat, most OpenAI-compatible providers).
    #[default]
    MaxTokens,
    /// `max_completion_tokens` (OpenAI official new models).
    MaxCompletionTokens,
}

/// How the upstream accepts a top-level thinking control parameter.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThinkingParam {
    /// `thinking: {"type": "enabled"|"disabled"}` (DeepSeek chat).
    #[default]
    TopLevel,
    /// No top-level thinking parameter (OpenAI, Grok); effort maps to `reasoning_effort` only.
    None,
}

/// What to do with Anthropic server-side tool definitions (web_search_*, web_fetch_*,
/// code_execution_*, tool_search_*) that have no Chat Completions equivalent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServerToolPolicy {
    /// Drop server tool definitions (safe default; the model simply cannot call them).
    #[default]
    Drop,
    /// Map web_search/web_fetch server tools to function tools with query/url parameters.
    MapToFunction,
    /// Keep the tool definition object unchanged (for upstreams that recognize the types).
    Passthrough,
    /// Emit the provider's native search tool shape from `search_tool_template` when the
    /// upstream has server-side search in chat completions (e.g. glm web_search object).
    /// web_fetch and other server tools are dropped.
    ProviderNative,
}

/// What `response_format` values the upstream accepts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseFormatCap {
    /// `{"type":"text"}` only.
    Text,
    /// `{"type":"text"}` and `{"type":"json_object"}` (DeepSeek).
    #[default]
    JsonObject,
    /// `text`, `json_object`, and `json_schema` (OpenAI official).
    JsonSchema,
}

/// Reasoning effort configuration for an upstream model.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ReasoningEffortConfig {
    /// Whether to send `reasoning_effort` at all.
    #[serde(default = "default_reasoning_effort_enabled")]
    pub enabled: bool,
    /// Effort levels the upstream accepts, in increasing order (e.g. DeepSeek: low, high, max).
    #[serde(default = "default_reasoning_effort_levels")]
    pub levels: Vec<String>,
    /// Default effort to send when the downstream request does not specify one.
    #[serde(default)]
    pub default: Option<String>,
}

impl Default for ReasoningEffortConfig {
    fn default() -> Self {
        Self {
            enabled: default_reasoning_effort_enabled(),
            levels: default_reasoning_effort_levels(),
            default: None,
        }
    }
}

fn default_reasoning_effort_enabled() -> bool {
    true
}

fn default_reasoning_effort_levels() -> Vec<String> {
    ["low", "medium", "high", "xhigh", "max"]
        .into_iter()
        .map(str::to_owned)
        .collect()
}

/// Per-model (or provider-default) capability knobs the converter consults.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct ModelCapabilities {
    pub max_tokens_field: MaxTokensField,
    pub thinking_param: ThinkingParam,
    pub reasoning_effort: Option<ReasoningEffortConfig>,
    pub server_tools: ServerToolPolicy,
    pub image_input: bool,
    pub parallel_tool_calls: bool,
    pub response_format: ResponseFormatCap,
    /// Whether to request streamed usage (`stream_options.include_usage`).
    pub stream_include_usage: bool,
    /// Upper bound applied to the downstream `max_tokens` (e.g. DeepSeek caps at 393216).
    /// `None` passes the downstream value through unchanged.
    #[serde(default)]
    pub max_tokens_cap: Option<u64>,
    /// Native search tool template for `ServerToolPolicy::ProviderNative` (e.g.
    /// `{"type":"web_search","web_search":{"search_result":true}}` for glm). The optional
    /// `{search_query}` placeholder is replaced with the last user message.
    #[serde(default)]
    pub search_tool_template: Option<serde_json::Value>,
    /// Native search request parameters for `ServerToolPolicy::ProviderNative`, merged into
    /// the top-level request when the upstream enables search via parameters rather than a
    /// tool (e.g. qwen's `{"enable_search": true, "search_options": {...}}`). The optional
    /// `{search_query}` placeholder is replaced with the last user message.
    #[serde(default)]
    pub search_request_params: Option<serde_json::Value>,
}

impl Default for ModelCapabilities {
    fn default() -> Self {
        Self {
            max_tokens_field: MaxTokensField::MaxTokens,
            thinking_param: ThinkingParam::TopLevel,
            reasoning_effort: None,
            server_tools: ServerToolPolicy::Drop,
            image_input: false,
            parallel_tool_calls: true,
            response_format: ResponseFormatCap::JsonObject,
            stream_include_usage: true,
            max_tokens_cap: None,
            search_tool_template: None,
            search_request_params: None,
        }
    }
}

impl ModelCapabilities {
    /// Resolve the effective capabilities for a model: model-level override if present,
    /// otherwise the provider default, otherwise the built-in default.
    pub fn resolve(
        model: &str,
        provider_default: Option<&ModelCapabilities>,
        model_overrides: &HashMap<String, ModelCapabilities>,
    ) -> Self {
        model_overrides
            .get(model)
            .cloned()
            .or_else(|| provider_default.cloned())
            .unwrap_or_default()
    }

    /// Clamp a Claude effort level to the closest level this upstream accepts.
    ///
    /// Mirrors DeepSeek's documented behavior: medium/xhigh map to high when high is supported;
    /// values above the top accepted level map to the top.
    pub fn clamp_effort(&self, effort: &str) -> Option<String> {
        let levels = self.reasoning_effort.as_ref()?.levels.as_slice();
        if levels.is_empty() {
            return None;
        }
        if levels.iter().any(|level| level == effort) {
            return Some(effort.to_string());
        }
        let top = levels.last().map(String::as_str).unwrap_or_default();
        let clamped = match effort {
            "minimal" | "low" => levels.first().map(String::as_str).unwrap_or(top),
            "medium" => levels
                .iter()
                .find(|level| level.as_str() == "high")
                .map(String::as_str)
                .unwrap_or(top),
            "high" => levels
                .iter()
                .find(|level| level.as_str() == "high")
                .map(String::as_str)
                .unwrap_or(top),
            "xhigh" => levels
                .iter()
                .find(|level| level.as_str() == "high")
                .map(String::as_str)
                .unwrap_or(top),
            _ => top,
        };
        Some(clamped.to_string())
    }
}

/// Whether a provider converts downstream Anthropic Messages requests to its upstream API.
pub fn converts_messages_to_upstream(
    upstream_api: UpstreamApi,
    accepted: &[DownstreamApi],
) -> bool {
    upstream_api != UpstreamApi::Passthrough && accepted.contains(&DownstreamApi::AnthropicMessages)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_effort_matches_deepseek_levels() {
        let caps = ModelCapabilities {
            reasoning_effort: Some(ReasoningEffortConfig {
                enabled: true,
                levels: vec!["low".into(), "high".into(), "max".into()],
                default: Some("high".into()),
            }),
            ..ModelCapabilities::default()
        };
        assert_eq!(caps.clamp_effort("low").as_deref(), Some("low"));
        assert_eq!(caps.clamp_effort("medium").as_deref(), Some("high"));
        assert_eq!(caps.clamp_effort("high").as_deref(), Some("high"));
        assert_eq!(caps.clamp_effort("xhigh").as_deref(), Some("high"));
        assert_eq!(caps.clamp_effort("max").as_deref(), Some("max"));
    }

    #[test]
    fn clamp_effort_passes_through_openai_levels() {
        let caps = ModelCapabilities {
            reasoning_effort: Some(ReasoningEffortConfig::default()),
            ..ModelCapabilities::default()
        };
        assert_eq!(caps.clamp_effort("medium").as_deref(), Some("medium"));
        assert_eq!(caps.clamp_effort("xhigh").as_deref(), Some("xhigh"));
    }

    #[test]
    fn clamp_effort_disabled_when_no_config() {
        let caps = ModelCapabilities::default();
        assert_eq!(caps.clamp_effort("high"), None);
    }

    #[test]
    fn resolve_prefers_model_override_over_provider_default() {
        let provider_default = ModelCapabilities {
            image_input: true,
            ..ModelCapabilities::default()
        };
        let overrides = HashMap::from([(
            "deepseek-v4-pro".to_string(),
            ModelCapabilities {
                image_input: false,
                ..ModelCapabilities::default()
            },
        )]);
        let caps =
            ModelCapabilities::resolve("deepseek-v4-pro", Some(&provider_default), &overrides);
        assert!(!caps.image_input);
        let other =
            ModelCapabilities::resolve("deepseek-v4-flash", Some(&provider_default), &overrides);
        assert!(other.image_input);
        let unknown = ModelCapabilities::resolve("unknown-model", None, &overrides);
        assert!(!unknown.image_input);
    }

    #[test]
    fn converts_messages_to_upstream_requires_both_sides() {
        assert!(converts_messages_to_upstream(
            UpstreamApi::OpenAiChatCompletions,
            &[DownstreamApi::AnthropicMessages]
        ));
        assert!(!converts_messages_to_upstream(
            UpstreamApi::Passthrough,
            &[DownstreamApi::AnthropicMessages]
        ));
        assert!(!converts_messages_to_upstream(
            UpstreamApi::OpenAiChatCompletions,
            &[]
        ));
    }
}
