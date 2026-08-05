use bytes::Bytes;
use http::{header, HeaderMap, Method};
use serde_json::{Map, Value};

use crate::config::{Config, ModelMapping};

pub struct RequestRewriteContext<'a> {
    pub method: &'a Method,
    pub forwarded_path: &'a str,
    pub provider_name: &'a str,
    pub headers: &'a HeaderMap,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestRewriteOutcome {
    pub body: Bytes,
    pub body_changed: bool,
    pub applied_model_mapping: Option<AppliedModelMapping>,
    pub anthropic_beta_updates: Vec<AnthropicBetaUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedModelMapping {
    pub from_model: String,
    pub from_reasoning_effort: Option<String>,
    pub to_model: Option<String>,
    pub to_reasoning_effort: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnthropicBetaUpdate {
    Ensure(AnthropicBetaMarker),
    RemoveByPrefix(AnthropicBetaPrefix),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnthropicBetaMarker {
    Effort,
    Context1m,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnthropicBetaPrefix {
    Context1m,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelEndpoint {
    Responses,
    Messages,
    ChatCompletions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetFieldResult {
    Missing,
    Unchanged,
    Changed,
}

const CLAUDE_CONTEXT_1M_MODEL_SUFFIX: &str = "[1m]";
const ANTHROPIC_CONTEXT_1M_BETA_PREFIX: &str = "context-1m";

pub fn request_rewrites_may_apply(cfg: &Config, ctx: &RequestRewriteContext<'_>) -> bool {
    cfg.rewrite.is_enabled()
        && *ctx.method == Method::POST
        && model_endpoint(ctx.forwarded_path).is_some()
        && request_content_encoding_allows_json(ctx.headers)
}

pub fn apply_request_rewrites(
    cfg: &Config,
    ctx: &RequestRewriteContext<'_>,
    body: Bytes,
) -> RequestRewriteOutcome {
    if !request_rewrites_may_apply(cfg, ctx) {
        return passthrough(body);
    }

    let Some(endpoint) = model_endpoint(ctx.forwarded_path) else {
        return passthrough(body);
    };
    let Ok(mut json) = serde_json::from_slice::<Value>(&body) else {
        return passthrough(body);
    };
    if !json.is_object() {
        return passthrough(body);
    }

    let Some(model) = top_level_string(&json, "model").map(str::to_owned) else {
        return passthrough(body);
    };
    let current_model = current_model(endpoint, &model, ctx.headers);
    let reasoning_effort = current_reasoning_effort(&json);
    let Some(mapping) = select_model_mapping(
        &cfg.rewrite.model_mappings,
        ctx.provider_name,
        &model,
        &current_model,
        reasoning_effort.as_deref(),
    ) else {
        return passthrough(body);
    };

    let mut body_changed = false;
    let mut anthropic_beta_updates = Vec::new();
    if let Some(to_model) = &mapping.to_model {
        let target_model = target_body_model(endpoint, to_model);
        body_changed |= set_top_level_string(&mut json, "model", &target_model);
        if endpoint == ModelEndpoint::Messages {
            update_messages_context_1m_beta(
                ctx.headers,
                &current_model,
                to_model,
                &mut anthropic_beta_updates,
            );
        }
    }
    if let Some(to_reasoning_effort) = &mapping.to_reasoning_effort {
        body_changed |= set_reasoning_effort(&mut json, endpoint, to_reasoning_effort);
        if endpoint == ModelEndpoint::Messages {
            push_unique_anthropic_beta_update(
                &mut anthropic_beta_updates,
                AnthropicBetaUpdate::Ensure(AnthropicBetaMarker::Effort),
            );
        }
    }

    let applied_model_mapping = Some(AppliedModelMapping {
        from_model: current_model,
        from_reasoning_effort: reasoning_effort,
        to_model: mapping.to_model.clone(),
        to_reasoning_effort: mapping.to_reasoning_effort.clone(),
    });

    if !body_changed {
        return RequestRewriteOutcome {
            body,
            body_changed,
            applied_model_mapping,
            anthropic_beta_updates,
        };
    }

    match serde_json::to_vec(&json) {
        Ok(rewritten) => RequestRewriteOutcome {
            body: Bytes::from(rewritten),
            body_changed,
            applied_model_mapping,
            anthropic_beta_updates,
        },
        Err(_) => RequestRewriteOutcome {
            body,
            body_changed: false,
            applied_model_mapping: None,
            anthropic_beta_updates: Vec::new(),
        },
    }
}

fn passthrough(body: Bytes) -> RequestRewriteOutcome {
    RequestRewriteOutcome {
        body,
        body_changed: false,
        applied_model_mapping: None,
        anthropic_beta_updates: Vec::new(),
    }
}

fn model_endpoint(path: &str) -> Option<ModelEndpoint> {
    let path = path.trim_matches('/');
    if path.is_empty() {
        return None;
    }
    if path == "chat/completions" || path.ends_with("/chat/completions") {
        return Some(ModelEndpoint::ChatCompletions);
    }
    if path == "responses" || path.ends_with("/responses") {
        return Some(ModelEndpoint::Responses);
    }
    if path == "messages" || path.ends_with("/messages") {
        return Some(ModelEndpoint::Messages);
    }
    None
}

fn request_content_encoding_allows_json(headers: &HeaderMap) -> bool {
    let Some(value) = headers.get(header::CONTENT_ENCODING) else {
        return true;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    value
        .split(',')
        .map(str::trim)
        .all(|coding| coding.is_empty() || coding.eq_ignore_ascii_case("identity"))
}

fn select_model_mapping<'a>(
    mappings: &'a [ModelMapping],
    provider_name: &str,
    body_model: &str,
    current_model: &str,
    reasoning_effort: Option<&str>,
) -> Option<&'a ModelMapping> {
    let mut best = None;
    let mut best_score = (false, false, false);

    for mapping in mappings {
        let Some(model_specific) = mapping_matches(
            mapping,
            provider_name,
            body_model,
            current_model,
            reasoning_effort,
        ) else {
            continue;
        };

        let score = (
            mapping.provider.is_some(),
            mapping.from_reasoning_effort.is_some(),
            model_specific,
        );
        if best.is_none() || score > best_score {
            best = Some(mapping);
            best_score = score;
        }
    }

    best
}

fn mapping_matches(
    mapping: &ModelMapping,
    provider_name: &str,
    body_model: &str,
    current_model: &str,
    reasoning_effort: Option<&str>,
) -> Option<bool> {
    if mapping
        .provider
        .as_ref()
        .is_some_and(|providers| !providers.iter().any(|provider| provider == provider_name))
    {
        return None;
    }
    let model_specific = if mapping
        .from_model
        .iter()
        .any(|model| model == current_model)
    {
        true
    } else if current_model != body_model
        && mapping.from_model.iter().any(|model| model == body_model)
    {
        false
    } else {
        return None;
    };
    match mapping.from_reasoning_effort.as_deref() {
        Some(expected) if reasoning_effort != Some(expected) => None,
        _ => Some(model_specific),
    }
}

fn top_level_string<'a>(json: &'a Value, field: &str) -> Option<&'a str> {
    json.as_object()?.get(field)?.as_str()
}

fn current_model(endpoint: ModelEndpoint, body_model: &str, headers: &HeaderMap) -> String {
    if endpoint != ModelEndpoint::Messages {
        return body_model.to_string();
    }
    if split_context_1m_model(body_model).is_some() || !headers_have_context_1m_beta(headers) {
        return body_model.to_string();
    }
    format!("{body_model}{CLAUDE_CONTEXT_1M_MODEL_SUFFIX}")
}

fn target_body_model(endpoint: ModelEndpoint, model: &str) -> String {
    if endpoint == ModelEndpoint::Messages {
        if let Some(base_model) = split_context_1m_model(model) {
            return base_model.to_string();
        }
    }
    model.to_string()
}

fn update_messages_context_1m_beta(
    headers: &HeaderMap,
    current_model: &str,
    target_model: &str,
    updates: &mut Vec<AnthropicBetaUpdate>,
) {
    if split_context_1m_model(target_model).is_some() {
        push_unique_anthropic_beta_update(
            updates,
            AnthropicBetaUpdate::Ensure(AnthropicBetaMarker::Context1m),
        );
    } else if split_context_1m_model(current_model).is_some()
        || headers_have_context_1m_beta(headers)
    {
        push_unique_anthropic_beta_update(
            updates,
            AnthropicBetaUpdate::RemoveByPrefix(AnthropicBetaPrefix::Context1m),
        );
    }
}

fn split_context_1m_model(model: &str) -> Option<&str> {
    let base = model.strip_suffix(CLAUDE_CONTEXT_1M_MODEL_SUFFIX)?;
    if base.is_empty() {
        None
    } else {
        Some(base)
    }
}

fn headers_have_context_1m_beta(headers: &HeaderMap) -> bool {
    headers.get_all("anthropic-beta").iter().any(|value| {
        value.to_str().ok().is_some_and(|value| {
            value
                .split(',')
                .map(str::trim)
                .any(beta_token_is_context_1m)
        })
    })
}

fn beta_token_is_context_1m(beta: &str) -> bool {
    beta.eq_ignore_ascii_case(ANTHROPIC_CONTEXT_1M_BETA_PREFIX)
        || beta
            .get(..ANTHROPIC_CONTEXT_1M_BETA_PREFIX.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(ANTHROPIC_CONTEXT_1M_BETA_PREFIX))
            && beta.as_bytes().get(ANTHROPIC_CONTEXT_1M_BETA_PREFIX.len()) == Some(&b'-')
}

fn push_unique_anthropic_beta_update(
    updates: &mut Vec<AnthropicBetaUpdate>,
    update: AnthropicBetaUpdate,
) {
    if !updates.contains(&update) {
        updates.push(update);
    }
}

fn current_reasoning_effort(json: &Value) -> Option<String> {
    json.pointer("/reasoning/effort")
        .and_then(Value::as_str)
        .or_else(|| json.get("reasoning_effort").and_then(Value::as_str))
        .or_else(|| {
            json.pointer("/output_config/effort")
                .and_then(Value::as_str)
        })
        .map(str::to_owned)
}

fn set_top_level_string(json: &mut Value, field: &str, value: &str) -> bool {
    let Some(object) = json.as_object_mut() else {
        return false;
    };
    set_string_field(object, field, value) == SetFieldResult::Changed
}

fn set_reasoning_effort(json: &mut Value, endpoint: ModelEndpoint, effort: &str) -> bool {
    match set_existing_reasoning_effort(json, effort) {
        SetFieldResult::Changed => return true,
        SetFieldResult::Unchanged => return false,
        SetFieldResult::Missing => {}
    }
    match set_existing_top_level_reasoning_effort(json, effort) {
        SetFieldResult::Changed => return true,
        SetFieldResult::Unchanged => return false,
        SetFieldResult::Missing => {}
    }
    match set_existing_output_config_effort(json, effort) {
        SetFieldResult::Changed => return true,
        SetFieldResult::Unchanged => return false,
        SetFieldResult::Missing => {}
    }
    if json
        .get("reasoning")
        .is_some_and(|reasoning| reasoning.is_object())
    {
        return set_reasoning_effort_object(json, effort);
    }

    match endpoint {
        ModelEndpoint::Responses => set_reasoning_effort_object(json, effort),
        ModelEndpoint::Messages => set_output_config_effort_object(json, effort),
        ModelEndpoint::ChatCompletions => set_top_level_string(json, "reasoning_effort", effort),
    }
}

fn set_existing_reasoning_effort(json: &mut Value, effort: &str) -> SetFieldResult {
    let Some(reasoning) = json.get_mut("reasoning").and_then(Value::as_object_mut) else {
        return SetFieldResult::Missing;
    };
    if !reasoning.contains_key("effort") {
        return SetFieldResult::Missing;
    }
    set_string_field(reasoning, "effort", effort)
}

fn set_existing_top_level_reasoning_effort(json: &mut Value, effort: &str) -> SetFieldResult {
    let Some(object) = json.as_object_mut() else {
        return SetFieldResult::Missing;
    };
    if !object.contains_key("reasoning_effort") {
        return SetFieldResult::Missing;
    }
    set_string_field(object, "reasoning_effort", effort)
}

fn set_existing_output_config_effort(json: &mut Value, effort: &str) -> SetFieldResult {
    let Some(output_config) = json.get_mut("output_config").and_then(Value::as_object_mut) else {
        return SetFieldResult::Missing;
    };
    if !output_config.contains_key("effort") {
        return SetFieldResult::Missing;
    }
    set_string_field(output_config, "effort", effort)
}

fn set_reasoning_effort_object(json: &mut Value, effort: &str) -> bool {
    let Some(object) = json.as_object_mut() else {
        return false;
    };
    let reasoning = object
        .entry("reasoning".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    if !reasoning.is_object() {
        *reasoning = Value::Object(Map::new());
    }
    let Some(reasoning) = reasoning.as_object_mut() else {
        return false;
    };
    set_string_field(reasoning, "effort", effort) == SetFieldResult::Changed
}

fn set_output_config_effort_object(json: &mut Value, effort: &str) -> bool {
    let Some(object) = json.as_object_mut() else {
        return false;
    };
    let output_config = object
        .entry("output_config".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    if !output_config.is_object() {
        *output_config = Value::Object(Map::new());
    }
    let Some(output_config) = output_config.as_object_mut() else {
        return false;
    };
    set_string_field(output_config, "effort", effort) == SetFieldResult::Changed
}

fn set_string_field(object: &mut Map<String, Value>, field: &str, value: &str) -> SetFieldResult {
    match object.get(field) {
        Some(Value::String(current)) if current == value => SetFieldResult::Unchanged,
        Some(_) => {
            object.insert(field.to_string(), Value::String(value.to_string()));
            SetFieldResult::Changed
        }
        None => {
            object.insert(field.to_string(), Value::String(value.to_string()));
            SetFieldResult::Changed
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, net::SocketAddr, time::Duration};

    use bytes::Bytes;
    use http::{header, HeaderMap, Method};
    use serde_json::json;
    use url::Url;

    use crate::config::{
        BodyLogCompression, Config, LoggingConfig, ModelMapping, Provider, RewriteConfig,
        StatisticsConfig,
    };

    use super::{
        apply_request_rewrites, request_rewrites_may_apply, AnthropicBetaMarker,
        AnthropicBetaPrefix, AnthropicBetaUpdate, RequestRewriteContext,
    };

    fn test_config(model_mappings: Vec<ModelMapping>) -> Config {
        Config {
            listen_addrs: vec![SocketAddr::from(([127, 0, 0, 1], 8080))],
            listen_base_path: "/".to_string(),
            rpc_listen_addr: SocketAddr::from(([127, 0, 0, 1], 8081)),
            rpc_token: None,
            upstream_connect_timeout: None,
            upstream_idle_timeout: None,
            reject_messages_count_tokens: true,
            drop_responses_slow_down_errors: true,
            convert_429_to_503: true,
            transparent_retry_count: 0,
            transparent_retry_head_requests: false,
            transparent_retry_backoff_step: Duration::ZERO,
            request_body_buffer_max_bytes: 64 * 1024 * 1024,
            default_provider: "provider_a".to_string(),
            providers: HashMap::from([(
                "provider_a".to_string(),
                Provider {
                    base_url: Url::parse("https://api.example.com/").unwrap(),
                    api_key: "replace-me".to_string(),
                    authorization_header: None,
                },
            )]),
            rewrite: RewriteConfig { model_mappings },
            logging: LoggingConfig {
                log_requests: false,
                log_responses: false,
                log_bodies: false,
                max_body_log_bytes: 8192,
                exchange_log_dir: None,
                exchange_body_max_bytes: None,
                exchange_body_compression: BodyLogCompression::None,
                reconstruct_responses: false,
                level: "info".to_string(),
                rule: None,
            },
            statistics: StatisticsConfig {
                enabled: false,
                database_path: "unused.sqlite3".into(),
                request_body_max_bytes: 1024,
                response_body_max_bytes: 1024,
            },
        }
    }

    fn ctx<'a>(
        method: &'a Method,
        forwarded_path: &'a str,
        provider_name: &'a str,
        headers: &'a HeaderMap,
    ) -> RequestRewriteContext<'a> {
        RequestRewriteContext {
            method,
            forwarded_path,
            provider_name,
            headers,
        }
    }

    fn mapping(
        from_model: &str,
        from_effort: Option<&str>,
        to_model: &str,
        to_effort: Option<&str>,
    ) -> ModelMapping {
        ModelMapping {
            provider: None,
            from_model: vec![from_model.to_string()],
            from_reasoning_effort: from_effort.map(str::to_string),
            to_model: Some(to_model.to_string()),
            to_reasoning_effort: to_effort.map(str::to_string),
        }
    }

    #[test]
    fn disabled_config_is_lazy_passthrough() {
        let cfg = test_config(Vec::new());
        let headers = HeaderMap::new();
        let body = Bytes::from_static(br#"{"model":"gpt-5.5"}"#);

        assert!(!request_rewrites_may_apply(
            &cfg,
            &ctx(&Method::POST, "/v1/responses", "provider_a", &headers)
        ));
        let out = apply_request_rewrites(
            &cfg,
            &ctx(&Method::POST, "/v1/responses", "provider_a", &headers),
            body.clone(),
        );

        assert!(!out.body_changed);
        assert_eq!(out.body, body);
        assert!(out.applied_model_mapping.is_none());
        assert!(out.anthropic_beta_updates.is_empty());
    }

    #[test]
    fn malformed_json_request_body_is_passthrough() {
        let cfg = test_config(vec![mapping("gpt-5.5", None, "grok-4.5", Some("high"))]);
        let headers = HeaderMap::new();
        let body = Bytes::from_static(br#"{"model":"gpt-5.5""#);

        let out = apply_request_rewrites(
            &cfg,
            &ctx(&Method::POST, "/v1/responses", "provider_a", &headers),
            body.clone(),
        );

        assert_eq!(out.body, body);
        assert!(!out.body_changed);
        assert!(out.applied_model_mapping.is_none());
        assert!(out.anthropic_beta_updates.is_empty());
    }

    #[test]
    fn non_object_json_request_body_is_passthrough() {
        let cfg = test_config(vec![mapping("gpt-5.5", None, "grok-4.5", Some("high"))]);
        let headers = HeaderMap::new();
        let body = Bytes::from_static(br#"["gpt-5.5"]"#);

        let out = apply_request_rewrites(
            &cfg,
            &ctx(&Method::POST, "/v1/responses", "provider_a", &headers),
            body.clone(),
        );

        assert_eq!(out.body, body);
        assert!(!out.body_changed);
        assert!(out.applied_model_mapping.is_none());
        assert!(out.anthropic_beta_updates.is_empty());
    }

    #[test]
    fn wrong_typed_effort_containers_are_replaced_without_panicking() {
        let cfg = test_config(vec![mapping(
            "claude-sonnet-4-6",
            None,
            "claude-opus-4-7",
            Some("max"),
        )]);
        let headers = HeaderMap::new();
        let body = Bytes::from(
            json!({
                "model": "claude-sonnet-4-6",
                "output_config": "malformed",
                "thinking": 42,
                "max_tokens": 32000,
                "messages": []
            })
            .to_string(),
        );

        let out = apply_request_rewrites(
            &cfg,
            &ctx(&Method::POST, "/v1/messages", "provider_a", &headers),
            body,
        );
        let rewritten: serde_json::Value = serde_json::from_slice(&out.body).unwrap();

        assert!(out.body_changed);
        assert_eq!(
            out.anthropic_beta_updates,
            vec![AnthropicBetaUpdate::Ensure(AnthropicBetaMarker::Effort)]
        );
        assert_eq!(rewritten["model"], "claude-opus-4-7");
        assert_eq!(rewritten["output_config"]["effort"], "max");
    }

    #[test]
    fn rewrites_responses_model_and_reasoning_effort_shape_from_logs() {
        let cfg = test_config(vec![mapping(
            "gpt-5.5",
            Some("xhigh"),
            "grok-4.5",
            Some("high"),
        )]);
        let headers = HeaderMap::new();
        let body = Bytes::from(
            json!({
                "model": "gpt-5.5",
                "reasoning": {"effort": "xhigh", "summary": "auto"},
                "stream": true,
                "input": []
            })
            .to_string(),
        );

        let out = apply_request_rewrites(
            &cfg,
            &ctx(&Method::POST, "/v1/responses", "provider_a", &headers),
            body,
        );
        let rewritten: serde_json::Value = serde_json::from_slice(&out.body).unwrap();

        assert!(out.body_changed);
        assert!(out.anthropic_beta_updates.is_empty());
        assert_eq!(rewritten["model"], "grok-4.5");
        assert_eq!(rewritten["reasoning"]["effort"], "high");
        assert_eq!(rewritten["reasoning"]["summary"], "auto");
    }

    #[test]
    fn rewrites_messages_model_and_output_config_effort_shape_from_claude_code_logs() {
        let cfg = test_config(vec![mapping(
            "claude-sonnet-4-6",
            Some("low"),
            "claude-opus-4-7",
            Some("max"),
        )]);
        let headers = HeaderMap::new();
        let body = Bytes::from(
            json!({
                "model": "claude-sonnet-4-6",
                "thinking": {"type": "adaptive"},
                "output_config": {"effort": "low"},
                "max_tokens": 32000,
                "messages": []
            })
            .to_string(),
        );

        let out = apply_request_rewrites(
            &cfg,
            &ctx(&Method::POST, "/v1/messages", "provider_a", &headers),
            body,
        );
        let rewritten: serde_json::Value = serde_json::from_slice(&out.body).unwrap();

        assert!(out.body_changed);
        assert_eq!(
            out.anthropic_beta_updates,
            vec![AnthropicBetaUpdate::Ensure(AnthropicBetaMarker::Effort)]
        );
        assert_eq!(rewritten["model"], "claude-opus-4-7");
        assert_eq!(rewritten["thinking"]["type"], "adaptive");
        assert_eq!(rewritten["output_config"]["effort"], "max");
    }

    #[test]
    fn rewrites_messages_effort_without_overwriting_thinking_type() {
        let cfg = test_config(vec![mapping(
            "claude-fable-5",
            None,
            "claude-sonnet-5",
            Some("max"),
        )]);
        let headers = HeaderMap::new();
        let body = Bytes::from(
            json!({
                "model": "claude-fable-5",
                "thinking": {"type": "adaptive"},
                "max_tokens": 1,
                "messages": []
            })
            .to_string(),
        );

        let out = apply_request_rewrites(
            &cfg,
            &ctx(&Method::POST, "/v1/messages", "provider_a", &headers),
            body,
        );
        let rewritten: serde_json::Value = serde_json::from_slice(&out.body).unwrap();

        assert!(out.body_changed);
        assert_eq!(
            out.anthropic_beta_updates,
            vec![AnthropicBetaUpdate::Ensure(AnthropicBetaMarker::Effort)]
        );
        assert_eq!(rewritten["model"], "claude-sonnet-5");
        assert_eq!(rewritten["thinking"]["type"], "adaptive");
        assert_eq!(rewritten["output_config"]["effort"], "max");
    }

    #[test]
    fn rewrites_messages_model_to_claude_context_1m_header_variant() {
        let cfg = test_config(vec![mapping(
            "claude-sonnet-5",
            None,
            "claude-sonnet-5[1m]",
            None,
        )]);
        let headers = HeaderMap::new();
        let body = Bytes::from(
            json!({
                "model": "claude-sonnet-5",
                "thinking": {"type": "adaptive"},
                "output_config": {"effort": "xhigh"},
                "max_tokens": 32000,
                "messages": []
            })
            .to_string(),
        );

        let out = apply_request_rewrites(
            &cfg,
            &ctx(&Method::POST, "/v1/messages", "provider_a", &headers),
            body.clone(),
        );

        assert!(!out.body_changed);
        assert_eq!(out.body, body);
        assert_eq!(
            out.anthropic_beta_updates,
            vec![AnthropicBetaUpdate::Ensure(AnthropicBetaMarker::Context1m)]
        );
        let applied = out.applied_model_mapping.unwrap();
        assert_eq!(applied.from_model, "claude-sonnet-5");
        assert_eq!(applied.to_model.as_deref(), Some("claude-sonnet-5[1m]"));
    }

    #[test]
    fn rewrites_messages_model_from_claude_context_1m_header_variant() {
        let cfg = test_config(vec![mapping(
            "claude-sonnet-5[1m]",
            None,
            "claude-sonnet-5",
            None,
        )]);
        let mut headers = HeaderMap::new();
        headers.insert(
            "anthropic-beta",
            "claude-code-20250219,context-1m-2025-08-07,effort-2025-11-24"
                .parse()
                .unwrap(),
        );
        let body = Bytes::from(
            json!({
                "model": "claude-sonnet-5",
                "thinking": {"type": "adaptive"},
                "output_config": {"effort": "xhigh"},
                "max_tokens": 32000,
                "messages": []
            })
            .to_string(),
        );

        let out = apply_request_rewrites(
            &cfg,
            &ctx(&Method::POST, "/v1/messages", "provider_a", &headers),
            body.clone(),
        );

        assert!(!out.body_changed);
        assert_eq!(out.body, body);
        assert_eq!(
            out.anthropic_beta_updates,
            vec![AnthropicBetaUpdate::RemoveByPrefix(
                AnthropicBetaPrefix::Context1m
            )]
        );
        let applied = out.applied_model_mapping.unwrap();
        assert_eq!(applied.from_model, "claude-sonnet-5[1m]");
        assert_eq!(applied.to_model.as_deref(), Some("claude-sonnet-5"));
    }

    #[test]
    fn messages_context_1m_model_mapping_beats_base_model_mapping_at_same_specificity() {
        let cfg = test_config(vec![
            mapping("claude-sonnet-5", None, "base-target", None),
            mapping("claude-sonnet-5[1m]", None, "variant-target", None),
        ]);
        let mut headers = HeaderMap::new();
        headers.insert(
            "anthropic-beta",
            "claude-code-20250219,context-1m-2025-08-07"
                .parse()
                .unwrap(),
        );
        let body = Bytes::from(
            json!({
                "model": "claude-sonnet-5",
                "messages": []
            })
            .to_string(),
        );

        let out = apply_request_rewrites(
            &cfg,
            &ctx(&Method::POST, "/v1/messages", "provider_a", &headers),
            body,
        );
        let rewritten: serde_json::Value = serde_json::from_slice(&out.body).unwrap();

        assert_eq!(rewritten["model"], "variant-target");
        assert_eq!(
            out.anthropic_beta_updates,
            vec![AnthropicBetaUpdate::RemoveByPrefix(
                AnthropicBetaPrefix::Context1m
            )]
        );
        let applied = out.applied_model_mapping.unwrap();
        assert_eq!(applied.from_model, "claude-sonnet-5[1m]");
        assert_eq!(applied.to_model.as_deref(), Some("variant-target"));
    }

    #[test]
    fn rewrites_chat_completions_model_and_adds_reasoning_effort_shape_from_logs() {
        let cfg = test_config(vec![mapping(
            "grok-4.5",
            None,
            "grok-4.20-non-reasoning",
            Some("low"),
        )]);
        let headers = HeaderMap::new();
        let body = Bytes::from(
            json!({
                "model": "grok-4.5",
                "messages": [],
                "stream": true,
                "stream_options": {"include_usage": true}
            })
            .to_string(),
        );

        let out = apply_request_rewrites(
            &cfg,
            &ctx(
                &Method::POST,
                "/v1/chat/completions",
                "provider_a",
                &headers,
            ),
            body,
        );
        let rewritten: serde_json::Value = serde_json::from_slice(&out.body).unwrap();

        assert!(out.body_changed);
        assert_eq!(rewritten["model"], "grok-4.20-non-reasoning");
        assert_eq!(rewritten["reasoning_effort"], "low");
    }

    #[test]
    fn provider_and_effort_specific_mapping_wins() {
        let mut provider_mapping = mapping("gpt-5.5", Some("xhigh"), "provider-specific", None);
        provider_mapping.provider = Some(vec!["provider_a".to_string(), "provider_b".to_string()]);
        let cfg = test_config(vec![
            mapping("gpt-5.5", None, "model-only", None),
            mapping("gpt-5.5", Some("xhigh"), "effort-specific", None),
            provider_mapping,
        ]);
        let headers = HeaderMap::new();
        let body =
            Bytes::from(json!({"model": "gpt-5.5", "reasoning": {"effort": "xhigh"}}).to_string());

        let out = apply_request_rewrites(
            &cfg,
            &ctx(&Method::POST, "/v1/responses", "provider_a", &headers),
            body,
        );
        let rewritten: serde_json::Value = serde_json::from_slice(&out.body).unwrap();

        assert_eq!(rewritten["model"], "provider-specific");
    }

    #[test]
    fn mapping_matches_any_configured_source_model() {
        let mut model_mapping = mapping("gpt-5.5", None, "grok-4.5", None);
        model_mapping.from_model.push("gpt-5.4".to_string());
        let cfg = test_config(vec![model_mapping]);
        let headers = HeaderMap::new();
        let body = Bytes::from_static(br#"{"model":"gpt-5.4"}"#);

        let out = apply_request_rewrites(
            &cfg,
            &ctx(&Method::POST, "/v1/responses", "provider_a", &headers),
            body,
        );
        let rewritten: serde_json::Value = serde_json::from_slice(&out.body).unwrap();

        assert!(out.body_changed);
        assert_eq!(rewritten["model"], "grok-4.5");
    }

    #[test]
    fn mapping_can_rewrite_effort_without_rewriting_model() {
        let cfg = test_config(vec![ModelMapping {
            provider: None,
            from_model: vec!["gpt-5.5".to_string()],
            from_reasoning_effort: None,
            to_model: None,
            to_reasoning_effort: Some("high".to_string()),
        }]);
        let headers = HeaderMap::new();
        let body = Bytes::from_static(
            br#"{"model":"gpt-5.5","reasoning":{"effort":"low","summary":"auto"}}"#,
        );

        let out = apply_request_rewrites(
            &cfg,
            &ctx(&Method::POST, "/v1/responses", "provider_a", &headers),
            body,
        );
        let rewritten: serde_json::Value = serde_json::from_slice(&out.body).unwrap();

        assert!(out.body_changed);
        assert_eq!(rewritten["model"], "gpt-5.5");
        assert_eq!(rewritten["reasoning"]["effort"], "high");
        assert_eq!(rewritten["reasoning"]["summary"], "auto");
        assert_eq!(out.applied_model_mapping.unwrap().to_model, None);
    }

    #[test]
    fn compressed_request_bodies_are_not_rewritten() {
        let cfg = test_config(vec![mapping("gpt-5.5", None, "grok-4.5", None)]);
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_ENCODING, "gzip".parse().unwrap());
        let body = Bytes::from_static(br#"{"model":"gpt-5.5"}"#);

        let out = apply_request_rewrites(
            &cfg,
            &ctx(&Method::POST, "/v1/responses", "provider_a", &headers),
            body.clone(),
        );

        assert_eq!(out.body, body);
        assert!(!out.body_changed);
    }
}
