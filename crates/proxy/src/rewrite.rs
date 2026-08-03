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
    pub requires_anthropic_effort_beta: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedModelMapping {
    pub from_model: String,
    pub from_reasoning_effort: Option<String>,
    pub to_model: String,
    pub to_reasoning_effort: Option<String>,
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
    let reasoning_effort = current_reasoning_effort(&json);
    let Some(mapping) = select_model_mapping(
        &cfg.rewrite.model_mappings,
        ctx.provider_name,
        &model,
        reasoning_effort.as_deref(),
    ) else {
        return passthrough(body);
    };

    let mut body_changed = false;
    body_changed |= set_top_level_string(&mut json, "model", &mapping.to_model);
    let mut requires_anthropic_effort_beta = false;
    if let Some(to_reasoning_effort) = &mapping.to_reasoning_effort {
        body_changed |= set_reasoning_effort(&mut json, endpoint, to_reasoning_effort);
        requires_anthropic_effort_beta = endpoint == ModelEndpoint::Messages;
    }

    let applied_model_mapping = Some(AppliedModelMapping {
        from_model: model,
        from_reasoning_effort: reasoning_effort,
        to_model: mapping.to_model.clone(),
        to_reasoning_effort: mapping.to_reasoning_effort.clone(),
    });

    if !body_changed {
        return RequestRewriteOutcome {
            body,
            body_changed,
            applied_model_mapping,
            requires_anthropic_effort_beta,
        };
    }

    match serde_json::to_vec(&json) {
        Ok(rewritten) => RequestRewriteOutcome {
            body: Bytes::from(rewritten),
            body_changed,
            applied_model_mapping,
            requires_anthropic_effort_beta,
        },
        Err(_) => RequestRewriteOutcome {
            body,
            body_changed: false,
            applied_model_mapping: None,
            requires_anthropic_effort_beta: false,
        },
    }
}

fn passthrough(body: Bytes) -> RequestRewriteOutcome {
    RequestRewriteOutcome {
        body,
        body_changed: false,
        applied_model_mapping: None,
        requires_anthropic_effort_beta: false,
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
    model: &str,
    reasoning_effort: Option<&str>,
) -> Option<&'a ModelMapping> {
    let mut best = None;
    let mut best_score = (false, false);

    for mapping in mappings {
        if !mapping_matches(mapping, provider_name, model, reasoning_effort) {
            continue;
        }

        let score = (
            mapping.provider.is_some(),
            mapping.from_reasoning_effort.is_some(),
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
    model: &str,
    reasoning_effort: Option<&str>,
) -> bool {
    if mapping
        .provider
        .as_deref()
        .is_some_and(|p| p != provider_name)
    {
        return false;
    }
    if mapping.from_model != model {
        return false;
    }
    match mapping.from_reasoning_effort.as_deref() {
        Some(expected) => reasoning_effort == Some(expected),
        None => true,
    }
}

fn top_level_string<'a>(json: &'a Value, field: &str) -> Option<&'a str> {
    json.as_object()?.get(field)?.as_str()
}

fn current_reasoning_effort(json: &Value) -> Option<String> {
    json.pointer("/reasoning/effort")
        .and_then(Value::as_str)
        .or_else(|| json.get("reasoning_effort").and_then(Value::as_str))
        .or_else(|| {
            json.pointer("/output_config/effort")
                .and_then(Value::as_str)
        })
        .or_else(|| json.pointer("/thinking/type").and_then(Value::as_str))
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
    match set_existing_thinking_type(json, effort) {
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

fn set_existing_thinking_type(json: &mut Value, effort: &str) -> SetFieldResult {
    let Some(thinking) = json.get_mut("thinking").and_then(Value::as_object_mut) else {
        return SetFieldResult::Missing;
    };
    if !thinking.contains_key("type") {
        return SetFieldResult::Missing;
    }
    set_string_field(thinking, "type", effort)
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
    };

    use super::{apply_request_rewrites, request_rewrites_may_apply, RequestRewriteContext};

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
            from_model: from_model.to_string(),
            from_reasoning_effort: from_effort.map(str::to_string),
            to_model: to_model.to_string(),
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
        assert!(!out.requires_anthropic_effort_beta);
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
        assert!(!out.requires_anthropic_effort_beta);
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
        assert!(out.requires_anthropic_effort_beta);
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
        assert!(!out.requires_anthropic_effort_beta);
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
        assert!(out.requires_anthropic_effort_beta);
        assert_eq!(rewritten["model"], "claude-opus-4-7");
        assert_eq!(rewritten["thinking"]["type"], "adaptive");
        assert_eq!(rewritten["output_config"]["effort"], "max");
    }

    #[test]
    fn rewrites_messages_model_and_legacy_thinking_type_shape_from_claude_code_logs() {
        let cfg = test_config(vec![mapping(
            "claude-fable-5",
            Some("adaptive"),
            "claude-sonnet-5",
            Some("disabled"),
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
        assert!(out.requires_anthropic_effort_beta);
        assert_eq!(rewritten["model"], "claude-sonnet-5");
        assert_eq!(rewritten["thinking"]["type"], "disabled");
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
        provider_mapping.provider = Some("provider_a".to_string());
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
