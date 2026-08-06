//! Shared construction helpers for provider Chat Completions requests.

use serde_json::{Map, Value};

use crate::dialect::{MaxTokensField, ModelCapabilities, RequestConversionReport};

pub(crate) fn max_tokens_field(field: MaxTokensField) -> &'static str {
    match field {
        MaxTokensField::MaxTokens => "max_tokens",
        MaxTokensField::MaxCompletionTokens => "max_completion_tokens",
    }
}

pub(crate) fn copy_string(
    request: &Map<String, Value>,
    out: &mut Map<String, Value>,
    from: &str,
    to: &str,
) {
    if let Some(value) = request.get(from).and_then(Value::as_str) {
        out.insert(to.to_owned(), Value::String(value.to_owned()));
    }
}

pub(crate) fn copy_u64(
    request: &Map<String, Value>,
    out: &mut Map<String, Value>,
    from: &str,
    to: &str,
) {
    if let Some(value) = request.get(from).and_then(Value::as_u64) {
        out.insert(to.to_owned(), Value::from(value));
    }
}

pub(crate) fn copy_bool(
    request: &Map<String, Value>,
    out: &mut Map<String, Value>,
    from: &str,
    to: &str,
) {
    if let Some(value) = request.get(from).and_then(Value::as_bool) {
        out.insert(to.to_owned(), Value::Bool(value));
    }
}

pub(crate) fn copy_value(
    request: &Map<String, Value>,
    out: &mut Map<String, Value>,
    from: &str,
    to: &str,
) {
    if let Some(value) = request.get(from).filter(|value| !value.is_null()) {
        out.insert(to.to_owned(), value.clone());
    }
}

pub(crate) fn apply_max_tokens_cap(caps: &ModelCapabilities, out: &mut Map<String, Value>) {
    let Some(cap) = caps.max_tokens_cap else {
        return;
    };
    let field = max_tokens_field(caps.max_tokens_field);
    let Some(value) = out.get(field).and_then(Value::as_u64) else {
        return;
    };
    if value > cap {
        out.insert(field.to_owned(), Value::from(cap));
    }
}

pub(crate) fn add_forced_server_tools(
    caps: &ModelCapabilities,
    report: &mut RequestConversionReport,
) {
    for tool in &caps.always_enable_tools {
        let normalized = match tool.as_str() {
            "WebSearch" => "web_search",
            "WebFetch" => "web_fetch",
            "CodeExecution" => "code_execution",
            other => other,
        };
        if !report
            .mapped_server_tools
            .iter()
            .any(|mapped| mapped == normalized)
        {
            report.mapped_server_tools.push(normalized.to_owned());
        }
    }
}
