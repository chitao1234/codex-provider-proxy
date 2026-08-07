//! OpenAI Responses response conversion and Ark-specific request normalization.

use serde_json::{json, Map, Value};

use crate::chat::{
    extract_usage, first_choice_finish_reason, first_choice_message, upstream_id, upstream_model,
    ChatUsage,
};
use crate::error::ConversionError;

/// Apply Ark-style parameter normalization to a rendered Responses request object.
///
/// - `reasoning.effort` -> Ark `thinking: {"type": "enabled"|"disabled"}`.
/// - web_search tools: `search_context_size` -> `max_keyword`.
pub fn apply_ark_style_to_responses(request: &mut Map<String, Value>) {
    // reasoning.effort -> Ark thinking.type.
    let effort = request
        .get("reasoning")
        .and_then(Value::as_object)
        .and_then(|reasoning| reasoning.get("effort"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    if let Some(effort) = &effort {
        let thinking_type = ark_thinking_type(effort);
        request.insert("thinking".to_string(), json!({"type": thinking_type}));
        request.remove("reasoning");
    }
    // Ark rejects the official `metadata` and legacy `user` fields (`unknown field
    // "metadata"` / `unknown field "user"`, observed in practice); the identity/cache
    // keys they carry are mapped to Ark's `prompt_cache_key` separately. Ark also
    // enforces a 64-character limit on `prompt_cache_key`; claude's device-id
    // metadata.user_id JSON exceeds it, so the unusable key is dropped.
    request.remove("metadata");
    request.remove("user");
    if let Some(key) = request.get("prompt_cache_key").and_then(Value::as_str) {
        if key.len() > 64 {
            request.remove("prompt_cache_key");
        }
    }
    normalize_ark_web_search_tools(request);
}

fn ark_thinking_type(effort: &str) -> &'static str {
    if matches!(effort, "none" | "minimal") {
        "disabled"
    } else {
        "enabled"
    }
}

/// Normalize OpenAI-style web_search tools to Ark's flat shape.
///
/// Ark declares the tool flat (`{"type":"web_search","max_keyword":3}`) without the
/// OpenAI `web_search` sub-object; `search_context_size` maps to `max_keyword`
/// (low=1, medium=3, else 5). Preview/versioned variants (`web_search_preview`,
/// `web_search_20260209`) normalize to `web_search`.
fn normalize_ark_web_search_tools(request: &mut Map<String, Value>) {
    let Some(tools) = request.get_mut("tools").and_then(Value::as_array_mut) else {
        return;
    };
    for tool in tools {
        let Some(object) = tool.as_object_mut() else {
            continue;
        };
        let kind = object
            .get("type")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_default();
        if kind != "web_search" && kind != "web_search_preview" && !kind.starts_with("web_search_")
        {
            continue;
        }
        // Flatten the OpenAI sub-object onto the tool.
        if let Some(nested) = object.remove("web_search") {
            if let Some(nested) = nested.as_object() {
                for (key, value) in nested {
                    object.entry(key.clone()).or_insert_with(|| value.clone());
                }
            }
        }
        if let Some(context) = object.remove("search_context_size") {
            let max_keyword = match context.as_str() {
                Some("low") => 1,
                Some("medium") => 3,
                _ => 5,
            };
            object.insert("max_keyword".to_owned(), Value::from(max_keyword));
        }
        if kind != "web_search" {
            object.insert("type".to_owned(), Value::String("web_search".to_owned()));
        }
    }
}

/// Map Responses `reasoning.effort` to the upstream thinking/reasoning controls.
pub fn convert_chat_response_to_responses(body: &Value) -> Result<Value, ConversionError> {
    let message = first_choice_message(body)
        .ok_or_else(|| ConversionError::invalid("upstream chat response missing message"))?;

    let response_id = synth_response_id(upstream_id(body));
    let model = upstream_model(body).unwrap_or_default().to_string();
    let finish_reason = first_choice_finish_reason(body);
    let (status, incomplete_details) = map_finish_reason_to_status(finish_reason);

    let mut output: Vec<Value> = Vec::new();
    // Reasoning item.
    if let Some(reasoning) = message
        .get("reasoning_content")
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
    {
        output.push(json!({
            "id": format!("rs_{}", &response_id[5..]),
            "type": "reasoning",
            "status": "completed",
            "summary": [{"type": "summary_text", "text": reasoning}],
        }));
    }
    // Message item with output_text content.
    let text = message
        .get("content")
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
        .unwrap_or_default();
    // Tool call items.
    let tool_calls = message.get("tool_calls").and_then(Value::as_array);
    if !text.is_empty() || tool_calls.is_some() {
        let mut content = Vec::new();
        if !text.is_empty() {
            content.push(json!({"type": "output_text", "text": text, "annotations": []}));
        }
        let mut msg = Map::new();
        msg.insert(
            "id".to_string(),
            json!(format!("msg_{}", &response_id[5..])),
        );
        msg.insert("type".to_string(), json!("message"));
        msg.insert("status".to_string(), json!("completed"));
        msg.insert("role".to_string(), json!("assistant"));
        msg.insert("content".to_string(), Value::Array(content));
        output.push(Value::Object(msg));
    }
    if let Some(tool_calls) = tool_calls {
        for (index, call) in tool_calls.iter().enumerate() {
            let Some(call) = call.as_object() else {
                continue;
            };
            let function = call.get("function").and_then(Value::as_object);
            let name = function
                .and_then(|f| f.get("name"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            let call_id = call
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .unwrap_or_default();
            let arguments = function
                .and_then(|f| f.get("arguments"))
                .and_then(Value::as_str)
                .unwrap_or("{}");
            output.push(json!({
                "id": format!("fc_{}_{}", &response_id[5..], index),
                "type": "function_call",
                "status": "completed",
                "call_id": call_id,
                "name": name,
                "arguments": arguments,
            }));
        }
    }

    let usage = extract_usage(body);
    Ok(json!({
        "id": response_id,
        "object": "response",
        "created_at": chrono_now(),
        "status": status,
        "incomplete_details": incomplete_details,
        "error": null,
        "model": model,
        "output": output,
        "usage": responses_usage(usage),
    }))
}

/// Extract the assistant chat message from an upstream Chat Completions body,
/// for transcript storage (non-streaming path; the streaming path exposes the
/// same shape via `ResponsesRenderer::assistant_turn`).
pub fn chat_response_assistant_turn(body: &Value) -> Option<Value> {
    let message = first_choice_message(body)?;
    let has_text = message
        .get("content")
        .and_then(Value::as_str)
        .is_some_and(|text| !text.is_empty());
    let has_calls = message
        .get("tool_calls")
        .and_then(Value::as_array)
        .is_some_and(|calls| !calls.is_empty());
    if !has_text && !has_calls {
        return None;
    }
    Some(message.clone())
}

fn synth_response_id(upstream_id: Option<&str>) -> String {
    match upstream_id {
        Some(id) if id.starts_with("resp_") => id.to_string(),
        Some(id) => format!("resp_{id}"),
        None => String::new(),
    }
}

fn map_finish_reason_to_status(finish_reason: Option<&str>) -> (&'static str, Value) {
    match finish_reason {
        Some("length") => ("incomplete", json!({"reason": "max_output_tokens"})),
        Some("content_filter") => ("incomplete", json!({"reason": "content_filter"})),
        _ => ("completed", Value::Null),
    }
}

fn responses_usage(usage: ChatUsage) -> Value {
    let mut usage_out = Map::new();
    if let Some(input) = usage.prompt_tokens {
        usage_out.insert("input_tokens".to_string(), json!(input));
    }
    if let Some(output) = usage.completion_tokens {
        usage_out.insert("output_tokens".to_string(), json!(output));
    }
    if let Some(total) = usage
        .prompt_tokens
        .and_then(|i| usage.completion_tokens.map(|o| i + o))
    {
        usage_out.insert("total_tokens".to_string(), json!(total));
    }
    if let Some(cached) = usage.cached_tokens {
        usage_out.insert(
            "input_tokens_details".to_string(),
            json!({"cached_tokens": cached}),
        );
    }
    if let Some(reasoning) = usage.reasoning_tokens {
        usage_out.insert(
            "output_tokens_details".to_string(),
            json!({"reasoning_tokens": reasoning}),
        );
    }
    Value::Object(usage_out)
}

fn chrono_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
