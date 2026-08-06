//! OpenAI Responses API (downstream) to Chat Completions (upstream) conversion.
//!
//! The upstream side (Chat Completions shapes) is shared with the Messages conversion;
//! this module owns the downstream Responses protocol: request mapping (input items to
//! chat messages), non-streaming response mapping, and the streaming SSE state machine.

use std::collections::HashMap;

use bytes::Bytes;
use serde_json::{json, Map, Value};

use crate::chat::{
    delta_string, extract_usage, first_choice_delta, first_choice_finish_reason,
    first_choice_message, is_usage_only_chunk, upstream_id, upstream_model, ChatUsage,
};
use crate::dialect::{ModelCapabilities, RequestConversionReport};
use crate::error::ConversionError;
use crate::sse::encode_sse_event;

/// Convert an OpenAI Responses request body into a Chat Completions request body.
///
/// Shares the upstream parameter mapping with the Messages path via `ChatRequestBuilder`,
/// but expands the Responses `input` item array instead of Messages content blocks.
///
/// `previous_messages` carries the chat transcript stored for an earlier synthesized
/// `response_id` when the client continues with `previous_response_id`; it is prepended
/// to the messages converted from the current `input`, so the stateless upstream sees
/// the full conversation.
pub fn convert_responses_request(
    body: &Value,
    caps: &ModelCapabilities,
    previous_messages: Option<&[Value]>,
    last_reasoning: Option<&str>,
) -> Result<(Value, RequestConversionReport), ConversionError> {
    let Some(request) = body.as_object() else {
        return Err(ConversionError::invalid(
            "request body is not a JSON object",
        ));
    };

    let mut out = Map::new();
    copy_string(request, &mut out, "model", "model");
    copy_u64(
        request,
        &mut out,
        "max_output_tokens",
        max_tokens_field(caps.max_tokens_field),
    );
    copy_bool(request, &mut out, "stream", "stream");
    let stream = request
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if stream && caps.stream_include_usage {
        out.insert("stream_options".to_string(), json!({"include_usage": true}));
    }
    apply_max_tokens_cap(caps, &mut out);
    crate::messages_to_chat::apply_reasoning_split(caps, &mut out);
    copy_value(request, &mut out, "temperature", "temperature");
    copy_value(request, &mut out, "top_p", "top_p");
    copy_value(
        request,
        &mut out,
        "parallel_tool_calls",
        "parallel_tool_calls",
    );
    convert_reasoning_effort(request, caps, &mut out)?;
    convert_text_format(request, caps, &mut out)?;
    convert_tool_choice(request, &mut out);

    let instructions = request.get("instructions").and_then(Value::as_str);
    let messages = expand_input(request.get("input"), caps, last_reasoning)?;
    let (tools, mut report) = convert_tools(request.get("tools"), caps)?;

    // Force-enable configured server tools even when the client did not declare them
    // (mirrors the Messages path).
    if !caps.always_enable_tools.is_empty() {
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
                report.mapped_server_tools.push(normalized.to_string());
            }
        }
    }
    // Merge provider-native request params for mapped server tools.
    let query = last_input_user_text(request.get("input"));
    merge_native_params(&mut out, caps, &report, query.as_deref());

    let mut chat_messages: Vec<Value> = Vec::with_capacity(
        messages.len()
            + usize::from(instructions.is_some())
            + previous_messages.map_or(0, |previous| previous.len()),
    );
    if let Some(instructions) = instructions.filter(|s| !s.trim().is_empty()) {
        chat_messages.push(json!({"role": "system", "content": instructions}));
    }
    if let Some(previous) = previous_messages {
        chat_messages.extend_from_slice(previous);
    }
    chat_messages.extend(messages);
    out.insert("messages".to_string(), Value::Array(chat_messages));
    if !tools.is_empty() {
        out.insert("tools".to_string(), Value::Array(tools));
    }

    Ok((Value::Object(out), report))
}

// Report of what the request conversion did (shared with the Messages path via
// `crate::dialect::RequestConversionReport`).

fn max_tokens_field(field: crate::dialect::MaxTokensField) -> &'static str {
    match field {
        crate::dialect::MaxTokensField::MaxTokens => "max_tokens",
        crate::dialect::MaxTokensField::MaxCompletionTokens => "max_completion_tokens",
    }
}

fn copy_string(request: &Map<String, Value>, out: &mut Map<String, Value>, from: &str, to: &str) {
    if let Some(value) = request.get(from).and_then(Value::as_str) {
        out.insert(to.to_string(), Value::String(value.to_string()));
    }
}

fn copy_u64(request: &Map<String, Value>, out: &mut Map<String, Value>, from: &str, to: &str) {
    if let Some(value) = request.get(from).and_then(Value::as_u64) {
        out.insert(to.to_string(), Value::from(value));
    }
}

fn copy_bool(request: &Map<String, Value>, out: &mut Map<String, Value>, from: &str, to: &str) {
    if let Some(value) = request.get(from).and_then(Value::as_bool) {
        out.insert(to.to_string(), Value::Bool(value));
    }
}

fn copy_value(request: &Map<String, Value>, out: &mut Map<String, Value>, from: &str, to: &str) {
    if let Some(value) = request.get(from) {
        if !value.is_null() {
            out.insert(to.to_string(), value.clone());
        }
    }
}

fn apply_max_tokens_cap(caps: &ModelCapabilities, out: &mut Map<String, Value>) {
    let Some(cap) = caps.max_tokens_cap else {
        return;
    };
    let field = max_tokens_field(caps.max_tokens_field);
    let Some(value) = out.get(field).and_then(Value::as_u64) else {
        return;
    };
    if value > cap {
        out.insert(field.to_string(), Value::from(cap));
    }
}

/// Map Responses `reasoning.effort` to the upstream thinking/reasoning controls.
fn convert_reasoning_effort(
    request: &Map<String, Value>,
    caps: &ModelCapabilities,
    out: &mut Map<String, Value>,
) -> Result<(), ConversionError> {
    let effort = request
        .get("reasoning")
        .and_then(Value::as_object)
        .and_then(|reasoning| reasoning.get("effort"))
        .and_then(Value::as_str)
        .map(str::to_owned);

    // Reuse the same thinking-param mapping as the Messages path.
    match caps.thinking_param {
        crate::dialect::ThinkingParam::TopLevel => {
            if let Some(effort) = &effort {
                let thinking_type = if effort == "none" || effort == "minimal" {
                    "disabled"
                } else {
                    "enabled"
                };
                out.insert("thinking".to_string(), json!({"type": thinking_type}));
            }
        }
        crate::dialect::ThinkingParam::Adaptive => {
            if let Some(effort) = &effort {
                let thinking_type = if effort == "none" || effort == "minimal" {
                    "disabled"
                } else {
                    "adaptive"
                };
                out.insert("thinking".to_string(), json!({"type": thinking_type}));
            }
        }
        crate::dialect::ThinkingParam::EnableThinking => {
            let enabled = effort
                .as_deref()
                .is_none_or(|effort| effort != "none" && effort != "minimal");
            out.insert("enable_thinking".to_string(), json!(enabled));
        }
        crate::dialect::ThinkingParam::None => {}
    }

    if let Some(config) = caps
        .reasoning_effort
        .as_ref()
        .filter(|config| config.enabled)
    {
        let level = effort
            .as_deref()
            .or(config.default.as_deref())
            .and_then(|effort| caps.clamp_effort(effort));
        if let Some(level) = level {
            out.insert("reasoning_effort".to_string(), Value::String(level));
        }
    }
    Ok(())
}

/// Map Responses `text.format` to `response_format`.
fn convert_text_format(
    request: &Map<String, Value>,
    caps: &ModelCapabilities,
    out: &mut Map<String, Value>,
) -> Result<(), ConversionError> {
    let Some(format) = request
        .get("text")
        .and_then(Value::as_object)
        .and_then(|text| text.get("format"))
    else {
        return Ok(());
    };
    let format_type = format.get("type").and_then(Value::as_str).unwrap_or("text");
    match format_type {
        "text" => {}
        "json_object" => {
            out.insert(
                "response_format".to_string(),
                json!({"type": "json_object"}),
            );
        }
        "json_schema" => match caps.response_format {
            crate::dialect::ResponseFormatCap::JsonSchema => {
                let schema = format.get("schema").cloned().unwrap_or_else(|| json!({}));
                out.insert(
                    "response_format".to_string(),
                    json!({"type": "json_schema", "json_schema": {"name": "output", "schema": schema}}),
                );
            }
            crate::dialect::ResponseFormatCap::JsonObject => {
                out.insert(
                    "response_format".to_string(),
                    json!({"type": "json_object"}),
                );
            }
            crate::dialect::ResponseFormatCap::Text => {
                return Err(ConversionError::unsupported(
                    "text.format",
                    "upstream does not support structured output formats",
                ));
            }
        },
        other => {
            return Err(ConversionError::unsupported(
                "text.format",
                format!("unknown format type {other:?}"),
            ));
        }
    }
    Ok(())
}

/// Map Responses `tool_choice` to Chat Completions `tool_choice`.
fn convert_tool_choice(request: &Map<String, Value>, out: &mut Map<String, Value>) {
    let Some(choice) = request.get("tool_choice") else {
        return;
    };
    let mapped: Option<Value> = match choice {
        Value::String(s) => Some(json!(s)),
        Value::Object(object) => match object.get("type").and_then(Value::as_str) {
            Some("function") => object
                .get("name")
                .and_then(Value::as_str)
                .map(|name| json!({"type": "function", "function": {"name": name}})),
            Some("allowed_tools") => {
                // allowed_tools limits the subset; map to auto (upstream has no equivalent).
                Some(json!("auto"))
            }
            _ => None,
        },
        _ => None,
    };
    if let Some(mapped) = mapped {
        out.insert("tool_choice".to_string(), mapped);
    }
}

/// Expand the Responses `input` into chat messages.
///
/// Rules:
/// - `function_call` items become tool_calls on the enclosing assistant message.
/// - `function_call_output` items become `role: "tool"` messages (call_id correlation).
/// - `reasoning` items (echoed thinking from the previous turn) are attached to the
///   following assistant message as `reasoning_content` — upstreams that require it
///   (DeepSeek, MiMo, MiniMax) reject the request with 400 otherwise.
/// - `web_search_call` / `item_reference` items are dropped.
fn expand_input(
    input: Option<&Value>,
    caps: &ModelCapabilities,
    last_reasoning: Option<&str>,
) -> Result<Vec<Value>, ConversionError> {
    let Some(input) = input else {
        return Ok(Vec::new());
    };
    match input {
        Value::String(text) => Ok(vec![json!({"role": "user", "content": text})]),
        Value::Array(items) => {
            let mut out: Vec<Value> = Vec::new();
            // Pending tool_calls to attach to the next assistant message.
            let mut pending_assistant: Option<Map<String, Value>> = None;
            // Echoed reasoning seen before the assistant message it belongs to.
            let mut pending_reasoning: Option<String> = None;
            for item in items {
                let Some(object) = item.as_object() else {
                    continue;
                };
                let item_type = object.get("type").and_then(Value::as_str).unwrap_or("");
                match item_type {
                    "message" => {
                        let role = object.get("role").and_then(Value::as_str).unwrap_or("user");
                        let content = object.get("content");
                        match role {
                            "assistant" => {
                                // Flush any pending assistant message first.
                                if let Some(assistant) = pending_assistant.take() {
                                    out.push(Value::Object(assistant));
                                }
                                let mut msg = Map::new();
                                msg.insert("role".to_string(), json!("assistant"));
                                if let Some(content) = content {
                                    let (text, _) = responses_content_text(content);
                                    msg.insert("content".to_string(), json!(text));
                                } else {
                                    msg.insert("content".to_string(), json!(""));
                                }
                                // Attach echoed reasoning (from the preceding reasoning
                                // item) to this assistant message.
                                if let Some(reasoning) = pending_reasoning.take() {
                                    msg.insert(
                                        "reasoning_content".to_string(),
                                        Value::String(reasoning),
                                    );
                                }
                                pending_assistant = Some(msg);
                            }
                            "system" | "developer" => {
                                let text = content
                                    .map(responses_content_text)
                                    .map(|(t, _)| t)
                                    .unwrap_or_default();
                                out.push(json!({"role": "system", "content": text}));
                            }
                            _ => {
                                // user (or other) message.
                                if let Some(assistant) = pending_assistant.take() {
                                    out.push(Value::Object(assistant));
                                }
                                out.push(expand_user_message(object, caps)?);
                            }
                        }
                    }
                    "function_call" => {
                        // Ensure a pending assistant message exists.
                        if pending_assistant.is_none() {
                            let mut assistant = Map::new();
                            assistant.insert("role".to_string(), json!("assistant"));
                            assistant.insert("content".to_string(), json!(""));
                            if let Some(reasoning) = pending_reasoning.take() {
                                assistant.insert(
                                    "reasoning_content".to_string(),
                                    Value::String(reasoning),
                                );
                            }
                            pending_assistant = Some(assistant);
                        }
                        let call_id = object.get("call_id").and_then(Value::as_str).unwrap_or("");
                        let name = object.get("name").and_then(Value::as_str).unwrap_or("");
                        let arguments = object
                            .get("arguments")
                            .and_then(Value::as_str)
                            .unwrap_or("{}");
                        let tool_call = json!({
                            "id": call_id,
                            "type": "function",
                            "function": {"name": name, "arguments": arguments}
                        });
                        let assistant = pending_assistant.as_mut().expect("just ensured");
                        let tool_calls = assistant
                            .entry("tool_calls".to_string())
                            .or_insert_with(|| json!([]));
                        if !tool_calls.is_array() {
                            *tool_calls = json!([]);
                        }
                        tool_calls
                            .as_array_mut()
                            .expect("just ensured")
                            .push(tool_call);
                    }
                    "function_call_output" => {
                        if let Some(assistant) = pending_assistant.take() {
                            out.push(Value::Object(assistant));
                        }
                        let call_id = object.get("call_id").and_then(Value::as_str).unwrap_or("");
                        let output = object
                            .get("output")
                            .map(|o| match o {
                                Value::String(s) => s.clone(),
                                other => other.to_string(),
                            })
                            .unwrap_or_default();
                        out.push(
                            json!({"role": "tool", "tool_call_id": call_id, "content": output}),
                        );
                    }
                    "reasoning" => {
                        // The client (e.g. Codex) echoes the reasoning it received from
                        // the previous turn as a reasoning item preceding the assistant
                        // message it belongs to. Stash it and attach it to that message
                        // as reasoning_content — upstreams that require it (DeepSeek,
                        // MiMo, MiniMax) reject the request with 400 otherwise.
                        let reasoning = object
                            .get("summary")
                            .and_then(Value::as_array)
                            .and_then(|summary| {
                                summary
                                    .iter()
                                    .filter_map(|s| {
                                        s.get("text").and_then(Value::as_str).map(str::to_owned)
                                    })
                                    .reduce(|acc, text| format!("{acc}\n{text}"))
                            })
                            .or_else(|| {
                                object
                                    .get("summary_text")
                                    .and_then(Value::as_str)
                                    .map(str::to_owned)
                            })
                            .or_else(|| {
                                object
                                    .get("text")
                                    .and_then(Value::as_str)
                                    .map(str::to_owned)
                            })
                            .filter(|text| !text.trim().is_empty())
                            .or_else(|| last_reasoning.map(str::to_owned));
                        if let Some(reasoning) = reasoning {
                            pending_reasoning = Some(reasoning);
                        }
                    }
                    _ => {
                        // web_search_call, item_reference, etc.: drop.
                    }
                }
            }
            if let Some(assistant) = pending_assistant.take() {
                out.push(Value::Object(assistant));
            }
            Ok(out)
        }
        _ => Err(ConversionError::invalid("input must be a string or array")),
    }
}

/// Extract text from a Responses message content (string or content-part array).
fn responses_content_text(content: &Value) -> (String, bool) {
    match content {
        Value::String(text) => (text.clone(), false),
        Value::Array(parts) => {
            let mut texts = Vec::new();
            let mut has_image = false;
            for part in parts {
                let Some(object) = part.as_object() else {
                    continue;
                };
                match object.get("type").and_then(Value::as_str) {
                    Some("input_text") | Some("output_text") => {
                        if let Some(text) = object.get("text").and_then(Value::as_str) {
                            if !text.is_empty() {
                                texts.push(text.to_string());
                            }
                        }
                    }
                    Some("input_image") => has_image = true,
                    _ => {}
                }
            }
            (texts.join("\n\n"), has_image)
        }
        _ => (String::new(), false),
    }
}

fn expand_user_message(
    object: &Map<String, Value>,
    caps: &ModelCapabilities,
) -> Result<Value, ConversionError> {
    let Some(content) = object.get("content") else {
        return Ok(json!({"role": "user", "content": ""}));
    };
    let (text, has_image) = responses_content_text(content);
    if !has_image {
        return Ok(json!({"role": "user", "content": text}));
    }
    if !caps.image_input {
        return Err(ConversionError::unsupported(
            "input[].content",
            "image input is not supported by this upstream",
        ));
    }
    // Build image_url parts from input_image parts.
    let mut parts: Vec<Value> = Vec::new();
    if !text.is_empty() {
        parts.push(json!({"type": "text", "text": text}));
    }
    if let Value::Array(content_parts) = content {
        for part in content_parts {
            let Some(object) = part.as_object() else {
                continue;
            };
            if object.get("type").and_then(Value::as_str) != Some("input_image") {
                continue;
            }
            let url = object
                .get("image_url")
                .and_then(Value::as_str)
                .or_else(|| object.get("file_id").and_then(Value::as_str))
                .unwrap_or_default();
            if !url.is_empty() {
                parts.push(json!({"type": "image_url", "image_url": {"url": url}}));
            }
        }
    }
    Ok(json!({"role": "user", "content": parts}))
}

/// Convert Responses `tools` (flat function or builtin) to Chat function tools.
fn convert_tools(
    tools: Option<&Value>,
    caps: &ModelCapabilities,
) -> Result<(Vec<Value>, RequestConversionReport), ConversionError> {
    let mut report = RequestConversionReport::default();
    let Some(tools) = tools else {
        return Ok((Vec::new(), report));
    };
    let Some(tools) = tools.as_array() else {
        return Err(ConversionError::invalid("tools must be an array"));
    };
    let mut out = Vec::with_capacity(tools.len());
    for tool in tools {
        let Some(object) = tool.as_object() else {
            continue;
        };
        let tool_type = object.get("type").and_then(Value::as_str).unwrap_or("");
        match tool_type {
            "function" => {
                let name = object
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if name.is_empty() {
                    continue;
                }
                let description = object
                    .get("description")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .unwrap_or_default();
                let parameters = object
                    .get("parameters")
                    .map(crate::messages_to_chat::normalize_input_schema)
                    .unwrap_or_else(|| json!({"type": "object", "properties": {}}));
                let mut function = Map::new();
                function.insert("name".to_string(), json!(name));
                if !description.is_empty() {
                    function.insert("description".to_string(), json!(description));
                }
                function.insert("parameters".to_string(), parameters);
                if object.get("strict").and_then(Value::as_bool) == Some(true) {
                    function.insert("strict".to_string(), json!(true));
                }
                out.push(json!({"type": "function", "function": function}));
            }
            "web_search" | "web_search_preview" => match caps.server_tools {
                crate::dialect::ServerToolPolicy::ProviderNative => {
                    if let Some(template) = caps.search_tool_template.clone() {
                        report.mapped_server_tools.push("web_search".to_string());
                        out.push(crate::messages_to_chat::render_search_tool_template(
                            &template, None,
                        ));
                    } else if caps.search_request_params.is_some() {
                        report.mapped_server_tools.push("web_search".to_string());
                    } else {
                        report.dropped_server_tools.push("web_search".to_string());
                    }
                }
                crate::dialect::ServerToolPolicy::Drop => {
                    report.dropped_server_tools.push("web_search".to_string());
                }
                crate::dialect::ServerToolPolicy::MapToFunction => {
                    report.mapped_server_tools.push("web_search".to_string());
                    out.push(crate::messages_to_chat::function_tool(
                            "web_search",
                            "Search the web for current information.",
                            json!({"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]}),
                        ));
                }
                crate::dialect::ServerToolPolicy::Passthrough => {
                    out.push(tool.clone());
                }
            },
            "code_interpreter" => {
                if caps.code_interpreter_request_params.is_some() {
                    report
                        .mapped_server_tools
                        .push("code_execution".to_string());
                } else {
                    report
                        .dropped_server_tools
                        .push("code_execution".to_string());
                }
            }
            _ => {
                // Unknown builtin (file_search, mcp, image_search...): drop.
                report.dropped_server_tools.push(tool_type.to_string());
            }
        }
    }
    Ok((out, report))
}

/// The last user text in a Responses `input`, for search query templating.
fn last_input_user_text(input: Option<&Value>) -> Option<String> {
    let input = input?;
    if let Some(text) = input.as_str() {
        let text = text.trim();
        return (!text.is_empty()).then(|| text.chars().take(60).collect());
    }
    let items = input.as_array()?;
    for item in items.iter().rev() {
        let object = item.as_object()?;
        if object.get("type").and_then(Value::as_str) != Some("message") {
            continue;
        }
        if object.get("role").and_then(Value::as_str) != Some("user") {
            continue;
        }
        let Some(content) = object.get("content") else {
            continue;
        };
        let (text, _) = responses_content_text(content);
        let text = text.trim();
        if !text.is_empty() {
            return Some(text.chars().take(60).collect());
        }
    }
    None
}

/// Merge provider-native request params for mapped tools (search/fetch/code).
pub fn merge_native_params(
    out: &mut Map<String, Value>,
    caps: &ModelCapabilities,
    report: &RequestConversionReport,
    last_user_query: Option<&str>,
) {
    if let Some(params) = &caps.search_request_params {
        if report.mapped_server_tools.iter().any(|t| t == "web_search") {
            if let Value::Object(merged) =
                crate::messages_to_chat::render_search_tool_template(params, last_user_query)
            {
                out.extend(merged);
            }
        }
    }
    if let Some(params) = &caps.code_interpreter_request_params {
        if report
            .mapped_server_tools
            .iter()
            .any(|t| t == "code_execution")
        {
            if let Value::Object(merged) =
                crate::messages_to_chat::render_search_tool_template(params, None)
            {
                out.extend(merged);
            }
            out.insert("stream".to_string(), json!(true));
        }
    }
}

// ---------------------------------------------------------------------------
// Non-streaming response conversion
// ---------------------------------------------------------------------------

/// Convert a non-streaming Chat Completions response into a Responses response object.
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
/// same shape via `ResponsesStreamConverter::assistant_turn`).
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

// ---------------------------------------------------------------------------
// Streaming state machine
// ---------------------------------------------------------------------------

/// Accumulated state of one upstream tool call, for the closing events.
struct FunctionCallState {
    item_id: String,
    call_id: String,
    name: String,
    arguments: String,
    output_index: usize,
}

/// Incremental converter from upstream Chat Completions SSE chunks to downstream
/// Responses API SSE events.
pub struct ResponsesStreamConverter {
    started: bool,
    response_id: String,
    model: String,
    output_index: usize,
    sequence_number: u64,
    reasoning_item: Option<usize>,
    reasoning_text: String,
    message_item: Option<usize>,
    message_text: String,
    function_calls: HashMap<usize, FunctionCallState>,
    assistant_turn: Option<Value>,
    usage: ChatUsage,
    finished: bool,
}

impl Default for ResponsesStreamConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl ResponsesStreamConverter {
    pub fn new() -> Self {
        Self {
            started: false,
            response_id: String::new(),
            model: String::new(),
            output_index: 0,
            sequence_number: 0,
            reasoning_item: None,
            reasoning_text: String::new(),
            message_item: None,
            message_text: String::new(),
            function_calls: HashMap::new(),
            assistant_turn: None,
            usage: ChatUsage::default(),
            finished: false,
        }
    }

    /// The synthesized downstream `response_id`, available once the stream started.
    pub fn response_id(&self) -> Option<&str> {
        self.started.then_some(self.response_id.as_str())
    }

    /// The assistant chat message produced by this stream (text and/or tool calls),
    /// for transcript storage. Available after `finish`.
    pub fn assistant_turn(&self) -> Option<&Value> {
        self.assistant_turn.as_ref()
    }

    /// Process one upstream SSE `data:` payload; appends Responses events to `out`.
    pub fn on_chunk(&mut self, payload: &str, out: &mut Vec<Bytes>) -> Result<(), ConversionError> {
        if self.finished {
            return Ok(());
        }
        if payload == "[DONE]" {
            self.finish(out);
            return Ok(());
        }
        let chunk: Value = serde_json::from_str(payload)
            .map_err(|_| ConversionError::invalid("upstream SSE chunk is not valid JSON"))?;

        // Upstream error event: emit a downstream `error` SSE event and stop, instead
        // of faking response.completed.
        if let Some(error) = chunk.get("error").and_then(Value::as_object) {
            self.emit_error(error, out);
            return Ok(());
        }

        if !self.started {
            self.begin(&chunk, out);
        }

        if let Some(delta) = first_choice_delta(&chunk) {
            self.process_delta(delta, out);
        }
        let usage = extract_usage(&chunk);
        if !usage.is_empty() {
            self.usage = usage;
        }
        if is_usage_only_chunk(&chunk) {
            self.emit_completed(out);
        }
        Ok(())
    }

    /// End of stream: emit response.completed (or incomplete) if not already sent.
    pub fn finish(&mut self, out: &mut Vec<Bytes>) {
        if self.finished {
            return;
        }
        self.finished = true;
        if !self.started {
            return;
        }
        self.emit_completed(out);
    }

    fn emit_error(&mut self, error: &Map<String, Value>, out: &mut Vec<Bytes>) {
        self.finished = true;
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("upstream request failed");
        let code = error
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or("api_error");
        self.emit(
            "error",
            &json!({
                "type": "error",
                "code": code,
                "message": message,
                "param": null,
            }),
            out,
        );
    }

    fn begin(&mut self, chunk: &Value, out: &mut Vec<Bytes>) {
        self.started = true;
        self.response_id = synth_response_id(upstream_id(chunk));
        self.model = upstream_model(chunk).unwrap_or_default().to_string();
        self.emit(
            "response.created",
            &json!({
                "type": "response.created",
                "response": self.response_snapshot("in_progress"),
            }),
            out,
        );
        self.emit(
            "response.in_progress",
            &json!({
                "type": "response.in_progress",
                "response": self.response_snapshot("in_progress"),
            }),
            out,
        );
    }

    fn response_snapshot(&self, status: &str) -> Value {
        json!({
            "id": self.response_id,
            "object": "response",
            "status": status,
            "model": self.model,
            "output": [],
            "usage": null,
        })
    }

    fn process_delta(&mut self, delta: &Value, out: &mut Vec<Bytes>) {
        if let Some(reasoning) = delta_string(delta, "reasoning_content") {
            self.ensure_reasoning_item(out);
            self.reasoning_text.push_str(reasoning);
            let index = self.reasoning_item.unwrap_or(0);
            self.emit(
                "response.reasoning_text.delta",
                &json!({
                    "type": "response.reasoning_text.delta",
                    "item_id": format!("rs_{}", &self.response_id[5..]),
                    "output_index": index,
                    "content_index": 0,
                    "delta": reasoning,
                }),
                out,
            );
        }
        if let Some(text) = delta_string(delta, "content") {
            self.ensure_message_item(out);
            self.message_text.push_str(text);
            let index = self.message_item.unwrap_or(0);
            self.emit(
                "response.output_text.delta",
                &json!({
                    "type": "response.output_text.delta",
                    "item_id": format!("msg_{}", &self.response_id[5..]),
                    "output_index": index,
                    "content_index": 0,
                    "delta": text,
                }),
                out,
            );
        }
        if let Some(tool_calls) = delta.get("tool_calls").and_then(Value::as_array) {
            for (position, call) in tool_calls.iter().enumerate() {
                self.process_tool_call(position, call, out);
            }
        }
    }

    fn ensure_reasoning_item(&mut self, out: &mut Vec<Bytes>) {
        if self.reasoning_item.is_some() {
            return;
        }
        let index = self.output_index;
        self.output_index += 1;
        self.reasoning_item = Some(index);
        self.emit(
            "response.output_item.added",
            &json!({
                "type": "response.output_item.added",
                "output_index": index,
                "item": {
                    "id": format!("rs_{}", &self.response_id[5..]),
                    "type": "reasoning",
                    "status": "in_progress",
                    "summary": [],
                },
            }),
            out,
        );
    }

    fn ensure_message_item(&mut self, out: &mut Vec<Bytes>) {
        if self.message_item.is_some() {
            return;
        }
        let index = self.output_index;
        self.output_index += 1;
        self.message_item = Some(index);
        self.emit(
            "response.output_item.added",
            &json!({
                "type": "response.output_item.added",
                "output_index": index,
                "item": {
                    "id": format!("msg_{}", &self.response_id[5..]),
                    "type": "message",
                    "status": "in_progress",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "", "annotations": []}],
                },
            }),
            out,
        );
    }

    fn process_tool_call(&mut self, position: usize, call: &Value, out: &mut Vec<Bytes>) {
        let Some(call) = call.as_object() else { return };
        let index = call
            .get("index")
            .and_then(Value::as_u64)
            .map(|i| i as usize)
            .unwrap_or(position);
        let function = call.get("function").and_then(Value::as_object);
        let new_args = function
            .and_then(|f| f.get("arguments"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        let name = function
            .and_then(|f| f.get("name"))
            .and_then(Value::as_str)
            .unwrap_or_default();

        if let Some(state) = self.function_calls.get_mut(&index) {
            // First delta carries id/name; later deltas only carry argument increments.
            if !name.is_empty() {
                state.name = name.to_string();
            }
            if !new_args.is_empty() {
                state.arguments.push_str(new_args);
            }
        } else {
            let item_id = format!("fc_{}_{}", &self.response_id[5..], index);
            let call_id = call.get("id").and_then(Value::as_str).unwrap_or_default();
            let output_index = self.output_index;
            self.output_index += 1;
            self.function_calls.insert(
                index,
                FunctionCallState {
                    item_id: item_id.clone(),
                    call_id: call_id.to_string(),
                    name: name.to_string(),
                    arguments: new_args.to_string(),
                    output_index,
                },
            );
            self.emit(
                "response.output_item.added",
                &json!({
                    "type": "response.output_item.added",
                    "output_index": output_index,
                    "item": {
                        "id": item_id,
                        "type": "function_call",
                        "status": "in_progress",
                        "call_id": call_id,
                        "name": name,
                        "arguments": "",
                    },
                }),
                out,
            );
        };

        if !new_args.is_empty() {
            let (item_id, delta_index) = self
                .function_calls
                .get(&index)
                .map(|state| (state.item_id.clone(), state.output_index))
                .unwrap_or_default();
            self.emit(
                "response.function_call_arguments.delta",
                &json!({
                    "type": "response.function_call_arguments.delta",
                    "item_id": item_id,
                    "output_index": delta_index,
                    "delta": new_args,
                }),
                out,
            );
        }
    }

    fn emit_completed(&mut self, out: &mut Vec<Bytes>) {
        // Close open items.
        let mut args_done: Vec<Value> = Vec::new();
        let mut items_done: Vec<Value> = Vec::new();
        if let Some(index) = self.reasoning_item.take() {
            items_done.push(json!({
                "type": "response.output_item.done",
                "output_index": index,
                "item": {
                    "id": format!("rs_{}", &self.response_id[5..]),
                    "type": "reasoning",
                    "status": "completed",
                    "summary": [],
                },
            }));
        }
        // Build the assistant chat turn for transcript storage as items close.
        let mut assistant = json!({"role": "assistant", "content": ""});
        let reasoning_text = std::mem::take(&mut self.reasoning_text);
        let mut tool_calls: Vec<Value> = Vec::new();
        if let Some(index) = self.message_item.take() {
            let text = std::mem::take(&mut self.message_text);
            assistant["content"] = json!(text);
            items_done.push(json!({
                "type": "response.output_item.done",
                "output_index": index,
                "item": {
                    "id": format!("msg_{}", &self.response_id[5..]),
                    "type": "message",
                    "status": "completed",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": text, "annotations": []}],
                },
            }));
        }
        // Carry the upstream reasoning so a later turn can reattach it as
        // reasoning_content (MiMo requires it on tool-call turns, else 400).
        if !reasoning_text.trim().is_empty() {
            assistant["reasoning_content"] = json!(reasoning_text);
        }
        // Function call items: close each with the accumulated arguments
        // (function_call_arguments.done), then emit the completed item carrying
        // the full name/arguments for clients that rebuild calls from done events.
        let fc_indices: Vec<usize> = self.function_calls.keys().copied().collect();
        for index in fc_indices {
            let Some(state) = self.function_calls.remove(&index) else {
                continue;
            };
            tool_calls.push(json!({
                "id": state.call_id,
                "type": "function",
                "function": {"name": state.name, "arguments": state.arguments},
            }));
            args_done.push(json!({
                "type": "response.function_call_arguments.done",
                "item_id": state.item_id,
                "output_index": state.output_index,
                "arguments": state.arguments,
            }));
            items_done.push(json!({
                "type": "response.output_item.done",
                "output_index": state.output_index,
                "item": {
                    "id": state.item_id,
                    "type": "function_call",
                    "status": "completed",
                    "call_id": state.call_id,
                    "name": state.name,
                    "arguments": state.arguments,
                },
            }));
        }
        if !tool_calls.is_empty() {
            assistant["tool_calls"] = json!(tool_calls);
        }
        // A turn is worth recording when the assistant said something or called tools.
        if !assistant["content"].as_str().is_none_or(str::is_empty) || !tool_calls.is_empty() {
            self.assistant_turn = Some(assistant);
        }
        for event in args_done {
            self.emit("response.function_call_arguments.done", &event, out);
        }
        for event in items_done {
            self.emit("response.output_item.done", &event, out);
        }

        self.emit(
            "response.completed",
            &json!({
                "type": "response.completed",
                "response": {
                    "id": self.response_id,
                    "object": "response",
                    "status": "completed",
                    "model": self.model,
                    "output": [],
                    "usage": responses_usage(self.usage),
                },
            }),
            out,
        );
        self.finished = true;
    }

    fn emit(&mut self, event_type: &str, data: &Value, out: &mut Vec<Bytes>) {
        self.sequence_number += 1;
        let mut event = data.clone();
        if let Value::Object(obj) = &mut event {
            obj.insert("sequence_number".to_string(), json!(self.sequence_number));
        }
        out.push(encode_sse_event(event_type, &event));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caps() -> ModelCapabilities {
        ModelCapabilities::default()
    }

    #[test]
    fn converts_basic_request() {
        let body = json!({
            "model": "gpt-5.6",
            "input": "hello",
            "max_output_tokens": 100,
            "stream": true,
            "instructions": "You are helpful"
        });
        let (out, _) = convert_responses_request(&body, &caps(), None, None).unwrap();
        assert_eq!(out["model"], "gpt-5.6");
        assert_eq!(out["max_tokens"], 100);
        assert_eq!(out["messages"][0]["role"], "system");
        assert_eq!(out["messages"][0]["content"], "You are helpful");
        assert_eq!(out["messages"][1]["role"], "user");
        assert_eq!(out["messages"][1]["content"], "hello");
        assert_eq!(out["stream_options"]["include_usage"], true);
    }

    #[test]
    fn maps_max_tokens_field_per_capability() {
        let mut caps = caps();
        caps.max_tokens_field = crate::dialect::MaxTokensField::MaxCompletionTokens;
        let body = json!({"model": "m", "max_output_tokens": 1000, "input": "hi"});
        let (out, _) = convert_responses_request(&body, &caps, None, None).unwrap();
        assert_eq!(out["max_completion_tokens"], 1000);
    }

    #[test]
    fn expands_input_items_with_tool_loop() {
        let body = json!({
            "model": "m",
            "max_output_tokens": 10,
            "input": [
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "weather?"}]},
                {"type": "function_call", "call_id": "call_1", "name": "get_weather", "arguments": "{\"city\": \"Paris\"}"},
                {"type": "function_call_output", "call_id": "call_1", "output": "sunny"},
                {"type": "message", "role": "user", "content": "thanks"}
            ]
        });
        let (out, _) = convert_responses_request(&body, &caps(), None, None).unwrap();
        let messages = out["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0]["role"], "user");
        assert_eq!(messages[0]["content"], "weather?");
        assert_eq!(messages[1]["role"], "assistant");
        assert_eq!(messages[1]["content"], "");
        assert_eq!(messages[1]["tool_calls"][0]["id"], "call_1");
        assert_eq!(
            messages[1]["tool_calls"][0]["function"]["name"],
            "get_weather"
        );
        assert_eq!(messages[2]["role"], "tool");
        assert_eq!(messages[2]["tool_call_id"], "call_1");
        assert_eq!(messages[2]["content"], "sunny");
        assert_eq!(messages[3]["role"], "user");
    }

    #[test]
    fn attaches_echoed_reasoning_to_following_assistant_message() {
        // Codex echoes the previous turn's reasoning as a reasoning item BEFORE the
        // assistant message it belongs to (real codex input sequence).
        let body = json!({
            "model": "m",
            "input": [
                {"type": "message", "role": "user", "content": "weather?"},
                {"type": "reasoning", "summary": [{"type": "summary_text", "text": "I should check the weather."}]},
                {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": ""}]},
                {"type": "function_call", "call_id": "call_1", "name": "get_weather", "arguments": "{\"city\": \"Paris\"}"},
                {"type": "function_call_output", "call_id": "call_1", "output": "sunny"}
            ]
        });
        let (out, _) = convert_responses_request(&body, &caps(), None, None).unwrap();
        let messages = out["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 3);
        // The assistant message carries the echoed reasoning as reasoning_content.
        let assistant = &messages[1];
        assert_eq!(assistant["role"], "assistant");
        assert_eq!(
            assistant["reasoning_content"],
            "I should check the weather."
        );
        assert_eq!(
            assistant["tool_calls"][0]["function"]["name"],
            "get_weather"
        );
        assert_eq!(messages[2]["role"], "tool");
    }

    #[test]
    fn attaches_echoed_reasoning_to_function_call_without_message() {
        // Some clients only send reasoning + function_call (no assistant message).
        let body = json!({
            "model": "m",
            "input": [
                {"type": "reasoning", "summary": [{"type": "summary_text", "text": "Think first"}]},
                {"type": "function_call", "call_id": "c1", "name": "f", "arguments": "{}"}
            ]
        });
        let (out, _) = convert_responses_request(&body, &caps(), None, None).unwrap();
        let assistant = &out["messages"].as_array().unwrap()[0];
        assert_eq!(assistant["role"], "assistant");
        assert_eq!(assistant["reasoning_content"], "Think first");
        assert_eq!(assistant["tool_calls"][0]["id"], "c1");
    }

    #[test]
    fn reattaches_last_reasoning_when_client_echoes_empty() {
        // Codex echoes reasoning as `{"summary": []}` (empty); the converter must
        // fall back to the remembered upstream reasoning.
        let body = json!({
            "model": "m",
            "input": [
                {"type": "reasoning", "summary": [], "content": null},
                {"type": "function_call", "call_id": "c1", "name": "exec_command", "arguments": "{}"}
            ]
        });
        let (out, _) =
            convert_responses_request(&body, &caps(), None, Some("remembered thinking")).unwrap();
        let assistant = &out["messages"].as_array().unwrap()[0];
        assert_eq!(assistant["reasoning_content"], "remembered thinking");
        assert_eq!(assistant["tool_calls"][0]["id"], "c1");
    }

    #[test]
    fn maps_reasoning_effort_and_format() {
        let mut caps = caps();
        caps.reasoning_effort = Some(crate::dialect::ReasoningEffortConfig {
            enabled: true,
            levels: vec!["low".into(), "high".into(), "max".into()],
            default: None,
        });
        let body = json!({
            "model": "m",
            "max_output_tokens": 1,
            "input": "hi",
            "reasoning": {"effort": "xhigh"},
            "text": {"format": {"type": "json_object"}}
        });
        let (out, _) = convert_responses_request(&body, &caps, None, None).unwrap();
        assert_eq!(out["thinking"]["type"], "enabled");
        assert_eq!(out["reasoning_effort"], "high");
        assert_eq!(out["response_format"]["type"], "json_object");
    }

    #[test]
    fn builtin_tools_map_to_provider_native_params() {
        let mut caps = caps();
        caps.server_tools = crate::dialect::ServerToolPolicy::ProviderNative;
        caps.always_enable_tools = vec!["web_search".to_string(), "code_execution".to_string()];
        caps.search_request_params =
            Some(json!({"web_search": {"enable": true, "search_query": "{search_query}"}}));
        caps.code_interpreter_request_params = Some(json!({"enable_code_interpreter": true}));

        let body = json!({
            "model": "m",
            "input": "what's the weather in Paris?",
            "stream": false,
            "tools": [{"type": "web_search_preview"}]
        });
        let (out, report) = convert_responses_request(&body, &caps, None, None).unwrap();
        assert_eq!(out["web_search"]["enable"], true);
        assert!(out["web_search"]["search_query"]
            .as_str()
            .unwrap()
            .contains("Paris"));
        assert_eq!(out["enable_code_interpreter"], true);
        // code interpreter forces streaming upstream (qwen requirement).
        assert_eq!(out["stream"], true);
        // Both tools were mapped, not dropped.
        assert_eq!(report.mapped_server_tools.len(), 2);
        assert!(report.dropped_server_tools.is_empty());
    }

    #[test]
    fn builtin_tools_dropped_when_upstream_has_no_equivalent() {
        let body = json!({
            "model": "m",
            "input": "hi",
            "tools": [{"type": "web_search_preview"}, {"type": "file_search"}]
        });
        let (out, report) = convert_responses_request(&body, &caps(), None, None).unwrap();
        assert!(out.get("tools").is_none() || out["tools"].as_array().unwrap().is_empty());
        assert_eq!(
            report.dropped_server_tools,
            vec!["web_search".to_string(), "file_search".to_string()]
        );
    }

    #[test]
    fn prepends_previous_messages_before_current_input() {
        let body = json!({
            "model": "m",
            "input": [
                {"type": "function_call_output", "call_id": "call_1", "output": "sunny"},
                {"type": "message", "role": "user", "content": "thanks"}
            ]
        });
        let previous = vec![
            json!({"role": "user", "content": "weather in Paris?"}),
            json!({"role": "assistant", "tool_calls": [{"id": "call_1", "type": "function", "function": {"name": "get_weather", "arguments": "{}"}}]}),
        ];
        let (out, _) = convert_responses_request(&body, &caps(), Some(&previous), None).unwrap();
        let messages = out["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0]["role"], "user");
        assert_eq!(messages[0]["content"], "weather in Paris?");
        assert_eq!(messages[1]["role"], "assistant");
        assert!(messages[1].get("tool_calls").is_some());
        assert_eq!(messages[2]["role"], "tool");
        assert_eq!(messages[2]["tool_call_id"], "call_1");
        assert_eq!(messages[3]["role"], "user");
        assert_eq!(messages[3]["content"], "thanks");
    }

    #[test]
    fn prepends_previous_after_instructions() {
        let body = json!({"model": "m", "input": "hi", "instructions": "You are helpful"});
        let previous = vec![json!({"role": "user", "content": "earlier"})];
        let (out, _) = convert_responses_request(&body, &caps(), Some(&previous), None).unwrap();
        let messages = out["messages"].as_array().unwrap();
        assert_eq!(messages[0]["role"], "system");
        assert_eq!(messages[1]["role"], "user");
        assert_eq!(messages[1]["content"], "earlier");
        assert_eq!(messages[2]["content"], "hi");
    }

    #[test]
    fn converts_non_streaming_response() {
        let body = json!({
            "id": "chatcmpl-abc",
            "model": "gpt-5.6",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello!",
                    "reasoning_content": "thinking",
                    "tool_calls": [{"id": "call_1", "type": "function", "function": {"name": "get_weather", "arguments": "{\"city\":\"Paris\"}"}}]
                },
                "finish_reason": "tool_calls"
            }],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "prompt_tokens_details": {"cached_tokens": 2}, "completion_tokens_details": {"reasoning_tokens": 3}}
        });
        let out = convert_chat_response_to_responses(&body).unwrap();
        assert_eq!(out["id"], "resp_chatcmpl-abc");
        assert_eq!(out["object"], "response");
        assert_eq!(out["status"], "completed");
        assert_eq!(out["output"][0]["type"], "reasoning");
        assert_eq!(out["output"][1]["type"], "message");
        assert_eq!(out["output"][1]["content"][0]["type"], "output_text");
        assert_eq!(out["output"][1]["content"][0]["text"], "Hello!");
        assert_eq!(out["output"][2]["type"], "function_call");
        assert_eq!(out["output"][2]["call_id"], "call_1");
        assert_eq!(out["usage"]["input_tokens"], 10);
        assert_eq!(out["usage"]["output_tokens"], 5);
        assert_eq!(out["usage"]["input_tokens_details"]["cached_tokens"], 2);
        assert_eq!(out["usage"]["output_tokens_details"]["reasoning_tokens"], 3);
    }

    #[test]
    fn maps_length_to_incomplete() {
        let body = json!({
            "id": "x",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "partial"}, "finish_reason": "length"}]
        });
        let out = convert_chat_response_to_responses(&body).unwrap();
        assert_eq!(out["status"], "incomplete");
        assert_eq!(out["incomplete_details"]["reason"], "max_output_tokens");
    }

    #[test]
    fn stream_converts_chunks_to_responses_events() {
        let mut converter = ResponsesStreamConverter::new();
        let mut out = Vec::new();
        converter.on_chunk(r#"{"id":"x","object":"chat.completion.chunk","model":"gpt-5.6","choices":[{"index":0,"delta":{"role":"assistant","content":null,"reasoning_content":""},"finish_reason":null}]}"#, &mut out).unwrap();
        converter.on_chunk(r#"{"id":"x","choices":[{"index":0,"delta":{"content":null,"reasoning_content":"We"},"finish_reason":null}]}"#, &mut out).unwrap();
        converter.on_chunk(r#"{"id":"x","choices":[{"index":0,"delta":{"content":"Hi","reasoning_content":null},"finish_reason":null}]}"#, &mut out).unwrap();
        converter.on_chunk(r#"{"id":"x","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":null}"#, &mut out).unwrap();
        converter
            .on_chunk(
                r#"{"id":"x","choices":[],"usage":{"prompt_tokens":6,"completion_tokens":9}}"#,
                &mut out,
            )
            .unwrap();
        converter.on_chunk("[DONE]", &mut out).unwrap();

        let text = String::from_utf8_lossy(&out.concat()).into_owned();
        assert!(text.contains("event: response.created"));
        assert!(text.contains("event: response.in_progress"));
        assert!(text.contains("\"type\":\"response.reasoning_text.delta\""));
        assert!(text.contains("\"delta\":\"We\""));
        assert!(text.contains("\"type\":\"response.output_text.delta\""));
        assert!(text.contains("\"delta\":\"Hi\""));
        assert!(text.contains("event: response.completed"));
        assert_eq!(text.matches("event: response.completed").count(), 1);
        // sequence numbers present and increasing.
        assert!(text.contains("\"sequence_number\":1"));
        assert!(text.contains("\"sequence_number\":7"));
    }

    #[test]
    fn stream_converts_tool_calls() {
        let mut converter = ResponsesStreamConverter::new();
        let mut out = Vec::new();
        converter.on_chunk(r#"{"id":"x","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":""}}]},"finish_reason":null}]}"#, &mut out).unwrap();
        converter.on_chunk(r#"{"id":"x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"city\": \"Pa"}}]},"finish_reason":null}]}"#, &mut out).unwrap();
        converter.on_chunk(r#"{"id":"x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"ris\"}"}}]},"finish_reason":null}]}"#, &mut out).unwrap();
        converter.on_chunk(r#"{"id":"x","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}],"usage":null}"#, &mut out).unwrap();
        converter.on_chunk("[DONE]", &mut out).unwrap();

        let text = String::from_utf8_lossy(&out.concat()).into_owned();
        assert!(text.contains("\"type\":\"function_call\""));
        assert!(text.contains("\"call_id\":\"call_1\""));
        assert!(text.contains("\"type\":\"response.function_call_arguments.delta\""));
        // Closing events carry the accumulated arguments and name.
        assert!(text.contains("\"type\":\"response.function_call_arguments.done\""));
        assert!(text.contains("\"arguments\":\"{\\\"city\\\": \\\"Paris\\\"}\""));
        assert!(text.contains("\"name\":\"get_weather\""));
        assert!(text.contains("event: response.completed"));
    }

    #[test]
    fn stream_exposes_response_id_and_assistant_turn() {
        let mut converter = ResponsesStreamConverter::new();
        let mut out = Vec::new();
        converter
            .on_chunk(
                r#"{"id":"chatcmpl-9","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":""}}]},"finish_reason":null}]}"#,
                &mut out,
            )
            .unwrap();
        converter
            .on_chunk(
                r#"{"id":"chatcmpl-9","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"city\":\"Paris\"}"}}]},"finish_reason":null}]}"#,
                &mut out,
            )
            .unwrap();
        converter
            .on_chunk(
                r#"{"id":"chatcmpl-9","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}],"usage":null}"#,
                &mut out,
            )
            .unwrap();
        converter.on_chunk("[DONE]", &mut out).unwrap();

        assert_eq!(converter.response_id(), Some("resp_chatcmpl-9"));
        let turn = converter.assistant_turn().expect("assistant turn recorded");
        assert_eq!(turn["role"], "assistant");
        assert_eq!(turn["tool_calls"][0]["function"]["name"], "get_weather");
        assert_eq!(
            turn["tool_calls"][0]["function"]["arguments"],
            r#"{"city":"Paris"}"#
        );
    }

    #[test]
    fn stream_assistant_turn_carries_text() {
        let mut converter = ResponsesStreamConverter::new();
        let mut out = Vec::new();
        converter
            .on_chunk(
                r#"{"id":"x","choices":[{"index":0,"delta":{"content":"Hi "},"finish_reason":null}]}"#,
                &mut out,
            )
            .unwrap();
        converter
            .on_chunk(
                r#"{"id":"x","choices":[{"index":0,"delta":{"content":"there"},"finish_reason":"stop"}]}"#,
                &mut out,
            )
            .unwrap();
        converter.finish(&mut out);
        let turn = converter.assistant_turn().expect("assistant turn recorded");
        assert_eq!(turn["content"], "Hi there");
        assert!(turn.get("tool_calls").is_none());
    }

    #[test]
    fn stream_error_event_emits_error_and_no_completed() {
        let mut converter = ResponsesStreamConverter::new();
        let mut out = Vec::new();
        converter
            .on_chunk(
                r#"{"id":"x","choices":[{"index":0,"delta":{"content":"Hi"},"finish_reason":null}]}"#,
                &mut out,
            )
            .unwrap();
        converter
            .on_chunk(
                r#"{"error":{"message":"upstream exploded","type":"server_error","code":"upstream_error"}}"#,
                &mut out,
            )
            .unwrap();
        converter.on_chunk("[DONE]", &mut out).unwrap();

        let text = String::from_utf8_lossy(&out.concat()).into_owned();
        assert!(text.contains("event: error"));
        assert!(text.contains("\"code\":\"upstream_error\""));
        assert!(text.contains("\"message\":\"upstream exploded\""));
        assert!(!text.contains("event: response.completed"));
    }

    #[test]
    fn finish_without_done_emits_completed() {
        let mut converter = ResponsesStreamConverter::new();
        let mut out = Vec::new();
        converter
            .on_chunk(
                r#"{"id":"x","choices":[{"index":0,"delta":{"content":"hi"}}]}"#,
                &mut out,
            )
            .unwrap();
        converter.finish(&mut out);
        let text = String::from_utf8_lossy(&out.concat()).into_owned();
        assert!(text.contains("event: response.completed"));
        assert_eq!(text.matches("event: response.completed").count(), 1);
    }
}
