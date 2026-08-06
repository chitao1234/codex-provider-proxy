//! OpenAI Responses API (downstream) to Chat Completions (upstream) conversion.
//!
//! The upstream side (Chat Completions shapes) is shared with the Messages conversion;
//! this module owns the downstream Responses protocol: request mapping (input items to
//! chat messages), non-streaming response mapping, and the streaming SSE state machine.

use serde_json::{json, Map, Value};

use crate::chat::{
    extract_usage, first_choice_finish_reason, first_choice_message, upstream_id, upstream_model,
    ChatUsage,
};
use crate::chat_request::{
    add_forced_server_tools, apply_max_tokens_cap, copy_bool, copy_string, copy_u64, copy_value,
    max_tokens_field,
};
use crate::dialect::{ModelCapabilities, RequestConversionReport};
use crate::error::ConversionError;

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

    add_forced_server_tools(caps, &mut report);
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
    // Ark (Volcengine) Responses-style parameter normalization.
    if caps.ark_style {
        apply_ark_style(&mut out);
    }

    Ok((Value::Object(out), report))
}

/// Normalize request parameters to Ark (Volcengine) Responses style.
///
/// Normalize the Chat Completions request generated from an Ark-style Responses request.
fn apply_ark_style(out: &mut Map<String, Value>) {
    // reasoning_effort (from downstream reasoning.effort) -> Ark thinking.type.
    if let Some(effort) = out.get("reasoning_effort").and_then(Value::as_str) {
        let thinking_type = if matches!(effort, "none" | "minimal") {
            "disabled"
        } else {
            "enabled"
        };
        out.insert("thinking".to_string(), json!({"type": thinking_type}));
        out.remove("reasoning_effort");
    }
    normalize_ark_web_search_tools(out);
}

/// Apply Ark-style parameter normalization directly to a Responses request object
/// (for same-protocol passthrough where the downstream already speaks Responses).
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

fn normalize_ark_web_search_tools(request: &mut Map<String, Value>) {
    if let Some(tools) = request.get_mut("tools").and_then(Value::as_array_mut) {
        for tool in tools {
            let Some(object) = tool.as_object_mut() else {
                continue;
            };
            if object.get("type").and_then(Value::as_str) != Some("web_search") {
                continue;
            }
            let search = object
                .entry("web_search".to_string())
                .or_insert_with(|| json!({}));
            if let Some(search) = search.as_object_mut() {
                if let Some(context) = search.remove("search_context_size") {
                    let max_keyword = match context.as_str() {
                        Some("low") => 1,
                        Some("medium") => 3,
                        _ => 5,
                    };
                    search.insert("max_keyword".to_string(), Value::from(max_keyword));
                }
            }
        }
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

// ---------------------------------------------------------------------------
// Streaming state machine
// ---------------------------------------------------------------------------

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
    fn ark_style_normalizes_reasoning_and_tools() {
        let mut caps = caps();
        caps.ark_style = true;
        caps.server_tools = crate::dialect::ServerToolPolicy::ProviderNative;
        caps.search_tool_template =
            Some(json!({"type": "web_search", "web_search": {"search_context_size": "medium"}}));
        let body = json!({
            "model": "doubao-seed-2-1-pro-260628",
            "input": "hi",
            "reasoning": {"effort": "high"},
            "tools": [{"type": "web_search_preview"}]
        });
        let (out, _) = convert_responses_request(&body, &caps, None, None).unwrap();
        assert_eq!(out["thinking"]["type"], "enabled");
        assert!(out.get("reasoning_effort").is_none());
        // Ark does not support caching in codex flow; nothing injected.
        assert!(out.get("caching").is_none());
        // web_search tool: search_context_size -> max_keyword.
        let tools = out["tools"].as_array().unwrap();
        let search = tools
            .iter()
            .find(|t| t.get("type") == Some(&json!("web_search")))
            .expect("web_search tool present");
        assert_eq!(search["web_search"]["max_keyword"], 3);
        assert!(search["web_search"].get("search_context_size").is_none());
    }

    #[test]
    fn ark_style_disables_thinking_for_minimal() {
        let mut caps = caps();
        caps.ark_style = true;
        let body = json!({
            "model": "m",
            "input": "hi",
            "reasoning": {"effort": "minimal"}
        });
        let (out, _) = convert_responses_request(&body, &caps, None, None).unwrap();
        assert_eq!(out["thinking"]["type"], "disabled");
    }

    #[test]
    fn ark_style_to_responses_object_mutates_in_place() {
        let mut request = json!({
            "model": "doubao-seed-2-1-pro-260628",
            "input": "hi",
            "reasoning": {"effort": "high"},
            "tools": [{"type": "web_search", "web_search": {"search_context_size": "low"}}]
        });
        let obj = request.as_object_mut().unwrap();
        apply_ark_style_to_responses(obj);
        assert_eq!(request["thinking"]["type"], "enabled");
        // Ark does not support caching in codex flow; nothing injected.
        assert!(request.get("caching").is_none());
        assert_eq!(request["tools"][0]["web_search"]["max_keyword"], 1);
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
}
