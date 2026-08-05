//! Anthropic Messages (downstream) to OpenAI Chat Completions (upstream) conversion:
//! request mapping, non-streaming response mapping, and the streaming SSE state machine.

use std::collections::HashMap;

use bytes::Bytes;
use serde_json::{json, Map, Value};

use crate::chat::{
    delta_string, extract_usage, first_choice, first_choice_delta, first_choice_finish_reason,
    first_choice_message, is_usage_only_chunk, upstream_id, upstream_model, ChatUsage,
};
use crate::dialect::{
    MaxTokensField, ModelCapabilities, ResponseFormatCap, ServerToolPolicy, ThinkingParam,
};
use crate::error::ConversionError;
use crate::messages::{
    classify_block, content_text_and_has_image, is_server_tool, server_tool_function_name,
    tool_result_to_string, ContentBlock, SystemPrompt,
};
use crate::sse::encode_sse_event;

/// What the request conversion did, for logging.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RequestConversionReport {
    /// Server tool definitions dropped because the upstream has no equivalent.
    pub dropped_server_tools: Vec<String>,
    /// Server tools mapped to function tools (web_search/web_fetch).
    pub mapped_server_tools: Vec<String>,
}

/// Convert an Anthropic Messages request body into a Chat Completions request body.
pub fn convert_messages_request(
    body: &Value,
    caps: &ModelCapabilities,
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
        "max_tokens",
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
    copy_value(request, &mut out, "temperature", "temperature");
    copy_user(request, &mut out);
    convert_stop_sequences(request, &mut out);
    convert_effort_and_thinking(request, caps, &mut out)?;
    convert_tool_choice(request, &mut out);
    convert_parallel_tool_use(request, caps, &mut out);
    convert_response_format(request, caps, &mut out)?;

    let system = SystemPrompt::parse(request.get("system"));
    let messages = expand_messages(request.get("messages"), caps)?;
    let (tools, mut report) = convert_tools(request, caps)?;
    // Force-enable configured server tools even when the client did not declare them.
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
    if let Some(search_params) = &caps.search_request_params {
        if report
            .mapped_server_tools
            .iter()
            .any(|tool| tool == "web_search")
        {
            let query = last_user_query(request.get("messages"));
            let rendered = render_search_tool_template(search_params, query.as_deref());
            if let Value::Object(params) = rendered {
                out.extend(params);
            }
        }
    }
    if let Some(fetch_params) = &caps.fetch_request_params {
        if report
            .mapped_server_tools
            .iter()
            .any(|tool| tool == "web_fetch")
        {
            let query = last_user_query(request.get("messages"));
            let rendered = render_search_tool_template(fetch_params, query.as_deref());
            if let Value::Object(params) = rendered {
                out.extend(params);
            }
        }
    }
    if let Some(code_params) = &caps.code_interpreter_request_params {
        if report
            .mapped_server_tools
            .iter()
            .any(|tool| tool == "code_execution")
        {
            let rendered = render_search_tool_template(code_params, None);
            if let Value::Object(params) = rendered {
                out.extend(params);
            }
            // qwen code interpreter requires streaming.
            out.insert("stream".to_string(), json!(true));
        }
    }

    let mut chat_messages: Vec<Value> =
        Vec::with_capacity(messages.len() + usize::from(!system.is_empty()));
    if !system.is_empty() {
        chat_messages.push(json!({"role": "system", "content": system.joined()}));
    }
    chat_messages.extend(messages);
    out.insert("messages".to_string(), Value::Array(chat_messages));
    if !tools.is_empty() {
        out.insert("tools".to_string(), Value::Array(tools));
    }

    Ok((Value::Object(out), report))
}

fn max_tokens_field(field: MaxTokensField) -> &'static str {
    match field {
        MaxTokensField::MaxTokens => "max_tokens",
        MaxTokensField::MaxCompletionTokens => "max_completion_tokens",
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

/// Clamp the copied `max_tokens` to the upstream's cap when configured.
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

fn copy_user(request: &Map<String, Value>, out: &mut Map<String, Value>) {
    if let Some(user) = request.get("user").and_then(Value::as_str) {
        if !user.is_empty() {
            out.insert("user".to_string(), Value::String(user.to_string()));
        }
    }
}

fn convert_stop_sequences(request: &Map<String, Value>, out: &mut Map<String, Value>) {
    let Some(sequences) = request.get("stop_sequences").and_then(Value::as_array) else {
        return;
    };
    let sequences: Vec<&str> = sequences
        .iter()
        .filter_map(Value::as_str)
        .filter(|sequence| !sequence.is_empty())
        .collect();
    match sequences.as_slice() {
        [] => {}
        [single] => {
            out.insert("stop".to_string(), Value::String((*single).to_string()));
        }
        many => {
            out.insert(
                "stop".to_string(),
                Value::Array(
                    many.iter()
                        .map(|s| Value::String((*s).to_string()))
                        .collect(),
                ),
            );
        }
    }
}

fn convert_effort_and_thinking(
    request: &Map<String, Value>,
    caps: &ModelCapabilities,
    out: &mut Map<String, Value>,
) -> Result<(), ConversionError> {
    let effort = request
        .get("output_config")
        .and_then(Value::as_object)
        .and_then(|output_config| output_config.get("effort"))
        .and_then(Value::as_str)
        .map(str::to_owned);

    if caps.thinking_param == ThinkingParam::TopLevel {
        let thinking = request.get("thinking");
        let thinking_type = thinking
            .and_then(|thinking| thinking.get("type"))
            .and_then(Value::as_str);
        match thinking_type {
            Some("disabled") => {
                out.insert("thinking".to_string(), json!({"type": "disabled"}));
            }
            Some("adaptive") | Some("enabled") | None if thinking.is_some() => {
                out.insert("thinking".to_string(), json!({"type": "enabled"}));
            }
            _ => {}
        }
    } else if caps.thinking_param == ThinkingParam::EnableThinking {
        // qwen / DashScope uses a top-level boolean.
        let thinking = request.get("thinking");
        let disabled = thinking
            .and_then(|thinking| thinking.get("type"))
            .and_then(Value::as_str)
            == Some("disabled");
        out.insert("enable_thinking".to_string(), json!(!disabled));
    }

    let reasoning_config = caps.reasoning_effort.as_ref();
    let configured = reasoning_config.filter(|config| config.enabled);
    if let Some(config) = configured {
        let level = effort
            .as_deref()
            .or(config.default.as_deref())
            .and_then(|effort| caps.clamp_effort(effort));
        if let Some(level) = level {
            out.insert(
                "reasoning_effort".to_string(),
                Value::String(level.to_string()),
            );
        }
    }
    Ok(())
}

fn convert_tool_choice(request: &Map<String, Value>, out: &mut Map<String, Value>) {
    let Some(choice) = request.get("tool_choice") else {
        return;
    };
    let Some(object) = choice.as_object() else {
        return;
    };
    let mapped = match object.get("type").and_then(Value::as_str) {
        Some("auto") => Some(json!("auto")),
        Some("any") => Some(json!("required")),
        Some("none") => Some(json!("none")),
        Some("tool") => object
            .get("name")
            .and_then(Value::as_str)
            .map(|name| json!({"type": "function", "function": {"name": name}})),
        _ => None,
    };
    if let Some(mapped) = mapped {
        out.insert("tool_choice".to_string(), mapped);
    }
}

fn convert_parallel_tool_use(
    request: &Map<String, Value>,
    caps: &ModelCapabilities,
    out: &mut Map<String, Value>,
) {
    if !caps.parallel_tool_calls {
        return;
    }
    let downstream_value = request
        .get("parallel_tool_use")
        .and_then(Value::as_bool)
        .or_else(|| {
            request
                .get("disable_parallel_tool_use")
                .and_then(Value::as_bool)
                .map(|disabled| !disabled)
        });
    if let Some(parallel) = downstream_value {
        out.insert("parallel_tool_calls".to_string(), Value::Bool(parallel));
    }
}

fn convert_response_format(
    request: &Map<String, Value>,
    caps: &ModelCapabilities,
    out: &mut Map<String, Value>,
) -> Result<(), ConversionError> {
    let Some(format) = request
        .get("output_config")
        .and_then(Value::as_object)
        .and_then(|output_config| output_config.get("format"))
    else {
        return Ok(());
    };
    let format_type = format.get("type").and_then(Value::as_str).unwrap_or("text");
    match format_type {
        "text" => {}
        "json_schema" => match caps.response_format {
            ResponseFormatCap::JsonSchema => {
                let schema = format.get("schema").cloned().unwrap_or_else(|| json!({}));
                out.insert(
                    "response_format".to_string(),
                    json!({"type": "json_schema", "json_schema": {"name": "output", "schema": schema}}),
                );
            }
            ResponseFormatCap::JsonObject => {
                out.insert(
                    "response_format".to_string(),
                    json!({"type": "json_object"}),
                );
            }
            ResponseFormatCap::Text => {
                return Err(ConversionError::unsupported(
                    "output_config.format",
                    "upstream does not support structured output formats",
                ));
            }
        },
        other => {
            return Err(ConversionError::unsupported(
                "output_config.format",
                format!("unknown format type {other:?}"),
            ));
        }
    }
    Ok(())
}

/// Expand the Anthropic `messages` array into Chat messages.
///
/// Rules (see design doc §4.2):
/// - Mid-conversation `role: "system"` becomes a `<system-reminder>` user message.
/// - `tool_result` blocks become `role: "tool"` messages emitted before the enclosing user turn.
/// - Assistant messages merge text + thinking + tool_use into one message; empty content is `""`.
fn expand_messages(
    messages: Option<&Value>,
    caps: &ModelCapabilities,
) -> Result<Vec<Value>, ConversionError> {
    let Some(messages) = messages else {
        return Ok(Vec::new());
    };
    let Some(messages) = messages.as_array() else {
        return Err(ConversionError::invalid("messages must be an array"));
    };

    let mut out: Vec<Value> = Vec::with_capacity(messages.len());
    for message in messages {
        let Some(object) = message.as_object() else {
            continue;
        };
        let role = object
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match role {
            "system" => {
                let text = object
                    .get("content")
                    .map(|content| content_text_and_has_image(content).0)
                    .unwrap_or_default();
                if !text.trim().is_empty() {
                    out.push(json!({"role": "user", "content": format!("<system-reminder>\n{text}\n</system-reminder>")}));
                }
            }
            "user" => {
                let Some(content) = object.get("content") else {
                    continue;
                };
                let Some(blocks) = content.as_array() else {
                    // Plain-string user turn (tool results only appear in block arrays).
                    out.push(json!({"role": "user", "content": content.clone()}));
                    continue;
                };
                let mut tool_results = Vec::new();
                let mut non_tool_blocks = Vec::new();
                for block in blocks {
                    match classify_block(block) {
                        ContentBlock::ToolResult {
                            tool_use_id,
                            content,
                            is_error,
                        } => {
                            let Some(tool_use_id) = tool_use_id.filter(|id| !id.is_empty()) else {
                                continue;
                            };
                            let mut text = tool_result_to_string(content, caps.image_input);
                            if is_error && !text.is_empty() {
                                text = format!("[error] {text}");
                            }
                            tool_results.push(json!({"role": "tool", "tool_call_id": tool_use_id, "content": text}));
                        }
                        _ => non_tool_blocks.push(block.clone()),
                    }
                }
                if !tool_results.is_empty() {
                    out.extend(tool_results);
                }
                if non_tool_blocks.is_empty() {
                    continue;
                }
                out.push(expand_user_content(&non_tool_blocks, caps)?);
            }
            "assistant" => out.push(expand_assistant_message(object)?),
            _ => out.push(message.clone()),
        }
    }
    Ok(out)
}

fn expand_user_content(
    blocks: &[Value],
    caps: &ModelCapabilities,
) -> Result<Value, ConversionError> {
    let mut text_parts = Vec::new();
    let mut images: Vec<Value> = Vec::new();
    for block in blocks {
        match classify_block(block) {
            ContentBlock::Text(text) if !text.is_empty() => text_parts.push(text.to_string()),
            ContentBlock::Image => images.push(convert_image_block(block, caps)?),
            ContentBlock::Thinking(_) | ContentBlock::RedactedThinking => {}
            _ => {
                return Err(ConversionError::unsupported(
                    "messages[].content",
                    "cannot convert content block type in user message",
                ));
            }
        }
    }
    if images.is_empty() {
        return Ok(json!({"role": "user", "content": text_parts.join("\n\n")}));
    }
    let mut content: Vec<Value> = Vec::new();
    if !text_parts.is_empty() {
        content.push(json!({"type": "text", "text": text_parts.join("\n\n")}));
    }
    content.extend(images);
    Ok(json!({"role": "user", "content": content}))
}

fn convert_image_block(block: &Value, caps: &ModelCapabilities) -> Result<Value, ConversionError> {
    if !caps.image_input {
        return Err(ConversionError::unsupported(
            "messages[].content",
            "image input is not supported by this upstream",
        ));
    }
    let source = block.get("source");
    let url = match source
        .and_then(|source| source.get("type"))
        .and_then(Value::as_str)
    {
        Some("base64") => {
            let media_type = source
                .and_then(|source| source.get("media_type"))
                .and_then(Value::as_str)
                .unwrap_or("application/octet-stream");
            let data = source
                .and_then(|source| source.get("data"))
                .and_then(Value::as_str)
                .ok_or_else(|| ConversionError::invalid("image block missing base64 data"))?;
            format!("data:{media_type};base64,{data}")
        }
        Some("url") => source
            .and_then(|source| source.get("url"))
            .and_then(Value::as_str)
            .ok_or_else(|| ConversionError::invalid("image block missing url"))?
            .to_string(),
        _ => {
            return Err(ConversionError::unsupported(
                "messages[].content",
                "unsupported image source type",
            ));
        }
    };
    Ok(json!({"type": "image_url", "image_url": {"url": url}}))
}

fn expand_assistant_message(object: &Map<String, Value>) -> Result<Value, ConversionError> {
    let Some(content) = object.get("content") else {
        return Ok(json!({"role": "assistant", "content": ""}));
    };
    let Some(blocks) = content.as_array() else {
        // Rare: assistant content as a plain string.
        let mut out = json!({"role": "assistant"});
        out.as_object_mut()
            .unwrap()
            .insert("content".to_string(), content.clone());
        return Ok(out);
    };

    let mut text_parts = Vec::new();
    let mut reasoning_parts = Vec::new();
    let mut tool_calls: Vec<Value> = Vec::new();
    for block in blocks {
        match classify_block(block) {
            ContentBlock::Text(text) if !text.is_empty() => text_parts.push(text.to_string()),
            ContentBlock::Thinking(text) if !text.trim().is_empty() => {
                reasoning_parts.push(text.to_string());
            }
            ContentBlock::RedactedThinking => {}
            ContentBlock::ToolUse { id, name, input } => {
                if id.is_empty() || name.is_empty() {
                    continue;
                }
                let arguments = match input {
                    Some(input) if input.is_object() => input.to_string(),
                    Some(input) if input.is_null() => "{}".to_string(),
                    _ => "{}".to_string(),
                };
                tool_calls.push(json!({
                    "id": id,
                    "type": "function",
                    "function": {"name": name, "arguments": arguments}
                }));
            }
            _ => {
                return Err(ConversionError::unsupported(
                    "messages[].content",
                    "cannot convert content block type in assistant message",
                ));
            }
        }
    }

    let mut out = Map::new();
    out.insert("role".to_string(), json!("assistant"));
    // OpenAI requires content to exist; empty string when the turn has only tool calls.
    out.insert("content".to_string(), json!(text_parts.join("\n\n")));
    if !reasoning_parts.is_empty() {
        out.insert(
            "reasoning_content".to_string(),
            json!(reasoning_parts.join("\n\n")),
        );
    }
    if !tool_calls.is_empty() {
        out.insert("tool_calls".to_string(), Value::Array(tool_calls));
    }
    Ok(Value::Object(out))
}

/// Convert the Anthropic `tools` array to Chat function tools per the server-tool policy.
fn convert_tools(
    request: &Map<String, Value>,
    caps: &ModelCapabilities,
) -> Result<(Vec<Value>, RequestConversionReport), ConversionError> {
    let mut report = RequestConversionReport::default();
    let Some(tools) = request.get("tools") else {
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
        if is_server_tool(tool) {
            match caps.server_tools {
                ServerToolPolicy::Drop => {
                    let name = tool_name(object);
                    report.dropped_server_tools.push(name);
                }
                ServerToolPolicy::MapToFunction => {
                    match server_tool_function_name(tool).as_deref() {
                        Some("web_search") => {
                            report.mapped_server_tools.push("web_search".to_string());
                            out.push(function_tool(
                                "web_search",
                                "Search the web for current information. Call this when the user asks about current events, recent information, or anything that may have changed since the model's knowledge cutoff.",
                                json!({"type": "object", "properties": {"query": {"type": "string", "description": "The search query"}}, "required": ["query"]}),
                            ));
                        }
                        Some("web_fetch") => {
                            report.mapped_server_tools.push("web_fetch".to_string());
                            out.push(function_tool(
                                "web_fetch",
                                "Fetch the content of a specific URL. Call this when you need the full content of a page the user referenced.",
                                json!({"type": "object", "properties": {"url": {"type": "string", "description": "The URL to fetch"}}, "required": ["url"]}),
                            ));
                        }
                        _ => {
                            // code_execution, bash, etc. have no client-executed function
                            // equivalent; drop them.
                            let name = tool_name(object);
                            report.dropped_server_tools.push(name);
                        }
                    }
                }
                ServerToolPolicy::ProviderNative => {
                    match server_tool_function_name(tool).as_deref() {
                        Some("web_search") => {
                            if let Some(template) = caps.search_tool_template.clone() {
                                report.mapped_server_tools.push("web_search".to_string());
                                // The upstream needs a real search query (glm requires it);
                                // derive one from the last user message.
                                let query = last_user_query(request.get("messages"));
                                out.push(render_search_tool_template(&template, query.as_deref()));
                            } else if caps.search_request_params.is_some() {
                                // Search is enabled via top-level request parameters
                                // (e.g. qwen enable_search); the tool itself is dropped.
                                report.mapped_server_tools.push("web_search".to_string());
                            } else {
                                report.dropped_server_tools.push("web_search".to_string());
                            }
                        }
                        Some("web_fetch") => {
                            if caps.fetch_request_params.is_some() {
                                // Web fetch is enabled via top-level request parameters
                                // (e.g. qwen agent_max); the tool itself is dropped.
                                report.mapped_server_tools.push("web_fetch".to_string());
                            } else {
                                report.dropped_server_tools.push("web_fetch".to_string());
                            }
                        }
                        Some("code_execution") => {
                            if caps.code_interpreter_request_params.is_some() {
                                // Code execution is enabled via top-level request parameters
                                // (e.g. qwen enable_code_interpreter); the tool itself is dropped.
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
                            // code_execution and other server tools have no native chat form.
                            let name = tool_name(object);
                            report.dropped_server_tools.push(name);
                        }
                    }
                }
                ServerToolPolicy::Passthrough => out.push(tool.clone()),
            }
            continue;
        }

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
            .get("input_schema")
            .map(normalize_input_schema)
            .unwrap_or_else(|| json!({"type": "object", "properties": {}}));
        out.push(function_tool(name, &description, parameters));
    }
    Ok((out, report))
}

fn tool_name(object: &Map<String, Value>) -> String {
    object
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

/// Render a native search tool template, substituting the `{search_query}` placeholder
/// (when present) with a query derived from the conversation (the last user text).
fn render_search_tool_template(template: &Value, query: Option<&str>) -> Value {
    let Some(object) = template.as_object() else {
        return template.clone();
    };
    let mut rendered = object.clone();
    let replacement = query.filter(|q| !q.is_empty()).unwrap_or("search");
    let substitute = |value: &mut Value| {
        if let Value::String(text) = value {
            if text.contains("{search_query}") {
                *value = Value::String(text.replace("{search_query}", replacement));
            }
        }
    };
    for value in rendered.values_mut() {
        substitute(value);
        if let Value::Object(nested) = value {
            for nested_value in nested.values_mut() {
                substitute(nested_value);
            }
        }
    }
    Value::Object(rendered)
}

/// Extract a search query from the last user message (text blocks joined, trimmed).
fn last_user_query(messages: Option<&Value>) -> Option<String> {
    let messages = messages?.as_array()?;
    for message in messages.iter().rev() {
        let object = message.as_object()?;
        if object.get("role").and_then(Value::as_str) != Some("user") {
            continue;
        }
        let (text, _) = content_text_and_has_image(object.get("content")?);
        let text = text.trim();
        if !text.is_empty() {
            return Some(text.chars().take(60).collect());
        }
    }
    None
}

fn function_tool(name: &str, description: &str, parameters: Value) -> Value {
    let mut function = Map::new();
    function.insert("name".to_string(), json!(name));
    if !description.is_empty() {
        function.insert("description".to_string(), json!(description));
    }
    function.insert("parameters".to_string(), parameters);
    json!({"type": "function", "function": function})
}

/// Normalize an Anthropic input_schema for use as Chat `parameters`:
/// strip the `$schema` marker and ensure `properties` exists on object schemas.
fn normalize_input_schema(schema: &Value) -> Value {
    let Some(object) = schema.as_object() else {
        return json!({"type": "object", "properties": {}});
    };
    let mut normalized = object.clone();
    normalized.remove("$schema");
    if normalized.get("type").and_then(Value::as_str) == Some("object")
        && !normalized.contains_key("properties")
    {
        normalized.insert("properties".to_string(), json!({}));
    }
    Value::Object(normalized)
}

// ---------------------------------------------------------------------------
// Response conversion
// ---------------------------------------------------------------------------

/// Convert a non-streaming Chat Completions response body into an Anthropic Messages body.
pub fn convert_chat_response(body: &Value) -> Result<Value, ConversionError> {
    if first_choice(body).is_none() {
        return Err(ConversionError::invalid(
            "upstream chat response missing choices",
        ));
    }
    let message = first_choice_message(body)
        .ok_or_else(|| ConversionError::invalid("upstream chat response missing message"))?;

    let message_id = synth_message_id(upstream_id(body));
    let model = upstream_model(body).unwrap_or_default().to_string();

    let mut content: Vec<Value> = Vec::new();
    if let Some(reasoning) = message
        .get("reasoning_content")
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
    {
        content.push(json!({
            "type": "thinking",
            "thinking": reasoning,
            "signature": message_id,
        }));
    }
    if let Some(text) = message
        .get("content")
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
    {
        content.push(json!({"type": "text", "text": text}));
    }
    if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
        for call in tool_calls {
            let Some(call) = call.as_object() else {
                continue;
            };
            let function = call.get("function").and_then(Value::as_object);
            let name = function
                .and_then(|function| function.get("name"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            let id = call
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .unwrap_or_default();
            if name.is_empty() {
                continue;
            }
            let input = function
                .and_then(|function| function.get("arguments"))
                .and_then(Value::as_str)
                .and_then(|arguments| serde_json::from_str::<Value>(arguments).ok())
                .filter(Value::is_object)
                .unwrap_or_else(|| json!({}));
            content.push(json!({"type": "tool_use", "id": id, "name": name, "input": input}));
        }
    }

    let finish_reason = first_choice_finish_reason(body);
    let saw_tool_call = !content.is_empty()
        && content
            .iter()
            .any(|block| block.get("type").and_then(Value::as_str) == Some("tool_use"));
    let stop_reason = map_finish_reason(finish_reason, saw_tool_call);

    let usage = extract_usage(body);
    let usage_value = messages_usage(usage);

    Ok(json!({
        "id": message_id,
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": content,
        "stop_reason": stop_reason,
        "stop_sequence": null,
        "usage": usage_value,
    }))
}

/// Build the Anthropic usage object from Chat usage (present fields only).
fn messages_usage(usage: ChatUsage) -> Value {
    let mut usage_out = Map::new();
    if let Some(input) = usage.prompt_tokens {
        usage_out.insert("input_tokens".to_string(), json!(input));
    }
    if let Some(output) = usage.completion_tokens {
        usage_out.insert("output_tokens".to_string(), json!(output));
    }
    if let Some(cached) = usage.cached_tokens {
        usage_out.insert("cache_read_input_tokens".to_string(), json!(cached));
    }
    if let Some(reasoning) = usage.reasoning_tokens {
        usage_out.insert(
            "output_tokens_details".to_string(),
            json!({"thinking_tokens": reasoning}),
        );
    }
    Value::Object(usage_out)
}

/// `msg_<upstream id>`; upstream ids already prefixed with `msg_` pass through.
fn synth_message_id(upstream_id: Option<&str>) -> String {
    match upstream_id {
        Some(id) if id.starts_with("msg_") => id.to_string(),
        Some(id) => format!("msg_{id}"),
        None => String::new(),
    }
}

fn map_finish_reason(finish_reason: Option<&str>, saw_tool_call: bool) -> &'static str {
    match finish_reason {
        Some("stop") => "end_turn",
        Some("length") => "max_tokens",
        Some("tool_calls") | Some("function_call") => {
            if saw_tool_call {
                "tool_use"
            } else {
                // Never announce tool_use without any announced tool_use block.
                "end_turn"
            }
        }
        Some("content_filter") => "end_turn",
        _ => "end_turn",
    }
}

// ---------------------------------------------------------------------------
// Streaming state machine
// ---------------------------------------------------------------------------

/// Per-upstream-index tool call accumulator.
#[derive(Debug, Default)]
struct ToolAccumulator {
    id: Option<String>,
    name: Option<String>,
    /// Full accumulated arguments JSON.
    arguments: String,
    /// Arguments already emitted as `input_json_delta` (clients append each delta, so we
    /// must send increments, never the cumulative value).
    emitted_arguments_len: usize,
    /// Downstream content-block index once the tool_use block has been announced.
    block_index: Option<usize>,
}

/// Incremental converter from upstream Chat Completions SSE chunks to downstream
/// Anthropic Messages SSE events.
///
/// Feed one `data:` payload (JSON chunk or `[DONE]`) per `on_chunk` call; each call appends
/// zero or more complete SSE events (each ending with `\n\n`) to `out`. Call `finish` when the
/// upstream stream ends (with or without `[DONE]`) so `message_delta`/`message_stop` are always
/// emitted exactly once.
pub struct ChatStreamConverter {
    started: bool,
    ended: bool,
    message_id: String,
    model: String,
    stop_reason: Option<&'static str>,
    text_open: bool,
    text_index: usize,
    thinking_open: bool,
    thinking_index: usize,
    next_block_index: usize,
    tools: HashMap<usize, ToolAccumulator>,
    usage: ChatUsage,
    message_delta_sent: bool,
    message_stop_sent: bool,
}

impl Default for ChatStreamConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl ChatStreamConverter {
    pub fn new() -> Self {
        Self {
            started: false,
            ended: false,
            message_id: String::new(),
            model: String::new(),
            stop_reason: None,
            text_open: false,
            text_index: 0,
            thinking_open: false,
            thinking_index: 0,
            next_block_index: 0,
            tools: HashMap::new(),
            usage: ChatUsage::default(),
            message_delta_sent: false,
            message_stop_sent: false,
        }
    }

    /// Process one upstream SSE `data:` payload. `payload` is the raw line content
    /// (`{"object":"chat.completion.chunk",...}` or `[DONE]`).
    pub fn on_chunk(&mut self, payload: &str, out: &mut Vec<Bytes>) -> Result<(), ConversionError> {
        if self.ended {
            return Ok(());
        }
        if payload == "[DONE]" {
            self.finish(out);
            return Ok(());
        }
        let chunk: Value = serde_json::from_str(payload)
            .map_err(|_| ConversionError::invalid("upstream SSE chunk is not valid JSON"))?;

        if !self.started {
            self.begin(&chunk, out);
        }

        if let Some(delta) = first_choice_delta(&chunk) {
            self.process_delta(delta, out);
        }
        if let Some(finish_reason) = first_choice_finish_reason(&chunk) {
            self.stop_reason = Some(map_finish_reason(Some(finish_reason), self.saw_tool_call()));
            self.close_blocks(out);
        }
        let usage = extract_usage(&chunk);
        if !usage.is_empty() {
            self.usage = usage;
        }
        if is_usage_only_chunk(&chunk) {
            // Final usage chunk (`choices: []`), when `stream_options.include_usage` is set.
            self.emit_message_delta(out);
            self.emit_message_stop(out);
        }
        Ok(())
    }

    /// End of the upstream stream (with or without `[DONE]`): close any open blocks and emit
    /// `message_delta`/`message_stop` exactly once.
    pub fn finish(&mut self, out: &mut Vec<Bytes>) {
        if self.ended {
            return;
        }
        self.ended = true;
        if !self.started {
            return;
        }
        self.close_blocks(out);
        self.emit_message_delta(out);
        self.emit_message_stop(out);
    }

    fn saw_tool_call(&self) -> bool {
        self.tools.values().any(|tool| tool.block_index.is_some())
    }

    fn begin(&mut self, chunk: &Value, out: &mut Vec<Bytes>) {
        self.started = true;
        self.message_id = synth_message_id(upstream_id(chunk));
        self.model = upstream_model(chunk).unwrap_or_default().to_string();
        out.push(encode_sse_event(
            "message_start",
            &json!({
                "type": "message_start",
                "message": {
                    "id": self.message_id,
                    "type": "message",
                    "role": "assistant",
                    "model": self.model,
                    "content": [],
                    "stop_reason": null,
                    "stop_sequence": null,
                    "usage": {"input_tokens": 0, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0, "output_tokens": 0}
                }
            }),
        ));
    }

    fn process_delta(&mut self, delta: &Value, out: &mut Vec<Bytes>) {
        if let Some(reasoning) = delta_string(delta, "reasoning_content") {
            self.close_text(out);
            self.open_thinking(out);
            self.thinking_delta(reasoning, out);
        }
        if let Some(text) = delta_string(delta, "content") {
            self.close_thinking(out);
            self.open_text(out);
            self.text_delta(text, out);
        }
        if let Some(tool_calls) = delta.get("tool_calls").and_then(Value::as_array) {
            for (position, call) in tool_calls.iter().enumerate() {
                self.process_tool_call(position, call, out);
            }
        }
    }

    fn open_text(&mut self, out: &mut Vec<Bytes>) {
        if self.text_open {
            return;
        }
        self.text_open = true;
        self.text_index = self.allocate_block_index();
        out.push(encode_sse_event(
            "content_block_start",
            &json!({
                "type": "content_block_start",
                "index": self.text_index,
                "content_block": {"type": "text", "text": ""}
            }),
        ));
    }

    fn open_thinking(&mut self, out: &mut Vec<Bytes>) {
        if self.thinking_open {
            return;
        }
        self.thinking_open = true;
        self.thinking_index = self.allocate_block_index();
        out.push(encode_sse_event(
            "content_block_start",
            &json!({
                "type": "content_block_start",
                "index": self.thinking_index,
                "content_block": {"type": "thinking", "thinking": "", "signature": ""}
            }),
        ));
    }

    fn text_delta(&mut self, text: &str, out: &mut Vec<Bytes>) {
        out.push(encode_sse_event(
            "content_block_delta",
            &json!({
                "type": "content_block_delta",
                "index": self.text_index,
                "delta": {"type": "text_delta", "text": text}
            }),
        ));
    }

    fn thinking_delta(&mut self, thinking: &str, out: &mut Vec<Bytes>) {
        out.push(encode_sse_event(
            "content_block_delta",
            &json!({
                "type": "content_block_delta",
                "index": self.thinking_index,
                "delta": {"type": "thinking_delta", "thinking": thinking}
            }),
        ));
    }

    fn close_text(&mut self, out: &mut Vec<Bytes>) {
        if !self.text_open {
            return;
        }
        self.text_open = false;
        out.push(encode_sse_event(
            "content_block_stop",
            &json!({"type": "content_block_stop", "index": self.text_index}),
        ));
    }

    fn close_thinking(&mut self, out: &mut Vec<Bytes>) {
        if !self.thinking_open {
            return;
        }
        self.thinking_open = false;
        out.push(encode_sse_event(
            "content_block_stop",
            &json!({"type": "content_block_stop", "index": self.thinking_index}),
        ));
    }

    fn process_tool_call(&mut self, position: usize, call: &Value, out: &mut Vec<Bytes>) {
        let Some(call) = call.as_object() else { return };
        let upstream_index = call
            .get("index")
            .and_then(Value::as_u64)
            .map(|index| index as usize)
            .unwrap_or(position);

        let new_id = call
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .map(str::to_owned);
        let function = call.get("function").and_then(Value::as_object);
        let new_name = function
            .and_then(|function| function.get("name"))
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .map(str::to_owned);
        let new_arguments = function
            .and_then(|function| function.get("arguments"))
            .and_then(Value::as_str)
            .map(str::to_owned);

        let announce = {
            let accumulator = self.tools.entry(upstream_index).or_default();
            if let Some(id) = new_id {
                accumulator.id = Some(id);
            }
            if let Some(name) = new_name {
                accumulator.name = Some(name);
            }
            if let Some(arguments) = new_arguments {
                accumulator.arguments.push_str(&arguments);
            }
            accumulator.block_index.is_none()
                && accumulator.id.is_some()
                && accumulator.name.is_some()
        };

        if announce {
            let (id, name) = {
                let accumulator = &self.tools[&upstream_index];
                (
                    accumulator.id.clone().expect("checked above"),
                    accumulator.name.clone().expect("checked above"),
                )
            };
            self.close_text(out);
            self.close_thinking(out);
            let index = self.allocate_block_index();
            self.tools
                .get_mut(&upstream_index)
                .expect("inserted above")
                .block_index = Some(index);
            out.push(encode_sse_event(
                "content_block_start",
                &json!({
                    "type": "content_block_start",
                    "index": index,
                    "content_block": {"type": "tool_use", "id": id, "name": name, "input": {}}
                }),
            ));
        }

        // Emit only the arguments increment since the last emission: Anthropic clients
        // (e.g. the SDK MessageStream) append each `partial_json` to the running buffer.
        let (block_index, increment) = {
            let accumulator = &mut self.tools.get_mut(&upstream_index).expect("inserted above");
            if accumulator.block_index.is_none() {
                (None, None)
            } else {
                let start = accumulator.emitted_arguments_len;
                let increment = accumulator.arguments[start..].to_string();
                accumulator.emitted_arguments_len = accumulator.arguments.len();
                (accumulator.block_index, Some(increment))
            }
        };
        if let (Some(index), Some(increment)) = (block_index, increment) {
            if !increment.is_empty() {
                out.push(encode_sse_event(
                    "content_block_delta",
                    &json!({
                        "type": "content_block_delta",
                        "index": index,
                        "delta": {"type": "input_json_delta", "partial_json": increment}
                    }),
                ));
            }
        }
    }

    fn close_blocks(&mut self, out: &mut Vec<Bytes>) {
        self.close_thinking(out);
        self.close_text(out);
        let mut indices: Vec<(usize, String)> = self
            .tools
            .values()
            .filter_map(|tool| {
                tool.block_index.map(|index| {
                    let start = tool.emitted_arguments_len;
                    (index, tool.arguments[start..].to_string())
                })
            })
            .collect();
        indices.sort_by_key(|(index, _)| *index);
        for (index, arguments) in indices {
            if !arguments.is_empty() {
                out.push(encode_sse_event(
                    "content_block_delta",
                    &json!({
                        "type": "content_block_delta",
                        "index": index,
                        "delta": {"type": "input_json_delta", "partial_json": arguments}
                    }),
                ));
            }
            out.push(encode_sse_event(
                "content_block_stop",
                &json!({"type": "content_block_stop", "index": index}),
            ));
        }
        self.tools.clear();
    }

    fn emit_message_delta(&mut self, out: &mut Vec<Bytes>) {
        if self.message_delta_sent {
            return;
        }
        self.message_delta_sent = true;
        let stop_reason = self.stop_reason.unwrap_or("end_turn");
        let mut usage_out = Map::new();
        if let Some(output) = self.usage.completion_tokens {
            usage_out.insert("output_tokens".to_string(), json!(output));
        }
        out.push(encode_sse_event(
            "message_delta",
            &json!({
                "type": "message_delta",
                "delta": {"stop_reason": stop_reason, "stop_sequence": null},
                "usage": usage_out
            }),
        ));
    }

    fn emit_message_stop(&mut self, out: &mut Vec<Bytes>) {
        if self.message_stop_sent {
            return;
        }
        self.message_stop_sent = true;
        out.push(encode_sse_event(
            "message_stop",
            &json!({"type": "message_stop"}),
        ));
    }

    fn allocate_block_index(&mut self) -> usize {
        let index = self.next_block_index;
        self.next_block_index += 1;
        index
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn caps() -> ModelCapabilities {
        ModelCapabilities::default()
    }

    #[test]
    fn clamps_max_tokens_to_upstream_cap() {
        let mut caps = caps();
        caps.max_tokens_cap = Some(393216);
        let body = json!({"model": "m", "max_tokens": 1000000, "messages": []});
        let (out, _) = convert_messages_request(&body, &caps).unwrap();
        assert_eq!(out["max_tokens"], 393216);

        let body = json!({"model": "m", "max_tokens": 64000, "messages": []});
        let (out, _) = convert_messages_request(&body, &caps).unwrap();
        assert_eq!(out["max_tokens"], 64000);
    }

    #[test]
    fn converts_basic_request() {
        let body = json!({
            "model": "deepseek-v4-pro",
            "max_tokens": 64000,
            "stream": true,
            "temperature": 0.5,
            "stop_sequences": ["END"],
            "system": "You are helpful",
            "messages": [{"role": "user", "content": "hello"}]
        });
        let (out, report) = convert_messages_request(&body, &caps()).unwrap();
        assert_eq!(out["model"], "deepseek-v4-pro");
        assert_eq!(out["max_tokens"], 64000);
        assert_eq!(out["stream"], true);
        assert_eq!(out["stream_options"]["include_usage"], true);
        assert_eq!(out["stop"], "END");
        assert_eq!(out["messages"][0]["role"], "system");
        assert_eq!(out["messages"][1]["role"], "user");
        assert!(report.dropped_server_tools.is_empty());
    }

    #[test]
    fn maps_max_tokens_field_per_capability() {
        let mut caps = caps();
        caps.max_tokens_field = MaxTokensField::MaxCompletionTokens;
        let body = json!({"model": "gpt-5.5", "max_tokens": 1000, "messages": []});
        let (out, _) = convert_messages_request(&body, &caps).unwrap();
        assert_eq!(out["max_completion_tokens"], 1000);
        assert!(out.get("max_tokens").is_none());
    }

    #[test]
    fn strips_cache_control_and_unknown_anthropic_fields() {
        let body = json!({
            "model": "deepseek-v4-pro",
            "max_tokens": 100,
            "context_management": {"edits": []},
            "metadata": {"client_app": "claude-code"},
            "cache_control": {"type": "ephemeral"},
            "service_tier": "standard",
            "system": [{"type": "text", "text": "x-anthropic-billing-header: cc_version=2;", "cache_control": {"type": "ephemeral"}}, {"type": "text", "text": "real system"}],
            "messages": [{"role": "user", "content": [{"type": "text", "text": "hi", "cache_control": {"type": "ephemeral"}}]}]
        });
        let (out, _) = convert_messages_request(&body, &caps()).unwrap();
        assert!(out.get("context_management").is_none());
        assert!(out.get("metadata").is_none());
        assert!(out.get("cache_control").is_none());
        assert!(out.get("service_tier").is_none());
        assert_eq!(out["messages"][0]["content"], "real system");
        assert_eq!(out["messages"][1]["content"], "hi");
    }

    #[test]
    fn converts_tool_loop_messages() {
        let body = json!({
            "model": "deepseek-v4-pro",
            "max_tokens": 100,
            "messages": [
                {"role": "user", "content": "weather?"},
                {"role": "assistant", "content": [
                    {"type": "thinking", "thinking": "let me call the tool", "signature": "sig1"},
                    {"type": "tool_use", "id": "toolu_1", "name": "get_weather", "input": {"city": "Paris"}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "toolu_1", "content": "sunny"},
                    {"type": "text", "text": "now summarize"}
                ]}
            ]
        });
        let (out, _) = convert_messages_request(&body, &caps()).unwrap();
        let messages = out["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[1]["role"], "assistant");
        assert_eq!(messages[1]["content"], "");
        assert_eq!(messages[1]["reasoning_content"], "let me call the tool");
        assert_eq!(messages[1]["tool_calls"][0]["id"], "toolu_1");
        assert_eq!(
            messages[1]["tool_calls"][0]["function"]["name"],
            "get_weather"
        );
        assert_eq!(
            messages[1]["tool_calls"][0]["function"]["arguments"],
            "{\"city\":\"Paris\"}"
        );
        assert_eq!(messages[2]["role"], "tool");
        assert_eq!(messages[2]["tool_call_id"], "toolu_1");
        assert_eq!(messages[2]["content"], "sunny");
        assert_eq!(messages[3]["role"], "user");
        assert_eq!(messages[3]["content"], "now summarize");
    }

    #[test]
    fn mid_conversation_system_becomes_system_reminder() {
        let body = json!({
            "model": "m",
            "max_tokens": 10,
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "system", "content": "Terse mode"},
                {"role": "assistant", "content": "ok"}
            ]
        });
        let (out, _) = convert_messages_request(&body, &caps()).unwrap();
        let messages = out["messages"].as_array().unwrap();
        assert_eq!(messages[1]["role"], "user");
        assert!(messages[1]["content"]
            .as_str()
            .unwrap()
            .contains("<system-reminder>"));
    }

    #[test]
    fn converts_tools_and_drops_server_tools_by_default() {
        let body = json!({
            "model": "m",
            "max_tokens": 10,
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [
                {"name": "Read", "description": "read a file", "input_schema": {"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]}},
                {"type": "web_search_20260209", "name": "web_search"},
                {"type": "code_execution_20260120", "name": "code_execution"}
            ]
        });
        let (out, report) = convert_messages_request(&body, &caps()).unwrap();
        let tools = out["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["type"], "function");
        assert_eq!(tools[0]["function"]["name"], "Read");
        assert!(tools[0]["function"]["parameters"].get("$schema").is_none());
        assert!(tools[0]["function"]["parameters"]
            .get("properties")
            .is_some());
        assert_eq!(
            report.dropped_server_tools,
            vec!["web_search", "code_execution"]
        );
    }

    #[test]
    fn maps_server_tools_to_provider_native_shape() {
        let mut caps = caps();
        caps.server_tools = ServerToolPolicy::ProviderNative;
        caps.search_tool_template = Some(json!({
            "type": "web_search",
            "web_search": {"search_result": true, "search_query": "{search_query}"}
        }));
        let body = json!({
            "model": "m",
            "max_tokens": 10,
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [
                {"type": "web_search_20260209", "name": "web_search"},
                {"type": "web_fetch_20260209", "name": "web_fetch"},
                {"type": "code_execution_20260120", "name": "code_execution"}
            ]
        });
        let (out, report) = convert_messages_request(&body, &caps).unwrap();
        let tools = out["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["type"], "web_search");
        assert_eq!(tools[0]["web_search"]["search_result"], true);
        // {search_query} is replaced with the last user message text so the provider
        // actually performs a search (glm requires a real query).
        assert_eq!(tools[0]["web_search"]["search_query"], "hi");
        assert_eq!(report.mapped_server_tools, vec!["web_search"]);
        assert_eq!(
            report.dropped_server_tools,
            vec!["web_fetch", "code_execution"]
        );
    }

    #[test]
    fn always_enable_tools_forces_params_without_client_tools() {
        let mut caps = caps();
        caps.server_tools = ServerToolPolicy::ProviderNative;
        caps.search_request_params = Some(json!({"enable_search": true}));
        caps.code_interpreter_request_params = Some(json!({"enable_code_interpreter": true}));
        caps.always_enable_tools = vec!["web_search".to_string(), "code_execution".to_string()];
        // Client declares no server tools at all.
        let body = json!({
            "model": "m",
            "max_tokens": 10,
            "stream": true,
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [{"name": "Read", "description": "read", "input_schema": {"type": "object"}}]
        });
        let (out, report) = convert_messages_request(&body, &caps).unwrap();
        // Search and code interpreter params are force-merged.
        assert_eq!(out["enable_search"], true);
        assert_eq!(out["enable_code_interpreter"], true);
        // Tools array still only has the client's own function.
        let tools = out["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["function"]["name"], "Read");
        assert!(report
            .mapped_server_tools
            .contains(&"web_search".to_string()));
        assert!(report
            .mapped_server_tools
            .contains(&"code_execution".to_string()));
    }

    #[test]
    fn provider_native_code_interpreter_params_enable_qwen() {
        let mut caps = caps();
        caps.server_tools = ServerToolPolicy::ProviderNative;
        caps.code_interpreter_request_params = Some(json!({
            "enable_code_interpreter": true,
            "enable_thinking": true
        }));
        let body = json!({
            "model": "m",
            "max_tokens": 10,
            "stream": false,
            "messages": [{"role": "user", "content": "12 的 3 次方是多少？"}],
            "tools": [
                {"type": "code_execution_20260120", "name": "code_execution"},
                {"name": "Read", "description": "read", "input_schema": {"type": "object"}}
            ]
        });
        let (out, report) = convert_messages_request(&body, &caps).unwrap();
        assert_eq!(out["enable_code_interpreter"], true);
        assert_eq!(out["enable_thinking"], true);
        // qwen code interpreter requires streaming; forced on.
        assert_eq!(out["stream"], true);
        let tools = out["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["function"]["name"], "Read");
        assert_eq!(report.mapped_server_tools, vec!["code_execution"]);
    }

    #[test]
    fn provider_native_fetch_params_enable_qwen_web_extractor() {
        let mut caps = caps();
        caps.server_tools = ServerToolPolicy::ProviderNative;
        caps.fetch_request_params = Some(json!({
            "enable_search": true,
            "search_options": {"search_strategy": "agent_max"},
            "enable_thinking": true
        }));
        let body = json!({
            "model": "m",
            "max_tokens": 10,
            "messages": [{"role": "user", "content": "总结 https://example.com/a 的内容"}],
            "tools": [
                {"type": "web_fetch_20260209", "name": "web_fetch"},
                {"name": "Read", "description": "read", "input_schema": {"type": "object"}}
            ]
        });
        let (out, report) = convert_messages_request(&body, &caps).unwrap();
        // agent_max lets the model scrape URLs from the prompt.
        assert_eq!(out["enable_search"], true);
        assert_eq!(out["search_options"]["search_strategy"], "agent_max");
        assert_eq!(out["enable_thinking"], true);
        let tools = out["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["function"]["name"], "Read");
        assert_eq!(report.mapped_server_tools, vec!["web_fetch"]);
    }

    #[test]
    fn provider_native_with_request_params_enables_qwen_search() {
        let mut caps = caps();
        caps.server_tools = ServerToolPolicy::ProviderNative;
        caps.search_request_params = Some(json!({
            "enable_search": true,
            "search_options": {"forced_search": true}
        }));
        let body = json!({
            "model": "m",
            "max_tokens": 10,
            "messages": [{"role": "user", "content": "今天的 AI 新闻"}],
            "tools": [
                {"type": "web_search_20260209", "name": "web_search"},
                {"type": "web_fetch_20260209", "name": "web_fetch"}
            ]
        });
        let (out, report) = convert_messages_request(&body, &caps).unwrap();
        // Search is a top-level request parameter, not a tool.
        assert_eq!(out["enable_search"], true);
        assert_eq!(out["search_options"]["forced_search"], true);
        assert!(out.get("tools").is_none());
        assert_eq!(report.mapped_server_tools, vec!["web_search"]);
        assert_eq!(report.dropped_server_tools, vec!["web_fetch"]);
    }

    #[test]
    fn provider_native_without_template_drops_search_tools() {
        let mut caps = caps();
        caps.server_tools = ServerToolPolicy::ProviderNative;
        let body = json!({
            "model": "m",
            "max_tokens": 10,
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [
                {"type": "web_search_20260209", "name": "web_search"},
                {"name": "Read", "description": "read", "input_schema": {"type": "object"}}
            ]
        });
        let (out, report) = convert_messages_request(&body, &caps).unwrap();
        let tools = out["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["function"]["name"], "Read");
        assert_eq!(report.dropped_server_tools, vec!["web_search"]);
    }

    #[test]
    fn maps_server_tools_to_functions_when_configured() {
        let mut caps = caps();
        caps.server_tools = ServerToolPolicy::MapToFunction;
        let body = json!({
            "model": "m",
            "max_tokens": 10,
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [
                {"type": "web_search_20260209", "name": "web_search"},
                {"type": "web_fetch_20260209", "name": "web_fetch"},
                {"type": "code_execution_20260120", "name": "code_execution"}
            ]
        });
        let (out, report) = convert_messages_request(&body, &caps).unwrap();
        let tools = out["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 2);
        assert_eq!(tools[0]["function"]["name"], "web_search");
        assert_eq!(tools[0]["function"]["parameters"]["required"][0], "query");
        assert_eq!(tools[1]["function"]["name"], "web_fetch");
        assert_eq!(report.mapped_server_tools, vec!["web_search", "web_fetch"]);
        assert_eq!(report.dropped_server_tools, vec!["code_execution"]);
    }

    #[test]
    fn converts_tool_choice() {
        let cases = [
            (json!({"type": "auto"}), json!("auto")),
            (json!({"type": "any"}), json!("required")),
            (json!({"type": "none"}), json!("none")),
            (
                json!({"type": "tool", "name": "f"}),
                json!({"type": "function", "function": {"name": "f"}}),
            ),
        ];
        for (choice, expected) in cases {
            let body =
                json!({"model": "m", "max_tokens": 1, "messages": [], "tool_choice": choice});
            let (out, _) = convert_messages_request(&body, &caps()).unwrap();
            assert_eq!(out["tool_choice"], expected);
        }
    }

    #[test]
    fn converts_thinking_to_qwen_enable_thinking() {
        let mut caps = caps();
        caps.thinking_param = ThinkingParam::EnableThinking;
        caps.reasoning_effort = None;
        let body = json!({
            "model": "qwen3.7-plus",
            "max_tokens": 1,
            "thinking": {"type": "adaptive"},
            "messages": []
        });
        let (out, _) = convert_messages_request(&body, &caps).unwrap();
        assert_eq!(out["enable_thinking"], true);
        assert!(out.get("thinking").is_none());

        let body = json!({
            "model": "qwen3.7-plus",
            "max_tokens": 1,
            "thinking": {"type": "disabled"},
            "messages": []
        });
        let (out, _) = convert_messages_request(&body, &caps).unwrap();
        assert_eq!(out["enable_thinking"], false);
    }

    #[test]
    fn converts_effort_and_thinking_for_deepseek() {
        let mut caps = caps();
        caps.reasoning_effort = Some(crate::dialect::ReasoningEffortConfig {
            enabled: true,
            levels: vec!["low".into(), "high".into(), "max".into()],
            default: Some("high".into()),
        });
        let body = json!({
            "model": "deepseek-v4-pro",
            "max_tokens": 1,
            "thinking": {"type": "adaptive"},
            "output_config": {"effort": "xhigh"},
            "messages": []
        });
        let (out, _) = convert_messages_request(&body, &caps).unwrap();
        assert_eq!(out["thinking"]["type"], "enabled");
        assert_eq!(out["reasoning_effort"], "high");
    }

    #[test]
    fn disabled_thinking_maps_for_deepseek() {
        let mut caps = caps();
        caps.reasoning_effort = Some(crate::dialect::ReasoningEffortConfig {
            enabled: false,
            levels: vec!["low".into(), "high".into(), "max".into()],
            default: None,
        });
        let body = json!({
            "model": "deepseek-v4-pro",
            "max_tokens": 1,
            "thinking": {"type": "disabled"},
            "messages": []
        });
        let (out, _) = convert_messages_request(&body, &caps).unwrap();
        assert_eq!(out["thinking"]["type"], "disabled");
        assert!(out.get("reasoning_effort").is_none());
    }

    #[test]
    fn rejects_image_input_when_unsupported() {
        let body = json!({
            "model": "m",
            "max_tokens": 1,
            "messages": [{"role": "user", "content": [{"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "aaa"}}]}]
        });
        let err = convert_messages_request(&body, &caps()).unwrap_err();
        assert!(err.to_string().contains("image"));
    }

    #[test]
    fn converts_image_when_supported() {
        let mut caps = caps();
        caps.image_input = true;
        let body = json!({
            "model": "m",
            "max_tokens": 1,
            "messages": [{"role": "user", "content": [{"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "aaa"}}, {"type": "text", "text": "what is this"}]}]
        });
        let (out, _) = convert_messages_request(&body, &caps).unwrap();
        let content = out["messages"][0]["content"].as_array().unwrap();
        assert_eq!(content[0]["type"], "text");
        assert_eq!(content[0]["text"], "what is this");
        assert_eq!(content[1]["type"], "image_url");
        assert!(content[1]["image_url"]["url"]
            .as_str()
            .unwrap()
            .starts_with("data:image/png;base64,"));
    }

    #[test]
    fn converts_non_streaming_response_with_reasoning_and_tools() {
        let body = json!({
            "id": "ff11019d-aa8d-4251-8ff0-d7644de0987e",
            "model": "deepseek-v4-pro",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello!",
                    "reasoning_content": "thinking text",
                    "tool_calls": [{"id": "call_1", "type": "function", "function": {"name": "get_weather", "arguments": "{\"city\": \"Paris\"}"}}]
                },
                "finish_reason": "tool_calls"
            }],
            "usage": {"prompt_tokens": 10, "completion_tokens": 72, "prompt_tokens_details": {"cached_tokens": 2}, "completion_tokens_details": {"reasoning_tokens": 69}}
        });
        let out = convert_chat_response(&body).unwrap();
        assert_eq!(out["id"], "msg_ff11019d-aa8d-4251-8ff0-d7644de0987e");
        assert_eq!(out["type"], "message");
        assert_eq!(out["stop_reason"], "tool_use");
        assert_eq!(out["content"][0]["type"], "thinking");
        assert_eq!(
            out["content"][0]["signature"],
            "msg_ff11019d-aa8d-4251-8ff0-d7644de0987e"
        );
        assert_eq!(out["content"][1]["type"], "text");
        assert_eq!(out["content"][2]["type"], "tool_use");
        assert_eq!(out["content"][2]["input"]["city"], "Paris");
        assert_eq!(out["usage"]["input_tokens"], 10);
        assert_eq!(out["usage"]["output_tokens"], 72);
        assert_eq!(out["usage"]["cache_read_input_tokens"], 2);
        assert_eq!(out["usage"]["output_tokens_details"]["thinking_tokens"], 69);
    }

    #[test]
    fn downgrades_tool_calls_finish_without_tool_blocks() {
        let body = json!({
            "id": "x",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "done"}, "finish_reason": "tool_calls"}]
        });
        let out = convert_chat_response(&body).unwrap();
        assert_eq!(out["stop_reason"], "end_turn");
    }

    #[test]
    fn stream_converts_deepseek_chunks() {
        let mut converter = ChatStreamConverter::new();
        let mut out = Vec::new();
        converter.on_chunk(r#"{"id":"0e414c94","object":"chat.completion.chunk","model":"deepseek-v4-pro","choices":[{"index":0,"delta":{"role":"assistant","content":null,"reasoning_content":""},"finish_reason":null}],"usage":null}"#, &mut out).unwrap();
        converter.on_chunk(r#"{"id":"0e414c94","choices":[{"index":0,"delta":{"content":null,"reasoning_content":"We"},"finish_reason":null}],"usage":null}"#, &mut out).unwrap();
        converter.on_chunk(r#"{"id":"0e414c94","choices":[{"index":0,"delta":{"content":"Hi","reasoning_content":null},"finish_reason":null}],"usage":null}"#, &mut out).unwrap();
        converter.on_chunk(r#"{"id":"0e414c94","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":null}"#, &mut out).unwrap();
        converter.on_chunk(r#"{"id":"0e414c94","choices":[],"usage":{"prompt_tokens":6,"completion_tokens":9,"total_tokens":15}}"#, &mut out).unwrap();
        converter.on_chunk("[DONE]", &mut out).unwrap();

        let text = String::from_utf8_lossy(&out.concat()).into_owned();
        assert!(text.contains("event: message_start"));
        assert!(text.contains("\"id\":\"msg_0e414c94\""));
        assert!(text.contains("\"type\":\"thinking_delta\""));
        assert!(text.contains("\"thinking\":\"We\""));
        assert!(text.contains("\"type\":\"text_delta\""));
        assert!(text.contains("\"text\":\"Hi\""));
        assert!(text.contains("\"stop_reason\":\"end_turn\""));
        assert!(text.contains("event: message_stop"));
        // exactly one message_stop
        assert_eq!(text.matches("event: message_stop").count(), 1);
    }

    #[test]
    fn stream_converts_tool_calls_accumulated_across_chunks() {
        let mut converter = ChatStreamConverter::new();
        let mut out = Vec::new();
        converter.on_chunk(r#"{"id":"x","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":""}}]},"finish_reason":null}]}"#, &mut out).unwrap();
        converter.on_chunk(r#"{"id":"x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"city\": \"Pa"}}]},"finish_reason":null}]}"#, &mut out).unwrap();
        converter.on_chunk(r#"{"id":"x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"ris\"}"}}]},"finish_reason":null}]}"#, &mut out).unwrap();
        converter.on_chunk(r#"{"id":"x","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}],"usage":null}"#, &mut out).unwrap();
        converter.on_chunk("[DONE]", &mut out).unwrap();

        let text = String::from_utf8_lossy(&out.concat()).into_owned();
        assert!(text.contains("\"type\":\"tool_use\""));
        assert!(text.contains("\"name\":\"get_weather\""));
        assert!(text.contains("\"stop_reason\":\"tool_use\""));
        // partial_json carries increments: each chunk's arguments slice is sent separately,
        // and the client appends them (Anthropic SDK MessageStream does `buf += partial_json`).
        assert!(text.contains("\"partial_json\":\"{\\\"city\\\": \\\"Pa\""));
        assert!(text.contains("\"partial_json\":\"ris\\\"}\""));
        // No cumulative value is ever sent.
        assert!(!text.contains("\"partial_json\":\"{\\\"city\\\": \\\"Paris\\\"}\""));
        assert_eq!(text.matches("event: message_stop").count(), 1);
    }

    #[test]
    fn finish_without_done_emits_terminators() {
        let mut converter = ChatStreamConverter::new();
        let mut out = Vec::new();
        converter
            .on_chunk(
                r#"{"id":"x","choices":[{"index":0,"delta":{"content":"hi"}}]}"#,
                &mut out,
            )
            .unwrap();
        converter.finish(&mut out);
        let text = String::from_utf8_lossy(&out.concat()).into_owned();
        assert!(text.contains("event: message_delta"));
        assert!(text.contains("event: message_stop"));
        assert_eq!(text.matches("event: message_stop").count(), 1);
    }

    #[test]
    fn finish_is_idempotent() {
        let mut converter = ChatStreamConverter::new();
        let mut out = Vec::new();
        converter.finish(&mut out);
        converter.finish(&mut out);
        assert!(out.is_empty());
    }
}
