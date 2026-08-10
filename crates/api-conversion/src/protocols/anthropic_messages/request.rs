use serde_json::{Map, Value};

use crate::{
    dialect::{ModelCapabilities, RequestConversionReport, ThinkingParam},
    error::ConversionError,
    ir::{ContentItem, ContentPart, GenerationControls, RequestIr},
    protocols::request_support,
    provider_tools::{self, MappedFunctionTool, ToolTarget},
};

pub fn render(
    request: &RequestIr,
    capabilities: &ModelCapabilities,
) -> Result<(Value, RequestConversionReport), ConversionError> {
    if !capabilities.image_input && request.transcript.iter().any(item_has_image) {
        return Err(ConversionError::unsupported(
            "messages[].content.image",
            "upstream does not support image input",
        ));
    }
    let mut output = opaque_root(request);
    if let Some(model) = &request.model {
        output.insert("model".to_owned(), Value::String(model.clone()));
    }
    if let Some(stream) = request.stream {
        output.insert("stream".to_owned(), Value::Bool(stream));
    }
    crate::protocols::request_support::render_messages_state(request, capabilities, &mut output);
    render_generation(&request.generation, capabilities, &mut output);

    let mut system_parts = request
        .instructions
        .iter()
        .flat_map(content_item_text)
        .collect::<Vec<_>>();
    let mut messages = Vec::new();
    for item in &request.transcript {
        match item {
            ContentItem::Message { role, content, .. }
                if role == "system" || role == "developer" =>
            {
                system_parts.extend(content.iter().filter_map(text_part));
            }
            ContentItem::Message { role, content, .. } => {
                messages.push(request_support::replay_item_extensions(
                    serde_json::json!({
                        "role": normalize_role(role),
                        "content": content.iter().map(|p| render_content_part(p, capabilities)).collect::<Vec<_>>(),
                    }),
                    item,
                ));
            }
            ContentItem::Reasoning { content, .. } => {
                messages.push(request_support::replay_item_extensions(
                    serde_json::json!({
                        "role": "assistant",
                        "content": content.iter().map(|p| render_thinking_part(p, capabilities)).collect::<Vec<_>>(),
                    }),
                    item,
                ))
            }
            ContentItem::Refusal { content, .. } => {
                messages.push(request_support::replay_item_extensions(
                    serde_json::json!({
                        "role": "assistant",
                        "content": content.iter().map(|p| render_content_part(p, capabilities)).collect::<Vec<_>>(),
                    }),
                    item,
                ))
            }
            ContentItem::ToolUse { id, call } => messages.push(serde_json::json!({
                "role": "assistant",
                "content": [render_tool_use(id.as_deref(), call)],
            })),
            ContentItem::ToolResult { result } => messages.push(serde_json::json!({
                "role": "user",
                "content": [render_tool_result(result, capabilities)],
            })),
            ContentItem::ServerTool { payload, .. } | ContentItem::Unknown { payload, .. } => {
                if payload.is_object() {
                    messages.push(serde_json::json!({"role": "assistant", "content": [payload]}));
                }
            }
        }
    }
    if !system_parts.is_empty() {
        output.insert(
            "system".to_owned(),
            Value::String(system_parts.join("\n\n")),
        );
    }
    output.insert("messages".to_owned(), Value::Array(messages));
    let (tools, report) = provider_tools::render(
        &request.tools,
        ToolTarget::AnthropicMessages,
        capabilities,
        render_tool,
        render_mapped_tool,
        request.last_user_text().as_deref(),
    );
    if !tools.is_empty() {
        output.insert("tools".to_owned(), Value::Array(tools));
    }
    if let Some(choice) = &request.tool_choice {
        output.insert("tool_choice".to_owned(), render_tool_choice(choice));
    }
    Ok((Value::Object(output), report))
}

fn opaque_root(request: &RequestIr) -> Map<String, Value> {
    request
        .opaque
        .iter()
        .filter(|extension| {
            extension.scope == crate::ir::ExtensionScope::Request
                && extension.is_replayable_as_opaque()
        })
        .map(|extension| (extension.key.clone(), extension.value.clone()))
        .collect()
}

fn render_generation(
    generation: &GenerationControls,
    capabilities: &ModelCapabilities,
    output: &mut Map<String, Value>,
) {
    for (key, value) in [
        ("temperature", generation.temperature.as_ref()),
        ("top_p", generation.top_p.as_ref()),
    ] {
        if let Some(value) = value {
            output.insert(key.to_owned(), value.clone());
        }
    }
    if let Some(tokens) = generation.max_output_tokens.as_ref() {
        let value = tokens.as_u64().map_or(tokens.clone(), |tokens| {
            Value::from(
                capabilities
                    .max_tokens_cap
                    .map_or(tokens, |cap| tokens.min(cap)),
            )
        });
        output.insert("max_tokens".to_owned(), value);
    } else if let Some(default) = capabilities.default_max_tokens {
        output.insert(
            "max_tokens".to_owned(),
            Value::from(
                capabilities
                    .max_tokens_cap
                    .map_or(default, |cap| default.min(cap)),
            ),
        );
    }
    if let Some(config) = &generation.output_config {
        output.insert("output_config".to_owned(), config.clone());
    }
    let effort = generation.effort();
    let thinking_type = generation.thinking_type();
    match capabilities.thinking_param {
        ThinkingParam::TopLevel | ThinkingParam::Adaptive => {
            if generation.thinking.is_some() || effort.is_some() {
                let enabled = thinking_type != Some("disabled")
                    && effort.is_none_or(|value| value != "none" && value != "minimal");
                let enabled_type = if capabilities.thinking_param == ThinkingParam::Adaptive {
                    "adaptive"
                } else {
                    "enabled"
                };
                let mut thinking = generation
                    .thinking
                    .as_ref()
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default();
                thinking.insert(
                    "type".to_owned(),
                    Value::String(if enabled { enabled_type } else { "disabled" }.to_owned()),
                );
                output.insert("thinking".to_owned(), Value::Object(thinking));
            }
        }
        ThinkingParam::EnableThinking => {
            if generation.thinking.is_some() || effort.is_some() {
                let enabled = thinking_type != Some("disabled")
                    && effort.is_none_or(|value| value != "none" && value != "minimal");
                output.insert("enable_thinking".to_owned(), Value::Bool(enabled));
            }
        }
        ThinkingParam::None => {}
    }
    if capabilities.accept_stop {
        let values = generation.stop_values();
        if !values.is_empty() {
            output.insert("stop_sequences".to_owned(), Value::Array(values));
        }
    }
    if let Some(parallel) = generation
        .parallel_tool_use
        .as_ref()
        .or(generation.parallel_tool_calls.as_ref())
    {
        output.insert("parallel_tool_use".to_owned(), parallel.clone());
    }
}

fn render_content_part(part: &ContentPart, capabilities: &ModelCapabilities) -> Value {
    let rendered = match part.semantic() {
        ContentPart::Text(text) => serde_json::json!({"type": "text", "text": text}),
        ContentPart::Refusal(text) => serde_json::json!({"type": "text", "text": text}),
        ContentPart::Reasoning { text, signature } => {
            let mut value = serde_json::json!({"type": "thinking", "thinking": text});
            if let Some(signature) = signature {
                value["signature"] = Value::String(signature.clone());
            }
            value
        }
        ContentPart::Image {
            media_type,
            data,
            url,
        } => {
            if let Some(url) = url {
                serde_json::json!({"type": "image", "source": {"type": "url", "url": url}})
            } else {
                serde_json::json!({"type": "image", "source": {"type": "base64", "media_type": media_type, "data": data}})
            }
        }
        ContentPart::Json(value) => value.clone(),
        ContentPart::Extended { .. } => unreachable!("semantic content is never extended"),
    };
    request_support::replay_content_extensions(rendered, part, capabilities.cache.block_cache_control)
}

fn render_thinking_part(part: &ContentPart, capabilities: &ModelCapabilities) -> Value {
    match part.semantic() {
        // A Responses reasoning item (`{"type":"reasoning","summary":[{"type":
        // "summary_text","text":...}]}`) becomes an Anthropic thinking block; the
        // Messages dialect has no `reasoning` content type (MiMo rejects it).
        ContentPart::Json(value) => {
            let is_reasoning_item = value.get("type").and_then(Value::as_str) == Some("reasoning");
            if !is_reasoning_item {
                return value.clone();
            }
            let text = value
                .get("summary")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|item| item.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n");
            if text.is_empty() {
                return Value::Null;
            }
            serde_json::json!({"type": "thinking", "thinking": text})
        }
        _ => render_content_part(part, capabilities),
    }
}

fn render_tool_use(id: Option<&str>, call: &crate::ir::ToolCall) -> Value {
    let arguments = serde_json::from_str::<Value>(&call.arguments)
        .unwrap_or_else(|_| Value::String(call.arguments.clone()));
    let mut value = serde_json::json!({
        "type": "tool_use",
        "id": call.id.as_deref().or(id).unwrap_or_default(),
        "name": call.name,
        "input": arguments,
    });
    if let Some(status) = &call.status {
        value["status"] = Value::String(status.clone());
    }
    if let Some(object) = value.as_object_mut() {
        for (key, value) in call.metadata.iter().chain(&call.function_metadata) {
            object.entry(key.clone()).or_insert_with(|| value.clone());
        }
        request_support::replay_scoped_extensions(
            object,
            &call.extensions,
            &[
                crate::ir::ExtensionScope::ToolCall,
                crate::ir::ExtensionScope::ToolFunction,
            ],
        );
    }
    value
}

fn render_tool_result(result: &crate::ir::ToolResult, capabilities: &ModelCapabilities) -> Value {
    let mut value = serde_json::json!({
        "type": "tool_result",
        "tool_use_id": result.call_id,
        "content": result.content.iter().map(|p| render_content_part(p, capabilities)).collect::<Vec<_>>(),
        "is_error": result.is_error,
    });
    if let Some(object) = value.as_object_mut() {
        request_support::replay_scoped_extensions(
            object,
            &result.extensions,
            &[crate::ir::ExtensionScope::ContentItem],
        );
    }
    value
}

fn render_tool(tool: &crate::ir::ToolDefinition) -> Value {
    let mut value = serde_json::json!({
        "name": tool.name,
        "input_schema": tool.input_schema.clone().unwrap_or_else(|| serde_json::json!({})),
    });
    if let Some(description) = &tool.description {
        value["description"] = Value::String(description.clone());
    }
    if let Some(object) = value.as_object_mut() {
        for (key, value) in tool.metadata.iter().chain(&tool.function_metadata) {
            object.entry(key.clone()).or_insert_with(|| value.clone());
        }
        request_support::replay_scoped_extensions(
            object,
            &tool.extensions,
            &[
                crate::ir::ExtensionScope::ToolDefinition,
                crate::ir::ExtensionScope::ToolFunction,
            ],
        );
    }
    value
}

fn render_mapped_tool(_tool: &crate::ir::ToolDefinition, mapped: &MappedFunctionTool) -> Value {
    serde_json::json!({
        "name": mapped.name,
        "description": mapped.description,
        "input_schema": mapped.parameters,
    })
}

fn render_tool_choice(choice: &Value) -> Value {
    match choice.get("type").and_then(Value::as_str) {
        Some("auto") => serde_json::json!({"type": "auto"}),
        Some("none") => serde_json::json!({"type": "none"}),
        Some("any") | Some("required") => serde_json::json!({"type": "any"}),
        Some("tool") | Some("function") => choice
            .get("name")
            .map(|name| serde_json::json!({"type": "tool", "name": name}))
            .unwrap_or_else(|| serde_json::json!({"type": "auto"})),
        _ => choice.clone(),
    }
}

fn normalize_role(role: &str) -> &str {
    match role {
        "assistant" => "assistant",
        _ => "user",
    }
}

fn text_part(part: &ContentPart) -> Option<String> {
    match part.semantic() {
        ContentPart::Text(text) | ContentPart::Refusal(text) => Some(text.clone()),
        _ => None,
    }
}

fn content_item_text(item: &ContentItem) -> Vec<String> {
    match item {
        ContentItem::Message { content, .. }
        | ContentItem::Refusal { content, .. }
        | ContentItem::Reasoning { content, .. } => content.iter().filter_map(text_part).collect(),
        _ => Vec::new(),
    }
}

fn item_has_image(item: &ContentItem) -> bool {
    match item {
        ContentItem::Message { content, .. }
        | ContentItem::Refusal { content, .. }
        | ContentItem::Reasoning { content, .. }
        | ContentItem::ToolResult {
            result: crate::ir::ToolResult { content, .. },
        } => content.iter().any(part_has_image),
        _ => false,
    }
}

fn part_has_image(part: &ContentPart) -> bool {
    match part.semantic() {
        ContentPart::Image { .. } => true,
        ContentPart::Json(value) => value
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|kind| kind.contains("image")),
        _ => false,
    }
}
