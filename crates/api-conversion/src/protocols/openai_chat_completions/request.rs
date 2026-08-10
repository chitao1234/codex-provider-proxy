use serde_json::{Map, Value};

use crate::{
    dialect::{
        MaxTokensField, ModelCapabilities, RequestConversionReport, ResponseFormatCap,
        ThinkingParam,
    },
    error::ConversionError,
    ir::{ContentItem, ContentPart, GenerationControls, RequestIr, ToolCall},
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
    let mut output: Map<String, Value> = request
        .opaque
        .iter()
        .filter(|extension| {
            extension.scope == crate::ir::ExtensionScope::Request
                && extension.is_replayable_as_opaque()
        })
        .map(|extension| (extension.key.clone(), extension.value.clone()))
        .collect();
    if let Some(model) = &request.model {
        output.insert("model".to_owned(), Value::String(model.clone()));
    }
    if let Some(stream) = request.stream {
        output.insert("stream".to_owned(), Value::Bool(stream));
    }
    crate::protocols::request_support::render_openai_state(
        request,
        capabilities,
        false,
        &mut output,
    );
    render_generation(
        &request.generation,
        request.stream,
        capabilities,
        &mut output,
    )?;

    let mut messages = Vec::new();
    for instruction in &request.instructions {
        if let Some(text) = content_item_text(instruction) {
            messages.push(request_support::replay_item_extensions(
                serde_json::json!({
                    "role": "system",
                    "content": [{"type": "text", "text": text}],
                }),
                instruction,
            ));
        }
    }
    let mut pending_reasoning = None;
    for item in &request.transcript {
        match item {
            ContentItem::Message { role, content, .. } => {
                if role == "system" || role == "developer" {
                    let text = content
                        .iter()
                        .filter_map(text_part)
                        .collect::<Vec<_>>()
                        .join("\n\n");
                    if !text.is_empty() {
                        let role = if role == "developer" && !capabilities.accept_developer_role {
                            "system"
                        } else {
                            role
                        };
                        messages.push(request_support::replay_item_extensions(
                            serde_json::json!({"role": role, "content": [{"type": "text", "text": text}]}),
                            item,
                        ));
                    }
                    continue;
                }
                let mut message = serde_json::json!({
                    "role": role,
                    "content": content.iter().map(|p| render_content_part(p, capabilities)).collect::<Vec<_>>(),
                });
                if role == "assistant" {
                    apply_pending_reasoning(&mut message, &mut pending_reasoning);
                }
                messages.push(request_support::replay_item_extensions(message, item));
            }
            ContentItem::Reasoning { content, .. } => {
                pending_reasoning = Some((
                    content
                        .iter()
                        .filter_map(text_part)
                        .collect::<Vec<_>>()
                        .join("\n"),
                    item.extensions().to_vec(),
                ));
            }
            ContentItem::Refusal { content, .. } => messages.push(
                request_support::replay_item_extensions(
                    serde_json::json!({
                        "role": "assistant",
                        "content": content.iter().map(|p| render_content_part(p, capabilities)).collect::<Vec<_>>(),
                    }),
                    item,
                ),
            ),
            ContentItem::ToolUse { id, call } => {
                let tool_call = render_tool_call(id.as_deref(), call);
                if let Some(message) = messages.last_mut().filter(|message| {
                    message.get("role").and_then(Value::as_str) == Some("assistant")
                }) {
                    apply_pending_reasoning(message, &mut pending_reasoning);
                    if let Some(calls) = message["tool_calls"].as_array_mut() {
                        calls.push(tool_call);
                    } else {
                        message["tool_calls"] = Value::Array(vec![tool_call]);
                    }
                } else {
                    let mut message = serde_json::json!({
                        "role": "assistant",
                        "content": null,
                        "tool_calls": [tool_call],
                    });
                    apply_pending_reasoning(&mut message, &mut pending_reasoning);
                    messages.push(message);
                }
            }
            ContentItem::ToolResult { result } => messages.push(
                request_support::replay_item_extensions(
                    serde_json::json!({
                        "role": "tool",
                        "tool_call_id": result.call_id,
                        // The protocol accepts string or array; some compatible
                        // upstreams (MiMo) reject the array form with 400.
                        "content": if capabilities.tool_content_string {
                            Value::String(request_support::content_as_string(&result.content))
                        } else {
                            Value::Array(result.content.iter().map(|p| render_content_part(p, capabilities)).collect::<Vec<_>>())
                        },
                    }),
                    item,
                ),
            ),
            ContentItem::ServerTool { .. } | ContentItem::Unknown { .. } => {}
        }
    }
    if pending_reasoning.is_some() {
        let mut message = serde_json::json!({"role": "assistant", "content": ""});
        apply_pending_reasoning(&mut message, &mut pending_reasoning);
        messages.push(message);
    }
    output.insert("messages".to_owned(), Value::Array(messages));
    let last_user_text = request.last_user_text();
    let (tools, mut report) = provider_tools::render(
        &request.tools,
        ToolTarget::OpenAiChatCompletions,
        capabilities,
        render_tool,
        render_mapped_tool,
        last_user_text.as_deref(),
    );
    if !tools.is_empty() {
        output.insert("tools".to_owned(), Value::Array(tools));
    }
    if let Some(choice) = &request.tool_choice {
        output.insert("tool_choice".to_owned(), choice.clone());
    }
    provider_tools::merge_native_params(
        &mut output,
        capabilities,
        &mut report,
        last_user_text.as_deref(),
    );
    Ok((Value::Object(output), report))
}

fn render_generation(
    generation: &GenerationControls,
    stream: Option<bool>,
    capabilities: &ModelCapabilities,
    output: &mut Map<String, Value>,
) -> Result<(), ConversionError> {
    for (key, value) in [
        ("temperature", generation.temperature.as_ref()),
        ("top_p", generation.top_p.as_ref()),
    ] {
        if let Some(value) = value {
            output.insert(key.to_owned(), value.clone());
        }
    }
    if let Some(tokens) = &generation.max_output_tokens {
        let value = tokens.as_u64().map_or(tokens.clone(), |tokens| {
            Value::from(
                capabilities
                    .max_tokens_cap
                    .map_or(tokens, |cap| tokens.min(cap)),
            )
        });
        let field = match capabilities.max_tokens_field {
            MaxTokensField::MaxTokens => "max_tokens",
            MaxTokensField::MaxCompletionTokens => "max_completion_tokens",
        };
        output.insert(field.to_owned(), value);
    }
    if capabilities.accept_stop {
        let stop = generation.stop_values();
        if !stop.is_empty() {
            output.insert(
                "stop".to_owned(),
                if stop.len() == 1 {
                    stop[0].clone()
                } else {
                    Value::Array(stop)
                },
            );
        }
    }
    if stream == Some(true) && capabilities.stream_include_usage {
        output.insert(
            "stream_options".to_owned(),
            serde_json::json!({"include_usage": true}),
        );
    }
    if let Some(split) = capabilities.reasoning_split {
        output.insert("reasoning_split".to_owned(), Value::Bool(split));
    }
    let effort = generation.effort();
    let thinking_type = generation.thinking_type();
    match capabilities.thinking_param {
        ThinkingParam::TopLevel | ThinkingParam::Adaptive => {
            if generation.thinking.is_some() || effort.is_some() {
                let enabled = thinking_type != Some("disabled")
                    && effort.is_none_or(|effort| effort != "none" && effort != "minimal");
                let enabled_type = if capabilities.thinking_param == ThinkingParam::Adaptive {
                    "adaptive"
                } else {
                    "enabled"
                };
                output.insert(
                    "thinking".to_owned(),
                    serde_json::json!({"type": if enabled { enabled_type } else { "disabled" }}),
                );
            }
        }
        ThinkingParam::EnableThinking => {
            if generation.thinking.is_some() || effort.is_some() {
                let enabled = thinking_type != Some("disabled")
                    && effort.is_none_or(|effort| effort != "none" && effort != "minimal");
                output.insert("enable_thinking".to_owned(), Value::Bool(enabled));
            }
        }
        ThinkingParam::None => {}
    }
    if let Some(config) = capabilities
        .reasoning_effort
        .as_ref()
        .filter(|config| config.enabled)
    {
        if let Some(level) = effort
            .or(config.default.as_deref())
            .and_then(|effort| capabilities.clamp_effort(effort))
        {
            output.insert("reasoning_effort".to_owned(), Value::String(level));
        }
    }
    if let Some(format) = generation.output_format() {
        render_response_format(format, capabilities, output)?;
    }
    if capabilities.parallel_tool_calls {
        if let Some(parallel) = generation
            .parallel_tool_calls
            .as_ref()
            .or(generation.parallel_tool_use.as_ref())
        {
            output.insert("parallel_tool_calls".to_owned(), parallel.clone());
        }
    }
    Ok(())
}

fn render_response_format(
    format: &Value,
    capabilities: &ModelCapabilities,
    output: &mut Map<String, Value>,
) -> Result<(), ConversionError> {
    match format.get("type").and_then(Value::as_str).unwrap_or("text") {
        "text" => Ok(()),
        "json_object" => {
            if capabilities.response_format == ResponseFormatCap::Text {
                return Err(ConversionError::unsupported(
                    "output_config.format",
                    "upstream does not support structured output formats",
                ));
            }
            output.insert(
                "response_format".to_owned(),
                serde_json::json!({"type": "json_object"}),
            );
            Ok(())
        }
        "json_schema" => match capabilities.response_format {
            ResponseFormatCap::JsonSchema => {
                output.insert(
                    "response_format".to_owned(),
                    serde_json::json!({
                        "type": "json_schema",
                        "json_schema": {
                            "name": format.get("name").and_then(Value::as_str).unwrap_or("output"),
                            "description": format.get("description"),
                            "schema": format.get("schema").cloned().unwrap_or_else(|| serde_json::json!({})),
                            "strict": format.get("strict").cloned().unwrap_or(Value::Bool(false)),
                        }
                    }),
                );
                Ok(())
            }
            ResponseFormatCap::JsonObject => {
                output.insert(
                    "response_format".to_owned(),
                    serde_json::json!({"type": "json_object"}),
                );
                Ok(())
            }
            ResponseFormatCap::Text => Err(ConversionError::unsupported(
                "output_config.format",
                "upstream does not support structured output formats",
            )),
        },
        _ => Err(ConversionError::unsupported(
            "output_config.format",
            "unsupported structured output format",
        )),
    }
}

fn render_content_part(part: &ContentPart, capabilities: &ModelCapabilities) -> Value {
    let rendered = match part.semantic() {
        ContentPart::Text(text) => serde_json::json!({"type": "text", "text": text}),
        ContentPart::Refusal(text) => serde_json::json!({"type": "text", "text": text}),
        ContentPart::Reasoning { text, .. } => serde_json::json!({"type": "text", "text": text}),
        ContentPart::Image { url, data, .. } => url.as_ref().map_or_else(
            || serde_json::json!({"type": "image_url", "image_url": {"url": data}}),
            |url| serde_json::json!({"type": "image_url", "image_url": {"url": url}}),
        ),
        ContentPart::Json(value) => value.clone(),
        ContentPart::Extended { .. } => unreachable!("semantic content is never extended"),
    };
    request_support::replay_content_extensions(rendered, part, capabilities.cache.block_cache_control)
}

fn render_tool_call(id: Option<&str>, call: &ToolCall) -> Value {
    let mut value = serde_json::json!({
        "id": call.id.as_deref().or(id),
        "type": "function",
        "function": {"name": call.name, "arguments": call.arguments}
    });
    if let Some(object) = value.as_object_mut() {
        request_support::replay_scoped_extensions(
            object,
            &call.extensions,
            &[crate::ir::ExtensionScope::ToolCall],
        );
        if let Some(function) = object.get_mut("function").and_then(Value::as_object_mut) {
            for (key, value) in call.metadata.iter().chain(&call.function_metadata) {
                function.entry(key.clone()).or_insert_with(|| value.clone());
            }
            request_support::replay_scoped_extensions(
                function,
                &call.extensions,
                &[crate::ir::ExtensionScope::ToolFunction],
            );
        }
    }
    value
}

fn render_tool(tool: &crate::ir::ToolDefinition) -> Value {
    let mut value = serde_json::json!({"type": "function", "function": {"name": tool.name, "description": tool.description, "parameters": tool.input_schema.clone().unwrap_or_else(|| serde_json::json!({}))}});
    if let Some(object) = value.as_object_mut() {
        request_support::replay_scoped_extensions(
            object,
            &tool.extensions,
            &[crate::ir::ExtensionScope::ToolDefinition],
        );
        if let Some(function) = object.get_mut("function").and_then(Value::as_object_mut) {
            for (key, value) in tool.metadata.iter().chain(&tool.function_metadata) {
                function.entry(key.clone()).or_insert_with(|| value.clone());
            }
            request_support::replay_scoped_extensions(
                function,
                &tool.extensions,
                &[crate::ir::ExtensionScope::ToolFunction],
            );
        }
    }
    value
}

fn apply_pending_reasoning(
    message: &mut Value,
    pending: &mut Option<(String, Vec<crate::ir::Extension>)>,
) {
    let Some((reasoning, extensions)) = pending.take() else {
        return;
    };
    message["reasoning_content"] = Value::String(reasoning);
    if let Some(object) = message.as_object_mut() {
        request_support::replay_scoped_extensions(
            object,
            &extensions,
            &[crate::ir::ExtensionScope::ContentItem],
        );
    }
}

fn render_mapped_tool(_tool: &crate::ir::ToolDefinition, mapped: &MappedFunctionTool) -> Value {
    serde_json::json!({
        "type": "function",
        "function": {
            "name": mapped.name,
            "description": mapped.description,
            "parameters": mapped.parameters,
        }
    })
}

fn text_part(part: &ContentPart) -> Option<String> {
    match part.semantic() {
        ContentPart::Text(text) | ContentPart::Refusal(text) => Some(text.clone()),
        ContentPart::Reasoning { text, .. } => Some(text.clone()),
        ContentPart::Image { .. } | ContentPart::Json(_) => None,
        ContentPart::Extended { .. } => unreachable!("semantic content is never extended"),
    }
}

fn content_item_text(item: &ContentItem) -> Option<String> {
    match item {
        ContentItem::Message { content, .. } | ContentItem::Refusal { content, .. } => Some(
            content
                .iter()
                .filter_map(text_part)
                .collect::<Vec<_>>()
                .join("\n"),
        ),
        _ => None,
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
