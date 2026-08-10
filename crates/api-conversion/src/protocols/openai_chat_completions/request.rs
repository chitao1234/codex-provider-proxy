use serde_json::{Map, Value};

use crate::{
    dialect::{ModelCapabilities, RequestConversionReport},
    error::ConversionError,
    ir::{ContentItem, ContentPart, GenerationControls, RequestIr, ToolCall},
};

pub fn render(
    request: &RequestIr,
    capabilities: &ModelCapabilities,
) -> Result<(Value, RequestConversionReport), ConversionError> {
    let mut output: Map<String, Value> = request
        .opaque
        .iter()
        .filter(|extension| extension.scope == crate::ir::ExtensionScope::Request)
        .map(|extension| (extension.key.clone(), extension.value.clone()))
        .collect();
    if let Some(model) = &request.model {
        output.insert("model".to_owned(), Value::String(model.clone()));
    }
    if let Some(stream) = request.stream {
        output.insert("stream".to_owned(), Value::Bool(stream));
    }
    if let Some(user_id) = request.state.cache.key.as_ref().or(request
        .state
        .identity
        .user_id
        .as_ref())
    {
        output.insert("user".to_owned(), Value::String(user_id.clone()));
    }
    if let Some(metadata) = &request.state.metadata {
        output.insert("metadata".to_owned(), metadata.clone());
    }
    render_generation(&request.generation, capabilities, &mut output);

    let mut messages = Vec::new();
    for instruction in &request.instructions {
        if let Some(text) = content_item_text(instruction) {
            messages.push(serde_json::json!({
                "role": "system",
                "content": [{"type": "text", "text": text}],
            }));
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
                        messages.push(serde_json::json!({"role": role, "content": [{"type": "text", "text": text}]}));
                    }
                    continue;
                }
                let mut message = serde_json::json!({
                    "role": role,
                    "content": content.iter().map(render_content_part).collect::<Vec<_>>(),
                });
                if role == "assistant" {
                    if let Some(reasoning) = pending_reasoning.take() {
                        message["reasoning_content"] = Value::String(reasoning);
                    }
                }
                messages.push(message);
            }
            ContentItem::Reasoning { content, .. } => {
                pending_reasoning = Some(
                    content
                        .iter()
                        .filter_map(text_part)
                        .collect::<Vec<_>>()
                        .join("\n"),
                );
            }
            ContentItem::Refusal { content, .. } => messages.push(serde_json::json!({
                "role": "assistant",
                "content": content.iter().map(render_content_part).collect::<Vec<_>>(),
            })),
            ContentItem::ToolUse { id, call } => {
                messages.push(render_tool_message(id.as_deref(), call))
            }
            ContentItem::ToolResult { result } => messages.push(serde_json::json!({
                "role": "tool",
                "tool_call_id": result.call_id,
                "content": result.content.iter().map(render_content_part).collect::<Vec<_>>(),
            })),
            ContentItem::ServerTool { .. } | ContentItem::Unknown { .. } => {}
        }
    }
    if let Some(reasoning) = pending_reasoning {
        messages.push(
            serde_json::json!({"role": "assistant", "content": "", "reasoning_content": reasoning}),
        );
    }
    output.insert("messages".to_owned(), Value::Array(messages));
    if !request.tools.is_empty() {
        output.insert(
            "tools".to_owned(),
            Value::Array(request.tools.iter().map(render_tool).collect()),
        );
    }
    if let Some(choice) = &request.tool_choice {
        output.insert("tool_choice".to_owned(), choice.clone());
    }
    Ok((Value::Object(output), RequestConversionReport::default()))
}

fn render_generation(
    generation: &GenerationControls,
    capabilities: &ModelCapabilities,
    output: &mut Map<String, Value>,
) {
    if let Some(tokens) = &generation.max_output_tokens {
        let value = tokens.as_u64().map_or(tokens.clone(), |tokens| {
            Value::from(
                capabilities
                    .max_tokens_cap
                    .map_or(tokens, |cap| tokens.min(cap)),
            )
        });
        output.insert("max_tokens".to_owned(), value);
    }
    if let Some(stop) = generation
        .stop_sequences
        .as_ref()
        .or(generation.stop.as_ref())
    {
        output.insert("stop".to_owned(), stop.clone());
    }
    if let Some(reasoning) = &generation.reasoning_effort {
        output.insert("reasoning_effort".to_owned(), reasoning.clone());
    }
    if let Some(config) = &generation.output_config {
        if let Some(effort) = config.get("effort") {
            output.insert("reasoning_effort".to_owned(), effort.clone());
        }
        if let Some(format) = config.get("format") {
            output.insert("response_format".to_owned(), format.clone());
        }
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
}

fn render_content_part(part: &ContentPart) -> Value {
    match part {
        ContentPart::Text(text) => serde_json::json!({"type": "text", "text": text}),
        ContentPart::Refusal(text) => serde_json::json!({"type": "text", "text": text}),
        ContentPart::Reasoning { text, .. } => serde_json::json!({"type": "text", "text": text}),
        ContentPart::Image { url, data, .. } => url.as_ref().map_or_else(
            || serde_json::json!({"type": "image_url", "image_url": {"url": data}}),
            |url| serde_json::json!({"type": "image_url", "image_url": {"url": url}}),
        ),
        ContentPart::Json(value) => value.clone(),
    }
}

fn render_tool_message(id: Option<&str>, call: &ToolCall) -> Value {
    serde_json::json!({
        "role": "assistant",
        "content": null,
        "tool_calls": [{
            "id": call.id.as_deref().or(id),
            "type": "function",
            "function": {"name": call.name, "arguments": call.arguments}
        }]
    })
}

fn render_tool(tool: &crate::ir::ToolDefinition) -> Value {
    serde_json::json!({"type": "function", "function": {"name": tool.name, "description": tool.description, "parameters": tool.input_schema.clone().unwrap_or_else(|| serde_json::json!({}))}})
}

fn text_part(part: &ContentPart) -> Option<String> {
    match part {
        ContentPart::Text(text) | ContentPart::Refusal(text) => Some(text.clone()),
        _ => None,
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
