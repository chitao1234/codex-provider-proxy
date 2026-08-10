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
    let mut output = opaque_root(request);
    insert_string(&mut output, "model", request.model.as_deref());
    if let Some(stream) = request.stream {
        output.insert("stream".to_owned(), Value::Bool(stream));
    }
    render_state(request, capabilities, &mut output);
    render_generation(&request.generation, capabilities, &mut output);
    if !request.instructions.is_empty() {
        output.insert(
            "instructions".to_owned(),
            Value::String(render_instructions(&request.instructions)),
        );
    }
    output.insert(
        "input".to_owned(),
        Value::Array(
            request
                .transcript
                .iter()
                .flat_map(render_input_item)
                .collect(),
        ),
    );
    if !request.tools.is_empty() {
        output.insert(
            "tools".to_owned(),
            Value::Array(request.tools.iter().map(render_tool).collect::<Vec<_>>()),
        );
    }
    if let Some(tool_choice) = &request.tool_choice {
        output.insert("tool_choice".to_owned(), tool_choice.clone());
    }
    let report = RequestConversionReport::default();
    Ok((Value::Object(output), report))
}

fn opaque_root(request: &RequestIr) -> Map<String, Value> {
    request
        .opaque
        .iter()
        .filter(|extension| extension.scope == crate::ir::ExtensionScope::Request)
        .map(|extension| (extension.key.clone(), extension.value.clone()))
        .collect()
}

fn render_state(
    request: &RequestIr,
    capabilities: &ModelCapabilities,
    output: &mut Map<String, Value>,
) {
    let state = &request.state;
    if let Some(conversation) = &state.conversation {
        output.insert("conversation".to_owned(), conversation.clone());
    }
    if let Some(response_id) = &state.source_response_id {
        output.insert(
            "previous_response_id".to_owned(),
            Value::String(response_id.clone()),
        );
    }
    if let Some(metadata) = &state.metadata {
        output.insert("metadata".to_owned(), metadata.clone());
    }
    if let Some(user_id) = &state.identity.user_id {
        output.insert("user".to_owned(), Value::String(user_id.clone()));
    }
    if let Some(safety_identifier) = &state.safety_identifier {
        output.insert(
            "safety_identifier".to_owned(),
            Value::String(safety_identifier.clone()),
        );
    }
    if let Some(store) = &state.store {
        output.insert("store".to_owned(), store.clone());
    }
    if let Some(include) = &state.include {
        output.insert("include".to_owned(), include.clone());
    }
    if let Some(truncation) = &state.truncation {
        output.insert("truncation".to_owned(), truncation.clone());
    }
    if capabilities.cache.prompt_cache_key {
        if let Some(key) = &state.cache.key {
            output.insert("prompt_cache_key".to_owned(), Value::String(key.clone()));
        }
        if let Some(options) = &state.cache.options {
            output.insert("prompt_cache_options".to_owned(), options.clone());
        }
        if let Some(retention) = &state.cache.retention {
            output.insert("prompt_cache_retention".to_owned(), retention.clone());
        }
    }
}

fn render_generation(
    generation: &GenerationControls,
    capabilities: &ModelCapabilities,
    output: &mut Map<String, Value>,
) {
    for (key, value) in [
        ("reasoning", generation.reasoning.as_ref()),
        ("text", generation.text.as_ref()),
        ("stop", generation.stop.as_ref()),
    ] {
        if let Some(value) = value {
            output.insert(key.to_owned(), value.clone());
        }
    }
    if let Some(parallel) = generation
        .parallel_tool_calls
        .as_ref()
        .or(generation.parallel_tool_use.as_ref())
        .and_then(Value::as_bool)
    {
        output.insert("parallel_tool_calls".to_owned(), Value::Bool(parallel));
    }
    if let Some(mut tokens) = generation.max_output_tokens.clone() {
        if let Some(cap) = capabilities.max_tokens_cap {
            if let Some(number) = tokens.as_u64() {
                tokens = Value::from(number.min(cap));
            }
        }
        output.insert("max_output_tokens".to_owned(), tokens);
    }
    if let Some(effort) = &generation.reasoning_effort {
        output.insert(
            "reasoning".to_owned(),
            serde_json::json!({"effort": effort}),
        );
    }
    if let Some(config) = &generation.output_config {
        if let Some(effort) = config.get("effort") {
            output.insert(
                "reasoning".to_owned(),
                serde_json::json!({"effort": effort}),
            );
        }
        if let Some(format) = config.get("format") {
            output
                .entry("text".to_owned())
                .or_insert_with(|| Value::Object(Map::new()));
            if let Some(text) = output.get_mut("text").and_then(Value::as_object_mut) {
                text.insert("format".to_owned(), format.clone());
            }
        }
    }
}

fn render_instructions(instructions: &[ContentItem]) -> String {
    instructions
        .iter()
        .flat_map(content_item_text)
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn render_input_item(item: &ContentItem) -> Vec<Value> {
    match item {
        ContentItem::Message { role, content, .. } => vec![serde_json::json!({
            "type": "message",
            "role": role,
            "content": content.iter().map(render_input_part).collect::<Vec<_>>(),
        })],
        ContentItem::Reasoning { id, content, .. } => {
            if let Some(raw) = content.iter().find_map(|part| match part {
                ContentPart::Json(value) if value.get("type").is_some() => Some(value.clone()),
                _ => None,
            }) {
                vec![raw]
            } else {
                let mut object = serde_json::json!({
                    "type": "reasoning",
                    "summary": content.iter().map(render_summary_part).collect::<Vec<_>>(),
                });
                if let Some(id) = id {
                    object["id"] = Value::String(id.clone());
                }
                vec![object]
            }
        }
        ContentItem::Refusal { id, content, .. } => {
            let mut object = serde_json::json!({
                "type": "message",
                "role": "assistant",
                "content": content.iter().map(render_input_part).collect::<Vec<_>>(),
            });
            if let Some(id) = id {
                object["id"] = Value::String(id.clone());
            }
            vec![object]
        }
        ContentItem::ToolUse { id, call } => vec![render_tool_call(id.as_deref(), call)],
        ContentItem::ToolResult { result } => vec![serde_json::json!({
            "type": "function_call_output",
            "call_id": result.call_id,
            "output": result.content.iter().map(render_input_part).collect::<Vec<_>>(),
        })],
        ContentItem::ServerTool { payload, .. } | ContentItem::Unknown { payload, .. } => {
            vec![payload.clone()]
        }
    }
}

fn render_input_part(part: &ContentPart) -> Value {
    match part {
        ContentPart::Text(text) => serde_json::json!({"type": "input_text", "text": text}),
        ContentPart::Refusal(text) => serde_json::json!({"type": "text", "text": text}),
        ContentPart::Reasoning { text, .. } => {
            serde_json::json!({"type": "input_text", "text": text})
        }
        ContentPart::Json(value) => value.clone(),
        ContentPart::Image {
            media_type,
            data,
            url,
        } => {
            let mut object = serde_json::json!({"type": "input_image"});
            if let Some(url) = url {
                object["image_url"] = Value::String(url.clone());
            }
            if let Some(data) = data {
                object["image_data"] = Value::String(data.clone());
            }
            if let Some(media_type) = media_type {
                object["media_type"] = Value::String(media_type.clone());
            }
            object
        }
    }
}

fn render_summary_part(part: &ContentPart) -> Value {
    match part {
        ContentPart::Reasoning { text, .. } | ContentPart::Text(text) => {
            serde_json::json!({"type": "summary_text", "text": text})
        }
        ContentPart::Json(value) => value.clone(),
        other => render_input_part(other),
    }
}

fn render_tool_call(id: Option<&str>, call: &ToolCall) -> Value {
    let mut object = serde_json::json!({
        "type": "function_call",
        "call_id": call.id.as_deref().or(id),
        "name": call.name,
        "arguments": call.arguments,
    });
    if let Some(status) = &call.status {
        object["status"] = Value::String(status.clone());
    }
    if let Some(object) = object.as_object_mut() {
        for (key, value) in &call.metadata {
            object.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }
    object
}

fn render_tool(tool: &crate::ir::ToolDefinition) -> Value {
    let mut object = serde_json::json!({
        "type": tool.kind,
        "name": tool.name,
    });
    if let Some(description) = &tool.description {
        object["description"] = Value::String(description.clone());
    }
    if let Some(schema) = &tool.input_schema {
        object["parameters"] = schema.clone();
    }
    if let Some(object) = object.as_object_mut() {
        for (key, value) in &tool.metadata {
            object.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }
    object
}

fn content_item_text(item: &ContentItem) -> Vec<String> {
    match item {
        ContentItem::Message { content, .. }
        | ContentItem::Refusal { content, .. }
        | ContentItem::Reasoning { content, .. } => content
            .iter()
            .filter_map(|part| match part {
                ContentPart::Text(text)
                | ContentPart::Refusal(text)
                | ContentPart::Reasoning { text, .. } => Some(text.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn insert_string(output: &mut Map<String, Value>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        output.insert(key.to_owned(), Value::String(value.to_owned()));
    }
}
