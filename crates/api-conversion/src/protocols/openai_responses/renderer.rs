use serde_json::{json, Map, Value};

use crate::ir::{ContentItem, ContentPart, Extension, ExtensionScope, ResponseIr};

pub fn render(response: &ResponseIr) -> Value {
    let response_id = response.response_id.as_deref().map(|id| {
        if id.starts_with("resp_") || id.starts_with("resp-") {
            id.to_owned()
        } else {
            format!("resp_{id}")
        }
    });
    let mut object = Map::new();
    object.insert("id".into(), response_id.map_or(Value::Null, Value::String));
    object.insert("object".into(), Value::String("response".into()));
    if let Some(model) = &response.model {
        object.insert("model".into(), Value::String(model.clone()));
    }
    object.insert(
        "status".into(),
        Value::String(
            if response.error.is_some() {
                "failed"
            } else {
                "completed"
            }
            .into(),
        ),
    );
    object.insert(
        "output".into(),
        Value::Array(
            response
                .output
                .iter()
                .enumerate()
                .map(|(index, item)| render_item(index, item))
                .collect(),
        ),
    );
    object.insert("usage".into(), render_usage(&response.usage));
    for extension in &response.extensions {
        if extension.is_replayable_in(ExtensionScope::Response) {
            object
                .entry(extension.key.clone())
                .or_insert_with(|| extension.value.clone());
        }
    }
    Value::Object(object)
}

pub(crate) fn render_usage(usage: &crate::ir::Usage) -> Value {
    let mut rendered = serde_json::Map::new();
    let mut insert = |key: &str, value: Option<u64>| {
        if let Some(value) = value {
            rendered.insert(key.to_owned(), serde_json::json!(value));
        }
    };
    insert("input_tokens", usage.input_tokens);
    insert("output_tokens", usage.output_tokens);
    // Codex requires total_tokens to be a number, never null and never absent.
    let total = usage.total_tokens.or_else(|| {
        usage
            .input_tokens
            .zip(usage.output_tokens)
            .and_then(|(input, output)| input.checked_add(output))
    });
    rendered.insert(
        "total_tokens".to_owned(),
        serde_json::json!(total.unwrap_or(0)),
    );
    let mut input_details = serde_json::Map::new();
    insert_details(
        &mut input_details,
        "cached_tokens",
        usage.cached_input_tokens,
    );
    insert_details(
        &mut input_details,
        "cache_write_tokens",
        usage.cache_write_input_tokens,
    );
    if !input_details.is_empty() {
        rendered.insert(
            "input_tokens_details".to_owned(),
            serde_json::Value::Object(input_details),
        );
    }
    let mut output_details = serde_json::Map::new();
    insert_details(
        &mut output_details,
        "reasoning_tokens",
        usage.reasoning_tokens,
    );
    if !output_details.is_empty() {
        rendered.insert(
            "output_tokens_details".to_owned(),
            serde_json::Value::Object(output_details),
        );
    }
    let mut rendered = serde_json::Value::Object(rendered);
    if let Some(object) = rendered.as_object_mut() {
        for extension in &usage.extensions {
            if extension.is_replayable_in(ExtensionScope::Usage) {
                object
                    .entry(extension.key.clone())
                    .or_insert_with(|| extension.value.clone());
            }
        }
    }
    rendered
}

fn insert_details(output: &mut serde_json::Map<String, Value>, key: &str, value: Option<u64>) {
    if let Some(value) = value {
        output.insert(key.to_owned(), serde_json::json!(value));
    }
}

pub(crate) fn render_item(index: usize, item: &ContentItem) -> Value {
    let mut rendered = match item {
        ContentItem::Message {
            id, role, content, ..
        } => {
            json!({"id": id.clone().unwrap_or_else(|| format!("msg_{index}")), "type": "message", "status": "completed", "role": role, "content": content.iter().filter_map(render_content).collect::<Vec<_>>() })
        }
        ContentItem::Reasoning { id, content, .. } => {
            json!({"id": id.clone().unwrap_or_else(|| format!("rs_{index}")), "type": "reasoning", "status": "completed", "summary": content.iter().filter_map(render_summary_content).collect::<Vec<_>>() })
        }
        ContentItem::Refusal { id, content, .. } => {
            json!({"id": id.clone().unwrap_or_else(|| format!("msg_{index}")), "type": "message", "status": "completed", "role": "assistant", "content": content.iter().filter_map(render_content).collect::<Vec<_>>() })
        }
        ContentItem::ToolUse { id, call } => {
            json!({"id": id.clone().unwrap_or_else(|| format!("fc_{index}")), "type": "function_call", "status": call.status.clone().unwrap_or_else(|| "completed".into()), "call_id": call.id, "name": call.name, "arguments": call.arguments})
        }
        ContentItem::ToolResult { result } => {
            json!({"type": "function_call_output", "call_id": result.call_id, "output": result.content.iter().filter_map(render_part_value).collect::<Vec<_>>() })
        }
        ContentItem::ServerTool {
            id, kind, payload, ..
        } => json!({"id": id, "type": kind, "status": "completed", "payload": payload}),
        ContentItem::Unknown {
            id, kind, payload, ..
        } => {
            let mut value = payload.clone();
            if let Some(object) = value.as_object_mut() {
                object.entry("id").or_insert_with(|| {
                    Value::String(id.clone().unwrap_or_else(|| format!("item_{index}")))
                });
                object
                    .entry("type")
                    .or_insert_with(|| Value::String(kind.clone()));
            }
            value
        }
    };
    if let Some(object) = rendered.as_object_mut() {
        replay_item_extensions(object, item);
    }
    rendered
}

fn replay_item_extensions(output: &mut Map<String, Value>, item: &ContentItem) {
    match item {
        ContentItem::Message { extensions, .. } => {
            replay_scoped(output, extensions, ExtensionScope::Message);
            replay_scoped(output, extensions, ExtensionScope::ContentItem);
        }
        ContentItem::Refusal { extensions, .. }
        | ContentItem::Reasoning { extensions, .. }
        | ContentItem::ServerTool { extensions, .. }
        | ContentItem::Unknown { extensions, .. } => {
            replay_scoped(output, extensions, ExtensionScope::ContentItem)
        }
        ContentItem::ToolUse { call, .. } => {
            replay_map(output, &call.metadata);
            replay_map(output, &call.function_metadata);
            replay_scoped(output, &call.extensions, ExtensionScope::ToolCall);
            replay_scoped(output, &call.extensions, ExtensionScope::ToolFunction);
        }
        ContentItem::ToolResult { result } => {
            replay_scoped(output, &result.extensions, ExtensionScope::ContentItem)
        }
    }
}

fn replay_scoped(output: &mut Map<String, Value>, extensions: &[Extension], scope: ExtensionScope) {
    for extension in extensions {
        if extension.is_replayable_in(scope) {
            output
                .entry(extension.key.clone())
                .or_insert_with(|| extension.value.clone());
        }
    }
}

fn replay_map(
    output: &mut Map<String, Value>,
    extensions: &std::collections::BTreeMap<String, Value>,
) {
    for (key, value) in extensions {
        output.entry(key.clone()).or_insert_with(|| value.clone());
    }
}

fn render_content(part: &ContentPart) -> Option<Value> {
    let rendered = match part.semantic() {
        ContentPart::Text(text) => Some(json!({"type":"output_text","text":text,"annotations":[]})),
        ContentPart::Refusal(text) => Some(json!({"type":"refusal","refusal":text})),
        ContentPart::Json(value) => Some(value.clone()),
        _ => None,
    }?;
    Some(replay_content_part_extensions(rendered, part))
}
fn render_part_value(part: &ContentPart) -> Option<Value> {
    match part.semantic() {
        ContentPart::Text(text) | ContentPart::Refusal(text) => Some(Value::String(text.clone())),
        ContentPart::Json(value) => Some(value.clone()),
        _ => None,
    }
}

fn render_summary_content(part: &ContentPart) -> Option<Value> {
    let rendered = match part.semantic() {
        ContentPart::Reasoning { text, .. } | ContentPart::Text(text) => {
            json!({"type":"summary_text","text":text})
        }
        ContentPart::Json(value) => value.clone(),
        _ => return None,
    };
    Some(replay_content_part_extensions(rendered, part))
}

fn replay_content_part_extensions(mut rendered: Value, part: &ContentPart) -> Value {
    if let Some(object) = rendered.as_object_mut() {
        replay_scoped(object, part.extensions(), ExtensionScope::ContentPart);
    }
    rendered
}
