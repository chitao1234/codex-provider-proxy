use crate::ir::{ContentItem, ContentPart, Extension, ExtensionScope, ResponseIr, StopReason};
use serde_json::{json, Map, Value};

pub fn render(response: &ResponseIr) -> Value {
    let response_id = response.response_id.as_deref().map(|id| {
        if id.starts_with("msg_") {
            id.to_owned()
        } else {
            format!("msg_{}", id.trim_start_matches("chatcmpl-"))
        }
    });
    let mut content = Vec::new();
    for item in &response.output {
        content.extend(render_item(item));
    }
    let stop_reason = response.stop_reason.as_ref().map(|reason| match reason {
        StopReason::Natural => "end_turn".to_owned(),
        StopReason::ToolUse => "tool_use".to_owned(),
        StopReason::MaxTokens => "max_tokens".to_owned(),
        StopReason::ContentFilter | StopReason::Error | StopReason::Cancelled => {
            "end_turn".to_owned()
        }
        StopReason::Unknown(reason) => reason.clone(),
    });
    let mut rendered = json!({
        "id": response_id,
        "type": "message",
        "role": "assistant",
        "model": response.model,
        "content": content,
        "stop_reason": stop_reason,
        "usage": {
            "input_tokens": response.usage.input_tokens,
            "output_tokens": response.usage.output_tokens,
            "total_tokens": response.usage.total_tokens,
            "cache_read_input_tokens": response.usage.cached_input_tokens,
            "cache_creation_input_tokens": response.usage.cache_write_input_tokens,
            "output_tokens_details": {"thinking_tokens": response.usage.reasoning_tokens},
        },
    });
    if let Some(usage) = rendered.get_mut("usage").and_then(Value::as_object_mut) {
        replay_scoped(usage, &response.usage.extensions, ExtensionScope::Usage);
    }
    if let Some(details) = response.extensions.iter().find(|extension| {
        extension.scope == ExtensionScope::Response && extension.key == "output_tokens_details"
    }) {
        let mut output_details = details.value.clone();
        if let Some(object) = output_details.as_object_mut() {
            if let Some(reasoning_tokens) = object.remove("reasoning_tokens") {
                object.insert("thinking_tokens".into(), reasoning_tokens);
            }
        }
        rendered["usage"]["output_tokens_details"] = output_details;
    }
    if let Some(object) = rendered.as_object_mut() {
        replay_response_extensions(object, response);
        for item in &response.output {
            if let ContentItem::Message { extensions, .. } = item {
                replay_scoped(object, extensions, ExtensionScope::Message);
            }
        }
    }
    rendered
}

fn render_item(item: &ContentItem) -> Vec<Value> {
    match item {
        ContentItem::Message { content, .. } | ContentItem::Refusal { content, .. } => content
            .iter()
            .filter_map(render_content)
            .map(|mut block| {
                replay_content_item_extensions(&mut block, item);
                block
            })
            .collect(),
        ContentItem::Reasoning { content, .. } => content
            .iter()
            .filter_map(|part| match part.semantic() {
                ContentPart::Reasoning { text, signature } => Some(replay_content_part_extensions(
                    json!({"type":"thinking","thinking":text,"signature":signature}),
                    part,
                )),
                _ => None,
            })
            .map(|mut block| {
                replay_content_item_extensions(&mut block, item);
                block
            })
            .collect(),
        ContentItem::ToolUse { id, call } => {
            let mut block = json!({
                "type": "tool_use",
                "id": id.clone().or_else(|| call.id.clone()),
                "name": call.name,
                "input": serde_json::from_str::<Value>(&call.arguments)
                    .unwrap_or_else(|_| Value::String(call.arguments.clone())),
            });
            if let Some(object) = block.as_object_mut() {
                replay_map(object, &call.metadata);
                replay_map(object, &call.function_metadata);
                replay_scoped(object, &call.extensions, ExtensionScope::ToolCall);
                replay_scoped(object, &call.extensions, ExtensionScope::ToolFunction);
            }
            vec![block]
        }
        ContentItem::ToolResult { result } => {
            let mut block = json!({
                "type": "tool_result",
                "tool_use_id": result.call_id,
                "content": result.content.iter().filter_map(render_content).collect::<Vec<_>>(),
                "is_error": result.is_error,
            });
            if let Some(object) = block.as_object_mut() {
                replay_scoped(object, &result.extensions, ExtensionScope::ContentItem);
            }
            vec![block]
        }
        ContentItem::ServerTool {
            id,
            kind,
            payload,
            extensions,
        }
        | ContentItem::Unknown {
            id,
            kind,
            payload,
            extensions,
        } => {
            let mut block = payload.clone();
            if !block.is_object() {
                block = json!({"payload": block});
            }
            if let Some(object) = block.as_object_mut() {
                object
                    .entry("type")
                    .or_insert_with(|| Value::String(kind.clone()));
                if let Some(id) = id {
                    object
                        .entry("id")
                        .or_insert_with(|| Value::String(id.clone()));
                }
                replay_scoped(object, extensions, ExtensionScope::ContentItem);
            }
            vec![block]
        }
    }
}

fn replay_content_item_extensions(block: &mut Value, item: &ContentItem) {
    let Some(object) = block.as_object_mut() else {
        return;
    };
    match item {
        ContentItem::Refusal { extensions, .. } | ContentItem::Reasoning { extensions, .. } => {
            replay_scoped(object, extensions, ExtensionScope::ContentItem)
        }
        ContentItem::Message { extensions, .. } => {
            replay_scoped(object, extensions, ExtensionScope::ContentItem)
        }
        _ => {}
    }
}

fn replay_response_extensions(output: &mut Map<String, Value>, response: &ResponseIr) {
    for extension in response.extensions.iter().chain(response.opaque.iter()) {
        if extension.key != "output_tokens_details"
            && extension.is_replayable_in(ExtensionScope::Response)
        {
            output
                .entry(extension.key.clone())
                .or_insert_with(|| extension.value.clone());
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
        ContentPart::Text(text) => Some(json!({"type":"text","text":text})),
        ContentPart::Refusal(text) => Some(json!({"type":"text","text":text})),
        ContentPart::Json(value) => Some(value.clone()),
        _ => None,
    }?;
    Some(replay_content_part_extensions(rendered, part))
}

fn replay_content_part_extensions(mut rendered: Value, part: &ContentPart) -> Value {
    if let Some(object) = rendered.as_object_mut() {
        replay_scoped(object, part.extensions(), ExtensionScope::ContentPart);
    }
    rendered
}
