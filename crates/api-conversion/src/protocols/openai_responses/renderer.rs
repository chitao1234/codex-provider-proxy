use serde_json::{json, Map, Value};

use crate::ir::{ContentItem, ContentPart, ResponseIr};

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
    object.insert("usage".into(), json!({"input_tokens": response.usage.input_tokens, "output_tokens": response.usage.output_tokens, "total_tokens": response.usage.total_tokens, "input_tokens_details": {"cached_tokens": response.usage.cached_input_tokens}}));
    for extension in response.extensions.iter().chain(response.opaque.iter()) {
        object
            .entry(extension.key.clone())
            .or_insert_with(|| extension.value.clone());
    }
    Value::Object(object)
}

fn render_item(index: usize, item: &ContentItem) -> Value {
    match item {
        ContentItem::Message { role, content, .. } => {
            json!({"id": format!("msg_{index}"), "type": "message", "status": "completed", "role": role, "content": content.iter().filter_map(render_content).collect::<Vec<_>>() })
        }
        ContentItem::Reasoning { id, content, .. } => {
            json!({"id": id.clone().unwrap_or_else(|| format!("rs_{index}")), "type": "reasoning", "status": "completed", "summary": content.iter().filter_map(|part| match part { ContentPart::Reasoning { text, .. } => Some(json!({"type":"summary_text","text":text})), ContentPart::Text(text) => Some(json!({"type":"summary_text","text":text})), _ => None }).collect::<Vec<_>>() })
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
                object
                    .entry("id")
                    .or_insert_with(|| id.clone().map_or(Value::Null, Value::String));
                object
                    .entry("type")
                    .or_insert_with(|| Value::String(kind.clone()));
            }
            value
        }
    }
}

fn render_content(part: &ContentPart) -> Option<Value> {
    match part {
        ContentPart::Text(text) => Some(json!({"type":"output_text","text":text,"annotations":[]})),
        ContentPart::Refusal(text) => Some(json!({"type":"refusal","refusal":text})),
        ContentPart::Json(value) => Some(value.clone()),
        _ => None,
    }
}
fn render_part_value(part: &ContentPart) -> Option<Value> {
    match part {
        ContentPart::Text(text) | ContentPart::Refusal(text) => Some(Value::String(text.clone())),
        ContentPart::Json(value) => Some(value.clone()),
        _ => None,
    }
}
