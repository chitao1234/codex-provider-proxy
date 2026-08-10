use crate::ir::{ContentItem, ContentPart, ResponseIr};
use serde_json::{json, Value};

pub fn render(response: &ResponseIr) -> Value {
    let mut content = Vec::new();
    for item in &response.output {
        match item { ContentItem::Message { content: parts, .. } | ContentItem::Refusal { content: parts, .. } => content.extend(parts.iter().filter_map(render_content)), ContentItem::Reasoning { content: parts, .. } => content.extend(parts.iter().filter_map(|part| match part { ContentPart::Reasoning { text, signature } => Some(json!({"type":"thinking","thinking":text,"signature":signature})), _ => None })), ContentItem::ToolUse { id, call } => content.push(json!({"type":"tool_use","id":id.clone().or_else(|| call.id.clone()),"name":call.name,"input":serde_json::from_str::<Value>(&call.arguments).unwrap_or_else(|_| Value::String(call.arguments.clone()))})), ContentItem::Unknown { .. } | ContentItem::ToolResult { .. } | ContentItem::ServerTool { .. } => {} }
    }
    json!({"id": response.response_id, "type":"message", "role":"assistant", "model":response.model, "content":content, "stop_reason": response.original_stop_reason.clone().or_else(|| response.stop_reason.as_ref().map(ToString::to_string)), "usage":{"input_tokens":response.usage.input_tokens,"output_tokens":response.usage.output_tokens,"cache_read_input_tokens":response.usage.cached_input_tokens}})
}

fn render_content(part: &ContentPart) -> Option<Value> {
    match part {
        ContentPart::Text(text) => Some(json!({"type":"text","text":text})),
        ContentPart::Refusal(text) => Some(json!({"type":"text","text":text})),
        ContentPart::Json(value) => Some(value.clone()),
        _ => None,
    }
}
