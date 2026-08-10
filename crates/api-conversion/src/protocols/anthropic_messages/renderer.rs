use crate::ir::{ContentItem, ContentPart, ResponseIr, StopReason};
use serde_json::{json, Value};

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
        match item { ContentItem::Message { content: parts, .. } | ContentItem::Refusal { content: parts, .. } => content.extend(parts.iter().filter_map(render_content)), ContentItem::Reasoning { content: parts, .. } => content.extend(parts.iter().filter_map(|part| match part { ContentPart::Reasoning { text, signature } => Some(json!({"type":"thinking","thinking":text,"signature":signature})), _ => None })), ContentItem::ToolUse { id, call } => content.push(json!({"type":"tool_use","id":id.clone().or_else(|| call.id.clone()),"name":call.name,"input":serde_json::from_str::<Value>(&call.arguments).unwrap_or_else(|_| Value::String(call.arguments.clone()))})), ContentItem::Unknown { .. } | ContentItem::ToolResult { .. } | ContentItem::ServerTool { .. } => {} }
    }
    let stop_reason = response.stop_reason.as_ref().map(|reason| match reason {
        StopReason::Natural => "end_turn".to_owned(),
        StopReason::ToolUse => "tool_use".to_owned(),
        StopReason::MaxTokens => "max_tokens".to_owned(),
        StopReason::ContentFilter => "end_turn".to_owned(),
        StopReason::Error => "end_turn".to_owned(),
        StopReason::Cancelled => "end_turn".to_owned(),
        StopReason::Unknown(reason) => reason.clone(),
    });
    let mut rendered = json!({"id": response_id, "type":"message", "role":"assistant", "model":response.model, "content":content, "stop_reason": stop_reason, "usage":{"input_tokens":response.usage.input_tokens,"output_tokens":response.usage.output_tokens,"total_tokens":response.usage.total_tokens,"cache_read_input_tokens":response.usage.cached_input_tokens}});
    if let Some(details) = response
        .extensions
        .iter()
        .find(|extension| extension.key == "output_tokens_details")
    {
        let mut output_details = details.value.clone();
        if let Some(object) = output_details.as_object_mut() {
            if let Some(reasoning_tokens) = object.remove("reasoning_tokens") {
                object.insert("thinking_tokens".into(), reasoning_tokens);
            }
        }
        rendered["usage"]["output_tokens_details"] = output_details;
    }
    rendered
}

fn render_content(part: &ContentPart) -> Option<Value> {
    match part {
        ContentPart::Text(text) => Some(json!({"type":"text","text":text})),
        ContentPart::Refusal(text) => Some(json!({"type":"text","text":text})),
        ContentPart::Json(value) => Some(value.clone()),
        _ => None,
    }
}
