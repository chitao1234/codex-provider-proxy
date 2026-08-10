use serde_json::Value;

use crate::{
    error::ConversionError,
    ir::{ContentItem, ContentPart, ExtensionScope, RequestIr},
    protocols::request_support::{self, object, parse_parts, parse_tool_call, parse_tool_result},
};

pub fn parse(body: &Value) -> Result<RequestIr, ConversionError> {
    let source = object(body)?;
    let mut request = request_support::base_request(source, "anthropic_messages");
    request.instructions = source
        .get("system")
        .map(|value| {
            vec![ContentItem::Message {
                id: None,
                role: "system".to_owned(),
                content: parse_parts(value, "anthropic_messages"),
                extensions: Vec::new(),
            }]
        })
        .unwrap_or_default();
    request.transcript = source
        .get("messages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(parse_message)
        .collect();
    Ok(request)
}

fn parse_message(value: &Value) -> Vec<ContentItem> {
    let Some(object) = value.as_object() else {
        return Vec::new();
    };
    let role = object
        .get("role")
        .and_then(Value::as_str)
        .unwrap_or("user")
        .to_owned();
    let message_extensions = request_support::object_extensions(
        object,
        &["role", "content"],
        ExtensionScope::Message,
        "anthropic_messages",
    );
    parse_parts(
        object.get("content").unwrap_or(&Value::Null),
        "anthropic_messages",
    )
    .into_iter()
    .map(|part| match part.semantic() {
        ContentPart::Reasoning { .. } => ContentItem::Reasoning {
            id: None,
            content: vec![part],
            extensions: message_extensions.clone(),
        },
        ContentPart::Refusal(_) => ContentItem::Refusal {
            id: None,
            content: vec![part],
            extensions: message_extensions.clone(),
        },
        part => ContentItem::Message {
            id: None,
            role: role.clone(),
            content: vec![part.clone()],
            extensions: message_extensions.clone(),
        },
    })
    .chain(
        object
            .get("content")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|block| {
                let object = block.as_object()?;
                match object.get("type").and_then(Value::as_str) {
                    Some("tool_use") => Some(ContentItem::ToolUse {
                        id: object.get("id").and_then(Value::as_str).map(str::to_owned),
                        call: parse_tool_call(object, "anthropic_messages"),
                    }),
                    Some("tool_result") => Some(ContentItem::ToolResult {
                        result: parse_tool_result(object, "anthropic_messages"),
                    }),
                    _ => None,
                }
            }),
    )
    .collect()
}
