use serde_json::Value;

use crate::{
    error::ConversionError,
    ir::{ContentItem, ContentPart, RequestIr},
    protocols::request_support::{self, object, parse_parts, parse_tool_call, parse_tool_result},
};

pub fn parse(body: &Value) -> Result<RequestIr, ConversionError> {
    let source = object(body)?;
    let mut request = request_support::base_request(source, "openai_responses");
    request.instructions = source
        .get("instructions")
        .map(|value| {
            vec![ContentItem::Message {
                role: "system".to_owned(),
                content: parse_parts(value),
                extensions: Vec::new(),
            }]
        })
        .unwrap_or_default();
    if let Some(input) = source.get("input") {
        request.transcript = parse_input(input);
    }
    Ok(request)
}

fn parse_input(input: &Value) -> Vec<ContentItem> {
    if let Some(text) = input.as_str() {
        return vec![ContentItem::Message {
            role: "user".to_owned(),
            content: vec![ContentPart::Text(text.to_owned())],
            extensions: Vec::new(),
        }];
    }
    input
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(parse_item)
        .collect()
}

fn parse_item(value: &Value) -> Vec<ContentItem> {
    let Some(object) = value.as_object() else {
        return vec![ContentItem::Unknown {
            id: None,
            kind: "unknown".to_owned(),
            payload: value.clone(),
            extensions: Vec::new(),
        }];
    };
    match object.get("type").and_then(Value::as_str) {
        Some("message") => vec![ContentItem::Message {
            role: object
                .get("role")
                .and_then(Value::as_str)
                .unwrap_or("user")
                .to_owned(),
            content: parse_parts(object.get("content").unwrap_or(&Value::Null)),
            extensions: Vec::new(),
        }],
        Some("reasoning") => vec![ContentItem::Reasoning {
            id: object.get("id").and_then(Value::as_str).map(str::to_owned),
            content: vec![ContentPart::Json(value.clone())],
            extensions: Vec::new(),
        }],
        Some("function_call" | "custom_tool_call") => vec![ContentItem::ToolUse {
            id: object
                .get("id")
                .or_else(|| object.get("call_id"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            call: parse_tool_call(object),
        }],
        Some("function_call_output") => vec![ContentItem::ToolResult {
            result: parse_tool_result(object),
        }],
        Some("refusal") => vec![ContentItem::Refusal {
            id: object.get("id").and_then(Value::as_str).map(str::to_owned),
            content: parse_parts(object.get("content").unwrap_or(value)),
            extensions: Vec::new(),
        }],
        _ => vec![ContentItem::Unknown {
            id: object.get("id").and_then(Value::as_str).map(str::to_owned),
            kind: object
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_owned(),
            payload: value.clone(),
            extensions: Vec::new(),
        }],
    }
}
