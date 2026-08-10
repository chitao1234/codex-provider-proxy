use serde_json::Value;

use crate::{
    error::ConversionError,
    ir::{ContentItem, ContentPart, RequestIr},
    protocols::request_support::{self, object, parse_parts, parse_tool_call},
};

pub fn parse(body: &Value) -> Result<RequestIr, ConversionError> {
    let source = object(body)?;
    let mut request = request_support::base_request(source, "openai_chat_completions");
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
    let mut items = vec![ContentItem::Message {
        role,
        content: parse_parts(object.get("content").unwrap_or(&Value::Null)),
        extensions: Vec::new(),
    }];
    if let Some(tool_calls) = object.get("tool_calls").and_then(Value::as_array) {
        items.extend(tool_calls.iter().filter_map(|tool| {
            let object = tool.as_object()?;
            let function = object.get("function").and_then(Value::as_object)?;
            Some(ContentItem::ToolUse {
                id: object.get("id").and_then(Value::as_str).map(str::to_owned),
                call: parse_tool_call(function),
            })
        }));
    }
    if let Some(reasoning) = object.get("reasoning_content").and_then(Value::as_str) {
        items.insert(
            0,
            ContentItem::Reasoning {
                id: None,
                content: vec![ContentPart::Reasoning {
                    text: reasoning.to_owned(),
                    signature: None,
                }],
                extensions: Vec::new(),
            },
        );
    }
    items
}
