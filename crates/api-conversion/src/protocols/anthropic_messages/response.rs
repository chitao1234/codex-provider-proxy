use serde_json::{Map, Value};

use crate::{
    error::ConversionError,
    ir::{ContentItem, ContentPart, ResponseIr, StopReason, ToolCall, Usage},
};

pub fn parse(body: &Value) -> Result<ResponseIr, ConversionError> {
    let object = body
        .as_object()
        .ok_or_else(|| ConversionError::invalid("messages response must be an object"))?;
    let mut output = Vec::new();
    for block in object
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(block_object) = block.as_object() else {
            output.push(ContentItem::Unknown {
                id: None,
                kind: "unknown".into(),
                payload: block.clone(),
                extensions: Vec::new(),
            });
            continue;
        };
        match block_object.get("type").and_then(Value::as_str) {
            Some("text") => output.push(ContentItem::Message {
                role: "assistant".into(),
                content: vec![ContentPart::Text(
                    block_object
                        .get("text")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .into(),
                )],
                extensions: extensions(block_object, &["type", "text"]),
            }),
            Some("thinking") => output.push(ContentItem::Reasoning {
                id: block_object
                    .get("id")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                content: vec![ContentPart::Reasoning {
                    text: block_object
                        .get("thinking")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .into(),
                    signature: block_object
                        .get("signature")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                }],
                extensions: extensions(block_object, &["type", "thinking", "signature", "id"]),
            }),
            Some("redacted_thinking") => output.push(ContentItem::Unknown {
                id: block_object
                    .get("id")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                kind: "redacted_thinking".into(),
                payload: block.clone(),
                extensions: Vec::new(),
            }),
            Some("tool_use") => output.push(ContentItem::ToolUse {
                id: block_object
                    .get("id")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                call: ToolCall {
                    id: block_object
                        .get("id")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    name: block_object
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .into(),
                    arguments: serde_json::to_string(
                        block_object.get("input").unwrap_or(&Value::Null),
                    )
                    .unwrap_or_default(),
                    status: Some("completed".into()),
                    metadata: extensions(block_object, &["type", "id", "name", "input"])
                        .into_iter()
                        .map(|e| (e.key, e.value))
                        .collect(),
                    function_metadata: Map::new().into_iter().collect(),
                    extensions: Vec::new(),
                },
            }),
            _ => output.push(ContentItem::Unknown {
                id: block_object
                    .get("id")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                kind: block_object
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .into(),
                payload: block.clone(),
                extensions: Vec::new(),
            }),
        }
    }
    let usage = object
        .get("usage")
        .and_then(Value::as_object)
        .map(|usage| Usage {
            input_tokens: usage.get("input_tokens").and_then(Value::as_u64),
            output_tokens: usage.get("output_tokens").and_then(Value::as_u64),
            total_tokens: usage.get("total_tokens").and_then(Value::as_u64),
            cached_input_tokens: usage.get("cache_read_input_tokens").and_then(Value::as_u64),
            cache_write_input_tokens: usage
                .get("cache_creation_input_tokens")
                .and_then(Value::as_u64),
            cache_miss_input_tokens: None,
            reasoning_tokens: usage
                .get("output_tokens_details")
                .and_then(|details| details.get("thinking_tokens"))
                .and_then(Value::as_u64),
            extensions: Vec::new(),
        })
        .unwrap_or_default();
    let reason = object
        .get("stop_reason")
        .and_then(Value::as_str)
        .unwrap_or("end_turn");
    let stop_reason = match reason {
        "tool_use" => StopReason::ToolUse,
        "max_tokens" => StopReason::MaxTokens,
        "stop_sequence" | "end_turn" => StopReason::Natural,
        other => StopReason::Unknown(other.into()),
    };
    Ok(ResponseIr {
        response_id: object.get("id").and_then(Value::as_str).map(str::to_owned),
        model: object
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_owned),
        output,
        usage,
        stop_reason: Some(stop_reason),
        original_stop_reason: Some(reason.into()),
        extensions: extensions(
            object,
            &[
                "id",
                "type",
                "role",
                "model",
                "content",
                "stop_reason",
                "stop_sequence",
                "usage",
            ],
        ),
        ..ResponseIr::default()
    })
}

fn extensions(object: &Map<String, Value>, structural: &[&str]) -> Vec<crate::ir::Extension> {
    object
        .iter()
        .filter(|(key, _)| !structural.contains(&key.as_str()))
        .map(|(key, value)| {
            crate::ir::Extension::response_opaque("anthropic.messages", key, value.clone())
        })
        .collect()
}
