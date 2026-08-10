use serde_json::{Map, Value};

use crate::{
    error::ConversionError,
    ir::{
        ContentItem, ContentPart, ExtensionScope, ResponseIr, StopReason, ToolCall, ToolResult,
        Usage,
    },
};

pub fn parse(body: &Value) -> Result<ResponseIr, ConversionError> {
    let object = body
        .as_object()
        .ok_or_else(|| ConversionError::invalid("responses response must be an object"))?;
    let mut output = Vec::new();
    for item in object
        .get("output")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(item_object) = item.as_object() else {
            output.push(ContentItem::Unknown {
                id: None,
                kind: "unknown".into(),
                payload: item.clone(),
                extensions: Vec::new(),
            });
            continue;
        };
        let id = item_object
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        match item_object.get("type").and_then(Value::as_str) {
            Some("message") => {
                let role = item_object
                    .get("role")
                    .and_then(Value::as_str)
                    .unwrap_or("assistant")
                    .to_owned();
                let mut content = Vec::new();
                for part in item_object
                    .get("content")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    let Some(part_object) = part.as_object() else {
                        content.push(ContentPart::Json(part.clone()));
                        continue;
                    };
                    match part_object.get("type").and_then(Value::as_str) {
                        Some("output_text") => content.push(
                            ContentPart::Text(
                                part_object
                                    .get("text")
                                    .and_then(Value::as_str)
                                    .unwrap_or_default()
                                    .into(),
                            )
                            .with_extensions(extensions(
                                part_object,
                                &["type", "text", "annotations"],
                                ExtensionScope::ContentPart,
                            )),
                        ),
                        Some("refusal") => content.push(
                            ContentPart::Refusal(
                                part_object
                                    .get("refusal")
                                    .and_then(Value::as_str)
                                    .unwrap_or_default()
                                    .into(),
                            )
                            .with_extensions(extensions(
                                part_object,
                                &["type", "refusal"],
                                ExtensionScope::ContentPart,
                            )),
                        ),
                        _ => content.push(ContentPart::Json(part.clone())),
                    }
                }
                output.push(ContentItem::Message {
                    id: item_object
                        .get("id")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    role,
                    content,
                    extensions: extensions(
                        item_object,
                        &["type", "id", "status", "role", "content"],
                        ExtensionScope::Message,
                    ),
                });
            }
            Some("reasoning") => {
                let mut content = Vec::new();
                for summary in item_object
                    .get("summary")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    if let Some(text) = summary.get("text").and_then(Value::as_str) {
                        content.push(
                            ContentPart::Reasoning {
                                text: text.into(),
                                signature: None,
                            }
                            .with_extensions(
                                summary
                                    .as_object()
                                    .map(|summary| {
                                        extensions(
                                            summary,
                                            &["type", "text"],
                                            ExtensionScope::ContentPart,
                                        )
                                    })
                                    .unwrap_or_default(),
                            ),
                        );
                    } else {
                        content.push(ContentPart::Json(summary.clone()));
                    }
                }
                output.push(ContentItem::Reasoning {
                    id,
                    content,
                    extensions: extensions(
                        item_object,
                        &["type", "id", "status", "summary"],
                        ExtensionScope::ContentItem,
                    ),
                });
            }
            Some("function_call" | "custom_tool_call") => output.push(ContentItem::ToolUse {
                id: id.clone(),
                call: ToolCall {
                    id: item_object
                        .get("call_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .or(id),
                    name: item_object
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .into(),
                    arguments: item_object
                        .get("arguments")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .into(),
                    status: item_object
                        .get("status")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    metadata: item_object
                        .get("metadata")
                        .and_then(Value::as_object)
                        .cloned()
                        .unwrap_or_default()
                        .into_iter()
                        .collect(),
                    function_metadata: Map::new().into_iter().collect(),
                    extensions: extensions(
                        item_object,
                        &[
                            "type",
                            "id",
                            "status",
                            "call_id",
                            "name",
                            "arguments",
                            "metadata",
                        ],
                        ExtensionScope::ToolCall,
                    ),
                },
            }),
            Some("function_call_output") => output.push(ContentItem::ToolResult {
                result: ToolResult {
                    call_id: item_object
                        .get("call_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    content: vec![ContentPart::Json(
                        item_object.get("output").cloned().unwrap_or(Value::Null),
                    )],
                    is_error: false,
                    extensions: extensions(
                        item_object,
                        &["type", "call_id", "output"],
                        ExtensionScope::ContentItem,
                    ),
                },
            }),
            _ => output.push(ContentItem::Unknown {
                id,
                kind: item_object
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .into(),
                payload: item.clone(),
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
            cached_input_tokens: usage
                .get("input_tokens_details")
                .and_then(Value::as_object)
                .and_then(|v| v.get("cached_tokens"))
                .and_then(Value::as_u64),
            cache_write_input_tokens: usage
                .get("input_tokens_details")
                .and_then(Value::as_object)
                .and_then(|details| details.get("cache_write_tokens"))
                .and_then(Value::as_u64),
            cache_miss_input_tokens: None,
            reasoning_tokens: usage
                .get("output_tokens_details")
                .and_then(Value::as_object)
                .and_then(|details| details.get("reasoning_tokens"))
                .and_then(Value::as_u64),
            extensions: usage_extensions(
                usage,
                &[
                    "input_tokens",
                    "output_tokens",
                    "total_tokens",
                    "input_tokens_details",
                    "output_tokens_details",
                ],
            ),
        })
        .unwrap_or_default();
    let status = object
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("completed");
    let reason = object
        .get("incomplete_details")
        .and_then(Value::as_object)
        .and_then(|v| v.get("reason"))
        .and_then(Value::as_str)
        .unwrap_or(status);
    let stop_reason = match status {
        "in_progress" | "queued" => None,
        "failed" => Some(StopReason::Error),
        "cancelled" => Some(StopReason::Cancelled),
        _ => Some(match reason {
            "max_output_tokens" => StopReason::MaxTokens,
            "content_filter" => StopReason::ContentFilter,
            "tool_use" => StopReason::ToolUse,
            "completed" | "stop" => StopReason::Natural,
            other => StopReason::Unknown(other.into()),
        }),
    };
    let error = object.get("error").filter(|v| !v.is_null()).cloned();
    Ok(ResponseIr {
        response_id: object.get("id").and_then(Value::as_str).map(str::to_owned),
        model: object
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_owned),
        output,
        usage,
        stop_reason,
        original_stop_reason: (!reason.is_empty()).then(|| reason.into()),
        error,
        extensions: extensions(
            object,
            &[
                "id",
                "object",
                "created_at",
                "model",
                "status",
                "output",
                "usage",
                "error",
                "incomplete_details",
            ],
            ExtensionScope::Response,
        ),
        ..ResponseIr::default()
    })
}

fn extensions(
    object: &Map<String, Value>,
    structural: &[&str],
    scope: ExtensionScope,
) -> Vec<crate::ir::Extension> {
    object
        .iter()
        .filter(|(key, _)| !structural.contains(&key.as_str()))
        .map(|(key, value)| {
            crate::ir::Extension::scoped_opaque("openai.responses", key, value.clone(), scope)
        })
        .collect()
}

fn usage_extensions(object: &Map<String, Value>, structural: &[&str]) -> Vec<crate::ir::Extension> {
    object
        .iter()
        .filter(|(key, _)| !structural.contains(&key.as_str()))
        .map(|(key, value)| {
            crate::ir::Extension::scoped_opaque(
                "openai.responses",
                key,
                value.clone(),
                ExtensionScope::Usage,
            )
        })
        .collect()
}
