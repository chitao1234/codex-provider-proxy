use serde_json::{Map, Value};

use crate::{
    error::ConversionError,
    ir::{ContentItem, ContentPart, ResponseIr, StopReason, ToolCall, Usage},
};

pub fn parse(body: &Value) -> Result<ResponseIr, ConversionError> {
    let object = body
        .as_object()
        .ok_or_else(|| ConversionError::invalid("chat response must be an object"))?;
    let choice = object
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(Value::as_object);
    let message = choice
        .and_then(|choice| choice.get("message"))
        .and_then(Value::as_object);
    let mut output = Vec::new();
    if let Some(message) = message {
        let role = message
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or("assistant")
            .to_owned();
        let mut content = Vec::new();
        if let Some(text) = message.get("content").and_then(Value::as_str) {
            content.push(ContentPart::Text(text.to_owned()));
        }
        if let Some(reasoning) = message.get("reasoning_content").and_then(Value::as_str) {
            output.push(ContentItem::Reasoning {
                id: None,
                content: vec![ContentPart::Reasoning {
                    text: reasoning.to_owned(),
                    signature: None,
                }],
                extensions: Vec::new(),
            });
        }
        if !content.is_empty() {
            output.push(ContentItem::Message {
                role,
                content,
                extensions: extensions(
                    message,
                    &[
                        "role",
                        "content",
                        "reasoning_content",
                        "tool_calls",
                        "refusal",
                    ],
                ),
            });
        }
        if let Some(refusal) = message.get("refusal").and_then(Value::as_str) {
            output.push(ContentItem::Refusal {
                id: None,
                content: vec![ContentPart::Refusal(refusal.to_owned())],
                extensions: Vec::new(),
            });
        }
        if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
            for tool in tool_calls {
                let Some(function) = tool.get("function").and_then(Value::as_object) else {
                    output.push(ContentItem::Unknown {
                        id: tool.get("id").and_then(Value::as_str).map(str::to_owned),
                        kind: "tool_call".to_owned(),
                        payload: tool.clone(),
                        extensions: Vec::new(),
                    });
                    continue;
                };
                let name = function
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                let arguments = function
                    .get("arguments")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                output.push(ContentItem::ToolUse {
                    id: tool.get("id").and_then(Value::as_str).map(str::to_owned),
                    call: ToolCall {
                        id: tool.get("id").and_then(Value::as_str).map(str::to_owned),
                        name,
                        arguments,
                        status: Some("completed".to_owned()),
                        metadata: tool
                            .as_object()
                            .map(|tool| extensions(tool, &["id", "type", "function"]))
                            .unwrap_or_default()
                            .into_iter()
                            .map(|extension| (extension.key, extension.value))
                            .collect(),
                        function_metadata: extensions(function, &["name", "arguments"])
                            .into_iter()
                            .map(|extension| (extension.key, extension.value))
                            .collect(),
                        extensions: Vec::new(),
                    },
                });
            }
        }
    }
    let usage = object
        .get("usage")
        .and_then(Value::as_object)
        .map(parse_usage)
        .unwrap_or_default();
    let reason = choice
        .and_then(|choice| choice.get("finish_reason"))
        .and_then(Value::as_str)
        .unwrap_or("stop");
    let stop_reason = match reason {
        "tool_calls" | "function_call" => StopReason::ToolUse,
        "length" => StopReason::MaxTokens,
        "content_filter" => StopReason::ContentFilter,
        "stop" => StopReason::Natural,
        other => StopReason::Unknown(other.to_owned()),
    };
    let mut response = ResponseIr {
        response_id: object.get("id").and_then(Value::as_str).map(str::to_owned),
        model: object
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_owned),
        output,
        usage,
        stop_reason: Some(stop_reason),
        original_stop_reason: Some(reason.to_owned()),
        ..ResponseIr::default()
    };
    response.extensions = extensions(
        object,
        &["id", "object", "created", "model", "choices", "usage"],
    );
    if let Some(details) = object
        .get("usage")
        .and_then(Value::as_object)
        .and_then(|usage| usage.get("completion_tokens_details"))
    {
        response.extensions.push(crate::ir::Extension::new(
            "openai.chat_completions",
            "output_tokens_details",
            details.clone(),
        ));
    }
    Ok(response)
}

fn parse_usage(object: &Map<String, Value>) -> Usage {
    Usage {
        input_tokens: object.get("prompt_tokens").and_then(Value::as_u64),
        output_tokens: object.get("completion_tokens").and_then(Value::as_u64),
        total_tokens: object
            .get("total_tokens")
            .and_then(Value::as_u64)
            .or_else(|| {
                object
                    .get("prompt_tokens")
                    .and_then(Value::as_u64)
                    .zip(object.get("completion_tokens").and_then(Value::as_u64))
                    .map(|(input, output)| input + output)
            }),
        cached_input_tokens: object
            .get("prompt_tokens_details")
            .and_then(Value::as_object)
            .and_then(|details| details.get("cached_tokens"))
            .and_then(Value::as_u64)
            .or_else(|| object.get("cached_tokens").and_then(Value::as_u64)),
    }
}

fn extensions(object: &Map<String, Value>, structural: &[&str]) -> Vec<crate::ir::Extension> {
    object
        .iter()
        .filter(|(key, _)| !structural.contains(&key.as_str()))
        .map(|(key, value)| {
            crate::ir::Extension::new("openai.chat_completions", key, value.clone())
        })
        .collect()
}
