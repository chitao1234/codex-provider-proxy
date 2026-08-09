//! Protocol-neutral representation of a completed, non-streaming response.

use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Map, Value};

use crate::chat::{extract_usage, first_choice, first_choice_message, upstream_id, upstream_model};
use crate::conversion_utils::format_url_citations;
use crate::error::ConversionError;
use crate::request_ir::{ContentExtensions, IrContent, IrToolCall, Protocol, RequestIr};
use crate::stream::{map_chat_finish_reason, StopReason};

#[derive(Debug, Clone)]
pub struct ResponseIr {
    id: String,
    model: String,
    created_at: Option<Value>,
    status: String,
    incomplete_details: Option<Value>,
    error: Option<Value>,
    termination: ResponseTermination,
    items: Vec<ResponseItem>,
    usage: ResponseUsage,
    opaque_extensions: Map<String, Value>,
}

#[derive(Debug, Clone)]
enum ResponseItem {
    Reasoning {
        id: Option<String>,
        status: Option<String>,
        text: String,
        signature: Option<String>,
        opaque_extensions: Map<String, Value>,
    },
    Message {
        id: Option<String>,
        status: Option<String>,
        content: Vec<ResponseContent>,
        opaque_extensions: Map<String, Value>,
    },
    ToolCall {
        id: Option<String>,
        call_id: String,
        name: String,
        arguments: Value,
        status: Option<String>,
        opaque_extensions: Map<String, Value>,
    },
    Opaque(Value),
}

#[derive(Debug, Clone)]
struct ResponseText {
    text: String,
    annotations: Vec<Value>,
    opaque_extensions: Map<String, Value>,
}

#[derive(Debug, Clone)]
enum ResponseContent {
    Text(ResponseText),
    Opaque(Value),
}

#[derive(Default)]
struct AssistantOutputAccumulator {
    content: Vec<IrContent>,
    item_id: Option<String>,
    status: Option<String>,
    message_extensions: Map<String, Value>,
    tool_calls: Vec<IrToolCall>,
    phase: u8,
    has_message: bool,
}

impl AssistantOutputAccumulator {
    fn flush(&mut self, request: &mut RequestIr) {
        request.append_assistant_output(
            std::mem::take(&mut self.content),
            self.item_id.take(),
            self.status.take(),
            std::mem::take(&mut self.tool_calls),
            std::mem::take(&mut self.message_extensions),
        );
        self.phase = 0;
        self.has_message = false;
    }
}

#[derive(Debug, Clone)]
struct ResponseTermination {
    reason: StopReason,
    protocol_reason: Option<String>,
    stop_sequence: Option<Value>,
}

#[derive(Debug, Clone, Default)]
struct ResponseUsage {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    total_tokens: Option<u64>,
    cached_tokens: Option<u64>,
    cache_write_tokens: Option<u64>,
    cache_miss_tokens: Option<u64>,
    reasoning_tokens: Option<u64>,
    opaque_extensions: Map<String, Value>,
}

impl ResponseIr {
    pub fn parse(source: Protocol, body: &Value) -> Result<Self, ConversionError> {
        match source {
            Protocol::OpenAiChatCompletions => Self::parse_chat(body),
            Protocol::AnthropicMessages => Self::parse_messages(body),
            Protocol::OpenAiResponses => Self::parse_responses(body),
        }
    }

    pub fn render(&self, target: Protocol) -> Result<Value, ConversionError> {
        match target {
            Protocol::AnthropicMessages => Ok(self.render_messages()),
            Protocol::OpenAiResponses => Ok(self.render_responses()),
            Protocol::OpenAiChatCompletions => Err(ConversionError::unsupported(
                "response",
                "Chat Completions is not a downstream response format",
            )),
        }
    }

    pub fn id(&self) -> Option<&str> {
        (!self.id.is_empty()).then_some(self.id.as_str())
    }

    pub fn stop_reason(&self) -> StopReason {
        self.termination.reason
    }

    pub fn protocol_stop_reason(&self) -> Option<&str> {
        self.termination.protocol_reason.as_deref()
    }

    /// Whether this response contains terminal assistant output suitable for a
    /// later continuation transcript.
    pub fn is_continuable(&self) -> bool {
        self.error.as_ref().is_none_or(Value::is_null)
            && matches!(self.status.as_str(), "completed" | "incomplete")
    }

    pub fn last_reasoning(&self) -> Option<&str> {
        self.items.iter().rev().find_map(|item| match item {
            ResponseItem::Reasoning { text, .. } if !text.trim().is_empty() => Some(text.as_str()),
            _ => None,
        })
    }

    pub fn append_to_request(&self, request: &mut RequestIr) {
        let mut output = AssistantOutputAccumulator::default();
        for item in &self.items {
            match item {
                ResponseItem::Reasoning {
                    id,
                    status,
                    text,
                    signature,
                    opaque_extensions,
                } => {
                    if output.phase > 0 {
                        output.flush(request);
                    }
                    output.content.push(IrContent::Thinking {
                        id: id.clone(),
                        status: status.clone(),
                        text: text.clone(),
                        signature: signature.clone(),
                        extensions: ContentExtensions {
                            cache_control: None,
                            opaque: opaque_extensions.clone(),
                        },
                    });
                }
                ResponseItem::Message {
                    id,
                    status,
                    content: parts,
                    opaque_extensions,
                } => {
                    if output.has_message || output.phase > 1 {
                        output.flush(request);
                    }
                    output.phase = 1;
                    output.has_message = true;
                    output.item_id.clone_from(id);
                    output.status.clone_from(status);
                    output.message_extensions.extend(opaque_extensions.clone());
                    output.content.extend(parts.iter().map(|part| match part {
                        ResponseContent::Text(part) => {
                            let mut opaque = part.opaque_extensions.clone();
                            if !part.annotations.is_empty() {
                                opaque.insert(
                                    "annotations".to_owned(),
                                    Value::Array(part.annotations.clone()),
                                );
                            }
                            IrContent::Text {
                                text: part.text.clone(),
                                extensions: ContentExtensions {
                                    cache_control: None,
                                    opaque,
                                },
                            }
                        }
                        ResponseContent::Opaque(value) => IrContent::Raw(value.clone()),
                    }));
                }
                ResponseItem::ToolCall {
                    id,
                    call_id,
                    name,
                    arguments,
                    status,
                    opaque_extensions,
                } => {
                    output.phase = 2;
                    output.tool_calls.push(IrToolCall {
                        id: id.clone(),
                        call_id: Some(call_id.clone()),
                        name: name.clone(),
                        arguments: arguments.clone(),
                        status: status.clone(),
                        opaque_extensions: opaque_extensions.clone(),
                    });
                }
                ResponseItem::Opaque(value) => {
                    output.flush(request);
                    request.append_extension_item("openai.responses".to_owned(), value.clone());
                }
            }
        }
        output.flush(request);
    }

    fn parse_chat(body: &Value) -> Result<Self, ConversionError> {
        if first_choice(body).is_none() {
            return Err(ConversionError::invalid(
                "upstream chat response missing choices",
            ));
        }
        let message = first_choice_message(body)
            .ok_or_else(|| ConversionError::invalid("upstream chat response missing message"))?;
        let id = upstream_id(body).unwrap_or_default().to_owned();
        let model = upstream_model(body).unwrap_or_default().to_owned();
        let mut items = Vec::new();
        if let Some(text) = message
            .get("reasoning_content")
            .and_then(Value::as_str)
            .filter(|text| !text.trim().is_empty())
        {
            items.push(ResponseItem::Reasoning {
                id: None,
                status: Some("completed".to_owned()),
                text: text.to_owned(),
                signature: Some(synth_message_id(&id)),
                opaque_extensions: Map::new(),
            });
        }
        let mut content = Vec::new();
        if let Some(text) = message
            .get("content")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
        {
            content.push(ResponseContent::Text(ResponseText {
                text: text.to_owned(),
                annotations: message
                    .get("annotations")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default(),
                opaque_extensions: Map::new(),
            }));
        }
        if !content.is_empty() {
            items.push(ResponseItem::Message {
                id: None,
                status: Some("completed".to_owned()),
                content,
                opaque_extensions: Map::new(),
            });
        }
        if let Some(calls) = message.get("tool_calls").and_then(Value::as_array) {
            for call in calls {
                let Some(call) = call.as_object() else {
                    continue;
                };
                let function = call.get("function").and_then(Value::as_object);
                let name = function
                    .and_then(|function| function.get("name"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if name.is_empty() {
                    continue;
                }
                items.push(ResponseItem::ToolCall {
                    id: call.get("id").and_then(Value::as_str).map(str::to_owned),
                    call_id: call
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    name: name.to_owned(),
                    arguments: function
                        .and_then(|function| function.get("arguments"))
                        .cloned()
                        .unwrap_or_else(|| Value::String("{}".to_owned())),
                    status: Some("completed".to_owned()),
                    opaque_extensions: Map::new(),
                });
            }
        }
        let saw_tool_call = items
            .iter()
            .any(|item| matches!(item, ResponseItem::ToolCall { .. }));
        let stop_reason = map_chat_finish_reason(
            body.get("choices")
                .and_then(Value::as_array)
                .and_then(|choices| choices.first())
                .and_then(|choice| choice.get("finish_reason"))
                .and_then(Value::as_str),
            saw_tool_call,
        );
        let usage = extract_usage(body);
        let total_tokens = body
            .get("usage")
            .and_then(|usage| usage.get("total_tokens"))
            .and_then(Value::as_u64)
            .or_else(|| sum_tokens(usage.prompt_tokens, usage.completion_tokens));
        Ok(Self {
            id,
            model,
            created_at: Some(json!(now_unix_seconds())),
            status: status_for_stop(stop_reason).to_owned(),
            incomplete_details: incomplete_details_for_stop(stop_reason),
            error: None,
            termination: ResponseTermination {
                reason: stop_reason,
                protocol_reason: body
                    .get("choices")
                    .and_then(Value::as_array)
                    .and_then(|choices| choices.first())
                    .and_then(|choice| choice.get("finish_reason"))
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                stop_sequence: None,
            },
            items,
            usage: ResponseUsage {
                input_tokens: usage.prompt_tokens,
                output_tokens: usage.completion_tokens,
                total_tokens,
                cached_tokens: usage.cached_tokens,
                cache_write_tokens: usage.cache_write_tokens,
                cache_miss_tokens: usage.prompt_cache_miss_tokens,
                reasoning_tokens: usage.reasoning_tokens,
                opaque_extensions: body
                    .get("usage")
                    .and_then(Value::as_object)
                    .map(|usage| {
                        opaque_fields(
                            usage,
                            &[
                                "prompt_tokens",
                                "completion_tokens",
                                "total_tokens",
                                "cached_tokens",
                                "prompt_cache_hit_tokens",
                                "prompt_cache_miss_tokens",
                                "prompt_tokens_details",
                                "completion_tokens_details",
                            ],
                        )
                    })
                    .unwrap_or_default(),
            },
            opaque_extensions: Map::new(),
        })
    }

    fn parse_messages(body: &Value) -> Result<Self, ConversionError> {
        let message = body
            .as_object()
            .ok_or_else(|| ConversionError::invalid("Messages response is not an object"))?;
        let id = string_field(message, "id");
        let model = string_field(message, "model");
        let mut items = Vec::new();
        let mut message_content = Vec::new();
        if let Some(content) = message.get("content").and_then(Value::as_array) {
            for block in content {
                let Some(object) = block.as_object() else {
                    flush_message_content(&mut items, &mut message_content);
                    items.push(ResponseItem::Opaque(block.clone()));
                    continue;
                };
                match object.get("type").and_then(Value::as_str) {
                    Some("thinking") => {
                        flush_message_content(&mut items, &mut message_content);
                        items.push(ResponseItem::Reasoning {
                            id: None,
                            status: Some("completed".to_owned()),
                            text: object
                                .get("thinking")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_owned(),
                            signature: object
                                .get("signature")
                                .and_then(Value::as_str)
                                .map(str::to_owned),
                            opaque_extensions: opaque_fields(
                                object,
                                &["type", "thinking", "signature"],
                            ),
                        });
                    }
                    Some("text") => message_content.push(ResponseContent::Text(ResponseText {
                        text: object
                            .get("text")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned(),
                        annotations: Vec::new(),
                        opaque_extensions: opaque_fields(object, &["type", "text"]),
                    })),
                    Some("tool_use") => {
                        flush_message_content(&mut items, &mut message_content);
                        items.push(ResponseItem::ToolCall {
                            id: object.get("id").and_then(Value::as_str).map(str::to_owned),
                            call_id: object
                                .get("id")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_owned(),
                            name: object
                                .get("name")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_owned(),
                            arguments: object.get("input").cloned().unwrap_or_else(|| json!({})),
                            status: Some("completed".to_owned()),
                            opaque_extensions: opaque_fields(
                                object,
                                &["type", "id", "name", "input"],
                            ),
                        });
                    }
                    _ => {
                        flush_message_content(&mut items, &mut message_content);
                        items.push(ResponseItem::Opaque(block.clone()));
                    }
                }
            }
        }
        flush_message_content(&mut items, &mut message_content);
        let stop_reason = match message.get("stop_reason").and_then(Value::as_str) {
            Some("tool_use") => StopReason::ToolUse,
            Some("max_tokens") => StopReason::MaxTokens,
            Some("end_turn" | "stop_sequence") => StopReason::Stop,
            _ => StopReason::Other,
        };
        Ok(Self {
            id,
            model,
            created_at: None,
            status: status_for_stop(stop_reason).to_owned(),
            incomplete_details: incomplete_details_for_stop(stop_reason),
            error: None,
            termination: ResponseTermination {
                reason: stop_reason,
                protocol_reason: message
                    .get("stop_reason")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                stop_sequence: message.get("stop_sequence").cloned(),
            },
            items,
            usage: parse_messages_usage(message.get("usage")),
            opaque_extensions: opaque_fields(
                message,
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
        })
    }

    fn parse_responses(body: &Value) -> Result<Self, ConversionError> {
        let response = body
            .as_object()
            .ok_or_else(|| ConversionError::invalid("Responses response is not an object"))?;
        let mut items = Vec::new();
        if let Some(output) = response.get("output").and_then(Value::as_array) {
            for item in output {
                let Some(object) = item.as_object() else {
                    items.push(ResponseItem::Opaque(item.clone()));
                    continue;
                };
                match object.get("type").and_then(Value::as_str) {
                    Some("reasoning") => items.push(ResponseItem::Reasoning {
                        id: object.get("id").and_then(Value::as_str).map(str::to_owned),
                        status: object
                            .get("status")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        text: object
                            .get("summary")
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                            .filter_map(|summary| summary.get("text").and_then(Value::as_str))
                            .collect::<Vec<_>>()
                            .join("\n"),
                        signature: None,
                        opaque_extensions: opaque_fields(
                            object,
                            &["id", "type", "status", "summary"],
                        ),
                    }),
                    Some("message") => {
                        let content = object
                            .get("content")
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                            .map(|part| {
                                let Some(part_object) = part.as_object() else {
                                    return ResponseContent::Opaque(part.clone());
                                };
                                if part_object.get("type").and_then(Value::as_str)
                                    != Some("output_text")
                                {
                                    return ResponseContent::Opaque(part.clone());
                                }
                                ResponseContent::Text(ResponseText {
                                    text: part_object
                                        .get("text")
                                        .and_then(Value::as_str)
                                        .unwrap_or_default()
                                        .to_owned(),
                                    annotations: part_object
                                        .get("annotations")
                                        .and_then(Value::as_array)
                                        .cloned()
                                        .unwrap_or_default(),
                                    opaque_extensions: opaque_fields(
                                        part_object,
                                        &["type", "text", "annotations"],
                                    ),
                                })
                            })
                            .collect();
                        items.push(ResponseItem::Message {
                            id: object.get("id").and_then(Value::as_str).map(str::to_owned),
                            status: object
                                .get("status")
                                .and_then(Value::as_str)
                                .map(str::to_owned),
                            content,
                            opaque_extensions: opaque_fields(
                                object,
                                &["id", "type", "status", "role", "content"],
                            ),
                        });
                    }
                    Some("function_call") => items.push(ResponseItem::ToolCall {
                        id: object.get("id").and_then(Value::as_str).map(str::to_owned),
                        call_id: object
                            .get("call_id")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned(),
                        name: object
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned(),
                        arguments: object
                            .get("arguments")
                            .cloned()
                            .unwrap_or_else(|| Value::String("{}".to_owned())),
                        status: object
                            .get("status")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        opaque_extensions: opaque_fields(
                            object,
                            &["id", "type", "status", "call_id", "name", "arguments"],
                        ),
                    }),
                    _ => items.push(ResponseItem::Opaque(item.clone())),
                }
            }
        }
        let status = response
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("completed")
            .to_owned();
        let incomplete_details = response.get("incomplete_details").cloned();
        let termination = responses_termination(&status, incomplete_details.as_ref(), &items);
        Ok(Self {
            id: string_field(response, "id"),
            model: string_field(response, "model"),
            created_at: response.get("created_at").cloned(),
            status,
            incomplete_details,
            error: response.get("error").cloned(),
            termination,
            items,
            usage: parse_responses_usage(response.get("usage")),
            opaque_extensions: opaque_fields(
                response,
                &[
                    "id",
                    "object",
                    "created_at",
                    "status",
                    "incomplete_details",
                    "error",
                    "model",
                    "output",
                    "usage",
                ],
            ),
        })
    }

    fn render_messages(&self) -> Value {
        let mut content = Vec::new();
        let mut saw_tool_call = false;
        for item in &self.items {
            match item {
                ResponseItem::Reasoning {
                    text,
                    signature,
                    opaque_extensions,
                    ..
                } if !text.trim().is_empty() => {
                    let mut block = opaque_extensions.clone();
                    block.insert("type".to_owned(), json!("thinking"));
                    block.insert("thinking".to_owned(), json!(text));
                    block.insert(
                        "signature".to_owned(),
                        json!(signature.as_deref().unwrap_or_default()),
                    );
                    content.push(Value::Object(block));
                }
                ResponseItem::Message { content: parts, .. } => {
                    for part in parts {
                        if let ResponseContent::Text(part) = part {
                            let mut block = part.opaque_extensions.clone();
                            block.insert("type".to_owned(), json!("text"));
                            block.insert("text".to_owned(), json!(part.text));
                            content.push(Value::Object(block));
                            if !part.annotations.is_empty() {
                                if let Some(sources) = format_url_citations(Some(&Value::Array(
                                    part.annotations.clone(),
                                ))) {
                                    content.push(json!({"type":"text", "text":sources}));
                                }
                            }
                        }
                    }
                }
                ResponseItem::ToolCall {
                    call_id,
                    name,
                    arguments,
                    opaque_extensions,
                    ..
                } => {
                    saw_tool_call = true;
                    let mut block = opaque_extensions.clone();
                    block.insert("type".to_owned(), json!("tool_use"));
                    block.insert("id".to_owned(), json!(call_id));
                    block.insert("name".to_owned(), json!(name));
                    block.insert("input".to_owned(), arguments_as_object(arguments));
                    content.push(Value::Object(block));
                }
                ResponseItem::Opaque(_) | ResponseItem::Reasoning { .. } => {}
            }
        }
        let stop_reason = match self.termination.reason {
            StopReason::ToolUse => "tool_use",
            StopReason::MaxTokens => "max_tokens",
            StopReason::ContentFilter => "refusal",
            StopReason::Stop | StopReason::Other if saw_tool_call => "tool_use",
            StopReason::Stop | StopReason::Other => "end_turn",
        };
        let mut out = Map::new();
        out.insert("id".to_owned(), json!(synth_message_id(&self.id)));
        out.insert("type".to_owned(), json!("message"));
        out.insert("role".to_owned(), json!("assistant"));
        out.insert("model".to_owned(), json!(self.model));
        out.insert("content".to_owned(), Value::Array(content));
        out.insert("stop_reason".to_owned(), json!(stop_reason));
        out.insert(
            "stop_sequence".to_owned(),
            self.termination
                .stop_sequence
                .clone()
                .unwrap_or(Value::Null),
        );
        out.insert("usage".to_owned(), self.usage.render_messages());
        Value::Object(out)
    }

    fn render_responses(&self) -> Value {
        let response_id = synth_response_id(&self.id);
        let suffix = response_id.strip_prefix("resp_").unwrap_or(&response_id);
        let mut output = Vec::new();
        for (index, item) in self.items.iter().enumerate() {
            match item {
                ResponseItem::Reasoning {
                    id,
                    status,
                    text,
                    opaque_extensions,
                    ..
                } if !text.trim().is_empty() => {
                    let mut rendered = opaque_extensions.clone();
                    rendered.insert(
                        "id".to_owned(),
                        json!(id.clone().unwrap_or_else(|| format!("rs_{suffix}"))),
                    );
                    rendered.insert("type".to_owned(), json!("reasoning"));
                    rendered.insert(
                        "status".to_owned(),
                        json!(status.as_deref().unwrap_or("completed")),
                    );
                    rendered.insert(
                        "summary".to_owned(),
                        json!([{"type":"summary_text", "text":text}]),
                    );
                    output.push(Value::Object(rendered));
                }
                ResponseItem::Message {
                    id,
                    status,
                    content,
                    opaque_extensions,
                } => {
                    let mut rendered_content = Vec::new();
                    for part in content {
                        match part {
                            ResponseContent::Text(part) => {
                                let mut rendered = part.opaque_extensions.clone();
                                rendered.insert("type".to_owned(), json!("output_text"));
                                rendered.insert("text".to_owned(), json!(part.text));
                                rendered.insert(
                                    "annotations".to_owned(),
                                    Value::Array(part.annotations.clone()),
                                );
                                rendered_content.push(Value::Object(rendered));
                            }
                            ResponseContent::Opaque(value) => {
                                rendered_content.push(value.clone());
                            }
                        }
                    }
                    let mut rendered = opaque_extensions.clone();
                    rendered.insert(
                        "id".to_owned(),
                        json!(id.clone().unwrap_or_else(|| format!("msg_{suffix}"))),
                    );
                    rendered.insert("type".to_owned(), json!("message"));
                    rendered.insert(
                        "status".to_owned(),
                        json!(status.as_deref().unwrap_or("completed")),
                    );
                    rendered.insert("role".to_owned(), json!("assistant"));
                    rendered.insert("content".to_owned(), Value::Array(rendered_content));
                    output.push(Value::Object(rendered));
                }
                ResponseItem::ToolCall {
                    id,
                    call_id,
                    name,
                    arguments,
                    status,
                    opaque_extensions,
                } => {
                    let mut rendered = opaque_extensions.clone();
                    rendered.insert(
                        "id".to_owned(),
                        json!(id.clone().unwrap_or_else(|| format!("fc_{suffix}_{index}"))),
                    );
                    rendered.insert("type".to_owned(), json!("function_call"));
                    rendered.insert(
                        "status".to_owned(),
                        json!(status.as_deref().unwrap_or("completed")),
                    );
                    rendered.insert("call_id".to_owned(), json!(call_id));
                    rendered.insert("name".to_owned(), json!(name));
                    rendered.insert(
                        "arguments".to_owned(),
                        json!(arguments_as_string(arguments)),
                    );
                    output.push(Value::Object(rendered));
                }
                ResponseItem::Opaque(value) => output.push(value.clone()),
                ResponseItem::Reasoning { .. } => {}
            }
        }
        let mut out = self.opaque_extensions.clone();
        out.insert("id".to_owned(), json!(response_id));
        out.insert("object".to_owned(), json!("response"));
        if let Some(created_at) = &self.created_at {
            out.insert("created_at".to_owned(), created_at.clone());
        }
        out.insert("status".to_owned(), json!(self.status));
        out.insert(
            "incomplete_details".to_owned(),
            self.incomplete_details.clone().unwrap_or(Value::Null),
        );
        out.insert(
            "error".to_owned(),
            self.error.clone().unwrap_or(Value::Null),
        );
        out.insert("model".to_owned(), json!(self.model));
        out.insert("output".to_owned(), Value::Array(output));
        out.insert("usage".to_owned(), self.usage.render_responses());
        Value::Object(out)
    }
}

fn flush_message_content(items: &mut Vec<ResponseItem>, content: &mut Vec<ResponseContent>) {
    if content.is_empty() {
        return;
    }
    items.push(ResponseItem::Message {
        id: None,
        status: Some("completed".to_owned()),
        content: std::mem::take(content),
        opaque_extensions: Map::new(),
    });
}

impl ResponseUsage {
    fn render_messages(&self) -> Value {
        let mut out = self.opaque_extensions.clone();
        insert_u64(&mut out, "input_tokens", self.input_tokens);
        insert_u64(&mut out, "output_tokens", self.output_tokens);
        insert_u64(&mut out, "cache_read_input_tokens", self.cached_tokens);
        insert_u64(
            &mut out,
            "cache_creation_input_tokens",
            self.cache_write_tokens,
        );
        if let Some(reasoning) = self.reasoning_tokens {
            out.insert(
                "output_tokens_details".to_owned(),
                json!({"thinking_tokens":reasoning}),
            );
        }
        Value::Object(out)
    }

    fn render_responses(&self) -> Value {
        let mut out = self.opaque_extensions.clone();
        insert_u64(&mut out, "input_tokens", self.input_tokens);
        insert_u64(&mut out, "output_tokens", self.output_tokens);
        insert_u64(&mut out, "total_tokens", self.total_tokens);
        let mut input_details = Map::new();
        insert_u64(&mut input_details, "cached_tokens", self.cached_tokens);
        insert_u64(
            &mut input_details,
            "cache_write_tokens",
            self.cache_write_tokens,
        );
        insert_u64(
            &mut input_details,
            "cache_miss_tokens",
            self.cache_miss_tokens,
        );
        if !input_details.is_empty() {
            out.insert(
                "input_tokens_details".to_owned(),
                Value::Object(input_details),
            );
        }
        if let Some(reasoning) = self.reasoning_tokens {
            out.insert(
                "output_tokens_details".to_owned(),
                json!({"reasoning_tokens":reasoning}),
            );
        }
        Value::Object(out)
    }
}

fn parse_messages_usage(usage: Option<&Value>) -> ResponseUsage {
    let usage = usage.and_then(Value::as_object);
    ResponseUsage {
        input_tokens: nested_u64(usage, "input_tokens"),
        output_tokens: nested_u64(usage, "output_tokens"),
        total_tokens: None,
        cached_tokens: nested_u64(usage, "cache_read_input_tokens"),
        cache_write_tokens: nested_u64(usage, "cache_creation_input_tokens"),
        cache_miss_tokens: None,
        reasoning_tokens: usage
            .and_then(|usage| usage.get("output_tokens_details"))
            .and_then(Value::as_object)
            .and_then(|details| details.get("thinking_tokens"))
            .and_then(Value::as_u64),
        opaque_extensions: usage
            .map(|usage| {
                opaque_fields(
                    usage,
                    &[
                        "input_tokens",
                        "output_tokens",
                        "cache_read_input_tokens",
                        "cache_creation_input_tokens",
                        "output_tokens_details",
                    ],
                )
            })
            .unwrap_or_default(),
    }
}

fn parse_responses_usage(usage: Option<&Value>) -> ResponseUsage {
    let usage = usage.and_then(Value::as_object);
    let input_details = usage
        .and_then(|usage| usage.get("input_tokens_details"))
        .and_then(Value::as_object);
    let output_details = usage
        .and_then(|usage| usage.get("output_tokens_details"))
        .and_then(Value::as_object);
    let input_tokens = nested_u64(usage, "input_tokens");
    let output_tokens = nested_u64(usage, "output_tokens");
    ResponseUsage {
        input_tokens,
        output_tokens,
        total_tokens: nested_u64(usage, "total_tokens")
            .or_else(|| sum_tokens(input_tokens, output_tokens)),
        cached_tokens: nested_u64(input_details, "cached_tokens"),
        cache_write_tokens: nested_u64(input_details, "cache_write_tokens"),
        cache_miss_tokens: nested_u64(input_details, "cache_miss_tokens"),
        reasoning_tokens: nested_u64(output_details, "reasoning_tokens"),
        opaque_extensions: usage
            .map(|usage| {
                opaque_fields(
                    usage,
                    &[
                        "input_tokens",
                        "output_tokens",
                        "total_tokens",
                        "input_tokens_details",
                        "output_tokens_details",
                    ],
                )
            })
            .unwrap_or_default(),
    }
}

fn opaque_fields(object: &Map<String, Value>, structural: &[&str]) -> Map<String, Value> {
    object
        .iter()
        .filter(|(key, _)| !structural.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn string_field(object: &Map<String, Value>, field: &str) -> String {
    object
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn nested_u64(object: Option<&Map<String, Value>>, field: &str) -> Option<u64> {
    object
        .and_then(|object| object.get(field))
        .and_then(Value::as_u64)
}

fn sum_tokens(input_tokens: Option<u64>, output_tokens: Option<u64>) -> Option<u64> {
    input_tokens?.checked_add(output_tokens?)
}

fn insert_u64(object: &mut Map<String, Value>, field: &str, value: Option<u64>) {
    if let Some(value) = value {
        object.insert(field.to_owned(), json!(value));
    }
}

fn arguments_as_object(arguments: &Value) -> Value {
    match arguments {
        Value::String(arguments) => serde_json::from_str(arguments)
            .ok()
            .filter(Value::is_object)
            .unwrap_or_else(|| json!({})),
        Value::Object(_) => arguments.clone(),
        _ => json!({}),
    }
}

fn arguments_as_string(arguments: &Value) -> String {
    match arguments {
        Value::String(arguments) => arguments.clone(),
        value => serde_json::to_string(value).unwrap_or_else(|_| "{}".to_owned()),
    }
}

fn synth_message_id(id: &str) -> String {
    if id.starts_with("msg_") {
        id.to_owned()
    } else {
        format!("msg_{id}")
    }
}

fn synth_response_id(id: &str) -> String {
    if id.starts_with("resp_") {
        id.to_owned()
    } else {
        format!("resp_{id}")
    }
}

fn status_for_stop(reason: StopReason) -> &'static str {
    match reason {
        StopReason::MaxTokens | StopReason::ContentFilter => "incomplete",
        _ => "completed",
    }
}

fn incomplete_details_for_stop(reason: StopReason) -> Option<Value> {
    match reason {
        StopReason::MaxTokens => Some(json!({"reason":"max_output_tokens"})),
        StopReason::ContentFilter => Some(json!({"reason":"content_filter"})),
        _ => None,
    }
}

fn responses_termination(
    status: &str,
    incomplete_details: Option<&Value>,
    items: &[ResponseItem],
) -> ResponseTermination {
    let protocol_reason = incomplete_details
        .and_then(|details| details.get("reason"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| (status != "completed").then(|| status.to_owned()));
    let reason = match protocol_reason.as_deref() {
        Some("max_output_tokens" | "max_tokens") => StopReason::MaxTokens,
        Some("content_filter" | "refusal") => StopReason::ContentFilter,
        Some("incomplete") => StopReason::MaxTokens,
        Some(_) => StopReason::Other,
        None if items
            .iter()
            .any(|item| matches!(item, ResponseItem::ToolCall { .. })) =>
        {
            StopReason::ToolUse
        }
        None => StopReason::Stop,
    };
    ResponseTermination {
        reason,
        protocol_reason,
        stop_sequence: None,
    }
}

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::dialect::ModelCapabilities;

    fn convert(source: Protocol, target: Protocol, body: Value) -> Value {
        ResponseIr::parse(source, &body)
            .unwrap()
            .render(target)
            .unwrap()
    }

    #[test]
    fn chat_to_messages_preserves_reasoning_tools_usage_and_citations() {
        let output = convert(
            Protocol::OpenAiChatCompletions,
            Protocol::AnthropicMessages,
            json!({
                "id": "chat-1",
                "model": "m",
                "choices": [{
                    "message": {
                        "content": "answer",
                        "reasoning_content": "thought",
                        "annotations": [{"type":"url_citation", "url":"https://example.com", "title":"Source"}],
                        "tool_calls": [{"id":"call-1", "function":{"name":"lookup", "arguments":"{\"q\":1}"}}]
                    },
                    "finish_reason": "tool_calls"
                }],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 5,
                    "prompt_tokens_details": {"cached_tokens": 3},
                    "completion_tokens_details": {"reasoning_tokens": 2}
                }
            }),
        );
        assert_eq!(output["id"], "msg_chat-1");
        assert_eq!(output["stop_reason"], "tool_use");
        assert_eq!(output["content"][0]["type"], "thinking");
        assert_eq!(output["content"][1]["text"], "answer");
        assert!(output["content"][2]["text"]
            .as_str()
            .unwrap()
            .contains("Source: https://example.com"));
        assert_eq!(output["content"][3]["input"]["q"], 1);
        assert_eq!(output["usage"]["cache_read_input_tokens"], 3);
        assert_eq!(
            output["usage"]["output_tokens_details"]["thinking_tokens"],
            2
        );
    }

    #[test]
    fn chat_to_responses_renders_neutral_items() {
        let output = convert(
            Protocol::OpenAiChatCompletions,
            Protocol::OpenAiResponses,
            json!({
                "id": "chat-1",
                "model": "m",
                "choices": [{
                    "message": {
                        "content": "answer",
                        "reasoning_content": "thought",
                        "tool_calls": [{"id":"call-1", "function":{"name":"lookup", "arguments":"{}"}}]
                    },
                    "finish_reason": "tool_calls"
                }],
                "usage": {"prompt_tokens": 2, "completion_tokens": 3}
            }),
        );
        assert_eq!(output["id"], "resp_chat-1");
        assert_eq!(output["output"][0]["type"], "reasoning");
        assert_eq!(output["output"][1]["type"], "message");
        assert_eq!(output["output"][2]["type"], "function_call");
        assert_eq!(output["usage"]["total_tokens"], 5);
    }

    #[test]
    fn messages_to_responses_preserves_semantics() {
        let output = convert(
            Protocol::AnthropicMessages,
            Protocol::OpenAiResponses,
            json!({
                "id": "msg-1",
                "model": "m",
                "content": [
                    {"type":"thinking", "thinking":"thought", "signature":"sig"},
                    {"type":"text", "text":"answer"},
                    {"type":"tool_use", "id":"call-1", "name":"lookup", "input":{"q":1}}
                ],
                "stop_reason": "tool_use",
                "usage": {"input_tokens": 4, "output_tokens": 6, "cache_read_input_tokens": 2}
            }),
        );
        assert_eq!(output["id"], "resp_msg-1");
        assert_eq!(output["output"][0]["summary"][0]["text"], "thought");
        assert_eq!(output["output"][1]["content"][0]["text"], "answer");
        assert_eq!(output["output"][2]["arguments"], "{\"q\":1}");
        assert_eq!(output["usage"]["input_tokens_details"]["cached_tokens"], 2);
        assert!(output["usage"].get("total_tokens").is_none());
    }

    #[test]
    fn messages_to_responses_preserves_interleaved_block_order() {
        let output = convert(
            Protocol::AnthropicMessages,
            Protocol::OpenAiResponses,
            json!({
                "id": "msg-1",
                "model": "m",
                "content": [
                    {"type":"text", "text":"before"},
                    {"type":"tool_use", "id":"call-1", "name":"lookup", "input":{}},
                    {"type":"vendor_block", "payload":1},
                    {"type":"thinking", "thinking":"later", "signature":"sig"},
                    {"type":"text", "text":"after"}
                ],
                "stop_reason": "tool_use",
                "usage": {"input_tokens": 1, "output_tokens": 2}
            }),
        );
        let output = output["output"].as_array().unwrap();
        assert_eq!(output[0]["content"][0]["text"], "before");
        assert_eq!(output[1]["type"], "function_call");
        assert_eq!(output[2]["type"], "vendor_block");
        assert_eq!(output[3]["type"], "reasoning");
        assert_eq!(output[4]["content"][0]["text"], "after");
    }

    #[test]
    fn responses_to_messages_preserves_semantics() {
        let output = convert(
            Protocol::OpenAiResponses,
            Protocol::AnthropicMessages,
            json!({
                "id": "resp-1",
                "model": "m",
                "status": "incomplete",
                "output": [
                    {"type":"reasoning", "summary":[{"type":"summary_text", "text":"thought"}]},
                    {"type":"message", "content":[{"type":"output_text", "text":"answer", "annotations":[]}]},
                    {"type":"function_call", "call_id":"call-1", "name":"lookup", "arguments":"{\"q\":1}"}
                ],
                "usage": {"input_tokens": 4, "output_tokens": 6, "input_tokens_details":{"cached_tokens":2}}
            }),
        );
        assert_eq!(output["id"], "msg_resp-1");
        assert_eq!(output["stop_reason"], "max_tokens");
        assert_eq!(output["content"][0]["thinking"], "thought");
        assert_eq!(output["content"][1]["text"], "answer");
        assert_eq!(output["content"][2]["input"]["q"], 1);
    }

    #[test]
    fn responses_roundtrip_preserves_opaque_fields() {
        let output = convert(
            Protocol::OpenAiResponses,
            Protocol::OpenAiResponses,
            json!({
                "id": "third-party-id",
                "object": "response",
                "status": "completed",
                "model": "m",
                "output": [],
                "usage": {"input_tokens": 2, "output_tokens": 3, "x_details":{"cached":true}},
                "conversation": "conv-1",
                "x_provider": {"trace":"t"}
            }),
        );
        assert_eq!(output["id"], "resp_third-party-id");
        assert_eq!(output["usage"]["total_tokens"], 5);
        assert_eq!(output["usage"]["x_details"]["cached"], true);
        assert_eq!(output["conversation"], "conv-1");
        assert_eq!(output["x_provider"]["trace"], "t");
    }

    #[test]
    fn responses_message_preserves_unknown_content_parts() {
        let body = json!({
            "id": "resp-1",
            "model": "m",
            "status": "completed",
            "output": [{
                "id": "msg-1",
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [
                    {"type":"output_text", "text":"before", "annotations":[]},
                    {"type":"refusal", "refusal":"not allowed", "vendor_code":7},
                    {"type":"output_text", "text":"after", "annotations":[]}
                ]
            }],
            "usage": {"input_tokens":1, "output_tokens":2}
        });
        let response = ResponseIr::parse(Protocol::OpenAiResponses, &body).unwrap();

        let responses = response.render(Protocol::OpenAiResponses).unwrap();
        assert_eq!(responses["output"][0]["content"][0]["text"], "before");
        assert_eq!(responses["output"][0]["content"][1]["type"], "refusal");
        assert_eq!(responses["output"][0]["content"][1]["vendor_code"], 7);
        assert_eq!(responses["output"][0]["content"][2]["text"], "after");

        let messages = response.render(Protocol::AnthropicMessages).unwrap();
        let content = messages["content"].as_array().unwrap();
        assert_eq!(content.len(), 2);
        assert_eq!(content[0]["text"], "before");
        assert_eq!(content[1]["text"], "after");
    }

    #[test]
    fn response_ir_exposes_normalized_and_protocol_stop_reasons() {
        let response = ResponseIr::parse(
            Protocol::OpenAiResponses,
            &json!({
                "id": "resp-1",
                "status": "incomplete",
                "incomplete_details": {"reason": "max_output_tokens"},
                "output": []
            }),
        )
        .unwrap();

        assert_eq!(response.stop_reason(), StopReason::MaxTokens);
        assert_eq!(response.protocol_stop_reason(), Some("max_output_tokens"));
    }

    #[test]
    fn only_terminal_non_error_responses_are_continuable() {
        for (status, error, expected) in [
            ("completed", Value::Null, true),
            ("incomplete", Value::Null, true),
            ("in_progress", Value::Null, false),
            ("failed", json!({"message":"boom"}), false),
            ("completed", json!({"message":"boom"}), false),
        ] {
            let response = ResponseIr::parse(
                Protocol::OpenAiResponses,
                &json!({
                    "id": "resp_1",
                    "model": "m",
                    "status": status,
                    "error": error,
                    "output": []
                }),
            )
            .unwrap();
            assert_eq!(response.is_continuable(), expected, "status={status}");
        }
    }

    #[test]
    fn response_ir_appends_protocol_neutral_continuation_items() {
        let mut request = RequestIr::parse(
            Protocol::OpenAiResponses,
            &json!({"model": "m", "input": "hi"}),
        )
        .unwrap();
        let response = ResponseIr::parse(
            Protocol::OpenAiResponses,
            &json!({
                "id": "resp-1",
                "model": "m",
                "status": "completed",
                "output": [
                    {"id":"rs_1", "type":"reasoning", "status":"completed", "summary":[{"type":"summary_text", "text":"thought"}]},
                    {"id":"msg_1", "type":"message", "status":"completed", "role":"assistant", "content":[{"type":"output_text", "text":"answer", "annotations":[]}]},
                    {"id":"search_1", "type":"web_search_call", "status":"completed", "vendor_trace":"kept"},
                    {"id":"fc_1", "type":"function_call", "status":"completed", "call_id":"call_1", "name":"lookup", "arguments":"{\"q\":1}", "x_provider":"kept"}
                ]
            }),
        )
        .unwrap();

        response.append_to_request(&mut request);
        let rendered = request
            .render_with_report(Protocol::OpenAiResponses, &ModelCapabilities::default())
            .unwrap()
            .0;
        let input = rendered["input"].as_array().unwrap();
        assert_eq!(input[1]["id"], "rs_1");
        assert_eq!(input[1]["status"], "completed");
        assert_eq!(input[1]["summary"][0]["text"], "thought");
        assert_eq!(input[2]["id"], "msg_1");
        assert_eq!(input[2]["status"], "completed");
        assert_eq!(input[2]["content"][0]["text"], "answer");
        assert_eq!(input[3]["type"], "web_search_call");
        assert_eq!(input[3]["vendor_trace"], "kept");
        assert_eq!(input[4]["id"], "fc_1");
        assert_eq!(input[4]["status"], "completed");
        assert_eq!(input[4]["x_provider"], "kept");
    }
}
