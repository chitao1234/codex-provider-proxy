use bytes::Bytes;
use serde_json::json;

use crate::{
    error::ConversionError,
    ir::{
        CanonicalResponseEvent, ContentPart, Extension, ExtensionScope, ItemKind, ReplayPolicy,
        StopReason, ToolCall, Usage,
    },
    sse,
};

#[derive(Default)]
pub struct Parser {
    finished: bool,
    started: bool,
    item_kinds: std::collections::BTreeMap<String, ItemKind>,
}

impl Parser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on_event(
        &mut self,
        event_type: &str,
        payload: &str,
        out: &mut Vec<CanonicalResponseEvent>,
    ) -> Result<(), ConversionError> {
        if self.finished {
            return Ok(());
        }
        let data: serde_json::Value = serde_json::from_str(payload)
            .map_err(|_| ConversionError::invalid("upstream SSE data is not valid JSON"))?;
        match event_type {
            "response.created" | "response.in_progress" => {
                if !self.started {
                    self.started = true;
                    let response = data.get("response");
                    out.push(CanonicalResponseEvent::StreamStarted {
                        response_id: response
                            .and_then(|response| response.get("id"))
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned),
                        model: response
                            .and_then(|response| response.get("model"))
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned),
                    });
                }
            }
            "response.output_item.added" => self.item_added(&data, out),
            "response.output_text.delta" => self.content_delta(&data, "delta", false, out),
            "response.refusal.delta" => self.content_delta(&data, "delta", true, out),
            "response.reasoning_text.delta" | "response.reasoning_summary_text.delta" => {
                if let Some(text) = data.get("delta").and_then(serde_json::Value::as_str) {
                    let item_id = item_id(&data).unwrap_or_else(|| "reasoning".to_owned());
                    self.ensure_item(&item_id, ItemKind::Reasoning, out);
                    out.push(CanonicalResponseEvent::ContentDelta {
                        item_id,
                        content: ContentPart::Reasoning {
                            text: text.to_owned(),
                            signature: None,
                        },
                    });
                }
            }
            "response.function_call_arguments.delta" => {
                if let (Some(item_id), Some(arguments)) = (
                    item_id(&data),
                    data.get("delta").and_then(serde_json::Value::as_str),
                ) {
                    out.push(CanonicalResponseEvent::ToolArgumentsDelta {
                        item_id,
                        arguments: arguments.to_owned(),
                    });
                }
            }
            "response.output_item.done" => {
                if let Some(item) = data.get("item") {
                    self.item_done(item, out);
                }
                if let Some(item_id) = item_id(&data).or_else(|| {
                    data.get("item")
                        .and_then(|item| item.get("id"))
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                }) {
                    out.push(CanonicalResponseEvent::ItemCompleted { item_id });
                }
            }
            "response.completed" | "response.incomplete" => {
                self.finished = true;
                let response = data.get("response");
                if let Some(usage) = response.and_then(|response| response.get("usage")) {
                    out.push(CanonicalResponseEvent::UsageUpdated(parse_usage(usage)));
                }
                let status = response
                    .and_then(|response| response.get("status"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(if event_type == "response.incomplete" {
                        "incomplete"
                    } else {
                        "completed"
                    });
                let original = response
                    .and_then(|response| response.get("incomplete_details"))
                    .and_then(|details| details.get("reason"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(status)
                    .to_owned();
                out.push(CanonicalResponseEvent::ResponseCompleted {
                    stop_reason: if status == "incomplete" {
                        StopReason::MaxTokens
                    } else {
                        StopReason::Natural
                    },
                    original_reason: Some(original),
                });
            }
            "error" => {
                self.finished = true;
                let error = data.get("error");
                out.push(CanonicalResponseEvent::StreamError {
                    message: error
                        .and_then(|error| error.get("message"))
                        .or_else(|| data.get("message"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("upstream request failed")
                        .to_owned(),
                    code: error
                        .and_then(|error| error.get("code"))
                        .or_else(|| data.get("code"))
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned),
                });
            }
            "response.content_part.added"
            | "response.content_part.done"
            | "response.output_text.done"
            | "response.refusal.done"
            | "response.reasoning_text.done"
            | "response.reasoning_summary_part.added"
            | "response.reasoning_summary_text.done"
            | "response.function_call_arguments.done" => {}
            _ => out.push(protocol_extension(event_type, data)),
        }
        Ok(())
    }

    pub fn finish(&mut self, _out: &mut Vec<CanonicalResponseEvent>) {
        self.finished = true;
    }

    fn item_added(&mut self, data: &serde_json::Value, out: &mut Vec<CanonicalResponseEvent>) {
        let Some(item) = data.get("item") else { return };
        let id = item
            .get("id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .or_else(|| item_id(data))
            .unwrap_or_else(|| format!("item:{}", self.item_kinds.len()));
        match item.get("type").and_then(serde_json::Value::as_str) {
            Some("reasoning") => self.ensure_item(&id, ItemKind::Reasoning, out),
            Some("message") => self.ensure_item(&id, ItemKind::Message, out),
            Some("function_call" | "custom_tool_call") => {
                self.item_kinds.insert(id.clone(), ItemKind::ToolCall);
                out.push(CanonicalResponseEvent::ToolCallStarted {
                    item_id: id,
                    call: ToolCall {
                        id: item
                            .get("call_id")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned),
                        name: item
                            .get("name")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default()
                            .to_owned(),
                        arguments: String::new(),
                        status: item
                            .get("status")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned),
                        metadata: Default::default(),
                        function_metadata: Default::default(),
                        extensions: Vec::new(),
                    },
                });
            }
            Some(kind) => {
                self.item_kinds.insert(id.clone(), ItemKind::Unknown);
                out.push(CanonicalResponseEvent::OpaqueItemStarted {
                    item_id: id,
                    kind: kind.to_owned(),
                    payload: item.clone(),
                });
            }
            None => {}
        }
    }

    fn item_done(&mut self, item: &serde_json::Value, out: &mut Vec<CanonicalResponseEvent>) {
        let Some(id) = item.get("id").and_then(serde_json::Value::as_str) else {
            return;
        };
        if !self.item_kinds.contains_key(id) {
            let kind = item
                .get("type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown");
            self.item_kinds.insert(id.to_owned(), ItemKind::Unknown);
            out.push(CanonicalResponseEvent::OpaqueItemStarted {
                item_id: id.to_owned(),
                kind: kind.to_owned(),
                payload: item.clone(),
            });
        }
        if item.get("type").and_then(serde_json::Value::as_str) == Some("function_call") {
            if let Some(arguments) = item
                .get("arguments")
                .and_then(serde_json::Value::as_str)
                .filter(|arguments| !arguments.is_empty())
            {
                out.push(CanonicalResponseEvent::ToolArgumentsDelta {
                    item_id: id.to_owned(),
                    arguments: arguments.to_owned(),
                });
            }
        }
    }

    fn content_delta(
        &mut self,
        data: &serde_json::Value,
        field: &str,
        refusal: bool,
        out: &mut Vec<CanonicalResponseEvent>,
    ) {
        if let Some(text) = data.get(field).and_then(serde_json::Value::as_str) {
            let item_id = item_id(data).unwrap_or_else(|| {
                if refusal {
                    "refusal".to_owned()
                } else {
                    "message".to_owned()
                }
            });
            self.ensure_item(
                &item_id,
                if refusal {
                    ItemKind::Refusal
                } else {
                    ItemKind::Message
                },
                out,
            );
            out.push(CanonicalResponseEvent::ContentDelta {
                item_id,
                content: if refusal {
                    ContentPart::Refusal(text.to_owned())
                } else {
                    ContentPart::Text(text.to_owned())
                },
            });
        }
    }

    fn ensure_item(
        &mut self,
        item_id: &str,
        kind: ItemKind,
        out: &mut Vec<CanonicalResponseEvent>,
    ) {
        if self.item_kinds.insert(item_id.to_owned(), kind).is_none() {
            out.push(CanonicalResponseEvent::ItemStarted {
                item_id: item_id.to_owned(),
                item: kind,
            });
        }
    }
}

fn item_id(data: &serde_json::Value) -> Option<String> {
    data.get("item_id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

fn parse_usage(value: &serde_json::Value) -> Usage {
    let field = |name| value.get(name).and_then(serde_json::Value::as_u64);
    Usage {
        input_tokens: field("input_tokens"),
        output_tokens: field("output_tokens"),
        total_tokens: field("total_tokens"),
        cached_input_tokens: value
            .get("input_tokens_details")
            .and_then(|details| details.get("cached_tokens"))
            .and_then(serde_json::Value::as_u64),
    }
}

fn protocol_extension(kind: &str, value: serde_json::Value) -> CanonicalResponseEvent {
    CanonicalResponseEvent::ProtocolExtension(Extension {
        namespace: "openai.responses".to_owned(),
        key: kind.to_owned(),
        value,
        scope: ExtensionScope::Response,
        source_protocol: Some("openai_responses".to_owned()),
        provider: None,
        replay: ReplayPolicy::NeverReplay,
    })
}

#[derive(Default)]
pub struct Renderer {
    response_id: Option<String>,
    model: Option<String>,
    next_index: usize,
    items: std::collections::BTreeMap<String, usize>,
    finished: bool,
}

impl Renderer {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn response_id(&self) -> Option<&str> {
        self.response_id.as_deref()
    }
    pub fn on_event(&mut self, event: &CanonicalResponseEvent, out: &mut Vec<Bytes>) {
        if self.finished {
            return;
        }
        match event {
            CanonicalResponseEvent::StreamStarted { response_id, model } => {
                self.response_id = response_id.as_deref().map(synth_response_id);
                self.model.clone_from(model);
                let response = json!({"id": self.response_id, "object":"response", "model":model, "status":"in_progress", "output":[]});
                self.emit(
                    "response.created",
                    json!({"type":"response.created","response":response}),
                    out,
                );
                self.emit(
                    "response.in_progress",
                    json!({"type":"response.in_progress","response":response}),
                    out,
                );
            }
            CanonicalResponseEvent::ItemStarted { item_id, item } => {
                if self.items.contains_key(item_id) {
                    return;
                }
                let index = self.next_index;
                self.next_index += 1;
                self.items.insert(item_id.clone(), index);
                let (kind, payload) = match item {
                    ItemKind::Reasoning => (
                        "reasoning",
                        json!({"id":item_id,"type":"reasoning","status":"in_progress","summary":[]}),
                    ),
                    ItemKind::ToolCall => (
                        "function_call",
                        json!({"id":item_id,"type":"function_call","status":"in_progress","call_id":item_id,"name":"","arguments":""}),
                    ),
                    ItemKind::Refusal | ItemKind::Message => (
                        "message",
                        json!({"id":item_id,"type":"message","status":"in_progress","role":"assistant","content":[]}),
                    ),
                    _ => (
                        "unknown",
                        json!({"id":item_id,"type":"unknown","status":"in_progress"}),
                    ),
                };
                self.emit("response.output_item.added", json!({"type":"response.output_item.added","output_index":index,"item":payload}), out);
                let _ = kind;
            }
            CanonicalResponseEvent::OpaqueItemStarted { .. } => {}
            CanonicalResponseEvent::ContentDelta { item_id, content } => {
                let Some(index) = self.items.get(item_id).copied() else {
                    return;
                };
                match content { ContentPart::Text(delta) => self.emit("response.output_text.delta", json!({"type":"response.output_text.delta","item_id":item_id,"output_index":index,"content_index":0,"delta":delta}), out), ContentPart::Refusal(delta) => self.emit("response.refusal.delta", json!({"type":"response.refusal.delta","item_id":item_id,"output_index":index,"content_index":0,"delta":delta}), out), ContentPart::Reasoning { text, .. } => self.emit("response.reasoning_text.delta", json!({"type":"response.reasoning_text.delta","item_id":item_id,"output_index":index,"content_index":0,"delta":text}), out), _ => {} }
            }
            CanonicalResponseEvent::ToolCallStarted { item_id, call } => {
                self.on_event(
                    &CanonicalResponseEvent::ItemStarted {
                        item_id: item_id.clone(),
                        item: ItemKind::ToolCall,
                    },
                    out,
                );
                if let Some(index) = self.items.get(item_id).copied() {
                    self.emit("response.function_call_arguments.delta", json!({"type":"response.function_call_arguments.delta","item_id":item_id,"output_index":index,"delta":""}), out);
                    let _ = call;
                }
            }
            CanonicalResponseEvent::ToolArgumentsDelta { item_id, arguments } => {
                if let Some(index) = self.items.get(item_id).copied() {
                    self.emit("response.function_call_arguments.delta", json!({"type":"response.function_call_arguments.delta","item_id":item_id,"output_index":index,"delta":arguments}), out);
                }
            }
            CanonicalResponseEvent::ResponseCompleted { stop_reason, .. } => {
                self.finished = true;
                self.emit("response.completed", json!({"type":"response.completed","response":{"id":self.response_id,"object":"response","model":self.model,"status":"completed","output":[],"stop_reason":stop_reason_name(stop_reason)}}), out);
                self.emit("response.done", json!({"type":"response.done"}), out);
            }
            CanonicalResponseEvent::StreamError { message, code } => {
                self.finished = true;
                self.emit(
                    "error",
                    json!({"type":"error","error":{"message":message,"code":code}}),
                    out,
                );
            }
            CanonicalResponseEvent::UsageUpdated(_)
            | CanonicalResponseEvent::ItemMetadataUpdated { .. }
            | CanonicalResponseEvent::ItemCompleted { .. }
            | CanonicalResponseEvent::ProtocolExtension(_) => {}
        }
    }
    pub fn finish(&mut self, _out: &mut Vec<Bytes>) {}
    fn emit(&self, kind: &str, data: serde_json::Value, out: &mut Vec<Bytes>) {
        out.push(sse::encode_sse_event(kind, &data));
    }
}
fn synth_response_id(upstream_id: &str) -> String {
    if upstream_id.starts_with("resp_") {
        upstream_id.to_owned()
    } else {
        format!("resp_{upstream_id}")
    }
}
fn stop_reason_name(reason: &StopReason) -> &'static str {
    match reason {
        StopReason::Natural => "stop",
        StopReason::ToolUse => "tool_calls",
        StopReason::MaxTokens => "length",
        StopReason::ContentFilter => "content_filter",
        _ => "other",
    }
}
