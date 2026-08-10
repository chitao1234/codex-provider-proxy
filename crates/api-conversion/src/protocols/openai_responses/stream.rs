use bytes::Bytes;
use serde_json::json;

use crate::{
    ir::{CanonicalResponseEvent, ContentPart, ItemKind, StopReason},
    sse,
};

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
