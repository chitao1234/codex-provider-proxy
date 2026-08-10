use crate::{
    ir::{CanonicalResponseEvent, ContentPart, ItemKind, StopReason},
    sse,
};
use bytes::Bytes;
use serde_json::json;

#[derive(Default)]
pub struct Renderer {
    started: bool,
    finished: bool,
}
impl Renderer {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn on_event(&mut self, event: &CanonicalResponseEvent, out: &mut Vec<Bytes>) {
        if self.finished {
            return;
        }
        match event {
  CanonicalResponseEvent::StreamStarted { response_id, model } => { self.started=true; self.emit("message_start", json!({"type":"message_start","message":{"id":response_id,"type":"message","role":"assistant","model":model,"content":[],"stop_reason":null,"usage":{}}}), out); }
  CanonicalResponseEvent::ItemStarted { item_id, item } => { let (index, block)=match item { ItemKind::Reasoning=>(0,json!({"type":"thinking","thinking":"","signature":null})), ItemKind::ToolCall=>(0,json!({"type":"tool_use","id":item_id,"name":"","input":{}})), _=>(0,json!({"type":"text","text":""})) }; self.emit("content_block_start", json!({"type":"content_block_start","index":index,"content_block":block}), out); }
  CanonicalResponseEvent::ContentDelta { content, .. } => { let (kind,delta)=match content { ContentPart::Reasoning{text,..}=>("thinking_delta",json!({"type":"thinking_delta","thinking":text})), ContentPart::Text(text)=>("text_delta",json!({"type":"text_delta","text":text})), ContentPart::Refusal(text)=>("text_delta",json!({"type":"text_delta","text":text})), _=>return }; self.emit("content_block_delta", json!({"type":"content_block_delta","index":0,"delta":delta}), out); let _=kind; }
  CanonicalResponseEvent::ToolArgumentsDelta { arguments, .. } => self.emit("content_block_delta", json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":arguments}}), out),
  CanonicalResponseEvent::ResponseCompleted { stop_reason, .. } => { self.finished=true; self.emit("message_delta", json!({"type":"message_delta","delta":{"stop_reason":anthropic_reason(stop_reason)},"usage":{}}), out); self.emit("message_stop", json!({"type":"message_stop"}), out); }
  CanonicalResponseEvent::StreamError { message, .. } => { self.finished=true; self.emit("error", json!({"type":"error","error":{"type":"api_error","message":message}}), out); }
  CanonicalResponseEvent::UsageUpdated(_) | CanonicalResponseEvent::ItemMetadataUpdated { .. } | CanonicalResponseEvent::ItemCompleted { .. } | CanonicalResponseEvent::ToolCallStarted { .. } | CanonicalResponseEvent::ProtocolExtension(_) => {}
 }
    }
    pub fn finish(&mut self, _: &mut Vec<Bytes>) {}
    fn emit(&self, kind: &str, data: serde_json::Value, out: &mut Vec<Bytes>) {
        out.push(sse::encode_sse_event(kind, &data));
    }
}
fn anthropic_reason(reason: &StopReason) -> &'static str {
    match reason {
        StopReason::ToolUse => "tool_use",
        StopReason::MaxTokens => "max_tokens",
        _ => "end_turn",
    }
}
