//! Anthropic Messages SSE parser: upstream Messages SSE events into the semantic
//! `StreamEvent` IR.
//!
//! This is the counterpart of `MessagesRenderer`: it translates the upstream
//! protocol's content-block state machine (message_start, content_block_start/delta/
//! stop, message_delta, message_stop) into protocol-independent events, preserving
//! thinking signatures and tool-call increments.

use serde_json::{Map, Value};

use crate::chat::ChatUsage;
use crate::error::ConversionError;
use crate::stream::{StopReason, StreamEvent};

/// Accumulated tool-call state across upstream content blocks.
#[derive(Default)]
struct ToolAccumulator {
    arguments: String,
    /// Arguments already emitted as deltas (increments only).
    emitted_len: usize,
}

/// Parse upstream Anthropic Messages SSE events into `StreamEvent`s.
pub struct MessagesParser {
    started: bool,
    finished: bool,
    /// Upstream id/model captured from message_start.
    message_id: String,
    model: String,
    /// Tool call index -> accumulated state.
    tools: HashMap<usize, ToolAccumulator>,
    /// Anthropic content block index -> protocol-neutral tool call index.
    tool_blocks: HashMap<usize, usize>,
    /// Anthropic content block indices whose type is not modeled semantically.
    extension_blocks: HashSet<usize>,
    /// Next tool index to assign (Anthropic blocks have no index, use sequence).
    next_tool_index: usize,
    usage: ChatUsage,
    /// Whether message_delta already delivered usage (the final usage chunk).
    end_emitted: bool,
}

use std::collections::{HashMap, HashSet};

impl Default for MessagesParser {
    fn default() -> Self {
        Self::new()
    }
}

impl MessagesParser {
    pub fn new() -> Self {
        Self {
            started: false,
            finished: false,
            message_id: String::new(),
            model: String::new(),
            tools: HashMap::new(),
            tool_blocks: HashMap::new(),
            extension_blocks: HashSet::new(),
            next_tool_index: 0,
            usage: ChatUsage::default(),
            end_emitted: false,
        }
    }

    /// Process one upstream SSE event, appending semantic events to `out`.
    /// `event_type` is the SSE event name (e.g. "message_start"); `data` is the
    /// parsed JSON payload.
    pub fn on_event(
        &mut self,
        event_type: &str,
        data: &Value,
        out: &mut Vec<StreamEvent>,
    ) -> Result<(), ConversionError> {
        if self.finished {
            return Ok(());
        }
        match event_type {
            "message_start" => {
                self.started = true;
                let message = data.get("message").and_then(Value::as_object);
                self.message_id = message
                    .and_then(|m| m.get("id"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                self.model = message
                    .and_then(|m| m.get("model"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                // Initial usage from message_start (may be all zeros).
                if let Some(usage) = message.and_then(|m| m.get("usage")) {
                    self.usage = extract_anthropic_usage(usage);
                }
                out.push(StreamEvent::Start {
                    id: self.message_id.clone(),
                    model: self.model.clone(),
                });
            }
            "content_block_start" => {
                let block = data.get("content_block").and_then(Value::as_object);
                let block_type = block
                    .and_then(|b| b.get("type"))
                    .and_then(Value::as_str)
                    .unwrap_or("text");
                let index = data
                    .get("index")
                    .and_then(Value::as_u64)
                    .map(|i| i as usize)
                    .unwrap_or_default();
                match block_type {
                    "tool_use" => {
                        let id = block
                            .and_then(|b| b.get("id"))
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        let name = block
                            .and_then(|b| b.get("name"))
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        let tool_index = self.next_tool_index;
                        self.next_tool_index += 1;
                        self.tool_blocks.insert(index, tool_index);
                        self.tools.insert(tool_index, ToolAccumulator::default());
                        out.push(StreamEvent::ToolCallStart {
                            index: tool_index,
                            id,
                            name,
                            opaque_extensions: block
                                .map(|block| opaque_fields(block, &["type", "id", "name", "input"]))
                                .unwrap_or_default(),
                            function_extensions: Map::new(),
                        });
                    }
                    "text" | "thinking" => {}
                    _ => {
                        self.extension_blocks.insert(index);
                        out.push(messages_extension(event_type, data));
                    }
                }
            }
            "content_block_delta" => {
                let delta = data.get("delta").and_then(Value::as_object);
                match delta
                    .and_then(|delta| delta.get("type"))
                    .and_then(Value::as_str)
                {
                    Some("thinking_delta") => {
                        if let Some(text) = delta
                            .and_then(|delta| delta.get("thinking"))
                            .and_then(Value::as_str)
                        {
                            out.push(StreamEvent::ReasoningDelta {
                                text: text.to_owned(),
                                signature: None,
                            });
                        }
                    }
                    Some("signature_delta") => {
                        if let Some(signature) = delta
                            .and_then(|delta| delta.get("signature"))
                            .and_then(Value::as_str)
                        {
                            out.push(StreamEvent::ReasoningDelta {
                                text: String::new(),
                                signature: Some(signature.to_owned()),
                            });
                        }
                    }
                    Some("input_json_delta") => {
                        let partial = delta
                            .and_then(|delta| delta.get("partial_json"))
                            .and_then(Value::as_str);
                        let tool_index = data
                            .get("index")
                            .and_then(Value::as_u64)
                            .and_then(|index| self.tool_blocks.get(&(index as usize)))
                            .copied();
                        if let (Some(partial), Some(tool_index)) = (partial, tool_index) {
                            let acc = self.tools.get_mut(&tool_index).expect("tool exists");
                            acc.arguments.push_str(partial);
                            if acc.emitted_len < acc.arguments.len() {
                                let increment = acc.arguments[acc.emitted_len..].to_string();
                                acc.emitted_len = acc.arguments.len();
                                if !increment.is_empty() {
                                    out.push(StreamEvent::ToolCallArgsDelta {
                                        index: tool_index,
                                        args: increment,
                                    });
                                }
                            }
                        }
                    }
                    Some("text_delta") | None => {
                        if let Some(text) = delta
                            .and_then(|delta| delta.get("text"))
                            .and_then(Value::as_str)
                        {
                            out.push(StreamEvent::TextDelta {
                                text: text.to_owned(),
                            });
                        }
                    }
                    Some(_) => out.push(messages_extension(event_type, data)),
                }
            }
            "content_block_stop" => {
                if let Some(index) = data
                    .get("index")
                    .and_then(Value::as_u64)
                    .map(|index| index as usize)
                {
                    self.tool_blocks.remove(&index);
                    if self.extension_blocks.remove(&index) {
                        out.push(messages_extension(event_type, data));
                    }
                }
            }
            "message_delta" => {
                // stop_reason + usage.
                let delta = data.get("delta").and_then(Value::as_object);
                let protocol_reason = delta
                    .and_then(|d| d.get("stop_reason"))
                    .and_then(Value::as_str);
                let stop_reason = protocol_reason
                    .map(map_stop_reason)
                    .unwrap_or(StopReason::Stop);
                if let Some(usage) = data.get("usage") {
                    self.usage = extract_anthropic_usage(usage);
                }
                if !self.end_emitted {
                    self.end_emitted = true;
                    out.push(StreamEvent::End {
                        stop_reason,
                        protocol_reason: protocol_reason.map(str::to_owned),
                        usage: self.usage,
                    });
                }
            }
            "message_stop" => {
                self.finished = true;
                if !self.end_emitted {
                    self.end_emitted = true;
                    out.push(StreamEvent::End {
                        stop_reason: StopReason::Stop,
                        protocol_reason: None,
                        usage: self.usage,
                    });
                }
            }
            "error" => {
                self.finished = true;
                let error = data.get("error").and_then(Value::as_object);
                out.push(StreamEvent::Error {
                    message: error
                        .and_then(|e| e.get("message"))
                        .and_then(Value::as_str)
                        .unwrap_or("upstream request failed")
                        .to_string(),
                    code: error
                        .and_then(|e| e.get("type"))
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                });
            }
            _ => out.push(messages_extension(event_type, data)),
        }
        Ok(())
    }

    /// Mark transport EOF without inventing a protocol-level completion.
    pub fn finish(&mut self, _out: &mut Vec<StreamEvent>) {
        self.finished = true;
    }
}

fn messages_extension(kind: &str, payload: &Value) -> StreamEvent {
    StreamEvent::Extension {
        namespace: "anthropic.messages".to_owned(),
        kind: kind.to_owned(),
        payload: payload.clone(),
    }
}

fn opaque_fields(object: &Map<String, Value>, structural: &[&str]) -> Map<String, Value> {
    object
        .iter()
        .filter(|(key, _)| !structural.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn extract_anthropic_usage(usage: &Value) -> ChatUsage {
    let usage = usage.as_object();
    let u64_field = |field: &str| usage.and_then(|u| u.get(field)).and_then(Value::as_u64);
    ChatUsage {
        prompt_tokens: u64_field("input_tokens"),
        completion_tokens: u64_field("output_tokens"),
        cached_tokens: u64_field("cache_read_input_tokens"),
        cache_write_tokens: None,
        prompt_cache_miss_tokens: None,
        reasoning_tokens: usage
            .and_then(|u| u.get("output_tokens_details"))
            .and_then(Value::as_object)
            .and_then(|d| d.get("reasoning_tokens"))
            .and_then(Value::as_u64),
    }
}

fn map_stop_reason(reason: &str) -> StopReason {
    match reason {
        "end_turn" => StopReason::Stop,
        "tool_use" => StopReason::ToolUse,
        "max_tokens" => StopReason::MaxTokens,
        "refusal" => StopReason::ContentFilter,
        _ => StopReason::Other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_basic_messages_stream() {
        let mut parser = MessagesParser::new();
        let mut out = Vec::new();
        parser
            .on_event(
                "message_start",
                &json!({"message": {"id": "msg_1", "model": "m", "usage": {"input_tokens": 5, "output_tokens": 0}}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "content_block_start",
                &json!({"index": 0, "content_block": {"type": "thinking", "thinking": ""}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "content_block_delta",
                &json!({"index": 0, "delta": {"type": "thinking_delta", "thinking": "We"}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event("content_block_stop", &json!({"index": 0}), &mut out)
            .unwrap();
        parser
            .on_event(
                "content_block_start",
                &json!({"index": 1, "content_block": {"type": "text", "text": ""}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "content_block_delta",
                &json!({"index": 1, "delta": {"type": "text_delta", "text": "Hi"}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "message_delta",
                &json!({"delta": {"stop_reason": "end_turn"}, "usage": {"input_tokens": 5, "output_tokens": 2}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event("message_stop", &json!({}), &mut out)
            .unwrap();

        assert!(
            matches!(&out[0], StreamEvent::Start { id, model } if id == "msg_1" && model == "m")
        );
        assert!(matches!(&out[1], StreamEvent::ReasoningDelta { text, .. } if text == "We"));
        assert!(matches!(&out[2], StreamEvent::TextDelta { text } if text == "Hi"));
        let end = out.last().unwrap();
        assert!(
            matches!(end, StreamEvent::End { stop_reason, protocol_reason, .. }
                if *stop_reason == StopReason::Stop
                    && protocol_reason.as_deref() == Some("end_turn"))
        );
    }

    #[test]
    fn parses_thinking_signature_delta() {
        let mut parser = MessagesParser::new();
        let mut out = Vec::new();
        parser
            .on_event(
                "content_block_start",
                &json!({"index": 2, "content_block": {"type": "thinking", "thinking": ""}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "content_block_delta",
                &json!({"index": 2, "delta": {"type": "thinking_delta", "thinking": "We"}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "content_block_delta",
                &json!({"index": 2, "delta": {"type": "signature_delta", "signature": "sig"}}),
                &mut out,
            )
            .unwrap();

        assert!(matches!(
            &out[0],
            StreamEvent::ReasoningDelta { text, signature }
                if text == "We" && signature.is_none()
        ));
        assert!(matches!(
            &out[1],
            StreamEvent::ReasoningDelta { text, signature }
                if text.is_empty() && signature.as_deref() == Some("sig")
        ));
    }

    #[test]
    fn parses_tool_use_with_increments() {
        let mut parser = MessagesParser::new();
        let mut out = Vec::new();
        parser
            .on_event(
                "message_start",
                &json!({"message": {"id": "msg_1", "model": "m"}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "content_block_start",
                &json!({"index": 0, "content_block": {"type": "tool_use", "id": "toolu_1", "name": "get_weather", "input": {}, "x_tool": 1}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "content_block_delta",
                &json!({"index": 0, "delta": {"type": "input_json_delta", "partial_json": "{\"city\": \"Pa"}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "content_block_delta",
                &json!({"index": 0, "delta": {"type": "input_json_delta", "partial_json": "ris\"}"}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "message_delta",
                &json!({"delta": {"stop_reason": "tool_use"}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event("message_stop", &json!({}), &mut out)
            .unwrap();

        assert!(
            matches!(&out[1], StreamEvent::ToolCallStart { index: 0, id, name, .. } if id == "toolu_1" && name == "get_weather")
        );
        assert!(matches!(
            &out[1],
            StreamEvent::ToolCallStart { opaque_extensions, .. }
                if opaque_extensions["x_tool"] == 1
        ));
        assert!(
            matches!(&out[2], StreamEvent::ToolCallArgsDelta { index: 0, args } if args == "{\"city\": \"Pa")
        );
        assert!(
            matches!(&out[3], StreamEvent::ToolCallArgsDelta { index: 0, args } if args == "ris\"}")
        );
        assert!(
            matches!(out.last().unwrap(), StreamEvent::End { stop_reason, .. } if *stop_reason == StopReason::ToolUse)
        );
    }

    #[test]
    fn routes_tool_deltas_by_content_block_index() {
        let mut parser = MessagesParser::new();
        let mut out = Vec::new();
        parser
            .on_event(
                "content_block_start",
                &json!({"index": 1, "content_block": {"type": "tool_use", "id": "toolu_1", "name": "first", "input": {}}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "content_block_start",
                &json!({"index": 3, "content_block": {"type": "tool_use", "id": "toolu_2", "name": "second", "input": {}}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "content_block_delta",
                &json!({"index": 1, "delta": {"type": "input_json_delta", "partial_json": "{\"a\":"}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event("content_block_stop", &json!({"index": 1}), &mut out)
            .unwrap();
        parser
            .on_event(
                "content_block_delta",
                &json!({"index": 3, "delta": {"type": "input_json_delta", "partial_json": "{\"b\":"}}),
                &mut out,
            )
            .unwrap();

        assert!(matches!(
            &out[2],
            StreamEvent::ToolCallArgsDelta { index: 0, args } if args == "{\"a\":"
        ));
        assert!(matches!(
            &out[3],
            StreamEvent::ToolCallArgsDelta { index: 1, args } if args == "{\"b\":"
        ));
    }

    #[test]
    fn parses_error_event() {
        let mut parser = MessagesParser::new();
        let mut out = Vec::new();
        parser
            .on_event(
                "error",
                &json!({"error": {"type": "api_error", "message": "boom"}}),
                &mut out,
            )
            .unwrap();
        assert!(
            matches!(&out[0], StreamEvent::Error { message, code } if message == "boom" && code.as_deref() == Some("api_error"))
        );
    }

    #[test]
    fn preserves_unknown_block_lifecycle_as_extensions() {
        let mut parser = MessagesParser::new();
        let mut out = Vec::new();
        parser
            .on_event(
                "content_block_start",
                &json!({"index": 3, "content_block": {"type": "server_tool_use", "id": "srv_1"}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "content_block_delta",
                &json!({"index": 3, "delta": {"type": "server_tool_delta", "payload": 1}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event("content_block_stop", &json!({"index": 3}), &mut out)
            .unwrap();

        assert_eq!(out.len(), 3);
        for (event, kind) in out.iter().zip([
            "content_block_start",
            "content_block_delta",
            "content_block_stop",
        ]) {
            assert!(matches!(
                event,
                StreamEvent::Extension { namespace, kind: actual, .. }
                    if namespace == "anthropic.messages" && actual == kind
            ));
        }
    }

    #[test]
    fn finish_without_stop_does_not_invent_end() {
        let mut parser = MessagesParser::new();
        let mut out = Vec::new();
        parser
            .on_event(
                "message_start",
                &json!({"message": {"id": "msg_1", "model": "m"}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "content_block_start",
                &json!({"index": 0, "content_block": {"type": "text", "text": ""}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "content_block_delta",
                &json!({"index": 0, "delta": {"type": "text_delta", "text": "hi"}}),
                &mut out,
            )
            .unwrap();
        parser.finish(&mut out);
        assert!(!out
            .iter()
            .any(|event| matches!(event, StreamEvent::End { .. })));
    }

    #[test]
    fn pipeline_messages_to_responses_roundtrip() {
        // Upstream Messages SSE -> semantic events -> downstream Responses SSE.
        use crate::responses_renderer::ResponsesRenderer;

        let mut parser = MessagesParser::new();
        let mut renderer = ResponsesRenderer::new();
        let mut out = Vec::new();

        let events = [
            (
                "message_start",
                json!({"message": {"id": "msg_1", "model": "m", "usage": {"input_tokens": 5, "output_tokens": 0}}}),
            ),
            (
                "content_block_start",
                json!({"index": 0, "content_block": {"type": "thinking", "thinking": ""}}),
            ),
            (
                "content_block_delta",
                json!({"index": 0, "delta": {"type": "thinking_delta", "thinking": "We"}}),
            ),
            ("content_block_stop", json!({"index": 0})),
            (
                "content_block_start",
                json!({"index": 1, "content_block": {"type": "text", "text": ""}}),
            ),
            (
                "content_block_delta",
                json!({"index": 1, "delta": {"type": "text_delta", "text": "Hi"}}),
            ),
            ("content_block_stop", json!({"index": 1})),
            (
                "message_delta",
                json!({"delta": {"stop_reason": "end_turn"}, "usage": {"input_tokens": 5, "output_tokens": 2}}),
            ),
            ("message_stop", json!({})),
        ];
        for (event_type, data) in &events {
            let mut semantic = Vec::new();
            parser.on_event(event_type, data, &mut semantic).unwrap();
            for event in &semantic {
                renderer.on_event(event, &mut out);
            }
        }
        let text = String::from_utf8_lossy(&out.concat()).into_owned();
        assert!(text.contains("event: response.created"));
        assert!(text.contains("\"type\":\"response.reasoning_text.delta\""));
        assert!(text.contains("\"delta\":\"We\""));
        assert!(text.contains("\"type\":\"response.output_text.delta\""));
        assert!(text.contains("\"delta\":\"Hi\""));
        assert!(text.contains("event: response.completed"));
    }
}
