//! Anthropic Messages SSE parser: upstream Messages SSE events into the semantic
//! `StreamEvent` IR.
//!
//! This is the counterpart of `MessagesRenderer`: it translates the upstream
//! protocol's content-block state machine (message_start, content_block_start/delta/
//! stop, message_delta, message_stop) into protocol-independent events, preserving
//! thinking signatures and tool-call increments.

use serde_json::Value;

use crate::chat::ChatUsage;
use crate::error::ConversionError;
use crate::stream::{StopReason, StreamEvent};

/// Accumulated tool-call state across upstream content blocks.
#[derive(Default)]
struct ToolAccumulator {
    id: Option<String>,
    name: Option<String>,
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
    /// Index of the content block currently being deltad (if any).
    active_block: Option<usize>,
    /// Type of the active block: "text" | "thinking" | "tool_use".
    active_type: Option<&'static str>,
    /// Tool call index -> accumulated state.
    tools: HashMap<usize, ToolAccumulator>,
    /// Next tool index to assign (Anthropic blocks have no index, use sequence).
    next_tool_index: usize,
    usage: ChatUsage,
    /// Whether message_delta already delivered usage (the final usage chunk).
    end_emitted: bool,
}

use std::collections::HashMap;

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
            active_block: None,
            active_type: None,
            tools: HashMap::new(),
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
                self.active_block = Some(index);
                self.active_type = Some(match block_type {
                    "thinking" => "thinking",
                    "tool_use" => "tool_use",
                    _ => "text",
                });
                if block_type == "tool_use" {
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
                    self.tools.insert(
                        tool_index,
                        ToolAccumulator {
                            id: Some(id.clone()),
                            name: Some(name.clone()),
                            ..Default::default()
                        },
                    );
                    out.push(StreamEvent::ToolCallStart {
                        index: tool_index,
                        id,
                        name,
                    });
                }
            }
            "content_block_delta" => {
                let delta = data.get("delta").and_then(Value::as_object);
                match self.active_type {
                    Some("thinking") => {
                        if let Some(text) = delta.and_then(|d| d.get("thinking")) {
                            if let Some(text) = text.as_str() {
                                // thinking_delta carries the increment.
                                out.push(StreamEvent::ReasoningDelta {
                                    text: text.to_string(),
                                    signature: None,
                                });
                            }
                        }
                    }
                    Some("tool_use") => {
                        if let Some(partial) = delta.and_then(|d| d.get("partial_json")) {
                            if let Some(partial) = partial.as_str() {
                                let tool_index = self
                                    .tools
                                    .iter()
                                    .find(|(_, t)| t.name.is_some() && t.id.is_some())
                                    .map(|(i, _)| *i);
                                if let Some(tool_index) = tool_index {
                                    let acc = self.tools.get_mut(&tool_index).expect("tool exists");
                                    acc.arguments.push_str(partial);
                                    if acc.emitted_len < acc.arguments.len() {
                                        let increment =
                                            acc.arguments[acc.emitted_len..].to_string();
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
                        }
                    }
                    _ => {
                        // text_delta
                        if let Some(text) = delta.and_then(|d| d.get("text")) {
                            if let Some(text) = text.as_str() {
                                out.push(StreamEvent::TextDelta {
                                    text: text.to_string(),
                                });
                            }
                        }
                    }
                }
            }
            "content_block_stop" => {
                self.active_block = None;
                self.active_type = None;
            }
            "message_delta" => {
                // stop_reason + usage.
                let delta = data.get("delta").and_then(Value::as_object);
                let stop_reason = delta
                    .and_then(|d| d.get("stop_reason"))
                    .and_then(Value::as_str)
                    .map(map_stop_reason)
                    .unwrap_or(StopReason::Stop);
                if let Some(usage) = data.get("usage") {
                    self.usage = extract_anthropic_usage(usage);
                }
                if !self.end_emitted {
                    self.end_emitted = true;
                    out.push(StreamEvent::End {
                        stop_reason,
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
            _ => {
                // Unknown events (e.g. ping) are ignored.
            }
        }
        Ok(())
    }

    /// End of the upstream stream without a message_stop: emit a final End event.
    pub fn finish(&mut self, out: &mut Vec<StreamEvent>) {
        if self.finished {
            return;
        }
        self.finished = true;
        if !self.started {
            return;
        }
        if !self.end_emitted {
            self.end_emitted = true;
            out.push(StreamEvent::End {
                stop_reason: StopReason::Stop,
                usage: self.usage,
            });
        }
    }
}

fn extract_anthropic_usage(usage: &Value) -> ChatUsage {
    let usage = usage.as_object();
    let u64_field = |field: &str| usage.and_then(|u| u.get(field)).and_then(Value::as_u64);
    ChatUsage {
        prompt_tokens: u64_field("input_tokens"),
        completion_tokens: u64_field("output_tokens"),
        cached_tokens: u64_field("cache_read_input_tokens"),
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
            matches!(end, StreamEvent::End { stop_reason, .. } if *stop_reason == StopReason::Stop)
        );
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
                &json!({"index": 0, "content_block": {"type": "tool_use", "id": "toolu_1", "name": "get_weather", "input": {}}}),
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
            matches!(&out[1], StreamEvent::ToolCallStart { index: 0, id, name } if id == "toolu_1" && name == "get_weather")
        );
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
    fn finish_without_stop_emits_end() {
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
        assert!(matches!(out.last().unwrap(), StreamEvent::End { .. }));
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
