//! Anthropic Messages SSE renderer: semantic `StreamEvent`s into Messages SSE events.
//!
//! Extracted from the former `ChatStreamConverter`: this renderer owns the
//! downstream Anthropic concerns (content block state machine, thinking/text/tool_use
//! block lifecycle, message_delta/message_stop terminators, usage mapping).

use std::collections::HashMap;

use serde_json::{json, Map};

use crate::chat::ChatUsage;
use crate::sse::encode_sse_event;
use crate::stream::{StopReason, StreamEvent};

/// Accumulated tool-call render state (block index + arguments tail).
struct ToolRender {
    block_index: usize,
    arguments: String,
    emitted_len: usize,
}

/// Render semantic events into Anthropic Messages SSE bytes.
pub struct MessagesRenderer {
    started: bool,
    ended: bool,
    message_id: String,
    model: String,
    stop_reason: Option<&'static str>,
    text_open: bool,
    text_index: usize,
    thinking_open: bool,
    thinking_index: usize,
    next_block_index: usize,
    tools: HashMap<usize, ToolRender>,
    usage: ChatUsage,
    message_delta_sent: bool,
    message_stop_sent: bool,
}

impl Default for MessagesRenderer {
    fn default() -> Self {
        Self::new()
    }
}

impl MessagesRenderer {
    pub fn new() -> Self {
        Self {
            started: false,
            ended: false,
            message_id: String::new(),
            model: String::new(),
            stop_reason: None,
            text_open: false,
            text_index: 0,
            thinking_open: false,
            thinking_index: 0,
            next_block_index: 0,
            tools: HashMap::new(),
            usage: ChatUsage::default(),
            message_delta_sent: false,
            message_stop_sent: false,
        }
    }

    /// Process one semantic event, appending Messages SSE bytes to `out`.
    pub fn on_event(&mut self, event: &StreamEvent, out: &mut Vec<bytes::Bytes>) {
        if self.ended {
            return;
        }
        match event {
            StreamEvent::Start { id, model } => {
                self.started = true;
                self.message_id = synth_message_id(id);
                self.model = model.clone();
                out.push(encode_sse_event(
                    "message_start",
                    &json!({
                        "type": "message_start",
                        "message": {
                            "id": self.message_id,
                            "type": "message",
                            "role": "assistant",
                            "model": self.model,
                            "content": [],
                            "stop_reason": null,
                            "stop_sequence": null,
                            "usage": {"input_tokens": 0, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0, "output_tokens": 0}
                        }
                    }),
                ));
            }
            StreamEvent::ReasoningDelta { text, .. } => {
                self.close_text(out);
                self.open_thinking(out);
                self.thinking_delta(text, out);
            }
            StreamEvent::TextDelta { text } => {
                self.close_thinking(out);
                self.open_text(out);
                self.text_delta(text, out);
            }
            StreamEvent::ToolCallStart { index, id, name } => {
                self.close_text(out);
                self.close_thinking(out);
                let block_index = self.allocate_block_index();
                self.tools.insert(
                    *index,
                    ToolRender {
                        block_index,
                        arguments: String::new(),
                        emitted_len: 0,
                    },
                );
                out.push(encode_sse_event(
                    "content_block_start",
                    &json!({
                        "type": "content_block_start",
                        "index": block_index,
                        "content_block": {"type": "tool_use", "id": id, "name": name, "input": {}}
                    }),
                ));
            }
            StreamEvent::ToolCallArgsDelta { index, args } => {
                if let Some(tool) = self.tools.get_mut(index) {
                    tool.arguments.push_str(args);
                    if tool.emitted_len < tool.arguments.len() {
                        let increment = tool.arguments[tool.emitted_len..].to_string();
                        tool.emitted_len = tool.arguments.len();
                        if !increment.is_empty() {
                            out.push(encode_sse_event(
                                "content_block_delta",
                                &json!({
                                    "type": "content_block_delta",
                                    "index": tool.block_index,
                                    "delta": {"type": "input_json_delta", "partial_json": increment}
                                }),
                            ));
                        }
                    }
                }
            }
            StreamEvent::End { stop_reason, usage } => {
                self.stop_reason = Some(map_stop_reason(*stop_reason));
                self.usage = *usage;
                self.close_blocks(out);
                self.emit_message_delta(out);
                self.emit_message_stop(out);
            }
            StreamEvent::Error { message, code } => {
                self.ended = true;
                out.push(encode_sse_event(
                    "error",
                    &json!({
                        "type": "error",
                        "error": {
                            "type": "api_error",
                            "message": format!(
                                "{}: {message}",
                                code.as_deref().unwrap_or("api_error")
                            ),
                        },
                    }),
                ));
            }
        }
    }

    /// End the stream (upstream ended without a finish_reason): close open blocks and
    /// emit terminators exactly once.
    pub fn finish(&mut self, out: &mut Vec<bytes::Bytes>) {
        if self.ended {
            return;
        }
        self.ended = true;
        if !self.started {
            return;
        }
        self.close_blocks(out);
        self.emit_message_delta(out);
        self.emit_message_stop(out);
    }

    fn open_text(&mut self, out: &mut Vec<bytes::Bytes>) {
        if self.text_open {
            return;
        }
        self.text_open = true;
        self.text_index = self.allocate_block_index();
        out.push(encode_sse_event(
            "content_block_start",
            &json!({
                "type": "content_block_start",
                "index": self.text_index,
                "content_block": {"type": "text", "text": ""}
            }),
        ));
    }

    fn open_thinking(&mut self, out: &mut Vec<bytes::Bytes>) {
        if self.thinking_open {
            return;
        }
        self.thinking_open = true;
        self.thinking_index = self.allocate_block_index();
        out.push(encode_sse_event(
            "content_block_start",
            &json!({
                "type": "content_block_start",
                "index": self.thinking_index,
                "content_block": {"type": "thinking", "thinking": "", "signature": ""}
            }),
        ));
    }

    fn text_delta(&mut self, text: &str, out: &mut Vec<bytes::Bytes>) {
        out.push(encode_sse_event(
            "content_block_delta",
            &json!({
                "type": "content_block_delta",
                "index": self.text_index,
                "delta": {"type": "text_delta", "text": text}
            }),
        ));
    }

    fn thinking_delta(&mut self, thinking: &str, out: &mut Vec<bytes::Bytes>) {
        out.push(encode_sse_event(
            "content_block_delta",
            &json!({
                "type": "content_block_delta",
                "index": self.thinking_index,
                "delta": {"type": "thinking_delta", "thinking": thinking}
            }),
        ));
    }

    fn close_text(&mut self, out: &mut Vec<bytes::Bytes>) {
        if !self.text_open {
            return;
        }
        self.text_open = false;
        out.push(encode_sse_event(
            "content_block_stop",
            &json!({"type": "content_block_stop", "index": self.text_index}),
        ));
    }

    fn close_thinking(&mut self, out: &mut Vec<bytes::Bytes>) {
        if !self.thinking_open {
            return;
        }
        self.thinking_open = false;
        out.push(encode_sse_event(
            "content_block_stop",
            &json!({"type": "content_block_stop", "index": self.thinking_index}),
        ));
    }

    fn close_blocks(&mut self, out: &mut Vec<bytes::Bytes>) {
        self.close_thinking(out);
        self.close_text(out);
        let mut indices: Vec<(usize, usize)> = self
            .tools
            .iter()
            .map(|(upstream_index, tool)| (*upstream_index, tool.block_index))
            .collect();
        indices.sort_by_key(|(_, block_index)| *block_index);
        for (upstream_index, block_index) in indices {
            let Some(tool) = self.tools.get_mut(&upstream_index) else {
                continue;
            };
            if tool.emitted_len < tool.arguments.len() {
                let increment = tool.arguments[tool.emitted_len..].to_string();
                tool.emitted_len = tool.arguments.len();
                if !increment.is_empty() {
                    out.push(encode_sse_event(
                        "content_block_delta",
                        &json!({
                            "type": "content_block_delta",
                            "index": block_index,
                            "delta": {"type": "input_json_delta", "partial_json": increment}
                        }),
                    ));
                }
            }
            out.push(encode_sse_event(
                "content_block_stop",
                &json!({"type": "content_block_stop", "index": block_index}),
            ));
        }
        self.tools.clear();
    }

    fn emit_message_delta(&mut self, out: &mut Vec<bytes::Bytes>) {
        if self.message_delta_sent {
            return;
        }
        self.message_delta_sent = true;
        let stop_reason = self.stop_reason.unwrap_or("end_turn");
        let mut usage_out = Map::new();
        if let Some(output) = self.usage.completion_tokens {
            usage_out.insert("output_tokens".to_string(), json!(output));
        }
        out.push(encode_sse_event(
            "message_delta",
            &json!({
                "type": "message_delta",
                "delta": {"stop_reason": stop_reason, "stop_sequence": null},
                "usage": usage_out
            }),
        ));
    }

    fn emit_message_stop(&mut self, out: &mut Vec<bytes::Bytes>) {
        if self.message_stop_sent {
            return;
        }
        self.message_stop_sent = true;
        out.push(encode_sse_event(
            "message_stop",
            &json!({"type": "message_stop"}),
        ));
    }

    fn allocate_block_index(&mut self) -> usize {
        let index = self.next_block_index;
        self.next_block_index += 1;
        index
    }
}

fn synth_message_id(upstream_id: &str) -> String {
    if upstream_id.is_empty() {
        return String::new();
    }
    if upstream_id.starts_with("msg_") {
        upstream_id.to_string()
    } else {
        format!("msg_{upstream_id}")
    }
}

fn map_stop_reason(reason: StopReason) -> &'static str {
    match reason {
        StopReason::Stop => "end_turn",
        StopReason::ToolUse => "tool_use",
        StopReason::MaxTokens => "max_tokens",
        StopReason::ContentFilter => "refusal",
        StopReason::Other => "end_turn",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chat_parser::ChatParser;

    fn render(events: Vec<StreamEvent>) -> String {
        let mut renderer = MessagesRenderer::new();
        let mut out = Vec::new();
        for event in &events {
            renderer.on_event(event, &mut out);
        }
        renderer.finish(&mut out);
        String::from_utf8_lossy(&out.concat()).into_owned()
    }

    /// Full pipeline: chat SSE chunks -> semantic events -> Messages SSE.
    fn pipeline(chunks: &[&str], finish: bool) -> String {
        let mut parser = ChatParser::new();
        let mut renderer = MessagesRenderer::new();
        let mut out = Vec::new();
        for chunk in chunks {
            let mut events = Vec::new();
            parser.on_chunk(chunk, &mut events).expect("chunk parses");
            for event in &events {
                renderer.on_event(event, &mut out);
            }
        }
        if finish {
            let mut events = Vec::new();
            parser.finish(&mut events);
            for event in &events {
                renderer.on_event(event, &mut out);
            }
            renderer.finish(&mut out);
        }
        String::from_utf8_lossy(&out.concat()).into_owned()
    }

    #[test]
    fn renders_basic_anthropic_stream() {
        let text = render(vec![
            StreamEvent::Start {
                id: "x".to_string(),
                model: "m".to_string(),
            },
            StreamEvent::ReasoningDelta {
                text: "We".to_string(),
                signature: None,
            },
            StreamEvent::TextDelta {
                text: "Hi".to_string(),
            },
            StreamEvent::End {
                stop_reason: StopReason::Stop,
                usage: ChatUsage::default(),
            },
        ]);
        assert!(text.contains("event: message_start"));
        assert!(text.contains("\"type\":\"thinking\""));
        assert!(text.contains("\"thinking\":\"We\""));
        assert!(text.contains("\"type\":\"text\""));
        assert!(text.contains("\"text\":\"Hi\""));
        assert!(text.contains("\"stop_reason\":\"end_turn\""));
        assert!(text.contains("event: message_stop"));
    }

    #[test]
    fn renders_tool_calls_with_increments() {
        let text = render(vec![
            StreamEvent::Start {
                id: "x".to_string(),
                model: "m".to_string(),
            },
            StreamEvent::ToolCallStart {
                index: 0,
                id: "call_1".to_string(),
                name: "get_weather".to_string(),
            },
            StreamEvent::ToolCallArgsDelta {
                index: 0,
                args: "{\"city\": \"Pa".to_string(),
            },
            StreamEvent::ToolCallArgsDelta {
                index: 0,
                args: "ris\"}".to_string(),
            },
            StreamEvent::End {
                stop_reason: StopReason::ToolUse,
                usage: ChatUsage::default(),
            },
        ]);
        assert!(text.contains("\"type\":\"tool_use\""));
        assert!(text.contains("\"id\":\"call_1\""));
        assert!(text.contains("\"name\":\"get_weather\""));
        assert!(text.contains("\"partial_json\":\"{\\\"city\\\": \\\"Pa\""));
        assert!(text.contains("\"partial_json\":\"ris\\\"}\""));
        assert!(!text.contains("\"partial_json\":\"{\\\"city\\\": \\\"Paris\\\"}\""));
        assert!(text.contains("\"stop_reason\":\"tool_use\""));
    }

    #[test]
    fn renders_error_event() {
        let text = render(vec![StreamEvent::Error {
            message: "upstream exploded".to_string(),
            code: Some("upstream_error".to_string()),
        }]);
        assert!(text.contains("event: error"));
        assert!(text.contains("upstream exploded"));
        assert!(!text.contains("event: message_stop"));
    }

    #[test]
    fn pipeline_converts_deepseek_chunks() {
        let text = pipeline(
            &[
                r#"{"id":"0e414c94","object":"chat.completion.chunk","model":"deepseek-v4-pro","choices":[{"index":0,"delta":{"role":"assistant","content":null,"reasoning_content":""},"finish_reason":null}],"usage":null}"#,
                r#"{"id":"0e414c94","choices":[{"index":0,"delta":{"content":null,"reasoning_content":"We"},"finish_reason":null}],"usage":null}"#,
                r#"{"id":"0e414c94","choices":[{"index":0,"delta":{"content":"Hi","reasoning_content":null},"finish_reason":null}],"usage":null}"#,
                r#"{"id":"0e414c94","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":null}"#,
                r#"{"id":"0e414c94","choices":[],"usage":{"prompt_tokens":6,"completion_tokens":9,"total_tokens":15}}"#,
                "[DONE]",
            ],
            true,
        );
        assert!(text.contains("event: message_start"));
        assert!(text.contains("\"id\":\"msg_0e414c94\""));
        assert!(text.contains("\"type\":\"thinking_delta\""));
        assert!(text.contains("\"thinking\":\"We\""));
        assert!(text.contains("\"type\":\"text_delta\""));
        assert!(text.contains("\"text\":\"Hi\""));
        assert!(text.contains("\"stop_reason\":\"end_turn\""));
        assert!(text.contains("event: message_stop"));
        assert_eq!(text.matches("event: message_stop").count(), 1);
    }

    #[test]
    fn pipeline_accumulates_tool_calls_across_chunks() {
        let text = pipeline(
            &[
                r#"{"id":"x","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":""}}]},"finish_reason":null}]}"#,
                r#"{"id":"x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"city\": \"Pa"}}]},"finish_reason":null}]}"#,
                r#"{"id":"x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"ris\"}"}}]},"finish_reason":null}]}"#,
                r#"{"id":"x","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}],"usage":null}"#,
                "[DONE]",
            ],
            true,
        );
        assert!(text.contains("\"type\":\"tool_use\""));
        assert!(text.contains("\"name\":\"get_weather\""));
        assert!(text.contains("\"stop_reason\":\"tool_use\""));
        assert!(text.contains("\"partial_json\":\"{\\\"city\\\": \\\"Pa\""));
        assert!(text.contains("\"partial_json\":\"ris\\\"}\""));
        assert!(!text.contains("\"partial_json\":\"{\\\"city\\\": \\\"Paris\\\"}\""));
        assert_eq!(text.matches("event: message_stop").count(), 1);
    }

    #[test]
    fn pipeline_emits_error_event() {
        let text = pipeline(
            &[
                r#"{"id":"x","choices":[{"index":0,"delta":{"content":"Hi"},"finish_reason":null}]}"#,
                r#"{"error":{"message":"upstream exploded","type":"server_error","code":"upstream_error"}}"#,
                "[DONE]",
            ],
            true,
        );
        assert!(text.contains("event: error"));
        assert!(text.contains("\"type\":\"error\""));
        assert!(text.contains("upstream exploded"));
        assert!(!text.contains("event: message_stop"));
    }

    #[test]
    fn pipeline_finish_without_done_emits_terminators() {
        let text = pipeline(
            &[r#"{"id":"x","choices":[{"index":0,"delta":{"content":"hi"}}]}"#],
            true,
        );
        assert!(text.contains("event: message_delta"));
        assert!(text.contains("event: message_stop"));
        assert_eq!(text.matches("event: message_stop").count(), 1);
    }

    #[test]
    fn pipeline_finish_is_idempotent() {
        let mut parser = ChatParser::new();
        let mut renderer = MessagesRenderer::new();
        let mut out = Vec::new();
        let mut events = Vec::new();
        parser.finish(&mut events);
        for event in &events {
            renderer.on_event(event, &mut out);
        }
        renderer.finish(&mut out);
        renderer.finish(&mut out);
        let text = String::from_utf8_lossy(&out.concat()).into_owned();
        assert_eq!(text.matches("event: message_stop").count(), 0);
    }
}
