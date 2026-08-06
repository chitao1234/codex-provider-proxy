//! Anthropic Messages SSE renderer: semantic `StreamEvent`s into Messages SSE events.
//!
//! Extracted from the former `ChatStreamConverter`: this renderer owns the
//! downstream Anthropic concerns (content block state machine, thinking/text/tool_use
//! block lifecycle, message_delta/message_stop terminators, usage mapping).

use std::collections::HashMap;

use serde_json::{json, Map, Value};

use crate::chat::ChatUsage;
use crate::error::ConversionError;
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

/// Convert a non-streaming OpenAI Responses response object into an Anthropic
/// Messages response body (for Responses-upstream providers).
pub fn convert_responses_response_to_messages(body: &Value) -> Result<Value, ConversionError> {
    let response = body
        .as_object()
        .ok_or_else(|| ConversionError::invalid("Responses response is not an object"))?;
    let response_id = response
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let model = response
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let status = response
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("completed");

    let mut content: Vec<Value> = Vec::new();
    let mut stop_reason = "end_turn";
    if let Some(output) = response.get("output").and_then(Value::as_array) {
        for item in output {
            let Some(item) = item.as_object() else {
                continue;
            };
            match item.get("type").and_then(Value::as_str) {
                Some("reasoning") => {
                    // Reasoning summary -> thinking block.
                    if let Some(summary) = item.get("summary").and_then(Value::as_array) {
                        let text: Vec<String> = summary
                            .iter()
                            .filter_map(|s| {
                                s.get("text").and_then(Value::as_str).map(str::to_owned)
                            })
                            .collect();
                        if !text.is_empty() {
                            content.push(json!({
                                "type": "thinking",
                                "thinking": text.join("\n"),
                                "signature": "",
                            }));
                        }
                    }
                }
                Some("message") => {
                    if let Some(content_parts) = item.get("content").and_then(Value::as_array) {
                        for part in content_parts {
                            if let Some(text) = part.get("text").and_then(Value::as_str) {
                                content.push(json!({"type": "text", "text": text}));
                            }
                        }
                    }
                }
                Some("function_call") => {
                    let id = item
                        .get("call_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let name = item.get("name").and_then(Value::as_str).unwrap_or_default();
                    let arguments = item
                        .get("arguments")
                        .and_then(Value::as_str)
                        .map(|args| {
                            serde_json::from_str::<Value>(args).unwrap_or_else(|_| json!({}))
                        })
                        .unwrap_or_else(|| json!({}));
                    content.push(json!({
                        "type": "tool_use",
                        "id": id,
                        "name": name,
                        "input": arguments,
                    }));
                    stop_reason = "tool_use";
                }
                _ => {}
            }
        }
    }
    if status == "incomplete" {
        stop_reason = "max_tokens";
    }

    // usage.
    let usage = response.get("usage").and_then(Value::as_object);
    let mut usage_out = serde_json::Map::new();
    if let Some(input) = usage
        .and_then(|u| u.get("input_tokens"))
        .and_then(Value::as_u64)
    {
        usage_out.insert("input_tokens".to_string(), json!(input));
    }
    if let Some(output) = usage
        .and_then(|u| u.get("output_tokens"))
        .and_then(Value::as_u64)
    {
        usage_out.insert("output_tokens".to_string(), json!(output));
    }
    if let Some(cached) = usage
        .and_then(|u| u.get("input_tokens_details"))
        .and_then(Value::as_object)
        .and_then(|d| d.get("cached_tokens"))
        .and_then(Value::as_u64)
    {
        usage_out.insert("cache_read_input_tokens".to_string(), json!(cached));
    }

    Ok(json!({
        "id": format!("msg_{response_id}"),
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": content,
        "stop_reason": stop_reason,
        "stop_sequence": null,
        "usage": Value::Object(usage_out),
    }))
}

/// Normalize a non-streaming third-party Responses response object to the official
/// shape (strip non-standard fields, ensure resp_ id prefix).
pub fn convert_responses_response_to_responses(body: &Value) -> Result<Value, ConversionError> {
    let mut out = body.clone();
    let Some(object) = out.as_object_mut() else {
        return Err(ConversionError::invalid(
            "Responses response is not an object",
        ));
    };
    // Normalize the id prefix.
    if let Some(id) = object.get("id").and_then(Value::as_str) {
        if !id.starts_with("resp_") {
            object.insert("id".to_string(), json!(format!("resp_{id}")));
        }
    }
    // total_tokens is required by some clients (Codex) even when the upstream omits it.
    if object.get("total_tokens").is_none() {
        if let (Some(input), Some(output)) = (
            object
                .get("usage")
                .and_then(|u| u.get("input_tokens"))
                .and_then(Value::as_u64),
            object
                .get("usage")
                .and_then(|u| u.get("output_tokens"))
                .and_then(Value::as_u64),
        ) {
            if let Some(usage) = object.get_mut("usage").and_then(Value::as_object_mut) {
                usage.insert("total_tokens".to_string(), json!(input + output));
            }
        }
    }
    // Third-party extra fields (Qwen x_details, MiniMax output_text /
    // safety_identifier / conversation) are intentionally preserved: the downstream
    // client receives the upstream's full response, and unknown fields are harmless
    // to standard parsers.
    Ok(out)
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
    fn converts_responses_response_to_messages_non_streaming() {
        let body = json!({
            "id": "resp_123",
            "object": "response",
            "status": "completed",
            "model": "m",
            "output": [
                {"id": "rs_1", "type": "reasoning", "status": "completed", "summary": [{"type": "summary_text", "text": "thinking here"}]},
                {"id": "msg_1", "type": "message", "status": "completed", "role": "assistant", "content": [{"type": "output_text", "text": "Hello", "annotations": []}]},
                {"id": "fc_1", "type": "function_call", "status": "completed", "call_id": "call_1", "name": "get_weather", "arguments": "{\"city\": \"Paris\"}"}
            ],
            "usage": {"input_tokens": 10, "output_tokens": 5, "input_tokens_details": {"cached_tokens": 2}}
        });
        let out = convert_responses_response_to_messages(&body).unwrap();
        assert_eq!(out["type"], "message");
        assert_eq!(out["stop_reason"], "tool_use");
        assert_eq!(out["content"][0]["type"], "thinking");
        assert_eq!(out["content"][0]["thinking"], "thinking here");
        assert_eq!(out["content"][1]["type"], "text");
        assert_eq!(out["content"][1]["text"], "Hello");
        assert_eq!(out["content"][2]["type"], "tool_use");
        assert_eq!(out["content"][2]["name"], "get_weather");
        assert_eq!(out["content"][2]["input"]["city"], "Paris");
        assert_eq!(out["usage"]["input_tokens"], 10);
        assert_eq!(out["usage"]["cache_read_input_tokens"], 2);
    }

    #[test]
    fn normalizes_responses_id_and_preserves_extra_fields() {
        let body = json!({
            "id": "06c38eca53023d7eb041ff0a2c8fcf5c",
            "object": "response",
            "status": "completed",
            "output_text": "Hello",
            "x_details": [1, 2],
            "safety_identifier": null,
            "output": []
        });
        let out = convert_responses_response_to_responses(&body).unwrap();
        assert_eq!(out["id"], "resp_06c38eca53023d7eb041ff0a2c8fcf5c");
        // Extra third-party fields are preserved by default.
        assert_eq!(out["output_text"], "Hello");
        assert_eq!(out["x_details"], json!([1, 2]));
        assert!(out.get("safety_identifier").is_some());
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
