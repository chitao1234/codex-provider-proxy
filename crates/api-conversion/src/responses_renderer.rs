//! OpenAI Responses SSE renderer: semantic `StreamEvent`s into official Responses SSE.
//!
//! Extracted from the former `ResponsesStreamConverter`: this renderer owns the
//! downstream Responses concerns (output item lifecycle, sequence numbers,
//! function_call items with accumulated arguments, the assistant turn for transcript
//! storage). The upstream parsing (chat chunks) is the parsers' job.

use std::collections::HashMap;

use bytes::Bytes;
use serde_json::{json, Value};

use crate::chat::ChatUsage;
use crate::error::ConversionError;
use crate::sse::encode_sse_event;
use crate::stream::{StopReason, StreamEvent};

/// Accumulated state of one function_call item, for the closing events.
struct FunctionCallState {
    item_id: String,
    call_id: String,
    name: String,
    arguments: String,
    output_index: usize,
}

/// Render semantic events into official OpenAI Responses SSE bytes.
pub struct ResponsesRenderer {
    started: bool,
    finished: bool,
    response_id: String,
    model: String,
    output_index: usize,
    sequence_number: u64,
    reasoning_item: Option<usize>,
    reasoning_text: String,
    message_item: Option<usize>,
    message_text: String,
    function_calls: HashMap<usize, FunctionCallState>,
    assistant_turn: Option<Value>,
    usage: ChatUsage,
}

impl Default for ResponsesRenderer {
    fn default() -> Self {
        Self::new()
    }
}

impl ResponsesRenderer {
    pub fn new() -> Self {
        Self {
            started: false,
            finished: false,
            response_id: String::new(),
            model: String::new(),
            output_index: 0,
            sequence_number: 0,
            reasoning_item: None,
            reasoning_text: String::new(),
            message_item: None,
            message_text: String::new(),
            function_calls: HashMap::new(),
            assistant_turn: None,
            usage: ChatUsage::default(),
        }
    }

    /// The synthesized downstream `response_id`, available once the stream started.
    pub fn response_id(&self) -> Option<&str> {
        self.started.then_some(self.response_id.as_str())
    }

    /// The assistant chat message produced by this stream (text and/or tool calls),
    /// for transcript storage. Available after the stream ends.
    pub fn assistant_turn(&self) -> Option<&Value> {
        self.assistant_turn.as_ref()
    }

    /// Process one semantic event, appending Responses SSE bytes to `out`.
    pub fn on_event(&mut self, event: &StreamEvent, out: &mut Vec<Bytes>) {
        if self.finished {
            return;
        }
        match event {
            StreamEvent::Start { id, model } => {
                self.started = true;
                self.response_id = synth_response_id(id);
                self.model = model.clone();
                self.emit(
                    "response.created",
                    &json!({
                        "type": "response.created",
                        "response": self.response_snapshot("in_progress"),
                    }),
                    out,
                );
                self.emit(
                    "response.in_progress",
                    &json!({
                        "type": "response.in_progress",
                        "response": self.response_snapshot("in_progress"),
                    }),
                    out,
                );
            }
            StreamEvent::ReasoningDelta { text, .. } => {
                self.ensure_reasoning_item(out);
                self.reasoning_text.push_str(text);
                let index = self.reasoning_item.unwrap_or(0);
                self.emit(
                    "response.reasoning_text.delta",
                    &json!({
                        "type": "response.reasoning_text.delta",
                        "item_id": format!("rs_{}", &self.response_id[5..]),
                        "output_index": index,
                        "content_index": 0,
                        "delta": text,
                    }),
                    out,
                );
            }
            StreamEvent::TextDelta { text } => {
                self.ensure_message_item(out);
                self.message_text.push_str(text);
                let index = self.message_item.unwrap_or(0);
                self.emit(
                    "response.output_text.delta",
                    &json!({
                        "type": "response.output_text.delta",
                        "item_id": format!("msg_{}", &self.response_id[5..]),
                        "output_index": index,
                        "content_index": 0,
                        "delta": text,
                    }),
                    out,
                );
            }
            StreamEvent::ToolCallStart { index, id, name } => {
                let item_id = format!("fc_{}_{}", &self.response_id[5..], index);
                let output_index = self.output_index;
                self.output_index += 1;
                self.function_calls.insert(
                    *index,
                    FunctionCallState {
                        item_id: item_id.clone(),
                        call_id: id.clone(),
                        name: name.clone(),
                        arguments: String::new(),
                        output_index,
                    },
                );
                self.emit(
                    "response.output_item.added",
                    &json!({
                        "type": "response.output_item.added",
                        "output_index": output_index,
                        "item": {
                            "id": item_id,
                            "type": "function_call",
                            "status": "in_progress",
                            "call_id": id,
                            "name": name,
                            "arguments": "",
                        },
                    }),
                    out,
                );
            }
            StreamEvent::ToolCallArgsDelta { index, args } => {
                let item_id = self
                    .function_calls
                    .get(index)
                    .map(|state| state.item_id.clone());
                let Some(item_id) = item_id else {
                    return;
                };
                let Some(state) = self.function_calls.get_mut(index) else {
                    return;
                };
                state.arguments.push_str(args);
                let output_index = state.output_index;
                self.emit(
                    "response.function_call_arguments.delta",
                    &json!({
                        "type": "response.function_call_arguments.delta",
                        "item_id": item_id,
                        "output_index": output_index,
                        "delta": args,
                    }),
                    out,
                );
            }
            StreamEvent::End { stop_reason, usage } => {
                self.usage = *usage;
                self.emit_completed(*stop_reason, out);
            }
            StreamEvent::Error { message, code } => {
                self.finished = true;
                self.emit(
                    "error",
                    &json!({
                        "type": "error",
                        "code": code.clone().unwrap_or_else(|| "api_error".to_string()),
                        "message": message,
                        "param": null,
                    }),
                    out,
                );
            }
        }
    }

    /// End the stream (upstream ended without a finish_reason): emit completed
    /// (or incomplete) if not already sent.
    pub fn finish(&mut self, out: &mut Vec<Bytes>) {
        if self.finished {
            return;
        }
        self.finished = true;
        if !self.started {
            return;
        }
        self.emit_completed(StopReason::Other, out);
    }

    fn ensure_reasoning_item(&mut self, out: &mut Vec<Bytes>) {
        if self.reasoning_item.is_some() {
            return;
        }
        let index = self.output_index;
        self.output_index += 1;
        self.reasoning_item = Some(index);
        self.emit(
            "response.output_item.added",
            &json!({
                "type": "response.output_item.added",
                "output_index": index,
                "item": {
                    "id": format!("rs_{}", &self.response_id[5..]),
                    "type": "reasoning",
                    "status": "in_progress",
                    "summary": [],
                },
            }),
            out,
        );
    }

    fn ensure_message_item(&mut self, out: &mut Vec<Bytes>) {
        if self.message_item.is_some() {
            return;
        }
        let index = self.output_index;
        self.output_index += 1;
        self.message_item = Some(index);
        self.emit(
            "response.output_item.added",
            &json!({
                "type": "response.output_item.added",
                "output_index": index,
                "item": {
                    "id": format!("msg_{}", &self.response_id[5..]),
                    "type": "message",
                    "status": "in_progress",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "", "annotations": []}],
                },
            }),
            out,
        );
    }

    fn emit_completed(&mut self, _stop_reason: StopReason, out: &mut Vec<Bytes>) {
        self.finished = true;
        // Close open items.
        let mut args_done: Vec<Value> = Vec::new();
        let mut items_done: Vec<Value> = Vec::new();
        if let Some(index) = self.reasoning_item.take() {
            items_done.push(json!({
                "type": "response.output_item.done",
                "output_index": index,
                "item": {
                    "id": format!("rs_{}", &self.response_id[5..]),
                    "type": "reasoning",
                    "status": "completed",
                    "summary": [],
                },
            }));
        }
        // Build the assistant chat turn for transcript storage as items close.
        let mut assistant = json!({"role": "assistant", "content": ""});
        let reasoning_text = std::mem::take(&mut self.reasoning_text);
        let mut tool_calls: Vec<Value> = Vec::new();
        if let Some(index) = self.message_item.take() {
            let text = std::mem::take(&mut self.message_text);
            assistant["content"] = json!(text);
            items_done.push(json!({
                "type": "response.output_item.done",
                "output_index": index,
                "item": {
                    "id": format!("msg_{}", &self.response_id[5..]),
                    "type": "message",
                    "status": "completed",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": text, "annotations": []}],
                },
            }));
        }
        // Carry the upstream reasoning so a later turn can reattach it as
        // reasoning_content (MiMo requires it on tool-call turns, else 400).
        if !reasoning_text.trim().is_empty() {
            assistant["reasoning_content"] = json!(reasoning_text);
        }
        // Function call items: close each with the accumulated arguments
        // (function_call_arguments.done), then emit the completed item carrying
        // the full name/arguments for clients that rebuild calls from done events.
        let fc_indices: Vec<usize> = self.function_calls.keys().copied().collect();
        for index in fc_indices {
            let Some(state) = self.function_calls.remove(&index) else {
                continue;
            };
            tool_calls.push(json!({
                "id": state.call_id,
                "type": "function",
                "function": {"name": state.name, "arguments": state.arguments},
            }));
            args_done.push(json!({
                "type": "response.function_call_arguments.done",
                "item_id": state.item_id,
                "output_index": state.output_index,
                "arguments": state.arguments,
            }));
            items_done.push(json!({
                "type": "response.output_item.done",
                "output_index": state.output_index,
                "item": {
                    "id": state.item_id,
                    "type": "function_call",
                    "status": "completed",
                    "call_id": state.call_id,
                    "name": state.name,
                    "arguments": state.arguments,
                },
            }));
        }
        if !tool_calls.is_empty() {
            assistant["tool_calls"] = json!(tool_calls);
        }
        // A turn is worth recording when the assistant said something or called tools.
        if !assistant["content"].as_str().is_none_or(str::is_empty) || !tool_calls.is_empty() {
            self.assistant_turn = Some(assistant);
        }
        for event in args_done {
            self.emit("response.function_call_arguments.done", &event, out);
        }
        for event in items_done {
            self.emit("response.output_item.done", &event, out);
        }

        self.emit(
            "response.completed",
            &json!({
                "type": "response.completed",
                "response": {
                    "id": self.response_id,
                    "object": "response",
                    "status": "completed",
                    "model": self.model,
                    "output": [],
                    "usage": responses_usage(self.usage),
                },
            }),
            out,
        );
    }

    fn response_snapshot(&self, status: &str) -> Value {
        json!({
            "id": self.response_id,
            "object": "response",
            "status": status,
            "model": self.model,
            "output": [],
            "usage": null,
        })
    }

    fn emit(&mut self, event_type: &str, data: &Value, out: &mut Vec<Bytes>) {
        self.sequence_number += 1;
        let mut event = data.clone();
        if let Value::Object(obj) = &mut event {
            obj.insert("sequence_number".to_string(), json!(self.sequence_number));
        }
        out.push(encode_sse_event(event_type, &event));
    }
}

fn synth_response_id(upstream_id: &str) -> String {
    if upstream_id.is_empty() {
        return String::new();
    }
    if upstream_id.starts_with("resp_") {
        upstream_id.to_string()
    } else {
        format!("resp_{upstream_id}")
    }
}

/// Map chat usage to the Responses usage shape.
pub fn responses_usage(usage: ChatUsage) -> Value {
    let mut out = serde_json::Map::new();
    if let Some(input) = usage.prompt_tokens {
        out.insert("input_tokens".to_string(), json!(input));
    }
    if let Some(output) = usage.completion_tokens {
        out.insert("output_tokens".to_string(), json!(output));
    }
    // total_tokens is required by some clients (Codex) even when the upstream omits it.
    let total = usage
        .prompt_tokens
        .zip(usage.completion_tokens)
        .map(|(input, output)| input + output);
    out.insert("total_tokens".to_string(), json!(total.unwrap_or_default()));
    let mut input_details = serde_json::Map::new();
    if let Some(cached) = usage.cached_tokens {
        input_details.insert("cached_tokens".to_string(), json!(cached));
    }
    if !input_details.is_empty() {
        out.insert(
            "input_tokens_details".to_string(),
            Value::Object(input_details),
        );
    }
    let mut output_details = serde_json::Map::new();
    if let Some(reasoning) = usage.reasoning_tokens {
        output_details.insert("reasoning_tokens".to_string(), json!(reasoning));
    }
    if !output_details.is_empty() {
        out.insert(
            "output_tokens_details".to_string(),
            Value::Object(output_details),
        );
    }
    Value::Object(out)
}

/// Convert a non-streaming Anthropic Messages response body into an official
/// Responses response object (for Messages-upstream providers).
pub fn convert_messages_response_to_responses(body: &Value) -> Result<Value, ConversionError> {
    // The Messages response body is the message object itself
    // ({"id","type":"message","content":[...],...}).
    let message = body
        .as_object()
        .ok_or_else(|| ConversionError::invalid("Messages response is not an object"))?;
    let response_id = message
        .get("id")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_default();
    let model = message
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let stop_reason = message
        .get("stop_reason")
        .and_then(Value::as_str)
        .unwrap_or("end_turn");

    let mut output: Vec<Value> = Vec::new();
    let mut message_content: Vec<Value> = Vec::new();
    let mut reasoning_summary = String::new();
    let mut tool_calls: Vec<Value> = Vec::new();
    if let Some(content) = message.get("content").and_then(Value::as_array) {
        for block in content {
            let Some(block) = block.as_object() else {
                continue;
            };
            match block.get("type").and_then(Value::as_str) {
                Some("thinking") => {
                    if let Some(text) = block.get("thinking").and_then(Value::as_str) {
                        reasoning_summary.push_str(text);
                    }
                }
                Some("text") => {
                    if let Some(text) = block.get("text").and_then(Value::as_str) {
                        message_content
                            .push(json!({"type": "output_text", "text": text, "annotations": []}));
                    }
                }
                Some("tool_use") => {
                    let id = block.get("id").and_then(Value::as_str).unwrap_or_default();
                    let name = block
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let input = block.get("input").cloned().unwrap_or_else(|| json!({}));
                    tool_calls.push(json!({
                        "id": id,
                        "type": "function_call",
                        "status": "completed",
                        "call_id": id,
                        "name": name,
                        "arguments": input.to_string(),
                    }));
                }
                _ => {}
            }
        }
    }
    if !reasoning_summary.trim().is_empty() {
        output.push(json!({
            "id": format!("rs_{}", &response_id[5..]),
            "type": "reasoning",
            "status": "completed",
            "summary": [{"type": "summary_text", "text": reasoning_summary}],
        }));
    }
    if !message_content.is_empty() || !tool_calls.is_empty() {
        let mut msg = serde_json::Map::new();
        msg.insert(
            "id".to_string(),
            json!(format!("msg_{}", &response_id[5..])),
        );
        msg.insert("type".to_string(), json!("message"));
        msg.insert("status".to_string(), json!("completed"));
        msg.insert("role".to_string(), json!("assistant"));
        msg.insert("content".to_string(), Value::Array(message_content));
        output.push(Value::Object(msg));
    }
    for call in tool_calls {
        output.push(call);
    }

    // usage from the Messages message.
    let usage = message.get("usage").and_then(Value::as_object);
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
        .and_then(|u| u.get("cache_read_input_tokens"))
        .and_then(Value::as_u64)
    {
        usage_out.insert(
            "input_tokens_details".to_string(),
            json!({"cached_tokens": cached}),
        );
    }

    Ok(json!({
        "id": format!("resp_{response_id}"),
        "object": "response",
        "status": if stop_reason == "max_tokens" { "incomplete" } else { "completed" },
        "incomplete_details": if stop_reason == "max_tokens" {
            json!({"reason": "max_output_tokens"})
        } else {
            Value::Null
        },
        "error": null,
        "model": model,
        "output": output,
        "usage": Value::Object(usage_out),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chat_parser::ChatParser;

    fn render(events: Vec<StreamEvent>) -> String {
        let mut renderer = ResponsesRenderer::new();
        let mut out = Vec::new();
        for event in &events {
            renderer.on_event(event, &mut out);
        }
        renderer.finish(&mut out);
        String::from_utf8_lossy(&out.concat()).into_owned()
    }

    /// Full pipeline: chat SSE chunks -> semantic events -> Responses SSE.
    fn pipeline(chunks: &[&str], finish: bool) -> String {
        let mut parser = ChatParser::new();
        let mut renderer = ResponsesRenderer::new();
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
    fn renders_basic_responses_stream() {
        let text = render(vec![
            StreamEvent::Start {
                id: "chatcmpl-9".to_string(),
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
        assert!(text.contains("event: response.created"));
        assert!(text.contains("\"type\":\"response.reasoning_text.delta\""));
        assert!(text.contains("\"delta\":\"We\""));
        assert!(text.contains("\"type\":\"response.output_text.delta\""));
        assert!(text.contains("\"delta\":\"Hi\""));
        assert!(text.contains("event: response.completed"));
        assert_eq!(text.matches("event: response.completed").count(), 1);
        assert!(text.contains("\"sequence_number\":1"));
        assert!(text.contains("\"sequence_number\":7"));
    }

    #[test]
    fn renders_tool_calls_with_full_arguments_in_done() {
        let text = render(vec![
            StreamEvent::Start {
                id: "chatcmpl-9".to_string(),
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
        assert!(text.contains("\"type\":\"function_call\""));
        assert!(text.contains("\"call_id\":\"call_1\""));
        assert!(text.contains("\"type\":\"response.function_call_arguments.delta\""));
        assert!(text.contains("\"type\":\"response.function_call_arguments.done\""));
        assert!(text.contains("\"arguments\":\"{\\\"city\\\": \\\"Paris\\\"}\""));
        assert!(text.contains("\"name\":\"get_weather\""));
        assert!(text.contains("event: response.completed"));
    }

    #[test]
    fn renders_error_event_without_completed() {
        let text = render(vec![
            StreamEvent::Start {
                id: "x".to_string(),
                model: "m".to_string(),
            },
            StreamEvent::TextDelta {
                text: "Hi".to_string(),
            },
            StreamEvent::Error {
                message: "upstream exploded".to_string(),
                code: Some("upstream_error".to_string()),
            },
        ]);
        assert!(text.contains("event: error"));
        assert!(text.contains("\"code\":\"upstream_error\""));
        assert!(text.contains("\"message\":\"upstream exploded\""));
        assert!(!text.contains("event: response.completed"));
    }

    #[test]
    fn exposes_assistant_turn_with_reasoning() {
        let mut renderer = ResponsesRenderer::new();
        let mut out = Vec::new();
        renderer.on_event(
            &StreamEvent::Start {
                id: "x".to_string(),
                model: "m".to_string(),
            },
            &mut out,
        );
        renderer.on_event(
            &StreamEvent::ReasoningDelta {
                text: "think".to_string(),
                signature: None,
            },
            &mut out,
        );
        renderer.on_event(
            &StreamEvent::ToolCallStart {
                index: 0,
                id: "call_1".to_string(),
                name: "f".to_string(),
            },
            &mut out,
        );
        renderer.on_event(
            &StreamEvent::ToolCallArgsDelta {
                index: 0,
                args: "{}".to_string(),
            },
            &mut out,
        );
        renderer.on_event(
            &StreamEvent::End {
                stop_reason: StopReason::ToolUse,
                usage: ChatUsage::default(),
            },
            &mut out,
        );
        let turn = renderer.assistant_turn().expect("assistant turn recorded");
        assert_eq!(turn["reasoning_content"], "think");
        assert_eq!(turn["tool_calls"][0]["function"]["name"], "f");
    }

    #[test]
    fn converts_messages_response_to_responses_non_streaming() {
        let body = json!({
            "id": "msg_123",
            "type": "message",
            "role": "assistant",
            "model": "m",
            "content": [
                {"type": "thinking", "thinking": "let me think", "signature": "s"},
                {"type": "text", "text": "Hello"},
                {"type": "tool_use", "id": "toolu_1", "name": "get_weather", "input": {"city": "Paris"}}
            ],
            "stop_reason": "tool_use",
            "usage": {"input_tokens": 10, "output_tokens": 5, "cache_read_input_tokens": 2}
        });
        let out = convert_messages_response_to_responses(&body).unwrap();
        assert_eq!(out["id"], "resp_msg_123");
        assert_eq!(out["status"], "completed");
        assert_eq!(out["output"][0]["type"], "reasoning");
        assert_eq!(out["output"][1]["type"], "message");
        assert_eq!(out["output"][1]["content"][0]["text"], "Hello");
        assert_eq!(out["output"][2]["type"], "function_call");
        assert_eq!(out["output"][2]["name"], "get_weather");
        assert_eq!(out["usage"]["input_tokens"], 10);
        assert_eq!(out["usage"]["input_tokens_details"]["cached_tokens"], 2);
    }

    #[test]
    fn converts_messages_response_incomplete_on_max_tokens() {
        let body = json!({
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "model": "m",
            "content": [{"type": "text", "text": "partial"}],
            "stop_reason": "max_tokens",
            "usage": {"input_tokens": 1, "output_tokens": 1}
        });
        let out = convert_messages_response_to_responses(&body).unwrap();
        assert_eq!(out["status"], "incomplete");
        assert_eq!(out["incomplete_details"]["reason"], "max_output_tokens");
    }

    #[test]
    fn pipeline_converts_chunks_to_responses_events() {
        let text = pipeline(
            &[
                r#"{"id":"x","object":"chat.completion.chunk","model":"gpt-5.6","choices":[{"index":0,"delta":{"role":"assistant","content":null,"reasoning_content":""},"finish_reason":null}]}"#,
                r#"{"id":"x","choices":[{"index":0,"delta":{"content":null,"reasoning_content":"We"},"finish_reason":null}]}"#,
                r#"{"id":"x","choices":[{"index":0,"delta":{"content":"Hi","reasoning_content":null},"finish_reason":null}]}"#,
                r#"{"id":"x","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":null}"#,
                r#"{"id":"x","choices":[],"usage":{"prompt_tokens":6,"completion_tokens":9}}"#,
                "[DONE]",
            ],
            true,
        );
        assert!(text.contains("event: response.created"));
        assert!(text.contains("event: response.in_progress"));
        assert!(text.contains("\"type\":\"response.reasoning_text.delta\""));
        assert!(text.contains("\"delta\":\"We\""));
        assert!(text.contains("\"type\":\"response.output_text.delta\""));
        assert!(text.contains("\"delta\":\"Hi\""));
        assert!(text.contains("event: response.completed"));
        assert_eq!(text.matches("event: response.completed").count(), 1);
        assert!(text.contains("\"sequence_number\":1"));
        assert!(text.contains("\"sequence_number\":7"));
    }

    #[test]
    fn pipeline_converts_tool_calls() {
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
        assert!(text.contains("\"type\":\"function_call\""));
        assert!(text.contains("\"call_id\":\"call_1\""));
        assert!(text.contains("\"type\":\"response.function_call_arguments.delta\""));
        assert!(text.contains("\"type\":\"response.function_call_arguments.done\""));
        assert!(text.contains("\"arguments\":\"{\\\"city\\\": \\\"Paris\\\"}\""));
        assert!(text.contains("\"name\":\"get_weather\""));
        assert!(text.contains("event: response.completed"));
    }

    #[test]
    fn pipeline_exposes_response_id_and_assistant_turn() {
        let mut parser = ChatParser::new();
        let mut renderer = ResponsesRenderer::new();
        let mut out = Vec::new();
        for chunk in [
            r#"{"id":"chatcmpl-9","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":""}}]},"finish_reason":null}]}"#,
            r#"{"id":"chatcmpl-9","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"city\":\"Paris\"}"}}]},"finish_reason":null}]}"#,
            r#"{"id":"chatcmpl-9","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}],"usage":null}"#,
            "[DONE]",
        ] {
            let mut events = Vec::new();
            parser.on_chunk(chunk, &mut events).unwrap();
            for event in &events {
                renderer.on_event(event, &mut out);
            }
        }
        assert_eq!(renderer.response_id(), Some("resp_chatcmpl-9"));
        let turn = renderer.assistant_turn().expect("assistant turn recorded");
        assert_eq!(turn["role"], "assistant");
        assert_eq!(turn["tool_calls"][0]["function"]["name"], "get_weather");
        assert_eq!(
            turn["tool_calls"][0]["function"]["arguments"],
            r#"{"city":"Paris"}"#
        );
    }

    #[test]
    fn pipeline_assistant_turn_carries_text() {
        let mut parser = ChatParser::new();
        let mut renderer = ResponsesRenderer::new();
        let mut out = Vec::new();
        for chunk in [
            r#"{"id":"x","choices":[{"index":0,"delta":{"content":"Hi "},"finish_reason":null}]}"#,
            r#"{"id":"x","choices":[{"index":0,"delta":{"content":"there"},"finish_reason":"stop"}]}"#,
        ] {
            let mut events = Vec::new();
            parser.on_chunk(chunk, &mut events).unwrap();
            for event in &events {
                renderer.on_event(event, &mut out);
            }
        }
        let mut events = Vec::new();
        parser.finish(&mut events);
        for event in &events {
            renderer.on_event(event, &mut out);
        }
        renderer.finish(&mut out);
        let turn = renderer.assistant_turn().expect("assistant turn recorded");
        assert_eq!(turn["content"], "Hi there");
        assert!(turn.get("tool_calls").is_none());
    }

    #[test]
    fn pipeline_error_event_emits_error_and_no_completed() {
        let text = pipeline(
            &[
                r#"{"id":"x","choices":[{"index":0,"delta":{"content":"Hi"},"finish_reason":null}]}"#,
                r#"{"error":{"message":"upstream exploded","type":"server_error","code":"upstream_error"}}"#,
                "[DONE]",
            ],
            true,
        );
        assert!(text.contains("event: error"));
        assert!(text.contains("\"code\":\"upstream_error\""));
        assert!(text.contains("\"message\":\"upstream exploded\""));
        assert!(!text.contains("event: response.completed"));
    }

    #[test]
    fn pipeline_finish_without_done_emits_completed() {
        let text = pipeline(
            &[r#"{"id":"x","choices":[{"index":0,"delta":{"content":"hi"}}]}"#],
            true,
        );
        assert!(text.contains("event: response.completed"));
        assert_eq!(text.matches("event: response.completed").count(), 1);
    }
}
