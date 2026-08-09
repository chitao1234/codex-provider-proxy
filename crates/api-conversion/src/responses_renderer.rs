//! OpenAI Responses SSE renderer: semantic `StreamEvent`s into official Responses SSE.
//!
//! Extracted from the former `ResponsesStreamConverter`: this renderer owns the
//! downstream Responses concerns (output item lifecycle, sequence numbers,
//! function_call items with accumulated arguments, the assistant turn for transcript
//! storage). The upstream parsing (chat chunks) is the parsers' job.

use std::collections::HashMap;

use bytes::Bytes;
use serde_json::{json, Map, Value};

use crate::chat::ChatUsage;
use crate::sse::encode_sse_event;
use crate::stream::{StopReason, StreamEvent};

/// Accumulated state of one function_call item, for the closing events.
struct FunctionCallState {
    item_id: String,
    call_id: String,
    name: String,
    arguments: String,
    output_index: usize,
    opaque_extensions: Map<String, Value>,
    function_extensions: Map<String, Value>,
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
            usage: ChatUsage::default(),
        }
    }

    /// The synthesized downstream `response_id`, available once the stream started.
    pub fn response_id(&self) -> Option<&str> {
        (self.started && !self.response_id.is_empty()).then_some(self.response_id.as_str())
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
                if text.is_empty() {
                    return;
                }
                self.ensure_reasoning_item(out);
                self.reasoning_text.push_str(text);
                let index = self.reasoning_item.unwrap_or(0);
                self.emit(
                    "response.reasoning_text.delta",
                    &json!({
                        "type": "response.reasoning_text.delta",
                        "item_id": format!("rs_{}", self.response_id_suffix()),
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
                        "item_id": format!("msg_{}", self.response_id_suffix()),
                        "output_index": index,
                        "content_index": 0,
                        "delta": text,
                    }),
                    out,
                );
            }
            StreamEvent::ToolCallStart {
                index,
                id,
                name,
                opaque_extensions,
                function_extensions,
            } => {
                let item_id = format!("fc_{}_{}", self.response_id_suffix(), index);
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
                        opaque_extensions: opaque_extensions.clone(),
                        function_extensions: function_extensions.clone(),
                    },
                );
                let mut item = opaque_extensions.clone();
                if !function_extensions.is_empty() {
                    item.insert(
                        "function".to_owned(),
                        Value::Object(function_extensions.clone()),
                    );
                }
                item.insert("id".to_owned(), json!(item_id));
                item.insert("type".to_owned(), json!("function_call"));
                item.insert("status".to_owned(), json!("in_progress"));
                item.insert("call_id".to_owned(), json!(id));
                item.insert("name".to_owned(), json!(name));
                item.insert("arguments".to_owned(), json!(""));
                self.emit(
                    "response.output_item.added",
                    &json!({
                        "type": "response.output_item.added",
                        "output_index": output_index,
                        "item": item,
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
            StreamEvent::End {
                stop_reason, usage, ..
            } => {
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
            StreamEvent::Extension { .. } => {}
        }
    }

    /// Mark transport EOF without inventing a protocol-level completion.
    pub fn finish(&mut self, _out: &mut Vec<Bytes>) {
        self.finished = true;
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
                    "id": format!("rs_{}", self.response_id_suffix()),
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
                    "id": format!("msg_{}", self.response_id_suffix()),
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
                    "id": format!("rs_{}", self.response_id_suffix()),
                    "type": "reasoning",
                    "status": "completed",
                    "summary": [],
                },
            }));
        }
        self.reasoning_text.clear();
        if let Some(index) = self.message_item.take() {
            let text = std::mem::take(&mut self.message_text);
            items_done.push(json!({
                "type": "response.output_item.done",
                "output_index": index,
                "item": {
                    "id": format!("msg_{}", self.response_id_suffix()),
                    "type": "message",
                    "status": "completed",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": text, "annotations": []}],
                },
            }));
        }
        // Function call items: close each with the accumulated arguments
        // (function_call_arguments.done), then emit the completed item carrying
        // the full name/arguments for clients that rebuild calls from done events.
        let fc_indices: Vec<usize> = self.function_calls.keys().copied().collect();
        for index in fc_indices {
            let Some(state) = self.function_calls.remove(&index) else {
                continue;
            };
            args_done.push(json!({
                "type": "response.function_call_arguments.done",
                "item_id": state.item_id,
                "output_index": state.output_index,
                "arguments": state.arguments,
            }));
            let mut item = state.opaque_extensions;
            if !state.function_extensions.is_empty() {
                item.insert(
                    "function".to_owned(),
                    Value::Object(state.function_extensions),
                );
            }
            item.insert("id".to_owned(), json!(state.item_id));
            item.insert("type".to_owned(), json!("function_call"));
            item.insert("status".to_owned(), json!("completed"));
            item.insert("call_id".to_owned(), json!(state.call_id));
            item.insert("name".to_owned(), json!(state.name));
            item.insert("arguments".to_owned(), json!(state.arguments));
            items_done.push(json!({
                "type": "response.output_item.done",
                "output_index": state.output_index,
                "item": item,
            }));
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

    fn response_id_suffix(&self) -> &str {
        self.response_id
            .strip_prefix("resp_")
            .unwrap_or(&self.response_id)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chat_parser::ChatParser;
    use crate::request_ir::Protocol;
    use crate::response_ir::ResponseIr;

    fn convert_response(source: Protocol, target: Protocol, body: &Value) -> Value {
        ResponseIr::parse(source, body)
            .unwrap()
            .render(target)
            .unwrap()
    }

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
                protocol_reason: None,
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
                opaque_extensions: Map::new(),
                function_extensions: Map::new(),
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
                protocol_reason: None,
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
    fn missing_upstream_id_does_not_panic() {
        let text = render(vec![
            StreamEvent::Start {
                id: String::new(),
                model: "m".to_string(),
            },
            StreamEvent::TextDelta {
                text: "Hi".to_string(),
            },
            StreamEvent::End {
                stop_reason: StopReason::Stop,
                protocol_reason: None,
                usage: ChatUsage::default(),
            },
        ]);

        assert!(text.contains("event: response.completed"));
        assert!(text.contains("\"id\":\"msg_\""));
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
        let out = convert_response(
            Protocol::AnthropicMessages,
            Protocol::OpenAiResponses,
            &body,
        );
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
        let out = convert_response(
            Protocol::AnthropicMessages,
            Protocol::OpenAiResponses,
            &body,
        );
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
    fn pipeline_preserves_chat_tool_call_extensions() {
        let text = pipeline(
            &[
                r#"{"id":"x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","x_call":1,"function":{"name":"lookup","arguments":"{}","x_function":2}}]},"finish_reason":null}]}"#,
                r#"{"id":"x","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}"#,
                "[DONE]",
            ],
            true,
        );
        assert!(text.contains("\"x_call\":1"));
        assert!(text.contains("\"function\":{\"x_function\":2}"));
        assert_eq!(text.matches("\"x_call\":1").count(), 2);
    }

    #[test]
    fn pipeline_exposes_response_id() {
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
    fn pipeline_finish_without_done_does_not_emit_completed() {
        let text = pipeline(
            &[r#"{"id":"x","choices":[{"index":0,"delta":{"content":"hi"}}]}"#],
            true,
        );
        assert!(!text.contains("event: response.completed"));
    }
}
