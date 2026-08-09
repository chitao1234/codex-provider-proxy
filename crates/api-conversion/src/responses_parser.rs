//! OpenAI Responses SSE parser: upstream Responses SSE events (official or
//! third-party with deviations) into the semantic `StreamEvent` IR.
//!
//! This is the counterpart of `ResponsesRenderer`: it translates the upstream
//! protocol's event stream (response.created, output_item.added, output_text.delta,
//! reasoning_text.delta / reasoning_summary_text.delta, function_call_arguments.delta,
//! response.completed, error) into protocol-independent events.
//!
//! Third-party deviations absorbed here:
//! - Qwen streams reasoning as `response.reasoning_summary_text.delta` instead of
//!   `response.reasoning_text.delta`, and marks truncation as `completed` (the
//!   parser surfaces max-token truncation via the reasoning item's absence).
//! - DeepSeek/MiniMax ids have no `resp_`/`msg_`/`rs_` prefixes (kept as-is; the
//!   renderer synthesizes prefixes downstream).

use serde_json::Value;

use crate::chat::ChatUsage;
use crate::error::ConversionError;
use crate::stream::{StopReason, StreamEvent};

/// Accumulated state of one function_call item across events.
#[derive(Default)]
struct FunctionCallAccumulator {
    arguments: String,
    /// Arguments already emitted as deltas (increments only).
    emitted_len: usize,
}

/// Parse upstream Responses SSE events into `StreamEvent`s.
pub struct ResponsesParser {
    started: bool,
    finished: bool,
    response_id: String,
    model: String,
    /// output_index of the currently open item (reasoning/message/function_call).
    /// None when no item is open.
    open_item: Option<(usize, ItemKind)>,
    /// Tool index -> accumulated function_call state.
    function_calls: HashMap<usize, FunctionCallAccumulator>,
    /// Responses output_index -> protocol-neutral tool index.
    tool_items: HashMap<usize, usize>,
    /// Responses item/call id -> protocol-neutral tool index.
    tool_ids: HashMap<String, usize>,
    next_tool_index: usize,
    /// Whether the reasoning item is open (for reasoning_summary_text delta handling).
    reasoning_open: bool,
    /// Whether the message item is open.
    message_open: bool,
    usage: ChatUsage,
    /// Whether the response ended (response.completed / response.incomplete / error).
    end_emitted: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ItemKind {
    Reasoning,
    Message,
    FunctionCall,
}

use std::collections::HashMap;

impl Default for ResponsesParser {
    fn default() -> Self {
        Self::new()
    }
}

impl ResponsesParser {
    pub fn new() -> Self {
        Self {
            started: false,
            finished: false,
            response_id: String::new(),
            model: String::new(),
            open_item: None,
            function_calls: HashMap::new(),
            tool_items: HashMap::new(),
            tool_ids: HashMap::new(),
            next_tool_index: 0,
            reasoning_open: false,
            message_open: false,
            usage: ChatUsage::default(),
            end_emitted: false,
        }
    }

    /// Process one upstream SSE event, appending semantic events to `out`.
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
            "response.created" => {
                self.started = true;
                let response = data.get("response").and_then(Value::as_object);
                self.response_id = response
                    .and_then(|r| r.get("id"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                self.model = response
                    .and_then(|r| r.get("model"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                out.push(StreamEvent::Start {
                    id: self.response_id.clone(),
                    model: self.model.clone(),
                });
            }
            "response.in_progress" | "response.output_item.added" => {
                // output_item.added opens an item.
                let item = data.get("item").and_then(Value::as_object);
                let item_type = item
                    .and_then(|i| i.get("type"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let output_index = data
                    .get("output_index")
                    .and_then(Value::as_u64)
                    .map(|i| i as usize)
                    .unwrap_or_default();
                match item_type {
                    "reasoning" => {
                        self.reasoning_open = true;
                        self.open_item = Some((output_index, ItemKind::Reasoning));
                    }
                    "message" => {
                        self.message_open = true;
                        self.open_item = Some((output_index, ItemKind::Message));
                    }
                    "function_call" => {
                        let item_id = item
                            .and_then(|i| i.get("id"))
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        let id = item
                            .and_then(|i| i.get("call_id"))
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        let name = item
                            .and_then(|i| i.get("name"))
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        let tool_index = self.next_tool_index;
                        self.next_tool_index += 1;
                        self.function_calls
                            .insert(tool_index, FunctionCallAccumulator::default());
                        self.tool_items.insert(output_index, tool_index);
                        if !item_id.is_empty() {
                            self.tool_ids.insert(item_id.to_owned(), tool_index);
                        }
                        if !id.is_empty() {
                            self.tool_ids.insert(id.clone(), tool_index);
                        }
                        self.open_item = Some((output_index, ItemKind::FunctionCall));
                        out.push(StreamEvent::ToolCallStart {
                            index: tool_index,
                            id,
                            name,
                        });
                    }
                    _ => {}
                }
            }
            "response.content_part.added" | "response.reasoning_summary_part.added" => {}
            "response.reasoning_text.delta" | "response.reasoning_summary_text.delta" => {
                // Both official reasoning_text.delta and Qwen's
                // reasoning_summary_text.delta carry the thinking increment.
                if let Some(delta) = data.get("delta").and_then(Value::as_str) {
                    self.reasoning_open = true;
                    out.push(StreamEvent::ReasoningDelta {
                        text: delta.to_string(),
                        signature: None,
                    });
                }
            }
            "response.output_text.delta" => {
                if let Some(delta) = data.get("delta").and_then(Value::as_str) {
                    self.message_open = true;
                    out.push(StreamEvent::TextDelta {
                        text: delta.to_string(),
                    });
                }
            }
            "response.function_call_arguments.delta" => {
                if let Some(delta) = data.get("delta").and_then(Value::as_str) {
                    let index = self.resolve_tool_index(data);
                    if let Some(index) = index {
                        let acc = self.function_calls.get_mut(&index).expect("exists");
                        acc.arguments.push_str(delta);
                        if acc.emitted_len < acc.arguments.len() {
                            let increment = acc.arguments[acc.emitted_len..].to_string();
                            acc.emitted_len = acc.arguments.len();
                            if !increment.is_empty() {
                                out.push(StreamEvent::ToolCallArgsDelta {
                                    index,
                                    args: increment,
                                });
                            }
                        }
                    }
                }
            }
            "response.function_call_arguments.done" => {
                // Arguments complete; the accumulated value is final. Emit any
                // remaining increment.
                if let Some(delta) = data.get("arguments").and_then(Value::as_str) {
                    let index = self.resolve_tool_index(data);
                    if let Some(index) = index {
                        let acc = self.function_calls.get_mut(&index).expect("exists");
                        if delta.len() >= acc.arguments.len() && delta.starts_with(&acc.arguments) {
                            acc.arguments.clear();
                            acc.arguments.push_str(delta);
                        }
                        if acc.emitted_len < acc.arguments.len() {
                            let increment = acc.arguments[acc.emitted_len..].to_string();
                            acc.emitted_len = acc.arguments.len();
                            if !increment.is_empty() {
                                out.push(StreamEvent::ToolCallArgsDelta {
                                    index,
                                    args: increment,
                                });
                            }
                        }
                    }
                }
            }
            "response.output_item.done" => {
                let item = data.get("item");
                let item_type = item
                    .and_then(|item| item.get("type"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if item_type == "function_call" {
                    if let Some(arguments) = item
                        .and_then(|item| item.get("arguments"))
                        .and_then(Value::as_str)
                    {
                        if let Some(index) = self.resolve_tool_index(data) {
                            let acc = self.function_calls.get_mut(&index).expect("exists");
                            if arguments.len() >= acc.arguments.len()
                                && arguments.starts_with(&acc.arguments)
                            {
                                acc.arguments.clear();
                                acc.arguments.push_str(arguments);
                            }
                            if acc.emitted_len < acc.arguments.len() {
                                let increment = acc.arguments[acc.emitted_len..].to_owned();
                                acc.emitted_len = acc.arguments.len();
                                out.push(StreamEvent::ToolCallArgsDelta {
                                    index,
                                    args: increment,
                                });
                            }
                        }
                    }
                } else if !matches!(item_type, "reasoning" | "message") {
                    out.push(StreamEvent::Extension {
                        namespace: "openai.responses".to_owned(),
                        kind: event_type.to_owned(),
                        payload: data.clone(),
                    });
                }
                // Close the open item.
                if let Some((_, kind)) = self.open_item.take() {
                    match kind {
                        ItemKind::Reasoning => self.reasoning_open = false,
                        ItemKind::Message => self.message_open = false,
                        ItemKind::FunctionCall => {}
                    }
                }
            }
            "response.content_part.done" | "response.reasoning_text.done" => {
                if let Some((_, kind)) = self.open_item.take() {
                    match kind {
                        ItemKind::Reasoning => self.reasoning_open = false,
                        ItemKind::Message => self.message_open = false,
                        ItemKind::FunctionCall => {}
                    }
                }
            }
            "response.completed" | "response.incomplete" => {
                let response = data.get("response").and_then(Value::as_object);
                // usage from the completed response.
                if let Some(usage) = response.and_then(|r| r.get("usage")) {
                    self.usage = extract_responses_usage(usage);
                }
                let status = response
                    .and_then(|r| r.get("status"))
                    .and_then(Value::as_str)
                    .unwrap_or("completed");
                let stop_reason = if status == "incomplete" {
                    StopReason::MaxTokens
                } else {
                    StopReason::Stop
                };
                let protocol_reason = response
                    .and_then(|response| response.get("incomplete_details"))
                    .and_then(|details| details.get("reason"))
                    .and_then(Value::as_str)
                    .unwrap_or(status)
                    .to_owned();
                self.end_emitted = true;
                self.finished = true;
                out.push(StreamEvent::End {
                    stop_reason,
                    protocol_reason: Some(protocol_reason),
                    usage: self.usage,
                });
            }
            "error" => {
                self.finished = true;
                self.end_emitted = true;
                // Third-party error bodies vary: {error:{...}} (DeepSeek/MiniMax)
                // or {code, message} without wrapper (Qwen).
                let error = data.get("error").and_then(Value::as_object);
                let message = error
                    .and_then(|e| e.get("message"))
                    .and_then(Value::as_str)
                    .or_else(|| data.get("message").and_then(Value::as_str))
                    .unwrap_or("upstream request failed")
                    .to_string();
                let code = error
                    .and_then(|e| e.get("code"))
                    .and_then(Value::as_str)
                    .or_else(|| data.get("code").and_then(Value::as_str))
                    .map(str::to_owned);
                out.push(StreamEvent::Error { message, code });
            }
            _ => {
                out.push(StreamEvent::Extension {
                    namespace: "openai.responses".to_owned(),
                    kind: event_type.to_owned(),
                    payload: data.clone(),
                });
            }
        }
        Ok(())
    }

    /// Mark transport EOF without inventing a protocol-level completion.
    pub fn finish(&mut self, _out: &mut Vec<StreamEvent>) {
        self.finished = true;
    }

    fn resolve_tool_index(&self, data: &Value) -> Option<usize> {
        data.get("output_index")
            .and_then(Value::as_u64)
            .and_then(|index| self.tool_items.get(&(index as usize)).copied())
            .or_else(|| {
                ["item_id", "call_id"]
                    .into_iter()
                    .filter_map(|field| data.get(field).and_then(Value::as_str))
                    .find_map(|id| self.tool_ids.get(id).copied())
            })
            .or_else(|| {
                self.open_item.and_then(|(output_index, kind)| {
                    (kind == ItemKind::FunctionCall)
                        .then(|| self.tool_items.get(&output_index).copied())
                        .flatten()
                })
            })
            .or_else(|| {
                (self.function_calls.len() == 1)
                    .then(|| self.function_calls.keys().next().copied())
                    .flatten()
            })
    }
}

fn extract_responses_usage(usage: &Value) -> ChatUsage {
    let usage = usage.as_object();
    let u64_field = |field: &str| usage.and_then(|u| u.get(field)).and_then(Value::as_u64);
    ChatUsage {
        prompt_tokens: u64_field("input_tokens"),
        completion_tokens: u64_field("output_tokens"),
        cached_tokens: usage
            .and_then(|u| u.get("input_tokens_details"))
            .and_then(Value::as_object)
            .and_then(|d| d.get("cached_tokens"))
            .and_then(Value::as_u64),
        cache_write_tokens: None,
        prompt_cache_miss_tokens: None,
        reasoning_tokens: usage
            .and_then(|u| u.get("output_tokens_details"))
            .and_then(Value::as_object)
            .and_then(|d| d.get("reasoning_tokens"))
            .and_then(Value::as_u64),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_basic_responses_stream() {
        let mut parser = ResponsesParser::new();
        let mut out = Vec::new();
        parser
            .on_event(
                "response.created",
                &json!({"response": {"id": "resp_1", "model": "m"}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "response.output_item.added",
                &json!({"output_index": 0, "item": {"type": "reasoning", "id": "rs_1"}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "response.reasoning_text.delta",
                &json!({"item_id": "rs_1", "delta": "We"}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "response.output_item.added",
                &json!({"output_index": 1, "item": {"type": "message", "id": "msg_1"}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "response.output_text.delta",
                &json!({"item_id": "msg_1", "delta": "Hi"}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "response.completed",
                &json!({"response": {"status": "completed", "usage": {"input_tokens": 6, "output_tokens": 9, "output_tokens_details": {"reasoning_tokens": 3}}}}),
                &mut out,
            )
            .unwrap();

        assert!(
            matches!(&out[0], StreamEvent::Start { id, model } if id == "resp_1" && model == "m")
        );
        assert!(matches!(&out[1], StreamEvent::ReasoningDelta { text, .. } if text == "We"));
        assert!(matches!(&out[2], StreamEvent::TextDelta { text } if text == "Hi"));
        let end = out.last().unwrap();
        assert!(
            matches!(end, StreamEvent::End { stop_reason, usage, .. } if *stop_reason == StopReason::Stop && usage.reasoning_tokens == Some(3))
        );
    }

    #[test]
    fn parses_qwen_reasoning_summary_delta() {
        // Qwen streams reasoning as reasoning_summary_text.delta instead of
        // reasoning_text.delta.
        let mut parser = ResponsesParser::new();
        let mut out = Vec::new();
        parser
            .on_event(
                "response.created",
                &json!({"response": {"id": "resp_1", "model": "m"}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "response.reasoning_summary_text.delta",
                &json!({"summary_index": 0, "delta": "thinking"}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "response.output_text.delta",
                &json!({"delta": "answer"}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "response.completed",
                &json!({"response": {"status": "completed", "usage": {}}}),
                &mut out,
            )
            .unwrap();
        assert!(matches!(&out[1], StreamEvent::ReasoningDelta { text, .. } if text == "thinking"));
        assert!(matches!(&out[2], StreamEvent::TextDelta { text } if text == "answer"));
    }

    #[test]
    fn parses_function_call_with_increments() {
        let mut parser = ResponsesParser::new();
        let mut out = Vec::new();
        parser
            .on_event(
                "response.created",
                &json!({"response": {"id": "resp_1", "model": "m"}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "response.output_item.added",
                &json!({"output_index": 0, "item": {"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "get_weather", "arguments": ""}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "response.function_call_arguments.delta",
                &json!({"item_id": "fc_1", "delta": "{\"city\": \"Pa"}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "response.function_call_arguments.delta",
                &json!({"item_id": "fc_1", "delta": "ris\"}"}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "response.completed",
                &json!({"response": {"status": "completed", "usage": {}}}),
                &mut out,
            )
            .unwrap();

        assert!(
            matches!(&out[1], StreamEvent::ToolCallStart { index: 0, id, name } if id == "call_1" && name == "get_weather")
        );
        assert!(
            matches!(&out[2], StreamEvent::ToolCallArgsDelta { index: 0, args } if args == "{\"city\": \"Pa")
        );
        assert!(
            matches!(&out[3], StreamEvent::ToolCallArgsDelta { index: 0, args } if args == "ris\"}")
        );
    }

    #[test]
    fn routes_interleaved_function_calls_by_output_index() {
        let mut parser = ResponsesParser::new();
        let mut out = Vec::new();
        parser
            .on_event(
                "response.created",
                &json!({"response": {"id": "resp_1", "model": "m"}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "response.output_item.added",
                &json!({"output_index": 1, "item": {"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "first"}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "response.output_item.added",
                &json!({"output_index": 4, "item": {"type": "function_call", "id": "fc_2", "call_id": "call_2", "name": "second"}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "response.function_call_arguments.delta",
                &json!({"output_index": 4, "item_id": "fc_2", "delta": "{\"b\":"}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "response.function_call_arguments.delta",
                &json!({"output_index": 1, "item_id": "fc_1", "delta": "{\"a\":1}"}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "response.function_call_arguments.delta",
                &json!({"output_index": 4, "item_id": "fc_2", "delta": "2}"}),
                &mut out,
            )
            .unwrap();

        assert!(matches!(
            &out[3],
            StreamEvent::ToolCallArgsDelta { index: 1, args } if args == "{\"b\":"
        ));
        assert!(matches!(
            &out[4],
            StreamEvent::ToolCallArgsDelta { index: 0, args } if args == "{\"a\":1}"
        ));
        assert!(matches!(
            &out[5],
            StreamEvent::ToolCallArgsDelta { index: 1, args } if args == "2}"
        ));
    }

    #[test]
    fn preserves_unknown_events_as_namespaced_extensions() {
        let mut parser = ResponsesParser::new();
        let mut out = Vec::new();
        let event = json!({"output_index": 2, "delta": {"query": "rust"}});
        parser
            .on_event("response.web_search_call.searching", &event, &mut out)
            .unwrap();

        assert!(matches!(
            &out[0],
            StreamEvent::Extension { namespace, kind, payload }
                if namespace == "openai.responses"
                    && kind == "response.web_search_call.searching"
                    && payload == &event
        ));
    }

    #[test]
    fn parses_error_variants() {
        // DeepSeek/MiniMax style: {error: {...}}
        let mut parser = ResponsesParser::new();
        let mut out = Vec::new();
        parser
            .on_event(
                "error",
                &json!({"error": {"message": "boom", "code": "invalid_request_error"}}),
                &mut out,
            )
            .unwrap();
        assert!(
            matches!(&out[0], StreamEvent::Error { message, code } if message == "boom" && code.as_deref() == Some("invalid_request_error"))
        );

        // Qwen style: {code, message} without error wrapper.
        let mut parser = ResponsesParser::new();
        let mut out = Vec::new();
        parser
            .on_event(
                "error",
                &json!({"code": "InvalidParameter", "message": "bad model"}),
                &mut out,
            )
            .unwrap();
        assert!(
            matches!(&out[0], StreamEvent::Error { message, code } if message == "bad model" && code.as_deref() == Some("InvalidParameter"))
        );
    }

    #[test]
    fn incomplete_marks_max_tokens() {
        let mut parser = ResponsesParser::new();
        let mut out = Vec::new();
        parser
            .on_event(
                "response.created",
                &json!({"response": {"id": "resp_1", "model": "m"}}),
                &mut out,
            )
            .unwrap();
        parser
            .on_event(
                "response.incomplete",
                &json!({"response": {"status": "incomplete", "incomplete_details": {"reason": "max_output_tokens"}}}),
                &mut out,
            )
            .unwrap();
        assert!(
            matches!(out.last().unwrap(), StreamEvent::End { stop_reason, protocol_reason, .. }
                if *stop_reason == StopReason::MaxTokens
                    && protocol_reason.as_deref() == Some("max_output_tokens"))
        );
    }
}
