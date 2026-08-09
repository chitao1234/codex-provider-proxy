//! Protocol-independent streaming response events (the semantic IR).
//!
//! Upstream protocol parsers (Chat Completions, Anthropic Messages, third-party
//! Responses) translate their SSE streams into `StreamEvent`s; downstream renderers
//! (Anthropic Messages, official OpenAI Responses) translate the event stream back
//! into their protocol's SSE. This decouples N upstream protocols from M downstream
//! protocols: N parsers + M renderers instead of N×M converters, and the semantic
//! IR preserves meaning that a Chat-Completions-shaped intermediate would lose
//! (thinking signatures, item ids, server-tool calls, usage granularity).

use std::collections::BTreeMap;

use serde_json::Map;
use serde_json::Value;

use crate::chat::ChatUsage;
use crate::request_ir::{ContentExtensions, IrContent, IrToolCall, RequestIr};

/// Why the response ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// Natural end (Anthropic `end_turn` / chat `stop`).
    Stop,
    /// The model requested tool calls (Anthropic `tool_use` / chat `tool_calls`).
    ToolUse,
    /// Output token budget exhausted (Anthropic `max_tokens` / chat `length`).
    MaxTokens,
    /// Content filter / safety stop.
    ContentFilter,
    /// Any other reason.
    Other,
}

/// One protocol-independent streaming response event.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    /// The response started; carries the upstream id/model for downstream id synthesis.
    Start { id: String, model: String },
    /// Increment of reasoning/thinking text. `signature` is present when the upstream
    /// protocol carries one (Anthropic Messages thinking blocks).
    ReasoningDelta {
        text: String,
        signature: Option<String>,
    },
    /// Increment of visible output text.
    TextDelta { text: String },
    /// Increment of a model refusal.
    RefusalDelta { refusal: String },
    /// A tool call began (id + name known; arguments may follow incrementally).
    ToolCallStart {
        index: usize,
        id: String,
        name: String,
        opaque_extensions: Map<String, Value>,
        function_extensions: Map<String, Value>,
    },
    /// Additional opaque metadata for a tool call that arrived after its start.
    ToolCallExtensions {
        index: usize,
        opaque_extensions: Map<String, Value>,
        function_extensions: Map<String, Value>,
    },
    /// Increment of a tool call's arguments (never cumulative).
    ToolCallArgsDelta { index: usize, args: String },
    /// The response ended, with the stop reason and final usage.
    End {
        stop_reason: StopReason,
        /// Original protocol/provider reason before normalization.
        protocol_reason: Option<String>,
        usage: ChatUsage,
    },
    /// An upstream stream error; the renderer terminates without faking a completion.
    Error {
        message: String,
        code: Option<String>,
    },
    /// Protocol-specific information not yet modeled by the semantic IR.
    /// Extensions remain explicitly namespaced instead of being coerced into text,
    /// reasoning, or function-call fields.
    Extension {
        namespace: String,
        kind: String,
        payload: Value,
    },
}

#[derive(Debug, Clone, Default)]
pub struct StreamingResponseIr {
    upstream_id: String,
    reasoning: String,
    reasoning_signature: Option<String>,
    text: String,
    refusal: String,
    tools: BTreeMap<usize, StreamingToolCall>,
    stop_reason: Option<StopReason>,
    protocol_stop_reason: Option<String>,
    failed: bool,
    extensions: Vec<(String, Value)>,
    continuation_extensions: Vec<(String, Value)>,
}

#[derive(Debug, Clone)]
struct StreamingToolCall {
    id: String,
    name: String,
    arguments: String,
    opaque_extensions: Map<String, Value>,
    function_extensions: Map<String, Value>,
}

impl StreamingResponseIr {
    pub fn on_event(&mut self, event: &StreamEvent) {
        match event {
            StreamEvent::Start { id, .. } => self.upstream_id.clone_from(id),
            StreamEvent::ReasoningDelta { text, signature } => {
                self.reasoning.push_str(text);
                if let Some(signature) = signature {
                    self.reasoning_signature = Some(signature.clone());
                }
            }
            StreamEvent::TextDelta { text } => self.text.push_str(text),
            StreamEvent::RefusalDelta { refusal } => self.refusal.push_str(refusal),
            StreamEvent::ToolCallStart {
                index,
                id,
                name,
                opaque_extensions,
                function_extensions,
            } => {
                self.tools.insert(
                    *index,
                    StreamingToolCall {
                        id: id.clone(),
                        name: name.clone(),
                        arguments: String::new(),
                        opaque_extensions: opaque_extensions.clone(),
                        function_extensions: function_extensions.clone(),
                    },
                );
            }
            StreamEvent::ToolCallArgsDelta { index, args } => {
                if let Some(tool) = self.tools.get_mut(index) {
                    tool.arguments.push_str(args);
                }
            }
            StreamEvent::ToolCallExtensions {
                index,
                opaque_extensions,
                function_extensions,
            } => {
                if let Some(tool) = self.tools.get_mut(index) {
                    tool.opaque_extensions.extend(opaque_extensions.clone());
                    tool.function_extensions.extend(function_extensions.clone());
                }
            }
            StreamEvent::End {
                stop_reason,
                protocol_reason,
                ..
            } => {
                self.stop_reason = Some(*stop_reason);
                self.protocol_stop_reason.clone_from(protocol_reason);
            }
            StreamEvent::Error { .. } => self.failed = true,
            StreamEvent::Extension {
                namespace,
                kind,
                payload,
            } => {
                if namespace == "openai.responses" && kind == "response.output_item.done" {
                    if let Some(item) = payload.get("item") {
                        let item = item.clone();
                        self.extensions.push((namespace.clone(), item.clone()));
                        self.continuation_extensions.push((namespace.clone(), item));
                    }
                } else {
                    self.extensions.push((
                        namespace.clone(),
                        serde_json::json!({"kind": kind, "payload": payload}),
                    ));
                }
            }
        }
    }

    pub fn openai_response_id(&self) -> Option<String> {
        if self.upstream_id.is_empty() || self.failed || self.stop_reason.is_none() {
            return None;
        }
        Some(if self.upstream_id.starts_with("resp_") {
            self.upstream_id.clone()
        } else {
            format!("resp_{}", self.upstream_id)
        })
    }

    pub fn has_started(&self) -> bool {
        !self.upstream_id.is_empty()
    }

    pub fn is_terminal(&self) -> bool {
        self.failed || self.stop_reason.is_some()
    }

    pub fn stop_reason(&self) -> Option<StopReason> {
        self.stop_reason
    }

    pub fn protocol_stop_reason(&self) -> Option<&str> {
        self.protocol_stop_reason.as_deref()
    }

    pub fn protocol_extensions(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.extensions
            .iter()
            .map(|(namespace, value)| (namespace.as_str(), value))
    }

    pub fn last_reasoning(&self) -> Option<&str> {
        (!self.reasoning.trim().is_empty()).then_some(self.reasoning.as_str())
    }

    pub fn append_to_request(&self, request: &mut RequestIr) {
        if self.failed || self.stop_reason.is_none() {
            return;
        }
        let mut content = Vec::new();
        if !self.reasoning.is_empty() || self.reasoning_signature.is_some() {
            content.push(IrContent::Thinking {
                id: None,
                status: Some("completed".to_owned()),
                text: self.reasoning.clone(),
                signature: self.reasoning_signature.clone(),
                extensions: ContentExtensions::default(),
            });
        }
        if !self.text.is_empty() {
            content.push(IrContent::Text {
                text: self.text.clone(),
                extensions: ContentExtensions::default(),
            });
        }
        if !self.refusal.is_empty() {
            content.push(IrContent::Refusal {
                refusal: self.refusal.clone(),
                extensions: ContentExtensions::default(),
            });
        }
        let tool_calls = self
            .tools
            .values()
            .map(|tool| IrToolCall {
                id: None,
                call_id: Some(tool.id.clone()),
                name: tool.name.clone(),
                arguments: serde_json::Value::String(tool.arguments.clone()),
                status: Some("completed".to_owned()),
                opaque_extensions: tool.opaque_extensions.clone(),
                function_extensions: tool.function_extensions.clone(),
            })
            .collect();
        request.append_assistant_output(
            content,
            None,
            Some("completed".to_owned()),
            tool_calls,
            Map::new(),
        );
        for (namespace, value) in &self.continuation_extensions {
            request.append_extension_item(namespace.clone(), value.clone());
        }
    }
}

impl StreamEvent {
    /// The upstream id of a Start event (default empty).
    pub fn start_id(&self) -> &str {
        match self {
            StreamEvent::Start { id, .. } => id,
            _ => "",
        }
    }
}

/// Map a Chat Completions `finish_reason` to the semantic stop reason.
pub fn map_chat_finish_reason(reason: Option<&str>, saw_tool_call: bool) -> StopReason {
    match reason {
        Some("stop") => StopReason::Stop,
        Some("tool_calls") if saw_tool_call => StopReason::ToolUse,
        Some("tool_calls") => StopReason::ToolUse,
        Some("length") => StopReason::MaxTokens,
        Some("content_filter") => StopReason::ContentFilter,
        _ => StopReason::Other,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::dialect::ModelCapabilities;
    use crate::request_ir::Protocol;

    #[test]
    fn accumulates_stream_into_protocol_neutral_continuation() {
        let mut response = StreamingResponseIr::default();
        for event in [
            StreamEvent::Start {
                id: "chatcmpl_1".to_owned(),
                model: "m".to_owned(),
            },
            StreamEvent::ReasoningDelta {
                text: "thought".to_owned(),
                signature: None,
            },
            StreamEvent::ReasoningDelta {
                text: String::new(),
                signature: Some("sig".to_owned()),
            },
            StreamEvent::TextDelta {
                text: "answer".to_owned(),
            },
            StreamEvent::ToolCallStart {
                index: 0,
                id: "call_1".to_owned(),
                name: "lookup".to_owned(),
                opaque_extensions: Map::from_iter([("x_call".to_owned(), json!(1))]),
                function_extensions: Map::from_iter([("x_function".to_owned(), json!(2))]),
            },
            StreamEvent::ToolCallArgsDelta {
                index: 0,
                args: "{\"q\":1}".to_owned(),
            },
            StreamEvent::ToolCallExtensions {
                index: 0,
                opaque_extensions: Map::from_iter([("x_late".to_owned(), json!(3))]),
                function_extensions: Map::from_iter([("x_function_late".to_owned(), json!(4))]),
            },
            StreamEvent::End {
                stop_reason: StopReason::ToolUse,
                protocol_reason: Some("tool_calls".to_owned()),
                usage: ChatUsage::default(),
            },
            StreamEvent::Extension {
                namespace: "openai.chat_completions".to_owned(),
                kind: "chat.completion.chunk".to_owned(),
                payload: json!({"root":{"service_tier":"priority"}}),
            },
        ] {
            response.on_event(&event);
        }

        assert_eq!(
            response.openai_response_id().as_deref(),
            Some("resp_chatcmpl_1")
        );
        assert_eq!(response.stop_reason(), Some(StopReason::ToolUse));
        assert_eq!(response.protocol_stop_reason(), Some("tool_calls"));
        assert_eq!(
            response.protocol_extensions().next(),
            Some((
                "openai.chat_completions",
                &json!({
                    "kind":"chat.completion.chunk",
                    "payload":{"root":{"service_tier":"priority"}}
                })
            ))
        );
        let mut request = RequestIr::parse(
            Protocol::AnthropicMessages,
            &json!({"model":"m", "max_tokens":16, "messages":[{"role":"user", "content":"hi"}]}),
        )
        .unwrap();
        response.append_to_request(&mut request);
        let rendered = request
            .render_with_report(Protocol::AnthropicMessages, &ModelCapabilities::default())
            .unwrap()
            .0;
        let assistant = &rendered["messages"][1];
        assert_eq!(assistant["content"][0]["thinking"], "thought");
        assert_eq!(assistant["content"][0]["signature"], "sig");
        assert_eq!(assistant["content"][1]["text"], "answer");
        assert_eq!(assistant["content"][2]["id"], "call_1");
        assert_eq!(assistant["content"][2]["x_call"], 1);
        assert_eq!(assistant["content"][2]["x_late"], 3);
        assert_eq!(assistant["content"][2]["function"]["x_function"], 2);
        assert_eq!(assistant["content"][2]["function"]["x_function_late"], 4);
    }

    #[test]
    fn does_not_create_continuation_for_empty_or_failed_stream() {
        let mut response = StreamingResponseIr::default();
        response.on_event(&StreamEvent::Start {
            id: String::new(),
            model: "m".to_owned(),
        });
        response.on_event(&StreamEvent::End {
            stop_reason: StopReason::Stop,
            protocol_reason: Some("stop".to_owned()),
            usage: ChatUsage::default(),
        });
        assert_eq!(response.openai_response_id(), None);

        response.on_event(&StreamEvent::Error {
            message: "failed".to_owned(),
            code: None,
        });
        assert_eq!(response.openai_response_id(), None);
    }

    #[test]
    fn keeps_stream_extensions_out_of_responses_continuation_input() {
        let mut response = StreamingResponseIr::default();
        for event in [
            StreamEvent::Start {
                id: "resp_1".to_owned(),
                model: "m".to_owned(),
            },
            StreamEvent::Extension {
                namespace: "openai.responses".to_owned(),
                kind: "response.web_search_call.searching".to_owned(),
                payload: json!({"type":"response.web_search_call.searching","output_index":0}),
            },
            StreamEvent::Extension {
                namespace: "openai.responses".to_owned(),
                kind: "response.output_item.done".to_owned(),
                payload: json!({
                    "type":"response.output_item.done",
                    "output_index":0,
                    "item":{"type":"web_search_call","id":"ws_1","status":"completed"}
                }),
            },
            StreamEvent::End {
                stop_reason: StopReason::Stop,
                protocol_reason: Some("completed".to_owned()),
                usage: ChatUsage::default(),
            },
        ] {
            response.on_event(&event);
        }

        let mut request = RequestIr::parse(
            Protocol::OpenAiResponses,
            &json!({"model":"m","input":"next"}),
        )
        .unwrap();
        response.append_to_request(&mut request);
        let rendered = request
            .render_with_report(Protocol::OpenAiResponses, &ModelCapabilities::default())
            .unwrap()
            .0;
        let input = rendered["input"].as_array().unwrap();
        assert!(input.iter().any(|item| item["id"] == "ws_1"));
        assert!(!input.iter().any(|item| {
            item["kind"] == "response.web_search_call.searching"
                || item["type"] == "response.web_search_call.searching"
        }));
    }
}
