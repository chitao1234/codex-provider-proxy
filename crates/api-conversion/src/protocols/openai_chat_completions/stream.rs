use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::{
    error::ConversionError,
    ir::{
        CanonicalResponseEvent, ContentPart, Extension, ExtensionScope, ItemKind, ReplayPolicy,
        StopReason, ToolCall, Usage,
    },
    protocols::openai_chat_completions::support::{
        extract_usage, first_choice_delta, first_choice_finish_reason, is_usage_only_chunk,
        upstream_id, upstream_model,
    },
};

#[derive(Default)]
struct ToolState {
    id: Option<String>,
    name: Option<String>,
    arguments: String,
    emitted: usize,
    opaque: Map<String, Value>,
    function: Map<String, Value>,
}

pub struct Parser {
    started: bool,
    finished: bool,
    tools: BTreeMap<usize, ToolState>,
    usage: Usage,
    pending_stop: Option<(StopReason, String)>,
    message_started: bool,
    reasoning_started: bool,
    refusal_started: bool,
}

impl Default for Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl Parser {
    pub fn new() -> Self {
        Self {
            started: false,
            finished: false,
            tools: BTreeMap::new(),
            usage: Usage::default(),
            pending_stop: None,
            message_started: false,
            reasoning_started: false,
            refusal_started: false,
        }
    }

    pub fn on_chunk(
        &mut self,
        payload: &str,
        out: &mut Vec<CanonicalResponseEvent>,
    ) -> Result<(), ConversionError> {
        if self.finished {
            return Ok(());
        }
        if payload == "[DONE]" {
            self.finish(out);
            return Ok(());
        }
        let chunk: Value = serde_json::from_str(payload)
            .map_err(|_| ConversionError::invalid("upstream SSE chunk is not valid JSON"))?;
        if let Some(error) = chunk.get("error").and_then(Value::as_object) {
            self.finished = true;
            out.push(CanonicalResponseEvent::StreamError {
                message: error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("upstream request failed")
                    .into(),
                code: error.get("code").and_then(Value::as_str).map(str::to_owned),
            });
            return Ok(());
        }
        if !self.started {
            self.started = true;
            out.push(CanonicalResponseEvent::StreamStarted {
                response_id: upstream_id(&chunk).map(str::to_owned),
                model: upstream_model(&chunk).map(str::to_owned),
            });
        }
        if let Some(delta) = first_choice_delta(&chunk).and_then(Value::as_object) {
            self.delta(delta, out);
        }
        let usage = extract_usage(&chunk);
        if !usage.is_empty() {
            self.usage = Usage {
                input_tokens: usage.prompt_tokens,
                output_tokens: usage.completion_tokens,
                total_tokens: usage
                    .prompt_tokens
                    .zip(usage.completion_tokens)
                    .map(|(input, output)| input + output),
                cached_input_tokens: usage.cached_tokens,
                cache_write_input_tokens: usage.cache_write_tokens,
                cache_miss_input_tokens: usage.prompt_cache_miss_tokens,
                reasoning_tokens: usage.reasoning_tokens,
                extensions: Vec::new(),
            };
            out.push(CanonicalResponseEvent::UsageUpdated(self.usage.clone()));
        }
        if let Some(reason) = first_choice_finish_reason(&chunk) {
            self.pending_stop = Some((
                map_stop(reason, self.tools.values().any(|tool| tool.name.is_some())),
                reason.into(),
            ));
            if !self.usage_is_empty() {
                self.finish(out);
            }
        }
        if is_usage_only_chunk(&chunk) {
            self.finish(out);
        }
        if let Some(extension) = chunk_extension(&chunk) {
            out.push(CanonicalResponseEvent::ProtocolExtension(extension));
        }
        Ok(())
    }

    pub fn finish(&mut self, out: &mut Vec<CanonicalResponseEvent>) {
        if self.finished {
            return;
        }
        self.finished = true;
        for (index, state) in &mut self.tools {
            if state.emitted < state.arguments.len() {
                let args = state.arguments[state.emitted..].to_owned();
                state.emitted = state.arguments.len();
                out.push(CanonicalResponseEvent::ToolArgumentsDelta {
                    item_id: format!("tool:{index}"),
                    arguments: args,
                });
            }
        }
        let (stop_reason, original_reason) = self
            .pending_stop
            .take()
            .unwrap_or((StopReason::Natural, "stop".into()));
        out.push(CanonicalResponseEvent::UsageUpdated(self.usage.clone()));
        out.push(CanonicalResponseEvent::ResponseCompleted {
            stop_reason,
            original_reason: Some(original_reason),
        });
    }

    fn usage_is_empty(&self) -> bool {
        self.usage.input_tokens.is_none()
            && self.usage.output_tokens.is_none()
            && self.usage.total_tokens.is_none()
    }

    fn delta(&mut self, delta: &Map<String, Value>, out: &mut Vec<CanonicalResponseEvent>) {
        if let Some(text) = delta.get("content").and_then(Value::as_str) {
            if !self.message_started {
                self.message_started = true;
                out.push(CanonicalResponseEvent::ItemStarted {
                    item_id: "message".into(),
                    provider_id: None,
                    item: ItemKind::Message,
                });
            }
            out.push(CanonicalResponseEvent::ContentDelta {
                item_id: "message".into(),
                content: ContentPart::Text(text.into()),
            });
        }
        if let Some(text) = delta.get("reasoning_content").and_then(Value::as_str) {
            if !self.reasoning_started {
                self.reasoning_started = true;
                out.push(CanonicalResponseEvent::ItemStarted {
                    item_id: "reasoning".into(),
                    provider_id: None,
                    item: ItemKind::Reasoning,
                });
            }
            out.push(CanonicalResponseEvent::ContentDelta {
                item_id: "reasoning".into(),
                content: ContentPart::Reasoning {
                    text: text.into(),
                    signature: None,
                },
            });
        }
        if let Some(text) = delta.get("refusal").and_then(Value::as_str) {
            if !self.refusal_started {
                self.refusal_started = true;
                out.push(CanonicalResponseEvent::ItemStarted {
                    item_id: "refusal".into(),
                    provider_id: None,
                    item: ItemKind::Refusal,
                });
            }
            out.push(CanonicalResponseEvent::ContentDelta {
                item_id: "refusal".into(),
                content: ContentPart::Refusal(text.into()),
            });
        }
        for call in delta
            .get("tool_calls")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(call_object) = call.as_object() else {
                continue;
            };
            let index = call_object
                .get("index")
                .and_then(Value::as_u64)
                .unwrap_or(0) as usize;
            let state = self.tools.entry(index).or_default();
            if let Some(id) = call_object.get("id").and_then(Value::as_str) {
                state.id = Some(id.into());
            }
            if let Some(function) = call_object.get("function").and_then(Value::as_object) {
                if let Some(name) = function.get("name").and_then(Value::as_str) {
                    state.name = Some(name.into());
                }
                if let Some(arguments) = function.get("arguments").and_then(Value::as_str) {
                    state.arguments.push_str(arguments);
                }
            }
            if state.id.is_some() && state.name.is_some() && state.emitted == 0 {
                out.push(CanonicalResponseEvent::ToolCallStarted {
                    item_id: format!("tool:{index}"),
                    provider_id: None,
                    call: ToolCall {
                        id: state.id.clone(),
                        name: state.name.clone().unwrap_or_default(),
                        arguments: String::new(),
                        status: Some("in_progress".into()),
                        metadata: state.opaque.clone().into_iter().collect(),
                        function_metadata: state.function.clone().into_iter().collect(),
                        extensions: Vec::new(),
                    },
                });
            }
            if state.emitted < state.arguments.len() {
                let args = state.arguments[state.emitted..].to_owned();
                state.emitted = state.arguments.len();
                out.push(CanonicalResponseEvent::ToolArgumentsDelta {
                    item_id: format!("tool:{index}"),
                    arguments: args,
                });
            }
        }
    }
}

fn map_stop(reason: &str, saw_tool: bool) -> StopReason {
    match reason {
        "tool_calls" | "function_call" if saw_tool => StopReason::ToolUse,
        "length" => StopReason::MaxTokens,
        "content_filter" => StopReason::ContentFilter,
        "stop" => StopReason::Natural,
        other => StopReason::Unknown(other.into()),
    }
}
fn chunk_extension(chunk: &Value) -> Option<Extension> {
    let object = chunk.as_object()?;
    let structural = [
        "id", "object", "created", "model", "choices", "usage", "error",
    ];
    let opaque: Map<String, Value> = object
        .iter()
        .filter(|(key, _)| !structural.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    (!opaque.is_empty()).then(|| Extension {
        namespace: "openai.chat_completions".into(),
        key: "chunk".into(),
        value: Value::Object(opaque),
        scope: ExtensionScope::Response,
        source_protocol: None,
        provider: None,
        replay: ReplayPolicy::NeverReplay,
    })
}
