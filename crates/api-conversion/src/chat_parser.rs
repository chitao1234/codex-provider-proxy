//! Chat Completions SSE parser: upstream `chat.completion.chunk` events into the
//! semantic `StreamEvent` IR.
//!
//! Extracted from the former `ChatStreamConverter`: this parser owns only the
//! upstream-shape concerns (delta field extraction, tool-call accumulation across
//! chunks, usage/finish extraction, MiMo `annotations` -> sources text). Rendering
//! the events into a downstream protocol is the renderers' job.

use std::collections::HashMap;

use serde_json::{Map, Value};

use crate::chat::{
    delta_string, extract_usage, first_choice_delta, first_choice_finish_reason,
    is_usage_only_chunk, upstream_id, upstream_model, ChatUsage,
};
use crate::error::ConversionError;
use crate::stream::{map_chat_finish_reason, StopReason, StreamEvent};

/// Accumulated state of one upstream tool call across chunks.
#[derive(Default)]
struct ToolAccumulator {
    id: Option<String>,
    name: Option<String>,
    announced: bool,
    arguments: String,
    opaque_extensions: Map<String, Value>,
    function_extensions: Map<String, Value>,
    /// Bytes of `arguments` already emitted as deltas (for increment-only output).
    emitted_len: usize,
}

/// Parse upstream Chat Completions SSE chunks into `StreamEvent`s.
pub struct ChatParser {
    started: bool,
    finished: bool,
    tools: HashMap<usize, ToolAccumulator>,
    usage: ChatUsage,
    pending_stop: Option<(StopReason, String)>,
}

impl Default for ChatParser {
    fn default() -> Self {
        Self::new()
    }
}

impl ChatParser {
    pub fn new() -> Self {
        Self {
            started: false,
            finished: false,
            tools: HashMap::new(),
            usage: ChatUsage::default(),
            pending_stop: None,
        }
    }

    /// Process one upstream SSE `data:` payload, appending semantic events to `out`.
    pub fn on_chunk(
        &mut self,
        payload: &str,
        out: &mut Vec<StreamEvent>,
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

        // Upstream error event: emit an error and stop instead of faking a completion.
        if let Some(error) = chunk.get("error").and_then(Value::as_object) {
            self.finished = true;
            out.push(StreamEvent::Error {
                message: error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("upstream request failed")
                    .to_string(),
                code: error.get("code").and_then(Value::as_str).map(str::to_owned),
            });
            return Ok(());
        }

        if !self.started {
            self.started = true;
            out.push(StreamEvent::Start {
                id: upstream_id(&chunk).unwrap_or_default().to_string(),
                model: upstream_model(&chunk).unwrap_or_default().to_string(),
            });
        }

        if let Some(delta) = first_choice_delta(&chunk) {
            self.process_delta(delta, out);
        }
        let usage = extract_usage(&chunk);
        if !usage.is_empty() {
            self.usage = usage;
        }
        if let Some(extension) = chat_chunk_extension(&chunk) {
            out.push(extension);
        }
        if let Some(finish_reason) = first_choice_finish_reason(&chunk) {
            let saw_tool_call = self.tools.values().any(|tool| tool.name.is_some());
            self.pending_stop = Some((
                map_chat_finish_reason(Some(finish_reason), saw_tool_call),
                finish_reason.to_owned(),
            ));
            if !self.usage.is_empty() {
                self.finish(out);
            }
            return Ok(());
        }
        if is_usage_only_chunk(&chunk) {
            // Final usage chunk (`choices: []`): the stream is complete.
            self.finish(out);
        }
        Ok(())
    }

    /// End of stream: flush any remaining tool-call argument increments.
    pub fn finish(&mut self, out: &mut Vec<StreamEvent>) {
        if self.finished {
            return;
        }
        self.finished = true;
        // Emit the final arguments increment for any open tool calls.
        let mut indices: Vec<usize> = self.tools.keys().copied().collect();
        indices.sort_unstable();
        for index in indices {
            let Some(tool) = self.tools.get_mut(&index) else {
                continue;
            };
            if !tool.announced {
                continue;
            }
            if tool.emitted_len < tool.arguments.len() {
                let args = tool.arguments[tool.emitted_len..].to_string();
                tool.emitted_len = tool.arguments.len();
                out.push(StreamEvent::ToolCallArgsDelta { index, args });
            }
        }
        if let Some((stop_reason, protocol_reason)) = self.pending_stop.take() {
            out.push(StreamEvent::End {
                stop_reason,
                protocol_reason: Some(protocol_reason),
                usage: self.usage,
            });
        }
    }

    fn process_delta(&mut self, delta: &Value, out: &mut Vec<StreamEvent>) {
        if let Some(reasoning) = delta_string(delta, "reasoning_content") {
            out.push(StreamEvent::ReasoningDelta {
                text: reasoning.to_string(),
                signature: None,
            });
        }
        if let Some(text) = delta_string(delta, "content") {
            out.push(StreamEvent::TextDelta {
                text: text.to_string(),
            });
        }
        if let Some(annotations) = delta.get("annotations") {
            // MiMo streams search sources in delta.annotations (url_citation items);
            // surface them as a trailing sources text block.
            if let Some(sources) = format_url_citations(Some(annotations)) {
                out.push(StreamEvent::TextDelta {
                    text: format!("\n\n{sources}"),
                });
            }
        }
        if let Some(tool_calls) = delta.get("tool_calls").and_then(Value::as_array) {
            for (position, call) in tool_calls.iter().enumerate() {
                self.process_tool_call(position, call, out);
            }
        }
    }

    fn process_tool_call(&mut self, position: usize, call: &Value, out: &mut Vec<StreamEvent>) {
        let Some(call) = call.as_object() else { return };
        let index = call
            .get("index")
            .and_then(Value::as_u64)
            .map(|index| index as usize)
            .unwrap_or(position);

        let new_id = call
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .map(str::to_owned);
        let function = call.get("function").and_then(Value::as_object);
        let new_name = function
            .and_then(|f| f.get("name"))
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .map(str::to_owned);
        let new_args = function
            .and_then(|f| f.get("arguments"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let call_extensions = opaque_fields(call, &["index", "id", "type", "function"]);
        let function_extensions = function
            .map(|function| opaque_fields(function, &["name", "arguments"]))
            .unwrap_or_default();

        // Announce the tool call once id + name are both known.
        let announce = {
            let accumulator = self.tools.entry(index).or_default();
            if let Some(id) = new_id {
                accumulator.id = Some(id);
            }
            if let Some(name) = new_name {
                accumulator.name = Some(name);
            }
            if let Some(arguments) = new_args {
                accumulator.arguments.push_str(&arguments);
            }
            accumulator.opaque_extensions.extend(call_extensions);
            accumulator.function_extensions.extend(function_extensions);
            !accumulator.announced && accumulator.id.is_some() && accumulator.name.is_some()
        };
        if announce {
            let (id, name, opaque_extensions, function_extensions) = {
                let acc = &self.tools[&index];
                (
                    acc.id.clone().expect("checked"),
                    acc.name.clone().expect("checked"),
                    acc.opaque_extensions.clone(),
                    acc.function_extensions.clone(),
                )
            };
            out.push(StreamEvent::ToolCallStart {
                index,
                id,
                name,
                opaque_extensions,
                function_extensions,
            });
            let acc = self.tools.get_mut(&index).expect("inserted above");
            acc.announced = true;
        }

        // Emit only the arguments increment since the last emission.
        let accumulator = self.tools.get_mut(&index).expect("inserted above");
        if accumulator.emitted_len < accumulator.arguments.len() {
            let args = accumulator.arguments[accumulator.emitted_len..].to_string();
            accumulator.emitted_len = accumulator.arguments.len();
            if !args.is_empty() {
                out.push(StreamEvent::ToolCallArgsDelta { index, args });
            }
        }
    }
}

fn opaque_fields(object: &Map<String, Value>, structural: &[&str]) -> Map<String, Value> {
    object
        .iter()
        .filter(|(key, _)| !structural.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn chat_chunk_extension(chunk: &Value) -> Option<StreamEvent> {
    let chunk = chunk.as_object()?;
    let mut payload = Map::new();
    let root = opaque_fields(
        chunk,
        &[
            "id", "object", "created", "model", "choices", "usage", "error",
        ],
    );
    if !root.is_empty() {
        payload.insert("root".to_owned(), Value::Object(root));
    }
    if let Some(choice) = chunk
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(Value::as_object)
    {
        let choice_extensions = opaque_fields(choice, &["index", "delta", "finish_reason"]);
        if !choice_extensions.is_empty() {
            payload.insert("choice".to_owned(), Value::Object(choice_extensions));
        }
        if let Some(delta) = choice.get("delta").and_then(Value::as_object) {
            let delta_extensions = opaque_fields(
                delta,
                &["role", "reasoning_content", "content", "tool_calls"],
            );
            if !delta_extensions.is_empty() {
                payload.insert("delta".to_owned(), Value::Object(delta_extensions));
            }
        }
    }
    if let Some(usage) = chunk.get("usage").and_then(Value::as_object) {
        let usage_extensions = opaque_fields(
            usage,
            &[
                "prompt_tokens",
                "completion_tokens",
                "total_tokens",
                "cached_tokens",
                "prompt_cache_hit_tokens",
                "prompt_cache_miss_tokens",
                "prompt_tokens_details",
                "completion_tokens_details",
            ],
        );
        if !usage_extensions.is_empty() {
            payload.insert("usage".to_owned(), Value::Object(usage_extensions));
        }
    }
    (!payload.is_empty()).then(|| StreamEvent::Extension {
        namespace: "openai.chat_completions".to_owned(),
        kind: "chat.completion.chunk".to_owned(),
        payload: Value::Object(payload),
    })
}

/// MiMo url_citation annotations -> a "Sources:" text block (Anthropic has no
/// citation block type, so sources are surfaced as trailing text).
fn format_url_citations(annotations: Option<&Value>) -> Option<String> {
    let annotations = match annotations {
        Some(Value::Array(items)) => items,
        _ => return None,
    };
    let citations: Vec<String> = annotations
        .iter()
        .filter_map(|a| {
            let object = a.as_object()?;
            if object.get("type").and_then(Value::as_str) != Some("url_citation") {
                return None;
            }
            let url = object
                .get("url")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let title = object
                .get("title")
                .and_then(Value::as_str)
                .unwrap_or_default();
            Some(if title.is_empty() {
                url.to_string()
            } else {
                format!("{title}: {url}")
            })
        })
        .collect();
    (!citations.is_empty()).then(|| format!("Sources:\n{}", citations.join("\n")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_basic_stream() {
        let mut parser = ChatParser::new();
        let mut events = Vec::new();
        parser
            .on_chunk(
                r#"{"id":"x","choices":[{"index":0,"delta":{"role":"assistant","content":null,"reasoning_content":""},"finish_reason":null}]}"#,
                &mut events,
            )
            .unwrap();
        parser
            .on_chunk(
                r#"{"id":"x","choices":[{"index":0,"delta":{"content":null,"reasoning_content":"We"},"finish_reason":null}]}"#,
                &mut events,
            )
            .unwrap();
        parser
            .on_chunk(
                r#"{"id":"x","choices":[{"index":0,"delta":{"content":"Hi","reasoning_content":null},"finish_reason":null}]}"#,
                &mut events,
            )
            .unwrap();
        parser
            .on_chunk(
                r#"{"id":"x","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":null}"#,
                &mut events,
            )
            .unwrap();
        parser.on_chunk("[DONE]", &mut events).unwrap();

        assert!(matches!(&events[0], StreamEvent::Start { id, .. } if id == "x"));
        assert!(matches!(&events[1], StreamEvent::ReasoningDelta { text, .. } if text == "We"));
        assert!(matches!(&events[2], StreamEvent::TextDelta { text } if text == "Hi"));
        assert!(
            matches!(&events[3], StreamEvent::End { stop_reason, protocol_reason, .. }
                if *stop_reason == crate::stream::StopReason::Stop
                    && protocol_reason.as_deref() == Some("stop"))
        );
    }

    #[test]
    fn captures_usage_from_finish_chunk() {
        let mut parser = ChatParser::new();
        let mut events = Vec::new();
        parser
            .on_chunk(
                r#"{"id":"x","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":6,"completion_tokens":9}}"#,
                &mut events,
            )
            .unwrap();

        assert!(matches!(
            events.last(),
            Some(StreamEvent::End { usage, .. })
                if usage.prompt_tokens == Some(6) && usage.completion_tokens == Some(9)
        ));
    }

    #[test]
    fn captures_usage_from_trailing_usage_only_chunk() {
        let mut parser = ChatParser::new();
        let mut events = Vec::new();
        parser
            .on_chunk(
                r#"{"id":"x","choices":[{"index":0,"delta":{"content":"Hi"},"finish_reason":null}]}"#,
                &mut events,
            )
            .unwrap();
        parser
            .on_chunk(
                r#"{"id":"x","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":null}"#,
                &mut events,
            )
            .unwrap();
        assert!(!events
            .iter()
            .any(|event| matches!(event, StreamEvent::End { .. })));
        parser
            .on_chunk(
                r#"{"id":"x","choices":[],"usage":{"prompt_tokens":6,"completion_tokens":9}}"#,
                &mut events,
            )
            .unwrap();

        assert!(matches!(
            events.last(),
            Some(StreamEvent::End { usage, protocol_reason, .. })
                if usage.prompt_tokens == Some(6)
                    && usage.completion_tokens == Some(9)
                    && protocol_reason.as_deref() == Some("stop")
        ));
    }

    #[test]
    fn parses_tool_calls_with_incremental_arguments() {
        let mut parser = ChatParser::new();
        let mut events = Vec::new();
        parser
            .on_chunk(
                r#"{"id":"x","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":""}}]},"finish_reason":null}]}"#,
                &mut events,
            )
            .unwrap();
        parser
            .on_chunk(
                r#"{"id":"x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"city\": \"Pa"}}]},"finish_reason":null}]}"#,
                &mut events,
            )
            .unwrap();
        parser
            .on_chunk(
                r#"{"id":"x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"ris\"}"}}]},"finish_reason":null}]}"#,
                &mut events,
            )
            .unwrap();
        parser.on_chunk("[DONE]", &mut events).unwrap();

        assert!(
            matches!(&events[1], StreamEvent::ToolCallStart { index: 0, id, name, .. } if id == "call_1" && name == "get_weather")
        );
        // Increments only, never cumulative.
        assert!(
            matches!(&events[2], StreamEvent::ToolCallArgsDelta { index: 0, args } if args == "{\"city\": \"Pa")
        );
        assert!(
            matches!(&events[3], StreamEvent::ToolCallArgsDelta { index: 0, args } if args == "ris\"}")
        );
        assert!(!events.iter().any(|e| matches!(e, StreamEvent::ToolCallArgsDelta { args, .. } if args == "{\"city\": \"Paris\"}")));
    }

    #[test]
    fn repeated_tool_metadata_does_not_restart_tool_call() {
        let mut parser = ChatParser::new();
        let mut events = Vec::new();
        for payload in [
            r#"{"id":"x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"lookup","arguments":"{\"a\":"}}]},"finish_reason":null}]}"#,
            r#"{"id":"x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"lookup","arguments":"1}"}}]},"finish_reason":null}]}"#,
            r#"{"id":"x","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}"#,
            "[DONE]",
        ] {
            parser.on_chunk(payload, &mut events).unwrap();
        }

        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, StreamEvent::ToolCallStart { .. }))
                .count(),
            1
        );
        let arguments = events
            .iter()
            .filter_map(|event| match event {
                StreamEvent::ToolCallArgsDelta { args, .. } => Some(args.as_str()),
                _ => None,
            })
            .collect::<String>();
        assert_eq!(arguments, "{\"a\":1}");
    }

    #[test]
    fn preserves_chunk_and_tool_call_extensions() {
        let mut parser = ChatParser::new();
        let mut events = Vec::new();
        parser
            .on_chunk(
                r#"{"id":"x","model":"m","service_tier":"priority","choices":[{"index":0,"logprobs":{"vendor":true},"x_choice":1,"delta":{"x_delta":2,"tool_calls":[{"index":0,"id":"call_1","type":"function","x_call":3,"function":{"name":"lookup","arguments":"{}","x_function":4}}]},"finish_reason":null}]}"#,
                &mut events,
            )
            .unwrap();

        let tool = events
            .iter()
            .find(|event| matches!(event, StreamEvent::ToolCallStart { .. }))
            .unwrap();
        assert!(matches!(
            tool,
            StreamEvent::ToolCallStart {
                opaque_extensions,
                function_extensions,
                ..
            } if opaque_extensions["x_call"] == 3 && function_extensions["x_function"] == 4
        ));
        let extension = events
            .iter()
            .find(|event| matches!(event, StreamEvent::Extension { .. }))
            .unwrap();
        assert!(matches!(
            extension,
            StreamEvent::Extension { namespace, kind, payload }
                if namespace == "openai.chat_completions"
                    && kind == "chat.completion.chunk"
                    && payload["root"]["service_tier"] == "priority"
                    && payload["choice"]["logprobs"]["vendor"] == true
                    && payload["choice"]["x_choice"] == 1
                    && payload["delta"]["x_delta"] == 2
        ));
    }

    #[test]
    fn parses_error_event() {
        let mut parser = ChatParser::new();
        let mut events = Vec::new();
        parser
            .on_chunk(
                r#"{"error":{"message":"upstream exploded","code":"upstream_error"}}"#,
                &mut events,
            )
            .unwrap();
        assert!(
            matches!(&events[0], StreamEvent::Error { message, code } if message == "upstream exploded" && code.as_deref() == Some("upstream_error"))
        );
    }

    #[test]
    fn maps_annotations_to_sources_text() {
        let mut parser = ChatParser::new();
        let mut events = Vec::new();
        parser
            .on_chunk(
                r#"{"id":"x","choices":[{"index":0,"delta":{"content":"answer","annotations":[{"type":"url_citation","url":"https://e.com","title":"T"}]},"finish_reason":null}]}"#,
                &mut events,
            )
            .unwrap();
        assert!(events.iter().any(
            |e| matches!(e, StreamEvent::TextDelta { text } if text.contains("T: https://e.com"))
        ));
    }
}
