//! Chat Completions SSE parser: upstream `chat.completion.chunk` events into the
//! semantic `StreamEvent` IR.
//!
//! Extracted from the former `ChatStreamConverter`: this parser owns only the
//! upstream-shape concerns (delta field extraction, tool-call accumulation across
//! chunks, usage/finish extraction, MiMo `annotations` -> sources text). Rendering
//! the events into a downstream protocol is the renderers' job.

use std::collections::HashMap;

use serde_json::Value;

use crate::chat::{
    delta_string, extract_usage, first_choice_delta, first_choice_finish_reason,
    is_usage_only_chunk, upstream_id, upstream_model, ChatUsage,
};
use crate::error::ConversionError;
use crate::stream::{map_chat_finish_reason, StreamEvent};

/// Accumulated state of one upstream tool call across chunks.
#[derive(Default)]
struct ToolAccumulator {
    id: Option<String>,
    name: Option<String>,
    arguments: String,
    /// Bytes of `arguments` already emitted as deltas (for increment-only output).
    emitted_len: usize,
}

/// Parse upstream Chat Completions SSE chunks into `StreamEvent`s.
pub struct ChatParser {
    started: bool,
    finished: bool,
    tools: HashMap<usize, ToolAccumulator>,
    usage: ChatUsage,
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
        if let Some(finish_reason) = first_choice_finish_reason(&chunk) {
            let saw_tool_call = self.tools.values().any(|tool| tool.name.is_some());
            out.push(StreamEvent::End {
                stop_reason: map_chat_finish_reason(Some(finish_reason), saw_tool_call),
                usage: self.usage,
            });
            // The response is complete once a finish_reason arrives; subsequent
            // usage-only chunks are absorbed by finish().
            self.finish(out);
            return Ok(());
        }
        let usage = extract_usage(&chunk);
        if !usage.is_empty() {
            self.usage = usage;
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
            if tool.name.is_none() {
                continue;
            }
            if tool.emitted_len < tool.arguments.len() {
                let args = tool.arguments[tool.emitted_len..].to_string();
                tool.emitted_len = tool.arguments.len();
                out.push(StreamEvent::ToolCallArgsDelta { index, args });
            }
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
            accumulator.id.is_some() && accumulator.name.is_some()
        };
        if announce {
            let (id, name) = {
                let acc = &self.tools[&index];
                (
                    acc.id.clone().expect("checked"),
                    acc.name.clone().expect("checked"),
                )
            };
            out.push(StreamEvent::ToolCallStart { index, id, name });
            // Clear id/name so the announce fires only once; arguments keep accumulating.
            let acc = self.tools.get_mut(&index).expect("inserted above");
            acc.id = None;
            acc.name = None;
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
            matches!(&events[3], StreamEvent::End { stop_reason, .. } if *stop_reason == crate::stream::StopReason::Stop)
        );
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
            matches!(&events[1], StreamEvent::ToolCallStart { index: 0, id, name } if id == "call_1" && name == "get_weather")
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
