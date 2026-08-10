use crate::{
    error::ConversionError,
    ir::{
        CanonicalResponseEvent, ContentPart, CoreUpdate, Extension, ExtensionScope, ItemKind,
        ReplayPolicy, StopReason, ToolCall, Usage,
    },
    sse,
};
use bytes::Bytes;
use serde_json::{json, Map, Value};

use std::collections::BTreeMap;

#[derive(Debug, Clone)]
enum BlockKind {
    Message,
    Reasoning,
    Tool,
    Extension,
}

pub struct Parser {
    finished: bool,
    completed: bool,
    blocks: BTreeMap<usize, (String, BlockKind)>,
    usage: Usage,
}

impl Default for Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl Parser {
    pub fn new() -> Self {
        Self {
            finished: false,
            completed: false,
            blocks: BTreeMap::new(),
            usage: Usage::default(),
        }
    }

    pub fn on_event(
        &mut self,
        event_type: &str,
        payload: &str,
        out: &mut Vec<CanonicalResponseEvent>,
    ) -> Result<(), ConversionError> {
        if self.finished {
            return Ok(());
        }
        let data: Value = serde_json::from_str(payload)
            .map_err(|_| ConversionError::invalid("upstream SSE data is not valid JSON"))?;
        match event_type {
            "message_start" => self.message_start(&data, out),
            "content_block_start" => self.block_start(&data, out),
            "content_block_delta" => self.block_delta(&data, out),
            "content_block_stop" => self.block_stop(&data, out),
            "message_delta" => self.message_delta(&data, out),
            "message_stop" => {
                self.finished = true;
                if !self.completed {
                    self.complete(StopReason::Natural, None, out);
                }
            }
            "error" => {
                self.finished = true;
                let error = data.get("error").and_then(Value::as_object);
                out.push(CanonicalResponseEvent::StreamError {
                    message: error
                        .and_then(|error| error.get("message"))
                        .and_then(Value::as_str)
                        .unwrap_or("upstream request failed")
                        .to_owned(),
                    code: error
                        .and_then(|error| error.get("type"))
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                });
            }
            "ping" => {}
            _ => out.push(protocol_extension(event_type, data)),
        }
        Ok(())
    }

    pub fn finish(&mut self, _out: &mut Vec<CanonicalResponseEvent>) {
        self.finished = true;
    }

    fn message_start(&mut self, data: &Value, out: &mut Vec<CanonicalResponseEvent>) {
        let message = data.get("message").and_then(Value::as_object);
        out.push(CanonicalResponseEvent::StreamStarted {
            response_id: message
                .and_then(|message| message.get("id"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            model: message
                .and_then(|message| message.get("model"))
                .and_then(Value::as_str)
                .map(str::to_owned),
        });
        if let Some(usage) = message.and_then(|message| message.get("usage")) {
            self.update_usage(usage, out);
        }
    }

    fn block_start(&mut self, data: &Value, out: &mut Vec<CanonicalResponseEvent>) {
        let index = event_index(data);
        let block = data.get("content_block").and_then(Value::as_object);
        let block_type = block
            .and_then(|block| block.get("type"))
            .and_then(Value::as_str)
            .unwrap_or("text");
        let item_id = block
            .and_then(|block| block.get("id"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| format!("{block_type}:{index}"));
        let provider_id = block
            .and_then(|block| block.get("id"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let kind = match block_type {
            "text" => {
                out.push(CanonicalResponseEvent::ItemStarted {
                    item_id: item_id.clone(),
                    provider_id: provider_id.clone(),
                    item: ItemKind::Message,
                });
                if let Some(text) = block
                    .and_then(|block| block.get("text"))
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                {
                    out.push(CanonicalResponseEvent::ContentDelta {
                        item_id: item_id.clone(),
                        content: ContentPart::Text(text.to_owned()),
                    });
                }
                BlockKind::Message
            }
            "thinking" => {
                out.push(CanonicalResponseEvent::ItemStarted {
                    item_id: item_id.clone(),
                    provider_id,
                    item: ItemKind::Reasoning,
                });
                BlockKind::Reasoning
            }
            "tool_use" => {
                let call = ToolCall {
                    id: block
                        .and_then(|block| block.get("id"))
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    name: block
                        .and_then(|block| block.get("name"))
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    arguments: String::new(),
                    status: Some("in_progress".to_owned()),
                    metadata: block
                        .map(|block| opaque_fields(block, &["type", "id", "name", "input"]))
                        .unwrap_or_default()
                        .into_iter()
                        .collect(),
                    function_metadata: BTreeMap::new(),
                    extensions: Vec::new(),
                };
                out.push(CanonicalResponseEvent::ToolCallStarted {
                    item_id: item_id.clone(),
                    provider_id: provider_id.clone(),
                    call,
                });
                BlockKind::Tool
            }
            _ => {
                out.push(protocol_extension("content_block_start", data.clone()));
                BlockKind::Extension
            }
        };
        self.blocks.insert(index, (item_id, kind));
    }

    fn block_delta(&mut self, data: &Value, out: &mut Vec<CanonicalResponseEvent>) {
        let index = event_index(data);
        let Some((item_id, kind)) = self.blocks.get(&index) else {
            out.push(protocol_extension("content_block_delta", data.clone()));
            return;
        };
        let delta = data.get("delta").and_then(Value::as_object);
        match delta
            .and_then(|delta| delta.get("type"))
            .and_then(Value::as_str)
        {
            Some("text_delta") if matches!(kind, BlockKind::Message) => {
                if let Some(text) = delta
                    .and_then(|delta| delta.get("text"))
                    .and_then(Value::as_str)
                {
                    out.push(CanonicalResponseEvent::ContentDelta {
                        item_id: item_id.clone(),
                        content: ContentPart::Text(text.to_owned()),
                    });
                }
            }
            Some("thinking_delta") if matches!(kind, BlockKind::Reasoning) => {
                if let Some(text) = delta
                    .and_then(|delta| delta.get("thinking"))
                    .and_then(Value::as_str)
                {
                    out.push(CanonicalResponseEvent::ContentDelta {
                        item_id: item_id.clone(),
                        content: ContentPart::Reasoning {
                            text: text.to_owned(),
                            signature: None,
                        },
                    });
                }
            }
            Some("signature_delta") if matches!(kind, BlockKind::Reasoning) => {
                if let Some(signature) = delta
                    .and_then(|delta| delta.get("signature"))
                    .and_then(Value::as_str)
                {
                    out.push(CanonicalResponseEvent::ContentDelta {
                        item_id: item_id.clone(),
                        content: ContentPart::Reasoning {
                            text: String::new(),
                            signature: Some(signature.to_owned()),
                        },
                    });
                }
            }
            Some("input_json_delta") if matches!(kind, BlockKind::Tool) => {
                if let Some(arguments) = delta
                    .and_then(|delta| delta.get("partial_json"))
                    .and_then(Value::as_str)
                {
                    out.push(CanonicalResponseEvent::ToolArgumentsDelta {
                        item_id: item_id.clone(),
                        arguments: arguments.to_owned(),
                    });
                }
            }
            _ => out.push(protocol_extension("content_block_delta", data.clone())),
        }
    }

    fn block_stop(&mut self, data: &Value, out: &mut Vec<CanonicalResponseEvent>) {
        let index = event_index(data);
        if let Some((item_id, kind)) = self.blocks.remove(&index) {
            if matches!(kind, BlockKind::Extension) {
                out.push(protocol_extension("content_block_stop", data.clone()));
            } else {
                out.push(CanonicalResponseEvent::ItemCompleted { item_id });
            }
        }
    }

    fn message_delta(&mut self, data: &Value, out: &mut Vec<CanonicalResponseEvent>) {
        if let Some(usage) = data.get("usage") {
            self.update_usage(usage, out);
        }
        let original = data
            .get("delta")
            .and_then(Value::as_object)
            .and_then(|delta| delta.get("stop_reason"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let reason = original
            .as_deref()
            .map(map_stop_reason)
            .unwrap_or(StopReason::Natural);
        self.complete(reason, original, out);
    }

    fn complete(
        &mut self,
        reason: StopReason,
        original: Option<String>,
        out: &mut Vec<CanonicalResponseEvent>,
    ) {
        if self.completed {
            return;
        }
        self.completed = true;
        out.push(CanonicalResponseEvent::ResponseCompleted {
            stop_reason: reason,
            original_reason: original,
        });
    }

    fn update_usage(&mut self, usage: &Value, out: &mut Vec<CanonicalResponseEvent>) {
        let object = usage.as_object();
        let field = |name: &str| {
            object
                .and_then(|usage| usage.get(name))
                .and_then(Value::as_u64)
        };
        self.usage.input_tokens = field("input_tokens").or(self.usage.input_tokens);
        self.usage.output_tokens = field("output_tokens").or(self.usage.output_tokens);
        self.usage.cached_input_tokens =
            field("cache_read_input_tokens").or(self.usage.cached_input_tokens);
        self.usage.cache_write_input_tokens =
            field("cache_creation_input_tokens").or(self.usage.cache_write_input_tokens);
        self.usage.reasoning_tokens = object
            .and_then(|usage| usage.get("output_tokens_details"))
            .and_then(|details| details.get("thinking_tokens"))
            .and_then(Value::as_u64)
            .or(self.usage.reasoning_tokens);
        self.usage.total_tokens = self
            .usage
            .input_tokens
            .zip(self.usage.output_tokens)
            .map(|(input, output)| input + output);
        out.push(CanonicalResponseEvent::UsageUpdated(self.usage.clone()));
    }
}

fn event_index(data: &Value) -> usize {
    data.get("index")
        .and_then(Value::as_u64)
        .unwrap_or_default() as usize
}

fn map_stop_reason(reason: &str) -> StopReason {
    match reason {
        "end_turn" | "stop_sequence" => StopReason::Natural,
        "tool_use" => StopReason::ToolUse,
        "max_tokens" => StopReason::MaxTokens,
        "refusal" => StopReason::ContentFilter,
        other => StopReason::Unknown(other.to_owned()),
    }
}

fn opaque_fields(object: &Map<String, Value>, structural: &[&str]) -> Map<String, Value> {
    object
        .iter()
        .filter(|(key, _)| !structural.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn protocol_extension(kind: &str, value: Value) -> CanonicalResponseEvent {
    CanonicalResponseEvent::ProtocolExtension(Extension {
        namespace: "anthropic.messages".to_owned(),
        key: kind.to_owned(),
        value,
        scope: ExtensionScope::Response,
        source_protocol: Some("anthropic_messages".to_owned()),
        provider: None,
        replay: ReplayPolicy::NeverReplay,
    })
}

#[derive(Debug, Clone, Copy)]
struct RenderedBlock {
    index: usize,
    open: bool,
}

#[derive(Default)]
pub struct Renderer {
    started: bool,
    finished: bool,
    message_id: Option<String>,
    model: Option<String>,
    next_index: usize,
    blocks: BTreeMap<String, RenderedBlock>,
    usage: Usage,
}

impl Renderer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on_update(&mut self, update: &CoreUpdate, out: &mut Vec<Bytes>) {
        if self.finished {
            return;
        }
        let event = &update.event;
        match event {
            CanonicalResponseEvent::StreamStarted { response_id, model } => {
                self.message_id = response_id.as_deref().map(synth_message_id);
                self.model.clone_from(model);
            }
            CanonicalResponseEvent::ItemStarted { item_id, item, .. } => {
                if *item != ItemKind::ToolCall {
                    self.start_block(item_id, empty_block(*item, item_id), out);
                }
            }
            CanonicalResponseEvent::OpaqueItemStarted {
                item_id,
                kind,
                payload,
            } => {
                self.start_block(item_id, opaque_block(item_id, kind, payload), out);
            }
            CanonicalResponseEvent::ContentDelta { item_id, content } => {
                let index = self.ensure_block(item_id, content_block(content), out);
                let Some(delta) = content_delta(content) else {
                    return;
                };
                self.emit(
                    "content_block_delta",
                    json!({"type":"content_block_delta","index":index,"delta":delta}),
                    out,
                );
            }
            CanonicalResponseEvent::ToolCallStarted { item_id, call, .. } => {
                let mut block = json!({
                    "type":"tool_use",
                    "id":call.id.as_deref().unwrap_or(item_id),
                    "name":call.name,
                    "input":{},
                });
                if let Some(object) = block.as_object_mut() {
                    for (key, value) in call.metadata.iter().chain(&call.function_metadata) {
                        object.entry(key.clone()).or_insert_with(|| value.clone());
                    }
                }
                let index = self.ensure_block(item_id, block, out);
                if !call.arguments.is_empty() {
                    self.emit_tool_arguments(index, &call.arguments, out);
                }
            }
            CanonicalResponseEvent::ToolArgumentsDelta { item_id, arguments } => {
                let index = self.ensure_block(
                    item_id,
                    json!({"type":"tool_use","id":item_id,"name":"","input":{}}),
                    out,
                );
                self.emit_tool_arguments(index, arguments, out);
            }
            CanonicalResponseEvent::UsageUpdated(_) => self.usage = update.snapshot.usage.clone(),
            CanonicalResponseEvent::ItemCompleted { item_id } => self.close_block(item_id, out),
            CanonicalResponseEvent::ResponseCompleted {
                stop_reason,
                original_reason,
            } => {
                self.ensure_started(out);
                self.close_all(out);
                self.finished = true;
                self.emit(
                    "message_delta",
                    json!({
                        "type":"message_delta",
                        "delta":{"stop_reason":anthropic_reason(stop_reason, original_reason.as_deref())},
                        "usage":render_usage(&self.usage),
                    }),
                    out,
                );
                self.emit("message_stop", json!({"type":"message_stop"}), out);
            }
            CanonicalResponseEvent::StreamError { message, code } => {
                self.finished = true;
                self.emit(
                    "error",
                    json!({"type":"error","error":{"type":code.as_deref().unwrap_or("api_error"),"message":message}}),
                    out,
                );
            }
            CanonicalResponseEvent::ItemMetadataUpdated { .. }
            | CanonicalResponseEvent::ProtocolExtension(_) => {}
        }
    }

    pub fn finish(&mut self, _: &mut Vec<Bytes>) {}

    fn ensure_started(&mut self, out: &mut Vec<Bytes>) {
        if self.started {
            return;
        }
        self.started = true;
        self.emit(
            "message_start",
            json!({
                "type":"message_start",
                "message":{
                    "id":self.message_id,
                    "type":"message",
                    "role":"assistant",
                    "model":self.model,
                    "content":[],
                    "stop_reason":null,
                    "usage":render_usage(&self.usage),
                }
            }),
            out,
        );
    }

    fn ensure_block(&mut self, item_id: &str, block: Value, out: &mut Vec<Bytes>) -> usize {
        self.blocks
            .get(item_id)
            .map(|block| block.index)
            .unwrap_or_else(|| self.start_block(item_id, block, out))
    }

    fn start_block(&mut self, item_id: &str, block: Value, out: &mut Vec<Bytes>) -> usize {
        self.ensure_started(out);
        if let Some(rendered) = self.blocks.get(item_id) {
            return rendered.index;
        }
        let index = self.next_index;
        self.next_index += 1;
        self.blocks
            .insert(item_id.to_owned(), RenderedBlock { index, open: true });
        self.emit(
            "content_block_start",
            json!({"type":"content_block_start","index":index,"content_block":block}),
            out,
        );
        index
    }

    fn close_block(&mut self, item_id: &str, out: &mut Vec<Bytes>) {
        let Some(rendered) = self.blocks.get_mut(item_id) else {
            return;
        };
        if !rendered.open {
            return;
        }
        rendered.open = false;
        let index = rendered.index;
        self.emit(
            "content_block_stop",
            json!({"type":"content_block_stop","index":index}),
            out,
        );
    }

    fn close_all(&mut self, out: &mut Vec<Bytes>) {
        let open = self
            .blocks
            .iter()
            .filter_map(|(item_id, block)| block.open.then_some(item_id.clone()))
            .collect::<Vec<_>>();
        for item_id in open {
            self.close_block(&item_id, out);
        }
    }

    fn emit_tool_arguments(&self, index: usize, arguments: &str, out: &mut Vec<Bytes>) {
        self.emit(
            "content_block_delta",
            json!({"type":"content_block_delta","index":index,"delta":{"type":"input_json_delta","partial_json":arguments}}),
            out,
        );
    }

    fn emit(&self, kind: &str, data: Value, out: &mut Vec<Bytes>) {
        out.push(sse::encode_sse_event(kind, &data));
    }
}

fn empty_block(kind: ItemKind, item_id: &str) -> Value {
    match kind {
        ItemKind::Reasoning => json!({"type":"thinking","thinking":"","signature":null}),
        ItemKind::Message | ItemKind::Refusal => json!({"type":"text","text":""}),
        ItemKind::ToolCall => json!({"type":"tool_use","id":item_id,"name":"","input":{}}),
        ItemKind::ToolResult | ItemKind::ServerTool | ItemKind::Unknown => {
            json!({"type":"unknown","id":item_id})
        }
    }
}

fn opaque_block(item_id: &str, kind: &str, payload: &Value) -> Value {
    let mut block = payload.clone();
    if !block.is_object() {
        block = json!({"payload":block});
    }
    if let Some(object) = block.as_object_mut() {
        object
            .entry("type")
            .or_insert_with(|| Value::String(kind.to_owned()));
        object
            .entry("id")
            .or_insert_with(|| Value::String(item_id.to_owned()));
    }
    block
}

fn content_block(content: &ContentPart) -> Value {
    match content {
        ContentPart::Reasoning { .. } => {
            json!({"type":"thinking","thinking":"","signature":null})
        }
        ContentPart::Text(_) | ContentPart::Refusal(_) => json!({"type":"text","text":""}),
        ContentPart::Json(value) => value.clone(),
        ContentPart::Image { .. } => json!({"type":"unknown"}),
    }
}

fn content_delta(content: &ContentPart) -> Option<Value> {
    match content {
        ContentPart::Reasoning { text, signature } if text.is_empty() => signature
            .as_ref()
            .map(|signature| json!({"type":"signature_delta","signature":signature})),
        ContentPart::Reasoning { text, .. } => {
            Some(json!({"type":"thinking_delta","thinking":text}))
        }
        ContentPart::Text(text) | ContentPart::Refusal(text) => {
            Some(json!({"type":"text_delta","text":text}))
        }
        ContentPart::Json(_) | ContentPart::Image { .. } => None,
    }
}

fn render_usage(usage: &Usage) -> Value {
    json!({
        "input_tokens":usage.input_tokens,
        "output_tokens":usage.output_tokens,
        "cache_read_input_tokens":usage.cached_input_tokens,
        "cache_creation_input_tokens":usage.cache_write_input_tokens,
    })
}
fn synth_message_id(upstream_id: &str) -> String {
    if upstream_id.starts_with("msg_") {
        upstream_id.to_owned()
    } else {
        format!("msg_{upstream_id}")
    }
}
fn anthropic_reason(reason: &StopReason, original_reason: Option<&str>) -> String {
    match reason {
        StopReason::Natural => "end_turn".to_owned(),
        StopReason::ToolUse => "tool_use".to_owned(),
        StopReason::MaxTokens => "max_tokens".to_owned(),
        StopReason::ContentFilter => "refusal".to_owned(),
        StopReason::Cancelled => "cancelled".to_owned(),
        StopReason::Error => "error".to_owned(),
        StopReason::Unknown(reason) => original_reason.unwrap_or(reason).to_owned(),
    }
}
