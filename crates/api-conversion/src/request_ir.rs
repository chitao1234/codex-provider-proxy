//! Protocol-neutral request representation.
//!
//! The IR deliberately keeps the original top-level object alongside its semantic items. A
//! renderer removes only fields owned by the source protocol and overlays the target shape;
//! unknown provider/client parameters therefore survive a conversion instead of being silently
//! discarded.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::dialect::{RequestConversionReport, ResponseFormatCap, ServerToolPolicy, ThinkingParam};
use crate::{dialect::ModelCapabilities, error::ConversionError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    AnthropicMessages,
    OpenAiChatCompletions,
    OpenAiResponses,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RequestIr {
    protocol: Protocol,
    original: Map<String, Value>,
    model: Option<String>,
    stream: Option<bool>,
    /// Output-token budget, protocol-neutral: Messages `max_tokens`, Chat
    /// `max_tokens`/`max_completion_tokens`, Responses `max_output_tokens`.
    /// Optional in Chat/Responses; required by the Messages API.
    max_output_tokens: Option<Value>,
    transcript: Vec<IrConversationItem>,
    instructions: Vec<Value>,
    tools: Vec<IrTool>,
    tool_choice: Option<Value>,
    /// Top-level fields not modeled by the neutral IR. They are opaque provider/client
    /// extensions and are preserved without participating in semantic mapping.
    opaque_extensions: BTreeMap<String, Value>,
    context: RequestContext,
}

#[derive(Debug, Clone, PartialEq)]
enum IrConversationItem {
    Message(IrMessage),
    Reasoning(Value),
}

/// Stateful request context shared by protocols and provider dialects.
///
/// These fields affect conversation affinity, abuse attribution, or prompt-cache reuse. They are
/// modeled separately from sampling parameters so continuation can inherit them intentionally.
#[derive(Debug, Clone, Default, PartialEq)]
struct RequestContext {
    previous_response_id: Option<String>,
    conversation: Option<Value>,
    end_user_id: Option<String>,
    safety_identifier: Option<String>,
    metadata: Option<Value>,
    cache: CacheContext,
    store: Option<Value>,
    include: Option<Value>,
    truncation: Option<Value>,
    /// Provider-specific identity/session/cache keys that the IR understands as context
    /// extensions and maps when a target protocol has an equivalent boundary field.
    represented_extensions: BTreeMap<String, Value>,
}

/// Protocol-neutral prompt-cache semantics.
///
/// `key` is the stable cache-affinity identity. Protocols expose it under different names
/// (`prompt_cache_key`, `metadata.user_id`, or `user`), so renderers map it only at the boundary.
#[derive(Debug, Clone, Default, PartialEq)]
struct CacheContext {
    key: Option<String>,
    /// OpenAI `prompt_cache_options` { mode, ttl } (GPT-5.6+).
    options: Option<Value>,
    /// Deprecated OpenAI `prompt_cache_retention` (in_memory / 24h).
    retention: Option<Value>,
    /// Top-level Anthropic `cache_control` (auto breakpoint).
    control: Option<Value>,
}

#[derive(Debug, Clone, PartialEq)]
struct IrMessage {
    role: String,
    content: Vec<IrContent>,
    item_id: Option<String>,
    tool_call_id: Option<String>,
    tool_calls: Vec<IrToolCall>,
    opaque_extensions: Map<String, Value>,
}

#[derive(Debug, Clone, Default, PartialEq)]
struct ContentExtensions {
    cache_control: Option<Value>,
    opaque: Map<String, Value>,
}

impl ContentExtensions {
    fn parse(object: &Map<String, Value>, structural: &[&str]) -> Self {
        Self {
            cache_control: object.get("cache_control").cloned(),
            opaque: opaque_fields(object, structural),
        }
    }

    fn render(&self, keep_cache_control: bool) -> Map<String, Value> {
        let mut object = self.opaque.clone();
        if keep_cache_control {
            if let Some(cache_control) = &self.cache_control {
                object.insert("cache_control".to_owned(), cache_control.clone());
            }
        }
        object
    }
}

#[derive(Debug, Clone, PartialEq)]
enum IrContent {
    Text {
        text: String,
        extensions: ContentExtensions,
    },
    Thinking {
        text: String,
        signature: Option<String>,
        extensions: ContentExtensions,
    },
    Image(Value),
    ToolUse(IrToolCall),
    ToolResult {
        call_id: String,
        content: Value,
        is_error: bool,
        extensions: ContentExtensions,
    },
    Raw(Value),
}

#[derive(Debug, Clone, PartialEq)]
struct IrToolCall {
    id: Option<String>,
    call_id: Option<String>,
    name: String,
    arguments: Value,
}

#[derive(Debug, Clone, PartialEq)]
struct IrTool {
    kind: String,
    name: Option<String>,
    description: Option<String>,
    parameters: Option<Value>,
    raw: Value,
}

impl RequestIr {
    pub fn parse(protocol: Protocol, body: &Value) -> Result<Self, ConversionError> {
        let original = body
            .as_object()
            .cloned()
            .ok_or_else(|| ConversionError::invalid("request body is not a JSON object"))?;
        let model = original
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let stream = original.get("stream").and_then(Value::as_bool);
        let max_output_tokens = original
            .get("max_tokens")
            .or_else(|| original.get("max_completion_tokens"))
            .or_else(|| original.get("max_output_tokens"))
            .cloned();
        let mut ir = Self {
            protocol,
            original,
            model,
            stream,
            max_output_tokens,
            transcript: Vec::new(),
            instructions: Vec::new(),
            tools: Vec::new(),
            tool_choice: None,
            opaque_extensions: BTreeMap::new(),
            context: RequestContext::default(),
        };

        match protocol {
            Protocol::AnthropicMessages => ir.parse_messages(),
            Protocol::OpenAiChatCompletions => ir.parse_chat(),
            Protocol::OpenAiResponses => ir.parse_responses(),
        }
        ir.parse_context();
        ir.collect_opaque_extensions();
        Ok(ir)
    }

    pub fn parse_with_context(
        protocol: Protocol,
        body: &Value,
        previous_request: Option<&RequestIr>,
        last_reasoning: Option<&str>,
    ) -> Result<Self, ConversionError> {
        let mut ir = Self::parse(protocol, body)?;
        if protocol == Protocol::OpenAiResponses {
            let mut current_transcript = std::mem::take(&mut ir.transcript);
            if let Some(previous_request) = previous_request {
                ir.transcript = previous_request.transcript.clone();
                ir.inherit_context(&previous_request.context);
            }
            if let Some(reasoning) = last_reasoning.filter(|value| !value.trim().is_empty()) {
                let current_has_reasoning = current_transcript
                    .iter()
                    .any(|item| matches!(item, IrConversationItem::Reasoning(value) if reasoning_text(value).is_some()));
                if !current_has_reasoning {
                    current_transcript.retain(|item| {
                        !matches!(item, IrConversationItem::Reasoning(value) if reasoning_text(value).is_none())
                    });
                    ir.transcript
                        .push(IrConversationItem::Reasoning(serde_json::json!({
                            "type": "reasoning",
                            "summary": [{"type": "summary_text", "text": reasoning}]
                        })));
                }
            }
            ir.transcript.extend(current_transcript);
        }
        Ok(ir)
    }

    fn parse_context(&mut self) {
        self.context.previous_response_id = self
            .original
            .get("previous_response_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        self.context.conversation = self.original.get("conversation").cloned();
        self.context.metadata = self.original.get("metadata").cloned();
        self.context.end_user_id = self
            .original
            .get("user")
            .or_else(|| self.original.get("user_id"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| {
                self.original
                    .get("metadata")
                    .and_then(|metadata| metadata.get("user_id"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            });
        self.context.safety_identifier = self
            .original
            .get("safety_identifier")
            .and_then(Value::as_str)
            .map(str::to_owned);
        self.context.cache.key = self
            .original
            .get("prompt_cache_key")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| {
                (self.protocol == Protocol::OpenAiChatCompletions)
                    .then(|| self.context.end_user_id.clone())
                    .flatten()
            })
            .or_else(|| {
                (self.protocol == Protocol::AnthropicMessages)
                    .then(|| {
                        self.original
                            .get("metadata")
                            .and_then(|metadata| metadata.get("user_id"))
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    })
                    .flatten()
            });
        self.context.cache.options = self.original.get("prompt_cache_options").cloned();
        self.context.cache.retention = self.original.get("prompt_cache_retention").cloned();
        self.context.cache.control = self.original.get("cache_control").cloned();
        self.context.store = self.original.get("store").cloned();
        self.context.include = self.original.get("include").cloned();
        self.context.truncation = self.original.get("truncation").cloned();

        for (key, value) in &self.original {
            if is_context_extension_key(key) {
                self.context
                    .represented_extensions
                    .insert(key.clone(), value.clone());
            }
        }
    }

    fn inherit_context(&mut self, previous: &RequestContext) {
        if self.context.conversation.is_none() {
            self.context.conversation.clone_from(&previous.conversation);
        }
        if self.context.end_user_id.is_none() {
            self.context.end_user_id.clone_from(&previous.end_user_id);
        }
        if self.context.safety_identifier.is_none() {
            self.context
                .safety_identifier
                .clone_from(&previous.safety_identifier);
        }
        if self.context.metadata.is_none() {
            self.context.metadata.clone_from(&previous.metadata);
        }
        if self.context.cache.key.is_none() {
            self.context.cache.key.clone_from(&previous.cache.key);
        }
        if self.context.cache.options.is_none() {
            self.context
                .cache
                .options
                .clone_from(&previous.cache.options);
        }
        if self.context.cache.retention.is_none() {
            self.context
                .cache
                .retention
                .clone_from(&previous.cache.retention);
        }
        if self.context.cache.control.is_none() {
            self.context
                .cache
                .control
                .clone_from(&previous.cache.control);
        }
        self.context.represented_extensions = previous
            .represented_extensions
            .iter()
            .chain(&self.context.represented_extensions)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
    }

    pub fn append_chat_assistant_turn(&mut self, turn: &Value) {
        let Ok(parsed) = Self::parse(
            Protocol::OpenAiChatCompletions,
            &serde_json::json!({"messages": [turn]}),
        ) else {
            return;
        };
        self.transcript.extend(parsed.transcript);
    }

    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    fn parse_messages(&mut self) {
        if let Some(system) = self.original.get("system") {
            self.instructions.push(system.clone());
        }
        if let Some(messages) = self
            .original
            .get("messages")
            .and_then(Value::as_array)
            .cloned()
        {
            let mut saw_non_system = false;
            for value in &messages {
                let is_system = value.get("role").and_then(Value::as_str) == Some("system");
                if !saw_non_system && is_system {
                    // Leading system messages are the protocol's top-level system,
                    // kept as instructions (the Messages dialect has no message form).
                    if let Some(content) = value.get("content") {
                        self.instructions.push(content.clone());
                    }
                    continue;
                }
                if !is_system {
                    saw_non_system = true;
                }
                // Mid-conversation system messages keep their role in the transcript;
                // the renderer maps them per target dialect (system-reminder for chat,
                // folded into top-level system for messages).
                self.parse_message(value, Protocol::AnthropicMessages);
            }
        }
        self.tools = self
            .original
            .get("tools")
            .and_then(Value::as_array)
            .map(|tools| tools.iter().map(IrTool::from_messages).collect())
            .unwrap_or_default();
        self.tool_choice = self.original.get("tool_choice").cloned();
    }

    fn parse_chat(&mut self) {
        if let Some(messages) = self
            .original
            .get("messages")
            .and_then(Value::as_array)
            .cloned()
        {
            for value in &messages {
                // Every message keeps its role in the transcript, including system and
                // developer; the renderer maps them per target dialect.
                self.parse_message(value, Protocol::OpenAiChatCompletions);
            }
        }
        self.tools = self
            .original
            .get("tools")
            .and_then(Value::as_array)
            .map(|tools| tools.iter().map(IrTool::from_chat).collect())
            .unwrap_or_default();
        self.tool_choice = self.original.get("tool_choice").cloned();
    }

    fn parse_responses(&mut self) {
        if let Some(instructions) = self.original.get("instructions") {
            self.instructions.push(instructions.clone());
        }
        if let Some(input) = self.original.get("input") {
            if let Some(text) = input.as_str() {
                self.transcript.push(IrConversationItem::Message(IrMessage {
                    role: "user".to_owned(),
                    content: vec![IrContent::Text {
                        text: text.to_owned(),
                        extensions: ContentExtensions::default(),
                    }],
                    item_id: None,
                    tool_call_id: None,
                    tool_calls: Vec::new(),
                    opaque_extensions: Map::new(),
                }));
            }
        }
        if let Some(input) = self
            .original
            .get("input")
            .and_then(Value::as_array)
            .cloned()
        {
            for value in &input {
                match value.get("type").and_then(Value::as_str) {
                    Some("message") => {
                        // Every message role (including developer) is kept in the
                        // transcript; the renderer maps them per target dialect.
                        self.parse_message(value, Protocol::OpenAiResponses);
                    }
                    Some("function_call") => self.transcript.push(IrConversationItem::Message(
                        IrMessage::assistant_tool_call(IrToolCall::from_responses_call(value)),
                    )),
                    Some("reasoning") => self
                        .transcript
                        .push(IrConversationItem::Reasoning(value.clone())),
                    Some("function_call_output") => {
                        // A string output is text; a non-string (JSON object) is kept raw.
                        let output = value.get("output").cloned().unwrap_or(Value::Null);
                        let content = match output {
                            Value::String(text) => vec![IrContent::Text {
                                text,
                                extensions: ContentExtensions::default(),
                            }],
                            other => vec![IrContent::Raw(other)],
                        };
                        self.transcript.push(IrConversationItem::Message(IrMessage {
                            role: "tool".to_owned(),
                            content,
                            item_id: value.get("id").and_then(Value::as_str).map(str::to_owned),
                            tool_call_id: value
                                .get("call_id")
                                .and_then(Value::as_str)
                                .map(str::to_owned),
                            tool_calls: Vec::new(),
                            opaque_extensions: opaque_fields(
                                value.as_object().unwrap_or(&Map::new()),
                                &["id", "type", "call_id", "output"],
                            ),
                        }))
                    }
                    _ => self
                        .transcript
                        .push(IrConversationItem::Message(IrMessage::custom(
                            value.clone(),
                        ))),
                }
            }
        }
        self.tools = self
            .original
            .get("tools")
            .and_then(Value::as_array)
            .map(|tools| tools.iter().map(IrTool::from_responses).collect())
            .unwrap_or_default();
        self.tool_choice = self.original.get("tool_choice").cloned();
    }

    fn parse_message(&mut self, value: &Value, protocol: Protocol) {
        let Some(object) = value.as_object() else {
            self.transcript
                .push(IrConversationItem::Message(IrMessage::custom(
                    value.clone(),
                )));
            return;
        };
        let role = object
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or("user")
            .to_owned();
        let mut message = IrMessage {
            role: role.clone(),
            content: normalize_content(object.get("content")),
            item_id: object.get("id").and_then(Value::as_str).map(str::to_owned),
            tool_call_id: object
                .get("tool_call_id")
                .or_else(|| object.get("call_id"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            tool_calls: Vec::new(),
            opaque_extensions: opaque_fields(
                object,
                &[
                    "id",
                    "type",
                    "role",
                    "content",
                    "tool_calls",
                    "tool_call_id",
                    "call_id",
                ],
            ),
        };
        if protocol != Protocol::AnthropicMessages {
            if let Some(calls) = object.get("tool_calls").and_then(Value::as_array) {
                message.tool_calls = calls.iter().map(IrToolCall::from_chat).collect();
            }
        }
        self.transcript.push(IrConversationItem::Message(message));
    }

    fn collect_opaque_extensions(&mut self) {
        const STRUCTURAL: &[&str] = &[
            "model",
            "messages",
            "system",
            "input",
            "instructions",
            "tools",
            "tool_choice",
            "previous_response_id",
            "stream",
            "max_tokens",
            "max_completion_tokens",
            "max_output_tokens",
        ];
        for (key, value) in &self.original {
            if !STRUCTURAL.contains(&key.as_str()) {
                self.opaque_extensions.insert(key.clone(), value.clone());
            }
        }
    }

    #[cfg(test)]
    fn render(&self, target: Protocol, caps: &ModelCapabilities) -> Result<Value, ConversionError> {
        self.render_with_report(target, caps)
            .map(|(rendered, _)| rendered)
    }

    /// Render into `target`, also returning the server-tool policy report so the
    /// proxy can surface dropped/mapped tools for any target dialect.
    pub fn render_with_report(
        &self,
        target: Protocol,
        caps: &ModelCapabilities,
    ) -> Result<(Value, RequestConversionReport), ConversionError> {
        match target {
            Protocol::AnthropicMessages => self.render_messages(caps),
            Protocol::OpenAiChatCompletions => self.render_chat(caps),
            Protocol::OpenAiResponses => self.render_responses(caps),
        }
    }

    fn render_chat(
        &self,
        caps: &ModelCapabilities,
    ) -> Result<(Value, RequestConversionReport), ConversionError> {
        let mut rendered = self.render_chat_base(caps);
        let report = self.normalize_chat_capabilities(caps, &mut rendered)?;
        Ok((Value::Object(rendered), report))
    }

    fn base_output(&self) -> Map<String, Value> {
        let mut out: Map<String, Value> = self.opaque_extensions.clone().into_iter().collect();
        if let Some(model) = &self.model {
            out.insert("model".to_owned(), Value::String(model.clone()));
        }
        if let Some(stream) = self.stream {
            out.insert("stream".to_owned(), Value::Bool(stream));
        }
        // Structural fields, including `previous_response_id`, are represented separately and
        // therefore never leak from the source protocol into a translated request.
        out
    }

    fn render_chat_base(&self, caps: &ModelCapabilities) -> Map<String, Value> {
        let mut out = self.base_output();
        self.apply_context(Protocol::OpenAiChatCompletions, caps, &mut out);
        for key in [
            "metadata",
            "cache_control",
            "context_management",
            "service_tier",
        ] {
            if self.protocol == Protocol::AnthropicMessages {
                out.remove(key);
            }
        }
        // Responses-owned top-level parameters are consumed by the capability
        // normalization (reasoning.effort, text.format) and must not leak into a
        // Chat Completions request the upstream does not recognize.
        if self.protocol == Protocol::OpenAiResponses {
            out.remove("reasoning");
            out.remove("text");
        }
        // The output budget renders under the provider's preferred field name;
        // `normalize_chat_capabilities` later clamps and renames it.
        if let Some(value) = &self.max_output_tokens {
            out.insert("max_tokens".to_owned(), value.clone());
        }
        let mut messages = Vec::new();
        for instruction in &self.instructions {
            match instruction {
                Value::String(text) => {
                    messages.push(serde_json::json!({
                        "role": "system",
                        "content": [{"type": "text", "text": text}]
                    }));
                }
                Value::Array(blocks) => {
                    for block in blocks {
                        let Some(text) = block.get("text").and_then(Value::as_str) else {
                            continue;
                        };
                        if text.starts_with("x-anthropic-billing-header:") {
                            continue;
                        }
                        messages.push(serde_json::json!({
                            "role": "system",
                            "content": [{"type": "text", "text": text}]
                        }));
                    }
                }
                _ => {}
            }
        }
        let mut pending_reasoning = None;
        let mut saw_non_system = false;
        for item in &self.transcript {
            match item {
                IrConversationItem::Reasoning(reasoning) => {
                    if let Some(text) = reasoning_text(reasoning) {
                        pending_reasoning = Some(text);
                    }
                }
                IrConversationItem::Message(message) => {
                    if message.role == "system" {
                        // The Chat dialect natively supports a leading system message;
                        // a mid-conversation one becomes a system-reminder user message
                        // (many chat providers mishandle a system message mid-conversation).
                        let text = message
                            .content
                            .iter()
                            .filter_map(|content| match content {
                                IrContent::Text { text, .. } => Some(text.as_str()),
                                _ => None,
                            })
                            .collect::<Vec<_>>()
                            .join("\n\n");
                        if !text.is_empty() {
                            if !saw_non_system {
                                messages.push(serde_json::json!({
                                    "role": "system",
                                    "content": [{"type": "text", "text": text}]
                                }));
                            } else {
                                messages.push(serde_json::json!({
                                    "role": "user",
                                    "content": [{
                                        "type": "text",
                                        "text": format!("<system-reminder>\n{text}\n</system-reminder>")
                                    }]
                                }));
                            }
                        }
                        continue;
                    }
                    saw_non_system = true;
                    if message.role == "developer" && !caps.accept_developer_role {
                        // The upstream rejects the developer role; fold into a system
                        // message (content preserved).
                        let text = message
                            .content
                            .iter()
                            .filter_map(|content| match content {
                                IrContent::Text { text, .. } => Some(text.clone()),
                                _ => None,
                            })
                            .collect::<Vec<_>>()
                            .join("\n\n");
                        if !text.is_empty() {
                            messages.push(serde_json::json!({
                                "role": "system",
                                "content": [{"type": "text", "text": text}]
                            }));
                        }
                        continue;
                    }
                    let mut rendered = message.to_chat_values(caps.cache.block_cache_control);
                    if message.role == "assistant" {
                        if let Some(reasoning) = pending_reasoning.take() {
                            if let Some(object) =
                                rendered.first_mut().and_then(Value::as_object_mut)
                            {
                                object.insert(
                                    "reasoning_content".to_owned(),
                                    Value::String(reasoning),
                                );
                            }
                        }
                    }
                    messages.extend(rendered);
                }
            }
        }
        if let Some(reasoning) = pending_reasoning {
            messages.push(serde_json::json!({
                "role":"assistant",
                "content":"",
                "reasoning_content":reasoning
            }));
        }
        out.insert("messages".to_owned(), Value::Array(messages));
        if !self.tools.is_empty() {
            out.insert(
                "tools".to_owned(),
                Value::Array(self.tools.iter().map(IrTool::to_chat).collect()),
            );
        }
        if let Some(choice) = &self.tool_choice {
            out.insert("tool_choice".to_owned(), choice.clone());
        }
        out
    }

    fn normalize_chat_capabilities(
        &self,
        caps: &ModelCapabilities,
        out: &mut Map<String, Value>,
    ) -> Result<RequestConversionReport, ConversionError> {
        if let Some(value) = out.remove("max_tokens") {
            let value = value.as_u64().map_or(value.clone(), |value| {
                Value::Number(
                    caps.max_tokens_cap
                        .map_or(value, |cap| value.min(cap))
                        .into(),
                )
            });
            let field = match caps.max_tokens_field {
                crate::dialect::MaxTokensField::MaxTokens => "max_tokens",
                crate::dialect::MaxTokensField::MaxCompletionTokens => "max_completion_tokens",
            };
            out.insert(field.to_owned(), value);
        }
        if self.stream == Some(true) && caps.stream_include_usage {
            out.insert(
                "stream_options".to_owned(),
                serde_json::json!({"include_usage": true}),
            );
        }
        if let Some(split) = caps.reasoning_split {
            out.insert("reasoning_split".to_owned(), Value::Bool(split));
        }
        if self.has_image_content() && !caps.image_input {
            return Err(ConversionError::unsupported(
                "messages[].content.image",
                "upstream does not support image input",
            ));
        }
        // Downstream effort arrives as Messages `output_config.effort` or Responses
        // `reasoning.effort`; both map to the upstream thinking/reasoning controls.
        let effort = self
            .original
            .get("output_config")
            .and_then(Value::as_object)
            .and_then(|config| config.get("effort"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| {
                self.original
                    .get("reasoning")
                    .and_then(Value::as_object)
                    .and_then(|reasoning| reasoning.get("effort"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            });
        let thinking_type = self
            .original
            .get("thinking")
            .and_then(|thinking| thinking.get("type"))
            .and_then(Value::as_str);
        match caps.thinking_param {
            ThinkingParam::TopLevel | ThinkingParam::Adaptive => {
                // A Messages thinking block maps its type; a Responses request has no
                // thinking parameter, so one is synthesized from reasoning.effort.
                let on = match caps.thinking_param {
                    ThinkingParam::Adaptive => "adaptive",
                    _ => "enabled",
                };
                let mapped = if self.original.contains_key("thinking") {
                    // A Messages thinking block maps its type; a missing type means on.
                    Some(if thinking_type == Some("disabled") {
                        "disabled"
                    } else {
                        on
                    })
                } else {
                    // A Responses request has no thinking parameter; synthesize one
                    // from reasoning.effort.
                    effort.as_deref().map(|effort| {
                        if effort == "none" || effort == "minimal" {
                            "disabled"
                        } else {
                            on
                        }
                    })
                };
                if let Some(mapped) = mapped {
                    out.insert("thinking".to_owned(), serde_json::json!({"type": mapped}));
                }
            }
            ThinkingParam::EnableThinking => {
                let disabled = thinking_type == Some("disabled")
                    || effort
                        .as_deref()
                        .is_some_and(|effort| effort == "none" || effort == "minimal");
                out.insert("enable_thinking".to_owned(), Value::Bool(!disabled));
                out.remove("thinking");
            }
            ThinkingParam::None => {
                out.remove("thinking");
                out.remove("enable_thinking");
            }
        }
        if let Some(config) = caps
            .reasoning_effort
            .as_ref()
            .filter(|config| config.enabled)
        {
            let level = effort
                .as_deref()
                .or(config.default.as_deref())
                .and_then(|effort| caps.clamp_effort(effort));
            if let Some(level) = level {
                out.insert("reasoning_effort".to_owned(), Value::String(level));
            }
        }
        if !caps.accept_stop {
            out.remove("stop");
        } else if let Some(sequences) = self
            .original
            .get("stop_sequences")
            .and_then(Value::as_array)
        {
            let sequences: Vec<Value> = sequences
                .iter()
                .filter(|value| value.as_str().is_some_and(|value| !value.is_empty()))
                .cloned()
                .collect();
            if !sequences.is_empty() {
                out.insert(
                    "stop".to_owned(),
                    if sequences.len() == 1 {
                        sequences[0].clone()
                    } else {
                        Value::Array(sequences)
                    },
                );
            }
        }
        if let Some(parallel) = self
            .original
            .get("parallel_tool_use")
            .and_then(Value::as_bool)
        {
            out.insert("parallel_tool_calls".to_owned(), Value::Bool(parallel));
        }
        if let Some(choice) = self.tool_choice.as_ref() {
            if let Some(object) = choice.as_object() {
                let mapped = match object.get("type").and_then(Value::as_str) {
                    Some("auto") => Some(serde_json::json!("auto")),
                    Some("any") => Some(serde_json::json!("required")),
                    Some("none") => Some(serde_json::json!("none")),
                    Some("tool") => object.get("name").map(
                        |name| serde_json::json!({"type":"function", "function":{"name":name}}),
                    ),
                    // OpenAI Responses shape: {"type":"function","name":...}.
                    Some("function") => object.get("name").map(
                        |name| serde_json::json!({"type":"function", "function":{"name":name}}),
                    ),
                    // allowed_tools limits the subset; map to auto (upstream has no equivalent).
                    Some("allowed_tools") => Some(serde_json::json!("auto")),
                    _ => None,
                };
                if let Some(mapped) = mapped {
                    out.insert("tool_choice".to_owned(), mapped);
                }
            }
        }
        let format = self
            .original
            .get("output_config")
            .and_then(Value::as_object)
            .and_then(|config| config.get("format"))
            .or_else(|| {
                // OpenAI Responses shape: {"text": {"format": ...}}.
                self.original
                    .get("text")
                    .and_then(Value::as_object)
                    .and_then(|text| text.get("format"))
            });
        if let Some(format) = format {
            match format.get("type").and_then(Value::as_str).unwrap_or("text") {
                "text" => {}
                "json_object" => {
                    // The simple json_object format is broadly supported; keep it.
                    out.insert(
                        "response_format".to_owned(),
                        serde_json::json!({"type":"json_object"}),
                    );
                }
                "json_schema" => match caps.response_format {
                    ResponseFormatCap::JsonSchema => {
                        out.insert(
                            "response_format".to_owned(),
                            serde_json::json!({
                                "type":"json_schema",
                                "json_schema":{"name":"output","schema":format.get("schema").cloned().unwrap_or_else(|| serde_json::json!({}))}
                            }),
                        );
                    }
                    ResponseFormatCap::JsonObject => {
                        out.insert(
                            "response_format".to_owned(),
                            serde_json::json!({"type":"json_object"}),
                        );
                    }
                    ResponseFormatCap::Text => {
                        return Err(ConversionError::unsupported(
                            "output_config.format",
                            "upstream does not support structured output formats",
                        ));
                    }
                },
                other => {
                    return Err(ConversionError::unsupported(
                        "output_config.format",
                        format!("unknown format type {other:?}"),
                    ));
                }
            }
        }
        let mut report = RequestConversionReport::default();
        let last_user_text = self.last_user_text();
        let rendered_tools = render_tools_with_policy(
            &self.tools,
            Protocol::OpenAiChatCompletions,
            caps,
            &mut report,
            IrTool::to_chat,
            |tool| {
                let (name, description, parameters) = server_tool_function_shape(tool);
                serde_json::json!({
                    "type": "function",
                    "function": {"name": name, "description": description, "parameters": parameters}
                })
            },
            last_user_text.as_deref(),
        );
        if !rendered_tools.is_empty() {
            out.insert("tools".to_owned(), Value::Array(rendered_tools));
        } else {
            out.remove("tools");
        }
        merge_native_params(out, caps, &mut report, last_user_text.as_deref());
        Ok(report)
    }

    fn last_user_text(&self) -> Option<String> {
        self.transcript.iter().rev().find_map(|item| {
            let IrConversationItem::Message(message) = item else {
                return None;
            };
            (message.role == "user").then(|| {
                message
                    .content
                    .iter()
                    .filter_map(|content| match content {
                        IrContent::Text { text, .. } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join(" ")
            })
        })
    }

    fn has_image_content(&self) -> bool {
        self.transcript
            .iter()
            .filter_map(|item| match item {
                IrConversationItem::Message(message) => Some(message),
                IrConversationItem::Reasoning(_) => None,
            })
            .flat_map(|message| message.content.iter())
            .any(|content| matches!(content, IrContent::Image(_)))
    }

    fn render_messages(
        &self,
        caps: &ModelCapabilities,
    ) -> Result<(Value, RequestConversionReport), ConversionError> {
        if self.has_image_content() && !caps.image_input {
            return Err(ConversionError::unsupported(
                "messages[].content.image",
                "upstream does not support image input",
            ));
        }
        let mut out = self.base_output();
        self.apply_context(Protocol::AnthropicMessages, caps, &mut out);
        // The Messages API requires max_tokens (no default; Kimi rejects a missing
        // field with 400). Clients speaking Chat/Responses may omit their optional
        // output budget (e.g. Codex without model metadata), so use the provider's
        // configured default when present, then clamp to the provider cap.
        let max_tokens = self
            .max_output_tokens
            .clone()
            .or_else(|| caps.default_max_tokens.map(Value::from))
            .unwrap_or(Value::Null);
        if !max_tokens.is_null() {
            let max_tokens = max_tokens.as_u64().map_or(max_tokens, |value| {
                serde_json::json!(caps.max_tokens_cap.map_or(value, |cap| value.min(cap)))
            });
            out.insert("max_tokens".to_owned(), max_tokens);
        }
        // Responses reasoning.effort maps to the upstream Messages thinking parameter
        // (Messages providers do not speak `output_config`).
        if let Some(reasoning) = out.remove("reasoning") {
            let effort = reasoning
                .get("effort")
                .and_then(Value::as_str)
                .map(str::to_owned);
            match caps.thinking_param {
                ThinkingParam::TopLevel | ThinkingParam::Adaptive => {
                    let on = match caps.thinking_param {
                        ThinkingParam::Adaptive => "adaptive",
                        _ => "enabled",
                    };
                    if let Some(effort) = &effort {
                        let thinking_type = if effort == "none" || effort == "minimal" {
                            "disabled"
                        } else {
                            on
                        };
                        out.insert(
                            "thinking".to_owned(),
                            serde_json::json!({"type": thinking_type}),
                        );
                    }
                }
                ThinkingParam::EnableThinking => {
                    let enabled = effort
                        .as_deref()
                        .is_none_or(|effort| effort != "none" && effort != "minimal");
                    out.insert("enable_thinking".to_owned(), Value::Bool(enabled));
                }
                ThinkingParam::None => {}
            }
        }
        if self.protocol == Protocol::OpenAiResponses {
            // `text` carries Responses output configuration; consumed by the
            // thinking mapping above (format has no Messages equivalent).
            out.remove("text");
        }
        // Chat `stop` maps to the Messages `stop_sequences` array.
        if let Some(stop) = out.remove("stop") {
            let sequences = match stop {
                Value::String(text) if !text.is_empty() => vec![Value::String(text)],
                Value::Array(values) => values
                    .into_iter()
                    .filter(|value| value.as_str().is_some_and(|text| !text.is_empty()))
                    .collect(),
                _ => Vec::new(),
            };
            if !sequences.is_empty() {
                out.insert("stop_sequences".to_owned(), Value::Array(sequences));
            }
        }
        // The Messages dialect has no system/developer message role: those messages
        // fold into the top-level system (collected across the transcript).
        let mut system_extra: Vec<String> = Vec::new();
        let mut messages = Vec::new();
        for item in &self.transcript {
            match item {
                IrConversationItem::Reasoning(reasoning) => {
                    let blocks = reasoning_to_messages_blocks(reasoning);
                    if !blocks.is_empty() {
                        messages.push(serde_json::json!({"role":"assistant", "content":blocks}));
                    }
                }
                IrConversationItem::Message(message) => {
                    if message.role == "system" || message.role == "developer" {
                        system_extra.extend(message.content.iter().filter_map(|content| {
                            if let IrContent::Text { text, .. } = content {
                                Some(text.clone())
                            } else {
                                None
                            }
                        }));
                        continue;
                    }
                    messages.extend(message.to_messages_values(caps.cache.block_cache_control));
                }
            }
        }
        let mut system_text = instructions_text(&self.instructions);
        if !system_extra.is_empty() {
            if !system_text.is_empty() {
                system_text.push('\n');
            }
            system_text.push_str(&system_extra.join("\n\n"));
        }
        if !system_text.is_empty() {
            out.insert("system".to_owned(), Value::String(system_text));
        }
        out.insert("messages".to_owned(), Value::Array(messages));
        let mut report = RequestConversionReport::default();
        let rendered_tools = render_tools_with_policy(
            &self.tools,
            Protocol::AnthropicMessages,
            caps,
            &mut report,
            IrTool::to_messages,
            |tool| {
                let (name, description, parameters) = server_tool_function_shape(tool);
                serde_json::json!({
                    "name": name,
                    "description": description,
                    "input_schema": parameters,
                })
            },
            self.last_user_text().as_deref(),
        );
        if !rendered_tools.is_empty() {
            out.insert("tools".to_owned(), Value::Array(rendered_tools));
        } else {
            out.remove("tools");
        }
        if let Some(choice) = &self.tool_choice {
            out.insert(
                "tool_choice".to_owned(),
                render_messages_tool_choice(choice),
            );
        }
        Ok((Value::Object(out), report))
    }

    fn render_responses(
        &self,
        caps: &ModelCapabilities,
    ) -> Result<(Value, RequestConversionReport), ConversionError> {
        if self.has_image_content() && !caps.image_input {
            return Err(ConversionError::unsupported(
                "messages[].content.image",
                "upstream does not support image input",
            ));
        }
        let mut out = self.base_output();
        self.apply_context(Protocol::OpenAiResponses, caps, &mut out);
        // Anthropic-owned parameters with no Responses equivalent are dropped
        // (`context_management` carries claude's edit-tool directives such as
        // clear_thinking, rejected by Ark). `metadata` and `cache_control` are
        // legal Responses fields or mapped by `apply_context` and stay; Ark
        // strips `metadata` under `ark_style`.
        if self.protocol == Protocol::AnthropicMessages {
            out.remove("context_management");
        }
        // Anthropic thinking -> Responses controls. Native-thinking upstreams (Ark)
        // accept the thinking object; qwen-style upstreams document reasoning.effort
        // as the control and ignore an unknown thinking object.
        if self.protocol == Protocol::AnthropicMessages {
            if let Some(thinking) = self.original.get("thinking") {
                let thinking_type = thinking.get("type").and_then(Value::as_str);
                match caps.thinking_param {
                    ThinkingParam::TopLevel | ThinkingParam::Adaptive => {
                        // Native thinking dialects keep the object with a normalized
                        // type (Ark accepts enabled/disabled/auto; adaptive is
                        // collapsed to the dialect's "on" value).
                        let on = match caps.thinking_param {
                            ThinkingParam::Adaptive => "adaptive",
                            _ => "enabled",
                        };
                        let mapped = if thinking_type == Some("disabled") {
                            "disabled"
                        } else {
                            on
                        };
                        out.insert("thinking".to_owned(), serde_json::json!({"type": mapped}));
                    }
                    ThinkingParam::EnableThinking => {
                        // qwen responses: reasoning.effort is the documented control;
                        // an unknown thinking object would be silently ignored. Only
                        // force the disabled case when no explicit effort was provided
                        // (an explicit output_config.effort is respected).
                        out.remove("thinking");
                        let has_explicit_effort = self
                            .original
                            .get("output_config")
                            .and_then(Value::as_object)
                            .and_then(|config| config.get("effort"))
                            .is_some();
                        if thinking_type == Some("disabled") && !has_explicit_effort {
                            out.insert(
                                "reasoning".to_owned(),
                                serde_json::json!({"effort": "none"}),
                            );
                        }
                    }
                    ThinkingParam::None => {
                        out.remove("thinking");
                    }
                }
            }
        }
        // Responses `max_output_tokens` is optional; pass the client budget through.
        if let Some(value) = &self.max_output_tokens {
            out.insert("max_output_tokens".to_owned(), value.clone());
        }
        if !self.instructions.is_empty() {
            out.insert(
                "instructions".to_owned(),
                Value::String(instructions_text(&self.instructions)),
            );
        }
        let mut input = Vec::new();
        for item in &self.transcript {
            match item {
                IrConversationItem::Reasoning(reasoning) => input.push(reasoning.clone()),
                IrConversationItem::Message(message) => {
                    if message.role == "developer" && !caps.accept_developer_role {
                        // The upstream rejects the developer role; fold into a system
                        // message (content preserved).
                        let text = message
                            .content
                            .iter()
                            .filter_map(|content| match content {
                                IrContent::Text { text, .. } => Some(text.clone()),
                                _ => None,
                            })
                            .collect::<Vec<_>>()
                            .join("\n\n");
                        if !text.is_empty() {
                            input.push(serde_json::json!({
                                "type": "message",
                                "role": "system",
                                "content": [{"type": "input_text", "text": text}],
                            }));
                        }
                        continue;
                    }
                    input.extend(message.to_responses_values(false));
                }
            }
        }
        out.insert("input".to_owned(), Value::Array(input));
        let mut report = RequestConversionReport::default();
        let last_user_text = self.last_user_text();
        let rendered_tools = render_tools_with_policy(
            &self.tools,
            Protocol::OpenAiResponses,
            caps,
            &mut report,
            IrTool::to_responses,
            |tool| {
                let (name, description, parameters) = server_tool_function_shape(tool);
                serde_json::json!({
                    "type": "function",
                    "name": name,
                    "description": description,
                    "parameters": parameters,
                })
            },
            last_user_text.as_deref(),
        );
        if !rendered_tools.is_empty() {
            out.insert("tools".to_owned(), Value::Array(rendered_tools));
        } else {
            out.remove("tools");
        }
        merge_native_params(&mut out, caps, &mut report, last_user_text.as_deref());
        if let Some(choice) = &self.tool_choice {
            out.insert(
                "tool_choice".to_owned(),
                render_responses_tool_choice(choice),
            );
        }
        normalize_responses_parameters(&mut out, caps);
        Ok((Value::Object(out), report))
    }

    fn apply_context(
        &self,
        target: Protocol,
        caps: &ModelCapabilities,
        out: &mut Map<String, Value>,
    ) {
        // Cache controls are emitted only when the upstream supports them
        // (conservative; see docs/cache-adaptation-design.md §1.2).
        // `prompt_cache_key` is a chat/responses routing hint (Kimi, MiniMax
        // responses, Grok); the Messages dialect maps the identity to metadata.user_id.
        let keep_key = matches!(
            target,
            Protocol::OpenAiChatCompletions | Protocol::OpenAiResponses
        ) && caps.cache.prompt_cache_key;
        // Top-level `cache_control` is the Anthropic auto-breakpoint; the
        // Responses/Chat dialects have no such field.
        let keep_top_level_cache_control =
            target == Protocol::AnthropicMessages && caps.cache.block_cache_control;
        // `prompt_cache_options`/deprecated `prompt_cache_retention` are OpenAI
        // protocol-only.
        let keep_options = target != Protocol::AnthropicMessages && caps.cache.prompt_cache_options;

        if !keep_key {
            out.remove("prompt_cache_key");
        }
        if !keep_options {
            out.remove("prompt_cache_options");
            out.remove("prompt_cache_retention");
        }
        if !keep_top_level_cache_control {
            out.remove("cache_control");
        }
        for (key, value) in &self.context.represented_extensions {
            if matches!(
                key.as_str(),
                "prompt_cache_key"
                    | "prompt_cache_retention"
                    | "prompt_cache_options"
                    | "cache_control"
            ) && !(key == "prompt_cache_key" && keep_key)
                && !(key == "cache_control" && keep_top_level_cache_control)
            {
                continue;
            }
            out.entry(key.clone()).or_insert_with(|| value.clone());
        }
        if let Some(metadata) = &self.context.metadata {
            out.entry("metadata".to_owned())
                .or_insert_with(|| metadata.clone());
        }
        if keep_top_level_cache_control {
            if let Some(cache_control) = &self.context.cache.control {
                out.entry("cache_control".to_owned())
                    .or_insert_with(|| cache_control.clone());
            }
        }
        if let Some(conversation) = &self.context.conversation {
            out.entry("conversation".to_owned())
                .or_insert_with(|| conversation.clone());
        }
        if let Some(identifier) = &self.context.safety_identifier {
            out.entry("safety_identifier".to_owned())
                .or_insert_with(|| Value::String(identifier.clone()));
        }
        if keep_key {
            if let Some(key) = &self.context.cache.key {
                out.entry("prompt_cache_key".to_owned())
                    .or_insert_with(|| Value::String(key.clone()));
            }
        }
        if keep_options {
            if let Some(options) = &self.context.cache.options {
                out.entry("prompt_cache_options".to_owned())
                    .or_insert_with(|| options.clone());
            }
            if let Some(retention) = &self.context.cache.retention {
                out.entry("prompt_cache_retention".to_owned())
                    .or_insert_with(|| retention.clone());
            }
        }
        for (key, value) in [
            ("store", self.context.store.as_ref()),
            ("include", self.context.include.as_ref()),
            ("truncation", self.context.truncation.as_ref()),
        ] {
            if let Some(value) = value {
                out.entry(key.to_owned()).or_insert_with(|| value.clone());
            }
        }

        match target {
            Protocol::AnthropicMessages => {
                if let Some(user_id) = self
                    .context
                    .cache
                    .key
                    .as_ref()
                    .or(self.context.end_user_id.as_ref())
                {
                    let metadata = out
                        .entry("metadata".to_owned())
                        .or_insert_with(|| Value::Object(Map::new()));
                    if let Some(metadata) = metadata.as_object_mut() {
                        metadata
                            .entry("user_id".to_owned())
                            .or_insert_with(|| Value::String(user_id.clone()));
                    }
                }
            }
            Protocol::OpenAiChatCompletions => {
                if let Some(user_id) = self
                    .context
                    .cache
                    .key
                    .as_ref()
                    .or(self.context.end_user_id.as_ref())
                {
                    out.entry("user".to_owned())
                        .or_insert_with(|| Value::String(user_id.clone()));
                }
            }
            Protocol::OpenAiResponses => {
                if let Some(user_id) = &self.context.end_user_id {
                    out.entry("user".to_owned())
                        .or_insert_with(|| Value::String(user_id.clone()));
                }
            }
        }
    }
}

fn is_context_extension_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    [
        "session",
        "conversation",
        "thread",
        "prompt_cache",
        "cache_key",
        "cache_id",
        "user_id",
        "tenant",
        "project",
        "trace_id",
        "client_id",
    ]
    .iter()
    .any(|fragment| key.contains(fragment))
}

fn normalize_content(content: Option<&Value>) -> Vec<IrContent> {
    match content {
        Some(Value::Array(values)) => values.iter().map(IrContent::parse).collect(),
        Some(Value::String(text)) => vec![IrContent::Text {
            text: text.clone(),
            extensions: ContentExtensions::default(),
        }],
        Some(value) if !value.is_null() => vec![IrContent::Raw(value.clone())],
        _ => Vec::new(),
    }
}

fn opaque_fields(object: &Map<String, Value>, structural: &[&str]) -> Map<String, Value> {
    object
        .iter()
        .filter(|(key, _)| !structural.contains(&key.as_str()) && key.as_str() != "cache_control")
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

/// Render the tools array under the provider's server-tool policy, shared by the
/// Chat and Responses renderers. `render_plain` renders a normal function tool in the
/// target dialect; `render_mapped` renders a mapped server tool as a function tool.
fn render_tools_with_policy(
    tools: &[IrTool],
    target: Protocol,
    caps: &ModelCapabilities,
    report: &mut RequestConversionReport,
    render_plain: impl Fn(&IrTool) -> Value,
    render_mapped: impl Fn(&IrTool) -> Value,
    last_user_query: Option<&str>,
) -> Vec<Value> {
    let mut rendered_tools = Vec::new();
    for tool in tools {
        if is_freeform_tool(tool) && target == Protocol::OpenAiResponses {
            // Codex's freeform tool (`type: "custom"` + `format`) is natively
            // supported by the Responses dialect (custom free-text tool); keep it
            // instead of treating it as a server-side tool.
            rendered_tools.push(tool.raw.clone());
            continue;
        }
        if is_server_tool(tool) {
            let name = tool.name.clone().unwrap_or_else(|| tool.kind.clone());
            match caps.server_tools {
                ServerToolPolicy::Drop => report.dropped_server_tools.push(name),
                ServerToolPolicy::MapToFunction => {
                    if matches!(
                        server_tool_short_name(tool),
                        Some("web_search" | "web_fetch")
                    ) {
                        let short = server_tool_short_name(tool).expect("matched above");
                        report.mapped_server_tools.push(short.to_owned());
                        rendered_tools.push(render_mapped(tool));
                    } else {
                        report.dropped_server_tools.push(name);
                    }
                }
                ServerToolPolicy::Passthrough => rendered_tools.push(tool.raw.clone()),
                ServerToolPolicy::ProviderNative => match server_tool_short_name(tool) {
                    Some("web_search") => {
                        if let Some(template) = &caps.search_tool_template {
                            rendered_tools.push(
                                crate::conversion_utils::render_search_tool_template(
                                    template,
                                    last_user_query,
                                ),
                            );
                            report.mapped_server_tools.push("web_search".to_owned());
                        } else if caps.search_request_params.is_some() {
                            report.mapped_server_tools.push("web_search".to_owned());
                        } else {
                            report.dropped_server_tools.push(name);
                        }
                    }
                    Some("web_fetch") if caps.fetch_request_params.is_some() => {
                        report.mapped_server_tools.push("web_fetch".to_owned());
                    }
                    Some("code_execution") if caps.code_interpreter_request_params.is_some() => {
                        report.mapped_server_tools.push("code_execution".to_owned());
                    }
                    _ => report.dropped_server_tools.push(name),
                },
            }
        } else {
            rendered_tools.push(render_plain(tool));
        }
    }
    rendered_tools
}

/// Merge top-level provider-native request parameters for mapped and always-enabled
/// server tools (e.g. qwen's `enable_search`). Search parameters may embed a
/// `{search_query}` placeholder resolved with the last user text.
fn merge_native_params(
    out: &mut Map<String, Value>,
    caps: &ModelCapabilities,
    report: &mut RequestConversionReport,
    last_user_query: Option<&str>,
) {
    if !report.mapped_server_tools.is_empty() {
        for (tool_name, params) in [
            ("web_search", caps.search_request_params.as_ref()),
            ("web_fetch", caps.fetch_request_params.as_ref()),
            (
                "code_execution",
                caps.code_interpreter_request_params.as_ref(),
            ),
        ] {
            if report
                .mapped_server_tools
                .iter()
                .any(|name| name == tool_name)
            {
                if let Some(params) = params {
                    let rendered = if tool_name == "web_search" {
                        crate::conversion_utils::render_search_tool_template(
                            params,
                            last_user_query,
                        )
                    } else {
                        params.clone()
                    };
                    if let Value::Object(rendered) = rendered {
                        out.extend(rendered);
                    }
                }
            }
        }
    }
    for tool_name in &caps.always_enable_tools {
        let normalized = match tool_name.as_str() {
            "WebSearch" => "web_search",
            "WebFetch" => "web_fetch",
            "CodeExecution" => "code_execution",
            other => other,
        };
        if report
            .mapped_server_tools
            .iter()
            .any(|mapped| mapped == normalized)
        {
            continue;
        }
        report.mapped_server_tools.push(normalized.to_owned());
        let params = match normalized {
            "web_search" => caps.search_request_params.as_ref(),
            "web_fetch" => caps.fetch_request_params.as_ref(),
            "code_execution" => caps.code_interpreter_request_params.as_ref(),
            _ => None,
        };
        if let Some(params) = params {
            let rendered = if normalized == "web_search" {
                crate::conversion_utils::render_search_tool_template(params, last_user_query)
            } else {
                params.clone()
            };
            if let Value::Object(rendered) = rendered {
                out.extend(rendered);
            }
        }
    }
    if report
        .mapped_server_tools
        .iter()
        .any(|name| name == "code_execution")
    {
        out.insert("stream".to_owned(), Value::Bool(true));
    }
}

/// Whether a parsed tool is a server-side tool (has a non-function type, or a recognized
/// server tool name). Mirrors the Messages-dialect `is_server_tool`: any explicit
/// non-`function`/`custom` type (`web_search_20260209`, `file_search`, `bash`, ...) and
/// the well-known camelCase names (`WebSearch`, `WebFetch`) count as server tools, so the
/// `ServerToolPolicy` applies to them instead of leaking a raw definition upstream.
/// Whether a parsed tool is a server-side tool (has a non-function type, or a recognized
/// server tool name). Mirrors the Messages-dialect `is_server_tool`: any explicit
/// non-`function`/`custom` type (`web_search_20260209`, `file_search`, `bash`, ...) and
/// the well-known camelCase names (`WebSearch`, `WebFetch`) count as server tools, so the
/// `ServerToolPolicy` applies to them instead of leaking a raw definition upstream.
///
/// Codex's freeform tool also reuses `type: "custom"` but carries no schema (a `format`
/// descriptor instead); see `is_freeform_tool`. It is a server-side tool for dialects
/// without a custom type (chat, messages), but the Responses dialect supports it natively.
fn is_server_tool(tool: &IrTool) -> bool {
    let kind = tool.kind.as_str();
    if kind != "function" && kind != "custom" && !kind.is_empty() {
        return true;
    }
    // Codex's freeform tool reuses the Messages `custom` type but is a no-schema
    // free-text tool (it carries a `format` descriptor, no input_schema) — unlike
    // Anthropic's `custom`, a schema'd client function. Upstreams that only speak
    // `function` reject it, so it is treated as a server-side tool and goes
    // through the `ServerToolPolicy` (dropped by default).
    if is_freeform_tool(tool) {
        return true;
    }
    tool.name
        .as_deref()
        .is_some_and(crate::server_tools::is_known_name)
}

/// Whether a tool is Codex's freeform tool: `type: "custom"` with a `format`
/// descriptor and no input schema. Distinct from Anthropic's `custom` (a schema'd
/// client function). The Responses dialect supports it natively; chat and messages
/// do not.
fn is_freeform_tool(tool: &IrTool) -> bool {
    tool.kind == "custom" && tool.parameters.is_none() && tool.raw.get("format").is_some()
}

/// Short name for a server tool, used to match provider-native policies and map to a
/// function tool: `web_search_20260209` -> `web_search`, `WebSearch` -> `web_search`,
/// OpenAI-style `web_search_preview` -> `web_search`, `code_interpreter` ->
/// `code_execution`.
fn server_tool_short_name(tool: &IrTool) -> Option<&str> {
    if let Some(name) = tool.name.as_deref() {
        if crate::server_tools::is_known_name(name) {
            return Some(match name {
                "WebSearch" => "web_search",
                "WebFetch" => "web_fetch",
                other => other,
            });
        }
    }
    let kind = tool.kind.as_str();
    let normalized = match kind {
        "web_search" | "web_search_preview" => Some("web_search"),
        "web_fetch" | "web_fetch_preview" => Some("web_fetch"),
        "code_interpreter" => Some("code_execution"),
        _ => crate::server_tools::is_known_name(kind).then_some(kind),
    };
    if normalized.is_some() {
        return normalized;
    }
    for prefix in crate::server_tools::TYPE_PREFIXES {
        if kind.starts_with(prefix) {
            // Versioned Anthropic type (`web_search_20260209`) or a preview variant
            // (`web_search_preview`): the short name is the prefix without the
            // trailing underscore.
            return Some(prefix.trim_end_matches('_'));
        }
    }
    None
}

/// The function-tool shape for a mapped server tool (`ServerToolPolicy::MapToFunction`).
/// The mapped function name is the normalized short name (`web_search`), matching what
/// the upstream's own search function would be called.
fn server_tool_function_shape(tool: &IrTool) -> (String, String, Value) {
    let name = server_tool_short_name(tool)
        .map(str::to_owned)
        .unwrap_or_else(|| {
            tool.name
                .clone()
                .unwrap_or_else(|| tool.kind.replace('-', "_"))
        });
    match server_tool_short_name(tool) {
        Some("web_search") => (
            name,
            "Search the web for current information. Call this when the user asks about current events, recent information, or anything that may have changed since the model's knowledge cutoff.".to_owned(),
            serde_json::json!({"type":"object","properties":{"query":{"type":"string","description":"The search query"}},"required":["query"]}),
        ),
        Some("web_fetch") => (
            name,
            "Fetch the content of a specific URL. Call this when you need the full content of a page the user referenced.".to_owned(),
            serde_json::json!({"type":"object","properties":{"url":{"type":"string","description":"The URL to fetch"}},"required":["url"]}),
        ),
        _ => (
            name,
            tool.description.clone().unwrap_or_default(),
            tool.parameters
                .clone()
                .unwrap_or_else(|| serde_json::json!({"type":"object","properties":{}})),
        ),
    }
}

fn normalize_responses_parameters(out: &mut Map<String, Value>, caps: &ModelCapabilities) {
    if let Some(value) = out.get_mut("max_output_tokens") {
        if let Some(cap) = caps.max_tokens_cap {
            if let Some(number) = value.as_u64() {
                *value = Value::Number(number.min(cap).into());
            }
        }
    }
    if let Some(value) = out.remove("reasoning_effort") {
        out.insert("reasoning".to_owned(), serde_json::json!({"effort": value}));
    }
    if let Some(value) = out.remove("output_config") {
        if let Some(effort) = value.get("effort") {
            out.insert(
                "reasoning".to_owned(),
                serde_json::json!({"effort": effort}),
            );
        }
        if let Some(format) = value.get("format") {
            // Messages structured-output request maps to Responses text.format.
            let text = out
                .entry("text".to_owned())
                .or_insert_with(|| Value::Object(Map::new()));
            if let Some(text) = text.as_object_mut() {
                text.insert("format".to_owned(), format.clone());
            }
        }
    }
    // Messages stop_sequences -> Responses `stop`.
    if let Some(sequences) = out
        .remove("stop_sequences")
        .and_then(|value| value.as_array().cloned())
    {
        let sequences: Vec<Value> = sequences
            .iter()
            .filter(|value| value.as_str().is_some_and(|text| !text.is_empty()))
            .cloned()
            .collect();
        if !sequences.is_empty() {
            out.insert(
                "stop".to_owned(),
                if sequences.len() == 1 {
                    sequences[0].clone()
                } else {
                    Value::Array(sequences)
                },
            );
        }
    }
}

/// Map a chat/responses tool_choice into the Messages dialect
/// (`{"type": "auto"|"any"|"none"|"tool", "name": ...}`).
fn render_messages_tool_choice(choice: &Value) -> Value {
    match choice {
        Value::String(value) => match value.as_str() {
            "required" => serde_json::json!({"type": "any"}),
            other => serde_json::json!({"type": other}),
        },
        Value::Object(object) => match object.get("type").and_then(Value::as_str) {
            Some("auto") | Some("any") | Some("none") | Some("tool") => choice.clone(),
            Some("function") => object
                .get("function")
                .and_then(Value::as_object)
                .and_then(|function| function.get("name"))
                .and_then(Value::as_str)
                .or_else(|| object.get("name").and_then(Value::as_str))
                .map(|name| serde_json::json!({"type":"tool", "name": name}))
                .unwrap_or_else(|| serde_json::json!({"type":"auto"})),
            _ => serde_json::json!({"type":"auto"}),
        },
        _ => serde_json::json!({"type":"auto"}),
    }
}

/// Map a messages/chat tool_choice into the Responses dialect
/// (`"auto"|"required"|"none"` or `{"type": "function", "name": ...}`).
fn render_responses_tool_choice(choice: &Value) -> Value {
    match choice {
        Value::String(value) => Value::String(value.clone()),
        Value::Object(object) => match object.get("type").and_then(Value::as_str) {
            Some("function") => object
                .get("name")
                .and_then(Value::as_str)
                .or_else(|| {
                    object
                        .get("function")
                        .and_then(Value::as_object)
                        .and_then(|function| function.get("name"))
                        .and_then(Value::as_str)
                })
                .map(|name| serde_json::json!({"type":"function", "name": name}))
                .unwrap_or_else(|| serde_json::json!("auto")),
            Some("tool") => object
                .get("name")
                .and_then(Value::as_str)
                .map(|name| serde_json::json!({"type":"function", "name": name}))
                .unwrap_or_else(|| serde_json::json!("auto")),
            Some("auto") | Some("any") | Some("none") | Some("required") => {
                match object.get("type").and_then(Value::as_str) {
                    Some("any") => serde_json::json!("required"),
                    Some(other) => serde_json::json!(other),
                    _ => serde_json::json!("auto"),
                }
            }
            _ => serde_json::json!("auto"),
        },
        _ => serde_json::json!("auto"),
    }
}

impl IrContent {
    fn parse(value: &Value) -> Self {
        let Some(object) = value.as_object() else {
            // A plain string is a text block (shorthand form); keep it as text so
            // content_as_string and friends treat it uniformly.
            if let Some(text) = value.as_str() {
                return Self::Text {
                    text: text.to_owned(),
                    extensions: ContentExtensions::default(),
                };
            }
            return Self::Raw(value.clone());
        };
        match object.get("type").and_then(Value::as_str) {
            Some("text" | "input_text" | "output_text") => Self::Text {
                text: object
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                extensions: ContentExtensions::parse(object, &["type", "text", "cache_control"]),
            },
            Some("thinking" | "reasoning_text") => Self::Thinking {
                text: object
                    .get("thinking")
                    .or_else(|| object.get("text"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                signature: object
                    .get("signature")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                extensions: ContentExtensions::parse(
                    object,
                    &["type", "thinking", "text", "signature", "cache_control"],
                ),
            },
            Some("image" | "input_image" | "image_url") => Self::Image(value.clone()),
            Some("tool_use") => Self::ToolUse(IrToolCall::from_messages_block(value)),
            Some("tool_result") => Self::ToolResult {
                call_id: object
                    .get("tool_use_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                content: object.get("content").cloned().unwrap_or(Value::Null),
                is_error: object
                    .get("is_error")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                extensions: ContentExtensions::parse(
                    object,
                    &[
                        "type",
                        "tool_use_id",
                        "content",
                        "is_error",
                        "cache_control",
                    ],
                ),
            },
            _ => Self::Raw(value.clone()),
        }
    }

    fn raw_value(&self) -> Value {
        match self {
            Self::Text { text, extensions } => {
                let mut object = extensions.render(true);
                object.insert("type".to_owned(), Value::String("text".to_owned()));
                object.insert("text".to_owned(), Value::String(text.clone()));
                Value::Object(object)
            }
            Self::Thinking {
                text,
                signature,
                extensions,
            } => {
                let mut object = extensions.render(true);
                object.insert("type".to_owned(), Value::String("thinking".to_owned()));
                object.insert("thinking".to_owned(), Value::String(text.clone()));
                if let Some(signature) = signature {
                    object.insert("signature".to_owned(), Value::String(signature.clone()));
                }
                Value::Object(object)
            }
            Self::Image(value) | Self::Raw(value) => value.clone(),
            Self::ToolUse(call) => call.to_messages_block(),
            Self::ToolResult {
                call_id,
                content,
                is_error,
                extensions,
            } => {
                let mut object = extensions.render(true);
                object.insert("type".to_owned(), Value::String("tool_result".to_owned()));
                object.insert("tool_use_id".to_owned(), Value::String(call_id.clone()));
                object.insert("content".to_owned(), content.clone());
                object.insert("is_error".to_owned(), Value::Bool(*is_error));
                Value::Object(object)
            }
        }
    }

    fn to_chat(&self) -> Value {
        match self {
            Self::Text { text, extensions } => {
                let mut object = extensions.render(true);
                object.insert("type".to_owned(), Value::String("text".to_owned()));
                object.insert("text".to_owned(), Value::String(text.clone()));
                Value::Object(object)
            }
            Self::Thinking { text, .. } => serde_json::json!({"type":"text", "text":text}),
            Self::Image(value) => image_to_chat(value),
            other => other.raw_value(),
        }
    }

    fn to_messages(&self) -> Value {
        self.raw_value()
    }

    fn to_responses(&self, role: &str) -> Value {
        match self {
            Self::Text { text, extensions } => {
                let mut object = extensions.render(true);
                object.insert(
                    "type".to_owned(),
                    Value::String(
                        if role == "assistant" {
                            "output_text"
                        } else {
                            "input_text"
                        }
                        .to_owned(),
                    ),
                );
                object.insert("text".to_owned(), Value::String(text.clone()));
                Value::Object(object)
            }
            Self::Image(value) => {
                let mut object = value.as_object().cloned().unwrap_or_default();
                object.insert("type".to_owned(), Value::String("input_image".to_owned()));
                Value::Object(object)
            }
            other => other.raw_value(),
        }
    }
}

/// Deep-copy the content, stripping block-level `cache_control` markers when the
/// upstream does not support them (conservative: never leak a control an upstream
/// would reject or silently ignore).
fn prepared_content(content: &[IrContent], keep_cache_control: bool) -> Vec<IrContent> {
    if keep_cache_control {
        return content.to_vec();
    }
    content
        .iter()
        .map(|part| match part {
            IrContent::Text { text, extensions } => {
                let mut extensions = extensions.clone();
                extensions.cache_control = None;
                IrContent::Text {
                    text: text.clone(),
                    extensions,
                }
            }
            IrContent::Thinking {
                text,
                signature,
                extensions,
            } => {
                let mut extensions = extensions.clone();
                extensions.cache_control = None;
                IrContent::Thinking {
                    text: text.clone(),
                    signature: signature.clone(),
                    extensions,
                }
            }
            IrContent::Image(value) => {
                let mut object = value.as_object().cloned().unwrap_or_default();
                object.remove("cache_control");
                IrContent::Image(Value::Object(object))
            }
            other => other.clone(),
        })
        .collect()
}

impl IrMessage {
    fn custom(value: Value) -> Self {
        Self {
            role: "custom".to_owned(),
            content: vec![IrContent::Raw(value)],
            item_id: None,
            tool_call_id: None,
            tool_calls: Vec::new(),
            opaque_extensions: Map::new(),
        }
    }

    fn assistant_tool_call(call: IrToolCall) -> Self {
        Self {
            role: "assistant".to_owned(),
            content: Vec::new(),
            item_id: None,
            tool_call_id: None,
            tool_calls: vec![call],
            opaque_extensions: Map::new(),
        }
    }

    fn to_chat_values(&self, keep_cache_control: bool) -> Vec<Value> {
        let content = prepared_content(&self.content, keep_cache_control);
        if self.role == "custom" {
            return content.iter().map(IrContent::raw_value).collect();
        }
        let mut object = self.opaque_extensions.clone();
        object.insert("role".to_owned(), Value::String(self.role.clone()));
        if self.role == "tool" {
            object.insert(
                "tool_call_id".to_owned(),
                Value::String(self.tool_call_id.clone().unwrap_or_default()),
            );
            object.insert(
                "content".to_owned(),
                Value::String(content_as_string(&content)),
            );
            return vec![Value::Object(object)];
        }
        if self.role == "user"
            && content
                .iter()
                .any(|content| matches!(content, IrContent::ToolResult { .. }))
        {
            let mut values = Vec::new();
            for content in &content {
                if let IrContent::ToolResult {
                    call_id, content, ..
                } = content
                {
                    let text = content_as_string(&[IrContent::parse(content)]);
                    values.push(
                        serde_json::json!({"role":"tool", "tool_call_id":call_id, "content":text}),
                    );
                }
            }
            let visible = content
                .iter()
                .filter(|content| !matches!(content, IrContent::ToolResult { .. }))
                .cloned()
                .collect::<Vec<_>>();
            if !visible.is_empty() {
                let mut user = object;
                user.insert("content".to_owned(), content_as_chat(&visible));
                values.push(Value::Object(user));
            }
            return values;
        }
        let mut calls = self.tool_calls.clone();
        calls.extend(content.iter().filter_map(|content| match content {
            IrContent::ToolUse(call) => Some(call.clone()),
            _ => None,
        }));
        let visible_content = content
            .iter()
            .filter(|content| {
                !matches!(content, IrContent::ToolUse(_) | IrContent::Thinking { .. })
            })
            .cloned()
            .collect::<Vec<_>>();
        object.insert("content".to_owned(), content_as_chat(&visible_content));
        if let Some(reasoning) = content.iter().find_map(|content| match content {
            IrContent::Thinking { text, .. } => Some(text.clone()),
            _ => None,
        }) {
            object.insert("reasoning_content".to_owned(), Value::String(reasoning));
        }
        if !calls.is_empty() {
            object.insert(
                "tool_calls".to_owned(),
                Value::Array(calls.iter().map(IrToolCall::to_chat).collect()),
            );
        }
        vec![Value::Object(object)]
    }

    fn to_messages_values(&self, keep_cache_control: bool) -> Vec<Value> {
        let content = prepared_content(&self.content, keep_cache_control);
        if self.role == "custom" {
            return Vec::new();
        }
        if self.role == "tool" {
            return vec![serde_json::json!({
                "role":"user",
                "content":[{"type":"tool_result", "tool_use_id":self.tool_call_id.clone().unwrap_or_default(), "content":content_as_chat(&content)}]
            })];
        }
        let mut blocks = content
            .iter()
            .map(IrContent::to_messages)
            .collect::<Vec<_>>();
        if self.role == "assistant" {
            blocks.extend(self.tool_calls.iter().map(IrToolCall::to_messages_block));
        }
        let mut object = self.opaque_extensions.clone();
        object.insert("role".to_owned(), Value::String(self.role.clone()));
        object.insert("content".to_owned(), Value::Array(blocks));
        vec![Value::Object(object)]
    }

    fn to_responses_values(&self, keep_cache_control: bool) -> Vec<Value> {
        let prepared = prepared_content(&self.content, keep_cache_control);
        if self.role == "custom" {
            return prepared.iter().map(IrContent::raw_value).collect();
        }
        let mut values = Vec::new();
        if self.role == "tool" {
            values.push(serde_json::json!({
                "type":"function_call_output",
                "call_id":self.tool_call_id.clone().unwrap_or_default(),
                "output":Value::String(content_as_string(&prepared))
            }));
            return values;
        }
        let mut content = Vec::new();
        for part in &prepared {
            match part {
                IrContent::ToolResult {
                    call_id, content, ..
                } => values.push(serde_json::json!({
                    "type":"function_call_output",
                    "call_id":call_id,
                    "output":content
                })),
                IrContent::ToolUse(call) => values.push(call.to_responses()),
                // Anthropic thinking blocks have no Responses content type; they become
                // standalone reasoning items preceding the assistant message.
                IrContent::Thinking { text, .. } if !text.trim().is_empty() => {
                    values.push(serde_json::json!({
                        "type":"reasoning",
                        "summary":[{"type":"summary_text","text":text}]
                    }));
                }
                _ => content.push(part.to_responses(&self.role)),
            }
        }
        if !content.is_empty() {
            values.push(serde_json::json!({"type":"message", "role":self.role, "content":content}));
        }
        values.extend(self.tool_calls.iter().map(IrToolCall::to_responses));
        values
    }
}

/// Join the collected system instructions into one string. Instruction entries may be
/// strings or Anthropic system block arrays; the Messages/Responses dialects take a
/// single system string, and some Anthropic-compatible upstreams (GLM) reject arrays.
fn instructions_text(instructions: &[Value]) -> String {
    instructions
        .iter()
        .filter_map(|instruction| match instruction {
            Value::String(text) => Some(text.clone()),
            Value::Array(blocks) => {
                let text = blocks
                    .iter()
                    .filter_map(|block| block.get("text").and_then(Value::as_str))
                    .collect::<Vec<_>>()
                    .join("\n\n");
                (!text.is_empty()).then_some(text)
            }
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// Render message content for the Chat dialect. Always uses the extended (array)
/// form — even a single text block — so converted requests are byte-stable across
/// turns and clients that pass the shorthand form ("content": "hi") produce the
/// same prefix as those that pass blocks. Prefix stability matters: every provider
/// cache is an exact-prefix match. (An empty content stays the empty string, which
/// the Chat dialect requires for assistant messages without text.)
fn content_as_chat(content: &[IrContent]) -> Value {
    if content.is_empty() {
        return Value::String(String::new());
    }
    let mut values = content
        .iter()
        .filter(|part| !matches!(part, IrContent::Image(_)))
        .map(IrContent::to_chat)
        .collect::<Vec<_>>();
    values.extend(
        content
            .iter()
            .filter(|part| matches!(part, IrContent::Image(_)))
            .map(IrContent::to_chat),
    );
    Value::Array(values)
}

/// Render content as a plain string for protocol slots that require one: Chat
/// `role: "tool"` content and Responses `function_call_output.output`. Text blocks
/// are joined; other block types (images) are omitted.
fn content_as_string(content: &[IrContent]) -> String {
    content
        .iter()
        .filter_map(|part| match part {
            IrContent::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn image_to_chat(value: &Value) -> Value {
    let Some(object) = value.as_object() else {
        return value.clone();
    };
    if object.get("type").and_then(Value::as_str) == Some("image_url") {
        return value.clone();
    }
    if let Some(source) = object.get("source").and_then(Value::as_object) {
        if source.get("type").and_then(Value::as_str) == Some("base64") {
            let media_type = source
                .get("media_type")
                .and_then(Value::as_str)
                .unwrap_or("application/octet-stream");
            let data = source
                .get("data")
                .and_then(Value::as_str)
                .unwrap_or_default();
            return serde_json::json!({"type":"image_url", "image_url":{"url":format!("data:{media_type};base64,{data}")}});
        }
        if let Some(url) = source.get("url").and_then(Value::as_str) {
            return serde_json::json!({"type":"image_url", "image_url":{"url":url}});
        }
    }
    value.clone()
}

fn reasoning_to_messages_blocks(reasoning: &Value) -> Vec<Value> {
    reasoning
        .get("summary")
        .and_then(Value::as_array)
        .map(|summary| {
            summary
                .iter()
                .filter_map(|item| {
                    let text = item.get("text").and_then(Value::as_str)?;
                    Some(serde_json::json!({"type":"thinking", "thinking":text}))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn reasoning_text(reasoning: &Value) -> Option<String> {
    let text = reasoning
        .get("summary")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| item.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n");
    (!text.is_empty()).then_some(text)
}

impl IrToolCall {
    fn from_chat(value: &Value) -> Self {
        let function = value.get("function").and_then(Value::as_object);
        Self {
            id: value.get("id").and_then(Value::as_str).map(str::to_owned),
            call_id: value.get("id").and_then(Value::as_str).map(str::to_owned),
            name: function
                .and_then(|f| f.get("name"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            arguments: function
                .and_then(|f| f.get("arguments"))
                .cloned()
                .unwrap_or_else(|| Value::String("{}".to_owned())),
        }
    }

    fn from_messages_block(value: &Value) -> Self {
        Self {
            id: value.get("id").and_then(Value::as_str).map(str::to_owned),
            call_id: value.get("id").and_then(Value::as_str).map(str::to_owned),
            name: value
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            arguments: value
                .get("input")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({})),
        }
    }

    fn from_responses_call(value: &Value) -> Self {
        Self {
            id: value.get("id").and_then(Value::as_str).map(str::to_owned),
            call_id: value
                .get("call_id")
                .and_then(Value::as_str)
                .map(str::to_owned),
            name: value
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            arguments: value
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| Value::String("{}".to_owned())),
        }
    }

    fn to_chat(&self) -> Value {
        let arguments = match &self.arguments {
            Value::String(value) => value.clone(),
            value => serde_json::to_string(value).unwrap_or_else(|_| "{}".to_owned()),
        };
        serde_json::json!({"id":self.id.clone().or_else(|| self.call_id.clone()).unwrap_or_default(), "type":"function", "function":{"name":self.name, "arguments":arguments}})
    }

    fn to_messages_block(&self) -> Value {
        // Anthropic tool_use.input must be an object; Chat/Responses carry arguments
        // as a JSON string, so parse it.
        let input = match &self.arguments {
            Value::String(text) => {
                serde_json::from_str(text).unwrap_or_else(|_| serde_json::json!({}))
            }
            value => value.clone(),
        };
        serde_json::json!({"type":"tool_use", "id":self.id.clone().or_else(|| self.call_id.clone()).unwrap_or_default(), "name":self.name, "input":input})
    }

    fn to_responses(&self) -> Value {
        // Responses function_call.arguments must be a JSON string; Messages input is an
        // object, so serialize it.
        let arguments = match &self.arguments {
            Value::String(value) => value.clone(),
            value => serde_json::to_string(value).unwrap_or_else(|_| "{}".to_owned()),
        };
        serde_json::json!({"type":"function_call", "id":self.id, "call_id":self.call_id.clone().or(self.id.clone()), "name":self.name, "arguments":arguments})
    }
}

impl IrTool {
    fn from_messages(value: &Value) -> Self {
        Self {
            kind: value
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("function")
                .to_owned(),
            name: value.get("name").and_then(Value::as_str).map(str::to_owned),
            description: value
                .get("description")
                .and_then(Value::as_str)
                .map(str::to_owned),
            parameters: value.get("input_schema").cloned(),
            raw: value.clone(),
        }
    }

    fn from_chat(value: &Value) -> Self {
        let function = value.get("function").and_then(Value::as_object);
        Self {
            kind: value
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("function")
                .to_owned(),
            name: function
                .and_then(|f| f.get("name"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            description: function
                .and_then(|f| f.get("description"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            parameters: function.and_then(|f| f.get("parameters")).cloned(),
            raw: value.clone(),
        }
    }

    fn from_responses(value: &Value) -> Self {
        Self {
            kind: value
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("function")
                .to_owned(),
            name: value.get("name").and_then(Value::as_str).map(str::to_owned),
            description: value
                .get("description")
                .and_then(Value::as_str)
                .map(str::to_owned),
            parameters: value.get("parameters").cloned(),
            raw: value.clone(),
        }
    }

    /// Whether the tool is a plain function or a schema'd `custom` (Anthropic client
    /// function) that can be rendered as a function in the chat/responses dialects.
    /// Schema-less `custom` (Codex's freeform) is handled by the server-tool policy.
    fn has_schema_function_kind(&self) -> bool {
        self.kind == "function" || (self.kind == "custom" && self.parameters.is_some())
    }

    fn to_chat(&self) -> Value {
        if !self.has_schema_function_kind() {
            return self.raw.clone();
        }
        let mut function = self
            .raw
            .get("function")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_else(|| self.raw.as_object().cloned().unwrap_or_default());
        // A schema'd Anthropic `custom` tool flattens into the function body; the
        // custom type tag and input_schema have no chat equivalents.
        function.remove("type");
        function.remove("input_schema");
        function.insert(
            "name".to_owned(),
            Value::String(self.name.clone().unwrap_or_default()),
        );
        function.insert(
            "description".to_owned(),
            Value::String(self.description.clone().unwrap_or_default()),
        );
        function.insert(
            "parameters".to_owned(),
            crate::conversion_utils::normalize_input_schema(
                &self
                    .parameters
                    .clone()
                    .unwrap_or_else(|| serde_json::json!({"type":"object"})),
            ),
        );
        serde_json::json!({"type":"function", "function":function})
    }

    fn to_messages(&self) -> Value {
        // The Messages dialect natively supports `custom` tools (schema'd client
        // functions); keep them as-is. Everything else renders as a plain tool.
        if self.kind == "custom" {
            return self.raw.clone();
        }
        if self.kind != "function" {
            return self.raw.clone();
        }
        let mut object = self.raw.as_object().cloned().unwrap_or_default();
        object.remove("type");
        object.remove("function");
        // Chat/Responses tool definitions carry `parameters`/`strict`/nested `tools`;
        // the Messages dialect accepts only `input_schema`.
        object.remove("parameters");
        object.remove("strict");
        object.remove("tools");
        object.insert(
            "name".to_owned(),
            Value::String(self.name.clone().unwrap_or_default()),
        );
        object.insert(
            "description".to_owned(),
            Value::String(self.description.clone().unwrap_or_default()),
        );
        object.insert(
            "input_schema".to_owned(),
            self.parameters
                .clone()
                .unwrap_or_else(|| serde_json::json!({"type":"object"})),
        );
        Value::Object(object)
    }

    fn to_responses(&self) -> Value {
        if !self.has_schema_function_kind() {
            return self.raw.clone();
        }
        let mut object = self.raw.as_object().cloned().unwrap_or_default();
        object.remove("input_schema");
        object.insert("type".to_owned(), Value::String("function".to_owned()));
        object.insert(
            "name".to_owned(),
            Value::String(self.name.clone().unwrap_or_default()),
        );
        object.insert(
            "description".to_owned(),
            Value::String(self.description.clone().unwrap_or_default()),
        );
        object.insert(
            "parameters".to_owned(),
            self.parameters
                .clone()
                .unwrap_or_else(|| serde_json::json!({"type":"object"})),
        );
        Value::Object(object)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn messages_to_responses_keeps_unknown_parameters_and_tool_metadata() {
        let request = json!({
            "model": "m",
            "max_tokens": 32,
            "system": "be precise",
            "messages": [{"role":"user", "content":[{"type":"text","text":"hello","cache_control":{"type":"ephemeral"}}]}],
            "tools": [{"name":"lookup","input_schema":{"type":"object"},"vendor_extension":{"x":true}}],
            "provider_budget": {"reasoning": 7}
        });
        let ir = RequestIr::parse(Protocol::AnthropicMessages, &request).unwrap();
        // Parse keeps the block cache_control in the IR.
        let IrConversationItem::Message(message) = &ir.transcript[0] else {
            panic!("expected message");
        };
        let IrContent::Text { extensions, .. } = &message.content[0] else {
            panic!("expected text block");
        };
        assert_eq!(
            extensions.cache_control.as_ref().unwrap()["type"],
            "ephemeral"
        );
        let responses = ir
            .render(Protocol::OpenAiResponses, &ModelCapabilities::default())
            .unwrap();
        assert_eq!(responses["provider_budget"]["reasoning"], 7);
        assert_eq!(responses["tools"][0]["vendor_extension"]["x"], true);
        assert_eq!(responses["input"][0]["content"][0]["type"], "input_text");
        // Responses has no block-level cache_control; it is stripped at render
        // (no provider supports it on the responses endpoint).
        assert!(responses["input"][0]["content"][0]
            .get("cache_control")
            .is_none());
    }

    #[test]
    fn responses_to_messages_preserves_continuation_and_reasoning() {
        let request = json!({
            "model": "m",
            "instructions": "be concise",
            "input": [
                {"type":"reasoning","summary":[{"type":"summary_text","text":"think"}]},
                {"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]},
                {"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{}"},
                {"type":"function_call_output","call_id":"call_1","output":"done"}
            ],
            "provider_budget": 9
        });
        let previous = RequestIr::parse(
            Protocol::OpenAiChatCompletions,
            &json!({"messages":[{"role":"user","content":"earlier"}]}),
        )
        .unwrap();
        let ir = RequestIr::parse_with_context(
            Protocol::OpenAiResponses,
            &request,
            Some(&previous),
            None,
        )
        .unwrap();
        let messages = ir
            .render(Protocol::AnthropicMessages, &ModelCapabilities::default())
            .unwrap();
        assert_eq!(messages["provider_budget"], 9);
        assert!(messages["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| {
                message["content"] == "earlier"
                    || message["content"]
                        .as_array()
                        .is_some_and(|blocks| blocks.iter().any(|block| block["text"] == "earlier"))
            }));
        assert!(messages["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| {
                message["content"]
                    .as_array()
                    .is_some_and(|blocks| blocks.iter().any(|block| block["type"] == "thinking"))
            }));
        assert!(messages["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| {
                message["content"]
                    .as_array()
                    .is_some_and(|blocks| blocks.iter().any(|block| block["type"] == "text"))
            }));
    }

    #[test]
    fn maps_user_identity_and_preserves_cache_affinity() {
        let request = json!({
            "model": "m",
            "metadata": {"user_id": "user-42", "client": "codex"},
            "cache_control": {"type": "ephemeral"},
            "session_id": "session-7",
            "messages": [{"role":"user", "content":"hello"}]
        });
        let ir = RequestIr::parse(Protocol::AnthropicMessages, &request).unwrap();
        let mut caps = ModelCapabilities::default();
        caps.cache.prompt_cache_key = true;
        let responses = ir.render(Protocol::OpenAiResponses, &caps).unwrap();

        assert_eq!(responses["user"], "user-42");
        assert_eq!(responses["metadata"]["client"], "codex");
        assert_eq!(responses["session_id"], "session-7");
        // Top-level cache_control has no Responses equivalent; stripped at render.
        assert!(responses.get("cache_control").is_none());
        assert_eq!(responses["prompt_cache_key"], "user-42");
    }

    #[test]
    fn separates_represented_context_extensions_from_opaque_extensions() {
        let ir = RequestIr::parse(
            Protocol::AnthropicMessages,
            &json!({
                "model": "m",
                "session_id": "session-7",
                "provider_budget": {"tokens": 123},
                "messages": [{"role":"user","content":"hello"}]
            }),
        )
        .unwrap();
        assert_eq!(ir.context.represented_extensions["session_id"], "session-7");
        assert_eq!(ir.opaque_extensions["provider_budget"]["tokens"], 123);
    }

    #[test]
    fn continuation_inherits_conversation_and_prompt_cache_context() {
        let previous = RequestIr::parse(
            Protocol::OpenAiResponses,
            &json!({
                "model": "m",
                "input": "first",
                "conversation": {"id": "conv-1"},
                "user": "user-1",
                "safety_identifier": "safe-1",
                "prompt_cache_key": "cache-1",
                "prompt_cache_retention": "24h",
                "metadata": {"trace": "root"},
                "session_id": "vendor-session-1"
            }),
        )
        .unwrap();
        let continued = RequestIr::parse_with_context(
            Protocol::OpenAiResponses,
            &json!({
                "model": "m",
                "previous_response_id": "resp-1",
                "input": "second"
            }),
            Some(&previous),
            None,
        )
        .unwrap();
        let messages = continued
            .render(Protocol::AnthropicMessages, &ModelCapabilities::default())
            .unwrap();

        assert_eq!(messages["metadata"]["user_id"], "cache-1");
        assert_eq!(messages["metadata"]["trace"], "root");
        assert_eq!(messages["conversation"]["id"], "conv-1");
        assert_eq!(messages["safety_identifier"], "safe-1");
        assert!(messages.get("prompt_cache_key").is_none());
        assert!(messages.get("prompt_cache_retention").is_none());
        assert_eq!(messages["session_id"], "vendor-session-1");
        assert!(messages.get("previous_response_id").is_none());
    }

    #[test]
    fn responses_cache_key_maps_to_messages_user_id() {
        let ir = RequestIr::parse(
            Protocol::OpenAiResponses,
            &json!({
                "model": "m",
                "prompt_cache_key": "cache-as-identity",
                "input": "hello"
            }),
        )
        .unwrap();
        let messages = ir
            .render(Protocol::AnthropicMessages, &ModelCapabilities::default())
            .unwrap();
        assert_eq!(messages["metadata"]["user_id"], "cache-as-identity");
        assert!(messages.get("prompt_cache_key").is_none());
    }

    #[test]
    fn responses_cache_key_takes_precedence_when_target_has_one_identity_slot() {
        let ir = RequestIr::parse(
            Protocol::OpenAiResponses,
            &json!({
                "model": "m",
                "user": "end-user",
                "prompt_cache_key": "cache-affinity",
                "input": "hello"
            }),
        )
        .unwrap();
        let messages = ir
            .render(Protocol::AnthropicMessages, &ModelCapabilities::default())
            .unwrap();
        assert_eq!(messages["metadata"]["user_id"], "cache-affinity");
    }

    #[test]
    fn empty_responses_reasoning_summary_is_replaced_from_state() {
        let ir = RequestIr::parse_with_context(
            Protocol::OpenAiResponses,
            &json!({
                "model": "m",
                "input": [
                    {"type":"reasoning","summary":[]},
                    {"type":"message","role":"user","content":[{"type":"input_text","text":"continue"}]}
                ]
            }),
            None,
            Some("remembered reasoning"),
        )
        .unwrap();
        let chat = ir
            .render(
                Protocol::OpenAiChatCompletions,
                &ModelCapabilities::default(),
            )
            .unwrap();
        assert_eq!(
            chat["messages"][1]["reasoning_content"],
            "remembered reasoning"
        );
    }

    #[test]
    fn responses_item_order_survives_neutral_ir_roundtrip() {
        let ir = RequestIr::parse(
            Protocol::OpenAiResponses,
            &json!({
                "model": "m",
                "input": [
                    {"type":"message","role":"user","content":[{"type":"input_text","text":"question"}]},
                    {"type":"reasoning","summary":[{"type":"summary_text","text":"think"}]},
                    {"type":"function_call","call_id":"call-1","name":"lookup","arguments":"{}"},
                    {"type":"function_call_output","call_id":"call-1","output":"answer"}
                ]
            }),
        )
        .unwrap();
        let responses = ir
            .render(Protocol::OpenAiResponses, &ModelCapabilities::default())
            .unwrap();
        let types = responses["input"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["type"].as_str().unwrap_or("message"))
            .collect::<Vec<_>>();
        assert_eq!(
            types,
            vec![
                "message",
                "reasoning",
                "function_call",
                "function_call_output"
            ]
        );
    }

    #[test]
    fn messages_cache_identity_maps_to_responses_prompt_cache_key() {
        let ir = RequestIr::parse(
            Protocol::AnthropicMessages,
            &json!({
                "model": "m",
                "metadata": {"user_id": "anthropic-cache-user"},
                "messages": [{"role":"user", "content":"hello"}]
            }),
        )
        .unwrap();
        let mut caps = ModelCapabilities::default();
        caps.cache.prompt_cache_key = true;
        let responses = ir.render(Protocol::OpenAiResponses, &caps).unwrap();
        assert_eq!(responses["prompt_cache_key"], "anthropic-cache-user");
    }

    #[test]
    fn responses_reasoning_effort_maps_to_thinking_and_effort() {
        let caps = ModelCapabilities {
            thinking_param: ThinkingParam::TopLevel,
            reasoning_effort: Some(crate::dialect::ReasoningEffortConfig {
                enabled: true,
                levels: vec!["low".into(), "high".into(), "max".into()],
                default: None,
            }),
            ..Default::default()
        };
        let request = json!({
            "model": "m",
            "max_output_tokens": 1,
            "input": "hi",
            "reasoning": {"effort": "xhigh"},
            "text": {"format": {"type": "json_object"}}
        });
        let ir = RequestIr::parse(Protocol::OpenAiResponses, &request).unwrap();
        let chat = ir.render(Protocol::OpenAiChatCompletions, &caps).unwrap();
        assert_eq!(chat["thinking"]["type"], "enabled");
        assert_eq!(chat["reasoning_effort"], "high");
        assert_eq!(chat["response_format"]["type"], "json_object");
        // Responses-owned parameters are consumed, not leaked.
        assert!(chat.get("reasoning").is_none());
        assert!(chat.get("text").is_none());
    }

    #[test]
    fn responses_reasoning_effort_minimal_disables_thinking() {
        let caps = ModelCapabilities {
            thinking_param: ThinkingParam::Adaptive,
            ..Default::default()
        };
        let request = json!({"model": "m", "input": "hi", "reasoning": {"effort": "minimal"}});
        let ir = RequestIr::parse(Protocol::OpenAiResponses, &request).unwrap();
        let chat = ir.render(Protocol::OpenAiChatCompletions, &caps).unwrap();
        assert_eq!(chat["thinking"]["type"], "disabled");
    }

    #[test]
    fn responses_tool_choice_function_maps_to_chat_shape() {
        let request = json!({
            "model": "m",
            "input": "hi",
            "tool_choice": {"type": "function", "name": "f"}
        });
        let ir = RequestIr::parse(Protocol::OpenAiResponses, &request).unwrap();
        let chat = ir
            .render(
                Protocol::OpenAiChatCompletions,
                &ModelCapabilities::default(),
            )
            .unwrap();
        assert_eq!(chat["tool_choice"]["type"], "function");
        assert_eq!(chat["tool_choice"]["function"]["name"], "f");

        let request = json!({
            "model": "m",
            "input": "hi",
            "tool_choice": {"type": "allowed_tools", "allowed_tools": ["web_search"]}
        });
        let ir = RequestIr::parse(Protocol::OpenAiResponses, &request).unwrap();
        let chat = ir
            .render(
                Protocol::OpenAiChatCompletions,
                &ModelCapabilities::default(),
            )
            .unwrap();
        assert_eq!(chat["tool_choice"], "auto");
    }

    #[test]
    fn responses_server_tools_map_to_native_params_with_query_substitution() {
        let caps = ModelCapabilities {
            server_tools: ServerToolPolicy::ProviderNative,
            always_enable_tools: vec!["web_search".to_string(), "code_execution".to_string()],
            search_request_params: Some(json!({
                "web_search": {"enable": true, "search_query": "{search_query}"}
            })),
            code_interpreter_request_params: Some(json!({"enable_code_interpreter": true})),
            ..Default::default()
        };
        let request = json!({
            "model": "m",
            "input": "what's the weather in Paris?",
            "stream": false,
            "tools": [{"type": "web_search_preview"}]
        });
        let ir = RequestIr::parse(Protocol::OpenAiResponses, &request).unwrap();
        let (chat, report) = ir
            .render_with_report(Protocol::OpenAiChatCompletions, &caps)
            .unwrap();
        assert_eq!(chat["web_search"]["enable"], true);
        assert!(chat["web_search"]["search_query"]
            .as_str()
            .unwrap()
            .contains("Paris"));
        assert_eq!(chat["enable_code_interpreter"], true);
        assert_eq!(chat["stream"], true);
        assert_eq!(report.mapped_server_tools.len(), 2);
        assert!(report.dropped_server_tools.is_empty());
    }

    #[test]
    fn broad_server_tool_kinds_are_dropped_or_mapped() {
        // file_search / bash / camelCase custom tools must be treated as server tools.
        let caps = ModelCapabilities {
            server_tools: ServerToolPolicy::MapToFunction,
            ..Default::default()
        };
        let request = json!({
            "model": "m",
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [
                {"type": "file_search", "name": "file_search"},
                {"type": "custom", "name": "WebSearch"},
                {"type": "bash_20250124", "name": "bash"},
                {"name": "Read", "input_schema": {"type": "object"}}
            ]
        });
        let ir = RequestIr::parse(Protocol::AnthropicMessages, &request).unwrap();
        let (chat, report) = ir
            .render_with_report(Protocol::OpenAiChatCompletions, &caps)
            .unwrap();
        let tools = chat["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 2); // WebSearch mapped + Read kept
        assert_eq!(tools[0]["function"]["name"], "web_search");
        assert_eq!(report.mapped_server_tools, vec!["web_search"]);
        assert_eq!(report.dropped_server_tools, vec!["file_search", "bash"]);
    }

    #[test]
    fn responses_to_messages_maps_effort_to_thinking() {
        let caps = ModelCapabilities {
            thinking_param: ThinkingParam::Adaptive,
            ..Default::default()
        };
        let request = json!({
            "model": "m",
            "input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]}],
            "reasoning": {"effort": "high"}
        });
        let ir = RequestIr::parse(Protocol::OpenAiResponses, &request).unwrap();
        let messages = ir.render(Protocol::AnthropicMessages, &caps).unwrap();
        assert_eq!(messages["thinking"]["type"], "adaptive");
        assert!(messages.get("output_config").is_none());
    }

    #[test]
    fn responses_renderer_applies_server_tool_policy() {
        let caps = ModelCapabilities {
            server_tools: ServerToolPolicy::ProviderNative,
            search_tool_template: Some(json!({
                "type": "web_search",
                "web_search": {"search_context_size": "medium"}
            })),
            ..Default::default()
        };
        let request = json!({
            "model": "m",
            "input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]}],
            "tools": [{"type": "web_search_preview"}]
        });
        let ir = RequestIr::parse(Protocol::OpenAiResponses, &request).unwrap();
        let responses = ir.render(Protocol::OpenAiResponses, &caps).unwrap();
        let tools = responses["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["type"], "web_search");
        assert_eq!(tools[0]["web_search"]["search_context_size"], "medium");
    }

    #[test]
    fn messages_to_responses_maps_tool_choice() {
        let request = json!({
            "model": "m",
            "messages": [{"role": "user", "content": "hi"}],
            "tool_choice": {"type": "tool", "name": "f"}
        });
        let ir = RequestIr::parse(Protocol::AnthropicMessages, &request).unwrap();
        let responses = ir
            .render(Protocol::OpenAiResponses, &ModelCapabilities::default())
            .unwrap();
        assert_eq!(responses["tool_choice"]["type"], "function");
        assert_eq!(responses["tool_choice"]["name"], "f");
    }

    #[test]
    fn chat_to_messages_maps_stop_and_tool_choice() {
        let request = json!({
            "model": "m",
            "messages": [{"role": "user", "content": "hi"}],
            "stop": ["END"],
            "tool_choice": {"type": "function", "function": {"name": "f"}}
        });
        let ir = RequestIr::parse(Protocol::OpenAiChatCompletions, &request).unwrap();
        let messages = ir
            .render(Protocol::AnthropicMessages, &ModelCapabilities::default())
            .unwrap();
        assert_eq!(messages["stop_sequences"][0], "END");
        assert_eq!(messages["tool_choice"]["type"], "tool");
        assert_eq!(messages["tool_choice"]["name"], "f");
    }

    #[test]
    fn responses_to_messages_stop_sequences_map_to_stop() {
        let request = json!({
            "model": "m",
            "messages": [{"role": "user", "content": "hi"}],
            "stop_sequences": ["END"]
        });
        let ir = RequestIr::parse(Protocol::AnthropicMessages, &request).unwrap();
        let responses = ir
            .render(Protocol::OpenAiResponses, &ModelCapabilities::default())
            .unwrap();
        assert_eq!(responses["stop"], "END");
    }

    #[test]
    fn render_messages_drops_namespace_tools_and_strips_chat_fields() {
        // Codex declares a `multi_agent_v1` namespace tool (nested `tools`, no schema)
        // alongside plain function tools with `parameters`/`strict`. The Messages renderer
        // must apply the server-tool policy (dropping the namespace) and strip the
        // chat-only `parameters`/`strict` fields.
        let request = json!({
            "model": "m",
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [
                {"type": "function", "name": "exec_command",
                 "description": "run", "parameters": {"type": "object", "properties": {"cmd": {"type": "string"}}}, "strict": false},
                {"type": "namespace", "name": "multi_agent_v1",
                 "description": "subagents", "tools": [{"type": "function", "name": "spawn_agent", "parameters": {"type": "object"}}]}
            ]
        });
        let ir = RequestIr::parse(Protocol::OpenAiResponses, &request).unwrap();
        let messages = ir
            .render(Protocol::AnthropicMessages, &ModelCapabilities::default())
            .unwrap();
        let tools = messages["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1, "namespace tool dropped, function tool kept");
        assert_eq!(tools[0]["name"], "exec_command");
        assert!(tools[0].get("parameters").is_none(), "parameters stripped");
        assert!(tools[0].get("strict").is_none(), "strict stripped");
        assert_eq!(
            tools[0]["input_schema"]["properties"]["cmd"]["type"],
            "string"
        );
    }

    #[test]
    fn messages_render_default_max_tokens_when_client_omits_budget() {
        // Chat/Responses clients may omit the optional output budget; the Messages
        // API requires max_tokens, so the provider-configured default is rendered.
        let request = json!({
            "model": "m",
            "input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]}]
        });
        let ir = RequestIr::parse(Protocol::OpenAiResponses, &request).unwrap();
        // Without a configured default the omission is preserved.
        let messages = ir
            .render(Protocol::AnthropicMessages, &ModelCapabilities::default())
            .unwrap();
        assert!(messages.get("max_tokens").is_none());

        let caps = ModelCapabilities {
            default_max_tokens: Some(8192),
            max_tokens_cap: Some(4096),
            ..Default::default()
        };
        let messages = ir.render(Protocol::AnthropicMessages, &caps).unwrap();
        assert_eq!(messages["max_tokens"], 4096, "default clamped by cap");
    }

    #[test]
    fn responses_render_maps_anthropic_thinking_per_dialect() {
        // Native-thinking dialect (Ark, TopLevel): the object is kept with a
        // normalized type; adaptive collapses to enabled.
        let caps = ModelCapabilities {
            thinking_param: ThinkingParam::TopLevel,
            ..Default::default()
        };
        let request = json!({
            "model": "m",
            "thinking": {"type": "adaptive", "display": "omitted"},
            "messages": [{"role": "user", "content": "hi"}]
        });
        let ir = RequestIr::parse(Protocol::AnthropicMessages, &request).unwrap();
        let responses = ir.render(Protocol::OpenAiResponses, &caps).unwrap();
        assert_eq!(responses["thinking"]["type"], "enabled");

        // qwen-style dialect (EnableThinking): the thinking object has no
        // documented Responses form and is consumed; disabled without an explicit
        // effort becomes reasoning.effort=none (the documented off switch).
        let caps = ModelCapabilities {
            thinking_param: ThinkingParam::EnableThinking,
            ..Default::default()
        };
        let request = json!({
            "model": "m",
            "thinking": {"type": "disabled"},
            "messages": [{"role": "user", "content": "hi"}]
        });
        let ir = RequestIr::parse(Protocol::AnthropicMessages, &request).unwrap();
        let responses = ir.render(Protocol::OpenAiResponses, &caps).unwrap();
        assert!(
            responses.get("thinking").is_none(),
            "thinking object consumed"
        );
        assert_eq!(responses["reasoning"]["effort"], "none");

        // An explicit effort is respected over the disabled thinking flag.
        let request = json!({
            "model": "m",
            "thinking": {"type": "disabled"},
            "output_config": {"effort": "high"},
            "messages": [{"role": "user", "content": "hi"}]
        });
        let ir = RequestIr::parse(Protocol::AnthropicMessages, &request).unwrap();
        let responses = ir.render(Protocol::OpenAiResponses, &caps).unwrap();
        assert!(responses.get("thinking").is_none());
        assert_eq!(responses["reasoning"]["effort"], "high");
    }

    #[test]
    fn codex_freeform_custom_tool_is_dropped_not_leaked() {
        // Codex's freeform tool is `type: "custom"` with a `format` descriptor and
        // no schema. It must go through the server-tool policy (dropped by default)
        // instead of leaking to upstreams that only accept `function` (chat 400) or
        // require `input_schema` (messages 400).
        let freeform = json!({
            "type": "custom",
            "name": "freeform",
            "description": "Emit arbitrary structured output",
            "format": {"type": "text", "syntax": "markdown", "definition": "..."}
        });
        let request = json!({
            "model": "m",
            "input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]}],
            "tools": [freeform]
        });
        let ir = RequestIr::parse(Protocol::OpenAiResponses, &request).unwrap();
        // Chat: dropped (no custom type).
        let (chat, report) = ir
            .render_with_report(
                Protocol::OpenAiChatCompletions,
                &ModelCapabilities::default(),
            )
            .unwrap();
        assert!(
            chat.get("tools").is_none(),
            "freeform tool dropped for chat"
        );
        assert_eq!(report.dropped_server_tools, vec!["freeform"]);
        // Messages: dropped (no custom free-text type).
        let (messages, report) = ir
            .render_with_report(Protocol::AnthropicMessages, &ModelCapabilities::default())
            .unwrap();
        assert!(
            messages.get("tools").is_none(),
            "freeform tool dropped for messages"
        );
        assert_eq!(report.dropped_server_tools, vec!["freeform"]);
    }

    #[test]
    fn codex_freeform_custom_tool_is_kept_for_responses() {
        // The Responses dialect natively supports `type: "custom"` free-text tools
        // (codex-researched from the OpenAI API reference: custom = free form text
        // inputs/outputs with a format descriptor); the tool is preserved as-is.
        let freeform = json!({
            "type": "custom",
            "name": "freeform",
            "description": "Emit arbitrary structured output",
            "format": {"type": "text"}
        });
        let request = json!({
            "model": "m",
            "input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]}],
            "tools": [freeform]
        });
        let ir = RequestIr::parse(Protocol::OpenAiResponses, &request).unwrap();
        let (responses, report) = ir
            .render_with_report(Protocol::OpenAiResponses, &ModelCapabilities::default())
            .unwrap();
        let tools = responses["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["type"], "custom");
        assert_eq!(tools[0]["name"], "freeform");
        assert_eq!(tools[0]["format"]["type"], "text");
        assert!(report.dropped_server_tools.is_empty());
    }

    #[test]
    fn anthropic_schema_custom_tool_is_preserved() {
        // Anthropic `custom` tools carry an input_schema and must keep working as
        // ordinary tools (they are schema'd client functions, not freeform).
        let request = json!({
            "model": "m",
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [{"type": "custom", "name": "my_custom", "description": "custom fn",
                       "input_schema": {"type": "object", "properties": {"q": {"type": "string"}}}}]
        });
        let ir = RequestIr::parse(Protocol::AnthropicMessages, &request).unwrap();
        let (chat, report) = ir
            .render_with_report(
                Protocol::OpenAiChatCompletions,
                &ModelCapabilities::default(),
            )
            .unwrap();
        let tools = chat["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1, "schema'd custom tool kept");
        assert_eq!(tools[0]["function"]["name"], "my_custom");
        assert!(report.dropped_server_tools.is_empty());
    }

    #[test]
    fn developer_messages_keep_role_in_ir_and_render() {
        // Parse must not lose the developer role: it stays an IrMessage and renders
        // as a developer message to dialects that support it (chat, responses).
        let request = json!({
            "model": "m",
            "input": [
                {"type": "message", "role": "developer",
                 "content": [{"type": "input_text", "text": "be precise"}]},
                {"type": "message", "role": "user",
                 "content": [{"type": "input_text", "text": "hi"}]}
            ]
        });
        let ir = RequestIr::parse(Protocol::OpenAiResponses, &request).unwrap();
        // IR keeps the developer message with its role and content.
        let roles: Vec<&str> = ir
            .transcript
            .iter()
            .filter_map(|item| match item {
                IrConversationItem::Message(message) => Some(message.role.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(roles, vec!["developer", "user"]);

        // Responses render keeps the developer message.
        let responses = ir
            .render(Protocol::OpenAiResponses, &ModelCapabilities::default())
            .unwrap();
        assert_eq!(responses["input"][0]["role"], "developer");

        // Chat render keeps the developer message.
        let chat = ir
            .render(
                Protocol::OpenAiChatCompletions,
                &ModelCapabilities::default(),
            )
            .unwrap();
        assert_eq!(chat["messages"][0]["role"], "developer");
        assert_eq!(chat["messages"][0]["content"][0]["text"], "be precise");
    }

    #[test]
    fn developer_and_mid_system_fold_into_messages_system() {
        // Messages has no developer/system message role: both fold into the top-level
        // system while keeping their content.
        let request = json!({
            "model": "m",
            "input": [
                {"type": "message", "role": "developer",
                 "content": [{"type": "input_text", "text": "be precise"}]},
                {"type": "message", "role": "user",
                 "content": [{"type": "input_text", "text": "first"}]},
                {"type": "message", "role": "system",
                 "content": [{"type": "input_text", "text": "mid-rule"}]},
                {"type": "message", "role": "user",
                 "content": [{"type": "input_text", "text": "second"}]}
            ]
        });
        let ir = RequestIr::parse(Protocol::OpenAiResponses, &request).unwrap();
        let messages = ir
            .render(Protocol::AnthropicMessages, &ModelCapabilities::default())
            .unwrap();
        let system = messages["system"].as_str().unwrap();
        assert!(system.contains("be precise"));
        assert!(system.contains("mid-rule"));
        // The messages array carries only the user turns.
        let roles: Vec<&str> = messages["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|m| m["role"].as_str())
            .collect();
        assert_eq!(roles, vec!["user", "user"]);
    }

    #[test]
    fn chat_mid_system_becomes_system_reminder_at_render() {
        // The system-reminder wrapping is a render-time behavior for chat upstreams;
        // parse keeps the system role.
        let request = json!({
            "model": "m",
            "messages": [
                {"role": "user", "content": "first"},
                {"role": "system", "content": "mid-rule"},
                {"role": "assistant", "content": "ok"}
            ]
        });
        let ir = RequestIr::parse(Protocol::OpenAiChatCompletions, &request).unwrap();
        let roles: Vec<&str> = ir
            .transcript
            .iter()
            .filter_map(|item| match item {
                IrConversationItem::Message(message) => Some(message.role.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(roles, vec!["user", "system", "assistant"]);

        let chat = ir
            .render(
                Protocol::OpenAiChatCompletions,
                &ModelCapabilities::default(),
            )
            .unwrap();
        let rendered = chat["messages"].as_array().unwrap();
        assert_eq!(rendered[1]["role"], "user");
        assert!(rendered[1]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("<system-reminder>"));
    }

    #[test]
    fn prompt_cache_options_kept_only_with_capability() {
        let request = json!({
            "model": "m",
            "input": "hi",
            "prompt_cache_key": "cache-1",
            "prompt_cache_options": {"mode": "explicit", "ttl": "30m"}
        });
        let ir = RequestIr::parse(Protocol::OpenAiResponses, &request).unwrap();

        // No capability: key and options stripped.
        let responses = ir
            .render(Protocol::OpenAiResponses, &ModelCapabilities::default())
            .unwrap();
        assert!(responses.get("prompt_cache_key").is_none());
        assert!(responses.get("prompt_cache_options").is_none());

        // prompt_cache_key only.
        let mut caps = ModelCapabilities::default();
        caps.cache.prompt_cache_key = true;
        let responses = ir.render(Protocol::OpenAiResponses, &caps).unwrap();
        assert_eq!(responses["prompt_cache_key"], "cache-1");
        assert!(responses.get("prompt_cache_options").is_none());

        // Both capabilities.
        let mut caps = ModelCapabilities::default();
        caps.cache.prompt_cache_key = true;
        caps.cache.prompt_cache_options = true;
        let responses = ir.render(Protocol::OpenAiResponses, &caps).unwrap();
        assert_eq!(responses["prompt_cache_options"]["mode"], "explicit");
        assert_eq!(responses["prompt_cache_options"]["ttl"], "30m");

        // Messages target never carries the OpenAI options.
        let messages = ir.render(Protocol::AnthropicMessages, &caps).unwrap();
        assert!(messages.get("prompt_cache_options").is_none());
    }

    #[test]
    fn single_text_block_always_renders_extended_form() {
        // Even when the client passes the shorthand ("content": "hi"), the chat
        // renderer emits the extended array form, keeping the converted prefix
        // byte-stable across turns and clients (exact-prefix caches depend on it).
        let request = json!({
            "model": "m",
            "messages": [
                {"role": "system", "content": "be concise"},
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "ok"},
                {"role": "tool", "tool_call_id": "call_1", "content": "result"}
            ]
        });
        let ir = RequestIr::parse(Protocol::OpenAiChatCompletions, &request).unwrap();
        let chat = ir
            .render(
                Protocol::OpenAiChatCompletions,
                &ModelCapabilities::default(),
            )
            .unwrap();
        let messages = chat["messages"].as_array().unwrap();
        // system, user, assistant: array form.
        for (index, role, text) in [
            (0, "system", "be concise"),
            (1, "user", "hi"),
            (2, "assistant", "ok"),
        ] {
            assert_eq!(messages[index]["role"], role);
            assert!(
                messages[index]["content"].is_array(),
                "{role} content is array"
            );
            assert_eq!(messages[index]["content"][0]["type"], "text");
            assert_eq!(messages[index]["content"][0]["text"], text);
        }
        // tool: string form (Chat dialect requires it).
        assert_eq!(messages[3]["role"], "tool");
        assert_eq!(messages[3]["content"], "result");

        // The same message with explicit blocks produces the identical shape.
        let request = json!({
            "model": "m",
            "messages": [{"role": "user", "content": [{"type": "text", "text": "hi"}]}]
        });
        let ir = RequestIr::parse(Protocol::OpenAiChatCompletions, &request).unwrap();
        let chat = ir
            .render(
                Protocol::OpenAiChatCompletions,
                &ModelCapabilities::default(),
            )
            .unwrap();
        assert_eq!(
            chat["messages"][0]["content"],
            serde_json::json!([{"type": "text", "text": "hi"}])
        );
    }

    #[test]
    fn block_cache_control_kept_or_stripped_by_capability() {
        let request = json!({
            "model": "m",
            "messages": [{"role":"user", "content":[
                {"type":"text", "text":"hello", "cache_control":{"type":"ephemeral"}}
            ]}]
        });
        let ir = RequestIr::parse(Protocol::AnthropicMessages, &request).unwrap();

        // No capability: block cache_control stripped from chat and responses.
        let chat = ir
            .render(
                Protocol::OpenAiChatCompletions,
                &ModelCapabilities::default(),
            )
            .unwrap();
        assert!(chat["messages"][0]["content"][0]
            .get("cache_control")
            .is_none());
        let responses = ir
            .render(Protocol::OpenAiResponses, &ModelCapabilities::default())
            .unwrap();
        assert!(responses["input"][0]["content"][0]
            .get("cache_control")
            .is_none());

        // Capability on: kept for chat (Qwen chat endpoint), stripped for responses
        // (no provider supports block-level cache_control on responses).
        let mut caps = ModelCapabilities::default();
        caps.cache.block_cache_control = true;
        let chat = ir.render(Protocol::OpenAiChatCompletions, &caps).unwrap();
        assert_eq!(
            chat["messages"][0]["content"][0]["cache_control"]["type"],
            "ephemeral"
        );
        let responses = ir.render(Protocol::OpenAiResponses, &caps).unwrap();
        assert!(responses["input"][0]["content"][0]
            .get("cache_control")
            .is_none());

        // Messages target keeps the block per capability: Anthropic-native for
        // supporting upstreams (MiniMax anthropic endpoint, Qwen), stripped for
        // upstreams that ignore it (DeepSeek/GLM/Kimi/MiMo/Grok).
        let messages = ir
            .render(Protocol::AnthropicMessages, &ModelCapabilities::default())
            .unwrap();
        assert!(messages["messages"][0]["content"][0]
            .get("cache_control")
            .is_none());
        let messages = ir.render(Protocol::AnthropicMessages, &caps).unwrap();
        assert_eq!(
            messages["messages"][0]["content"][0]["cache_control"]["type"],
            "ephemeral"
        );
    }

    #[test]
    fn top_level_cache_control_kept_only_for_messages_with_capability() {
        let request = json!({
            "model": "m",
            "cache_control": {"type": "ephemeral"},
            "messages": [{"role":"user", "content":"hello"}]
        });
        let ir = RequestIr::parse(Protocol::AnthropicMessages, &request).unwrap();

        // No capability: top-level cache_control stripped everywhere.
        let messages = ir
            .render(Protocol::AnthropicMessages, &ModelCapabilities::default())
            .unwrap();
        assert!(messages.get("cache_control").is_none());

        // Capability on: kept for Messages (Anthropic auto-breakpoint).
        let mut caps = ModelCapabilities::default();
        caps.cache.block_cache_control = true;
        let messages = ir.render(Protocol::AnthropicMessages, &caps).unwrap();
        assert_eq!(messages["cache_control"]["type"], "ephemeral");
        // Still stripped for chat/responses.
        let chat = ir.render(Protocol::OpenAiChatCompletions, &caps).unwrap();
        assert!(chat.get("cache_control").is_none());
        let responses = ir.render(Protocol::OpenAiResponses, &caps).unwrap();
        assert!(responses.get("cache_control").is_none());
    }

    #[test]
    fn messages_custom_tool_stays_custom() {
        // The Messages dialect natively supports `custom` tools; rendering back to
        // Messages keeps the custom type instead of flattening it.
        let request = json!({
            "model": "m",
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [{"type": "custom", "name": "my_custom", "description": "custom fn",
                       "input_schema": {"type": "object", "properties": {"q": {"type": "string"}}}}]
        });
        let ir = RequestIr::parse(Protocol::AnthropicMessages, &request).unwrap();
        let (messages, report) = ir
            .render_with_report(Protocol::AnthropicMessages, &ModelCapabilities::default())
            .unwrap();
        let tools = messages["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["type"], "custom", "custom type preserved");
        assert_eq!(tools[0]["name"], "my_custom");
        assert!(report.dropped_server_tools.is_empty());
    }

    #[test]
    fn developer_folds_when_upstream_rejects_role() {
        // accept_developer_role=false folds developer messages into system for both
        // chat and responses renderings; content is preserved.
        let caps = ModelCapabilities {
            accept_developer_role: false,
            ..Default::default()
        };
        let request = json!({
            "model": "m",
            "input": [
                {"type": "message", "role": "developer",
                 "content": [{"type": "input_text", "text": "be precise"}]},
                {"type": "message", "role": "user",
                 "content": [{"type": "input_text", "text": "hi"}]}
            ]
        });
        let ir = RequestIr::parse(Protocol::OpenAiResponses, &request).unwrap();

        let chat = ir.render(Protocol::OpenAiChatCompletions, &caps).unwrap();
        assert_eq!(chat["messages"][0]["role"], "system");
        assert_eq!(chat["messages"][0]["content"][0]["text"], "be precise");

        let responses = ir.render(Protocol::OpenAiResponses, &caps).unwrap();
        assert_eq!(responses["input"][0]["role"], "system");
        assert_eq!(responses["input"][0]["content"][0]["text"], "be precise");
    }

    #[test]
    fn chat_cache_identity_maps_to_responses_prompt_cache_key() {
        let ir = RequestIr::parse(
            Protocol::OpenAiChatCompletions,
            &json!({
                "model": "m",
                "user": "chat-cache-user",
                "messages": [{"role":"user", "content":"hello"}]
            }),
        )
        .unwrap();
        // Without the capability the key is stripped (conservative default).
        let responses = ir
            .render(Protocol::OpenAiResponses, &ModelCapabilities::default())
            .unwrap();
        assert!(responses.get("prompt_cache_key").is_none());
        // With the capability the identity maps to prompt_cache_key.
        let mut caps = ModelCapabilities::default();
        caps.cache.prompt_cache_key = true;
        let responses = ir.render(Protocol::OpenAiResponses, &caps).unwrap();
        assert_eq!(responses["prompt_cache_key"], "chat-cache-user");
    }
}
