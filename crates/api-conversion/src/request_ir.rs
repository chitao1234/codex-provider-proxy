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
    pub protocol: Protocol,
    pub original: Map<String, Value>,
    pub model: Option<String>,
    pub stream: Option<bool>,
    pub messages: Vec<IrMessage>,
    pub instructions: Vec<Value>,
    pub reasoning: Vec<Value>,
    pub tools: Vec<IrTool>,
    pub tool_choice: Option<Value>,
    pub parameters: BTreeMap<String, Value>,
    pub continuation: BTreeMap<String, Value>,
    pub context: RequestContext,
}

/// Stateful request context shared by protocols and provider dialects.
///
/// These fields affect conversation affinity, abuse attribution, or prompt-cache reuse. They are
/// modeled separately from sampling parameters so continuation can inherit them intentionally.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RequestContext {
    pub previous_response_id: Option<String>,
    pub conversation: Option<Value>,
    pub end_user_id: Option<String>,
    pub safety_identifier: Option<String>,
    pub metadata: Option<Value>,
    pub cache: CacheContext,
    pub store: Option<Value>,
    pub include: Option<Value>,
    pub truncation: Option<Value>,
    /// Provider-specific identity/session/cache keys that have no portable spelling yet.
    pub extensions: BTreeMap<String, Value>,
}

/// Protocol-neutral prompt-cache semantics.
///
/// `key` is the stable cache-affinity identity. Protocols expose it under different names
/// (`prompt_cache_key`, `metadata.user_id`, or `user`), so renderers map it only at the boundary.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CacheContext {
    pub key: Option<String>,
    pub retention: Option<Value>,
    pub control: Option<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IrMessage {
    pub role: String,
    pub content: Vec<IrContent>,
    pub item_id: Option<String>,
    pub tool_call_id: Option<String>,
    pub tool_calls: Vec<IrToolCall>,
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IrContent {
    Text {
        text: String,
        extra: Map<String, Value>,
    },
    Thinking {
        text: String,
        signature: Option<String>,
        extra: Map<String, Value>,
    },
    Image(Value),
    ToolUse(IrToolCall),
    ToolResult {
        call_id: String,
        content: Value,
        is_error: bool,
        extra: Map<String, Value>,
    },
    Raw(Value),
}

#[derive(Debug, Clone, PartialEq)]
pub struct IrToolCall {
    pub id: Option<String>,
    pub call_id: Option<String>,
    pub name: String,
    pub arguments: Value,
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IrTool {
    pub kind: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub parameters: Option<Value>,
    pub raw: Value,
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
        let mut ir = Self {
            protocol,
            original,
            model,
            stream,
            messages: Vec::new(),
            instructions: Vec::new(),
            reasoning: Vec::new(),
            tools: Vec::new(),
            tool_choice: None,
            parameters: BTreeMap::new(),
            continuation: BTreeMap::new(),
            context: RequestContext::default(),
        };

        match protocol {
            Protocol::AnthropicMessages => ir.parse_messages(),
            Protocol::OpenAiChatCompletions => ir.parse_chat(),
            Protocol::OpenAiResponses => ir.parse_responses(),
        }
        ir.parse_context();
        ir.collect_parameters();
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
            if let Some(previous_request) = previous_request {
                let mut messages = previous_request.messages.clone();
                messages.extend(ir.messages);
                ir.messages = messages;
                if ir.reasoning.is_empty() {
                    ir.reasoning = previous_request.reasoning.clone();
                }
                ir.inherit_context(&previous_request.context);
            }
            if let Some(reasoning) = last_reasoning.filter(|value| !value.trim().is_empty()) {
                if ir.reasoning.is_empty() {
                    ir.reasoning.push(serde_json::json!({
                        "type": "reasoning",
                        "summary": [{"type": "summary_text", "text": reasoning}]
                    }));
                }
            }
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
        self.context.cache.retention = self.original.get("prompt_cache_retention").cloned();
        self.context.cache.control = self.original.get("cache_control").cloned();
        self.context.store = self.original.get("store").cloned();
        self.context.include = self.original.get("include").cloned();
        self.context.truncation = self.original.get("truncation").cloned();

        for (key, value) in &self.original {
            if is_context_extension_key(key) {
                self.context.extensions.insert(key.clone(), value.clone());
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
        self.context.extensions = previous
            .extensions
            .iter()
            .chain(&self.context.extensions)
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
        self.messages.extend(parsed.messages);
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
            for value in &messages {
                if value.get("role").and_then(Value::as_str) == Some("system") {
                    if let Some(content) = value.get("content") {
                        self.instructions.push(content.clone());
                    }
                } else {
                    self.parse_message(value, Protocol::AnthropicMessages);
                }
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
                if value.get("role").and_then(Value::as_str) == Some("system") {
                    if let Some(content) = value.get("content") {
                        self.instructions.push(content.clone());
                    }
                } else {
                    self.parse_message(value, Protocol::OpenAiChatCompletions);
                }
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
        if let Some(previous) = self.original.get("previous_response_id") {
            self.continuation
                .insert("previous_response_id".to_owned(), previous.clone());
        }
        if let Some(input) = self.original.get("input") {
            if let Some(text) = input.as_str() {
                self.messages.push(IrMessage {
                    role: "user".to_owned(),
                    content: vec![IrContent::Text {
                        text: text.to_owned(),
                        extra: Map::new(),
                    }],
                    item_id: None,
                    tool_call_id: None,
                    tool_calls: Vec::new(),
                    extra: Map::new(),
                });
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
                    Some("message") => self.parse_message(value, Protocol::OpenAiResponses),
                    Some("function_call") => self.messages.push(IrMessage::assistant_tool_call(
                        IrToolCall::from_responses_call(value),
                    )),
                    Some("reasoning") => self.reasoning.push(value.clone()),
                    Some("function_call_output") => self.messages.push(IrMessage {
                        role: "tool".to_owned(),
                        content: vec![IrContent::Raw(
                            value.get("output").cloned().unwrap_or(Value::Null),
                        )],
                        item_id: value.get("id").and_then(Value::as_str).map(str::to_owned),
                        tool_call_id: value
                            .get("call_id")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        tool_calls: Vec::new(),
                        extra: Map::new(),
                    }),
                    _ => self.messages.push(IrMessage::custom(value.clone())),
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
            self.messages.push(IrMessage::custom(value.clone()));
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
            extra: object.clone(),
        };
        if protocol != Protocol::AnthropicMessages {
            if let Some(calls) = object.get("tool_calls").and_then(Value::as_array) {
                message.tool_calls = calls.iter().map(IrToolCall::from_chat).collect();
            }
        }
        self.messages.push(message);
    }

    fn collect_parameters(&mut self) {
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
        ];
        for (key, value) in &self.original {
            if !STRUCTURAL.contains(&key.as_str()) {
                self.parameters.insert(key.clone(), value.clone());
            }
        }
    }

    pub fn render(
        &self,
        target: Protocol,
        caps: &ModelCapabilities,
    ) -> Result<Value, ConversionError> {
        match target {
            Protocol::AnthropicMessages => self.render_messages(caps),
            Protocol::OpenAiChatCompletions => self.render_chat(caps),
            Protocol::OpenAiResponses => self.render_responses(caps),
        }
    }

    pub fn render_chat_with_capabilities(
        &self,
        caps: &ModelCapabilities,
    ) -> Result<(Value, RequestConversionReport), ConversionError> {
        let mut rendered = self.render_chat(caps)?;
        let report = self.normalize_chat_capabilities(
            caps,
            rendered
                .as_object_mut()
                .expect("chat renderer always returns an object"),
        )?;
        Ok((rendered, report))
    }

    fn base_output(&self, target: Protocol) -> Map<String, Value> {
        let mut out = self.original.clone();
        let keys = match target {
            Protocol::AnthropicMessages => [
                "messages",
                "system",
                "input",
                "instructions",
                "previous_response_id",
            ],
            Protocol::OpenAiChatCompletions => [
                "messages",
                "system",
                "input",
                "instructions",
                "previous_response_id",
            ],
            Protocol::OpenAiResponses => [
                "messages",
                "system",
                "input",
                "instructions",
                "previous_response_id",
            ],
        };
        for key in keys {
            out.remove(key);
        }
        out
    }

    fn render_chat(&self, caps: &ModelCapabilities) -> Result<Value, ConversionError> {
        let mut out = self.base_output(Protocol::OpenAiChatCompletions);
        self.apply_context(Protocol::OpenAiChatCompletions, &mut out);
        let mut messages = Vec::new();
        for instruction in &self.instructions {
            if let Some(text) = instruction.as_str() {
                messages.push(serde_json::json!({"role":"system", "content":text}));
            }
        }
        for message in &self.messages {
            messages.extend(message.to_chat_values());
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
        normalize_chat_parameters(&mut out, caps);
        self.normalize_chat_capabilities(caps, &mut out)?;
        Ok(Value::Object(out))
    }

    fn normalize_chat_capabilities(
        &self,
        caps: &ModelCapabilities,
        out: &mut Map<String, Value>,
    ) -> Result<RequestConversionReport, ConversionError> {
        if self
            .messages
            .iter()
            .flat_map(|message| message.content.iter())
            .any(|content| matches!(content, IrContent::Image(_)))
            && !caps.image_input
        {
            return Err(ConversionError::unsupported(
                "messages[].content.image",
                "upstream does not support image input",
            ));
        }
        if let Some(thinking) = self.original.get("thinking") {
            let thinking_type = thinking.get("type").and_then(Value::as_str);
            match caps.thinking_param {
                ThinkingParam::TopLevel => {
                    out.insert(
                        "thinking".to_owned(),
                        serde_json::json!({
                            "type": if thinking_type == Some("disabled") { "disabled" } else { "enabled" }
                        }),
                    );
                }
                ThinkingParam::Adaptive => {
                    out.insert(
                        "thinking".to_owned(),
                        serde_json::json!({
                            "type": if thinking_type == Some("disabled") { "disabled" } else { "adaptive" }
                        }),
                    );
                }
                ThinkingParam::EnableThinking => {
                    out.insert(
                        "enable_thinking".to_owned(),
                        Value::Bool(thinking_type != Some("disabled")),
                    );
                    out.remove("thinking");
                }
                ThinkingParam::None => {
                    out.remove("thinking");
                    out.remove("enable_thinking");
                }
            }
        }
        if let Some(config) = caps
            .reasoning_effort
            .as_ref()
            .filter(|config| config.enabled)
        {
            let effort = self
                .original
                .get("output_config")
                .and_then(Value::as_object)
                .and_then(|config| config.get("effort"))
                .and_then(Value::as_str)
                .or(config.default.as_deref())
                .and_then(|effort| caps.clamp_effort(effort));
            if let Some(effort) = effort {
                out.insert("reasoning_effort".to_owned(), Value::String(effort));
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
        if let Some(format) = self
            .original
            .get("output_config")
            .and_then(Value::as_object)
            .and_then(|config| config.get("format"))
        {
            match format.get("type").and_then(Value::as_str).unwrap_or("text") {
                "text" => {}
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
        let mut rendered_tools = Vec::new();
        for tool in &self.tools {
            if is_server_tool_kind(&tool.kind) {
                let name = tool.name.clone().unwrap_or_else(|| tool.kind.clone());
                match caps.server_tools {
                    ServerToolPolicy::Drop => report.dropped_server_tools.push(name),
                    ServerToolPolicy::MapToFunction => {
                        report.mapped_server_tools.push(name.clone());
                        rendered_tools.push(server_tool_as_function(tool));
                    }
                    ServerToolPolicy::Passthrough | ServerToolPolicy::ProviderNative => {
                        rendered_tools.push(tool.raw.clone());
                    }
                }
            } else {
                rendered_tools.push(tool.to_chat());
            }
        }
        if !self.tools.is_empty() {
            out.insert("tools".to_owned(), Value::Array(rendered_tools));
        }
        Ok(report)
    }

    fn render_messages(&self, _caps: &ModelCapabilities) -> Result<Value, ConversionError> {
        let mut out = self.base_output(Protocol::AnthropicMessages);
        self.apply_context(Protocol::AnthropicMessages, &mut out);
        if let Some(value) = out.remove("max_completion_tokens") {
            out.insert("max_tokens".to_owned(), value);
        }
        if let Some(value) = out.remove("max_output_tokens") {
            out.insert("max_tokens".to_owned(), value);
        }
        if let Some(reasoning) = out.remove("reasoning") {
            if let Some(effort) = reasoning.get("effort") {
                out.insert(
                    "output_config".to_owned(),
                    serde_json::json!({"effort": effort}),
                );
            }
        }
        if !self.instructions.is_empty() {
            out.insert(
                "system".to_owned(),
                if self.instructions.len() == 1 {
                    self.instructions[0].clone()
                } else {
                    Value::Array(self.instructions.clone())
                },
            );
        }
        let mut messages = Vec::new();
        if !self.reasoning.is_empty() {
            let blocks = self
                .reasoning
                .iter()
                .flat_map(reasoning_to_messages_blocks)
                .collect::<Vec<_>>();
            if !blocks.is_empty() {
                messages.push(serde_json::json!({"role":"assistant", "content":blocks}));
            }
        }
        messages.extend(self.messages.iter().flat_map(IrMessage::to_messages_values));
        out.insert("messages".to_owned(), Value::Array(messages));
        if !self.tools.is_empty() {
            out.insert(
                "tools".to_owned(),
                Value::Array(self.tools.iter().map(IrTool::to_messages).collect()),
            );
        }
        if let Some(choice) = &self.tool_choice {
            out.insert("tool_choice".to_owned(), choice.clone());
        }
        Ok(Value::Object(out))
    }

    fn render_responses(&self, caps: &ModelCapabilities) -> Result<Value, ConversionError> {
        let mut out = self.base_output(Protocol::OpenAiResponses);
        self.apply_context(Protocol::OpenAiResponses, &mut out);
        if !self.instructions.is_empty() {
            out.insert(
                "instructions".to_owned(),
                if self.instructions.len() == 1 {
                    self.instructions[0].clone()
                } else {
                    Value::Array(self.instructions.clone())
                },
            );
        }
        let mut input = self.reasoning.to_vec();
        input.extend(
            self.messages
                .iter()
                .flat_map(IrMessage::to_responses_values),
        );
        out.insert("input".to_owned(), Value::Array(input));
        if !self.tools.is_empty() {
            out.insert(
                "tools".to_owned(),
                Value::Array(self.tools.iter().map(IrTool::to_responses).collect()),
            );
        }
        if let Some(choice) = &self.tool_choice {
            out.insert("tool_choice".to_owned(), choice.clone());
        }
        normalize_responses_parameters(&mut out, caps);
        Ok(Value::Object(out))
    }

    fn apply_context(&self, target: Protocol, out: &mut Map<String, Value>) {
        if target != Protocol::OpenAiResponses {
            out.remove("prompt_cache_key");
            out.remove("prompt_cache_retention");
        }
        for (key, value) in &self.context.extensions {
            if target != Protocol::OpenAiResponses
                && matches!(key.as_str(), "prompt_cache_key" | "prompt_cache_retention")
            {
                continue;
            }
            out.entry(key.clone()).or_insert_with(|| value.clone());
        }
        if let Some(metadata) = &self.context.metadata {
            out.entry("metadata".to_owned())
                .or_insert_with(|| metadata.clone());
        }
        if let Some(cache_control) = &self.context.cache.control {
            out.entry("cache_control".to_owned())
                .or_insert_with(|| cache_control.clone());
        }
        if let Some(conversation) = &self.context.conversation {
            out.entry("conversation".to_owned())
                .or_insert_with(|| conversation.clone());
        }
        if let Some(identifier) = &self.context.safety_identifier {
            out.entry("safety_identifier".to_owned())
                .or_insert_with(|| Value::String(identifier.clone()));
        }
        if target == Protocol::OpenAiResponses {
            if let Some(key) = &self.context.cache.key {
                out.entry("prompt_cache_key".to_owned())
                    .or_insert_with(|| Value::String(key.clone()));
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
                if let Some(user_id) = self.context.end_user_id.as_ref().or(self
                    .context
                    .cache
                    .key
                    .as_ref())
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
                if let Some(user_id) = self.context.end_user_id.as_ref().or(self
                    .context
                    .cache
                    .key
                    .as_ref())
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
            extra: Map::new(),
        }],
        Some(value) if !value.is_null() => vec![IrContent::Raw(value.clone())],
        _ => Vec::new(),
    }
}

fn normalize_chat_parameters(out: &mut Map<String, Value>, caps: &ModelCapabilities) {
    if let Some(value) = out.remove("max_output_tokens") {
        out.insert(
            match caps.max_tokens_field {
                crate::dialect::MaxTokensField::MaxTokens => "max_tokens",
                crate::dialect::MaxTokensField::MaxCompletionTokens => "max_completion_tokens",
            }
            .to_owned(),
            value,
        );
    }
}

fn is_server_tool_kind(kind: &str) -> bool {
    kind.starts_with("web_search")
        || kind.starts_with("web_fetch")
        || kind.starts_with("code_execution")
        || kind.starts_with("tool_search")
        || kind.starts_with("computer_")
}

fn server_tool_as_function(tool: &IrTool) -> Value {
    let name = tool
        .name
        .clone()
        .unwrap_or_else(|| tool.kind.replace('-', "_"));
    serde_json::json!({
        "type": "function",
        "function": {
            "name": name,
            "description": tool.description.clone().unwrap_or_default(),
            "parameters": tool.parameters.clone().unwrap_or_else(|| serde_json::json!({"type":"object","properties":{}}))
        }
    })
}

fn normalize_responses_parameters(out: &mut Map<String, Value>, _caps: &ModelCapabilities) {
    if let Some(value) = out.remove("max_tokens") {
        out.insert("max_output_tokens".to_owned(), value);
    }
    if let Some(value) = out.remove("max_completion_tokens") {
        out.insert("max_output_tokens".to_owned(), value);
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
        } else {
            out.insert("output_config".to_owned(), value);
        }
    }
}

impl IrContent {
    fn parse(value: &Value) -> Self {
        let Some(object) = value.as_object() else {
            return Self::Raw(value.clone());
        };
        match object.get("type").and_then(Value::as_str) {
            Some("text" | "input_text" | "output_text") => Self::Text {
                text: object
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                extra: object.clone(),
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
                extra: object.clone(),
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
                extra: object.clone(),
            },
            _ => Self::Raw(value.clone()),
        }
    }

    fn raw_value(&self) -> Value {
        match self {
            Self::Text { text, extra } => {
                let mut object = extra.clone();
                object.insert("type".to_owned(), Value::String("text".to_owned()));
                object.insert("text".to_owned(), Value::String(text.clone()));
                Value::Object(object)
            }
            Self::Thinking {
                text,
                signature,
                extra,
            } => {
                let mut object = extra.clone();
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
                extra,
            } => {
                let mut object = extra.clone();
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
            Self::Text { text, extra } => {
                let mut object = extra.clone();
                object.insert("type".to_owned(), Value::String("text".to_owned()));
                object.insert("text".to_owned(), Value::String(text.clone()));
                Value::Object(object)
            }
            Self::Thinking { text, .. } => serde_json::json!({"type":"text", "text":text}),
            other => other.raw_value(),
        }
    }

    fn to_messages(&self) -> Value {
        self.raw_value()
    }

    fn to_responses(&self, role: &str) -> Value {
        match self {
            Self::Text { text, extra } => {
                let mut object = extra.clone();
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

impl IrMessage {
    fn custom(value: Value) -> Self {
        Self {
            role: "custom".to_owned(),
            content: vec![IrContent::Raw(value)],
            item_id: None,
            tool_call_id: None,
            tool_calls: Vec::new(),
            extra: Map::new(),
        }
    }

    fn assistant_tool_call(call: IrToolCall) -> Self {
        Self {
            role: "assistant".to_owned(),
            content: Vec::new(),
            item_id: None,
            tool_call_id: None,
            tool_calls: vec![call],
            extra: Map::new(),
        }
    }

    fn to_chat_values(&self) -> Vec<Value> {
        if self.role == "custom" {
            return self.content.iter().map(IrContent::raw_value).collect();
        }
        let mut object = self.extra.clone();
        for key in ["role", "content", "tool_calls", "tool_call_id", "id"] {
            object.remove(key);
        }
        object.insert("role".to_owned(), Value::String(self.role.clone()));
        if self.role == "tool" {
            object.insert(
                "tool_call_id".to_owned(),
                Value::String(self.tool_call_id.clone().unwrap_or_default()),
            );
            object.insert("content".to_owned(), content_as_chat(&self.content));
            return vec![Value::Object(object)];
        }
        object.insert("content".to_owned(), content_as_chat(&self.content));
        if !self.tool_calls.is_empty() {
            object.insert(
                "tool_calls".to_owned(),
                Value::Array(self.tool_calls.iter().map(IrToolCall::to_chat).collect()),
            );
        }
        vec![Value::Object(object)]
    }

    fn to_messages_values(&self) -> Vec<Value> {
        if self.role == "custom" {
            return Vec::new();
        }
        if self.role == "tool" {
            return vec![serde_json::json!({
                "role":"user",
                "content":[{"type":"tool_result", "tool_use_id":self.tool_call_id.clone().unwrap_or_default(), "content":content_as_chat(&self.content)}]
            })];
        }
        let mut blocks = self
            .content
            .iter()
            .map(IrContent::to_messages)
            .collect::<Vec<_>>();
        if self.role == "assistant" {
            blocks.extend(self.tool_calls.iter().map(IrToolCall::to_messages_block));
        }
        let mut object = self.extra.clone();
        for key in ["role", "content", "tool_calls", "tool_call_id", "id"] {
            object.remove(key);
        }
        object.insert("role".to_owned(), Value::String(self.role.clone()));
        object.insert("content".to_owned(), Value::Array(blocks));
        vec![Value::Object(object)]
    }

    fn to_responses_values(&self) -> Vec<Value> {
        if self.role == "custom" {
            return self.content.iter().map(IrContent::raw_value).collect();
        }
        let mut values = Vec::new();
        if self.role == "tool" {
            values.push(serde_json::json!({
                "type":"function_call_output",
                "call_id":self.tool_call_id.clone().unwrap_or_default(),
                "output":content_as_chat(&self.content)
            }));
            return values;
        }
        let mut content = Vec::new();
        for part in &self.content {
            match part {
                IrContent::ToolResult {
                    call_id, content, ..
                } => values.push(serde_json::json!({
                    "type":"function_call_output",
                    "call_id":call_id,
                    "output":content
                })),
                IrContent::ToolUse(call) => values.push(call.to_responses()),
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

fn content_as_chat(content: &[IrContent]) -> Value {
    if let [IrContent::Text { text, .. }] = content {
        return Value::String(text.clone());
    }
    Value::Array(content.iter().map(IrContent::to_chat).collect())
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
            extra: value.as_object().cloned().unwrap_or_default(),
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
            extra: value.as_object().cloned().unwrap_or_default(),
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
            extra: value.as_object().cloned().unwrap_or_default(),
        }
    }

    fn to_chat(&self) -> Value {
        serde_json::json!({"id":self.id.clone().or_else(|| self.call_id.clone()).unwrap_or_default(), "type":"function", "function":{"name":self.name, "arguments":self.arguments}})
    }

    fn to_messages_block(&self) -> Value {
        serde_json::json!({"type":"tool_use", "id":self.id.clone().or_else(|| self.call_id.clone()).unwrap_or_default(), "name":self.name, "input":self.arguments})
    }

    fn to_responses(&self) -> Value {
        serde_json::json!({"type":"function_call", "id":self.id, "call_id":self.call_id.clone().or(self.id.clone()), "name":self.name, "arguments":self.arguments})
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

    fn to_chat(&self) -> Value {
        if self.kind != "function" {
            return self.raw.clone();
        }
        let mut function = self
            .raw
            .get("function")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_else(|| self.raw.as_object().cloned().unwrap_or_default());
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
            self.parameters
                .clone()
                .unwrap_or_else(|| serde_json::json!({"type":"object"})),
        );
        serde_json::json!({"type":"function", "function":function})
    }

    fn to_messages(&self) -> Value {
        if self.kind != "function" {
            return self.raw.clone();
        }
        let mut object = self.raw.as_object().cloned().unwrap_or_default();
        object.remove("type");
        object.remove("function");
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
        if self.kind != "function" {
            return self.raw.clone();
        }
        let mut object = self.raw.as_object().cloned().unwrap_or_default();
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
        let responses = ir
            .render(Protocol::OpenAiResponses, &ModelCapabilities::default())
            .unwrap();
        assert_eq!(responses["provider_budget"]["reasoning"], 7);
        assert_eq!(responses["tools"][0]["vendor_extension"]["x"], true);
        assert_eq!(responses["input"][0]["content"][0]["type"], "input_text");
        assert_eq!(
            responses["input"][0]["content"][0]["cache_control"]["type"],
            "ephemeral"
        );
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
        let responses = ir
            .render(Protocol::OpenAiResponses, &ModelCapabilities::default())
            .unwrap();

        assert_eq!(responses["user"], "user-42");
        assert_eq!(responses["metadata"]["client"], "codex");
        assert_eq!(responses["cache_control"]["type"], "ephemeral");
        assert_eq!(responses["session_id"], "session-7");
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

        assert_eq!(messages["metadata"]["user_id"], "user-1");
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
        let responses = ir
            .render(Protocol::OpenAiResponses, &ModelCapabilities::default())
            .unwrap();
        assert_eq!(responses["prompt_cache_key"], "anthropic-cache-user");
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
        let responses = ir
            .render(Protocol::OpenAiResponses, &ModelCapabilities::default())
            .unwrap();
        assert_eq!(responses["prompt_cache_key"], "chat-cache-user");
    }
}
