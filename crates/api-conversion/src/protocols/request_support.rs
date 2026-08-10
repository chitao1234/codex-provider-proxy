use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use crate::{
    dialect::ModelCapabilities,
    error::ConversionError,
    ir::{
        CacheIdentity, ContentPart, ConversationState, Extension, ExtensionScope,
        GenerationControls, Identity, ReplayPolicy, RequestIr, ToolCall, ToolDefinition,
        ToolResult,
    },
};

pub fn render_openai_state(
    request: &RequestIr,
    capabilities: &ModelCapabilities,
    responses: bool,
    output: &mut Map<String, Value>,
) {
    replay_state_extensions(request, output);
    if let Some(metadata) = &request.state.metadata {
        output.insert("metadata".to_owned(), metadata.clone());
    }
    if let Some(safety_identifier) = &request.state.safety_identifier {
        output.insert(
            "safety_identifier".to_owned(),
            Value::String(safety_identifier.clone()),
        );
    }
    if let Some(store) = &request.state.store {
        output.insert("store".to_owned(), store.clone());
    }
    if capabilities.cache.prompt_cache_key {
        if let Some(key) = &request.state.cache.key {
            output.insert("prompt_cache_key".to_owned(), Value::String(key.clone()));
        }
    }
    if capabilities.cache.prompt_cache_options {
        if let Some(options) = &request.state.cache.options {
            output.insert("prompt_cache_options".to_owned(), options.clone());
        }
        if let Some(retention) = &request.state.cache.retention {
            output.insert("prompt_cache_retention".to_owned(), retention.clone());
        }
    }
    if responses {
        if let Some(conversation) = &request.state.conversation {
            output.insert("conversation".to_owned(), conversation.clone());
        }
        if let Some(response_id) = &request.state.source_response_id {
            output.insert(
                "previous_response_id".to_owned(),
                Value::String(response_id.clone()),
            );
        }
        if let Some(include) = &request.state.include {
            output.insert("include".to_owned(), include.clone());
        }
        if let Some(truncation) = &request.state.truncation {
            output.insert("truncation".to_owned(), truncation.clone());
        }
        if let Some(user_id) = &request.state.identity.user_id {
            output.insert("user".to_owned(), Value::String(user_id.clone()));
        }
    } else if let Some(user_id) = request.state.cache.key.as_ref().or(request
        .state
        .identity
        .user_id
        .as_ref())
    {
        output.insert("user".to_owned(), Value::String(user_id.clone()));
    }
}

pub fn render_messages_state(
    request: &RequestIr,
    capabilities: &ModelCapabilities,
    output: &mut Map<String, Value>,
) {
    replay_state_extensions(request, output);
    if let Some(cache_control) = capabilities
        .cache
        .block_cache_control
        .then_some(request.state.cache.control.as_ref())
        .flatten()
    {
        output.insert("cache_control".to_owned(), cache_control.clone());
    }
    let user_id = request
        .state
        .cache
        .key
        .as_ref()
        .or(request.state.identity.user_id.as_ref());
    if request.state.metadata.is_some() || user_id.is_some() {
        let mut metadata = request
            .state
            .metadata
            .as_ref()
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        if let Some(user_id) = user_id {
            metadata
                .entry("user_id".to_owned())
                .or_insert_with(|| Value::String(user_id.clone()));
        }
        output.insert("metadata".to_owned(), Value::Object(metadata));
    }
}

fn replay_state_extensions(request: &RequestIr, output: &mut Map<String, Value>) {
    for extension in &request.state.extensions {
        if extension.scope == ExtensionScope::State
            && extension.is_replayable_as_opaque()
            && !is_canonical_state_field(&extension.key)
        {
            output
                .entry(extension.key.clone())
                .or_insert_with(|| extension.value.clone());
        }
    }
}

fn is_canonical_state_field(key: &str) -> bool {
    matches!(
        key,
        "conversation"
            | "previous_response_id"
            | "user"
            | "user_id"
            | "safety_identifier"
            | "metadata"
            | "prompt_cache_key"
            | "prompt_cache_options"
            | "prompt_cache_retention"
            | "cache_control"
            | "store"
            | "include"
            | "truncation"
    )
}

pub fn object(body: &Value) -> Result<&Map<String, Value>, ConversionError> {
    body.as_object()
        .ok_or_else(|| ConversionError::invalid("request body is not a JSON object"))
}

pub fn base_request(body: &Map<String, Value>, source_protocol: &str) -> RequestIr {
    RequestIr {
        model: body.get("model").and_then(Value::as_str).map(str::to_owned),
        stream: body.get("stream").and_then(Value::as_bool),
        generation: parse_generation(body),
        state: parse_state(body, source_protocol),
        tools: parse_tools(body),
        opaque: parse_opaque(body, source_protocol),
        ..RequestIr::default()
    }
}

pub fn parse_generation(body: &Map<String, Value>) -> GenerationControls {
    GenerationControls {
        temperature: body.get("temperature").cloned(),
        top_p: body.get("top_p").cloned(),
        max_output_tokens: body
            .get("max_output_tokens")
            .or_else(|| body.get("max_completion_tokens"))
            .or_else(|| body.get("max_tokens"))
            .cloned(),
        stop: body.get("stop").cloned(),
        stop_sequences: body.get("stop_sequences").cloned(),
        reasoning: body.get("reasoning").cloned(),
        reasoning_effort: body.get("reasoning_effort").cloned(),
        thinking: body.get("thinking").cloned(),
        text: body.get("text").cloned(),
        output_config: body.get("output_config").cloned(),
        parallel_tool_use: body.get("parallel_tool_use").cloned(),
        parallel_tool_calls: body.get("parallel_tool_calls").cloned(),
    }
}

pub fn parse_state(body: &Map<String, Value>, source_protocol: &str) -> ConversationState {
    let conversation = body.get("conversation").cloned();
    let user_id = body
        .get("user")
        .or_else(|| body.get("user_id"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            body.get("metadata")
                .and_then(|metadata| metadata.get("user_id"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
    let cache_key = body
        .get("prompt_cache_key")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| user_id.clone());
    let represented = body
        .iter()
        .filter(|(key, _)| is_state_extension(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    ConversationState {
        logical_conversation_id: conversation.as_ref().and_then(|value| {
            value
                .as_str()
                .or_else(|| value.get("id").and_then(Value::as_str))
                .map(str::to_owned)
        }),
        conversation,
        source_response_id: body
            .get("previous_response_id")
            .and_then(Value::as_str)
            .map(str::to_owned),
        target_response_ids: BTreeMap::new(),
        identity: Identity {
            conversation_id: None,
            user_id,
            session_id: represented.iter().find_map(|(key, value)| {
                key.to_ascii_lowercase()
                    .ends_with("session_id")
                    .then(|| value.as_str().map(str::to_owned))
                    .flatten()
            }),
            response_id: body
                .get("previous_response_id")
                .and_then(Value::as_str)
                .map(str::to_owned),
        },
        cache: CacheIdentity {
            key: cache_key,
            options: body.get("prompt_cache_options").cloned(),
            retention: body.get("prompt_cache_retention").cloned(),
            control: body.get("cache_control").cloned(),
        },
        safety_identifier: body
            .get("safety_identifier")
            .and_then(Value::as_str)
            .map(str::to_owned),
        metadata: body.get("metadata").cloned(),
        store: body.get("store").cloned(),
        include: body.get("include").cloned(),
        truncation: body.get("truncation").cloned(),
        extensions: represented
            .iter()
            .map(|(key, value)| {
                extension("state", key, value, ExtensionScope::State, source_protocol)
            })
            .collect(),
    }
}

pub fn parse_tools(body: &Map<String, Value>) -> Vec<ToolDefinition> {
    body.get("tools")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|tool| {
            let object = tool.as_object()?;
            let function = object.get("function").and_then(Value::as_object);
            let kind = object
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("function")
                .to_owned();
            let name = object
                .get("name")
                .or_else(|| function.and_then(|value| value.get("name")))
                .and_then(Value::as_str)
                .unwrap_or("tool")
                .to_owned();
            let description = object
                .get("description")
                .or_else(|| function.and_then(|value| value.get("description")))
                .and_then(Value::as_str)
                .map(str::to_owned);
            let input_schema = object
                .get("parameters")
                .or_else(|| object.get("input_schema"))
                .or_else(|| function.and_then(|value| value.get("parameters")))
                .cloned();
            Some(ToolDefinition {
                kind,
                name,
                description,
                input_schema,
                metadata: object.clone().into_iter().collect(),
                function_metadata: function.cloned().unwrap_or_default().into_iter().collect(),
                extensions: Vec::new(),
            })
        })
        .collect()
}

pub fn parse_parts(value: &Value) -> Vec<ContentPart> {
    match value {
        Value::String(text) => vec![ContentPart::Text(text.clone())],
        Value::Array(values) => values.iter().flat_map(parse_part).collect(),
        _ => parse_part(value),
    }
}

pub fn parse_part(value: &Value) -> Vec<ContentPart> {
    let Some(object) = value.as_object() else {
        return vec![ContentPart::Json(value.clone())];
    };
    match object.get("type").and_then(Value::as_str) {
        Some("text" | "input_text" | "output_text") => object
            .get("text")
            .and_then(Value::as_str)
            .map(|text| vec![ContentPart::Text(text.to_owned())])
            .unwrap_or_default(),
        Some("refusal") => object
            .get("refusal")
            .or_else(|| object.get("text"))
            .and_then(Value::as_str)
            .map(|text| vec![ContentPart::Refusal(text.to_owned())])
            .unwrap_or_default(),
        Some("thinking" | "reasoning") => vec![ContentPart::Reasoning {
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
        }],
        Some("image" | "input_image" | "image_url") => vec![ContentPart::Json(value.clone())],
        _ => vec![ContentPart::Json(value.clone())],
    }
}

pub fn parse_tool_call(object: &Map<String, Value>) -> ToolCall {
    let arguments = object
        .get("arguments")
        .or_else(|| object.get("input"))
        .cloned()
        .unwrap_or(Value::Object(Map::new()));
    ToolCall {
        id: object
            .get("id")
            .or_else(|| object.get("call_id"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        name: object
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("tool")
            .to_owned(),
        arguments: arguments
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| arguments.to_string()),
        status: object
            .get("status")
            .and_then(Value::as_str)
            .map(str::to_owned),
        metadata: object.clone().into_iter().collect(),
        function_metadata: object
            .get("function")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect(),
        extensions: Vec::new(),
    }
}

pub fn parse_tool_result(object: &Map<String, Value>) -> ToolResult {
    ToolResult {
        call_id: object
            .get("call_id")
            .or_else(|| object.get("tool_use_id"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        content: parse_parts(
            object
                .get("output")
                .or_else(|| object.get("content"))
                .unwrap_or(&Value::Null),
        ),
        is_error: object
            .get("is_error")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        extensions: Vec::new(),
    }
}

pub fn extension(
    namespace: &str,
    key: &str,
    value: &Value,
    scope: ExtensionScope,
    source_protocol: &str,
) -> Extension {
    Extension {
        namespace: namespace.to_owned(),
        key: key.to_owned(),
        value: value.clone(),
        scope,
        source_protocol: Some(source_protocol.to_owned()),
        provider: None,
        replay: ReplayPolicy::ReplayAsTargetOpaque,
    }
}

pub fn parse_opaque(body: &Map<String, Value>, source_protocol: &str) -> Vec<Extension> {
    let structural = BTreeSet::from([
        "model",
        "stream",
        "messages",
        "system",
        "input",
        "instructions",
        "tools",
        "tool_choice",
        "previous_response_id",
        "conversation",
        "user",
        "user_id",
        "safety_identifier",
        "metadata",
        "prompt_cache_key",
        "prompt_cache_options",
        "prompt_cache_retention",
        "cache_control",
        "store",
        "include",
        "truncation",
        "max_tokens",
        "max_completion_tokens",
        "max_output_tokens",
        "temperature",
        "top_p",
        "stop",
        "stop_sequences",
        "reasoning",
        "reasoning_effort",
        "thinking",
        "text",
        "output_config",
        "parallel_tool_use",
        "parallel_tool_calls",
    ]);
    body.iter()
        .filter(|(key, _)| !structural.contains(key.as_str()) && !is_state_extension(key))
        .map(|(key, value)| extension("wire", key, value, ExtensionScope::Request, source_protocol))
        .collect()
}

fn is_state_extension(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    lower.ends_with("_id") || matches!(lower.as_str(), "session_id" | "conversation_id")
}
