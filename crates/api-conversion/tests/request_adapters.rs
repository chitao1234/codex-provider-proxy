use std::fs;

use codex_provider_proxy_api_conversion::{
    dialect::{
        MaxTokensField, ModelCapabilities, ReasoningEffortConfig, ResponseFormatCap,
        ServerToolPolicy, ThinkingParam,
    },
    ir::{ContentItem, StateSlot},
    protocol::Protocol,
    request::{RequestParserKind, RequestRendererKind},
};
use serde_json::Value;

fn fixture(name: &str) -> Value {
    let path = format!(
        "{}/tests/fixtures/phase0/requests/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap()
}

#[test]
fn each_protocol_has_independent_request_parser_and_renderer() {
    let cases = [
        (Protocol::OpenAiResponses, "openai_responses"),
        (Protocol::OpenAiChatCompletions, "openai_chat_completions"),
        (Protocol::AnthropicMessages, "anthropic_messages"),
    ];

    for (protocol, fixture_name) in cases {
        let parser = RequestParserKind::for_protocol(protocol);
        assert_eq!(parser.protocol(), protocol);
        let request = parser.parse(&fixture(fixture_name)).unwrap();
        let rendered = RequestRendererKind::for_protocol(protocol)
            .render(&request, &ModelCapabilities::default())
            .unwrap();
        assert!(rendered.body.is_object());
        assert_eq!(rendered.body["model"], "phase0-model");
    }
}

#[test]
fn request_extensions_record_their_source_protocol() {
    for (protocol, source_protocol) in [
        (Protocol::OpenAiResponses, "openai_responses"),
        (Protocol::OpenAiChatCompletions, "openai_chat_completions"),
        (Protocol::AnthropicMessages, "anthropic_messages"),
    ] {
        let request = RequestParserKind::for_protocol(protocol)
            .parse(&serde_json::json!({
                "model": "m",
                "x_provider_parameter": true,
                "x_session_id": "session-1"
            }))
            .unwrap();
        assert_eq!(
            request.opaque[0].source_protocol.as_deref(),
            Some(source_protocol)
        );
        assert_eq!(
            request.state.extensions[0].source_protocol.as_deref(),
            Some(source_protocol)
        );
    }
}

#[test]
fn adapter_dispatch_preserves_responses_continuation_context() {
    let parser = RequestParserKind::OpenAiResponses;
    let previous = parser
        .parse(&serde_json::json!({
            "model": "m",
            "input": "first",
            "prompt_cache_key": "cache-1"
        }))
        .unwrap();
    let current = parser
        .parse_with_context(
            &serde_json::json!({
                "model": "m",
                "previous_response_id": "resp-1",
                "input": "second"
            }),
            Some(&previous),
            None,
        )
        .unwrap();
    let mut capabilities = ModelCapabilities::default();
    capabilities.cache.prompt_cache_key = true;
    let rendered = RequestRendererKind::OpenAiResponses
        .render(&current, &capabilities)
        .unwrap();

    assert_eq!(rendered.body["previous_response_id"], "resp-1");
    assert_eq!(rendered.body["prompt_cache_key"], "cache-1");
}

#[test]
fn responses_adapter_exposes_semantic_v2_request_without_messages_shape() {
    let request = RequestParserKind::OpenAiResponses
        .parse(&serde_json::json!({
            "model": "m",
            "input": [
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hello"}]},
                {"type": "reasoning", "summary": [{"type": "summary_text", "text": "think"}]},
                {"type": "function_call", "call_id": "call-1", "name": "lookup", "arguments": "{\"q\":\"rust\"}"}
            ],
            "prompt_cache_key": "cache-1",
            "x_client_extension": {"keep": true}
        }))
        .unwrap();

    assert_eq!(request.model.as_deref(), Some("m"));
    assert_eq!(request.transcript.len(), 3);
    assert_eq!(request.state.cache.key.as_deref(), Some("cache-1"));
    let extension = request
        .opaque
        .iter()
        .find(|extension| extension.key == "x_client_extension")
        .unwrap();
    assert_eq!(extension.value["keep"], true);
    assert_eq!(
        extension.source_protocol.as_deref(),
        Some("openai_responses")
    );
    assert!(matches!(
        request.transcript[0],
        codex_provider_proxy_api_conversion::ir::ContentItem::Message { .. }
    ));
    assert!(matches!(
        request.transcript[1],
        codex_provider_proxy_api_conversion::ir::ContentItem::Reasoning { .. }
    ));
    assert!(matches!(
        request.transcript[2],
        codex_provider_proxy_api_conversion::ir::ContentItem::ToolUse { .. }
    ));
}

#[test]
fn v2_state_rendering_respects_cache_and_identity_capabilities() {
    let parser = RequestParserKind::for_protocol(Protocol::OpenAiResponses);
    let request = parser
        .parse(&serde_json::json!({
            "model": "m",
            "input": "hello",
            "user": "end-user",
            "metadata": {"tenant": "workspace"},
            "safety_identifier": "safety-1",
            "prompt_cache_key": "cache-1",
            "prompt_cache_options": {"mode": "extended"},
            "prompt_cache_retention": "24h",
            "cache_control": {"type": "ephemeral"},
            "x_provider_session_id": "session-1",
            "store": false
        }))
        .unwrap();
    let mut capabilities = ModelCapabilities::default();
    capabilities.cache.prompt_cache_key = true;
    capabilities.cache.prompt_cache_options = true;
    capabilities.cache.block_cache_control = true;

    let responses = RequestRendererKind::OpenAiResponses
        .render(&request, &capabilities)
        .unwrap()
        .body;
    assert_eq!(responses["user"], "end-user");
    assert_eq!(responses["prompt_cache_key"], "cache-1");
    assert_eq!(responses["prompt_cache_options"]["mode"], "extended");
    assert_eq!(responses["prompt_cache_retention"], "24h");
    assert_eq!(responses["x_provider_session_id"], "session-1");
    assert_eq!(responses["safety_identifier"], "safety-1");
    assert_eq!(responses["metadata"]["tenant"], "workspace");
    assert_eq!(responses["store"], false);
    assert!(request.state.provenance.iter().any(|entry| {
        entry.slot == StateSlot::CacheKey
            && entry.source_protocol == "openai_responses"
            && entry.source_field == "prompt_cache_key"
    }));
    assert!(request.state.provenance.iter().any(|entry| {
        entry.slot == StateSlot::UserId
            && entry.source_protocol == "openai_responses"
            && entry.source_field == "user"
    }));

    let chat = RequestRendererKind::OpenAiChatCompletions
        .render(&request, &capabilities)
        .unwrap()
        .body;
    assert_eq!(chat["user"], "cache-1");
    assert_eq!(chat["prompt_cache_key"], "cache-1");
    assert_eq!(chat["prompt_cache_options"]["mode"], "extended");
    assert_eq!(chat["x_provider_session_id"], "session-1");
    assert_eq!(chat["safety_identifier"], "safety-1");

    let messages = RequestRendererKind::AnthropicMessages
        .render(&request, &capabilities)
        .unwrap()
        .body;
    assert_eq!(messages["metadata"]["tenant"], "workspace");
    assert_eq!(messages["metadata"]["user_id"], "cache-1");
    assert_eq!(messages["cache_control"]["type"], "ephemeral");
    assert_eq!(messages["x_provider_session_id"], "session-1");

    let disabled = RequestRendererKind::OpenAiResponses
        .render(&request, &ModelCapabilities::default())
        .unwrap()
        .body;
    assert!(disabled.get("prompt_cache_key").is_none());
    assert!(disabled.get("prompt_cache_options").is_none());
    assert!(disabled.get("prompt_cache_retention").is_none());
    assert!(disabled.get("cache_control").is_none());
}

#[test]
fn chat_v2_renderer_applies_generation_capabilities() {
    let parser = RequestParserKind::for_protocol(Protocol::OpenAiResponses);
    let request = parser
        .parse(&serde_json::json!({
            "model": "m",
            "stream": true,
            "input": "hello",
            "max_output_tokens": 500,
            "temperature": 0.4,
            "top_p": 0.8,
            "reasoning": {"effort": "xhigh"},
            "text": {"format": {"type": "json_schema", "name": "answer", "schema": {"type": "object"}, "strict": true}},
            "stop": ["END", ""],
            "parallel_tool_calls": false
        }))
        .unwrap();
    let capabilities = ModelCapabilities {
        max_tokens_field: MaxTokensField::MaxCompletionTokens,
        max_tokens_cap: Some(200),
        thinking_param: ThinkingParam::Adaptive,
        reasoning_effort: Some(ReasoningEffortConfig {
            enabled: true,
            levels: vec!["low".to_owned(), "high".to_owned()],
            default: None,
        }),
        response_format: ResponseFormatCap::JsonSchema,
        reasoning_split: Some(true),
        ..ModelCapabilities::default()
    };
    let rendered = RequestRendererKind::OpenAiChatCompletions
        .render(&request, &capabilities)
        .unwrap()
        .body;

    assert_eq!(rendered["max_completion_tokens"], 200);
    assert!(rendered.get("max_tokens").is_none());
    assert_eq!(rendered["temperature"], 0.4);
    assert_eq!(rendered["top_p"], 0.8);
    assert_eq!(rendered["thinking"]["type"], "adaptive");
    assert_eq!(rendered["reasoning_effort"], "high");
    assert_eq!(rendered["reasoning_split"], true);
    assert_eq!(rendered["stop"], "END");
    assert_eq!(rendered["stream_options"]["include_usage"], true);
    assert_eq!(rendered["parallel_tool_calls"], false);
    assert_eq!(rendered["response_format"]["type"], "json_schema");
    assert_eq!(rendered["response_format"]["json_schema"]["name"], "answer");
    assert_eq!(
        rendered["response_format"]["json_schema"]["schema"]["type"],
        "object"
    );

    let rejecting = ModelCapabilities {
        accept_stop: false,
        response_format: ResponseFormatCap::Text,
        ..capabilities
    };
    assert!(RequestRendererKind::OpenAiChatCompletions
        .render(&request, &rejecting)
        .is_err());
}

#[test]
fn messages_v2_renderer_maps_generation_controls_by_dialect() {
    let parser = RequestParserKind::for_protocol(Protocol::OpenAiResponses);
    let request = parser
        .parse(&serde_json::json!({
            "model": "m",
            "input": "hello",
            "temperature": 0.3,
            "top_p": 0.7,
            "reasoning": {"effort": "high"},
            "stop": ["END", ""],
            "max_output_tokens": 400
        }))
        .unwrap();
    let capabilities = ModelCapabilities {
        thinking_param: ThinkingParam::Adaptive,
        max_tokens_cap: Some(300),
        ..ModelCapabilities::default()
    };
    let rendered = RequestRendererKind::AnthropicMessages
        .render(&request, &capabilities)
        .unwrap()
        .body;

    assert_eq!(rendered["max_tokens"], 300);
    assert_eq!(rendered["temperature"], 0.3);
    assert_eq!(rendered["top_p"], 0.7);
    assert_eq!(rendered["thinking"]["type"], "adaptive");
    assert_eq!(rendered["stop_sequences"], serde_json::json!(["END"]));
    assert!(rendered.get("reasoning").is_none());

    let no_stop = ModelCapabilities {
        accept_stop: false,
        ..capabilities
    };
    let rendered = RequestRendererKind::AnthropicMessages
        .render(&request, &no_stop)
        .unwrap()
        .body;
    assert!(rendered.get("stop_sequences").is_none());
}

#[test]
fn responses_v2_renderer_preserves_reasoning_extensions_and_role_policy() {
    let parser = RequestParserKind::for_protocol(Protocol::OpenAiResponses);
    let request = parser
        .parse(&serde_json::json!({
            "model": "m",
            "input": [
                {"type": "message", "role": "developer", "content": [{"type": "input_text", "text": "policy"}]},
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hello"}]}
            ],
            "reasoning": {"effort": "high", "provider_mode": "deep"},
            "text": {"verbosity": "low", "format": {"type": "json_object"}},
            "temperature": 0.2,
            "top_p": 0.9,
            "stop": "END"
        }))
        .unwrap();
    let capabilities = ModelCapabilities {
        accept_developer_role: false,
        ..ModelCapabilities::default()
    };
    let rendered = RequestRendererKind::OpenAiResponses
        .render(&request, &capabilities)
        .unwrap()
        .body;

    assert_eq!(rendered["input"][0]["role"], "system");
    assert_eq!(rendered["reasoning"]["effort"], "high");
    assert_eq!(rendered["reasoning"]["provider_mode"], "deep");
    assert_eq!(rendered["text"]["verbosity"], "low");
    assert_eq!(rendered["text"]["format"]["type"], "json_object");
    assert_eq!(rendered["temperature"], 0.2);
    assert_eq!(rendered["top_p"], 0.9);
    assert_eq!(rendered["stop"], "END");
}

#[test]
fn v2_server_tool_policies_are_applied_before_protocol_rendering() {
    let parser = RequestParserKind::for_protocol(Protocol::AnthropicMessages);
    let request = parser
        .parse(&serde_json::json!({
            "model": "m",
            "messages": [{"role": "user", "content": "rust releases"}],
            "tools": [
                {"type": "web_search_20250305", "name": "web_search"},
                {"type": "web_fetch_20250910", "name": "web_fetch"},
                {"type": "code_execution_20250825", "name": "code_execution"},
                {"name": "lookup", "description": "Lookup", "input_schema": {"type": "object"}}
            ]
        }))
        .unwrap();

    let dropped = RequestRendererKind::OpenAiChatCompletions
        .render(&request, &ModelCapabilities::default())
        .unwrap();
    assert_eq!(dropped.body["tools"].as_array().unwrap().len(), 1);
    assert_eq!(dropped.report.dropped_server_tools.len(), 3);

    let mapped_capabilities = ModelCapabilities {
        server_tools: ServerToolPolicy::MapToFunction,
        ..ModelCapabilities::default()
    };
    let mapped = RequestRendererKind::OpenAiChatCompletions
        .render(&request, &mapped_capabilities)
        .unwrap();
    let mapped_tools = mapped.body["tools"].as_array().unwrap();
    assert_eq!(mapped_tools.len(), 3);
    assert_eq!(mapped_tools[0]["function"]["name"], "web_search");
    assert_eq!(mapped_tools[1]["function"]["name"], "web_fetch");
    assert_eq!(
        mapped.report.mapped_server_tools,
        ["web_search", "web_fetch"]
    );
    assert_eq!(mapped.report.dropped_server_tools, ["code_execution"]);

    let passthrough_capabilities = ModelCapabilities {
        server_tools: ServerToolPolicy::Passthrough,
        ..ModelCapabilities::default()
    };
    let passthrough = RequestRendererKind::OpenAiResponses
        .render(&request, &passthrough_capabilities)
        .unwrap();
    assert_eq!(passthrough.body["tools"][0]["type"], "web_search_20250305");
    assert_eq!(passthrough.body["tools"][1]["type"], "web_fetch_20250910");

    let native_capabilities = ModelCapabilities {
        server_tools: ServerToolPolicy::ProviderNative,
        search_tool_template: Some(serde_json::json!({
            "type": "web_search",
            "query": "{search_query}"
        })),
        fetch_request_params: Some(serde_json::json!({"enable_fetch": true})),
        code_interpreter_request_params: Some(serde_json::json!({
            "enable_code_interpreter": true
        })),
        always_enable_tools: vec!["WebFetch".to_owned()],
        ..ModelCapabilities::default()
    };
    let native = RequestRendererKind::OpenAiResponses
        .render(&request, &native_capabilities)
        .unwrap();
    assert_eq!(native.body["tools"].as_array().unwrap().len(), 2);
    assert_eq!(native.body["tools"][0]["type"], "web_search");
    assert_eq!(native.body["tools"][0]["query"], "rust releases");
    assert_eq!(native.body["enable_fetch"], true);
    assert_eq!(native.body["enable_code_interpreter"], true);
    assert_eq!(native.body["stream"], true);
    assert_eq!(
        native.report.mapped_server_tools,
        ["web_search", "web_fetch", "code_execution"]
    );
}

#[test]
fn nested_request_extensions_survive_protocol_translation() {
    let parser = RequestParserKind::for_protocol(Protocol::OpenAiChatCompletions);
    let request = parser
        .parse(&serde_json::json!({
            "model": "m",
            "messages": [
                {
                    "role": "user",
                    "content": [{"type":"text","text":"hello","x_part":true}],
                    "x_message": "message-extra"
                },
                {
                    "role": "assistant",
                    "tool_calls": [{
                        "id":"call-1",
                        "type":"function",
                        "x_call": "call-extra",
                        "function": {
                            "name":"lookup",
                            "arguments":"{}",
                            "x_function": "function-extra"
                        }
                    }]
                }
            ],
            "tools": [{
                "type":"function",
                "x_definition":"definition-extra",
                "function": {
                    "name":"lookup",
                    "description":"Lookup",
                    "parameters":{"type":"object"},
                    "x_function_definition":"function-definition-extra"
                }
            }]
        }))
        .unwrap();
    let rendered = RequestRendererKind::OpenAiResponses
        .render(&request, &ModelCapabilities::default())
        .unwrap()
        .body;
    assert_eq!(rendered["input"][0]["x_message"], "message-extra");
    assert_eq!(rendered["input"][0]["content"][0]["x_part"], true);
    assert_eq!(rendered["input"][1]["x_call"], "call-extra");
    assert_eq!(rendered["input"][1]["x_function"], "function-extra");
    assert_eq!(rendered["tools"][0]["x_definition"], "definition-extra");
    assert_eq!(
        rendered["tools"][0]["x_function_definition"],
        "function-definition-extra"
    );
    assert_eq!(rendered["input"][0]["role"], "user");
}

#[test]
fn chat_tool_messages_map_without_empty_assistant_items() {
    let parser = RequestParserKind::for_protocol(Protocol::OpenAiChatCompletions);
    let request = parser
        .parse(&serde_json::json!({
            "model": "m",
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [{
                        "id": "call-1",
                        "type": "function",
                        "function": {"name": "lookup", "arguments": "{}"}
                    }]
                },
                {
                    "role": "tool",
                    "tool_call_id": "call-1",
                    "content": "done"
                }
            ]
        }))
        .unwrap();

    assert_eq!(request.transcript.len(), 2);
    assert!(matches!(request.transcript[0], ContentItem::ToolUse { .. }));
    assert!(matches!(
        request.transcript[1],
        ContentItem::ToolResult { .. }
    ));
}
