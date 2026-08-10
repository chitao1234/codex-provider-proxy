use std::fs;

use codex_provider_proxy_api_conversion::{
    dialect::{
        MaxTokensField, ModelCapabilities, ReasoningEffortConfig, ResponseFormatCap,
        ServerToolPolicy, ThinkingParam,
    },
    request_adapter::{RequestAdapter, RequestAdapterKind},
    request_ir::Protocol,
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
fn each_protocol_has_an_independent_request_adapter() {
    let cases = [
        (Protocol::OpenAiResponses, "openai_responses"),
        (Protocol::OpenAiChatCompletions, "openai_chat_completions"),
        (Protocol::AnthropicMessages, "anthropic_messages"),
    ];

    for (protocol, fixture_name) in cases {
        let adapter = RequestAdapterKind::for_protocol(protocol);
        assert_eq!(adapter.protocol(), protocol);
        let request = adapter.parse(&fixture(fixture_name)).unwrap();
        let rendered = adapter
            .render(&request, protocol, &ModelCapabilities::default())
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
        let adapter = RequestAdapterKind::for_protocol(protocol);
        let request = adapter
            .parse_v2(&serde_json::json!({
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
    let adapter = RequestAdapterKind::for_protocol(Protocol::OpenAiResponses);
    let previous = adapter
        .parse_v2(&serde_json::json!({
            "model": "m",
            "input": "first",
            "prompt_cache_key": "cache-1"
        }))
        .unwrap();
    let current = adapter
        .parse_v2_with_context(
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
    let rendered = adapter
        .render_v2(&current, Protocol::OpenAiResponses, &capabilities)
        .unwrap();

    assert_eq!(rendered.body["previous_response_id"], "resp-1");
    assert_eq!(rendered.body["prompt_cache_key"], "cache-1");
}

#[test]
fn responses_adapter_exposes_semantic_v2_request_without_messages_shape() {
    let adapter = RequestAdapterKind::for_protocol(Protocol::OpenAiResponses);
    let request = adapter
        .parse_v2(&serde_json::json!({
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
fn responses_v2_renderer_matches_legacy_fixture_shape() {
    for (protocol, fixture_name) in [
        (Protocol::OpenAiResponses, "openai_responses"),
        (Protocol::OpenAiChatCompletions, "openai_chat_completions"),
        (Protocol::AnthropicMessages, "anthropic_messages"),
    ] {
        let adapter = RequestAdapterKind::for_protocol(protocol);
        let source = fixture(fixture_name);
        let legacy = adapter
            .parse(&source)
            .unwrap()
            .render_with_report(Protocol::OpenAiResponses, &ModelCapabilities::default())
            .unwrap()
            .0;
        let v2 = adapter.parse_v2(&source).unwrap();
        let rendered = adapter
            .render_v2(
                &v2,
                Protocol::OpenAiResponses,
                &ModelCapabilities::default(),
            )
            .unwrap()
            .body;

        assert_eq!(rendered["model"], legacy["model"], "{fixture_name}");
        assert_eq!(rendered["stream"], legacy["stream"], "{fixture_name}");
        assert_eq!(rendered["input"], legacy["input"], "{fixture_name}");
    }
}

#[test]
fn messages_v2_renderer_preserves_identity_and_message_semantics() {
    let adapter = RequestAdapterKind::for_protocol(Protocol::AnthropicMessages);
    let source = fixture("anthropic_messages");
    let legacy = adapter
        .parse(&source)
        .unwrap()
        .render_with_report(Protocol::AnthropicMessages, &ModelCapabilities::default())
        .unwrap()
        .0;
    let v2 = adapter.parse_v2(&source).unwrap();
    let rendered = adapter
        .render_v2(
            &v2,
            Protocol::AnthropicMessages,
            &ModelCapabilities::default(),
        )
        .unwrap()
        .body;

    assert_eq!(rendered["model"], legacy["model"]);
    assert_eq!(rendered["messages"], legacy["messages"]);
    assert_eq!(rendered["metadata"], legacy["metadata"]);
}

#[test]
fn chat_v2_renderer_matches_cross_protocol_fixture_shape() {
    for (protocol, fixture_name) in [
        (Protocol::OpenAiResponses, "openai_responses"),
        (Protocol::AnthropicMessages, "anthropic_messages"),
    ] {
        let adapter = RequestAdapterKind::for_protocol(protocol);
        let source = fixture(fixture_name);
        let legacy = adapter
            .parse(&source)
            .unwrap()
            .render_with_report(
                Protocol::OpenAiChatCompletions,
                &ModelCapabilities::default(),
            )
            .unwrap()
            .0;
        let v2 = adapter.parse_v2(&source).unwrap();
        let rendered = adapter
            .render_v2(
                &v2,
                Protocol::OpenAiChatCompletions,
                &ModelCapabilities::default(),
            )
            .unwrap()
            .body;

        assert_eq!(rendered["model"], legacy["model"], "{fixture_name}");
        assert_eq!(rendered["messages"], legacy["messages"], "{fixture_name}");
    }
}

#[test]
fn v2_state_rendering_respects_cache_and_identity_capabilities() {
    let adapter = RequestAdapterKind::for_protocol(Protocol::OpenAiResponses);
    let request = adapter
        .parse_v2(&serde_json::json!({
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

    let responses = adapter
        .render_v2(&request, Protocol::OpenAiResponses, &capabilities)
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

    let chat = adapter
        .render_v2(&request, Protocol::OpenAiChatCompletions, &capabilities)
        .unwrap()
        .body;
    assert_eq!(chat["user"], "cache-1");
    assert_eq!(chat["prompt_cache_key"], "cache-1");
    assert_eq!(chat["prompt_cache_options"]["mode"], "extended");
    assert_eq!(chat["x_provider_session_id"], "session-1");
    assert_eq!(chat["safety_identifier"], "safety-1");

    let messages = adapter
        .render_v2(&request, Protocol::AnthropicMessages, &capabilities)
        .unwrap()
        .body;
    assert_eq!(messages["metadata"]["tenant"], "workspace");
    assert_eq!(messages["metadata"]["user_id"], "cache-1");
    assert_eq!(messages["cache_control"]["type"], "ephemeral");
    assert_eq!(messages["x_provider_session_id"], "session-1");

    let disabled = adapter
        .render_v2(
            &request,
            Protocol::OpenAiResponses,
            &ModelCapabilities::default(),
        )
        .unwrap()
        .body;
    assert!(disabled.get("prompt_cache_key").is_none());
    assert!(disabled.get("prompt_cache_options").is_none());
    assert!(disabled.get("prompt_cache_retention").is_none());
    assert!(disabled.get("cache_control").is_none());
}

#[test]
fn chat_v2_renderer_applies_generation_capabilities() {
    let adapter = RequestAdapterKind::for_protocol(Protocol::OpenAiResponses);
    let request = adapter
        .parse_v2(&serde_json::json!({
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
    let rendered = adapter
        .render_v2(&request, Protocol::OpenAiChatCompletions, &capabilities)
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
    assert!(adapter
        .render_v2(&request, Protocol::OpenAiChatCompletions, &rejecting,)
        .is_err());
}

#[test]
fn messages_v2_renderer_maps_generation_controls_by_dialect() {
    let adapter = RequestAdapterKind::for_protocol(Protocol::OpenAiResponses);
    let request = adapter
        .parse_v2(&serde_json::json!({
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
    let rendered = adapter
        .render_v2(&request, Protocol::AnthropicMessages, &capabilities)
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
    let rendered = adapter
        .render_v2(&request, Protocol::AnthropicMessages, &no_stop)
        .unwrap()
        .body;
    assert!(rendered.get("stop_sequences").is_none());
}

#[test]
fn responses_v2_renderer_preserves_reasoning_extensions_and_role_policy() {
    let adapter = RequestAdapterKind::for_protocol(Protocol::OpenAiResponses);
    let request = adapter
        .parse_v2(&serde_json::json!({
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
    let rendered = adapter
        .render_v2(&request, Protocol::OpenAiResponses, &capabilities)
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
    let adapter = RequestAdapterKind::for_protocol(Protocol::AnthropicMessages);
    let request = adapter
        .parse_v2(&serde_json::json!({
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

    let dropped = adapter
        .render_v2(
            &request,
            Protocol::OpenAiChatCompletions,
            &ModelCapabilities::default(),
        )
        .unwrap();
    assert_eq!(dropped.body["tools"].as_array().unwrap().len(), 1);
    assert_eq!(dropped.report.dropped_server_tools.len(), 3);

    let mapped_capabilities = ModelCapabilities {
        server_tools: ServerToolPolicy::MapToFunction,
        ..ModelCapabilities::default()
    };
    let mapped = adapter
        .render_v2(
            &request,
            Protocol::OpenAiChatCompletions,
            &mapped_capabilities,
        )
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
    let passthrough = adapter
        .render_v2(
            &request,
            Protocol::OpenAiResponses,
            &passthrough_capabilities,
        )
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
    let native = adapter
        .render_v2(&request, Protocol::OpenAiResponses, &native_capabilities)
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
