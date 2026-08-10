use std::fs;

use codex_provider_proxy_api_conversion::{
    dialect::ModelCapabilities,
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
fn adapter_dispatch_preserves_responses_continuation_context() {
    let adapter = RequestAdapterKind::for_protocol(Protocol::OpenAiResponses);
    let previous = adapter
        .parse(&serde_json::json!({
            "model": "m",
            "input": "first",
            "prompt_cache_key": "cache-1"
        }))
        .unwrap();
    let current = adapter
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
    let rendered = adapter
        .render(&current, Protocol::OpenAiResponses, &capabilities)
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
    assert!(request.opaque.iter().any(|extension| {
        extension.key == "x_client_extension" && extension.value["keep"] == true
    }));
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
