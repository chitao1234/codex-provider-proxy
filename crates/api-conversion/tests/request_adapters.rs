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
