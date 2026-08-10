use std::fs;

use codex_provider_proxy_api_conversion::{
    dialect::ModelCapabilities,
    ir::{CanonicalResponseEvent, ContentPart, ResponseAccumulator, ResponseIr, StopReason},
    protocols::{self, anthropic_messages, openai_chat_completions, openai_responses},
    request_adapter::{RequestAdapter, RequestAdapterKind},
    request_ir::Protocol,
};
use serde_json::Value;

fn read_json(path: &str) -> Value {
    let path = format!("{}/{}", env!("CARGO_MANIFEST_DIR"), path);
    serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap()
}

fn read_lines(path: &str) -> Vec<String> {
    let path = format!("{}/{}", env!("CARGO_MANIFEST_DIR"), path);
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect()
}

#[test]
fn phase0_request_fixtures_parse_and_preserve_unknown_fields() {
    for (protocol, path) in request_cases() {
        let adapter = RequestAdapterKind::for_protocol(protocol);
        let request = adapter.parse_v2(&read_json(path)).unwrap();
        let rendered = adapter
            .render_v2(&request, protocol, &ModelCapabilities::default())
            .unwrap()
            .body;
        assert_eq!(rendered["x_client_field"]["keep"], true, "{path}");
    }
}

#[test]
fn phase0_request_matrix_has_basic_outputs_for_each_target_protocol() {
    let targets = [Protocol::OpenAiResponses, Protocol::AnthropicMessages];
    for (source_protocol, path) in request_cases() {
        let adapter = RequestAdapterKind::for_protocol(source_protocol);
        let request = adapter.parse_v2(&read_json(path)).unwrap();
        for target in targets {
            let rendered = adapter
                .render_v2(&request, target, &ModelCapabilities::default())
                .unwrap()
                .body;
            assert_eq!(rendered["model"], "phase0-model", "{path} -> {target:?}");
        }
    }
}

#[test]
fn phase0_response_fixtures_parse_with_stable_terminal_semantics() {
    for (protocol, path) in response_cases() {
        let response = protocols::parse_response(protocol, &read_json(path)).unwrap();
        assert_eq!(response.stop_reason, Some(StopReason::Natural), "{path}");
        assert!(response.error.is_none(), "{path}");
        assert!(
            response
                .output
                .iter()
                .any(|item| item_has_text(item, "hello")),
            "{path}"
        );
        if protocol != Protocol::OpenAiChatCompletions {
            let rendered = protocols::render_response(protocol, &response).unwrap();
            assert!(rendered.to_string().contains("hello"), "{path}");
        }
    }
}

#[test]
fn phase0_response_matrix_has_basic_outputs_for_supported_targets() {
    let targets = [Protocol::OpenAiResponses, Protocol::AnthropicMessages];
    for (source_protocol, path) in response_cases() {
        let response = protocols::parse_response(source_protocol, &read_json(path)).unwrap();
        for target in targets {
            let rendered = protocols::render_response(target, &response).unwrap();
            assert!(
                rendered.to_string().contains("hello"),
                "{path} -> {target:?}"
            );
        }
    }
}

#[test]
fn phase0_state_and_cache_identity_mapping_is_explicit() {
    let adapter = RequestAdapterKind::for_protocol(Protocol::OpenAiResponses);
    let previous = adapter
        .parse_v2(&serde_json::json!({
            "model": "phase0-model",
            "input": "first",
            "prompt_cache_key": "phase0-cache"
        }))
        .unwrap();
    let continued = adapter
        .parse_v2_with_context(
            &serde_json::json!({
                "model": "phase0-model",
                "previous_response_id": "resp_phase0",
                "input": "second"
            }),
            Some(&previous),
            None,
        )
        .unwrap();

    let mut capabilities = ModelCapabilities::default();
    capabilities.cache.prompt_cache_key = true;
    let responses = adapter
        .render_v2(&continued, Protocol::OpenAiResponses, &capabilities)
        .unwrap()
        .body;
    assert_eq!(responses["previous_response_id"], "resp_phase0");
    assert_eq!(responses["prompt_cache_key"], "phase0-cache");

    let messages = adapter
        .render_v2(
            &continued,
            Protocol::AnthropicMessages,
            &ModelCapabilities::default(),
        )
        .unwrap()
        .body;
    assert_eq!(messages["metadata"]["user_id"], "phase0-cache");
    assert!(messages.get("previous_response_id").is_none());
}

#[test]
fn phase0_stream_fixtures_reach_the_same_basic_terminal_shape() {
    for protocol in response_protocols() {
        let events = stream_events(protocol);
        assert!(events
            .iter()
            .any(|event| matches!(event, CanonicalResponseEvent::StreamStarted { .. })));
        assert!(events.iter().any(|event| matches!(event, CanonicalResponseEvent::ContentDelta { content: ContentPart::Text(text), .. } if text == "hello")));
        assert!(events.iter().any(|event| matches!(
            event,
            CanonicalResponseEvent::ResponseCompleted {
                stop_reason: StopReason::Natural,
                ..
            }
        )));
    }
}

#[test]
fn phase0_complete_and_streaming_fixtures_share_final_semantics() {
    for (protocol, path) in response_cases() {
        let complete = protocols::parse_response(protocol, &read_json(path)).unwrap();
        let streamed = accumulate(stream_events(protocol));
        assert_eq!(streamed, complete, "{protocol:?}");
    }
}

fn stream_events(protocol: Protocol) -> Vec<CanonicalResponseEvent> {
    match protocol {
        Protocol::OpenAiChatCompletions => {
            let mut parser = openai_chat_completions::stream::Parser::new();
            let mut events = Vec::new();
            for line in read_lines("tests/fixtures/phase0/streams/openai_chat_completions.jsonl") {
                parser.on_chunk(&line, &mut events).unwrap();
            }
            events
        }
        Protocol::AnthropicMessages => {
            let mut parser = anthropic_messages::stream::Parser::new();
            parse_typed_stream(
                &mut |event_type, payload, events| parser.on_event(event_type, payload, events),
                "tests/fixtures/phase0/streams/anthropic_messages.jsonl",
            )
        }
        Protocol::OpenAiResponses => {
            let mut parser = openai_responses::stream::Parser::new();
            parse_typed_stream(
                &mut |event_type, payload, events| parser.on_event(event_type, payload, events),
                "tests/fixtures/phase0/streams/openai_responses.jsonl",
            )
        }
    }
}

fn parse_typed_stream(
    parser: &mut impl FnMut(
        &str,
        &str,
        &mut Vec<CanonicalResponseEvent>,
    )
        -> Result<(), codex_provider_proxy_api_conversion::error::ConversionError>,
    path: &str,
) -> Vec<CanonicalResponseEvent> {
    let mut events = Vec::new();
    for line in read_lines(path) {
        let event: Value = serde_json::from_str(&line).unwrap();
        parser(
            event["event_type"].as_str().unwrap(),
            &event["data"].to_string(),
            &mut events,
        )
        .unwrap();
    }
    events
}

fn accumulate(events: Vec<CanonicalResponseEvent>) -> ResponseIr {
    let mut accumulator = ResponseAccumulator::default();
    for event in events {
        accumulator.apply(event);
    }
    let snapshot = accumulator.snapshot();
    assert!(snapshot.terminal);
    assert!(!snapshot.failed);
    snapshot.response
}

fn request_cases() -> [(Protocol, &'static str); 3] {
    [
        (
            Protocol::OpenAiResponses,
            "tests/fixtures/phase0/requests/openai_responses.json",
        ),
        (
            Protocol::OpenAiChatCompletions,
            "tests/fixtures/phase0/requests/openai_chat_completions.json",
        ),
        (
            Protocol::AnthropicMessages,
            "tests/fixtures/phase0/requests/anthropic_messages.json",
        ),
    ]
}

fn response_cases() -> [(Protocol, &'static str); 3] {
    [
        (
            Protocol::OpenAiResponses,
            "tests/fixtures/phase0/responses/openai_responses.json",
        ),
        (
            Protocol::OpenAiChatCompletions,
            "tests/fixtures/phase0/responses/openai_chat_completions.json",
        ),
        (
            Protocol::AnthropicMessages,
            "tests/fixtures/phase0/responses/anthropic_messages.json",
        ),
    ]
}

fn response_protocols() -> [Protocol; 3] {
    [
        Protocol::OpenAiResponses,
        Protocol::OpenAiChatCompletions,
        Protocol::AnthropicMessages,
    ]
}

fn item_has_text(
    item: &codex_provider_proxy_api_conversion::ir::ContentItem,
    expected: &str,
) -> bool {
    match item {
        codex_provider_proxy_api_conversion::ir::ContentItem::Message { content, .. }
        | codex_provider_proxy_api_conversion::ir::ContentItem::Refusal { content, .. }
        | codex_provider_proxy_api_conversion::ir::ContentItem::Reasoning { content, .. } => {
            content
                .iter()
                .any(|part| matches!(part, ContentPart::Text(text) if text == expected))
        }
        _ => false,
    }
}
