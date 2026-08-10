use std::fs;

use codex_provider_proxy_api_conversion::{
    chat_parser::ChatParser,
    dialect::ModelCapabilities,
    messages_parser::MessagesParser,
    request_ir::{Protocol, RequestIr},
    response_ir::ResponseIr,
    responses_parser::ResponsesParser,
    stream::{StopReason, StreamEvent},
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
    let cases = [
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
    ];

    for (protocol, path) in cases {
        let source = read_json(path);
        let ir = RequestIr::parse(protocol, &source).unwrap();
        let rendered = ir
            .render_with_report(protocol, &ModelCapabilities::default())
            .unwrap()
            .0;
        assert_eq!(rendered["x_client_field"]["keep"], true, "{path}");
    }
}

#[test]
fn phase0_request_matrix_has_basic_outputs_for_each_target_protocol() {
    let sources = [
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
    ];
    // Chat Completions is not a downstream target in the current conversion matrix.
    let targets = [Protocol::OpenAiResponses, Protocol::AnthropicMessages];

    for (source_protocol, path) in sources {
        let ir = RequestIr::parse(source_protocol, &read_json(path)).unwrap();
        for target_protocol in targets {
            let rendered = ir
                .render_with_report(target_protocol, &ModelCapabilities::default())
                .unwrap()
                .0;
            assert!(rendered.is_object(), "{path} -> {target_protocol:?}");
            assert_eq!(
                rendered["model"], "phase0-model",
                "{path} -> {target_protocol:?}"
            );
        }
    }
}

#[test]
fn phase0_response_fixtures_parse_with_stable_terminal_semantics() {
    let cases = [
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
    ];

    for (protocol, path) in cases {
        let response = ResponseIr::parse(protocol, &read_json(path)).unwrap();
        assert!(response.is_continuable(), "{path}");
        assert_eq!(response.stop_reason(), StopReason::Stop, "{path}");
        assert_eq!(response.last_reasoning(), None, "{path}");
        if protocol != Protocol::OpenAiChatCompletions {
            let rendered = response.render(protocol).unwrap();
            assert!(rendered.to_string().contains("hello"), "{path}");
        }
    }
}

#[test]
fn phase0_response_matrix_has_basic_outputs_for_supported_targets() {
    let sources = [
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
    ];
    let targets = [Protocol::OpenAiResponses, Protocol::AnthropicMessages];

    for (source_protocol, path) in sources {
        let response = ResponseIr::parse(source_protocol, &read_json(path)).unwrap();
        for target_protocol in targets {
            let rendered = response.render(target_protocol).unwrap();
            assert!(rendered.is_object(), "{path} -> {target_protocol:?}");
            assert!(
                rendered.to_string().contains("hello"),
                "{path} -> {target_protocol:?}"
            );
        }
    }
}

#[test]
fn phase0_state_and_cache_identity_mapping_is_explicit() {
    let previous = RequestIr::parse(
        Protocol::OpenAiResponses,
        &serde_json::json!({
            "model": "phase0-model",
            "input": "first",
            "prompt_cache_key": "phase0-cache"
        }),
    )
    .unwrap();
    let continued = RequestIr::parse_with_context(
        Protocol::OpenAiResponses,
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
    let responses = continued
        .render_with_report(Protocol::OpenAiResponses, &capabilities)
        .unwrap()
        .0;
    assert_eq!(responses["previous_response_id"], "resp_phase0");
    assert_eq!(responses["prompt_cache_key"], "phase0-cache");

    let messages = continued
        .render_with_report(Protocol::AnthropicMessages, &ModelCapabilities::default())
        .unwrap()
        .0;
    assert_eq!(messages["metadata"]["user_id"], "phase0-cache");
    assert!(messages.get("previous_response_id").is_none());
}

#[test]
fn phase0_stream_fixtures_reach_the_same_basic_terminal_shape() {
    let mut streams = Vec::new();

    let mut chat = ChatParser::new();
    let mut chat_events = Vec::new();
    for line in read_lines("tests/fixtures/phase0/streams/openai_chat_completions.jsonl") {
        chat.on_chunk(&line, &mut chat_events).unwrap();
    }
    streams.push(chat_events);

    let mut messages = MessagesParser::new();
    let mut message_events = Vec::new();
    for line in read_lines("tests/fixtures/phase0/streams/anthropic_messages.jsonl") {
        let event: Value = serde_json::from_str(&line).unwrap();
        messages
            .on_event(
                event["event_type"].as_str().unwrap(),
                &event["data"],
                &mut message_events,
            )
            .unwrap();
    }
    streams.push(message_events);

    let mut responses = ResponsesParser::new();
    let mut response_events = Vec::new();
    for line in read_lines("tests/fixtures/phase0/streams/openai_responses.jsonl") {
        let event: Value = serde_json::from_str(&line).unwrap();
        responses
            .on_event(
                event["event_type"].as_str().unwrap(),
                &event["data"],
                &mut response_events,
            )
            .unwrap();
    }
    streams.push(response_events);

    for events in streams {
        assert!(events
            .iter()
            .any(|event| matches!(event, StreamEvent::Start { .. })));
        assert!(events
            .iter()
            .any(|event| matches!(event, StreamEvent::TextDelta { text } if text == "hello")));
        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::End {
                stop_reason: StopReason::Stop,
                ..
            }
        )));
    }
}
