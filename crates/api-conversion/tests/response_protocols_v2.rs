use codex_provider_proxy_api_conversion::{
    ir::{CanonicalResponseEvent, ContentItem, ResponseAccumulator, Usage},
    protocols::{anthropic_messages, openai_chat_completions, openai_responses, parse_response},
    request_ir::Protocol,
};
use serde_json::json;

#[test]
fn chat_response_parser_emits_v2_items_and_unknown_extensions() {
    let response = parse_response(Protocol::OpenAiChatCompletions, &json!({
        "id": "chat-1", "model": "m",
        "choices": [{"message": {"role": "assistant", "content": "ok", "x_vendor": true}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}
    })).unwrap();
    assert_eq!(response.response_id.as_deref(), Some("chat-1"));
    assert!(matches!(
        response.output.first(),
        Some(ContentItem::Message { .. })
    ));
    assert_eq!(response.usage.total_tokens, Some(3));
}

#[test]
fn responses_parser_keeps_future_items_as_unknown() {
    let response = parse_response(
        Protocol::OpenAiResponses,
        &json!({
            "id": "resp-1", "status": "completed",
            "output": [{"type": "future_item", "id": "f-1", "vendor": {"a": 1}}]
        }),
    )
    .unwrap();
    assert!(
        matches!(response.output.first(), Some(ContentItem::Unknown { kind, .. }) if kind == "future_item")
    );
}

#[test]
fn messages_parser_preserves_thinking_and_tool_use() {
    let response = parse_response(Protocol::AnthropicMessages, &json!({
        "id": "msg-1", "model": "m", "stop_reason": "tool_use",
        "content": [{"type": "thinking", "thinking": "plan", "signature": "sig"}, {"type": "tool_use", "id": "call-1", "name": "lookup", "input": {"q": 1}}]
    })).unwrap();
    assert_eq!(
        response.stop_reason,
        Some(codex_provider_proxy_api_conversion::ir::StopReason::ToolUse)
    );
    assert_eq!(response.output.len(), 2);
}

#[test]
fn chat_usage_sums_when_total_missing() {
    let response = parse_response(
        Protocol::OpenAiChatCompletions,
        &json!({"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5}}),
    )
    .unwrap();
    assert_eq!(response.usage.total_tokens, Some(15));
}

#[test]
fn chat_complete_and_streaming_usage_share_semantics() {
    let complete = parse_response(
        Protocol::OpenAiChatCompletions,
        &json!({
            "choices": [],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15,
                "prompt_tokens_details": {"cached_tokens": 4, "cache_write_tokens": 3},
                "prompt_cache_miss_tokens": 6,
                "completion_tokens_details": {"reasoning_tokens": 2}
            }
        }),
    )
    .unwrap();
    let mut parser = openai_chat_completions::stream::Parser::new();
    let mut events = Vec::new();
    parser
        .on_chunk(
            &json!({
                "id": "chat-usage",
                "choices": [{"delta": {}, "finish_reason": "stop"}],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 5,
                    "total_tokens": 15,
                    "prompt_tokens_details": {"cached_tokens": 4, "cache_write_tokens": 3},
                    "prompt_cache_miss_tokens": 6,
                    "completion_tokens_details": {"reasoning_tokens": 2}
                }
            })
            .to_string(),
            &mut events,
        )
        .unwrap();
    assert_eq!(accumulate_usage(events), complete.usage);
}

#[test]
fn messages_complete_and_streaming_usage_share_semantics() {
    let complete = parse_response(
        Protocol::AnthropicMessages,
        &json!({
            "id": "msg-usage",
            "content": [],
            "stop_reason": "end_turn",
            "usage": {
                "input_tokens": 10,
                "output_tokens": 5,
                "cache_read_input_tokens": 4,
                "cache_creation_input_tokens": 3,
                "output_tokens_details": {"thinking_tokens": 2}
            }
        }),
    )
    .unwrap();
    let mut parser = anthropic_messages::stream::Parser::new();
    let mut events = Vec::new();
    parser
        .on_event(
            "message_start",
            &json!({
                "message": {
                    "id": "msg-usage",
                    "usage": {"input_tokens": 10, "cache_read_input_tokens": 4}
                }
            })
            .to_string(),
            &mut events,
        )
        .unwrap();
    parser
        .on_event(
            "message_delta",
            &json!({
                "delta": {"stop_reason": "end_turn"},
                "usage": {
                    "output_tokens": 5,
                    "cache_creation_input_tokens": 3,
                    "output_tokens_details": {"thinking_tokens": 2}
                }
            })
            .to_string(),
            &mut events,
        )
        .unwrap();
    assert_eq!(accumulate_usage(events), complete.usage);
}

#[test]
fn responses_complete_and_streaming_usage_share_semantics() {
    let usage = json!({
        "input_tokens": 10,
        "output_tokens": 5,
        "total_tokens": 15,
        "input_tokens_details": {"cached_tokens": 4, "cache_write_tokens": 3},
        "output_tokens_details": {"reasoning_tokens": 2}
    });
    let complete = parse_response(
        Protocol::OpenAiResponses,
        &json!({
            "id": "resp-usage",
            "status": "completed",
            "output": [],
            "usage": usage
        }),
    )
    .unwrap();
    let mut parser = openai_responses::stream::Parser::new();
    let mut events = Vec::new();
    parser
        .on_event(
            "response.created",
            &json!({"response": {"id": "resp-usage", "status": "in_progress"}}).to_string(),
            &mut events,
        )
        .unwrap();
    parser
        .on_event(
            "response.completed",
            &json!({
                "response": {
                    "id": "resp-usage",
                    "status": "completed",
                    "usage": usage
                }
            })
            .to_string(),
            &mut events,
        )
        .unwrap();
    assert_eq!(accumulate_usage(events), complete.usage);
}

fn accumulate_usage(events: Vec<CanonicalResponseEvent>) -> Usage {
    let mut accumulator = ResponseAccumulator::default();
    for event in events {
        accumulator.apply(event);
    }
    let snapshot = accumulator.snapshot();
    assert!(snapshot.terminal);
    assert!(!snapshot.failed);
    snapshot.response.usage
}
