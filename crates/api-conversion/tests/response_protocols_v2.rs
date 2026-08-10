use codex_provider_proxy_api_conversion::{
    ir::ContentItem, protocols::parse_response, request_ir::Protocol,
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
