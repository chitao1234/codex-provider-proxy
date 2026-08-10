use codex_provider_proxy_api_conversion::{
    ir::{
        ContentItem, ContentPart, Extension, ExtensionScope, ReplayPolicy, ResponseIr, StopReason,
        ToolResult,
    },
    protocol::Protocol,
    protocols::{parse_response, render_response},
};
use serde_json::json;

#[test]
fn responses_renderer_consumes_v2_without_chat_shape() {
    let response = ResponseIr {
        response_id: Some("resp-1".into()),
        model: Some("m".into()),
        output: vec![ContentItem::Message {
            id: Some("msg-1".into()),
            role: "assistant".into(),
            content: vec![ContentPart::Text("ok".into())],
            extensions: vec![],
        }],
        stop_reason: Some(StopReason::Natural),
        ..ResponseIr::default()
    };
    let rendered = render_response(Protocol::OpenAiResponses, &response).unwrap();
    assert_eq!(rendered["id"], "resp-1");
    assert_eq!(rendered["output"][0]["type"], "message");
    assert!(rendered.get("choices").is_none());
}

#[test]
fn messages_renderer_preserves_tool_json_input() {
    let response = ResponseIr {
        response_id: Some("msg-1".into()),
        output: vec![ContentItem::ToolUse {
            id: Some("call-1".into()),
            call: codex_provider_proxy_api_conversion::ir::ToolCall {
                id: Some("call-1".into()),
                name: "lookup".into(),
                arguments: "{\"q\":1}".into(),
                status: Some("completed".into()),
                metadata: Default::default(),
                function_metadata: Default::default(),
                extensions: vec![],
            },
        }],
        ..ResponseIr::default()
    };
    let rendered = render_response(Protocol::AnthropicMessages, &response).unwrap();
    assert_eq!(rendered["content"][0]["type"], "tool_use");
    assert_eq!(rendered["content"][0]["input"], json!({"q":1}));
}

#[test]
fn response_extension_replay_policy_controls_provider_fields() {
    let parsed = parse_response(
        Protocol::OpenAiResponses,
        &json!({
            "id": "resp-1",
            "status": "completed",
            "output": [],
            "x_provider_trace": "trace-1"
        }),
    )
    .unwrap();
    let rendered = render_response(Protocol::AnthropicMessages, &parsed).unwrap();
    assert_eq!(rendered["x_provider_trace"], "trace-1");

    let mut blocked = ResponseIr::default();
    let mut extension = Extension::new("provider", "x_blocked", json!(true));
    extension.replay = codex_provider_proxy_api_conversion::ir::ReplayPolicy::NeverReplay;
    blocked.extensions.push(extension);
    let rendered = render_response(Protocol::OpenAiResponses, &blocked).unwrap();
    assert!(rendered.get("x_blocked").is_none());

    let mut wrong_scope = ResponseIr::default();
    wrong_scope.extensions.push(Extension::scoped_opaque(
        "provider",
        "x_item_only",
        json!(true),
        ExtensionScope::ContentItem,
    ));
    let rendered = render_response(Protocol::OpenAiResponses, &wrong_scope).unwrap();
    assert!(rendered.get("x_item_only").is_none());
}

#[test]
fn item_and_tool_extensions_replay_at_their_target_scope() {
    let parsed = parse_response(
        Protocol::OpenAiChatCompletions,
        &json!({
            "id": "chat-1",
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": "ok",
                    "x_message": "message-meta",
                    "tool_calls": [{
                        "id": "call-1",
                        "type": "function",
                        "x_tool": "tool-meta",
                        "function": {
                            "name": "lookup",
                            "arguments": "{}",
                            "x_function": "function-meta"
                        }
                    }]
                },
                "finish_reason": "tool_calls"
            }]
        }),
    )
    .unwrap();

    let responses = render_response(Protocol::OpenAiResponses, &parsed).unwrap();
    assert_eq!(responses["output"][0]["x_message"], "message-meta");
    assert!(responses.get("x_message").is_none());
    assert_eq!(responses["output"][1]["x_tool"], "tool-meta");
    assert_eq!(responses["output"][1]["x_function"], "function-meta");

    let messages = render_response(Protocol::AnthropicMessages, &parsed).unwrap();
    assert_eq!(messages["x_message"], "message-meta");
    assert_eq!(messages["content"][1]["x_tool"], "tool-meta");
    assert_eq!(messages["content"][1]["x_function"], "function-meta");
}

#[test]
fn messages_renderer_preserves_future_and_tool_result_items() {
    let mut never_replay = Extension::scoped_opaque(
        "provider",
        "x_blocked",
        json!(true),
        ExtensionScope::ContentItem,
    );
    never_replay.replay = ReplayPolicy::NeverReplay;
    let response = ResponseIr {
        output: vec![
            ContentItem::Unknown {
                id: Some("future-1".into()),
                kind: "future_item".into(),
                payload: json!({"type":"future_item","vendor":{"a":1}}),
                extensions: vec![never_replay],
            },
            ContentItem::ToolResult {
                result: ToolResult {
                    call_id: Some("call-1".into()),
                    content: vec![ContentPart::Text("done".into())],
                    is_error: false,
                    extensions: vec![],
                },
            },
        ],
        ..ResponseIr::default()
    };
    let rendered = render_response(Protocol::AnthropicMessages, &response).unwrap();
    assert_eq!(rendered["content"][0]["type"], "future_item");
    assert_eq!(rendered["content"][0]["vendor"]["a"], 1);
    assert!(rendered["content"][0].get("x_blocked").is_none());
    assert_eq!(rendered["content"][1]["type"], "tool_result");
    assert_eq!(rendered["content"][1]["tool_use_id"], "call-1");
}
