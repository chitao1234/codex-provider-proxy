use codex_provider_proxy_api_conversion::{
    ir::{ContentItem, ContentPart, Extension, ResponseIr, StopReason},
    protocols::{parse_response, render_response},
    request_ir::Protocol,
};
use serde_json::json;

#[test]
fn responses_renderer_consumes_v2_without_chat_shape() {
    let response = ResponseIr {
        response_id: Some("resp-1".into()),
        model: Some("m".into()),
        output: vec![ContentItem::Message {
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
}
