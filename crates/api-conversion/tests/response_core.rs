use codex_provider_proxy_api_conversion::{
    response_adapter::accumulate_stream,
    stream::{StopReason, StreamEvent},
};

#[test]
fn shared_core_accumulates_stream_semantics() {
    let snapshot = accumulate_stream([
        StreamEvent::Start {
            id: "resp-1".to_owned(),
            model: "model".to_owned(),
        },
        StreamEvent::TextDelta {
            text: "hello".to_owned(),
        },
        StreamEvent::ToolCallStart {
            index: 0,
            id: "call-1".to_owned(),
            name: "lookup".to_owned(),
            opaque_extensions: Default::default(),
            function_extensions: Default::default(),
        },
        StreamEvent::ToolCallArgsDelta {
            index: 0,
            args: "{}".to_owned(),
        },
        StreamEvent::End {
            stop_reason: StopReason::ToolUse,
            protocol_reason: Some("tool_use".to_owned()),
            usage: Default::default(),
        },
    ]);

    assert!(snapshot.terminal);
    assert!(!snapshot.failed);
    assert_eq!(snapshot.response.response_id.as_deref(), Some("resp-1"));
    assert_eq!(snapshot.response.output.len(), 2);
    assert_eq!(
        snapshot.response.stop_reason,
        Some(codex_provider_proxy_api_conversion::ir::StopReason::ToolUse)
    );
}
