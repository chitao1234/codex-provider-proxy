use codex_provider_proxy_api_conversion::ir::{
    CanonicalResponseEvent, ContentPart, ItemKind, ResponseAccumulator, StopReason, ToolCall,
};

#[test]
fn shared_core_accumulates_stream_semantics() {
    let mut accumulator = ResponseAccumulator::default();
    for event in [
        CanonicalResponseEvent::StreamStarted {
            response_id: Some("resp-1".to_owned()),
            model: Some("model".to_owned()),
        },
        CanonicalResponseEvent::ItemStarted {
            item_id: "message".to_owned(),
            provider_id: None,
            item: ItemKind::Message,
        },
        CanonicalResponseEvent::ContentDelta {
            item_id: "message".to_owned(),
            content: ContentPart::Text("hello".to_owned()),
        },
        CanonicalResponseEvent::ToolCallStarted {
            item_id: "tool-0".to_owned(),
            provider_id: None,
            call: ToolCall {
                id: Some("call-1".to_owned()),
                name: "lookup".to_owned(),
                arguments: String::new(),
                status: None,
                metadata: Default::default(),
                function_metadata: Default::default(),
                extensions: Vec::new(),
            },
        },
        CanonicalResponseEvent::ToolArgumentsDelta {
            item_id: "tool-0".to_owned(),
            arguments: "{}".to_owned(),
        },
        CanonicalResponseEvent::ResponseCompleted {
            stop_reason: StopReason::ToolUse,
            original_reason: Some("tool_use".to_owned()),
        },
    ] {
        accumulator.apply(event);
    }
    let snapshot = accumulator.snapshot();

    assert!(snapshot.terminal);
    assert!(!snapshot.failed);
    assert_eq!(snapshot.response.response_id.as_deref(), Some("resp-1"));
    assert_eq!(snapshot.response.output.len(), 2);
    assert_eq!(snapshot.response.stop_reason, Some(StopReason::ToolUse));
}

#[test]
fn provider_item_id_can_arrive_after_content() {
    let mut accumulator = ResponseAccumulator::default();
    accumulator.apply(CanonicalResponseEvent::ContentDelta {
        item_id: "message".to_owned(),
        content: ContentPart::Text("hello".to_owned()),
    });
    accumulator.apply(CanonicalResponseEvent::ItemStarted {
        item_id: "message".to_owned(),
        provider_id: Some("msg-provider".to_owned()),
        item: ItemKind::Message,
    });

    assert!(matches!(
        accumulator.response().output.first(),
        Some(codex_provider_proxy_api_conversion::ir::ContentItem::Message {
            id: Some(id),
            ..
        }) if id == "msg-provider"
    ));
}
