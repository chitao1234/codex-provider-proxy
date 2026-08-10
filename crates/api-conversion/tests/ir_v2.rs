use codex_provider_proxy_api_conversion::ir::{
    CanonicalResponseEvent, ContentPart, Extension, ExtensionScope, ItemKind, ReplayPolicy,
    ResponseAccumulator, StopReason, ToolCall, Usage,
};

#[test]
fn unknown_stop_reason_survives_normalization() {
    let mut accumulator = ResponseAccumulator::default();
    accumulator.apply(CanonicalResponseEvent::StreamStarted {
        response_id: Some("resp-1".to_owned()),
        model: Some("model-1".to_owned()),
    });
    accumulator.apply(CanonicalResponseEvent::ResponseCompleted {
        stop_reason: StopReason::Unknown("provider_future_reason".to_owned()),
        original_reason: Some("provider_future_reason".to_owned()),
    });

    let snapshot = accumulator.snapshot();
    assert!(snapshot.terminal);
    assert!(!snapshot.failed);
    assert_eq!(
        snapshot.response.stop_reason,
        Some(StopReason::Unknown("provider_future_reason".to_owned()))
    );
    assert_eq!(
        snapshot.response.original_stop_reason.as_deref(),
        Some("provider_future_reason")
    );
    assert!(accumulator.continuation_state().is_some());
}

#[test]
fn accumulator_reconciles_tool_arguments_and_monotonic_usage() {
    let mut accumulator = ResponseAccumulator::default();
    accumulator.apply(CanonicalResponseEvent::ItemStarted {
        item_id: "call-1".to_owned(),
        provider_id: Some("call-1".to_owned()),
        item: ItemKind::ToolCall,
    });
    accumulator.apply(CanonicalResponseEvent::ToolCallStarted {
        item_id: "call-1".to_owned(),
        provider_id: Some("call-1".to_owned()),
        call: ToolCall {
            id: Some("call-1".to_owned()),
            name: "lookup".to_owned(),
            arguments: String::new(),
            status: Some("in_progress".to_owned()),
            metadata: Default::default(),
            function_metadata: Default::default(),
            extensions: Vec::new(),
        },
    });
    accumulator.apply(CanonicalResponseEvent::ToolArgumentsDelta {
        item_id: "call-1".to_owned(),
        arguments: "{\"q\":".to_owned(),
    });
    accumulator.apply(CanonicalResponseEvent::ToolArgumentsDelta {
        item_id: "call-1".to_owned(),
        arguments: "\"rust\"}".to_owned(),
    });
    accumulator.apply(CanonicalResponseEvent::UsageUpdated(Usage {
        input_tokens: Some(10),
        output_tokens: Some(4),
        total_tokens: Some(14),
        cached_input_tokens: None,
        ..Usage::default()
    }));
    accumulator.apply(CanonicalResponseEvent::UsageUpdated(Usage {
        input_tokens: Some(8),
        output_tokens: Some(6),
        total_tokens: Some(16),
        cached_input_tokens: Some(2),
        cache_write_input_tokens: Some(3),
        reasoning_tokens: Some(5),
        ..Usage::default()
    }));

    let response = accumulator.response();
    let tool = response.output.first().expect("tool item");
    match tool {
        codex_provider_proxy_api_conversion::ir::ContentItem::ToolUse { call, .. } => {
            assert_eq!(call.arguments, r#"{"q":"rust"}"#);
        }
        other => panic!("expected tool call, got {other:?}"),
    }
    assert_eq!(response.usage.input_tokens, Some(10));
    assert_eq!(response.usage.output_tokens, Some(6));
    assert_eq!(response.usage.total_tokens, Some(16));
    assert_eq!(response.usage.cached_input_tokens, Some(2));
    assert_eq!(response.usage.cache_write_input_tokens, Some(3));
    assert_eq!(response.usage.reasoning_tokens, Some(5));
}

#[test]
fn stream_error_cannot_become_successful_completion() {
    let mut accumulator = ResponseAccumulator::default();
    accumulator.apply(CanonicalResponseEvent::ResponseCompleted {
        stop_reason: StopReason::Natural,
        original_reason: Some("stop".to_owned()),
    });
    accumulator.apply(CanonicalResponseEvent::StreamError {
        message: "late transport failure".to_owned(),
        code: Some("broken_pipe".to_owned()),
    });

    let snapshot = accumulator.snapshot();
    assert!(snapshot.terminal);
    assert!(snapshot.failed);
    assert_eq!(snapshot.response.stop_reason, Some(StopReason::Error));
    assert!(accumulator.continuation_state().is_none());
}

#[test]
fn extensions_are_scoped_and_replay_policy_is_explicit() {
    let mut extension = Extension::new("provider", "trace_id", serde_json::json!("trace-1"));
    extension.scope = ExtensionScope::State;
    extension.replay = ReplayPolicy::ReplayAsTargetOpaque;

    let mut accumulator = ResponseAccumulator::default();
    accumulator.apply(CanonicalResponseEvent::ProtocolExtension(extension.clone()));
    let response = accumulator.response();
    assert_eq!(response.extensions, vec![extension]);
}

#[test]
fn content_delta_preserves_non_text_content() {
    let mut accumulator = ResponseAccumulator::default();
    accumulator.apply(CanonicalResponseEvent::ItemStarted {
        item_id: "message-1".to_owned(),
        provider_id: Some("message-1".to_owned()),
        item: ItemKind::Message,
    });
    accumulator.apply(CanonicalResponseEvent::ContentDelta {
        item_id: "message-1".to_owned(),
        content: ContentPart::Reasoning {
            text: "think".to_owned(),
            signature: Some("sig".to_owned()),
        },
    });

    let response = accumulator.response();
    match response.output.first().expect("message item") {
        codex_provider_proxy_api_conversion::ir::ContentItem::Message { content, .. } => {
            assert!(matches!(
                content.first(),
                Some(ContentPart::Reasoning { .. })
            ));
        }
        other => panic!("expected message item, got {other:?}"),
    }
}
