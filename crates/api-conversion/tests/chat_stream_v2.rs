use codex_provider_proxy_api_conversion::{
    ir::{CanonicalResponseEvent, ContentItem, ExtensionScope, ResponseAccumulator, StopReason},
    protocols::openai_chat_completions::stream::Parser,
};

#[test]
fn chat_v2_stream_parser_emits_native_canonical_events() {
    let mut parser = Parser::new();
    let mut events = Vec::new();
    parser
        .on_chunk(
            r#"{"id":"chat-1","model":"m","choices":[{"delta":{"content":"hi"}}]}"#,
            &mut events,
        )
        .unwrap();
    parser.on_chunk(r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","function":{"name":"lookup","arguments":"{}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":1,"completion_tokens":2}}"#, &mut events).unwrap();
    assert!(events.iter().any(|event| matches!(event, CanonicalResponseEvent::StreamStarted { response_id, .. } if response_id.as_deref() == Some("chat-1"))));
    assert!(events
        .iter()
        .any(|event| matches!(event, CanonicalResponseEvent::ContentDelta { .. })));
    assert!(events
        .iter()
        .any(|event| matches!(event, CanonicalResponseEvent::ToolCallStarted { .. })));
    assert!(events.iter().any(|event| matches!(
        event,
        CanonicalResponseEvent::ResponseCompleted {
            stop_reason: StopReason::ToolUse,
            ..
        }
    )));
}

#[test]
fn chat_stream_tool_extensions_are_scoped_without_duplicate_starts() {
    let mut parser = Parser::new();
    let mut events = Vec::new();
    parser
        .on_chunk(
            r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","x_call":"call-meta","function":{"name":"lookup","arguments":"","x_function":"function-meta"}}]}}]}"#,
            &mut events,
        )
        .unwrap();
    parser
        .on_chunk(
            r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","x_late":true,"function":{"name":"lookup","arguments":""}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"x_usage":"usage-meta"}}"#,
            &mut events,
        )
        .unwrap();

    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, CanonicalResponseEvent::ToolCallStarted { .. }))
            .count(),
        1
    );
    assert!(events.iter().any(|event| matches!(
        event,
        CanonicalResponseEvent::ItemMetadataUpdated { metadata, .. }
            if metadata.iter().any(|extension| extension.key == "x_late")
    )));

    let mut accumulator = ResponseAccumulator::default();
    for event in events {
        accumulator.apply(event);
    }
    let ContentItem::ToolUse { call, .. } = &accumulator.response().output[0] else {
        panic!("expected tool call");
    };
    assert!(call.extensions.iter().any(|extension| {
        extension.scope == ExtensionScope::ToolCall && extension.key == "x_call"
    }));
    assert!(call.extensions.iter().any(|extension| {
        extension.scope == ExtensionScope::ToolFunction && extension.key == "x_function"
    }));
    assert!(call.extensions.iter().any(|extension| {
        extension.scope == ExtensionScope::ToolCall && extension.key == "x_late"
    }));
    assert!(accumulator
        .response()
        .usage
        .extensions
        .iter()
        .any(|extension| extension.key == "x_usage"));
}
