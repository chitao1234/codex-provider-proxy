use codex_provider_proxy_api_conversion::{
    ir::{CanonicalResponseEvent, StopReason},
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
