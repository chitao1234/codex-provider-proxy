use std::collections::BTreeMap;

use codex_provider_proxy_api_conversion::{
    ir::{
        CanonicalResponseEvent, ContentPart, ItemKind, ResponseAccumulator, StopReason, ToolCall,
        Usage,
    },
    protocols::anthropic_messages::stream::Renderer,
};
use serde_json::json;

#[test]
fn messages_stream_renderer_tracks_multiple_items_and_tool_metadata() {
    let mut renderer = Renderer::new();
    let mut accumulator = ResponseAccumulator::default();
    let mut output = Vec::new();
    let events = [
        CanonicalResponseEvent::StreamStarted {
            response_id: Some("response-1".into()),
            model: Some("model-1".into()),
        },
        CanonicalResponseEvent::UsageUpdated(Usage {
            input_tokens: Some(3),
            ..Usage::default()
        }),
        CanonicalResponseEvent::ItemStarted {
            item_id: "reasoning-1".into(),
            provider_id: Some("reasoning-1".into()),
            item: ItemKind::Reasoning,
        },
        CanonicalResponseEvent::ContentDelta {
            item_id: "reasoning-1".into(),
            content: ContentPart::Reasoning {
                text: "plan".into(),
                signature: None,
            },
        },
        CanonicalResponseEvent::ItemCompleted {
            item_id: "reasoning-1".into(),
        },
        CanonicalResponseEvent::ItemStarted {
            item_id: "message-1".into(),
            provider_id: Some("message-1".into()),
            item: ItemKind::Message,
        },
        CanonicalResponseEvent::ContentDelta {
            item_id: "message-1".into(),
            content: ContentPart::Text("answer".into()),
        },
        CanonicalResponseEvent::ItemCompleted {
            item_id: "message-1".into(),
        },
        CanonicalResponseEvent::ToolCallStarted {
            item_id: "tool-1".into(),
            provider_id: Some("tool-1".into()),
            call: ToolCall {
                id: Some("call-1".into()),
                name: "lookup".into(),
                arguments: String::new(),
                status: Some("in_progress".into()),
                metadata: BTreeMap::from([("x_tool".into(), json!(true))]),
                function_metadata: BTreeMap::new(),
                extensions: Vec::new(),
            },
        },
        CanonicalResponseEvent::ToolArgumentsDelta {
            item_id: "tool-1".into(),
            arguments: "{\"q\":1}".into(),
        },
        CanonicalResponseEvent::ItemCompleted {
            item_id: "tool-1".into(),
        },
        CanonicalResponseEvent::UsageUpdated(Usage {
            input_tokens: Some(3),
            output_tokens: Some(2),
            total_tokens: Some(5),
            ..Usage::default()
        }),
        CanonicalResponseEvent::ResponseCompleted {
            stop_reason: StopReason::ToolUse,
            original_reason: Some("tool_calls".into()),
        },
    ];
    for event in events {
        let update = accumulator.apply(event);
        renderer.on_update(&update, &mut output);
    }

    let rendered = String::from_utf8(
        output
            .into_iter()
            .flat_map(|chunk| chunk.to_vec())
            .collect(),
    )
    .unwrap();
    assert!(rendered.contains("event: message_start"));
    assert!(rendered.contains("\"input_tokens\":3"));
    assert!(rendered.contains("\"index\":0"));
    assert!(rendered.contains("\"index\":1"));
    assert!(rendered.contains("\"index\":2"));
    assert!(rendered.contains("\"name\":\"lookup\""));
    assert!(rendered.contains("\"x_tool\":true"));
    assert!(rendered.contains("\"partial_json\":\"{\\\"q\\\":1}\""));
    assert_eq!(rendered.matches("event: content_block_stop").count(), 3);
    assert!(rendered.contains("\"output_tokens\":2"));
    assert!(rendered.contains("\"stop_reason\":\"tool_use\""));
    assert_eq!(rendered.matches("event: message_stop").count(), 1);
}

#[test]
fn messages_stream_renderer_preserves_unknown_stop_reason() {
    let mut renderer = Renderer::new();
    let mut accumulator = ResponseAccumulator::default();
    let mut output = Vec::new();
    for event in [
        CanonicalResponseEvent::StreamStarted {
            response_id: Some("response-1".into()),
            model: Some("model-1".into()),
        },
        CanonicalResponseEvent::ResponseCompleted {
            stop_reason: StopReason::Unknown("future_stop".into()),
            original_reason: Some("future_stop".into()),
        },
    ] {
        let update = accumulator.apply(event);
        renderer.on_update(&update, &mut output);
    }

    let rendered = String::from_utf8(
        output
            .into_iter()
            .flat_map(|chunk| chunk.to_vec())
            .collect(),
    )
    .unwrap();
    assert!(rendered.contains("\"stop_reason\":\"future_stop\""));
}
