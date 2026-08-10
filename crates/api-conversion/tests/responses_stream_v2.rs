use std::collections::BTreeMap;

use codex_provider_proxy_api_conversion::{
    ir::{
        CanonicalResponseEvent, ContentItem, ContentPart, Extension, ExtensionScope, ItemKind,
        ReplayPolicy, ResponseAccumulator, StopReason, ToolCall, Usage,
    },
    protocols::openai_responses::stream::{Parser, Renderer},
    sse,
};
use serde_json::{json, Value};

#[test]
fn responses_stream_renderer_preserves_item_lifecycle_and_final_snapshot() {
    let mut accumulator = ResponseAccumulator::default();
    let mut renderer = Renderer::new();
    let mut output = Vec::new();
    let events = [
        CanonicalResponseEvent::StreamStarted {
            response_id: Some("response-1".into()),
            model: Some("model-1".into()),
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
            arguments: "{\"q\":".into(),
        },
        CanonicalResponseEvent::ToolArgumentsDelta {
            item_id: "tool-1".into(),
            arguments: "1}".into(),
        },
        CanonicalResponseEvent::ItemMetadataUpdated {
            item_id: "tool-1".into(),
            metadata: vec![Extension {
                namespace: "provider.future".into(),
                key: "x_future".into(),
                value: json!("kept"),
                scope: ExtensionScope::ToolCall,
                source_protocol: Some("provider_future".into()),
                provider: Some("future".into()),
                replay: ReplayPolicy::ReplayAsTargetOpaque,
            }],
        },
        CanonicalResponseEvent::ItemCompleted {
            item_id: "tool-1".into(),
        },
        CanonicalResponseEvent::ItemCompleted {
            item_id: "tool-1".into(),
        },
        CanonicalResponseEvent::OpaqueItemStarted {
            item_id: "future-1".into(),
            kind: "future_item".into(),
            payload: json!({"type":"future_item","provider_data":{"n":1}}),
        },
        CanonicalResponseEvent::UsageUpdated(Usage {
            input_tokens: Some(3),
            output_tokens: Some(2),
            total_tokens: Some(5),
            cached_input_tokens: Some(1),
            reasoning_tokens: Some(1),
            ..Usage::default()
        }),
        CanonicalResponseEvent::ResponseCompleted {
            stop_reason: StopReason::Unknown("future_stop".into()),
            original_reason: Some("future_stop".into()),
        },
    ];

    for event in events {
        let update = accumulator.apply(event);
        renderer.on_update(&update, &mut output);
    }

    let events = decode_events(&output);
    let added = events_of_type(&events, "response.output_item.added");
    assert_eq!(added.len(), 3);
    assert_eq!(added[0]["output_index"], 0);
    assert_eq!(added[0]["item"]["id"], "message-1");
    assert_eq!(added[1]["output_index"], 1);
    assert_eq!(added[1]["item"]["call_id"], "call-1");
    assert_eq!(added[1]["item"]["name"], "lookup");
    assert_eq!(added[1]["item"]["x_tool"], true);
    assert_eq!(added[2]["output_index"], 2);
    assert_eq!(added[2]["item"]["type"], "future_item");
    assert_eq!(added[2]["item"]["provider_data"]["n"], 1);

    let argument_deltas = events_of_type(&events, "response.function_call_arguments.delta");
    assert_eq!(argument_deltas.len(), 2);
    assert_eq!(argument_deltas[0]["delta"], "{\"q\":");
    assert_eq!(argument_deltas[1]["delta"], "1}");

    let done = events_of_type(&events, "response.output_item.done");
    assert_eq!(done.len(), 3);
    let tool_done = done
        .iter()
        .find(|event| event["item"]["id"] == "tool-1")
        .unwrap();
    assert_eq!(tool_done["item"]["arguments"], "{\"q\":1}");
    assert_eq!(tool_done["item"]["x_future"], "kept");

    let completed = events_of_type(&events, "response.completed");
    assert_eq!(completed.len(), 1);
    assert_eq!(
        completed[0]["response"]["output"].as_array().unwrap().len(),
        3
    );
    assert_eq!(completed[0]["response"]["output"][0]["id"], "message-1");
    assert_eq!(completed[0]["response"]["usage"]["total_tokens"], 5);
    assert_eq!(
        completed[0]["response"]["usage"]["input_tokens_details"]["cached_tokens"],
        1
    );
    assert_eq!(completed[0]["response"]["stop_reason"], "future_stop");
    assert_eq!(events_of_type(&events, "response.done").len(), 1);
}

#[test]
fn responses_stream_error_never_fabricates_success() {
    let mut accumulator = ResponseAccumulator::default();
    let mut renderer = Renderer::new();
    let mut output = Vec::new();
    for event in [
        CanonicalResponseEvent::StreamStarted {
            response_id: Some("response-1".into()),
            model: Some("model-1".into()),
        },
        CanonicalResponseEvent::StreamError {
            message: "failed".into(),
            code: Some("provider_error".into()),
        },
    ] {
        let update = accumulator.apply(event);
        renderer.on_update(&update, &mut output);
    }

    let events = decode_events(&output);
    assert_eq!(events_of_type(&events, "error").len(), 1);
    assert!(events_of_type(&events, "response.completed").is_empty());
    assert!(events_of_type(&events, "response.done").is_empty());
}

#[test]
fn responses_stream_parser_preserves_item_and_usage_extensions() {
    let mut parser = Parser::new();
    let mut events = Vec::new();
    parser
        .on_event(
            "response.created",
            &json!({"response":{"id":"resp-1","status":"in_progress"}}).to_string(),
            &mut events,
        )
        .unwrap();
    parser
        .on_event(
            "response.output_item.added",
            &json!({
                "output_index": 0,
                "item": {
                    "type": "message",
                    "id": "msg-1",
                    "role": "assistant",
                    "status": "in_progress",
                    "content": [],
                    "x_item": "early"
                }
            })
            .to_string(),
            &mut events,
        )
        .unwrap();
    parser
        .on_event(
            "response.output_item.done",
            &json!({
                "output_index": 0,
                "item": {
                    "type": "message",
                    "id": "msg-1",
                    "role": "assistant",
                    "status": "completed",
                    "content": [],
                    "x_item": "late"
                }
            })
            .to_string(),
            &mut events,
        )
        .unwrap();
    parser
        .on_event(
            "response.completed",
            &json!({
                "response": {
                    "id": "resp-1",
                    "status": "completed",
                    "usage": {"input_tokens":1,"output_tokens":2,"x_usage":true}
                }
            })
            .to_string(),
            &mut events,
        )
        .unwrap();

    let mut accumulator = ResponseAccumulator::default();
    for event in events {
        accumulator.apply(event);
    }
    let snapshot = accumulator.snapshot();
    let ContentItem::Message { extensions, .. } = &snapshot.response.output[0] else {
        panic!("expected message item");
    };
    let item_extension = extensions
        .iter()
        .find(|extension| extension.key == "x_item")
        .unwrap();
    assert_eq!(item_extension.value, json!("late"));
    assert!(snapshot
        .response
        .usage
        .extensions
        .iter()
        .any(|extension| extension.key == "x_usage" && extension.value == json!(true)));
}

fn decode_events(chunks: &[bytes::Bytes]) -> Vec<Value> {
    chunks
        .iter()
        .map(|chunk| {
            let event = sse::decode_event(chunk).unwrap();
            serde_json::from_str(&event.data.unwrap()).unwrap()
        })
        .collect()
}

fn events_of_type<'a>(events: &'a [Value], kind: &str) -> Vec<&'a Value> {
    events
        .iter()
        .filter(|event| event.get("type").and_then(Value::as_str) == Some(kind))
        .collect()
}
