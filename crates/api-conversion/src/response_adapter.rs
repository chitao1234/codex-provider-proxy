//! Shared response-core adapter for protocol-local stream semantics.

use serde_json::Value;

use crate::{
    chat::ChatUsage,
    ir::{
        CanonicalResponseEvent, ContentPart, Extension, ExtensionScope, ItemKind,
        ResponseAccumulator, ResponseSnapshot, StopReason, Usage,
    },
    stream::{StopReason as LegacyStopReason, StreamEvent},
};

pub fn apply_stream_event(
    accumulator: &mut ResponseAccumulator,
    event: &StreamEvent,
) -> crate::ir::CoreUpdate {
    match event {
        StreamEvent::Start { id, model } => {
            accumulator.apply(CanonicalResponseEvent::StreamStarted {
                response_id: (!id.is_empty()).then(|| id.clone()),
                model: (!model.is_empty()).then(|| model.clone()),
            })
        }
        StreamEvent::ReasoningDelta { text, signature } => {
            accumulator.apply(CanonicalResponseEvent::ItemStarted {
                item_id: "reasoning".to_owned(),
                item: ItemKind::Reasoning,
            });
            accumulator.apply(CanonicalResponseEvent::ContentDelta {
                item_id: "reasoning".to_owned(),
                content: ContentPart::Reasoning {
                    text: text.clone(),
                    signature: signature.clone(),
                },
            })
        }
        StreamEvent::TextDelta { text } => {
            accumulator.apply(CanonicalResponseEvent::ItemStarted {
                item_id: "message".to_owned(),
                item: ItemKind::Message,
            });
            accumulator.apply(CanonicalResponseEvent::ContentDelta {
                item_id: "message".to_owned(),
                content: ContentPart::Text(text.clone()),
            })
        }
        StreamEvent::RefusalDelta { refusal } => {
            accumulator.apply(CanonicalResponseEvent::ItemStarted {
                item_id: "refusal".to_owned(),
                item: ItemKind::Refusal,
            });
            accumulator.apply(CanonicalResponseEvent::ContentDelta {
                item_id: "refusal".to_owned(),
                content: ContentPart::Refusal(refusal.clone()),
            })
        }
        StreamEvent::ToolCallStart {
            index,
            id,
            name,
            opaque_extensions,
            function_extensions,
        } => {
            let item_id = format!("tool:{index}");
            accumulator.apply(CanonicalResponseEvent::ToolCallStarted {
                item_id,
                call: crate::ir::ToolCall {
                    id: Some(id.clone()),
                    name: name.clone(),
                    arguments: String::new(),
                    status: Some("in_progress".to_owned()),
                    metadata: opaque_extensions.clone().into_iter().collect(),
                    function_metadata: function_extensions.clone().into_iter().collect(),
                    extensions: Vec::new(),
                },
            })
        }
        StreamEvent::ToolCallExtensions {
            index,
            opaque_extensions,
            function_extensions,
        } => accumulator.apply(CanonicalResponseEvent::ItemMetadataUpdated {
            item_id: format!("tool:{index}"),
            metadata: extension_list(opaque_extensions, function_extensions),
        }),
        StreamEvent::ToolCallArgsDelta { index, args } => {
            accumulator.apply(CanonicalResponseEvent::ToolArgumentsDelta {
                item_id: format!("tool:{index}"),
                arguments: args.clone(),
            })
        }
        StreamEvent::End {
            stop_reason,
            protocol_reason,
            usage,
        } => {
            accumulator.apply(CanonicalResponseEvent::UsageUpdated(usage_from_chat(usage)));
            accumulator.apply(CanonicalResponseEvent::ResponseCompleted {
                stop_reason: normalize_stop_reason(*stop_reason),
                original_reason: protocol_reason.clone(),
            })
        }
        StreamEvent::Error { message, code } => {
            accumulator.apply(CanonicalResponseEvent::StreamError {
                message: message.clone(),
                code: code.clone(),
            })
        }
        StreamEvent::Extension {
            namespace,
            kind,
            payload,
        } => accumulator.apply(CanonicalResponseEvent::ProtocolExtension(Extension {
            namespace: namespace.clone(),
            key: kind.clone(),
            value: payload.clone(),
            scope: ExtensionScope::Response,
            source_protocol: None,
            provider: None,
            replay: crate::ir::ReplayPolicy::NeverReplay,
        })),
    }
}

pub fn accumulate_stream(events: impl IntoIterator<Item = StreamEvent>) -> ResponseSnapshot {
    let mut accumulator = ResponseAccumulator::default();
    for event in events {
        apply_stream_event(&mut accumulator, &event);
    }
    accumulator.snapshot()
}

pub fn usage_from_chat(usage: &ChatUsage) -> Usage {
    Usage {
        input_tokens: usage.prompt_tokens,
        output_tokens: usage.completion_tokens,
        total_tokens: usage
            .prompt_tokens
            .zip(usage.completion_tokens)
            .map(|(input, output)| input + output),
        cached_input_tokens: usage.cached_tokens,
    }
}

fn normalize_stop_reason(reason: LegacyStopReason) -> StopReason {
    match reason {
        LegacyStopReason::Stop => StopReason::Natural,
        LegacyStopReason::ToolUse => StopReason::ToolUse,
        LegacyStopReason::MaxTokens => StopReason::MaxTokens,
        LegacyStopReason::ContentFilter => StopReason::ContentFilter,
        LegacyStopReason::Other => StopReason::Unknown("other".to_owned()),
    }
}

fn extension_list(
    opaque: &serde_json::Map<String, Value>,
    function: &serde_json::Map<String, Value>,
) -> Vec<Extension> {
    opaque
        .iter()
        .chain(function.iter())
        .map(|(key, value)| Extension {
            namespace: "stream".to_owned(),
            key: key.clone(),
            value: value.clone(),
            scope: ExtensionScope::ToolCall,
            source_protocol: None,
            provider: None,
            replay: crate::ir::ReplayPolicy::NeverReplay,
        })
        .collect()
}
