use codex_provider_proxy_api_conversion::{
    ir::{CanonicalResponseEvent, ContentPart, ItemKind},
    protocols::openai_responses::stream::Renderer,
};

#[test]
fn responses_v2_renderer_emits_lifecycle_and_text_events() {
    let mut renderer = Renderer::new();
    let mut out = Vec::new();
    renderer.on_event(
        &CanonicalResponseEvent::StreamStarted {
            response_id: Some("resp-1".into()),
            model: Some("m".into()),
        },
        &mut out,
    );
    renderer.on_event(
        &CanonicalResponseEvent::ItemStarted {
            item_id: "msg-1".into(),
            item: ItemKind::Message,
        },
        &mut out,
    );
    renderer.on_event(
        &CanonicalResponseEvent::ContentDelta {
            item_id: "msg-1".into(),
            content: ContentPart::Text("hi".into()),
        },
        &mut out,
    );
    assert!(out
        .iter()
        .any(|event| event.starts_with(b"event: response.created")));
    assert!(out
        .iter()
        .any(|event| event.starts_with(b"event: response.output_text.delta")));
}
