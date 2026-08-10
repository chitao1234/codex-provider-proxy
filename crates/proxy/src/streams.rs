//! Generic parser→renderer stream wiring.
//!
//! A `CanonicalParserRendererStream` pipes an upstream SSE stream through a protocol-local parser
//! (producing `CanonicalResponseEvent`s) into a protocol-local renderer (producing
//! downstream SSE bytes). The upstream protocol and downstream protocol are chosen
//! independently, so N parsers × M renderers compose without N×M converters.

use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context as TaskContext, Poll},
};

use bytes::{Bytes, BytesMut};
use codex_provider_proxy_api_conversion::error::ConversionError;
use codex_provider_proxy_api_conversion::ir::{CanonicalResponseEvent, ResponseAccumulator};
use codex_provider_proxy_api_conversion::sse;
use futures_util::Stream;
use pin_project_lite::pin_project;

use crate::api_conversion::ResponseRecorder;

/// A protocol-local SSE parser that emits protocol-neutral response events.
pub trait CanonicalParser {
    fn on_event(
        &mut self,
        event_type: &str,
        payload: &str,
        out: &mut Vec<CanonicalResponseEvent>,
    ) -> Result<(), ConversionError>;
    fn finish(&mut self, out: &mut Vec<CanonicalResponseEvent>);
}

/// A protocol-local SSE renderer consuming protocol-neutral response events.
pub trait CanonicalRenderer {
    fn on_event(&mut self, event: &CanonicalResponseEvent, out: &mut Vec<Bytes>);
    fn finish(&mut self, out: &mut Vec<Bytes>);
    fn response_id(&self) -> Option<&str> {
        None
    }
}

impl CanonicalParser
    for codex_provider_proxy_api_conversion::protocols::openai_chat_completions::stream::Parser
{
    fn on_event(
        &mut self,
        _event_type: &str,
        payload: &str,
        out: &mut Vec<CanonicalResponseEvent>,
    ) -> Result<(), ConversionError> {
        self.on_chunk(payload, out)
    }

    fn finish(&mut self, out: &mut Vec<CanonicalResponseEvent>) {
        self.finish(out);
    }
}

impl CanonicalParser
    for codex_provider_proxy_api_conversion::protocols::anthropic_messages::stream::Parser
{
    fn on_event(
        &mut self,
        event_type: &str,
        payload: &str,
        out: &mut Vec<CanonicalResponseEvent>,
    ) -> Result<(), ConversionError> {
        self.on_event(event_type, payload, out)
    }

    fn finish(&mut self, out: &mut Vec<CanonicalResponseEvent>) {
        self.finish(out);
    }
}

impl CanonicalParser
    for codex_provider_proxy_api_conversion::protocols::openai_responses::stream::Parser
{
    fn on_event(
        &mut self,
        event_type: &str,
        payload: &str,
        out: &mut Vec<CanonicalResponseEvent>,
    ) -> Result<(), ConversionError> {
        self.on_event(event_type, payload, out)
    }

    fn finish(&mut self, out: &mut Vec<CanonicalResponseEvent>) {
        self.finish(out);
    }
}

impl CanonicalRenderer
    for codex_provider_proxy_api_conversion::protocols::anthropic_messages::stream::Renderer
{
    fn on_event(&mut self, event: &CanonicalResponseEvent, out: &mut Vec<Bytes>) {
        self.on_event(event, out);
    }

    fn finish(&mut self, out: &mut Vec<Bytes>) {
        self.finish(out);
    }
}

impl CanonicalRenderer
    for codex_provider_proxy_api_conversion::protocols::openai_responses::stream::Renderer
{
    fn on_event(&mut self, event: &CanonicalResponseEvent, out: &mut Vec<Bytes>) {
        self.on_event(event, out);
    }

    fn finish(&mut self, out: &mut Vec<Bytes>) {
        self.finish(out);
    }

    fn response_id(&self) -> Option<&str> {
        self.response_id()
    }
}

fn pending_has_data(pending: &[u8]) -> std::io::Result<bool> {
    if pending.is_empty() {
        return Ok(false);
    }
    sse::decode_event(pending)
        .map(|event| event.data.is_some())
        .map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("upstream SSE ended with invalid UTF-8: {err}"),
            )
        })
}

pin_project! {
    pub struct CanonicalParserRendererStream<S, P, R> {
        #[pin]
        inner: S,
        parser: P,
        renderer: R,
        recorder: Option<ResponseRecorder>,
        response: ResponseAccumulator,
        pending: BytesMut,
        out_events: VecDeque<Bytes>,
        pending_error: Option<std::io::Error>,
        finished: bool,
    }
}

impl<S, P, R> CanonicalParserRendererStream<S, P, R> {
    pub fn new(inner: S, parser: P, renderer: R) -> Self {
        Self::with_recorder(inner, parser, renderer, None)
    }

    pub fn with_recorder(
        inner: S,
        parser: P,
        renderer: R,
        recorder: Option<ResponseRecorder>,
    ) -> Self {
        Self {
            inner,
            parser,
            renderer,
            recorder,
            response: ResponseAccumulator::default(),
            pending: BytesMut::new(),
            out_events: VecDeque::new(),
            pending_error: None,
            finished: false,
        }
    }
}

impl<S, P, R> Stream for CanonicalParserRendererStream<S, P, R>
where
    S: Stream<Item = Result<Bytes, std::io::Error>>,
    P: CanonicalParser,
    R: CanonicalRenderer,
{
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if let Some(event) = this.out_events.pop_front() {
            return Poll::Ready(Some(Ok(event)));
        }
        if let Some(error) = this.pending_error.take() {
            return Poll::Ready(Some(Err(error)));
        }
        if *this.finished {
            return Poll::Ready(None);
        }

        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    this.pending.extend_from_slice(&chunk);
                    let mut rendered = Vec::new();
                    while let Some(boundary_end) =
                        crate::proxy::find_sse_event_boundary(this.pending.as_ref())
                    {
                        if boundary_end > sse::MAX_EVENT_BYTES {
                            *this.finished = true;
                            let error = oversized_sse_event_error();
                            if rendered.is_empty() {
                                return Poll::Ready(Some(Err(error)));
                            }
                            *this.pending_error = Some(error);
                            break;
                        }
                        let event_bytes = this.pending.split_to(boundary_end).freeze();
                        let event = match sse::decode_event(&event_bytes) {
                            Ok(event) => event,
                            Err(err) => {
                                *this.finished = true;
                                let error = std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    format!("upstream SSE event is not valid UTF-8: {err}"),
                                );
                                if rendered.is_empty() {
                                    return Poll::Ready(Some(Err(error)));
                                }
                                *this.pending_error = Some(error);
                                break;
                            }
                        };
                        let Some(payload) = event.data else {
                            continue;
                        };
                        let mut semantic_events = Vec::new();
                        if let Err(err) = this.parser.on_event(
                            event.event_type.unwrap_or(""),
                            &payload,
                            &mut semantic_events,
                        ) {
                            *this.finished = true;
                            let error = std::io::Error::new(std::io::ErrorKind::InvalidData, err);
                            if rendered.is_empty() {
                                return Poll::Ready(Some(Err(error)));
                            }
                            *this.pending_error = Some(error);
                            break;
                        }
                        for event in semantic_events {
                            this.response.apply(event.clone());
                            this.renderer.on_event(&event, &mut rendered);
                        }
                    }
                    if !*this.finished && this.pending.len() > sse::MAX_EVENT_BYTES {
                        *this.finished = true;
                        let error = oversized_sse_event_error();
                        if rendered.is_empty() {
                            return Poll::Ready(Some(Err(error)));
                        }
                        *this.pending_error = Some(error);
                    }
                    if !rendered.is_empty() {
                        this.out_events.extend(rendered);
                        return Poll::Ready(Some(Ok(this
                            .out_events
                            .pop_front()
                            .expect("events were just pushed"))));
                    }
                }
                Poll::Ready(Some(Err(err))) => {
                    *this.finished = true;
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(None) => {
                    *this.finished = true;
                    if pending_has_data(this.pending.as_ref())? {
                        return Poll::Ready(Some(Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "upstream SSE ended during a data event",
                        ))));
                    }
                    let mut semantic_events = Vec::new();
                    this.parser.finish(&mut semantic_events);
                    let mut rendered = Vec::new();
                    for event in semantic_events {
                        this.response.apply(event.clone());
                        this.renderer.on_event(&event, &mut rendered);
                    }
                    this.renderer.finish(&mut rendered);
                    let snapshot = this.response.snapshot();
                    let incomplete = snapshot.response.response_id.is_some() && !snapshot.terminal;
                    if let (Some(recorder), Some(response_id)) =
                        (this.recorder.as_ref(), this.renderer.response_id())
                    {
                        recorder.record_semantic(response_id, this.response.response());
                    }
                    if incomplete {
                        let error = std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "upstream SSE ended without a terminal protocol event",
                        );
                        if rendered.is_empty() {
                            return Poll::Ready(Some(Err(error)));
                        }
                        *this.pending_error = Some(error);
                    }
                    if rendered.is_empty() {
                        return Poll::Ready(None);
                    }
                    this.out_events.extend(rendered);
                    return Poll::Ready(Some(Ok(this
                        .out_events
                        .pop_front()
                        .expect("events were just pushed"))));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pin_project! {
    /// Pass through an SSE stream byte-for-byte while parsing a copy solely for
    /// protocol-neutral continuation recording. Parse failures never affect the
    /// downstream stream; they only disable recording for that response.
    pub struct RecordingPassthroughStream<S, P> {
        #[pin]
        inner: S,
        parser: P,
        recorder: Option<ResponseRecorder>,
        response: ResponseAccumulator,
        pending: BytesMut,
        recording: bool,
        finished: bool,
    }
}

impl<S, P> RecordingPassthroughStream<S, P> {
    pub fn new(inner: S, parser: P, recorder: Option<ResponseRecorder>) -> Self {
        let recording = recorder.is_some();
        Self {
            inner,
            parser,
            recorder,
            response: ResponseAccumulator::default(),
            pending: BytesMut::new(),
            recording,
            finished: false,
        }
    }
}

impl<S, P> Stream for RecordingPassthroughStream<S, P>
where
    S: Stream<Item = Result<Bytes, std::io::Error>>,
    P: CanonicalParser,
{
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.finished {
            return Poll::Ready(None);
        }
        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                if *this.recording {
                    this.pending.extend_from_slice(&chunk);
                    let mut events = Vec::new();
                    while let Some(boundary_end) =
                        crate::proxy::find_sse_event_boundary(this.pending.as_ref())
                    {
                        if boundary_end > sse::MAX_EVENT_BYTES {
                            *this.recording = false;
                            this.pending.clear();
                            break;
                        }
                        let event_bytes = this.pending.split_to(boundary_end).freeze();
                        let Ok(parsed) = sse::decode_event(&event_bytes) else {
                            *this.recording = false;
                            this.pending.clear();
                            break;
                        };
                        let Some(payload) = parsed.data else {
                            continue;
                        };
                        if this
                            .parser
                            .on_event(parsed.event_type.unwrap_or(""), &payload, &mut events)
                            .is_err()
                        {
                            *this.recording = false;
                            this.pending.clear();
                            break;
                        }
                    }
                    if *this.recording && this.pending.len() > sse::MAX_EVENT_BYTES {
                        *this.recording = false;
                        this.pending.clear();
                    }
                    for event in events {
                        this.response.apply(event);
                    }
                }
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(err))) => {
                *this.finished = true;
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => {
                *this.finished = true;
                if *this.recording && pending_has_data(this.pending.as_ref()).unwrap_or(true) {
                    *this.recording = false;
                }
                if *this.recording {
                    let mut events = Vec::new();
                    this.parser.finish(&mut events);
                    for event in events {
                        this.response.apply(event);
                    }
                    if let (Some(recorder), Some(response_id), true) = (
                        this.recorder.as_ref(),
                        this.response.response().response_id.as_deref(),
                        this.response.continuation_state().is_some(),
                    ) {
                        recorder.record_semantic(response_id, this.response.response());
                    }
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

fn oversized_sse_event_error() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "upstream SSE event exceeds the {}-byte conversion limit",
            sse::MAX_EVENT_BYTES
        ),
    )
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use codex_provider_proxy_api_conversion::{
        ir::{CanonicalResponseEvent, RequestIr},
        request_adapter::{RequestAdapter, RequestAdapterKind},
        request_ir::Protocol,
    };
    use futures_util::{stream, StreamExt};
    use serde_json::Value;

    use super::*;

    fn responses_request() -> RequestIr {
        RequestAdapterKind::for_protocol(Protocol::OpenAiResponses)
            .parse_v2(&serde_json::json!({"model":"model", "input":"hi"}))
            .unwrap()
    }

    struct ProbeParser {
        values: Rc<RefCell<Vec<Value>>>,
    }

    impl CanonicalParser for ProbeParser {
        fn on_event(
            &mut self,
            _event_type: &str,
            payload: &str,
            _out: &mut Vec<CanonicalResponseEvent>,
        ) -> Result<(), ConversionError> {
            let data: Value = serde_json::from_str(payload)
                .map_err(|_| ConversionError::invalid("invalid probe JSON"))?;
            self.values.borrow_mut().push(data.clone());
            Ok(())
        }

        fn finish(&mut self, _out: &mut Vec<CanonicalResponseEvent>) {}
    }

    struct NullRenderer;

    impl CanonicalRenderer for NullRenderer {
        fn on_event(&mut self, _event: &CanonicalResponseEvent, _out: &mut Vec<Bytes>) {}

        fn finish(&mut self, _out: &mut Vec<Bytes>) {}
    }

    struct StartParser;

    impl CanonicalParser for StartParser {
        fn on_event(
            &mut self,
            _event_type: &str,
            payload: &str,
            out: &mut Vec<CanonicalResponseEvent>,
        ) -> Result<(), ConversionError> {
            serde_json::from_str::<Value>(payload)
                .map_err(|_| ConversionError::invalid("invalid start JSON"))?;
            out.push(CanonicalResponseEvent::StreamStarted {
                response_id: Some("response_1".to_owned()),
                model: Some("model".to_owned()),
            });
            Ok(())
        }

        fn finish(&mut self, _out: &mut Vec<CanonicalResponseEvent>) {}
    }

    struct ByteRenderer;

    impl CanonicalRenderer for ByteRenderer {
        fn on_event(&mut self, _event: &CanonicalResponseEvent, out: &mut Vec<Bytes>) {
            out.push(Bytes::from_static(b"converted"));
        }

        fn finish(&mut self, _out: &mut Vec<Bytes>) {}
    }

    #[tokio::test]
    async fn parser_renderer_joins_multiline_sse_data() {
        let values = Rc::new(RefCell::new(Vec::new()));
        let body = Bytes::from_static(b"event: test\ndata: {\ndata: \"answer\": 42\ndata: }\n\n");
        let inner = stream::iter([Ok::<_, std::io::Error>(body)]);
        let mut converted = CanonicalParserRendererStream::new(
            inner,
            ProbeParser {
                values: values.clone(),
            },
            NullRenderer,
        );

        assert!(converted.next().await.is_none());
        assert_eq!(*values.borrow(), vec![serde_json::json!({"answer": 42})]);
    }

    #[tokio::test]
    async fn parser_renderer_rejects_malformed_and_truncated_data_events() {
        for (body, expected_kind) in [
            (
                Bytes::from_static(b"data: not-json\n\n"),
                std::io::ErrorKind::InvalidData,
            ),
            (
                Bytes::from_static(b"data: {\"answer\":42}"),
                std::io::ErrorKind::UnexpectedEof,
            ),
        ] {
            let inner = stream::iter([Ok::<_, std::io::Error>(body)]);
            let mut converted = CanonicalParserRendererStream::new(
                inner,
                ProbeParser {
                    values: Rc::new(RefCell::new(Vec::new())),
                },
                NullRenderer,
            );

            let error = converted.next().await.unwrap().unwrap_err();
            assert_eq!(error.kind(), expected_kind);
            assert!(converted.next().await.is_none());
        }
    }

    #[tokio::test]
    async fn parser_renderer_delivers_valid_events_before_later_error() {
        for (body, expected_kind) in [
            (
                Bytes::from_static(b"data: {\"valid\":true}\n\ndata: not-json\n\n"),
                std::io::ErrorKind::InvalidData,
            ),
            (
                Bytes::from_static(b"data: {\"valid\":true}\n\n"),
                std::io::ErrorKind::UnexpectedEof,
            ),
        ] {
            let inner = stream::iter([Ok::<_, std::io::Error>(body)]);
            let mut converted =
                CanonicalParserRendererStream::new(inner, StartParser, ByteRenderer);

            assert_eq!(converted.next().await.unwrap().unwrap(), "converted");
            let error = converted.next().await.unwrap().unwrap_err();
            assert_eq!(error.kind(), expected_kind);
            assert!(converted.next().await.is_none());
        }
    }

    #[tokio::test]
    async fn parser_renderer_rejects_oversized_unterminated_event() {
        let body = Bytes::from(vec![b'x'; sse::MAX_EVENT_BYTES + 1]);
        let inner = stream::iter([Ok::<_, std::io::Error>(body)]);
        let mut converted = CanonicalParserRendererStream::new(
            inner,
            ProbeParser {
                values: Rc::new(RefCell::new(Vec::new())),
            },
            NullRenderer,
        );

        let error = converted.next().await.unwrap().unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("conversion limit"));
    }

    #[tokio::test]
    async fn recording_passthrough_keeps_malformed_sse_and_skips_recording() {
        let store = crate::response_state::ResponseStateStore::new(4);
        let request_ir = responses_request();
        let recorder = ResponseRecorder::new(store.clone(), "provider", "model", request_ir);
        let body = Bytes::from_static(
            concat!(
                "event: response.created\n",
                "data: {\"response\":{\"id\":\"resp_1\",\"model\":\"model\"}}\n\n",
                "event: response.completed\n",
                "data: {\"response\":{\"status\":\"completed\",\"usage\":{}}}\n\n",
                "data: not-json\n\n",
                "data: {\"truncated\":true}",
            )
            .as_bytes(),
        );
        let inner = stream::iter([Ok::<_, std::io::Error>(body.clone())]);
        let mut passthrough = RecordingPassthroughStream::new(
            inner,
            codex_provider_proxy_api_conversion::protocols::openai_responses::stream::Parser::new(),
            Some(recorder),
        );
        let mut received = Vec::new();
        while let Some(chunk) = passthrough.next().await {
            received.extend_from_slice(&chunk.unwrap());
        }

        assert_eq!(received, body);
        assert!(store.get("resp_1").is_none());
    }

    #[tokio::test]
    async fn recording_passthrough_does_not_record_unterminated_response() {
        let store = crate::response_state::ResponseStateStore::new(4);
        let request_ir = responses_request();
        let recorder = ResponseRecorder::new(store.clone(), "provider", "model", request_ir);
        let body = Bytes::from_static(
            concat!(
                "event: response.created\n",
                "data: {\"response\":{\"id\":\"resp_1\",\"model\":\"model\"}}\n\n",
                "event: response.output_text.delta\n",
                "data: {\"delta\":\"partial\"}\n\n",
            )
            .as_bytes(),
        );
        let inner = stream::iter([Ok::<_, std::io::Error>(body.clone())]);
        let mut passthrough = RecordingPassthroughStream::new(
            inner,
            codex_provider_proxy_api_conversion::protocols::openai_responses::stream::Parser::new(),
            Some(recorder),
        );
        let mut received = Vec::new();
        while let Some(chunk) = passthrough.next().await {
            received.extend_from_slice(&chunk.unwrap());
        }

        assert_eq!(received, body);
        assert!(store.get("resp_1").is_none());
    }

    #[tokio::test]
    async fn recording_passthrough_disables_parsing_for_oversized_event() {
        let store = crate::response_state::ResponseStateStore::new(4);
        let request_ir = responses_request();
        let recorder = ResponseRecorder::new(store.clone(), "provider", "model", request_ir);
        let body = Bytes::from(vec![b'x'; sse::MAX_EVENT_BYTES + 1]);
        let inner = stream::iter([Ok::<_, std::io::Error>(body.clone())]);
        let mut passthrough = RecordingPassthroughStream::new(
            inner,
            codex_provider_proxy_api_conversion::protocols::openai_responses::stream::Parser::new(),
            Some(recorder),
        );

        assert_eq!(passthrough.next().await.unwrap().unwrap(), body);
        assert!(passthrough.next().await.is_none());
        assert!(store.get("resp_1").is_none());
    }
}
