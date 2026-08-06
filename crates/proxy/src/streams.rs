//! Generic parser→renderer stream wiring.
//!
//! A `ParserRendererStream` pipes an upstream SSE stream through an upstream parser
//! (producing semantic `StreamEvent`s) into a downstream renderer (producing
//! downstream SSE bytes). The upstream protocol and downstream protocol are chosen
//! independently, so N parsers × M renderers compose without N×M converters.

use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context as TaskContext, Poll},
};

use bytes::{Bytes, BytesMut};
use codex_provider_proxy_api_conversion::error::ConversionError;
use codex_provider_proxy_api_conversion::sse;
use codex_provider_proxy_api_conversion::stream::StreamEvent;
use futures_util::Stream;
use pin_project_lite::pin_project;
use serde_json::Value;

use crate::api_conversion::ResponseRecorder;

/// An upstream SSE parser producing semantic events.
pub trait Parser {
    fn on_event(
        &mut self,
        event_type: &str,
        data: &Value,
        out: &mut Vec<StreamEvent>,
    ) -> Result<(), ConversionError>;
    fn finish(&mut self, out: &mut Vec<StreamEvent>);
}

/// A downstream SSE renderer consuming semantic events.
pub trait Renderer {
    fn on_event(&mut self, event: &StreamEvent, out: &mut Vec<Bytes>);
    fn finish(&mut self, out: &mut Vec<Bytes>);
    fn response_id(&self) -> Option<&str> {
        None
    }
    fn assistant_turn(&self) -> Option<&Value> {
        None
    }
}

impl Parser for codex_provider_proxy_api_conversion::messages_parser::MessagesParser {
    fn on_event(
        &mut self,
        event_type: &str,
        data: &Value,
        out: &mut Vec<StreamEvent>,
    ) -> Result<(), ConversionError> {
        self.on_event(event_type, data, out)
    }

    fn finish(&mut self, out: &mut Vec<StreamEvent>) {
        self.finish(out);
    }
}

impl Parser for codex_provider_proxy_api_conversion::responses_parser::ResponsesParser {
    fn on_event(
        &mut self,
        event_type: &str,
        data: &Value,
        out: &mut Vec<StreamEvent>,
    ) -> Result<(), ConversionError> {
        self.on_event(event_type, data, out)
    }

    fn finish(&mut self, out: &mut Vec<StreamEvent>) {
        self.finish(out);
    }
}

impl Renderer for codex_provider_proxy_api_conversion::messages_renderer::MessagesRenderer {
    fn on_event(&mut self, event: &StreamEvent, out: &mut Vec<Bytes>) {
        self.on_event(event, out);
    }

    fn finish(&mut self, out: &mut Vec<Bytes>) {
        self.finish(out);
    }
}

impl Renderer for codex_provider_proxy_api_conversion::responses_renderer::ResponsesRenderer {
    fn on_event(&mut self, event: &StreamEvent, out: &mut Vec<Bytes>) {
        self.on_event(event, out);
    }

    fn finish(&mut self, out: &mut Vec<Bytes>) {
        self.finish(out);
    }

    fn response_id(&self) -> Option<&str> {
        self.response_id()
    }

    fn assistant_turn(&self) -> Option<&Value> {
        self.assistant_turn()
    }
}

pin_project! {
    pub struct ParserRendererStream<S, P, R> {
        #[pin]
        inner: S,
        parser: P,
        renderer: R,
        recorder: Option<ResponseRecorder>,
        pending: BytesMut,
        out_events: VecDeque<Bytes>,
        finished: bool,
    }
}

impl<S, P, R> ParserRendererStream<S, P, R> {
    pub fn new(inner: S, parser: P, renderer: R) -> Self {
        Self {
            inner,
            parser,
            renderer,
            recorder: None,
            pending: BytesMut::new(),
            out_events: VecDeque::new(),
            finished: false,
        }
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
            pending: BytesMut::new(),
            out_events: VecDeque::new(),
            finished: false,
        }
    }
}

impl<S, P, R> Stream for ParserRendererStream<S, P, R>
where
    S: Stream<Item = Result<Bytes, std::io::Error>>,
    P: Parser,
    R: Renderer,
{
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if let Some(event) = this.out_events.pop_front() {
            return Poll::Ready(Some(Ok(event)));
        }
        if *this.finished {
            return Poll::Ready(None);
        }
        let mut events = Vec::new();
        let mut rendered = Vec::new();
        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    this.pending.extend_from_slice(&chunk);
                    while let Some(boundary_end) =
                        crate::proxy::find_sse_event_boundary(this.pending.as_ref())
                    {
                        let event_bytes = this.pending.split_to(boundary_end).freeze();
                        let event_type = sse::first_event_type(&event_bytes).unwrap_or("");
                        if let Some(payload) = sse::first_data_payload(&event_bytes) {
                            if let Ok(parsed) = serde_json::from_str::<Value>(payload) {
                                if let Err(err) =
                                    this.parser.on_event(event_type, &parsed, &mut events)
                                {
                                    return Poll::Ready(Some(Err(std::io::Error::other(err))));
                                }
                            }
                        }
                    }
                    for event in &events {
                        this.renderer.on_event(event, &mut rendered);
                    }
                    events.clear();
                    if !rendered.is_empty() {
                        this.out_events.extend(std::mem::take(&mut rendered));
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
                    let mut final_events = Vec::new();
                    this.parser.finish(&mut final_events);
                    for event in &final_events {
                        this.renderer.on_event(event, &mut rendered);
                    }
                    this.renderer.finish(&mut rendered);
                    if let Some(recorder) = &this.recorder {
                        if let Some(response_id) = this.renderer.response_id() {
                            recorder.record(response_id, this.renderer.assistant_turn().cloned());
                        }
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
