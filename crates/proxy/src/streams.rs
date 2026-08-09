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
use codex_provider_proxy_api_conversion::stream::{StreamEvent, StreamingResponseIr};
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
}

pin_project! {
    pub struct ParserRendererStream<S, P, R> {
        #[pin]
        inner: S,
        parser: P,
        renderer: R,
        recorder: Option<ResponseRecorder>,
        response: StreamingResponseIr,
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
            response: StreamingResponseIr::default(),
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
            response: StreamingResponseIr::default(),
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
                        this.response.on_event(event);
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
                        this.response.on_event(event);
                        this.renderer.on_event(event, &mut rendered);
                    }
                    this.renderer.finish(&mut rendered);
                    if let (Some(recorder), Some(response_id)) =
                        (this.recorder.as_ref(), this.response.openai_response_id())
                    {
                        recorder.record_stream(&response_id, this.response);
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
        response: StreamingResponseIr,
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
            response: StreamingResponseIr::default(),
            pending: BytesMut::new(),
            recording,
            finished: false,
        }
    }
}

impl<S, P> Stream for RecordingPassthroughStream<S, P>
where
    S: Stream<Item = Result<Bytes, std::io::Error>>,
    P: Parser,
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
                        let event_bytes = this.pending.split_to(boundary_end).freeze();
                        let event_type = sse::first_event_type(&event_bytes).unwrap_or("");
                        let Some(payload) = sse::first_data_payload(&event_bytes) else {
                            continue;
                        };
                        let Ok(parsed) = serde_json::from_str::<Value>(payload) else {
                            *this.recording = false;
                            this.pending.clear();
                            break;
                        };
                        if this
                            .parser
                            .on_event(event_type, &parsed, &mut events)
                            .is_err()
                        {
                            *this.recording = false;
                            this.pending.clear();
                            break;
                        }
                    }
                    for event in &events {
                        this.response.on_event(event);
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
                if *this.recording {
                    let mut events = Vec::new();
                    this.parser.finish(&mut events);
                    for event in &events {
                        this.response.on_event(event);
                    }
                    if let (Some(recorder), Some(response_id)) =
                        (this.recorder.as_ref(), this.response.openai_response_id())
                    {
                        recorder.record_stream(&response_id, this.response);
                    }
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
