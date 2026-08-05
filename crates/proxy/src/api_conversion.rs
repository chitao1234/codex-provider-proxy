//! Proxy-side integration of the API conversion layer.
//!
//! Decides when a request must be converted (provider declares an upstream Chat Completions
//! API and accepts downstream Anthropic Messages), converts the request body after the
//! model-mapping rewrite, and adapts upstream response streams back to Messages form.

use std::{
    collections::VecDeque,
    fmt,
    pin::Pin,
    task::{Context as TaskContext, Poll},
};

use bytes::{Bytes, BytesMut};
use codex_provider_proxy_api_conversion::dialect::{
    converts_messages_to_upstream, ModelCapabilities,
};
use codex_provider_proxy_api_conversion::error::convert_chat_error_body;
use codex_provider_proxy_api_conversion::messages_to_chat::{
    convert_chat_response, convert_messages_request, ChatStreamConverter,
};
use codex_provider_proxy_api_conversion::{sse, ConversionError};
use futures_util::Stream;
use http::{HeaderMap, Method, StatusCode};
use pin_project_lite::pin_project;
use serde_json::Value;
use tracing::warn;

use crate::config::{Config, Provider};

/// Upstream path a converted Messages request is sent to.
pub const CHAT_COMPLETIONS_PATH: &str = "chat/completions";
const MESSAGES_PATH: &str = "messages";

/// Whether this provider converts downstream Anthropic Messages traffic.
pub fn provider_converts_messages(provider: &Provider) -> bool {
    converts_messages_to_upstream(provider.upstream_api, &provider.accept_downstream_apis)
}

/// Whether a forwarded path is the Anthropic Messages endpoint.
pub fn path_is_messages(path: &str) -> bool {
    let trimmed = path.trim_matches('/');
    trimmed == MESSAGES_PATH || trimmed.ends_with(&format!("/{MESSAGES_PATH}"))
}

/// The upstream path to use for a provider/forwarded-path pair: `/chat/completions` when the
/// provider converts Messages traffic, otherwise the forwarded path unchanged.
pub fn conversion_upstream_path<'a>(provider: &Provider, forwarded_path: &'a str) -> &'a str {
    if provider_converts_messages(provider) && path_is_messages(forwarded_path) {
        CHAT_COMPLETIONS_PATH
    } else {
        forwarded_path
    }
}

/// Whether the request side of a prepared attempt must be converted.
pub fn request_conversion_enabled(
    cfg: &Config,
    method: &Method,
    provider_name: &str,
    forwarded_path: &str,
) -> bool {
    *method == Method::POST
        && path_is_messages(forwarded_path)
        && cfg
            .providers
            .get(provider_name)
            .is_some_and(provider_converts_messages)
}

/// Convert a (model-mapped) Messages request body into Chat Completions form using the
/// provider's per-model capabilities.
pub fn convert_request_body(
    cfg: &Config,
    provider_name: &str,
    body: Bytes,
) -> Result<Bytes, RequestConversionRejected> {
    let provider = cfg
        .providers
        .get(provider_name)
        .expect("request conversion requires a resolved provider");
    let json: Value = serde_json::from_slice(&body)
        .map_err(|_| RequestConversionRejected::invalid("request body is not valid JSON"))?;
    if !json.is_object() {
        return Err(RequestConversionRejected::invalid(
            "request body is not a JSON object",
        ));
    }
    let model = json
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let caps = ModelCapabilities::resolve(
        model,
        provider.default_capabilities.as_ref(),
        &provider.model_capabilities,
    );
    let (converted, report) = convert_messages_request(&json, &caps)
        .map_err(RequestConversionRejected::from_conversion_error)?;
    if !report.dropped_server_tools.is_empty() {
        warn!(
            provider = %provider_name,
            model = %model,
            dropped_server_tools = ?report.dropped_server_tools,
            "dropped server-side tool definitions not supported by upstream"
        );
    }
    if !report.mapped_server_tools.is_empty() {
        warn!(
            provider = %provider_name,
            model = %model,
            mapped_server_tools = ?report.mapped_server_tools,
            "mapped server-side tools to function tools for upstream"
        );
    }
    serde_json::to_vec(&converted)
        .map(Bytes::from)
        .map_err(|_| RequestConversionRejected::invalid("failed to serialize converted request"))
}

/// A request that cannot be converted; carries the Anthropic-style error envelope to return
/// downstream as HTTP 400.
#[derive(Debug)]
pub struct RequestConversionRejected {
    pub error_body: Value,
}

impl RequestConversionRejected {
    fn from_conversion_error(err: ConversionError) -> Self {
        Self {
            error_body: err.to_anthropic_error_body(),
        }
    }

    fn invalid(message: &str) -> Self {
        Self::from_conversion_error(ConversionError::invalid(message))
    }
}

impl fmt::Display for RequestConversionRejected {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "request conversion rejected: {}",
            self.error_body
        )
    }
}

impl std::error::Error for RequestConversionRejected {}

/// Whether the response side of the final attempt must be converted back to Messages form.
pub fn response_conversion_enabled(
    cfg: &Config,
    method: &Method,
    provider_name: &str,
    forwarded_path: &str,
) -> bool {
    request_conversion_enabled(cfg, method, provider_name, forwarded_path)
}

/// Convert an upstream non-2xx JSON error body to the Anthropic error envelope.
pub fn convert_error_body(status: StatusCode, body: &Value) -> Value {
    convert_chat_error_body(status, body)
}

/// Convert a non-streaming upstream Chat Completions body into Messages JSON bytes.
pub fn convert_non_streaming_body(body: Bytes) -> Result<Bytes, std::io::Error> {
    let json: Value = serde_json::from_slice(&body).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "upstream chat response is not valid JSON",
        )
    })?;
    let converted = convert_chat_response(&json)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    serde_json::to_vec(&converted)
        .map(Bytes::from)
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "failed to serialize converted chat response",
            )
        })
}

// Adapt an upstream SSE stream (Chat Completions chunks) into downstream Messages events.
pin_project! {
    pub struct ChatToMessagesStream<S> {
        #[pin]
        inner: S,
        converter: ChatStreamConverter,
        pending: BytesMut,
        out_events: VecDeque<Bytes>,
        finished: bool,
    }
}

impl<S> ChatToMessagesStream<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            converter: ChatStreamConverter::new(),
            pending: BytesMut::new(),
            out_events: VecDeque::new(),
            finished: false,
        }
    }
}

impl<S> Stream for ChatToMessagesStream<S>
where
    S: Stream<Item = Result<Bytes, std::io::Error>>,
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

        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    this.pending.extend_from_slice(&chunk);
                    let mut events = Vec::new();
                    while let Some(boundary_end) =
                        crate::proxy::find_sse_event_boundary(this.pending.as_ref())
                    {
                        let event_bytes = this.pending.split_to(boundary_end).freeze();
                        if let Some(payload) = sse::first_data_payload(&event_bytes) {
                            if let Err(err) = this.converter.on_chunk(payload, &mut events) {
                                return Poll::Ready(Some(Err(std::io::Error::other(err))));
                            }
                        }
                    }
                    if !events.is_empty() {
                        this.out_events.extend(events);
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
                    let mut events = Vec::new();
                    this.converter.finish(&mut events);
                    if events.is_empty() {
                        return Poll::Ready(None);
                    }
                    this.out_events.extend(events);
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

// Adapt a non-streaming upstream Chat Completions body into Messages JSON bytes, buffering
// the whole response (bounded by `max_bytes`).
pin_project! {
    pub struct NonStreamingConversionStream<S> {
        #[pin]
        inner: S,
        max_bytes: usize,
        buffered: BytesMut,
        result: Option<Result<Bytes, std::io::Error>>,
        finished: bool,
    }
}

impl<S> NonStreamingConversionStream<S> {
    pub fn new(inner: S, max_bytes: usize) -> Self {
        Self {
            inner,
            max_bytes,
            buffered: BytesMut::new(),
            result: None,
            finished: false,
        }
    }
}

impl<S> Stream for NonStreamingConversionStream<S>
where
    S: Stream<Item = Result<Bytes, std::io::Error>>,
{
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if let Some(result) = this.result.take() {
            return Poll::Ready(Some(result));
        }
        if *this.finished {
            return Poll::Ready(None);
        }

        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    let new_len = this.buffered.len().saturating_add(chunk.len());
                    if new_len > *this.max_bytes {
                        *this.result = Some(Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "converted response exceeds the {}-byte buffering limit",
                                this.max_bytes
                            ),
                        )));
                        return Poll::Ready(this.result.take());
                    }
                    this.buffered.extend_from_slice(&chunk);
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(None) => {
                    *this.finished = true;
                    let buffered = std::mem::take(this.buffered).freeze();
                    *this.result = Some(convert_non_streaming_body(buffered));
                    return Poll::Ready(this.result.take());
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Drop the response headers that no longer describe the converted body.
pub fn strip_converted_response_headers(headers: &mut HeaderMap) {
    headers.remove(http::header::CONTENT_LENGTH);
    headers.remove(http::header::CONTENT_ENCODING);
    headers.remove(http::header::TRANSFER_ENCODING);
}
