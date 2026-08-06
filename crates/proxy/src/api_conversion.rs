//! Proxy-side integration of the API conversion layer.
//!
//! Decides when a request must be converted (provider declares an upstream Chat Completions
//! API and accepts downstream Anthropic Messages and/or OpenAI Responses), converts the
//! request body after the model-mapping rewrite, and adapts upstream response streams back
//! to the downstream dialect.

use std::{
    collections::VecDeque,
    fmt,
    pin::Pin,
    task::{Context as TaskContext, Poll},
};

use bytes::{Bytes, BytesMut};
use codex_provider_proxy_api_conversion::chat_parser::ChatParser;
pub use codex_provider_proxy_api_conversion::dialect::DownstreamApi;
use codex_provider_proxy_api_conversion::dialect::{ModelCapabilities, UpstreamApi};
use codex_provider_proxy_api_conversion::error::convert_chat_error_body;
use codex_provider_proxy_api_conversion::messages_renderer::MessagesRenderer;
use codex_provider_proxy_api_conversion::messages_to_chat::{
    convert_chat_response, convert_messages_request,
};
use codex_provider_proxy_api_conversion::responses::{
    chat_response_assistant_turn, convert_chat_response_to_responses, convert_responses_request,
};
use codex_provider_proxy_api_conversion::responses_renderer::ResponsesRenderer;
use codex_provider_proxy_api_conversion::{sse, ConversionError};
use futures_util::Stream;
use http::{HeaderMap, Method, StatusCode};
use pin_project_lite::pin_project;
use serde_json::Value;
use tracing::warn;

use crate::config::{Config, Provider};

/// Upstream path a converted request is sent to.
pub const CHAT_COMPLETIONS_PATH: &str = "chat/completions";
const MESSAGES_UPSTREAM_PATH: &str = "v1/messages";
const RESPONSES_UPSTREAM_PATH: &str = "v1/responses";
const MESSAGES_PATH: &str = "messages";
const RESPONSES_PATH: &str = "responses";

/// The downstream dialect a forwarded path maps to, if any.
pub fn downstream_api_for_path(path: &str) -> Option<DownstreamApi> {
    let trimmed = path.trim_matches('/');
    if trimmed == MESSAGES_PATH || trimmed.ends_with(&format!("/{MESSAGES_PATH}")) {
        Some(DownstreamApi::AnthropicMessages)
    } else if trimmed == RESPONSES_PATH || trimmed.ends_with(&format!("/{RESPONSES_PATH}")) {
        Some(DownstreamApi::OpenAiResponses)
    } else {
        None
    }
}

/// Whether this provider converts the downstream dialect for the given path.
pub fn provider_converts_path(provider: &Provider, path: &str) -> bool {
    let Some(api) = downstream_api_for_path(path) else {
        return false;
    };
    provider.upstream_api != UpstreamApi::Passthrough
        && provider.accept_downstream_apis.contains(&api)
}

/// The upstream path to use for a provider/forwarded-path pair: the provider's upstream
/// protocol endpoint when conversion applies, otherwise the forwarded path unchanged.
pub fn conversion_upstream_path<'a>(
    provider: &Provider,
    forwarded_path: &'a str,
) -> std::borrow::Cow<'a, str> {
    if !provider_converts_path(provider, forwarded_path) {
        return std::borrow::Cow::Borrowed(forwarded_path);
    }
    if let Some(path) = &provider.upstream_path {
        return std::borrow::Cow::Owned(path.clone());
    }
    match provider.upstream_api {
        UpstreamApi::OpenAiChatCompletions => std::borrow::Cow::Borrowed(CHAT_COMPLETIONS_PATH),
        UpstreamApi::AnthropicMessages => std::borrow::Cow::Borrowed(MESSAGES_UPSTREAM_PATH),
        UpstreamApi::OpenAiResponses => std::borrow::Cow::Borrowed(RESPONSES_UPSTREAM_PATH),
        UpstreamApi::Passthrough => std::borrow::Cow::Borrowed(forwarded_path),
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
        && cfg
            .providers
            .get(provider_name)
            .is_some_and(|provider| provider_converts_path(provider, forwarded_path))
}

/// Convert a (model-mapped) downstream request body into Chat Completions form using the
/// provider's per-model capabilities, dispatching on the downstream dialect.
///
/// `previous_messages` carries the chat transcript stored for `previous_response_id`
/// when the downstream request continues a Responses conversation. `last_reasoning`
/// carries the most recent upstream reasoning, reattached when the client echoes an
/// empty reasoning item (Codex sends `{"summary": []}`) — required by MiMo/DeepSeek
/// on tool-call turns, else 400.
pub fn convert_request_body(
    cfg: &Config,
    provider_name: &str,
    path: &str,
    body: Bytes,
    previous_messages: Option<&[Value]>,
    last_reasoning: Option<&str>,
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
    let dialect =
        downstream_api_for_path(path).expect("request conversion requires a known downstream path");
    // Upstream Messages: the request is already in Messages form when the downstream
    // speaks Messages (near-passthrough). Upstream Responses: the request is already
    // in Responses form when the downstream speaks Responses (same protocol).
    // The model rewrite handles model mapping.
    if provider.upstream_api == UpstreamApi::AnthropicMessages
        && dialect == DownstreamApi::AnthropicMessages
    {
        return Ok(body);
    }
    // Upstream Responses + downstream Responses is the same protocol: pass through
    // unchanged, except Ark-style parameter normalization (thinking/caching) when
    // the provider is Ark.
    if provider.upstream_api == UpstreamApi::OpenAiResponses
        && dialect == DownstreamApi::OpenAiResponses
    {
        if caps.ark_style {
            let mut json = json;
            if let Some(object) = json.as_object_mut() {
                codex_provider_proxy_api_conversion::responses::apply_ark_style_to_responses(
                    object,
                );
            }
            return serde_json::to_vec(&json)
                .map(Bytes::from)
                .map_err(|_| RequestConversionRejected::invalid("failed to serialize request"));
        }
        return Ok(body);
    }
    let (converted, report) = match dialect {
        DownstreamApi::AnthropicMessages => convert_messages_request(&json, &caps)
            .map_err(|err| RequestConversionRejected::from_conversion_error(err, dialect))?,
        DownstreamApi::OpenAiResponses => {
            convert_responses_request(&json, &caps, previous_messages, last_reasoning)
                .map_err(|err| RequestConversionRejected::from_conversion_error(err, dialect))?
        }
    };
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
    fn from_conversion_error(err: ConversionError, dialect: DownstreamApi) -> Self {
        Self {
            error_body: match dialect {
                DownstreamApi::AnthropicMessages => err.to_anthropic_error_body(),
                DownstreamApi::OpenAiResponses => err.to_openai_error_body(),
            },
        }
    }

    pub(crate) fn openai_invalid(message: impl Into<String>) -> Self {
        Self::from_conversion_error(
            ConversionError::invalid(message),
            DownstreamApi::OpenAiResponses,
        )
    }

    fn invalid(message: &str) -> Self {
        Self::from_conversion_error(
            ConversionError::invalid(message),
            DownstreamApi::AnthropicMessages,
        )
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

/// Records the chat transcript for a synthesized Responses response when its stream
/// (or non-streaming body) finishes, so a later `previous_response_id` can continue it.
/// Also remembers the last upstream reasoning so a subsequent request whose client
/// echoes an empty reasoning item (Codex sends `{"summary": []}`) can reattach the
/// full reasoning_content — required by MiMo/DeepSeek on tool-call turns, else 400.
#[derive(Clone)]
pub struct ResponseRecorder {
    store: crate::response_state::ResponseStateStore,
    provider_name: String,
    model: String,
    /// The converted request's messages: previous transcript (if any) plus the
    /// messages produced from the current `input`.
    request_messages: Vec<Value>,
}

impl ResponseRecorder {
    pub fn new(
        store: crate::response_state::ResponseStateStore,
        provider_name: impl Into<String>,
        model: impl Into<String>,
        request_messages: Vec<Value>,
    ) -> Self {
        Self {
            store,
            provider_name: provider_name.into(),
            model: model.into(),
            request_messages,
        }
    }

    /// Store the transcript for `response_id` (appending the assistant turn when present)
    /// and remember the turn's reasoning for the next request.
    pub fn record(&self, response_id: &str, assistant_turn: Option<Value>) {
        if let Some(turn) = &assistant_turn {
            if let Some(reasoning) = turn.get("reasoning_content").and_then(Value::as_str) {
                if !reasoning.trim().is_empty() {
                    self.store
                        .set_last_reasoning(&self.provider_name, reasoning.to_string());
                }
            }
        }
        let mut messages = self.request_messages.clone();
        if let Some(turn) = assistant_turn {
            messages.push(turn);
        }
        let now = crate::response_state::now_unix();
        self.store.put(crate::response_state::ResponseState::new(
            response_id.to_string(),
            self.provider_name.clone(),
            self.model.clone(),
            messages,
            now,
            crate::response_state::DEFAULT_RESPONSE_STATE_TTL_SECS,
        ));
    }
}

/// Convert an upstream non-2xx JSON error body to the downstream dialect's error envelope.
pub fn convert_error_body(status: StatusCode, body: &Value, path: &str) -> Value {
    match downstream_api_for_path(path) {
        Some(DownstreamApi::AnthropicMessages) => convert_chat_error_body(status, body),
        Some(DownstreamApi::OpenAiResponses) => body.clone(),
        None => body.clone(),
    }
}

/// Convert a non-streaming upstream Chat Completions body into the downstream dialect.
/// Convert a non-streaming upstream response body into the downstream dialect,
/// dispatching on the upstream protocol (chat / Messages / Responses).
pub fn convert_non_streaming_body(
    body: Bytes,
    path: &str,
    upstream_api: UpstreamApi,
) -> Result<Bytes, std::io::Error> {
    let json: Value = serde_json::from_slice(&body).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "upstream response is not valid JSON",
        )
    })?;
    let downstream = downstream_api_for_path(path).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "unknown downstream path")
    })?;
    let converted = match (upstream_api, downstream) {
        // Chat upstream: existing conversion.
        (UpstreamApi::OpenAiChatCompletions | UpstreamApi::Passthrough, DownstreamApi::AnthropicMessages) => {
            convert_chat_response(&json)
        }
        (UpstreamApi::OpenAiChatCompletions | UpstreamApi::Passthrough, DownstreamApi::OpenAiResponses) => {
            convert_chat_response_to_responses(&json)
        }
        // Messages upstream (Anthropic message JSON): render downstream directly.
        (UpstreamApi::AnthropicMessages, DownstreamApi::AnthropicMessages) => {
            // The body is already Anthropic Messages; pass through as-is.
            return Ok(body);
        }
        (UpstreamApi::AnthropicMessages, DownstreamApi::OpenAiResponses) => {
            codex_provider_proxy_api_conversion::responses_renderer::convert_messages_response_to_responses(&json)
        }
        // Responses upstream: normalize to downstream.
        (UpstreamApi::OpenAiResponses, DownstreamApi::AnthropicMessages) => {
            codex_provider_proxy_api_conversion::messages_renderer::convert_responses_response_to_messages(&json)
        }
        (UpstreamApi::OpenAiResponses, DownstreamApi::OpenAiResponses) => {
            codex_provider_proxy_api_conversion::messages_renderer::convert_responses_response_to_responses(&json)
        }
    }
    .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    serde_json::to_vec(&converted)
        .map(Bytes::from)
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "failed to serialize converted response",
            )
        })
}

// Adapt an upstream SSE stream (Chat Completions chunks) into downstream Messages events.
pin_project! {
    pub struct ChatToMessagesStream<S> {
        #[pin]
        inner: S,
        parser: ChatParser,
        renderer: MessagesRenderer,
        pending: BytesMut,
        out_events: VecDeque<Bytes>,
        finished: bool,
    }
}

impl<S> ChatToMessagesStream<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            parser: ChatParser::new(),
            renderer: MessagesRenderer::new(),
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
                        if let Some(payload) = sse::first_data_payload(&event_bytes) {
                            if let Err(err) = this.parser.on_chunk(payload, &mut events) {
                                return Poll::Ready(Some(Err(std::io::Error::other(err))));
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

// Adapt an upstream SSE stream (Chat Completions chunks) into downstream Responses events.
pin_project! {
    pub struct ChatToResponsesStream<S> {
        #[pin]
        inner: S,
        parser: ChatParser,
        renderer: ResponsesRenderer,
        recorder: Option<ResponseRecorder>,
        pending: BytesMut,
        out_events: VecDeque<Bytes>,
        finished: bool,
    }
}

impl<S> ChatToResponsesStream<S> {
    /// Create the stream with an optional transcript recorder invoked when the
    /// upstream stream ends.
    pub fn with_recorder(inner: S, recorder: Option<ResponseRecorder>) -> Self {
        Self {
            inner,
            parser: ChatParser::new(),
            renderer: ResponsesRenderer::new(),
            recorder,
            pending: BytesMut::new(),
            out_events: VecDeque::new(),
            finished: false,
        }
    }
}

impl<S> Stream for ChatToResponsesStream<S>
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
                            if let Err(err) = this.parser.on_chunk(payload, &mut events) {
                                return Poll::Ready(Some(Err(std::io::Error::other(err))));
                            }
                        }
                    }
                    let mut rendered = Vec::new();
                    for event in &events {
                        this.renderer.on_event(event, &mut rendered);
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
                    let mut events = Vec::new();
                    this.parser.finish(&mut events);
                    let mut rendered = Vec::new();
                    for event in &events {
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

// Adapt a non-streaming upstream Chat Completions body into the downstream dialect,
// buffering the whole response (bounded by `max_bytes`).
pin_project! {
    pub struct NonStreamingConversionStream<S> {
        #[pin]
        inner: S,
        max_bytes: usize,
        path: String,
        upstream_api: UpstreamApi,
        recorder: Option<ResponseRecorder>,
        buffered: BytesMut,
        result: Option<Result<Bytes, std::io::Error>>,
        finished: bool,
    }
}

impl<S> NonStreamingConversionStream<S> {
    /// Create the stream with an optional transcript recorder invoked once the
    /// whole upstream body has been buffered and converted.
    pub fn with_recorder(
        inner: S,
        max_bytes: usize,
        path: String,
        upstream_api: UpstreamApi,
        recorder: Option<ResponseRecorder>,
    ) -> Self {
        Self {
            inner,
            max_bytes,
            path,
            upstream_api,
            recorder,
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
                    if let Some(recorder) = &this.recorder {
                        if let Ok(json) = serde_json::from_slice::<Value>(&buffered) {
                            let turn = chat_response_assistant_turn(&json);
                            let converted = convert_non_streaming_body(
                                buffered.clone(),
                                this.path,
                                *this.upstream_api,
                            );
                            if let Ok(converted) = converted {
                                if let Ok(body) = serde_json::from_slice::<Value>(&converted) {
                                    if let Some(response_id) =
                                        body.get("id").and_then(Value::as_str)
                                    {
                                        recorder.record(response_id, turn);
                                    }
                                }
                            }
                        }
                    }
                    *this.result = Some(convert_non_streaming_body(
                        buffered,
                        this.path,
                        *this.upstream_api,
                    ));
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
