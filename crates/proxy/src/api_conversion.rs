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
use codex_provider_proxy_api_conversion::messages_to_chat::convert_chat_response;
use codex_provider_proxy_api_conversion::request_ir::{Protocol, RequestIr};
use codex_provider_proxy_api_conversion::responses::{
    chat_response_assistant_turn, convert_chat_response_to_responses,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestParserKind {
    AnthropicMessages,
    OpenAIResponses,
}

impl RequestParserKind {
    fn protocol(self) -> Protocol {
        match self {
            Self::AnthropicMessages => Protocol::AnthropicMessages,
            Self::OpenAIResponses => Protocol::OpenAiResponses,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestRendererKind {
    OpenAIChatCompletions,
    AnthropicMessages,
    OpenAIResponses,
}

impl RequestRendererKind {
    fn protocol(self) -> Protocol {
        match self {
            Self::OpenAIChatCompletions => Protocol::OpenAiChatCompletions,
            Self::AnthropicMessages => Protocol::AnthropicMessages,
            Self::OpenAIResponses => Protocol::OpenAiResponses,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestNormalizerKind {
    ArkResponses,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestPipeline {
    pub parser: RequestParserKind,
    pub renderer: RequestRendererKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestAction {
    Passthrough {
        normalizer: Option<RequestNormalizerKind>,
    },
    Transform(RequestPipeline),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseParserKind {
    OpenAIChatCompletions,
    AnthropicMessages,
    OpenAIResponses,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseRendererKind {
    AnthropicMessages,
    OpenAIResponses,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponsePipeline {
    pub parser: ResponseParserKind,
    pub renderer: ResponseRendererKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConversionPlan {
    pub downstream: DownstreamApi,
    pub upstream: UpstreamApi,
    pub request: RequestAction,
    pub response: ResponsePipeline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnsupportedConversion {
    pub upstream: UpstreamApi,
    pub downstream: DownstreamApi,
}

/// The downstream dialect a forwarded path maps to, if any.
pub fn downstream_api_for_path(path: &str) -> Option<DownstreamApi> {
    match path.trim_matches('/').rsplit('/').next() {
        Some(MESSAGES_PATH) => Some(DownstreamApi::AnthropicMessages),
        Some(RESPONSES_PATH) => Some(DownstreamApi::OpenAIResponses),
        _ => None,
    }
}

pub fn resolve_conversion_plan(
    provider: &Provider,
    method: &Method,
    path: &str,
) -> Result<Option<ConversionPlan>, UnsupportedConversion> {
    if *method != Method::POST {
        return Ok(None);
    }
    let Some(downstream) = downstream_api_for_path(path) else {
        return Ok(None);
    };
    if !provider.accept_downstream_apis.contains(&downstream) {
        return Ok(None);
    }
    let plan = match (provider.upstream_api, downstream) {
        (UpstreamApi::Passthrough, _) => return Ok(None),
        (UpstreamApi::OpenAiChatCompletions, DownstreamApi::AnthropicMessages) => ConversionPlan {
            downstream,
            upstream: provider.upstream_api,
            request: RequestAction::Transform(RequestPipeline {
                parser: RequestParserKind::AnthropicMessages,
                renderer: RequestRendererKind::OpenAIChatCompletions,
            }),
            response: response_pipeline(provider.upstream_api, downstream),
        },
        (UpstreamApi::OpenAiChatCompletions, DownstreamApi::OpenAIResponses) => ConversionPlan {
            downstream,
            upstream: provider.upstream_api,
            request: RequestAction::Transform(RequestPipeline {
                parser: RequestParserKind::OpenAIResponses,
                renderer: RequestRendererKind::OpenAIChatCompletions,
            }),
            response: response_pipeline(provider.upstream_api, downstream),
        },
        (UpstreamApi::AnthropicMessages, DownstreamApi::AnthropicMessages) => ConversionPlan {
            downstream,
            upstream: provider.upstream_api,
            request: RequestAction::Passthrough { normalizer: None },
            response: response_pipeline(provider.upstream_api, downstream),
        },
        (UpstreamApi::AnthropicMessages, DownstreamApi::OpenAIResponses) => ConversionPlan {
            downstream,
            upstream: provider.upstream_api,
            request: RequestAction::Transform(RequestPipeline {
                parser: RequestParserKind::OpenAIResponses,
                renderer: RequestRendererKind::AnthropicMessages,
            }),
            response: response_pipeline(provider.upstream_api, downstream),
        },
        (UpstreamApi::OpenAIResponses, DownstreamApi::OpenAIResponses) => ConversionPlan {
            downstream,
            upstream: provider.upstream_api,
            request: RequestAction::Passthrough {
                normalizer: Some(RequestNormalizerKind::ArkResponses),
            },
            response: response_pipeline(provider.upstream_api, downstream),
        },
        (UpstreamApi::OpenAIResponses, DownstreamApi::AnthropicMessages) => ConversionPlan {
            downstream,
            upstream: provider.upstream_api,
            request: RequestAction::Transform(RequestPipeline {
                parser: RequestParserKind::AnthropicMessages,
                renderer: RequestRendererKind::OpenAIResponses,
            }),
            response: response_pipeline(provider.upstream_api, downstream),
        },
    };
    Ok(Some(plan))
}

/// Whether this provider converts the downstream dialect for the given path.
pub fn provider_converts_path(provider: &Provider, path: &str) -> bool {
    resolve_conversion_plan(provider, &Method::POST, path)
        .ok()
        .flatten()
        .is_some()
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
        UpstreamApi::OpenAIResponses => std::borrow::Cow::Borrowed(RESPONSES_UPSTREAM_PATH),
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
    cfg.providers.get(provider_name).is_some_and(|provider| {
        resolve_conversion_plan(provider, method, forwarded_path).is_ok_and(|plan| plan.is_some())
    })
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
        .unwrap_or_default()
        .to_owned();
    let caps = ModelCapabilities::resolve(
        &model,
        provider.default_capabilities.as_ref(),
        &provider.model_capabilities,
    );
    let plan = resolve_conversion_plan(provider, &Method::POST, path)
        .map_err(|_| RequestConversionRejected::openai_invalid("unsupported API conversion"))?
        .ok_or_else(|| RequestConversionRejected::openai_invalid("conversion is not enabled"))?;
    let (converted, report) = match plan.request {
        RequestAction::Passthrough { normalizer: None } => return Ok(body),
        RequestAction::Passthrough {
            normalizer: Some(RequestNormalizerKind::ArkResponses),
        } => (json, Default::default()),
        RequestAction::Transform(pipeline) => {
            let source = pipeline.parser.protocol();
            let request = if source == Protocol::OpenAiResponses {
                RequestIr::parse_with_context(source, &json, previous_messages, last_reasoning)
            } else {
                RequestIr::parse(source, &json)
            }
            .map_err(|err| {
                RequestConversionRejected::from_conversion_error(err, plan.downstream)
            })?;
            if pipeline.renderer == RequestRendererKind::OpenAIChatCompletions {
                request
                    .render_chat_with_capabilities(&caps, previous_messages, last_reasoning)
                    .map_err(|err| {
                        RequestConversionRejected::from_conversion_error(err, plan.downstream)
                    })?
            } else {
                let rendered = request
                    .render(pipeline.renderer.protocol(), &caps)
                    .map_err(|err| {
                        RequestConversionRejected::from_conversion_error(err, plan.downstream)
                    })?;
                (rendered, Default::default())
            }
        }
    };
    let mut converted = converted;
    if provider.upstream_api == UpstreamApi::OpenAIResponses && caps.ark_style {
        if let Some(object) = converted.as_object_mut() {
            codex_provider_proxy_api_conversion::responses::apply_ark_style_to_responses(object);
        }
    }
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
                DownstreamApi::OpenAIResponses => err.to_openai_error_body(),
            },
        }
    }

    pub(crate) fn openai_invalid(message: impl Into<String>) -> Self {
        Self::from_conversion_error(
            ConversionError::invalid(message),
            DownstreamApi::OpenAIResponses,
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
    /// The protocol-neutral request transcript for this response chain.
    request_ir: RequestIr,
}

impl ResponseRecorder {
    pub fn new(
        store: crate::response_state::ResponseStateStore,
        provider_name: impl Into<String>,
        model: impl Into<String>,
        request_ir: RequestIr,
    ) -> Self {
        Self {
            store,
            provider_name: provider_name.into(),
            model: model.into(),
            request_ir,
        }
    }

    /// Store the transcript for `response_id` (appending the assistant turn when present)
    /// together with reasoning belonging to that response chain.
    pub fn record(&self, response_id: &str, assistant_turn: Option<Value>) {
        let last_reasoning = assistant_turn
            .as_ref()
            .and_then(|turn| turn.get("reasoning_content"))
            .and_then(Value::as_str)
            .filter(|reasoning| !reasoning.trim().is_empty())
            .map(str::to_owned);
        let mut request_ir = self.request_ir.clone();
        if let Some(turn) = assistant_turn {
            request_ir.append_chat_assistant_turn(&turn);
        }
        let now = crate::response_state::now_unix();
        self.store.put(crate::response_state::ResponseState::new(
            response_id.to_string(),
            self.provider_name.clone(),
            self.model.clone(),
            request_ir,
            last_reasoning,
            now,
            crate::response_state::DEFAULT_RESPONSE_STATE_TTL_SECS,
        ));
    }
}

/// Convert an upstream non-2xx JSON error body to the downstream dialect's error envelope.
pub fn convert_error_body(status: StatusCode, body: &Value, path: &str) -> Value {
    match downstream_api_for_path(path) {
        Some(DownstreamApi::AnthropicMessages) => convert_chat_error_body(status, body),
        Some(DownstreamApi::OpenAIResponses) => body.clone(),
        None => body.clone(),
    }
}

/// Convert a non-streaming upstream response body into the downstream dialect,
/// dispatching on the upstream protocol (chat / Messages / Responses).
#[cfg(test)]
pub fn convert_non_streaming_body(
    body: Bytes,
    path: &str,
    upstream_api: UpstreamApi,
) -> Result<Bytes, std::io::Error> {
    let downstream = downstream_api_for_path(path).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "unknown downstream path")
    })?;
    convert_non_streaming_body_with_pipeline(body, response_pipeline(upstream_api, downstream))
}

pub fn convert_non_streaming_body_with_pipeline(
    body: Bytes,
    pipeline: ResponsePipeline,
) -> Result<Bytes, std::io::Error> {
    let json: Value = serde_json::from_slice(&body).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "upstream response is not valid JSON",
        )
    })?;
    let converted = match (pipeline.parser, pipeline.renderer) {
        (ResponseParserKind::OpenAIChatCompletions, ResponseRendererKind::AnthropicMessages) => {
            convert_chat_response(&json)
        }
        (ResponseParserKind::OpenAIChatCompletions, ResponseRendererKind::OpenAIResponses) => {
            convert_chat_response_to_responses(&json)
        }
        (ResponseParserKind::AnthropicMessages, ResponseRendererKind::AnthropicMessages) => {
            return Ok(body);
        }
        (ResponseParserKind::AnthropicMessages, ResponseRendererKind::OpenAIResponses) => {
            codex_provider_proxy_api_conversion::responses_renderer::convert_messages_response_to_responses(&json)
        }
        (ResponseParserKind::OpenAIResponses, ResponseRendererKind::AnthropicMessages) => {
            codex_provider_proxy_api_conversion::messages_renderer::convert_responses_response_to_messages(&json)
        }
        (ResponseParserKind::OpenAIResponses, ResponseRendererKind::OpenAIResponses) => {
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

pub fn response_pipeline(upstream: UpstreamApi, downstream: DownstreamApi) -> ResponsePipeline {
    ResponsePipeline {
        parser: match upstream {
            UpstreamApi::AnthropicMessages => ResponseParserKind::AnthropicMessages,
            UpstreamApi::OpenAIResponses => ResponseParserKind::OpenAIResponses,
            UpstreamApi::OpenAiChatCompletions | UpstreamApi::Passthrough => {
                ResponseParserKind::OpenAIChatCompletions
            }
        },
        renderer: match downstream {
            DownstreamApi::AnthropicMessages => ResponseRendererKind::AnthropicMessages,
            DownstreamApi::OpenAIResponses => ResponseRendererKind::OpenAIResponses,
        },
    }
}

trait ChatStreamTarget {
    fn on_event(
        &mut self,
        event: &codex_provider_proxy_api_conversion::stream::StreamEvent,
        out: &mut Vec<Bytes>,
    );

    fn finish(&mut self, out: &mut Vec<Bytes>);
}

#[doc(hidden)]
pub struct MessagesStreamTarget(MessagesRenderer);

impl ChatStreamTarget for MessagesStreamTarget {
    fn on_event(
        &mut self,
        event: &codex_provider_proxy_api_conversion::stream::StreamEvent,
        out: &mut Vec<Bytes>,
    ) {
        self.0.on_event(event, out);
    }

    fn finish(&mut self, out: &mut Vec<Bytes>) {
        self.0.finish(out);
    }
}

#[doc(hidden)]
pub struct ResponsesStreamTarget {
    renderer: ResponsesRenderer,
    recorder: Option<ResponseRecorder>,
}

impl ChatStreamTarget for ResponsesStreamTarget {
    fn on_event(
        &mut self,
        event: &codex_provider_proxy_api_conversion::stream::StreamEvent,
        out: &mut Vec<Bytes>,
    ) {
        self.renderer.on_event(event, out);
    }

    fn finish(&mut self, out: &mut Vec<Bytes>) {
        self.renderer.finish(out);
        if let (Some(recorder), Some(response_id)) = (&self.recorder, self.renderer.response_id()) {
            recorder.record(response_id, self.renderer.assistant_turn().cloned());
        }
    }
}

pin_project! {
    pub struct ChatConversionStream<S, T> {
        #[pin]
        inner: S,
        parser: ChatParser,
        target: T,
        pending: BytesMut,
        out_events: VecDeque<Bytes>,
        finished: bool,
    }
}

pub type ChatToMessagesStream<S> = ChatConversionStream<S, MessagesStreamTarget>;
pub type ChatToResponsesStream<S> = ChatConversionStream<S, ResponsesStreamTarget>;

impl<S> ChatConversionStream<S, MessagesStreamTarget> {
    pub fn new(inner: S) -> Self {
        Self::from_target(inner, MessagesStreamTarget(MessagesRenderer::new()))
    }
}

impl<S> ChatConversionStream<S, ResponsesStreamTarget> {
    /// Create the stream with an optional transcript recorder invoked when the
    /// upstream stream ends.
    pub fn with_recorder(inner: S, recorder: Option<ResponseRecorder>) -> Self {
        Self::from_target(
            inner,
            ResponsesStreamTarget {
                renderer: ResponsesRenderer::new(),
                recorder,
            },
        )
    }
}

impl<S, T> ChatConversionStream<S, T> {
    fn from_target(inner: S, target: T) -> Self {
        Self {
            inner,
            parser: ChatParser::new(),
            target,
            pending: BytesMut::new(),
            out_events: VecDeque::new(),
            finished: false,
        }
    }
}

impl<S, T> Stream for ChatConversionStream<S, T>
where
    S: Stream<Item = Result<Bytes, std::io::Error>>,
    T: ChatStreamTarget,
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
                        this.target.on_event(event, &mut rendered);
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
                        this.target.on_event(event, &mut rendered);
                    }
                    this.target.finish(&mut rendered);
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
        pipeline: ResponsePipeline,
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
        pipeline: ResponsePipeline,
        recorder: Option<ResponseRecorder>,
    ) -> Self {
        Self {
            inner,
            max_bytes,
            pipeline,
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
                            let converted = convert_non_streaming_body_with_pipeline(
                                buffered.clone(),
                                *this.pipeline,
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
                    *this.result = Some(convert_non_streaming_body_with_pipeline(
                        buffered,
                        *this.pipeline,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn provider(upstream: UpstreamApi, downstream: DownstreamApi) -> Provider {
        Provider {
            upstream_api: upstream,
            accept_downstream_apis: vec![downstream],
            ..Provider::default()
        }
    }

    #[test]
    fn resolves_supported_conversion_matrix() {
        let cases = [
            (
                UpstreamApi::OpenAiChatCompletions,
                DownstreamApi::AnthropicMessages,
                "/v1/messages",
                RequestAction::Transform(RequestPipeline {
                    parser: RequestParserKind::AnthropicMessages,
                    renderer: RequestRendererKind::OpenAIChatCompletions,
                }),
                ResponsePipeline {
                    parser: ResponseParserKind::OpenAIChatCompletions,
                    renderer: ResponseRendererKind::AnthropicMessages,
                },
            ),
            (
                UpstreamApi::OpenAiChatCompletions,
                DownstreamApi::OpenAIResponses,
                "/v1/responses",
                RequestAction::Transform(RequestPipeline {
                    parser: RequestParserKind::OpenAIResponses,
                    renderer: RequestRendererKind::OpenAIChatCompletions,
                }),
                ResponsePipeline {
                    parser: ResponseParserKind::OpenAIChatCompletions,
                    renderer: ResponseRendererKind::OpenAIResponses,
                },
            ),
            (
                UpstreamApi::AnthropicMessages,
                DownstreamApi::AnthropicMessages,
                "/v1/messages",
                RequestAction::Passthrough { normalizer: None },
                ResponsePipeline {
                    parser: ResponseParserKind::AnthropicMessages,
                    renderer: ResponseRendererKind::AnthropicMessages,
                },
            ),
            (
                UpstreamApi::OpenAIResponses,
                DownstreamApi::OpenAIResponses,
                "/v1/responses",
                RequestAction::Passthrough {
                    normalizer: Some(RequestNormalizerKind::ArkResponses),
                },
                ResponsePipeline {
                    parser: ResponseParserKind::OpenAIResponses,
                    renderer: ResponseRendererKind::OpenAIResponses,
                },
            ),
        ];

        for (upstream, downstream, path, request, response) in cases {
            let plan =
                resolve_conversion_plan(&provider(upstream, downstream), &Method::POST, path)
                    .unwrap()
                    .unwrap();
            assert_eq!(plan.request, request);
            assert_eq!(plan.response, response);
        }
    }

    #[test]
    fn accepts_cross_protocol_requests() {
        for (upstream, downstream, path) in [
            (
                UpstreamApi::AnthropicMessages,
                DownstreamApi::OpenAIResponses,
                "/v1/responses",
            ),
            (
                UpstreamApi::OpenAIResponses,
                DownstreamApi::AnthropicMessages,
                "/v1/messages",
            ),
        ] {
            assert!(
                resolve_conversion_plan(&provider(upstream, downstream), &Method::POST, path,)
                    .unwrap()
                    .is_some()
            );
        }
    }

    #[test]
    fn normalizes_converted_messages_request_for_ark_responses_upstream() {
        let config = Config::from_toml_str(
            r#"
default_provider = "ark"
listen_addr = "127.0.0.1:0"
[providers.ark]
base_url = "https://ark.example.com/"
api_key = "test"
upstream_api = "openai_responses"
accept_downstream_apis = ["anthropic_messages"]
[providers.ark.capabilities]
ark_style = true
reasoning_effort = { enabled = true, levels = ["high"], default = "high" }
"#,
        )
        .unwrap();
        let body = serde_json::json!({
            "model": "ark-model",
            "max_tokens": 64,
            "messages": [{"role": "user", "content": "hello"}],
            "output_config": {"effort": "high"}
        });
        let converted = convert_request_body(
            &config,
            "ark",
            "/v1/messages",
            Bytes::from(serde_json::to_vec(&body).unwrap()),
            None,
            None,
        )
        .unwrap();
        let converted: Value = serde_json::from_slice(&converted).unwrap();
        assert_eq!(converted["thinking"]["type"], "enabled");
        assert!(converted.get("reasoning").is_none());
    }

    #[test]
    fn ignores_non_post_and_unaccepted_paths() {
        let provider = provider(
            UpstreamApi::OpenAiChatCompletions,
            DownstreamApi::OpenAIResponses,
        );
        assert_eq!(
            resolve_conversion_plan(&provider, &Method::GET, "/v1/responses").unwrap(),
            None
        );
        assert_eq!(
            resolve_conversion_plan(&provider, &Method::POST, "/v1/messages").unwrap(),
            None
        );
    }
}
