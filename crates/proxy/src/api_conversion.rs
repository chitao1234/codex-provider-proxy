//! Proxy-side integration of the API conversion layer.
//!
//! Decides when a request must be converted (provider declares an upstream Chat Completions
//! API and accepts downstream Anthropic Messages and/or OpenAI Responses), converts the
//! request body after the model-mapping rewrite, and adapts upstream response streams back
//! to the downstream dialect.

use std::{
    fmt,
    pin::Pin,
    task::{Context as TaskContext, Poll},
};

use bytes::{Bytes, BytesMut};
pub use codex_provider_proxy_api_conversion::dialect::DownstreamApi;
use codex_provider_proxy_api_conversion::dialect::{ModelCapabilities, UpstreamApi};
use codex_provider_proxy_api_conversion::error::ApiErrorIr;
use codex_provider_proxy_api_conversion::error::ConversionError;
use codex_provider_proxy_api_conversion::ir::ResponseAccumulator;
pub use codex_provider_proxy_api_conversion::plan::{
    ContinuationPolicy, ConversionPlan, RequestAction, ResponsePipeline, StreamPolicy,
};
use codex_provider_proxy_api_conversion::protocol::Protocol;
use codex_provider_proxy_api_conversion::provider_normalizer::RequestNormalizerKind;
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

fn downstream_protocol(downstream: DownstreamApi) -> Protocol {
    match downstream {
        DownstreamApi::AnthropicMessages => Protocol::AnthropicMessages,
        DownstreamApi::OpenAiResponses => Protocol::OpenAiResponses,
    }
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
        Some(RESPONSES_PATH) => Some(DownstreamApi::OpenAiResponses),
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
    let Some(upstream_protocol) = upstream_protocol(provider.upstream_api) else {
        return Ok(None);
    };
    let downstream_protocol = downstream_protocol(downstream);
    let normalizer = (upstream_protocol == Protocol::OpenAiResponses)
        .then_some(RequestNormalizerKind::ArkResponses);
    let plan = ConversionPlan::new(
        downstream,
        provider.upstream_api,
        downstream_protocol,
        upstream_protocol,
        normalizer,
    );
    Ok(Some(plan))
}

fn upstream_protocol(upstream: UpstreamApi) -> Option<Protocol> {
    match upstream {
        UpstreamApi::Passthrough => None,
        UpstreamApi::OpenAiChatCompletions => Some(Protocol::OpenAiChatCompletions),
        UpstreamApi::AnthropicMessages => Some(Protocol::AnthropicMessages),
        UpstreamApi::OpenAiResponses => Some(Protocol::OpenAiResponses),
    }
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
    cfg.providers.get(provider_name).is_some_and(|provider| {
        resolve_conversion_plan(provider, method, forwarded_path).is_ok_and(|plan| plan.is_some())
    })
}

/// Convert a (model-mapped) downstream request body into Chat Completions form using the
/// provider's per-model capabilities, dispatching on the downstream dialect.
///
/// `previous_request` carries the protocol-neutral transcript stored for `previous_response_id`
/// when the downstream request continues a Responses conversation. `last_reasoning`
/// carries the most recent upstream reasoning, reattached when the client echoes an
/// empty reasoning item (Codex sends `{"summary": []}`) — required by MiMo/DeepSeek
/// on tool-call turns, else 400.
pub fn convert_request_body(
    cfg: &Config,
    provider_name: &str,
    path: &str,
    body: Bytes,
    previous_request: Option<&codex_provider_proxy_api_conversion::ir::RequestIr>,
    last_reasoning: Option<&str>,
) -> Result<Bytes, RequestConversionRejected> {
    let provider = cfg
        .providers
        .get(provider_name)
        .expect("request conversion requires a resolved provider");
    let plan = resolve_conversion_plan(provider, &Method::POST, path)
        .map_err(|_| RequestConversionRejected::openai_invalid("unsupported API conversion"))?
        .ok_or_else(|| RequestConversionRejected::openai_invalid("conversion is not enabled"))?;
    let source = downstream_protocol(plan.downstream);
    let same_protocol = match plan.request {
        RequestAction::Passthrough => true,
        RequestAction::Transform(pipeline) => pipeline.target == source,
    };
    let json: Value = match serde_json::from_slice(&body) {
        Ok(json) => json,
        Err(_) if same_protocol => return Ok(body),
        Err(_) => {
            return Err(RequestConversionRejected::invalid(
                "request body is not valid JSON",
            ));
        }
    };
    if !json.is_object() {
        if same_protocol {
            return Ok(body);
        }
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
    let (converted, report) = match plan.request {
        RequestAction::Passthrough => return Ok(body),
        RequestAction::Transform(pipeline) => {
            if source == pipeline.target
                && pipeline.normalizer == Some(RequestNormalizerKind::ArkResponses)
                && !caps.ark_style
            {
                return Ok(body);
            }
            let request = match plan.continuation {
                ContinuationPolicy::None => pipeline.parser.parse(&json),
                ContinuationPolicy::ResponsesState => {
                    pipeline
                        .parser
                        .parse_with_context(&json, previous_request, last_reasoning)
                }
            }
            .map_err(|err| {
                RequestConversionRejected::from_conversion_error(err, plan.downstream)
            })?;
            let rendered_request = pipeline.renderer.render(&request, &caps).map_err(|err| {
                RequestConversionRejected::from_conversion_error(err, plan.downstream)
            })?;
            let rendered = rendered_request.body;
            let report = rendered_request.report;
            let rendered = if let Some(normalizer) = pipeline.normalizer {
                normalizer
                    .normalize(&request, &caps, pipeline.target, rendered)
                    .map_err(|err| {
                        RequestConversionRejected::from_conversion_error(err, plan.downstream)
                    })?
            } else {
                rendered
            };
            (rendered, report)
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
    request_ir: codex_provider_proxy_api_conversion::ir::RequestIr,
}

impl ResponseRecorder {
    pub fn new(
        store: crate::response_state::ResponseStateStore,
        provider_name: impl Into<String>,
        model: impl Into<String>,
        request_ir: codex_provider_proxy_api_conversion::ir::RequestIr,
    ) -> Self {
        Self {
            store,
            provider_name: provider_name.into(),
            model: model.into(),
            request_ir,
        }
    }

    pub fn record_semantic(
        &self,
        response_id: &str,
        response: &codex_provider_proxy_api_conversion::ir::ResponseIr,
    ) {
        let mut request = self.request_ir.clone();
        request.append_response(response);
        let last_reasoning = response.output.iter().rev().find_map(|item| match item {
            codex_provider_proxy_api_conversion::ir::ContentItem::Reasoning { content, .. } => {
                content.iter().rev().find_map(|part| match part.semantic() {
                    codex_provider_proxy_api_conversion::ir::ContentPart::Reasoning {
                        text,
                        ..
                    } if !text.trim().is_empty() => Some(text.clone()),
                    _ => None,
                })
            }
            _ => None,
        });
        let now = crate::response_state::now_unix();
        let state = crate::response_state::ResponseState::new(
            response_id.to_owned(),
            self.provider_name.clone(),
            self.model.clone(),
            request,
            last_reasoning,
            now,
            crate::response_state::DEFAULT_RESPONSE_STATE_TTL_SECS,
        );
        self.store.put(state);
    }
}

/// Convert an upstream non-2xx JSON error body to the downstream dialect's error envelope.
pub fn convert_error_body(status: StatusCode, body: &Value, pipeline: ResponsePipeline) -> Value {
    if pipeline.source == pipeline.target {
        return body.clone();
    }
    match pipeline.target {
        Protocol::AnthropicMessages => ApiErrorIr::parse(status, body).render_anthropic(),
        Protocol::OpenAiResponses => ApiErrorIr::parse(status, body).render_openai(),
        Protocol::OpenAiChatCompletions => body.clone(),
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

#[cfg(test)]
fn convert_non_streaming_body_with_pipeline(
    body: Bytes,
    pipeline: ResponsePipeline,
) -> Result<Bytes, std::io::Error> {
    convert_non_streaming_response(body, pipeline).map(|converted| converted.body)
}

struct ConvertedNonStreamingResponse {
    body: Bytes,
    response_id: Option<String>,
    semantic_response: Option<codex_provider_proxy_api_conversion::ir::ResponseIr>,
}

fn convert_non_streaming_response(
    body: Bytes,
    pipeline: ResponsePipeline,
) -> Result<ConvertedNonStreamingResponse, std::io::Error> {
    if pipeline.source == pipeline.target {
        let semantic_response = (pipeline.source == Protocol::OpenAiResponses)
            .then(|| {
                serde_json::from_slice::<Value>(&body)
                    .ok()
                    .and_then(|json| {
                        codex_provider_proxy_api_conversion::protocols::parse_response(
                            pipeline.source,
                            &json,
                        )
                        .ok()
                    })
            })
            .flatten();
        let response_id = semantic_response
            .as_ref()
            .and_then(|response| response.response_id.clone());
        return Ok(ConvertedNonStreamingResponse {
            body,
            response_id,
            semantic_response,
        });
    }
    let json: Value = serde_json::from_slice(&body).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "upstream response is not valid JSON",
        )
    })?;
    let semantic_response =
        codex_provider_proxy_api_conversion::protocols::parse_response(pipeline.source, &json)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    let converted = codex_provider_proxy_api_conversion::protocols::render_response(
        pipeline.target,
        &semantic_response,
    )
    .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    let response_id = converted
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_owned);
    let body = serde_json::to_vec(&converted)
        .map(Bytes::from)
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "failed to serialize converted response",
            )
        })?;
    Ok(ConvertedNonStreamingResponse {
        body,
        response_id,
        semantic_response: Some(semantic_response),
    })
}

#[cfg(test)]
pub fn response_pipeline(upstream: UpstreamApi, downstream: DownstreamApi) -> ResponsePipeline {
    ResponsePipeline {
        source: match upstream {
            UpstreamApi::AnthropicMessages => Protocol::AnthropicMessages,
            UpstreamApi::OpenAiResponses => Protocol::OpenAiResponses,
            UpstreamApi::OpenAiChatCompletions | UpstreamApi::Passthrough => {
                Protocol::OpenAiChatCompletions
            }
        },
        target: downstream_protocol(downstream),
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
        core_response: ResponseAccumulator,
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
            core_response: ResponseAccumulator::default(),
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
                    *this.result = Some(
                        convert_non_streaming_response(buffered, *this.pipeline).map(|converted| {
                            if let Some(response) = converted.semantic_response.as_ref() {
                                *this.core_response = codex_provider_proxy_api_conversion::ir::ResponseAccumulator::from_response(response.clone());
                            }
                            if let (Some(recorder), Some(response_id), Some(response)) = (
                                this.recorder.as_ref(),
                                converted.response_id.as_deref(),
                                converted
                                    .semantic_response
                                    .as_ref()
                                    .filter(|response| {
                                        response.error.is_none()
                                            && response
                                                .stop_reason
                                                .as_ref()
                                                .is_some_and(|reason| reason.is_successful())
                                    }),
                            ) {
                                recorder.record_semantic(response_id, response);
                            }
                            converted.body
                        }),
                    );
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
    use codex_provider_proxy_api_conversion::plan::{ExtensionPolicy, RequestPipeline};
    use codex_provider_proxy_api_conversion::request::{RequestParserKind, RequestRendererKind};
    use futures_util::{stream, StreamExt};

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
                    source: Protocol::AnthropicMessages,
                    target: Protocol::OpenAiChatCompletions,
                    parser: codex_provider_proxy_api_conversion::request::RequestParserKind::AnthropicMessages,
                    renderer: codex_provider_proxy_api_conversion::request::RequestRendererKind::OpenAiChatCompletions,
                    normalizer: None,
                }),
                ResponsePipeline {
                    source: Protocol::OpenAiChatCompletions,
                    target: Protocol::AnthropicMessages,
                },
            ),
            (
                UpstreamApi::OpenAiChatCompletions,
                DownstreamApi::OpenAiResponses,
                "/v1/responses",
                RequestAction::Transform(RequestPipeline {
                    source: Protocol::OpenAiResponses,
                    target: Protocol::OpenAiChatCompletions,
                    parser: codex_provider_proxy_api_conversion::request::RequestParserKind::OpenAiResponses,
                    renderer: codex_provider_proxy_api_conversion::request::RequestRendererKind::OpenAiChatCompletions,
                    normalizer: None,
                }),
                ResponsePipeline {
                    source: Protocol::OpenAiChatCompletions,
                    target: Protocol::OpenAiResponses,
                },
            ),
            (
                UpstreamApi::AnthropicMessages,
                DownstreamApi::AnthropicMessages,
                "/v1/messages",
                RequestAction::Passthrough,
                ResponsePipeline {
                    source: Protocol::AnthropicMessages,
                    target: Protocol::AnthropicMessages,
                },
            ),
            (
                UpstreamApi::OpenAiResponses,
                DownstreamApi::OpenAiResponses,
                "/v1/responses",
                RequestAction::Transform(RequestPipeline {
                    source: Protocol::OpenAiResponses,
                    target: Protocol::OpenAiResponses,
                    parser: codex_provider_proxy_api_conversion::request::RequestParserKind::OpenAiResponses,
                    renderer: codex_provider_proxy_api_conversion::request::RequestRendererKind::OpenAiResponses,
                    normalizer: Some(RequestNormalizerKind::ArkResponses),
                }),
                ResponsePipeline {
                    source: Protocol::OpenAiResponses,
                    target: Protocol::OpenAiResponses,
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
            assert_eq!(
                plan.continuation,
                if downstream == DownstreamApi::OpenAiResponses {
                    ContinuationPolicy::ResponsesState
                } else {
                    ContinuationPolicy::None
                }
            );
            assert_eq!(
                plan.stream,
                if response.source == response.target {
                    StreamPolicy::Passthrough
                } else {
                    StreamPolicy::Translate
                }
            );
            assert_eq!(plan.extensions, ExtensionPolicy::PreserveScopedOpaque);
        }
    }

    #[test]
    fn non_ark_responses_request_is_byte_transparent() {
        let config = Config::from_toml_str(
            r#"
default_provider = "responses"
listen_addr = "127.0.0.1:0"
[providers.responses]
base_url = "https://example.com/"
api_key = "test"
upstream_api = "openai_responses"
accept_downstream_apis = ["openai_responses"]
"#,
        )
        .unwrap();
        let body = Bytes::from_static(
            br#"{
  "model": "m",
  "input": "hello",
  "x_provider": {"preserve": true}
}"#,
        );

        let converted = convert_request_body(
            &config,
            "responses",
            "/v1/responses",
            body.clone(),
            None,
            None,
        )
        .unwrap();

        assert_eq!(converted, body);

        for body in [Bytes::from_static(b"not-json"), Bytes::from_static(b"[]")] {
            let converted = convert_request_body(
                &config,
                "responses",
                "/v1/responses",
                body.clone(),
                None,
                None,
            )
            .unwrap();
            assert_eq!(converted, body);
        }
    }

    #[test]
    fn accepts_cross_protocol_requests() {
        for (upstream, downstream, path) in [
            (
                UpstreamApi::AnthropicMessages,
                DownstreamApi::OpenAiResponses,
                "/v1/responses",
            ),
            (
                UpstreamApi::OpenAiResponses,
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
    fn ark_responses_server_tools_render_native_template() {
        let config = Config::from_toml_str(
            r#"
default_provider = "ark"
listen_addr = "127.0.0.1:0"
[providers.ark]
base_url = "https://ark.example.com/"
api_key = "test"
upstream_api = "openai_responses"
accept_downstream_apis = ["openai_responses"]
[providers.ark.capabilities]
ark_style = true
server_tools = "provider_native"
search_tool_template = { type = "web_search", web_search = { search_context_size = "medium" } }
"#,
        )
        .unwrap();
        let body = serde_json::json!({
            "model": "ark-model",
            "input": [{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}],
            "tools": [{"type": "web_search_preview"}]
        });
        let converted = convert_request_body(
            &config,
            "ark",
            "/v1/responses",
            Bytes::from(serde_json::to_vec(&body).unwrap()),
            None,
            None,
        )
        .unwrap();
        let converted: Value = serde_json::from_slice(&converted).unwrap();
        let tools = converted["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["type"], "web_search");
        // Ark declares the tool flat; normalization flattens the template's OpenAI
        // sub-object and maps search_context_size to max_keyword.
        assert!(
            tools[0].get("web_search").is_none(),
            "nested object flattened"
        );
        assert_eq!(tools[0]["max_keyword"], 3);
    }

    #[test]
    fn ark_explicit_cache_injection_respects_constraints() {
        let config = Config::from_toml_str(
            r#"
default_provider = "ark"
listen_addr = "127.0.0.1:0"
[providers.ark]
base_url = "https://ark.example.com/"
api_key = "test"
upstream_api = "openai_responses"
accept_downstream_apis = ["openai_responses"]
[providers.ark.capabilities]
ark_style = true
cache = { ark_explicit_cache = true }
"#,
        )
        .unwrap();
        let request = |input: Value| serde_json::json!({"model": "ark-model", "input": input});
        let render = |body: Value| {
            let converted = convert_request_body(
                &config,
                "ark",
                "/v1/responses",
                Bytes::from(serde_json::to_vec(&body).unwrap()),
                None,
                None,
            )
            .unwrap();
            serde_json::from_slice::<Value>(&converted).unwrap()
        };

        // Streaming first round: session caching only (prefix creation needs stream=false).
        let mut body = request(serde_json::json!([
            {"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}
        ]));
        body["stream"] = serde_json::json!(true);
        let out = render(body);
        assert_eq!(out["caching"]["type"], "enabled");
        assert!(out["caching"].get("prefix").is_none());

        // Non-streaming first round: prefix caching + store.
        let mut body = request(serde_json::json!([
            {"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}
        ]));
        body["stream"] = serde_json::json!(false);
        let out = render(body);
        assert_eq!(out["caching"]["type"], "enabled");
        assert_eq!(out["caching"]["prefix"], true);
        assert_eq!(out["store"], true);

        // Continuation round: session caching + previous_response_id passthrough.
        let body = serde_json::json!({
            "model": "ark-model",
            "previous_response_id": "resp_ark_upstream_id",
            "input": "continue"
        });
        let out = render(body);
        assert_eq!(out["caching"]["type"], "enabled");
        assert_eq!(out["previous_response_id"], "resp_ark_upstream_id");

        // Tools present: caching skipped (tools only allowed on the first round).
        let body = serde_json::json!({
            "model": "ark-model",
            "input": "hi",
            "tools": [{"type": "function", "name": "f", "parameters": {"type": "object"}}]
        });
        let out = render(body);
        assert!(out.get("caching").is_none());

        // Instructions present: caching skipped (must be empty to write/hit).
        let body = serde_json::json!({
            "model": "ark-model",
            "input": "hi",
            "instructions": "be precise"
        });
        let out = render(body);
        assert!(out.get("caching").is_none());

        // Capability off: nothing injected.
        let config = Config::from_toml_str(
            r#"
default_provider = "ark"
listen_addr = "127.0.0.1:0"
[providers.ark]
base_url = "https://ark.example.com/"
api_key = "test"
upstream_api = "openai_responses"
accept_downstream_apis = ["openai_responses"]
[providers.ark.capabilities]
ark_style = true
"#,
        )
        .unwrap();
        let out = convert_request_body(
            &config,
            "ark",
            "/v1/responses",
            Bytes::from(serde_json::to_vec(&request(serde_json::json!([
                {"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}
            ]))).unwrap()),
            None,
            None,
        )
        .unwrap();
        let out: Value = serde_json::from_slice(&out).unwrap();
        assert!(out.get("caching").is_none());
    }

    #[test]
    fn preserves_previous_response_id_for_responses_translation() {
        let config = Config::from_toml_str(
            r#"
default_provider = "responses"
listen_addr = "127.0.0.1:0"
[providers.responses]
base_url = "https://responses.example.com/"
api_key = "test"
upstream_api = "openai_responses"
accept_downstream_apis = ["openai_responses"]
"#,
        )
        .unwrap();
        let body = serde_json::json!({
            "model": "m",
            "previous_response_id": "resp-local",
            "input": "continue"
        });
        let converted = convert_request_body(
            &config,
            "responses",
            "/v1/responses",
            Bytes::from(serde_json::to_vec(&body).unwrap()),
            None,
            None,
        )
        .unwrap();
        let converted: Value = serde_json::from_slice(&converted).unwrap();
        assert_eq!(converted["previous_response_id"], "resp-local");
    }

    #[test]
    fn ignores_non_post_and_unaccepted_paths() {
        let provider = provider(
            UpstreamApi::OpenAiChatCompletions,
            DownstreamApi::OpenAiResponses,
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

    #[test]
    fn same_protocol_non_streaming_responses_preserve_original_bytes() {
        let body = Bytes::from_static(
            b"{ \"id\": \"resp_1\", \"model\": \"m\", \"status\": \"completed\", \"output\": [], \"vendor_field\": {\"x\":1} }\n",
        );
        let converted = convert_non_streaming_response(
            body.clone(),
            ResponsePipeline {
                source: Protocol::OpenAiResponses,
                target: Protocol::OpenAiResponses,
            },
        )
        .unwrap();

        assert_eq!(converted.body, body);
        assert_eq!(converted.response_id.as_deref(), Some("resp_1"));
        assert!(converted.semantic_response.is_some());
    }

    #[test]
    fn same_protocol_non_streaming_passthrough_ignores_invalid_json() {
        for (source, target) in [
            (
                Protocol::AnthropicMessages,
                DownstreamApi::AnthropicMessages,
            ),
            (Protocol::OpenAiResponses, DownstreamApi::OpenAiResponses),
        ] {
            let body = Bytes::from_static(b"provider returned non-json diagnostics");
            let converted = convert_non_streaming_response(
                body.clone(),
                ResponsePipeline {
                    source,
                    target: downstream_protocol(target),
                },
            )
            .unwrap();
            assert_eq!(converted.body, body);
            assert!(converted.semantic_response.is_none());
        }
    }

    #[tokio::test]
    async fn non_streaming_recorder_consumes_neutral_response_ir() {
        let cases = [
            (
                Protocol::AnthropicMessages,
                serde_json::json!({
                    "id": "msg_1",
                    "model": "m",
                    "content": [
                        {"type":"thinking", "thinking":"thought", "signature":"sig"},
                        {"type":"text", "text":"answer"},
                        {"type":"tool_use", "id":"call_1", "name":"lookup", "input":{"q":1}}
                    ],
                    "stop_reason": "tool_use",
                    "usage": {"input_tokens": 1, "output_tokens": 2}
                }),
                "resp_msg_1",
            ),
            (
                Protocol::OpenAiResponses,
                serde_json::json!({
                    "id": "resp_1",
                    "model": "m",
                    "status": "completed",
                    "output": [
                        {"id":"rs_1", "type":"reasoning", "status":"completed", "summary":[{"type":"summary_text", "text":"thought"}]},
                        {"id":"msg_1", "type":"message", "status":"completed", "role":"assistant", "content":[{"type":"output_text", "text":"answer"}]},
                        {"id":"fc_1", "type":"function_call", "status":"completed", "call_id":"call_1", "name":"lookup", "arguments":"{\"q\":1}"}
                    ],
                    "usage": {"input_tokens": 1, "output_tokens": 2}
                }),
                "resp_1",
            ),
        ];

        for (source, body, expected_id) in cases {
            let store = crate::response_state::ResponseStateStore::new(4);
            let request_ir = RequestParserKind::OpenAiResponses
                .parse(&serde_json::json!({"model":"m", "input":"hi"}))
                .unwrap();
            let recorder = ResponseRecorder::new(store.clone(), "provider", "m", request_ir);
            let inner = stream::iter([Ok::<_, std::io::Error>(Bytes::from(
                serde_json::to_vec(&body).unwrap(),
            ))]);
            let mut converted = NonStreamingConversionStream::with_recorder(
                inner,
                1024 * 1024,
                ResponsePipeline {
                    source,
                    target: Protocol::OpenAiResponses,
                },
                Some(recorder),
            );
            while let Some(chunk) = converted.next().await {
                chunk.unwrap();
            }

            let state = store.get(expected_id).expect("response state recorded");
            assert_eq!(state.last_reasoning.as_deref(), Some("thought"));
            let rendered = RequestRendererKind::OpenAiChatCompletions
                .render(&state.request_ir, &ModelCapabilities::default())
                .unwrap()
                .body;
            let messages = rendered["messages"].as_array().unwrap();
            assert_eq!(messages[1]["reasoning_content"], "thought");
            assert_eq!(messages[1]["content"][0]["text"], "answer");
            assert_eq!(messages[1]["tool_calls"][0]["id"], "call_1");
        }
    }

    #[tokio::test]
    async fn non_streaming_recorder_skips_non_continuable_responses() {
        for (status, error) in [
            ("in_progress", Value::Null),
            ("failed", serde_json::json!({"message":"boom"})),
        ] {
            let store = crate::response_state::ResponseStateStore::new(4);
            let request_ir = RequestParserKind::OpenAiResponses
                .parse(&serde_json::json!({"model":"m", "input":"hi"}))
                .unwrap();
            let recorder = ResponseRecorder::new(store.clone(), "provider", "m", request_ir);
            let body = serde_json::json!({
                "id": "resp_1",
                "model": "m",
                "status": status,
                "error": error,
                "output": []
            });
            let inner = stream::iter([Ok::<_, std::io::Error>(Bytes::from(
                serde_json::to_vec(&body).unwrap(),
            ))]);
            let mut converted = NonStreamingConversionStream::with_recorder(
                inner,
                1024 * 1024,
                ResponsePipeline {
                    source: Protocol::OpenAiResponses,
                    target: Protocol::OpenAiResponses,
                },
                Some(recorder),
            );
            while let Some(chunk) = converted.next().await {
                chunk.unwrap();
            }

            assert!(store.get("resp_1").is_none(), "status={status}");
        }
    }

    #[tokio::test]
    async fn chat_stream_recorder_consumes_streaming_response_ir() {
        let store = crate::response_state::ResponseStateStore::new(4);
        let request_ir = RequestParserKind::OpenAiResponses
            .parse(&serde_json::json!({"model":"m", "input":"hi"}))
            .unwrap();
        let recorder = ResponseRecorder::new(store.clone(), "provider", "m", request_ir);
        let body = concat!(
            "data: {\"id\":\"chatcmpl_1\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"thought\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"chatcmpl_1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"answer\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"chatcmpl_1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n",
        );
        let inner = stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(body.as_bytes()))]);
        let mut converted = crate::streams::CanonicalParserRendererStream::with_recorder(
            inner,
            codex_provider_proxy_api_conversion::protocols::openai_chat_completions::stream::Parser::new(),
            codex_provider_proxy_api_conversion::protocols::openai_responses::stream::Renderer::new(),
            Some(recorder),
        );
        while let Some(chunk) = converted.next().await {
            chunk.unwrap();
        }

        let state = store
            .get("resp_chatcmpl_1")
            .expect("stream response state recorded");
        assert_eq!(state.last_reasoning.as_deref(), Some("thought"));
        let request = state.request_ir;
        assert!(request.transcript.iter().any(|item| {
            matches!(
                item,
                codex_provider_proxy_api_conversion::ir::ContentItem::Reasoning { content, .. }
                    if content.iter().any(|part| matches!(
                        part,
                        codex_provider_proxy_api_conversion::ir::ContentPart::Reasoning { text, .. }
                            if text == "thought"
                    ))
            )
        }));
        assert!(request.transcript.iter().any(|item| {
            matches!(
                item,
                codex_provider_proxy_api_conversion::ir::ContentItem::Message { content, .. }
                    if content.iter().any(|part| matches!(
                        part,
                        codex_provider_proxy_api_conversion::ir::ContentPart::Text(text)
                            if text == "answer"
                    ))
            )
        }));
    }

    #[tokio::test]
    async fn chat_stream_delivers_valid_events_before_later_error() {
        let body = concat!(
            "data: {\"id\":\"chatcmpl_1\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"},\"finish_reason\":null}]}\n\n",
            "data: not-json\n\n",
        );
        let inner = stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(body.as_bytes()))]);
        let mut converted = crate::streams::CanonicalParserRendererStream::new(
            inner,
            codex_provider_proxy_api_conversion::protocols::openai_chat_completions::stream::Parser::new(),
            codex_provider_proxy_api_conversion::protocols::openai_responses::stream::Renderer::new(),
        );
        let mut output_count = 0;
        let error = loop {
            match converted.next().await {
                Some(Ok(_)) => output_count += 1,
                Some(Err(error)) => break error,
                None => panic!("expected malformed event error"),
            }
        };

        assert!(output_count > 0);
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(converted.next().await.is_none());
    }

    #[tokio::test]
    async fn chat_stream_rejects_oversized_unterminated_event() {
        let body = Bytes::from(vec![
            b'x';
            codex_provider_proxy_api_conversion::sse::MAX_EVENT_BYTES
                + 1
        ]);
        let inner = stream::iter([Ok::<_, std::io::Error>(body)]);
        let mut converted = crate::streams::CanonicalParserRendererStream::new(
            inner,
            codex_provider_proxy_api_conversion::protocols::openai_chat_completions::stream::Parser::new(),
            codex_provider_proxy_api_conversion::protocols::openai_responses::stream::Renderer::new(),
        );

        let error = converted.next().await.unwrap().unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("conversion limit"));
    }
}
