use std::{
    collections::HashSet,
    error::Error as StdError,
    fmt,
    future::Future,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{header, HeaderMap, HeaderName, HeaderValue, Request, Response, StatusCode},
    Router,
};
use bytes::{Bytes, BytesMut};
use futures_util::{Stream, TryStreamExt};
use pin_project_lite::pin_project;
use serde_json::Value;
use tokio::sync::Notify;
use tokio::time::{Instant as TokioInstant, Sleep};
use tracing::{debug, info, warn};
use url::Url;

use crate::{
    config::{Config, Provider},
    content_encoding,
    exchange_log::{
        maybe_create_exchange_logger, ExchangeFileLogger, ExchangeLogContext,
        SharedExchangeFileLogger,
    },
    log_capture::{Capture, CaptureSummary, SharedCapture},
    rewrite::{
        apply_request_rewrites, request_rewrites_may_apply, AnthropicBetaMarker,
        AnthropicBetaPrefix, AnthropicBetaUpdate, RequestRewriteContext,
    },
    runtime::RuntimeState,
    statistics::{StatisticsRequestContext, StatisticsTracker},
};

const MAX_ANCESTOR_PID_DEPTH: usize = 64;
const ANTHROPIC_BETA_HEADER: HeaderName = HeaderName::from_static("anthropic-beta");
const KEEP_ALIVE_HEADER: HeaderName = HeaderName::from_static("keep-alive");
const PROXY_CONNECTION_HEADER: HeaderName = HeaderName::from_static("proxy-connection");
const ANTHROPIC_EFFORT_BETA: &str = "effort-2025-11-24";
const ANTHROPIC_EFFORT_BETA_PREFIX: &str = "effort";
const ANTHROPIC_CONTEXT_1M_BETA: &str = "context-1m-2025-08-07";
const ANTHROPIC_CONTEXT_1M_BETA_PREFIX: &str = "context-1m";

#[derive(Clone)]
pub struct ProxyState {
    pub listen_addr: SocketAddr,
    pub runtime: RuntimeState,
}

#[derive(Clone)]
struct ResolvedProviderRoute {
    route_pid: Option<u32>,
    provider_name: String,
    provider: Provider,
}

#[derive(Clone)]
struct PreparedUpstreamAttempt {
    route_pid: Option<u32>,
    provider_name: String,
    url: Url,
    headers: HeaderMap,
    /// Transcript recorder for Responses downstream requests, set when the request
    /// was converted from `/v1/responses`; records the response when its stream ends.
    response_recorder: Option<crate::api_conversion::ResponseRecorder>,
}

#[derive(Clone)]
struct RetryRequestTemplate {
    method: http::Method,
    forwarded_path: String,
    incoming_query: Option<String>,
    base_headers: HeaderMap,
    request_body: Bytes,
}

#[derive(Clone)]
struct RetrySendArgs {
    state: ProxyState,
    request_id: u64,
    peer: SocketAddr,
    pid: Option<u32>,
    transparent_retry_count: u32,
    transparent_retry_backoff_step: Duration,
    idle_timeout: Option<Duration>,
    exchange_logger: Option<SharedExchangeFileLogger>,
    statistics: Option<StatisticsTracker>,
    request: RetryRequestTemplate,
    initial_attempt: PreparedUpstreamAttempt,
    initial_config: Arc<Config>,
    initial_http_client: reqwest::Client,
}

struct RetryAttemptContext {
    config: Arc<Config>,
    http_client: reqwest::Client,
    attempt: PreparedUpstreamAttempt,
    rerouted: bool,
}

struct ResolveUpstreamAttemptArgs<'a> {
    state: &'a ProxyState,
    cfg: &'a Config,
    default_provider: &'a str,
    pid: Option<u32>,
    peer: SocketAddr,
    request_id: u64,
    forwarded_path: &'a str,
    incoming_query: Option<&'a str>,
    base_headers: &'a HeaderMap,
}

struct UpstreamSendRequest {
    request_id: u64,
    peer: SocketAddr,
    pid: Option<u32>,
    initial_attempt: PreparedUpstreamAttempt,
    method: http::Method,
    forwarded_path: String,
    incoming_query: Option<String>,
    base_headers: HeaderMap,
    body: Body,
    http_client: reqwest::Client,
    exchange_logger: Option<SharedExchangeFileLogger>,
    statistics: Option<StatisticsTracker>,
}

enum SingleAttemptRequestBody {
    Stream(Body),
    Buffered(Bytes),
}

struct AttemptResponseLog<'a> {
    attempt_number: u32,
    attempt: &'a PreparedUpstreamAttempt,
    status: StatusCode,
    headers: HeaderMap,
    latency_ms: u128,
    is_final: bool,
}

pub fn router(state: ProxyState) -> Router {
    Router::new().fallback(handle_proxy).with_state(state)
}

pub async fn handle_proxy(
    State(state): State<ProxyState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    req: Request<Body>,
) -> Response<Body> {
    match handle_proxy_inner(state, peer, req).await {
        Ok(resp) => resp,
        Err(err) => proxy_error_response(err),
    }
}

fn proxy_error_response(err: anyhow::Error) -> Response<Body> {
    let err_chain = format!("{err:#}");
    warn!(error = %err_chain, "proxy error");
    if let Some(rejected) = err.chain().find_map(|cause| {
        cause
            .downcast_ref::<crate::api_conversion::RequestConversionRejected>()
            .or_else(|| {
                cause
                    .downcast_ref::<std::io::Error>()
                    .and_then(std::io::Error::get_ref)
                    .and_then(|source| {
                        source.downcast_ref::<crate::api_conversion::RequestConversionRejected>()
                    })
            })
    }) {
        let mut response = Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::from(rejected.error_body.to_string()))
            .expect("static response construction cannot fail");
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        return response;
    }
    if let Some(body_too_large) = err.chain().find_map(|cause| {
        cause.downcast_ref::<RequestBodyTooLarge>().or_else(|| {
            cause
                .downcast_ref::<std::io::Error>()
                .and_then(std::io::Error::get_ref)
                .and_then(|source| source.downcast_ref::<RequestBodyTooLarge>())
        })
    }) {
        return text_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            Body::from(format!("{body_too_large}\n")),
        );
    }
    text_response(
        StatusCode::BAD_GATEWAY,
        Body::from(format!("proxy error: {err_chain}\n")),
    )
}

#[derive(Debug)]
struct RequestBodyTooLarge {
    limit: usize,
}

impl fmt::Display for RequestBodyTooLarge {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "request body exceeds the configured {}-byte buffering limit",
            self.limit
        )
    }
}

impl StdError for RequestBodyTooLarge {}

async fn handle_proxy_inner(
    state: ProxyState,
    peer: SocketAddr,
    req: Request<Body>,
) -> Result<Response<Body>> {
    let request_id = state.runtime.next_request_id();
    let started = Instant::now();
    let routing = state.runtime.routing_snapshot().await;
    let cfg = routing.config.clone();
    let (parts, body) = req.into_parts();

    let forwarded_path = match strip_listen_base_path(&cfg.listen_base_path, parts.uri.path()) {
        Some(p) => p,
        None => return not_found_response(),
    };
    if cfg.reject_messages_count_tokens && path_is_messages_count_tokens(forwarded_path) {
        return not_found_response();
    }

    let pid = resolve_request_pid(&state, peer, request_id).await;
    let statistics = state
        .runtime
        .statistics()
        .begin_request(StatisticsRequestContext {
            peer,
            pid,
            method: parts.method.as_str(),
            path: parts.uri.path(),
        });
    let base_headers = filtered_incoming_headers(&parts.headers);
    let initial_attempt = resolve_upstream_attempt(ResolveUpstreamAttemptArgs {
        state: &state,
        cfg: &cfg,
        default_provider: &routing.default_provider,
        pid,
        peer,
        request_id,
        forwarded_path,
        incoming_query: parts.uri.query(),
        base_headers: &base_headers,
    })
    .await?;

    if cfg.logging.log_requests {
        info!(
            request_id,
            pid = ?pid,
            route_pid = ?initial_attempt.route_pid,
            peer = %peer,
            provider = %initial_attempt.provider_name,
            method = %parts.method,
            uri = %parts.uri,
            "request"
        );
        debug!(
            request_id,
            pid = ?pid,
            route_pid = ?initial_attempt.route_pid,
            provider = %initial_attempt.provider_name,
            headers = ?parts.headers,
            "request headers"
        );
    }

    let exchange_logger = maybe_create_exchange_logger(
        &cfg.logging,
        ExchangeLogContext {
            request_id,
            peer,
            pid,
            route_pid: initial_attempt.route_pid,
            provider_name: &initial_attempt.provider_name,
            method: &parts.method,
            uri: &parts.uri,
            upstream_url: &initial_attempt.url,
            request_headers: &parts.headers,
        },
    );

    let send_result = send_upstream_request(
        &state,
        &cfg,
        UpstreamSendRequest {
            request_id,
            peer,
            pid,
            initial_attempt: initial_attempt.clone(),
            method: parts.method.clone(),
            forwarded_path: forwarded_path.to_string(),
            incoming_query: parts.uri.query().map(str::to_owned),
            base_headers,
            body,
            http_client: routing.http_client.clone(),
            exchange_logger: exchange_logger.clone(),
            statistics: statistics.clone(),
        },
    )
    .await;

    let (resp, final_attempt, req_body_capture, final_attempt_number, final_attempt_latency_ms) =
        match send_result {
            Ok(resp) => resp,
            Err(err) => {
                let error_latency_ms = started.elapsed().as_millis();
                let err_text = format_error_chain(&err);
                let exchange_error_text = err_text.clone();
                with_exchange_logger_blocking(
                    exchange_logger.clone(),
                    request_id,
                    "mark upstream send error",
                    move |logger| {
                        logger.mark_upstream_send_error(error_latency_ms, &exchange_error_text);
                        logger.finalize();
                    },
                )
                .await;
                if let Some(statistics) = &statistics {
                    statistics.record_attempt_send_error(&err_text);
                    statistics.finalize();
                }
                return Err(err).context("send upstream request");
            }
        };
    let status = resp.status();
    let downstream_status = downstream_response_status(&cfg, status);
    let conversion_applies = crate::api_conversion::response_conversion_enabled(
        &cfg,
        &parts.method,
        &final_attempt.provider_name,
        forwarded_path,
    );
    let resp_headers = resp.headers().clone();

    if cfg.logging.log_responses {
        if status.is_success() {
            info!(
                request_id,
                pid = ?pid,
                route_pid = ?final_attempt.route_pid,
                peer = %peer,
                provider = %final_attempt.provider_name,
                status = %status,
                downstream_status = %downstream_status,
                latency_ms = started.elapsed().as_millis(),
                "response headers received"
            );
        } else {
            warn!(
                request_id,
                pid = ?pid,
                route_pid = ?final_attempt.route_pid,
                peer = %peer,
                provider = %final_attempt.provider_name,
                status = %status,
                downstream_status = %downstream_status,
                latency_ms = started.elapsed().as_millis(),
                "response headers received with non-2xx upstream status"
            );
        }
        debug!(
            request_id,
            pid = ?pid,
            route_pid = ?final_attempt.route_pid,
            provider = %final_attempt.provider_name,
            headers = ?resp_headers,
            "response headers"
        );
    }

    let resp_headers_for_log = resp_headers.clone();
    let upstream_latency_ms = started.elapsed().as_millis();
    if let Some(statistics) = &statistics {
        statistics.record_response(
            status,
            downstream_status,
            &resp_headers,
            upstream_latency_ms,
        );
    }
    let final_provider_name = final_attempt.provider_name.clone();
    let final_url = final_attempt.url.clone();
    let final_route_pid = final_attempt.route_pid;
    with_exchange_logger_blocking(
        exchange_logger.clone(),
        request_id,
        "record final upstream attempt",
        move |logger| {
            logger.record_attempt(
                final_attempt_number,
                crate::exchange_log::AttemptRouteContext {
                    route_pid: final_route_pid,
                    provider_name: &final_provider_name,
                    upstream_url: &final_url,
                },
                status,
                &resp_headers_for_log,
                final_attempt_latency_ms,
                true,
            );
            logger.write_response_headers(
                crate::exchange_log::AttemptRouteContext {
                    route_pid: final_route_pid,
                    provider_name: &final_provider_name,
                    upstream_url: &final_url,
                },
                downstream_status,
                &resp_headers_for_log,
                upstream_latency_ms,
            );
        },
    )
    .await;

    if cfg.logging.log_bodies {
        if let Some(cap) = req_body_capture {
            if let Some(summary) = capture_summary(&cap, request_id, "request") {
                debug!(
                    request_id,
                    pid = ?pid,
                    route_pid = ?final_attempt.route_pid,
                    provider = %final_attempt.provider_name,
                    truncated = summary.truncated,
                    body = %summary.as_lossy_utf8(),
                    "request body"
                );
            }
        }
    }

    if !status.is_success() && conversion_applies {
        return convert_upstream_error_response(
            &cfg,
            status,
            downstream_status,
            resp,
            &resp_headers,
            forwarded_path,
        )
        .await;
    }

    let mut out_headers = filtered_response_headers(&resp_headers);
    if conversion_applies {
        crate::api_conversion::strip_converted_response_headers(&mut out_headers);
        if !is_text_event_stream(&out_headers) {
            out_headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
        }
    }

    // Log the converted downstream response headers (log_conversion_pairs only).
    if conversion_applies && cfg.logging.log_conversion_pairs {
        let logger = exchange_logger.clone();
        let attempt_number = final_attempt_number;
        let out_headers_for_log = out_headers.clone();
        let downstream_status_for_log = downstream_status;
        with_exchange_logger_blocking(
            logger,
            request_id,
            "write converted response headers log",
            move |exchange_logger| {
                exchange_logger.write_converted_response_headers(
                    attempt_number,
                    downstream_status_for_log,
                    &out_headers_for_log,
                );
            },
        )
        .await;
    }

    let (resp_stream, resp_capture) = response_stream_and_capture(
        &cfg,
        request_id,
        resp,
        exchange_logger.clone(),
        statistics.clone(),
    );
    let resp_stream = maybe_filter_responses_slow_down_stream(
        &cfg,
        request_id,
        parts.uri.path(),
        &resp_headers,
        final_attempt.route_pid,
        &final_attempt.provider_name,
        resp_stream,
    );
    let resp_stream = if conversion_applies {
        if is_text_event_stream(&resp_headers) {
            match crate::api_conversion::downstream_api_for_path(forwarded_path) {
                Some(crate::api_conversion::DownstreamApi::AnthropicMessages) => Box::pin(
                    crate::api_conversion::ChatToMessagesStream::new(resp_stream),
                )
                    as BoxRespStream,
                Some(crate::api_conversion::DownstreamApi::OpenAiResponses) => {
                    Box::pin(crate::api_conversion::ChatToResponsesStream::with_recorder(
                        resp_stream,
                        final_attempt.response_recorder.clone(),
                    )) as BoxRespStream
                }
                None => resp_stream,
            }
        } else {
            Box::pin(
                crate::api_conversion::NonStreamingConversionStream::with_recorder(
                    resp_stream,
                    cfg.request_body_buffer_max_bytes,
                    forwarded_path.to_string(),
                    final_attempt.response_recorder.clone(),
                ),
            ) as BoxRespStream
        }
    } else {
        resp_stream
    };
    let statistics_for_stream = statistics.clone();
    let resp_stream = Box::pin(resp_stream.inspect_err(move |err| {
        if let Some(statistics) = &statistics_for_stream {
            statistics.record_response_stream_error(&err.to_string());
        }
    })) as BoxRespStream;
    // Observe the final downstream stream (converted or passthrough) for the
    // unscoped converted.response_body exchange file (log_conversion_pairs only).
    let resp_stream = if exchange_logger.is_some() && cfg.logging.log_conversion_pairs {
        let observers = BodyObservers::new(
            &cfg,
            exchange_logger.clone(),
            None,
            request_id,
            BodyObservationDirection::ResponseConverted,
            "append converted response body chunk",
        );
        observe_body_stream(resp_stream, observers)
    } else {
        resp_stream
    };
    let final_provider_name = final_attempt.provider_name.clone();
    let final_route_pid = final_attempt.route_pid;
    let resp_headers_for_body_log = resp_headers.clone();
    let body = Body::from_stream(LogOnEndStream::new(resp_stream, move || {
        if let Some(capture) = resp_capture {
            if let Some(summary) = capture_summary(&capture, request_id, "response") {
                match response_body_for_log(&summary, &resp_headers_for_body_log) {
                    Ok(body) => {
                        debug!(
                            request_id,
                            pid = ?pid,
                            route_pid = ?final_route_pid,
                            provider = %final_provider_name,
                            truncated = summary.truncated,
                            body = %body,
                            "response body"
                        );
                    }
                    Err(err) => {
                        debug!(
                            request_id,
                            pid = ?pid,
                            route_pid = ?final_route_pid,
                            provider = %final_provider_name,
                            truncated = summary.truncated,
                            captured_bytes = summary.bytes.len(),
                            content_encoding = ?resp_headers_for_body_log.get(header::CONTENT_ENCODING),
                            error = %err,
                            "response body capture could not be decoded for logging"
                        );
                    }
                }
            }
        }
        if let Some(exchange_logger) = exchange_logger {
            finish_and_finalize_exchange_logger_nonblocking(
                exchange_logger,
                request_id,
                final_attempt_number,
            );
        }
        if let Some(statistics) = statistics {
            statistics.finalize();
        }
    }));

    let mut response = Response::builder().status(downstream_status).body(body)?;
    *response.headers_mut() = out_headers;
    Ok(response)
}

fn text_response(status: StatusCode, body: Body) -> Response<Body> {
    let mut response = Response::new(body);
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

/// Convert a non-2xx upstream response of a converted request into the Anthropic error
/// envelope when the body is JSON; otherwise pass the body through with filtered headers.
async fn convert_upstream_error_response(
    cfg: &Config,
    upstream_status: StatusCode,
    downstream_status: StatusCode,
    resp: reqwest::Response,
    upstream_headers: &HeaderMap,
    forwarded_path: &str,
) -> Result<Response<Body>> {
    let body = buffer_response_body_for_conversion(resp, cfg.request_body_buffer_max_bytes).await?;
    let Ok(json) = serde_json::from_slice::<Value>(&body) else {
        let mut response = Response::builder()
            .status(downstream_status)
            .body(Body::from(body))?;
        *response.headers_mut() = filtered_response_headers(upstream_headers);
        return Ok(response);
    };
    let converted =
        crate::api_conversion::convert_error_body(upstream_status, &json, forwarded_path);
    let mut response = Response::builder()
        .status(downstream_status)
        .body(Body::from(converted.to_string()))?;
    let mut headers = filtered_response_headers(upstream_headers);
    crate::api_conversion::strip_converted_response_headers(&mut headers);
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    *response.headers_mut() = headers;
    Ok(response)
}

async fn buffer_response_body_for_conversion(
    resp: reqwest::Response,
    max_bytes: usize,
) -> std::result::Result<Bytes, std::io::Error> {
    let mut stream = TryStreamExt::map_err(resp.bytes_stream(), map_body_err);
    let mut buffered = BytesMut::new();
    while let Some(chunk) = stream.try_next().await? {
        ensure_request_body_fits(buffered.len(), chunk.len(), max_bytes)?;
        buffered.extend_from_slice(&chunk);
    }
    Ok(buffered.freeze())
}

fn not_found_response() -> Result<Response<Body>> {
    Ok(text_response(
        StatusCode::NOT_FOUND,
        Body::from("not found\n"),
    ))
}

async fn resolve_request_pid(state: &ProxyState, peer: SocketAddr, request_id: u64) -> Option<u32> {
    let pid_routes = state.runtime.pid_routes();
    let is_loopback_peer = peer.ip().is_loopback();
    let pid = if is_loopback_peer {
        match state
            .runtime
            .pid_resolver()
            .pid_for_peer(state.listen_addr, peer)
            .await
        {
            Ok(pid) => pid,
            Err(err) => {
                warn!(request_id, peer = %peer, error = %err, "pid resolution failed");
                None
            }
        }
    } else {
        if !pid_routes.is_empty() {
            debug!(
                request_id,
                peer = %peer,
                "non-loopback peer cannot be PID-routed; falling back to default provider"
            );
        }
        None
    };

    // `pid_for_peer` is best-effort and may return Ok(None) (e.g. short-lived connections,
    // /proc visibility/permission issues, or inability to map the socket to a process).
    // If PID routing is in use, surface this so it's not silently surprising.
    if is_loopback_peer && pid.is_none() && !pid_routes.is_empty() {
        warn!(
            request_id,
            peer = %peer,
            listen = %state.listen_addr,
            "pid could not be resolved for connection; falling back to default provider"
        );
    }

    pid
}

async fn resolve_provider_for_pid(
    state: &ProxyState,
    cfg: &Config,
    default_provider: &str,
    pid: Option<u32>,
    peer: SocketAddr,
    request_id: u64,
) -> Result<ResolvedProviderRoute> {
    let (provider_name, route_pid) = if let Some(pid) = pid {
        match find_provider_for_pid_or_ancestors(state, pid).await {
            Ok(Some((route_pid, provider_name))) => (provider_name, Some(route_pid)),
            Ok(None) => (default_provider.to_owned(), None),
            Err(err) => {
                warn!(
                    request_id,
                    pid,
                    peer = %peer,
                    error = %err,
                    "ancestor pid route lookup failed; falling back to default provider"
                );
                (default_provider.to_owned(), None)
            }
        }
    } else {
        (default_provider.to_owned(), None)
    };

    let provider = cfg
        .providers
        .get(&provider_name)
        .with_context(|| format!("provider {provider_name:?} missing from config"))?
        .clone();
    Ok(ResolvedProviderRoute {
        route_pid,
        provider_name,
        provider,
    })
}

fn prepare_upstream_attempt(
    route: ResolvedProviderRoute,
    forwarded_path: &str,
    incoming_query: Option<&str>,
    base_headers: &HeaderMap,
) -> Result<PreparedUpstreamAttempt> {
    let upstream_path =
        crate::api_conversion::conversion_upstream_path(&route.provider, forwarded_path);
    let url = build_outgoing_url(&route.provider, upstream_path, incoming_query)?;
    let mut headers = base_headers.clone();
    headers.insert(
        header::AUTHORIZATION,
        http::HeaderValue::from_str(&route.provider.authorization_value())
            .context("build Authorization header")?,
    );
    Ok(PreparedUpstreamAttempt {
        route_pid: route.route_pid,
        provider_name: route.provider_name,
        url,
        headers,
        response_recorder: None,
    })
}

async fn resolve_upstream_attempt(
    args: ResolveUpstreamAttemptArgs<'_>,
) -> Result<PreparedUpstreamAttempt> {
    let route = resolve_provider_for_pid(
        args.state,
        args.cfg,
        args.default_provider,
        args.pid,
        args.peer,
        args.request_id,
    )
    .await?;
    prepare_upstream_attempt(
        route,
        args.forwarded_path,
        args.incoming_query,
        args.base_headers,
    )
}

async fn send_upstream_request(
    state: &ProxyState,
    cfg: &Arc<Config>,
    request: UpstreamSendRequest,
) -> std::result::Result<
    (
        reqwest::Response,
        PreparedUpstreamAttempt,
        Option<SharedCapture>,
        u32,
        u128,
    ),
    std::io::Error,
> {
    let UpstreamSendRequest {
        request_id,
        peer,
        pid,
        initial_attempt,
        method,
        forwarded_path,
        incoming_query,
        base_headers,
        body,
        http_client,
        exchange_logger,
        statistics,
    } = request;
    if !transparent_retries_enabled(cfg, &method) {
        let mut initial_attempt = initial_attempt;
        let conversion_may_apply = crate::api_conversion::request_conversion_enabled(
            cfg,
            &method,
            &initial_attempt.provider_name,
            &forwarded_path,
        );
        let needs_buffering = request_rewrites_may_apply(
            cfg,
            &RequestRewriteContext {
                method: &method,
                forwarded_path: &forwarded_path,
                provider_name: &initial_attempt.provider_name,
                headers: &initial_attempt.headers,
            },
        ) || conversion_may_apply;
        // With log_conversion_pairs the attempt request_body file holds the
        // pre-conversion body; keep it here for begin_exchange_log_attempt.
        let mut pre_conversion_request_body: Option<Bytes> = None;
        let request_body = if needs_buffering {
            // Buffer (and log) the original downstream body before conversion, so the
            // unscoped request_body exchange file holds the pre-conversion request.
            let observers = BodyObservers::new(
                cfg,
                exchange_logger.clone(),
                statistics.clone(),
                request_id,
                BodyObservationDirection::Request,
                "buffer original request body",
            );
            let original =
                buffer_request_body(body, cfg.request_body_buffer_max_bytes, Some(&observers))
                    .await?;
            if cfg.logging.log_conversion_pairs {
                pre_conversion_request_body = Some(original.clone());
            }
            SingleAttemptRequestBody::Buffered(prepare_request_body_for_attempt(
                state,
                cfg,
                &method,
                &forwarded_path,
                &mut initial_attempt,
                original,
                request_id,
                1,
            )?)
        } else {
            SingleAttemptRequestBody::Stream(body)
        };
        let attempt_started = Instant::now();
        let upload_activity = match &request_body {
            SingleAttemptRequestBody::Stream(_) => {
                cfg.upstream_idle_timeout.map(|_| Arc::new(Notify::new()))
            }
            SingleAttemptRequestBody::Buffered(_) => None,
        };
        begin_exchange_log_attempt(
            exchange_logger.clone(),
            statistics.clone(),
            request_id,
            1,
            &initial_attempt,
            &method,
            pre_conversion_request_body,
            cfg.logging.log_conversion_pairs && conversion_may_apply,
        )
        .await;
        let (req_body, req_body_capture) = match request_body {
            SingleAttemptRequestBody::Stream(body) => maybe_wrap_request_body_for_logging(
                cfg,
                body,
                exchange_logger.clone(),
                statistics.clone(),
                upload_activity.clone(),
                request_id,
            ),
            SingleAttemptRequestBody::Buffered(request_body) => {
                body_from_bytes_for_logging(
                    cfg,
                    request_body,
                    exchange_logger.clone(),
                    statistics.clone(),
                    request_id,
                )
                .await
            }
        };
        let out = http_client
            .request(method, initial_attempt.url.clone())
            .headers(initial_attempt.headers.clone())
            .body(req_body);

        let resp = send_with_optional_idle_timeout(
            request_id,
            cfg.upstream_idle_timeout,
            upload_activity,
            out.send(),
        )
        .await;
        let resp = match resp {
            Ok(resp) => resp,
            Err(err) => {
                record_exchange_log_attempt_send_error(
                    exchange_logger.clone(),
                    statistics.clone(),
                    request_id,
                    1,
                    attempt_started.elapsed().as_millis(),
                    &err,
                    true,
                )
                .await;
                return Err(err);
            }
        };
        Ok((
            resp,
            initial_attempt,
            req_body_capture,
            1,
            attempt_started.elapsed().as_millis(),
        ))
    } else {
        let (request_body, req_body_capture) = buffer_request_body_for_retry(
            cfg,
            body,
            exchange_logger.clone(),
            statistics.clone(),
            request_id,
        )
        .await?;
        let (resp, final_attempt, attempt_count, final_attempt_latency_ms) =
            send_with_non_2xx_retries(RetrySendArgs {
                state: state.clone(),
                request_id,
                peer,
                pid,
                transparent_retry_count: cfg.transparent_retry_count,
                transparent_retry_backoff_step: cfg.transparent_retry_backoff_step,
                idle_timeout: cfg.upstream_idle_timeout,
                exchange_logger,
                statistics,
                request: RetryRequestTemplate {
                    method,
                    forwarded_path,
                    incoming_query,
                    base_headers,
                    request_body,
                },
                initial_attempt,
                initial_config: cfg.clone(),
                initial_http_client: http_client,
            })
            .await?;
        Ok((
            resp,
            final_attempt,
            req_body_capture,
            attempt_count,
            final_attempt_latency_ms,
        ))
    }
}

fn transparent_retries_enabled(cfg: &Config, method: &http::Method) -> bool {
    cfg.transparent_retry_count > 0
        && (*method != http::Method::HEAD || cfg.transparent_retry_head_requests)
}

async fn with_exchange_logger_blocking<F>(
    exchange_logger: Option<SharedExchangeFileLogger>,
    request_id: u64,
    action: &'static str,
    f: F,
) where
    F: FnOnce(&mut ExchangeFileLogger) + Send + 'static,
{
    let Some(exchange_logger) = exchange_logger else {
        return;
    };
    let join = tokio::task::spawn_blocking(move || {
        if let Ok(mut logger) = exchange_logger.lock() {
            f(&mut logger);
        }
    })
    .await;

    if let Err(err) = join {
        warn!(request_id, action, error = %err, "exchange logger task join failed");
    }
}

#[allow(clippy::too_many_arguments)]
async fn begin_exchange_log_attempt(
    exchange_logger: Option<SharedExchangeFileLogger>,
    statistics: Option<StatisticsTracker>,
    request_id: u64,
    attempt_number: u32,
    attempt: &PreparedUpstreamAttempt,
    method: &http::Method,
    request_body: Option<Bytes>,
    log_conversion_pairs: bool,
) {
    if let Some(statistics) = &statistics {
        statistics.begin_attempt(&attempt.provider_name, attempt.route_pid);
    }
    let provider_name = attempt.provider_name.clone();
    let upstream_url = attempt.url.clone();
    let route_pid = attempt.route_pid;
    let headers = attempt.headers.clone();
    let method = method.clone();
    with_exchange_logger_blocking(
        exchange_logger,
        request_id,
        "begin exchange log attempt",
        move |logger| {
            logger.begin_attempt(
                attempt_number,
                crate::exchange_log::AttemptRouteContext {
                    route_pid,
                    provider_name: &provider_name,
                    upstream_url: &upstream_url,
                },
                &method,
                &headers,
                request_body.as_ref(),
            );
            if log_conversion_pairs {
                logger.write_converted_request_headers(
                    attempt_number,
                    crate::exchange_log::AttemptRouteContext {
                        route_pid,
                        provider_name: &provider_name,
                        upstream_url: &upstream_url,
                    },
                    &method,
                    &headers,
                );
            }
        },
    )
    .await;
}

async fn record_exchange_log_attempt_response(
    exchange_logger: Option<SharedExchangeFileLogger>,
    request_id: u64,
    entry: AttemptResponseLog<'_>,
) {
    let AttemptResponseLog {
        attempt_number,
        attempt,
        status,
        headers,
        latency_ms,
        is_final,
    } = entry;
    let provider_name = attempt.provider_name.clone();
    let upstream_url = attempt.url.clone();
    let route_pid = attempt.route_pid;
    with_exchange_logger_blocking(
        exchange_logger,
        request_id,
        "record exchange log attempt response",
        move |logger| {
            logger.record_attempt(
                attempt_number,
                crate::exchange_log::AttemptRouteContext {
                    route_pid,
                    provider_name: &provider_name,
                    upstream_url: &upstream_url,
                },
                status,
                &headers,
                latency_ms,
                is_final,
            );
        },
    )
    .await;
}

async fn record_exchange_log_attempt_send_error(
    exchange_logger: Option<SharedExchangeFileLogger>,
    statistics: Option<StatisticsTracker>,
    request_id: u64,
    attempt_number: u32,
    latency_ms: u128,
    err: &std::io::Error,
    is_final: bool,
) {
    let err = format_error_chain(err);
    if let Some(statistics) = &statistics {
        statistics.record_attempt_send_error(&err);
    }
    with_exchange_logger_blocking(
        exchange_logger,
        request_id,
        "record exchange log attempt send error",
        move |logger| logger.record_attempt_send_error(attempt_number, latency_ms, &err, is_final),
    )
    .await;
}

async fn finish_exchange_log_attempt(
    exchange_logger: Option<SharedExchangeFileLogger>,
    request_id: u64,
    attempt_number: u32,
) {
    with_exchange_logger_blocking(
        exchange_logger,
        request_id,
        "finish exchange log attempt",
        move |logger| logger.finish_attempt(attempt_number),
    )
    .await;
}

async fn append_exchange_log_attempt_response_chunk(
    exchange_logger: Option<SharedExchangeFileLogger>,
    request_id: u64,
    attempt_number: u32,
    chunk: Bytes,
) {
    with_exchange_logger_blocking(
        exchange_logger,
        request_id,
        "append attempt response body chunk",
        move |logger| logger.on_attempt_response_body_chunk(attempt_number, &chunk),
    )
    .await;
}

fn finish_and_finalize_exchange_logger_nonblocking(
    exchange_logger: SharedExchangeFileLogger,
    request_id: u64,
    final_attempt_number: u32,
) {
    if let Ok(runtime_handle) = tokio::runtime::Handle::try_current() {
        runtime_handle.spawn_blocking(move || {
            if let Ok(mut logger) = exchange_logger.lock() {
                logger.finish_attempt(final_attempt_number);
                logger.finalize();
            }
        });
        return;
    }

    warn!(
        request_id,
        "no active Tokio runtime while finalizing exchange logger; finalizing inline"
    );
    if let Ok(mut logger) = exchange_logger.lock() {
        logger.finish_attempt(final_attempt_number);
        logger.finalize();
    }
}

async fn find_provider_for_pid_or_ancestors(
    state: &ProxyState,
    pid: u32,
) -> Result<Option<(u32, String)>> {
    let pid_routes = state.runtime.pid_routes();
    if let Some(provider) = pid_routes.get(&pid) {
        return Ok(Some((pid, provider.value().clone())));
    }

    let mut current = pid;
    for _ in 0..MAX_ANCESTOR_PID_DEPTH {
        let parent = match state.runtime.pid_resolver().parent_pid(current).await? {
            Some(ppid) => ppid,
            None => break,
        };
        if parent == 0 || parent == current {
            break;
        }

        if let Some(provider) = pid_routes.get(&parent) {
            return Ok(Some((parent, provider.value().clone())));
        }

        if parent == 1 {
            break;
        }
        current = parent;
    }

    Ok(None)
}

fn strip_listen_base_path<'a>(base_path: &str, incoming_path: &'a str) -> Option<&'a str> {
    if base_path == "/" {
        return Some(incoming_path);
    }
    if incoming_path == base_path {
        return Some("/");
    }
    let rest = incoming_path.strip_prefix(base_path)?;
    let rest = rest.strip_prefix('/')?;
    if rest.is_empty() {
        Some("/")
    } else {
        Some(rest)
    }
}

fn path_is_messages_count_tokens(path: &str) -> bool {
    let normalized = path.trim_matches('/');
    normalized == "messages/count_tokens" || normalized.ends_with("/messages/count_tokens")
}

fn build_outgoing_url(
    provider: &Provider,
    forwarded_path: &str,
    incoming_query: Option<&str>,
) -> Result<Url> {
    let mut url = provider.base_url.clone();
    let joined_path = join_paths(url.path(), forwarded_path);
    url.set_path(&joined_path);
    url.set_query(incoming_query);
    Ok(url)
}

fn join_paths(base_path: &str, incoming_path: &str) -> String {
    let base = if base_path.is_empty() { "/" } else { base_path };
    let base = base.trim_end_matches('/');
    let incoming = incoming_path.strip_prefix('/').unwrap_or(incoming_path);
    if base.is_empty() {
        format!("/{}", incoming)
    } else if incoming.is_empty() {
        format!("{}/", base)
    } else {
        format!("{}/{}", base, incoming)
    }
}

fn filtered_incoming_headers(headers: &HeaderMap) -> HeaderMap {
    filter_headers(headers, true)
}

fn filtered_response_headers(headers: &HeaderMap) -> HeaderMap {
    filter_headers(headers, false)
}

fn filter_headers(headers: &HeaderMap, strip_host_and_auth: bool) -> HeaderMap {
    let connection_headers = connection_header_names(headers);
    let mut out = HeaderMap::new();
    for (name, value) in headers.iter() {
        if is_hop_by_hop(name) || connection_headers.contains(name) {
            continue;
        }
        if strip_host_and_auth && (name == header::HOST || name == header::AUTHORIZATION) {
            continue;
        }
        out.append(name, value.clone());
    }
    out
}

fn connection_header_names(headers: &HeaderMap) -> HashSet<HeaderName> {
    headers
        .get_all(header::CONNECTION)
        .iter()
        .flat_map(|value| value.as_bytes().split(|byte| *byte == b','))
        .filter_map(|token| HeaderName::from_bytes(token.trim_ascii()).ok())
        .collect()
}

fn downstream_response_status(
    cfg: &crate::config::Config,
    upstream_status: StatusCode,
) -> StatusCode {
    if cfg.convert_429_to_503 && upstream_status == StatusCode::TOO_MANY_REQUESTS {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        upstream_status
    }
}

fn is_hop_by_hop(name: &HeaderName) -> bool {
    // Minimal hop-by-hop list for a reverse proxy:
    // https://www.rfc-editor.org/rfc/rfc7230#section-6.1
    name == header::CONNECTION
        || name == PROXY_CONNECTION_HEADER
        || name == KEEP_ALIVE_HEADER
        || name == header::TRANSFER_ENCODING
        || name == header::UPGRADE
        || name == header::TE
        || name == header::TRAILER
}

fn capture_summary(
    capture: &SharedCapture,
    request_id: u64,
    body_direction: &'static str,
) -> Option<CaptureSummary> {
    match capture.lock() {
        Ok(capture) => Some(capture.summary()),
        Err(err) => {
            warn!(
                request_id,
                body_direction,
                error = %err,
                "body capture lock poisoned; skipping body log"
            );
            None
        }
    }
}

fn response_body_for_log(
    summary: &CaptureSummary,
    headers: &HeaderMap,
) -> Result<String, std::io::Error> {
    content_encoding::decode_content_encoded_body(headers, &summary.bytes)
        .map(|body| String::from_utf8_lossy(&body).into_owned())
}

fn rewrite_request_body_for_attempt(
    cfg: &crate::config::Config,
    method: &http::Method,
    forwarded_path: &str,
    attempt: &mut PreparedUpstreamAttempt,
    request_body: Bytes,
    request_id: u64,
    attempt_number: u32,
) -> Bytes {
    let out = apply_request_rewrites(
        cfg,
        &RequestRewriteContext {
            method,
            forwarded_path,
            provider_name: &attempt.provider_name,
            headers: &attempt.headers,
        },
        request_body,
    );

    if let Some(mapping) = out.applied_model_mapping {
        if out.body_changed {
            attempt.headers.remove(header::CONTENT_LENGTH);
        }
        apply_anthropic_beta_updates(&mut attempt.headers, &out.anthropic_beta_updates);
        info!(
            request_id,
            attempt = attempt_number,
            route_pid = ?attempt.route_pid,
            provider = %attempt.provider_name,
            upstream_url = %attempt.url,
            from_model = %mapping.from_model,
            from_reasoning_effort = ?mapping.from_reasoning_effort,
            to_model = ?mapping.to_model,
            to_reasoning_effort = ?mapping.to_reasoning_effort,
            body_changed = out.body_changed,
            "request rewrite model mapping applied"
        );
    }

    out.body
}

/// Model-mapping rewrite followed by API format conversion when the provider converts
/// Anthropic Messages traffic.
#[allow(clippy::too_many_arguments)]
fn prepare_request_body_for_attempt(
    state: &ProxyState,
    cfg: &crate::config::Config,
    method: &http::Method,
    forwarded_path: &str,
    attempt: &mut PreparedUpstreamAttempt,
    request_body: Bytes,
    request_id: u64,
    attempt_number: u32,
) -> std::result::Result<Bytes, std::io::Error> {
    let body = rewrite_request_body_for_attempt(
        cfg,
        method,
        forwarded_path,
        attempt,
        request_body,
        request_id,
        attempt_number,
    );
    maybe_convert_request_body(
        state,
        cfg,
        method,
        forwarded_path,
        attempt,
        body,
        request_id,
        attempt_number,
    )
}

#[allow(clippy::too_many_arguments)]
fn maybe_convert_request_body(
    state: &ProxyState,
    cfg: &crate::config::Config,
    method: &http::Method,
    forwarded_path: &str,
    attempt: &mut PreparedUpstreamAttempt,
    body: Bytes,
    request_id: u64,
    attempt_number: u32,
) -> std::result::Result<Bytes, std::io::Error> {
    if !crate::api_conversion::request_conversion_enabled(
        cfg,
        method,
        &attempt.provider_name,
        forwarded_path,
    ) {
        return Ok(body);
    }
    let is_responses = matches!(
        crate::api_conversion::downstream_api_for_path(forwarded_path),
        Some(crate::api_conversion::DownstreamApi::OpenAiResponses)
    );
    let previous_messages = if is_responses {
        resolve_previous_response_messages(state, &attempt.provider_name, &body)
            .map_err(|rejected| std::io::Error::new(std::io::ErrorKind::InvalidInput, rejected))?
    } else {
        None
    };
    let converted = crate::api_conversion::convert_request_body(
        cfg,
        &attempt.provider_name,
        forwarded_path,
        body,
        previous_messages.as_deref(),
    )
    .map_err(|rejected| std::io::Error::new(std::io::ErrorKind::InvalidInput, rejected))?;
    if is_responses {
        // Hold the transcript recorder for the response side of this attempt so the
        // synthesized response can be continued with previous_response_id.
        if let Ok(json) = serde_json::from_slice::<Value>(&converted) {
            let model = json
                .get("model")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let messages = json
                .get("messages")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            attempt.response_recorder = Some(crate::api_conversion::ResponseRecorder::new(
                state.runtime.response_states(),
                attempt.provider_name.clone(),
                model,
                messages,
            ));
        }
    }
    attempt.headers.remove(header::CONTENT_LENGTH);
    attempt.headers.remove(ANTHROPIC_BETA_HEADER);
    attempt.headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    info!(
        request_id,
        attempt = attempt_number,
        route_pid = ?attempt.route_pid,
        provider = %attempt.provider_name,
        upstream_url = %attempt.url,
        "request converted from messages to chat completions"
    );
    Ok(converted)
}

/// Resolve the stored chat transcript for the request's `previous_response_id`,
/// rejecting unknown/expired ids and continuations across providers.
fn resolve_previous_response_messages(
    state: &ProxyState,
    provider_name: &str,
    body: &[u8],
) -> std::result::Result<Option<Vec<Value>>, crate::api_conversion::RequestConversionRejected> {
    let json: Value = serde_json::from_slice(body).map_err(|_| {
        crate::api_conversion::RequestConversionRejected::openai_invalid(
            "responses request body is not valid JSON",
        )
    })?;
    let Some(previous_id) = json
        .get("previous_response_id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
    else {
        return Ok(None);
    };
    let store = state.runtime.response_states();
    let Some(previous) = store.get(previous_id) else {
        return Err(
            crate::api_conversion::RequestConversionRejected::openai_invalid(format!(
                "unknown or expired previous_response_id {previous_id:?}"
            )),
        );
    };
    if previous.provider_name != provider_name {
        return Err(
            crate::api_conversion::RequestConversionRejected::openai_invalid(format!(
                "previous_response_id {previous_id:?} belongs to provider {:?}, not {provider_name:?}",
                previous.provider_name
            )),
        );
    }
    if let Ok(model) = serde_json::from_slice::<Value>(body) {
        let model = model
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !previous.model.is_empty() && previous.model != model {
            warn!(
                previous_response_id = %previous_id,
                previous_model = %previous.model,
                model = %model,
                "continuing a conversation with a different model"
            );
        }
    }
    Ok(Some(previous.chat_messages))
}

fn apply_anthropic_beta_updates(headers: &mut HeaderMap, updates: &[AnthropicBetaUpdate]) {
    for update in updates {
        match *update {
            AnthropicBetaUpdate::Ensure(marker) => {
                ensure_anthropic_beta(headers, marker);
            }
            AnthropicBetaUpdate::RemoveByPrefix(prefix) => {
                remove_anthropic_beta_by_prefix(headers, anthropic_beta_prefix_value(prefix));
            }
        }
    }
}

fn anthropic_beta_marker_value(marker: AnthropicBetaMarker) -> &'static str {
    match marker {
        AnthropicBetaMarker::Effort => ANTHROPIC_EFFORT_BETA,
        AnthropicBetaMarker::Context1m => ANTHROPIC_CONTEXT_1M_BETA,
    }
}

fn anthropic_beta_prefix_value(prefix: AnthropicBetaPrefix) -> &'static str {
    match prefix {
        AnthropicBetaPrefix::Context1m => ANTHROPIC_CONTEXT_1M_BETA_PREFIX,
    }
}

fn ensure_anthropic_beta(headers: &mut HeaderMap, marker: AnthropicBetaMarker) {
    let marker_value = anthropic_beta_marker_value(marker);
    if headers.get_all(&ANTHROPIC_BETA_HEADER).iter().any(|value| {
        value.to_str().ok().is_some_and(|value| {
            value
                .split(',')
                .map(str::trim)
                .any(|beta| anthropic_beta_marker_matches(marker, beta))
        })
    }) {
        return;
    }

    let Some(existing) = headers
        .get(&ANTHROPIC_BETA_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        headers.insert(
            ANTHROPIC_BETA_HEADER,
            HeaderValue::from_static(marker_value),
        );
        return;
    };

    headers.insert(
        ANTHROPIC_BETA_HEADER,
        HeaderValue::from_str(&format!("{existing},{marker_value}"))
            .expect("existing and marker values are valid ASCII"),
    );
}

fn anthropic_beta_marker_matches(marker: AnthropicBetaMarker, beta: &str) -> bool {
    match marker {
        AnthropicBetaMarker::Effort => {
            beta_token_matches_prefix(beta, ANTHROPIC_EFFORT_BETA_PREFIX)
        }
        AnthropicBetaMarker::Context1m => {
            beta_token_matches_prefix(beta, ANTHROPIC_CONTEXT_1M_BETA_PREFIX)
        }
    }
}

fn remove_anthropic_beta_by_prefix(headers: &mut HeaderMap, prefix: &str) {
    let values = headers
        .get_all(&ANTHROPIC_BETA_HEADER)
        .iter()
        .map(|value| value.to_str())
        .collect::<Result<Vec<_>, _>>();
    let Ok(values) = values else {
        return;
    };

    let remaining = values
        .into_iter()
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|beta| !beta.is_empty())
        .filter(|beta| !beta_token_matches_prefix(beta, prefix))
        .map(str::to_string)
        .collect::<Vec<_>>();

    headers.remove(&ANTHROPIC_BETA_HEADER);
    if remaining.is_empty() {
        return;
    }

    let value = remaining.join(",");
    if let Ok(value) = HeaderValue::from_str(&value) {
        headers.insert(ANTHROPIC_BETA_HEADER, value);
    }
}

fn beta_token_matches_prefix(beta: &str, prefix: &str) -> bool {
    beta.eq_ignore_ascii_case(prefix)
        || beta
            .get(..prefix.len())
            .is_some_and(|token_prefix| token_prefix.eq_ignore_ascii_case(prefix))
            && beta.as_bytes().get(prefix.len()) == Some(&b'-')
}

#[derive(Clone, Copy)]
enum BodyObservationDirection {
    Request,
    /// The request body after API format conversion; logged only in the per-attempt
    /// file (the unscoped request_body file holds the original downstream body).
    RequestConverted,
    Response,
    /// The response body after API format conversion; logged only in the unscoped
    /// response_body file (the per-attempt file holds the raw upstream body).
    ResponseConverted,
}

#[derive(Clone)]
struct BodyObservers {
    capture: Option<SharedCapture>,
    exchange_logger: Option<SharedExchangeFileLogger>,
    statistics: Option<StatisticsTracker>,
    upload_activity: Option<Arc<Notify>>,
    request_id: u64,
    direction: BodyObservationDirection,
    logger_action: &'static str,
    /// When enabled, the converted direction writes the `converted.*` exchange
    /// files instead of the plain ones (log_conversion_pairs config).
    log_conversion_pairs: bool,
}

impl BodyObservers {
    fn new(
        cfg: &crate::config::Config,
        exchange_logger: Option<SharedExchangeFileLogger>,
        statistics: Option<StatisticsTracker>,
        request_id: u64,
        direction: BodyObservationDirection,
        logger_action: &'static str,
    ) -> Self {
        let capture = cfg.logging.log_bodies.then(|| {
            Arc::new(std::sync::Mutex::new(Capture::new(
                cfg.logging.max_body_log_bytes,
            )))
        });
        Self {
            capture,
            exchange_logger,
            statistics,
            upload_activity: None,
            request_id,
            direction,
            logger_action,
            log_conversion_pairs: cfg.logging.log_conversion_pairs,
        }
    }

    fn with_upload_activity(mut self, upload_activity: Option<Arc<Notify>>) -> Self {
        self.upload_activity = upload_activity;
        self
    }

    fn is_active(&self) -> bool {
        self.capture.is_some()
            || self.exchange_logger.is_some()
            || self.statistics.is_some()
            || self.upload_activity.is_some()
    }

    fn capture(&self) -> Option<SharedCapture> {
        self.capture.clone()
    }

    async fn observe(&self, chunk: &Bytes) {
        if let Some(capture) = &self.capture {
            if let Ok(mut capture) = capture.lock() {
                capture.push_chunk(chunk);
            }
        }
        if let Some(statistics) = &self.statistics {
            match self.direction {
                BodyObservationDirection::Request | BodyObservationDirection::RequestConverted => {
                    statistics.capture_request_chunk(chunk)
                }
                BodyObservationDirection::Response
                | BodyObservationDirection::ResponseConverted => {
                    statistics.capture_response_chunk(chunk)
                }
            }
        }

        if let Some(exchange_logger) = &self.exchange_logger {
            let chunk = chunk.clone();
            let direction = self.direction;
            let log_conversion_pairs = self.log_conversion_pairs;
            with_exchange_logger_blocking(
                Some(exchange_logger.clone()),
                self.request_id,
                self.logger_action,
                move |logger| match direction {
                    BodyObservationDirection::Request => logger.on_request_body_chunk(&chunk),
                    BodyObservationDirection::RequestConverted => {
                        if log_conversion_pairs {
                            logger.on_converted_request_body_chunk(&chunk);
                        } else {
                            logger.on_request_body_chunk(&chunk);
                        }
                    }
                    BodyObservationDirection::Response => logger.on_response_body_chunk(&chunk),
                    BodyObservationDirection::ResponseConverted => {
                        if log_conversion_pairs {
                            logger.on_converted_response_body_chunk(&chunk);
                        }
                    }
                },
            )
            .await;
        }

        if let Some(upload_activity) = &self.upload_activity {
            upload_activity.notify_one();
        }
    }
}

fn observe_body_stream<S>(stream: S, observers: BodyObservers) -> BoxRespStream
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
{
    if !observers.is_active() {
        return Box::pin(stream);
    }

    Box::pin(stream.and_then(move |chunk| {
        let observers = observers.clone();
        async move {
            observers.observe(&chunk).await;
            Ok(chunk)
        }
    }))
}

async fn body_from_bytes_for_logging(
    cfg: &crate::config::Config,
    request_body: Bytes,
    exchange_logger: Option<SharedExchangeFileLogger>,
    statistics: Option<StatisticsTracker>,
    request_id: u64,
) -> (reqwest::Body, Option<SharedCapture>) {
    let observers = BodyObservers::new(
        cfg,
        exchange_logger,
        statistics,
        request_id,
        BodyObservationDirection::RequestConverted,
        "append buffered request body",
    );
    let capture = observers.capture();
    observers.observe(&request_body).await;

    (reqwest::Body::from(request_body), capture)
}

fn maybe_wrap_request_body_for_logging(
    cfg: &crate::config::Config,
    body: Body,
    exchange_logger: Option<SharedExchangeFileLogger>,
    statistics: Option<StatisticsTracker>,
    upload_activity: Option<Arc<Notify>>,
    request_id: u64,
) -> (reqwest::Body, Option<SharedCapture>) {
    let stream = TryStreamExt::map_err(body.into_data_stream(), map_body_err);
    let observers = BodyObservers::new(
        cfg,
        exchange_logger,
        statistics,
        request_id,
        BodyObservationDirection::Request,
        "append request body chunk",
    )
    .with_upload_activity(upload_activity);
    let capture = observers.capture();
    let stream = observe_body_stream(stream, observers);
    (reqwest::Body::wrap_stream(stream), capture)
}

async fn buffer_request_body(
    body: Body,
    max_bytes: usize,
    observers: Option<&BodyObservers>,
) -> std::result::Result<Bytes, std::io::Error> {
    let mut stream = TryStreamExt::map_err(body.into_data_stream(), map_body_err);
    let mut buffered = BytesMut::new();

    while let Some(chunk) = stream.try_next().await? {
        ensure_request_body_fits(buffered.len(), chunk.len(), max_bytes)?;
        buffered.extend_from_slice(&chunk);
        if let Some(observers) = observers {
            observers.observe(&chunk).await;
        }
    }

    Ok(buffered.freeze())
}

async fn buffer_request_body_for_retry(
    cfg: &crate::config::Config,
    body: Body,
    exchange_logger: Option<SharedExchangeFileLogger>,
    statistics: Option<StatisticsTracker>,
    request_id: u64,
) -> std::result::Result<(Bytes, Option<SharedCapture>), std::io::Error> {
    let observers = BodyObservers::new(
        cfg,
        exchange_logger,
        statistics,
        request_id,
        BodyObservationDirection::Request,
        "append buffered request body chunk",
    );
    let capture = observers.capture();
    let request_body =
        buffer_request_body(body, cfg.request_body_buffer_max_bytes, Some(&observers)).await?;

    Ok((request_body, capture))
}

fn ensure_request_body_fits(
    buffered_bytes: usize,
    next_chunk_bytes: usize,
    max_bytes: usize,
) -> std::io::Result<()> {
    if next_chunk_bytes > max_bytes.saturating_sub(buffered_bytes) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            RequestBodyTooLarge { limit: max_bytes },
        ));
    }
    Ok(())
}

type BoxRespStream =
    std::pin::Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static>>;

fn response_stream_and_capture(
    cfg: &crate::config::Config,
    request_id: u64,
    resp: reqwest::Response,
    exchange_logger: Option<SharedExchangeFileLogger>,
    statistics: Option<StatisticsTracker>,
) -> (BoxRespStream, Option<SharedCapture>) {
    let stream = TryStreamExt::map_err(resp.bytes_stream(), map_body_err);
    let observers = BodyObservers::new(
        cfg,
        exchange_logger,
        statistics,
        request_id,
        BodyObservationDirection::Response,
        "append response body chunk",
    );
    let capture = observers.capture();
    let stream = observe_body_stream(stream, observers);
    match cfg.upstream_idle_timeout {
        Some(idle_timeout) => {
            let stream = IdleTimeoutStream::new(
                stream,
                idle_timeout,
                request_id,
                "upstream response body download",
            );
            (Box::pin(stream), capture)
        }
        None => (stream, capture),
    }
}

fn maybe_filter_responses_slow_down_stream(
    cfg: &crate::config::Config,
    request_id: u64,
    request_path: &str,
    resp_headers: &HeaderMap,
    route_pid: Option<u32>,
    provider_name: &str,
    stream: BoxRespStream,
) -> BoxRespStream {
    if !should_drop_responses_slow_down_errors(cfg, request_path, resp_headers) {
        return stream;
    }

    Box::pin(ResponsesSlowDownDropStream::new(
        stream,
        request_id,
        route_pid,
        provider_name.to_string(),
    ))
}

fn should_drop_responses_slow_down_errors(
    cfg: &crate::config::Config,
    request_path: &str,
    resp_headers: &HeaderMap,
) -> bool {
    cfg.drop_responses_slow_down_errors
        && path_ends_with_responses(request_path)
        && is_text_event_stream(resp_headers)
}

fn is_text_event_stream(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(';')
                .next()
                .unwrap_or_default()
                .trim()
                .eq_ignore_ascii_case("text/event-stream")
        })
}

fn path_ends_with_responses(path: &str) -> bool {
    path_last_segment(path) == Some("responses")
}

fn path_last_segment(path: &str) -> Option<&str> {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    trimmed.rsplit('/').next()
}

fn responses_slow_down_error_code(event_bytes: &[u8]) -> Option<&'static str> {
    let payload = std::str::from_utf8(event_bytes).ok()?;
    let mut event_name: Option<&str> = None;
    let mut data_lines = Vec::new();

    for line in payload.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        if let Some(value) = line.strip_prefix("event:") {
            event_name = Some(value.trim());
            continue;
        }
        if let Some(value) = line.strip_prefix("data:") {
            data_lines.push(value.trim_start());
        }
    }

    if data_lines.is_empty() {
        return None;
    }

    let data = data_lines.join("\n");
    let data = data.trim();
    if data.is_empty() || data == "[DONE]" {
        return None;
    }

    let json: Value = serde_json::from_str(data).ok()?;
    let event_type = json.get("type").and_then(Value::as_str);
    let code = match (event_name, event_type) {
        (Some("response.failed"), _) | (_, Some("response.failed")) => {
            json.pointer("/response/error/code").and_then(Value::as_str)
        }
        (Some("error"), _) | (_, Some("error")) => {
            json.pointer("/error/code").and_then(Value::as_str)
        }
        _ => None,
    };

    match code {
        Some("slow_down") => Some("slow_down"),
        Some("server_is_overloaded") => Some("server_is_overloaded"),
        _ => None,
    }
}

pub(crate) fn find_sse_event_boundary(buf: &[u8]) -> Option<usize> {
    let mut idx = 0usize;
    while idx < buf.len() {
        match buf[idx] {
            b'\n' if buf.get(idx + 1) == Some(&b'\n') => return Some(idx + 2),
            b'\r'
                if buf.get(idx + 1) == Some(&b'\n')
                    && buf.get(idx + 2) == Some(&b'\r')
                    && buf.get(idx + 3) == Some(&b'\n') =>
            {
                return Some(idx + 4);
            }
            _ => {}
        }
        idx += 1;
    }
    None
}

fn slow_down_disconnect_error(code: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::ConnectionAborted,
        format!("dropped upstream responses SSE failure event with retryable overload code {code}"),
    )
}

pin_project! {
    struct ResponsesSlowDownDropStream<S> {
        #[pin]
        inner: S,
        request_id: u64,
        route_pid: Option<u32>,
        provider_name: String,
        pending: BytesMut,
        pending_error: Option<std::io::Error>,
        finished: bool,
    }
}

impl<S> ResponsesSlowDownDropStream<S> {
    fn new(inner: S, request_id: u64, route_pid: Option<u32>, provider_name: String) -> Self {
        Self {
            inner,
            request_id,
            route_pid,
            provider_name,
            pending: BytesMut::new(),
            pending_error: None,
            finished: false,
        }
    }
}

impl<S> Stream for ResponsesSlowDownDropStream<S>
where
    S: Stream<Item = Result<Bytes, std::io::Error>>,
{
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();

        if let Some(err) = this.pending_error.take() {
            return std::task::Poll::Ready(Some(Err(err)));
        }

        if *this.finished {
            return std::task::Poll::Ready(None);
        }

        loop {
            match this.inner.as_mut().poll_next(cx) {
                std::task::Poll::Ready(Some(Ok(chunk))) => {
                    this.pending.extend_from_slice(&chunk);
                    let mut passthrough = BytesMut::new();

                    while let Some(boundary_end) = find_sse_event_boundary(this.pending.as_ref()) {
                        let event_bytes = this.pending.split_to(boundary_end).freeze();
                        if let Some(code) = responses_slow_down_error_code(&event_bytes) {
                            warn_slow_down_drop(
                                *this.request_id,
                                *this.route_pid,
                                this.provider_name.as_str(),
                                code,
                            );
                            this.pending.clear();
                            *this.finished = true;
                            let err = slow_down_disconnect_error(code);
                            if !passthrough.is_empty() {
                                *this.pending_error = Some(err);
                                return std::task::Poll::Ready(Some(Ok(passthrough.freeze())));
                            }
                            return std::task::Poll::Ready(Some(Err(err)));
                        }
                        passthrough.extend_from_slice(&event_bytes);
                    }

                    if !passthrough.is_empty() {
                        return std::task::Poll::Ready(Some(Ok(passthrough.freeze())));
                    }
                }
                std::task::Poll::Ready(Some(Err(err))) => {
                    this.pending.clear();
                    *this.finished = true;
                    return std::task::Poll::Ready(Some(Err(err)));
                }
                std::task::Poll::Ready(None) => {
                    *this.finished = true;
                    if this.pending.is_empty() {
                        return std::task::Poll::Ready(None);
                    }

                    let tail = this.pending.split().freeze();
                    if let Some(code) = responses_slow_down_error_code(&tail) {
                        warn_slow_down_drop(
                            *this.request_id,
                            *this.route_pid,
                            this.provider_name.as_str(),
                            code,
                        );
                        return std::task::Poll::Ready(Some(Err(slow_down_disconnect_error(code))));
                    }

                    return std::task::Poll::Ready(Some(Ok(tail)));
                }
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
    }
}

fn warn_slow_down_drop(request_id: u64, route_pid: Option<u32>, provider_name: &str, code: &str) {
    warn!(
        request_id,
        route_pid = ?route_pid,
        provider = %provider_name,
        code,
        "dropping upstream responses SSE overload event and disconnecting client"
    );
}

fn map_body_err<E>(err: E) -> std::io::Error
where
    E: Into<Box<dyn StdError + Send + Sync>>,
{
    std::io::Error::other(err)
}

struct ErrorLogDetails {
    chain: String,
    root_cause: String,
}

fn error_chain_parts(err: &(dyn StdError + 'static)) -> Vec<String> {
    let mut parts = Vec::new();
    push_error_chain_part(&mut parts, err.to_string());

    let mut source = next_error_in_chain(err);
    while let Some(cause) = source {
        push_error_chain_part(&mut parts, cause.to_string());
        source = next_error_in_chain(cause);
    }

    parts
}

fn next_error_in_chain<'a>(
    err: &'a (dyn StdError + 'static),
) -> Option<&'a (dyn StdError + 'static)> {
    if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
        if let Some(inner) = io_err.get_ref() {
            return Some(inner as &(dyn StdError + 'static));
        }
    }

    err.source()
}

fn push_error_chain_part(parts: &mut Vec<String>, part: String) {
    if parts.last() != Some(&part) {
        parts.push(part);
    }
}

fn format_error_chain(err: &(dyn StdError + 'static)) -> String {
    error_chain_parts(err).join(": ")
}

fn error_log_details(err: &(dyn StdError + 'static)) -> ErrorLogDetails {
    let parts = error_chain_parts(err);
    let root_cause = parts.last().cloned().unwrap_or_else(|| err.to_string());
    ErrorLogDetails {
        chain: parts.join(": "),
        root_cause,
    }
}

fn find_error_in_chain<'a, E>(err: &'a (dyn StdError + 'static)) -> Option<&'a E>
where
    E: StdError + 'static,
{
    if let Some(found) = err.downcast_ref::<E>() {
        return Some(found);
    }

    let mut source = next_error_in_chain(err);
    while let Some(cause) = source {
        if let Some(found) = cause.downcast_ref::<E>() {
            return Some(found);
        }
        source = next_error_in_chain(cause);
    }

    None
}

async fn send_with_idle_timeout<F>(
    request_id: u64,
    idle_timeout: Duration,
    upload_activity: Option<Arc<Notify>>,
    send_fut: F,
) -> std::result::Result<reqwest::Response, std::io::Error>
where
    F: Future<Output = std::result::Result<reqwest::Response, reqwest::Error>>,
{
    tokio::pin!(send_fut);
    let sleep = tokio::time::sleep(idle_timeout);
    tokio::pin!(sleep);

    let timed_out = || {
        warn!(
            request_id,
            idle_timeout_secs = idle_timeout.as_secs(),
            "closing proxied connection after upstream idle timeout while sending request or waiting for response headers"
        );
        Err(idle_timeout_error(
            "sending request or waiting for upstream response headers",
            idle_timeout,
        ))
    };

    let Some(upload_activity) = upload_activity else {
        return tokio::select! {
            result = &mut send_fut => result.map_err(map_body_err),
            _ = &mut sleep => timed_out(),
        };
    };

    loop {
        tokio::select! {
            result = &mut send_fut => {
                return result.map_err(map_body_err);
            }
            _ = &mut sleep => {
                return timed_out();
            }
            _ = upload_activity.notified() => {
                sleep.as_mut().reset(TokioInstant::now() + idle_timeout);
            }
        }
    }
}

async fn send_with_optional_idle_timeout<F>(
    request_id: u64,
    idle_timeout: Option<Duration>,
    upload_activity: Option<Arc<Notify>>,
    send_fut: F,
) -> std::result::Result<reqwest::Response, std::io::Error>
where
    F: Future<Output = std::result::Result<reqwest::Response, reqwest::Error>>,
{
    match idle_timeout {
        Some(idle_timeout) => {
            send_with_idle_timeout(request_id, idle_timeout, upload_activity, send_fut).await
        }
        None => send_fut.await.map_err(map_body_err),
    }
}

async fn send_with_non_2xx_retries(
    args: RetrySendArgs,
) -> std::result::Result<(reqwest::Response, PreparedUpstreamAttempt, u32, u128), std::io::Error> {
    send_with_non_2xx_retries_with_sleep(args, tokio::time::sleep).await
}

async fn resolve_retry_attempt_context(
    args: &RetrySendArgs,
    attempt_index: u32,
) -> std::result::Result<RetryAttemptContext, std::io::Error> {
    if attempt_index == 0 {
        return Ok(RetryAttemptContext {
            config: args.initial_config.clone(),
            http_client: args.initial_http_client.clone(),
            attempt: args.initial_attempt.clone(),
            rerouted: false,
        });
    }

    let routing = args.state.runtime.routing_snapshot().await;
    let attempt = resolve_upstream_attempt(ResolveUpstreamAttemptArgs {
        state: &args.state,
        cfg: &routing.config,
        default_provider: &routing.default_provider,
        pid: args.pid,
        peer: args.peer,
        request_id: args.request_id,
        forwarded_path: &args.request.forwarded_path,
        incoming_query: args.request.incoming_query.as_deref(),
        base_headers: &args.request.base_headers,
    })
    .await
    .map_err(std::io::Error::other)?;

    Ok(RetryAttemptContext {
        config: routing.config,
        http_client: routing.http_client,
        attempt,
        rerouted: true,
    })
}

async fn update_exchange_logger_upstream_target(
    exchange_logger: Option<SharedExchangeFileLogger>,
    request_id: u64,
    attempt: &PreparedUpstreamAttempt,
) {
    let provider_name = attempt.provider_name.clone();
    let upstream_url = attempt.url.clone();
    let route_pid = attempt.route_pid;
    with_exchange_logger_blocking(
        exchange_logger,
        request_id,
        "update exchange logger upstream target",
        move |logger| {
            logger.update_upstream_target(crate::exchange_log::AttemptRouteContext {
                route_pid,
                provider_name: &provider_name,
                upstream_url: &upstream_url,
            });
        },
    )
    .await;
}

enum TransparentRetryReason<'a> {
    SendError(&'a std::io::Error),
    Non2xxStatus(StatusCode),
}

async fn wait_before_transparent_retry<S, Fut>(
    args: &RetrySendArgs,
    attempt_index: u32,
    attempt_number: u32,
    current_attempt: &PreparedUpstreamAttempt,
    reason: TransparentRetryReason<'_>,
    sleep_fn: &S,
) where
    S: Fn(Duration) -> Fut,
    Fut: Future<Output = ()>,
{
    let retry_backoff =
        linear_retry_backoff_delay(args.transparent_retry_backoff_step, attempt_number);
    let route_pid = current_attempt.route_pid;
    let provider = current_attempt.provider_name.as_str();
    let upstream_url = &current_attempt.url;
    let total_attempts = args.transparent_retry_count.saturating_add(1);
    let retries_remaining = args.transparent_retry_count - attempt_index;
    let retry_backoff_ms = retry_backoff.as_millis();
    match reason {
        TransparentRetryReason::SendError(err) => {
            let error_details = error_log_details(err);
            let reqwest_error = find_error_in_chain::<reqwest::Error>(err);
            let reqwest_is_request = reqwest_error.is_some_and(reqwest::Error::is_request);
            let reqwest_is_connect = reqwest_error.is_some_and(reqwest::Error::is_connect);
            let reqwest_is_timeout = reqwest_error.is_some_and(reqwest::Error::is_timeout);
            let reqwest_is_body = reqwest_error.is_some_and(reqwest::Error::is_body);
            warn!(
                args.request_id,
                error = %err,
                error_kind = ?err.kind(),
                error_root_cause = %error_details.root_cause,
                error_chain = %error_details.chain,
                reqwest_is_request,
                reqwest_is_connect,
                reqwest_is_timeout,
                reqwest_is_body,
                attempt = attempt_number,
                route_pid,
                provider = %provider,
                upstream_url = %upstream_url,
                total_attempts,
                retries_remaining,
                retry_backoff_ms,
                "upstream request send failed before downstream response; retrying transparently"
            );
        }
        TransparentRetryReason::Non2xxStatus(status) => {
            warn!(
                args.request_id,
                status = %status,
                attempt = attempt_number,
                route_pid,
                provider = %provider,
                upstream_url = %upstream_url,
                total_attempts,
                retries_remaining,
                retry_backoff_ms,
                "upstream returned non-2xx status; retrying transparently"
            );
        }
    }

    if !retry_backoff.is_zero() {
        sleep_fn(retry_backoff).await;
    }
}

async fn send_with_non_2xx_retries_with_sleep<S, Fut>(
    args: RetrySendArgs,
    sleep_fn: S,
) -> std::result::Result<(reqwest::Response, PreparedUpstreamAttempt, u32, u128), std::io::Error>
where
    S: Fn(Duration) -> Fut,
    Fut: Future<Output = ()>,
{
    for attempt in 0..=args.transparent_retry_count {
        let context = resolve_retry_attempt_context(&args, attempt).await?;
        let cfg = context.config;
        let http_client = context.http_client;
        let mut current_attempt = context.attempt;
        if context.rerouted {
            update_exchange_logger_upstream_target(
                args.exchange_logger.clone(),
                args.request_id,
                &current_attempt,
            )
            .await;
        }

        let attempt_started = Instant::now();
        let attempt_number = attempt.saturating_add(1);
        let request_body = prepare_request_body_for_attempt(
            &args.state,
            &cfg,
            &args.request.method,
            &args.request.forwarded_path,
            &mut current_attempt,
            args.request.request_body.clone(),
            args.request_id,
            attempt_number,
        )?;
        let attempt_request_body_for_log = if cfg.logging.log_conversion_pairs {
            // The attempt request_body file holds the pre-conversion body.
            Some(args.request.request_body.clone())
        } else {
            Some(request_body.clone())
        };
        begin_exchange_log_attempt(
            args.exchange_logger.clone(),
            args.statistics.clone(),
            args.request_id,
            attempt_number,
            &current_attempt,
            &args.request.method,
            attempt_request_body_for_log,
            cfg.logging.log_conversion_pairs
                && crate::api_conversion::request_conversion_enabled(
                    &cfg,
                    &args.request.method,
                    &current_attempt.provider_name,
                    &args.request.forwarded_path,
                ),
        )
        .await;
        let (req_body, _) = if cfg.logging.log_conversion_pairs {
            body_from_bytes_for_logging(
                &cfg,
                request_body,
                args.exchange_logger.clone(),
                None,
                args.request_id,
            )
            .await
        } else {
            (reqwest::Body::from(request_body), None)
        };
        let out = http_client
            .request(args.request.method.clone(), current_attempt.url.clone())
            .headers(current_attempt.headers.clone())
            .body(req_body);
        let resp =
            send_with_optional_idle_timeout(args.request_id, args.idle_timeout, None, out.send())
                .await;
        let resp = match resp {
            Ok(resp) => resp,
            Err(err) => {
                let is_final_attempt = attempt == args.transparent_retry_count;
                record_exchange_log_attempt_send_error(
                    args.exchange_logger.clone(),
                    args.statistics.clone(),
                    args.request_id,
                    attempt_number,
                    attempt_started.elapsed().as_millis(),
                    &err,
                    is_final_attempt,
                )
                .await;
                if is_final_attempt {
                    return Err(err);
                }

                wait_before_transparent_retry(
                    &args,
                    attempt,
                    attempt_number,
                    &current_attempt,
                    TransparentRetryReason::SendError(&err),
                    &sleep_fn,
                )
                .await;
                continue;
            }
        };

        let status = resp.status();
        let attempt_latency_ms = attempt_started.elapsed().as_millis();
        if status.is_success() || attempt == args.transparent_retry_count {
            return Ok((resp, current_attempt, attempt_number, attempt_latency_ms));
        }

        record_exchange_log_attempt_response(
            args.exchange_logger.clone(),
            args.request_id,
            AttemptResponseLog {
                attempt_number,
                attempt: &current_attempt,
                status,
                headers: resp.headers().clone(),
                latency_ms: attempt_latency_ms,
                is_final: false,
            },
        )
        .await;

        drain_response_body_with_optional_idle_timeout(
            args.request_id,
            args.idle_timeout,
            args.exchange_logger.clone(),
            attempt_number,
            resp,
            "reading non-final retry response body",
        )
        .await?;
        finish_exchange_log_attempt(
            args.exchange_logger.clone(),
            args.request_id,
            attempt_number,
        )
        .await;
        wait_before_transparent_retry(
            &args,
            attempt,
            attempt_number,
            &current_attempt,
            TransparentRetryReason::Non2xxStatus(status),
            &sleep_fn,
        )
        .await;
    }

    unreachable!("retry loop always returns")
}

fn linear_retry_backoff_delay(backoff_step: Duration, retry_number: u32) -> Duration {
    if retry_number == 0 || backoff_step.is_zero() {
        return Duration::ZERO;
    }

    backoff_step
        .checked_mul(retry_number)
        .unwrap_or(Duration::MAX)
}

async fn drain_response_body_with_optional_idle_timeout(
    request_id: u64,
    idle_timeout: Option<Duration>,
    exchange_logger: Option<SharedExchangeFileLogger>,
    attempt_number: u32,
    resp: reqwest::Response,
    phase: &'static str,
) -> std::result::Result<(), std::io::Error> {
    let mut stream = TryStreamExt::map_err(resp.bytes_stream(), map_body_err);

    loop {
        let next_chunk = match idle_timeout {
            Some(idle_timeout) => match tokio::time::timeout(idle_timeout, stream.try_next()).await
            {
                Ok(next) => next,
                Err(_) => {
                    warn!(
                        request_id,
                        phase,
                        idle_timeout_secs = idle_timeout.as_secs(),
                        "closing retry attempt body drain after upstream idle timeout"
                    );
                    return Err(idle_timeout_error(phase, idle_timeout));
                }
            },
            None => stream.try_next().await,
        }?;
        let Some(chunk) = next_chunk else {
            break;
        };
        append_exchange_log_attempt_response_chunk(
            exchange_logger.clone(),
            request_id,
            attempt_number,
            chunk,
        )
        .await;
    }

    Ok(())
}

fn idle_timeout_error(phase: &'static str, idle_timeout: Duration) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!(
            "no upstream data for {}s while {phase}",
            idle_timeout.as_secs()
        ),
    )
}

pin_project! {
    pub struct IdleTimeoutStream<S> {
        #[pin]
        inner: S,
        idle_timeout: Duration,
        request_id: u64,
        phase: &'static str,
        #[pin]
        sleep: Sleep,
        emitted_timeout: bool,
    }
}

impl<S> IdleTimeoutStream<S> {
    pub fn new(inner: S, idle_timeout: Duration, request_id: u64, phase: &'static str) -> Self {
        Self {
            inner,
            idle_timeout,
            request_id,
            phase,
            sleep: tokio::time::sleep(idle_timeout),
            emitted_timeout: false,
        }
    }
}

impl<S> Stream for IdleTimeoutStream<S>
where
    S: Stream<Item = Result<Bytes, std::io::Error>>,
{
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();

        if *this.emitted_timeout {
            return std::task::Poll::Ready(None);
        }

        match this.inner.as_mut().poll_next(cx) {
            std::task::Poll::Ready(Some(item)) => {
                if item.is_ok() {
                    this.sleep
                        .as_mut()
                        .reset(TokioInstant::now() + *this.idle_timeout);
                }
                std::task::Poll::Ready(Some(item))
            }
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => match this.sleep.as_mut().poll(cx) {
                std::task::Poll::Ready(()) => {
                    *this.emitted_timeout = true;
                    warn!(
                        request_id = *this.request_id,
                        phase = *this.phase,
                        idle_timeout_secs = this.idle_timeout.as_secs(),
                        "closing proxied stream after upstream idle timeout"
                    );
                    std::task::Poll::Ready(Some(Err(idle_timeout_error(
                        this.phase,
                        *this.idle_timeout,
                    ))))
                }
                std::task::Poll::Pending => std::task::Poll::Pending,
            },
        }
    }
}

pin_project! {
    pub struct LogOnEndStream<S, F: FnOnce()> {
        #[pin]
        inner: S,
        on_end: Option<F>,
    }

    impl<S, F: FnOnce()> PinnedDrop for LogOnEndStream<S, F>
    {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            if let Some(on_end) = this.on_end.take() {
                on_end();
            }
        }
    }
}

impl<S, F: FnOnce()> LogOnEndStream<S, F> {
    pub fn new(inner: S, on_end: F) -> Self {
        Self {
            inner,
            on_end: Some(on_end),
        }
    }
}

impl<S, F: FnOnce()> Stream for LogOnEndStream<S, F>
where
    S: Stream,
{
    type Item = S::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        match this.inner.poll_next(cx) {
            std::task::Poll::Ready(None) => {
                if let Some(on_end) = this.on_end.take() {
                    on_end();
                }
                std::task::Poll::Ready(None)
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        error::Error as StdError,
        fmt,
        net::SocketAddr,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use axum::{
        body::{to_bytes, Body, Bytes},
        extract::State,
        http::{header, HeaderMap, Method, Request, Response, StatusCode, Uri},
        response::IntoResponse,
        routing::any,
        Router,
    };
    use bytes::BytesMut;
    use futures_util::{stream, StreamExt};
    use pid_resolver::platform::default_pid_resolver;
    use serde_json::json;
    use tokio::sync::oneshot;
    use tracing_subscriber::EnvFilter;
    use url::Url;

    use crate::{
        config::{
            BodyLogCompression, Config, LoggingConfig, ModelMapping, Provider, RewriteConfig,
            StatisticsConfig,
        },
        rewrite::{AnthropicBetaMarker, AnthropicBetaPrefix, AnthropicBetaUpdate},
        runtime::RuntimeState,
        statistics::StatisticsManager,
    };

    use super::{
        apply_anthropic_beta_updates, buffer_request_body, downstream_response_status,
        error_log_details, filtered_incoming_headers, filtered_response_headers,
        format_error_chain, handle_proxy_inner, is_text_event_stream, join_paths,
        linear_retry_backoff_delay, maybe_filter_responses_slow_down_stream,
        path_is_messages_count_tokens, proxy_error_response, resolve_upstream_attempt,
        responses_slow_down_error_code, send_with_non_2xx_retries,
        send_with_non_2xx_retries_with_sleep, should_drop_responses_slow_down_errors,
        strip_listen_base_path, ProxyState, RequestBodyTooLarge, ResolveUpstreamAttemptArgs,
        RetryRequestTemplate, RetrySendArgs, ANTHROPIC_BETA_HEADER, KEEP_ALIVE_HEADER,
    };

    #[tokio::test]
    async fn request_body_buffer_accepts_exact_limit_and_rejects_over_limit() {
        let exact = buffer_request_body(Body::from("1234"), 4, None)
            .await
            .expect("exact limit should succeed");
        assert_eq!(exact, "1234");

        let error = buffer_request_body(Body::from("12345"), 4, None)
            .await
            .expect_err("over limit should fail");
        assert!(error
            .get_ref()
            .and_then(|source| source.downcast_ref::<RequestBodyTooLarge>())
            .is_some());
    }

    #[tokio::test]
    async fn request_body_limit_error_returns_payload_too_large() {
        let error = std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            RequestBodyTooLarge { limit: 4 },
        );
        let response = proxy_error_response(anyhow::Error::new(error));

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            body,
            "request body exceeds the configured 4-byte buffering limit\n"
        );
    }

    #[test]
    fn filtered_incoming_headers_removes_connection_named_header() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONNECTION, "Foo".parse().unwrap());
        headers.insert("foo", "remove-me".parse().unwrap());
        headers.insert(header::HOST, "proxy.test".parse().unwrap());
        headers.insert(header::AUTHORIZATION, "Bearer secret".parse().unwrap());
        headers.insert("x-preserved", "keep-me".parse().unwrap());

        let filtered = filtered_incoming_headers(&headers);

        assert_eq!(filtered.get("x-preserved").unwrap(), "keep-me");
        assert!(!filtered.contains_key(header::CONNECTION));
        assert!(!filtered.contains_key("foo"));
        assert!(!filtered.contains_key(header::HOST));
        assert!(!filtered.contains_key(header::AUTHORIZATION));
    }

    #[test]
    fn filtered_response_headers_removes_keep_alive() {
        let mut headers = HeaderMap::new();
        headers.insert(KEEP_ALIVE_HEADER, "timeout=5".parse().unwrap());
        headers.insert("x-preserved", "keep-me".parse().unwrap());

        let filtered = filtered_response_headers(&headers);

        assert_eq!(filtered.get("x-preserved").unwrap(), "keep-me");
        assert!(!filtered.contains_key(KEEP_ALIVE_HEADER));
    }

    #[test]
    fn filtered_response_headers_honors_multiple_connection_values() {
        let mut headers = HeaderMap::new();
        headers.append(header::CONNECTION, "Foo".parse().unwrap());
        headers.append(header::CONNECTION, "Bar, Baz".parse().unwrap());
        headers.insert("foo", "remove-me".parse().unwrap());
        headers.insert("bar", "remove-me".parse().unwrap());
        headers.insert("baz", "remove-me".parse().unwrap());
        headers.insert("x-preserved", "keep-me".parse().unwrap());

        let filtered = filtered_response_headers(&headers);

        assert_eq!(filtered.get("x-preserved").unwrap(), "keep-me");
        assert!(!filtered.contains_key(header::CONNECTION));
        assert!(!filtered.contains_key("foo"));
        assert!(!filtered.contains_key("bar"));
        assert!(!filtered.contains_key("baz"));
    }

    #[test]
    fn apply_anthropic_beta_updates_adds_deduplicates_and_removes_by_prefix() {
        let mut headers = HeaderMap::new();
        apply_anthropic_beta_updates(
            &mut headers,
            &[AnthropicBetaUpdate::Ensure(AnthropicBetaMarker::Effort)],
        );
        assert!(headers
            .get(ANTHROPIC_BETA_HEADER)
            .unwrap()
            .to_str()
            .unwrap()
            .contains("effort"));

        headers.insert(
            ANTHROPIC_BETA_HEADER,
            "claude-code-20250219".parse().unwrap(),
        );
        apply_anthropic_beta_updates(
            &mut headers,
            &[AnthropicBetaUpdate::Ensure(AnthropicBetaMarker::Effort)],
        );
        let beta = headers
            .get(ANTHROPIC_BETA_HEADER)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(beta.contains("claude-code-20250219"));
        assert!(beta.contains("effort"));

        headers.insert(
            ANTHROPIC_BETA_HEADER,
            "claude-code-20250219,context-1m-2099-01-01"
                .parse()
                .unwrap(),
        );
        apply_anthropic_beta_updates(
            &mut headers,
            &[AnthropicBetaUpdate::Ensure(AnthropicBetaMarker::Context1m)],
        );
        let beta = headers
            .get(ANTHROPIC_BETA_HEADER)
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(
            beta.split(',')
                .map(str::trim)
                .filter(|token| token.starts_with("context-1m"))
                .count(),
            1
        );

        apply_anthropic_beta_updates(
            &mut headers,
            &[
                AnthropicBetaUpdate::Ensure(AnthropicBetaMarker::Effort),
                AnthropicBetaUpdate::RemoveByPrefix(AnthropicBetaPrefix::Context1m),
            ],
        );
        let beta = headers
            .get(ANTHROPIC_BETA_HEADER)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(beta.contains("claude-code-20250219"));
        assert!(beta.contains("effort"));
        assert!(!beta.contains("context-1m"));
    }

    #[derive(Debug)]
    struct ChainedTestError {
        message: &'static str,
        source: Option<Box<ChainedTestError>>,
    }

    impl fmt::Display for ChainedTestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.message)
        }
    }

    impl StdError for ChainedTestError {
        fn source(&self) -> Option<&(dyn StdError + 'static)> {
            self.source
                .as_deref()
                .map(|err| err as &(dyn StdError + 'static))
        }
    }

    #[test]
    fn joins_paths_with_prefix() {
        assert_eq!(join_paths("/", "/v1/models"), "/v1/models");
        assert_eq!(join_paths("/v1/", "/models"), "/v1/models");
        assert_eq!(join_paths("/v1", "/models"), "/v1/models");
        assert_eq!(join_paths("/v1", "/"), "/v1/");
    }

    #[test]
    fn strips_listen_base_path() {
        assert_eq!(
            strip_listen_base_path("/", "/v1/models"),
            Some("/v1/models")
        );
        assert_eq!(strip_listen_base_path("/v1", "/v1/models"), Some("models"));
        assert_eq!(strip_listen_base_path("/v1", "/v1"), Some("/"));
        assert_eq!(strip_listen_base_path("/v1", "/v2/models"), None);
    }

    #[test]
    fn detects_messages_count_tokens_endpoint() {
        assert!(path_is_messages_count_tokens("/messages/count_tokens"));
        assert!(path_is_messages_count_tokens("messages/count_tokens"));
        assert!(path_is_messages_count_tokens("/messages/count_tokens/"));
        assert!(path_is_messages_count_tokens("/v1/messages/count_tokens"));
        assert!(path_is_messages_count_tokens(
            "/api/v1/messages/count_tokens"
        ));
        assert!(!path_is_messages_count_tokens(
            "/messages/count_tokens/extra"
        ));
        assert!(!path_is_messages_count_tokens("/messages"));
    }

    #[test]
    fn computes_linear_retry_backoff_delay() {
        assert_eq!(
            linear_retry_backoff_delay(Duration::from_millis(250), 1),
            Duration::from_millis(250)
        );
        assert_eq!(
            linear_retry_backoff_delay(Duration::from_millis(250), 3),
            Duration::from_millis(750)
        );
        assert_eq!(
            linear_retry_backoff_delay(Duration::ZERO, 3),
            Duration::ZERO
        );
    }

    #[test]
    fn formats_full_error_chain_for_send_error_logging() {
        let err = ChainedTestError {
            message: "error sending request for url (https://example.test/v1/responses)",
            source: Some(Box::new(ChainedTestError {
                message: "client error (Connect)",
                source: Some(Box::new(ChainedTestError {
                    message: "tcp connect error: Connection refused (os error 111)",
                    source: None,
                })),
            })),
        };

        assert_eq!(
            format_error_chain(&err),
            "error sending request for url (https://example.test/v1/responses): client error (Connect): tcp connect error: Connection refused (os error 111)"
        );

        let details = error_log_details(&err);
        assert_eq!(
            details.root_cause,
            "tcp connect error: Connection refused (os error 111)"
        );
    }

    #[test]
    fn formats_io_error_wrapped_source_chain_for_send_error_logging() {
        let err = std::io::Error::other(ChainedTestError {
            message: "error sending request for url (https://example.test/v1/responses)",
            source: Some(Box::new(ChainedTestError {
                message: "client error (Connect)",
                source: Some(Box::new(ChainedTestError {
                    message: "tcp connect error: Connection refused (os error 111)",
                    source: None,
                })),
            })),
        });

        assert_eq!(
            format_error_chain(&err),
            "error sending request for url (https://example.test/v1/responses): client error (Connect): tcp connect error: Connection refused (os error 111)"
        );
    }

    #[derive(Clone)]
    struct RetryServerState {
        statuses: Arc<Vec<StatusCode>>,
        call_count: Arc<AtomicUsize>,
    }

    async fn retry_server_handler(
        State(state): State<RetryServerState>,
        method: Method,
        body: Bytes,
    ) -> impl IntoResponse {
        if method != Method::HEAD {
            assert_eq!(body, Bytes::from_static(b"retry-body"));
        }

        let call_index = state.call_count.fetch_add(1, Ordering::SeqCst);
        let status = state
            .statuses
            .get(call_index)
            .copied()
            .or_else(|| state.statuses.last().copied())
            .unwrap_or(StatusCode::OK);
        (status, format!("attempt-{call_index}"))
    }

    async fn spawn_test_server(app: Router) -> (Url, oneshot::Sender<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });
        (Url::parse(&format!("http://{addr}/")).unwrap(), shutdown_tx)
    }

    async fn spawn_retry_server(
        statuses: Vec<StatusCode>,
    ) -> (Url, Arc<AtomicUsize>, oneshot::Sender<()>) {
        let call_count = Arc::new(AtomicUsize::new(0));
        let state = RetryServerState {
            statuses: Arc::new(statuses),
            call_count: call_count.clone(),
        };
        let app = Router::new()
            .route("/", any(retry_server_handler))
            .with_state(state);
        let (url, shutdown_tx) = spawn_test_server(app).await;
        (url, call_count, shutdown_tx)
    }

    fn unused_loopback_url() -> Url {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        Url::parse(&format!("http://{addr}/")).unwrap()
    }

    #[derive(Clone)]
    struct AuthCaptureServerState {
        status: StatusCode,
        label: Arc<str>,
        call_count: Arc<AtomicUsize>,
        auth_headers: Arc<Mutex<Vec<String>>>,
    }

    async fn auth_capture_server_handler(
        State(state): State<AuthCaptureServerState>,
        headers: HeaderMap,
        body: Bytes,
    ) -> impl IntoResponse {
        assert_eq!(body, Bytes::from_static(b"retry-body"));
        let auth = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        state.auth_headers.lock().unwrap().push(auth);
        state.call_count.fetch_add(1, Ordering::SeqCst);
        (state.status, state.label.to_string())
    }

    async fn spawn_auth_capture_server(
        status: StatusCode,
        label: &'static str,
    ) -> (
        Url,
        Arc<AtomicUsize>,
        Arc<Mutex<Vec<String>>>,
        oneshot::Sender<()>,
    ) {
        let call_count = Arc::new(AtomicUsize::new(0));
        let auth_headers = Arc::new(Mutex::new(Vec::new()));
        let state = AuthCaptureServerState {
            status,
            label: Arc::from(label),
            call_count: call_count.clone(),
            auth_headers: auth_headers.clone(),
        };
        let app = Router::new()
            .route("/", any(auth_capture_server_handler))
            .with_state(state);
        let (url, shutdown_tx) = spawn_test_server(app).await;
        (url, call_count, auth_headers, shutdown_tx)
    }

    #[derive(Clone)]
    struct PathCaptureServerState {
        requests: Arc<Mutex<Vec<Uri>>>,
    }

    async fn path_capture_server_handler(
        State(state): State<PathCaptureServerState>,
        uri: Uri,
    ) -> impl IntoResponse {
        state.requests.lock().unwrap().push(uri);
        (StatusCode::OK, "ok")
    }

    async fn spawn_path_capture_server() -> (Url, Arc<Mutex<Vec<Uri>>>, oneshot::Sender<()>) {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let state = PathCaptureServerState {
            requests: requests.clone(),
        };
        let app = Router::new()
            .route("/messages/count_tokens", any(path_capture_server_handler))
            .with_state(state);
        let (url, shutdown_tx) = spawn_test_server(app).await;
        (url, requests, shutdown_tx)
    }

    #[derive(Clone)]
    struct BodyCaptureServerState {
        bodies: Arc<Mutex<Vec<Bytes>>>,
        headers: Arc<Mutex<Vec<HeaderMap>>>,
    }

    async fn body_capture_server_handler(
        State(state): State<BodyCaptureServerState>,
        headers: HeaderMap,
        body: Bytes,
    ) -> impl IntoResponse {
        state.headers.lock().unwrap().push(headers);
        state.bodies.lock().unwrap().push(body);
        (StatusCode::OK, "ok")
    }

    async fn spawn_body_capture_server(
        path: &'static str,
    ) -> (
        Url,
        Arc<Mutex<Vec<Bytes>>>,
        Arc<Mutex<Vec<HeaderMap>>>,
        oneshot::Sender<()>,
    ) {
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let headers = Arc::new(Mutex::new(Vec::new()));
        let state = BodyCaptureServerState {
            bodies: bodies.clone(),
            headers: headers.clone(),
        };
        let app = Router::new()
            .route(path, any(body_capture_server_handler))
            .with_state(state);
        let (url, shutdown_tx) = spawn_test_server(app).await;
        (url, bodies, headers, shutdown_tx)
    }

    async fn compressed_response_handler() -> Response<Body> {
        let payload = br#"{"error":{"message":"decoded upstream response"},"type":"error"}"#;
        let compressed = zstd::stream::encode_all(payload.as_slice(), 3).unwrap();
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header(header::CONTENT_ENCODING, "zstd")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(compressed))
            .unwrap()
    }

    async fn spawn_compressed_response_server() -> (Url, oneshot::Sender<()>) {
        let app = Router::new().route("/messages", any(compressed_response_handler));
        spawn_test_server(app).await
    }

    fn test_logging_config() -> LoggingConfig {
        LoggingConfig {
            log_requests: false,
            log_responses: false,
            log_bodies: false,
            max_body_log_bytes: 8192,
            exchange_log_dir: None,
            exchange_body_max_bytes: None,
            exchange_body_compression: BodyLogCompression::None,
            reconstruct_responses: false,
            log_conversion_pairs: false,
            level: "info".to_string(),
            rule: None,
        }
    }

    fn test_proxy_state(config: Config) -> ProxyState {
        let (_filter_layer, filter_reload) =
            tracing_subscriber::reload::Layer::new(EnvFilter::new("info"));
        let statistics = StatisticsManager::new(&config.statistics).unwrap();
        let runtime = RuntimeState::new(
            Arc::new(config),
            default_pid_resolver(),
            reqwest::Client::new(),
            filter_reload,
            statistics,
        );
        ProxyState {
            listen_addr: "127.0.0.1:8080".parse().unwrap(),
            runtime,
        }
    }

    fn test_config(default_provider: &str, providers: HashMap<String, Provider>) -> Config {
        Config {
            listen_addrs: vec!["127.0.0.1:8080".parse().unwrap()],
            listen_base_path: "/".to_string(),
            rpc_listen_addr: "127.0.0.1:8081".parse().unwrap(),
            rpc_token: None,
            upstream_connect_timeout: None,
            upstream_idle_timeout: None,
            reject_messages_count_tokens: true,
            drop_responses_slow_down_errors: true,
            convert_429_to_503: true,
            transparent_retry_count: 0,
            transparent_retry_head_requests: false,
            transparent_retry_backoff_step: Duration::ZERO,
            request_body_buffer_max_bytes: 64 * 1024 * 1024,
            default_provider: default_provider.to_string(),
            providers,
            rewrite: RewriteConfig::default(),
            logging: test_logging_config(),
            statistics: StatisticsConfig {
                enabled: false,
                database_path: "unused.sqlite3".into(),
                request_body_max_bytes: 1024,
                response_body_max_bytes: 1024,
            },
        }
    }

    fn test_provider(name: &str) -> Provider {
        Provider {
            base_url: Url::parse("http://127.0.0.1:1/").unwrap(),
            api_key: format!("token-{name}"),
            authorization_header: None,
            ..Provider::default()
        }
    }

    #[tokio::test]
    async fn routing_snapshot_keeps_default_provider_in_current_config() {
        use tracing_subscriber::layer::SubscriberExt;

        let providers = HashMap::from([
            ("provider_a".to_string(), test_provider("a")),
            ("provider_b".to_string(), test_provider("b")),
        ]);
        let config = test_config("provider_a", providers);
        let (filter_layer, filter_reload) =
            tracing_subscriber::reload::Layer::new(EnvFilter::new("info"));
        let subscriber = tracing_subscriber::registry().with(filter_layer);
        let _subscriber_guard = tracing::subscriber::set_default(subscriber);
        let runtime = RuntimeState::new(
            Arc::new(config.clone()),
            default_pid_resolver(),
            reqwest::Client::new(),
            filter_reload,
            StatisticsManager::new(&config.statistics).unwrap(),
        );
        let state = ProxyState {
            listen_addr: "127.0.0.1:8080".parse().unwrap(),
            runtime,
        };

        assert!(
            state
                .runtime
                .set_default_provider("provider_b".to_string())
                .await
        );
        let overridden = state.runtime.routing_snapshot().await;
        assert_eq!(overridden.default_provider, "provider_b");
        assert!(overridden
            .config
            .providers
            .contains_key(&overridden.default_provider));

        assert!(
            !state
                .runtime
                .set_default_provider("missing".to_string())
                .await
        );
        assert_eq!(
            state.runtime.routing_snapshot().await.default_provider,
            "provider_b"
        );

        let reloaded = test_config(
            "provider_c",
            HashMap::from([("provider_c".to_string(), test_provider("c"))]),
        );
        state
            .runtime
            .apply_config(Arc::new(reloaded))
            .await
            .unwrap();

        let snapshot = state.runtime.routing_snapshot().await;
        assert_eq!(snapshot.default_provider, "provider_c");
        assert!(snapshot
            .config
            .providers
            .contains_key(&snapshot.default_provider));
        assert!(!snapshot.config.providers.contains_key("provider_b"));

        let mut rejected = test_config(
            "provider_d",
            HashMap::from([("provider_d".to_string(), test_provider("d"))]),
        );
        rejected.logging.rule = Some("[".to_string());
        state
            .runtime
            .apply_config(Arc::new(rejected))
            .await
            .expect_err("invalid logging filter should reject config");

        let snapshot_after_rejection = state.runtime.routing_snapshot().await;
        assert_eq!(snapshot_after_rejection.default_provider, "provider_c");
        assert!(snapshot_after_rejection
            .config
            .providers
            .contains_key("provider_c"));
        assert!(!snapshot_after_rejection
            .config
            .providers
            .contains_key("provider_d"));
    }

    #[tokio::test]
    async fn blocks_messages_count_tokens_by_default() {
        let mut providers = HashMap::new();
        providers.insert(
            "provider_a".to_string(),
            Provider {
                base_url: Url::parse("http://127.0.0.1:1/").unwrap(),
                api_key: "token-a".to_string(),
                authorization_header: None,
                ..Provider::default()
            },
        );
        let mut cfg = test_config("provider_a", providers);
        cfg.listen_base_path = "/v1".to_string();
        let state = test_proxy_state(cfg);

        let req = Request::builder()
            .method(Method::POST)
            .uri("/v1/messages/count_tokens?beta=true")
            .body(Body::empty())
            .unwrap();

        let resp = handle_proxy_inner(state, "127.0.0.1:50010".parse().unwrap(), req)
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn blocks_messages_count_tokens_by_suffix_when_base_path_is_root() {
        let mut providers = HashMap::new();
        providers.insert(
            "provider_a".to_string(),
            Provider {
                base_url: Url::parse("http://127.0.0.1:1/").unwrap(),
                api_key: "token-a".to_string(),
                authorization_header: None,
                ..Provider::default()
            },
        );
        let cfg = test_config("provider_a", providers);
        let state = test_proxy_state(cfg);

        let req = Request::builder()
            .method(Method::POST)
            .uri("/v1/messages/count_tokens?beta=true")
            .body(Body::empty())
            .unwrap();

        let resp = handle_proxy_inner(state, "127.0.0.1:50012".parse().unwrap(), req)
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn forwards_messages_count_tokens_when_rejection_disabled() {
        let (url, requests, shutdown_tx) = spawn_path_capture_server().await;
        let mut providers = HashMap::new();
        providers.insert(
            "provider_a".to_string(),
            Provider {
                base_url: url,
                api_key: "token-a".to_string(),
                authorization_header: None,
                ..Provider::default()
            },
        );
        let mut cfg = test_config("provider_a", providers);
        cfg.listen_base_path = "/v1".to_string();
        cfg.reject_messages_count_tokens = false;
        let state = test_proxy_state(cfg);

        let req = Request::builder()
            .method(Method::POST)
            .uri("/v1/messages/count_tokens?beta=true")
            .body(Body::empty())
            .unwrap();

        let resp = handle_proxy_inner(state, "127.0.0.1:50011".parse().unwrap(), req)
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let captured = requests.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].path(), "/messages/count_tokens");
        assert_eq!(captured[0].query(), Some("beta=true"));
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn rewrites_model_mapping_before_forwarding_request_body() {
        let (url, bodies, headers, shutdown_tx) = spawn_body_capture_server("/responses").await;
        let mut providers = HashMap::new();
        providers.insert(
            "provider_a".to_string(),
            Provider {
                base_url: url,
                api_key: "token-a".to_string(),
                authorization_header: None,
                ..Provider::default()
            },
        );
        let mut cfg = test_config("provider_a", providers);
        cfg.listen_base_path = "/v1".to_string();
        cfg.rewrite.model_mappings.push(ModelMapping {
            provider: Some(vec!["provider_a".to_string()]),
            from_model: vec!["gpt-5.5".to_string()],
            from_reasoning_effort: Some("xhigh".to_string()),
            to_model: Some("grok-4.20-non-reasoning".to_string()),
            to_reasoning_effort: Some("high".to_string()),
        });
        let state = test_proxy_state(cfg);
        let original_body = Bytes::from_static(
            br#"{"model":"gpt-5.5","reasoning":{"effort":"xhigh","summary":"auto"},"stream":true,"input":[]}"#,
        );

        let req = Request::builder()
            .method(Method::POST)
            .uri("/v1/responses")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_LENGTH, original_body.len())
            .body(Body::from(original_body))
            .unwrap();
        let resp = handle_proxy_inner(state, "127.0.0.1:50021".parse().unwrap(), req)
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let captured_bodies = bodies.lock().unwrap();
        assert_eq!(captured_bodies.len(), 1);
        let captured: serde_json::Value = serde_json::from_slice(&captured_bodies[0]).unwrap();
        assert_eq!(captured["model"], "grok-4.20-non-reasoning");
        assert_eq!(captured["reasoning"]["effort"], "high");
        assert_eq!(captured["reasoning"]["summary"], "auto");

        let captured_headers = headers.lock().unwrap();
        let content_length = captured_headers[0]
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok())
            .expect("rewritten request has content-length");
        assert_eq!(content_length, captured_bodies[0].len());
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn rewrites_messages_model_mapping_to_context_1m_header_variant() {
        let (url, bodies, headers, shutdown_tx) = spawn_body_capture_server("/messages").await;
        let mut providers = HashMap::new();
        providers.insert(
            "provider_a".to_string(),
            Provider {
                base_url: url,
                api_key: "token-a".to_string(),
                authorization_header: None,
                ..Provider::default()
            },
        );
        let mut cfg = test_config("provider_a", providers);
        cfg.listen_base_path = "/v1".to_string();
        cfg.rewrite.model_mappings.push(ModelMapping {
            provider: Some(vec!["provider_a".to_string()]),
            from_model: vec!["claude-sonnet-5".to_string()],
            from_reasoning_effort: None,
            to_model: Some("claude-sonnet-5[1m]".to_string()),
            to_reasoning_effort: None,
        });
        let state = test_proxy_state(cfg);
        let original_body = Bytes::from_static(
            br#"{"model":"claude-sonnet-5","thinking":{"type":"adaptive"},"output_config":{"effort":"xhigh"},"max_tokens":32000,"messages":[]}"#,
        );

        let req = Request::builder()
            .method(Method::POST)
            .uri("/v1/messages?beta=true")
            .header(header::CONTENT_TYPE, "application/json")
            .header(ANTHROPIC_BETA_HEADER, "claude-code-20250219")
            .header(header::CONTENT_LENGTH, original_body.len())
            .body(Body::from(original_body))
            .unwrap();
        let resp = handle_proxy_inner(state, "127.0.0.1:50021".parse().unwrap(), req)
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let captured_bodies = bodies.lock().unwrap();
        assert_eq!(captured_bodies.len(), 1);
        let captured: serde_json::Value = serde_json::from_slice(&captured_bodies[0]).unwrap();
        assert_eq!(captured["model"], "claude-sonnet-5");

        let captured_headers = headers.lock().unwrap();
        let beta = captured_headers[0]
            .get(ANTHROPIC_BETA_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("rewritten request has anthropic-beta");
        assert!(beta.contains("claude-code-20250219"));
        assert!(beta
            .split(',')
            .map(str::trim)
            .any(|token| token.starts_with("context-1m")));
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn malformed_json_rewrite_candidate_is_forwarded_unchanged() {
        let (url, bodies, _headers, shutdown_tx) = spawn_body_capture_server("/responses").await;
        let mut providers = HashMap::new();
        providers.insert(
            "provider_a".to_string(),
            Provider {
                base_url: url,
                api_key: "token-a".to_string(),
                authorization_header: None,
                ..Provider::default()
            },
        );
        let mut cfg = test_config("provider_a", providers);
        cfg.listen_base_path = "/v1".to_string();
        cfg.rewrite.model_mappings.push(ModelMapping {
            provider: Some(vec!["provider_a".to_string()]),
            from_model: vec!["gpt-5.5".to_string()],
            from_reasoning_effort: None,
            to_model: Some("grok-4.20-non-reasoning".to_string()),
            to_reasoning_effort: Some("high".to_string()),
        });
        let state = test_proxy_state(cfg);
        let original_body = Bytes::from_static(br#"{"model":"gpt-5.5""#);

        let req = Request::builder()
            .method(Method::POST)
            .uri("/v1/responses")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(original_body.clone()))
            .unwrap();
        let resp = handle_proxy_inner(state, "127.0.0.1:50022".parse().unwrap(), req)
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let captured_bodies = bodies.lock().unwrap();
        assert_eq!(captured_bodies.as_slice(), &[original_body]);
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn converts_upstream_429_to_503_by_default() {
        let (url, call_count, shutdown_tx) =
            spawn_retry_server(vec![StatusCode::TOO_MANY_REQUESTS]).await;
        let mut providers = HashMap::new();
        providers.insert(
            "provider_a".to_string(),
            Provider {
                base_url: url,
                api_key: "token-a".to_string(),
                authorization_header: None,
                ..Provider::default()
            },
        );
        let state = test_proxy_state(test_config("provider_a", providers));

        let req = Request::builder()
            .method(Method::POST)
            .uri("/")
            .body(Body::from(Bytes::from_static(b"retry-body")))
            .unwrap();

        let resp = handle_proxy_inner(state, "127.0.0.1:50016".parse().unwrap(), req)
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body, Bytes::from_static(b"attempt-0"));
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn preserves_upstream_429_when_conversion_disabled() {
        let (url, call_count, shutdown_tx) =
            spawn_retry_server(vec![StatusCode::TOO_MANY_REQUESTS]).await;
        let mut providers = HashMap::new();
        providers.insert(
            "provider_a".to_string(),
            Provider {
                base_url: url,
                api_key: "token-a".to_string(),
                authorization_header: None,
                ..Provider::default()
            },
        );
        let mut cfg = test_config("provider_a", providers);
        cfg.convert_429_to_503 = false;
        let state = test_proxy_state(cfg);

        let req = Request::builder()
            .method(Method::POST)
            .uri("/")
            .body(Body::from(Bytes::from_static(b"retry-body")))
            .unwrap();

        let resp = handle_proxy_inner(state, "127.0.0.1:50017".parse().unwrap(), req)
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body, Bytes::from_static(b"attempt-0"));
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn preserves_compressed_downstream_response_and_decodes_exchange_reconstruction() {
        let (url, shutdown_tx) = spawn_compressed_response_server().await;
        let mut providers = HashMap::new();
        providers.insert(
            "provider_a".to_string(),
            Provider {
                base_url: url,
                api_key: "token-a".to_string(),
                authorization_header: None,
                ..Provider::default()
            },
        );
        let mut cfg = test_config("provider_a", providers);
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos();
        let log_dir = std::env::temp_dir().join(format!(
            "codex-provider-proxy-compressed-response-{}-{unique}",
            std::process::id()
        ));
        cfg.logging.exchange_log_dir = Some(log_dir.clone());
        cfg.logging.exchange_body_compression = BodyLogCompression::Zstd;
        cfg.logging.reconstruct_responses = true;
        let state = test_proxy_state(cfg);

        let req = Request::builder()
            .method(Method::POST)
            .uri("/messages")
            .body(Body::empty())
            .unwrap();
        let resp = handle_proxy_inner(state, "127.0.0.1:50018".parse().unwrap(), req)
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            resp.headers().get(header::CONTENT_ENCODING).unwrap(),
            "zstd"
        );
        let downstream_body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let decoded_downstream = zstd::stream::decode_all(downstream_body.as_ref()).unwrap();
        assert_eq!(
            decoded_downstream,
            br#"{"error":{"message":"decoded upstream response"},"type":"error"}"#
        );

        let reconstructed_path = (0..50)
            .find_map(|_| {
                let path = std::fs::read_dir(&log_dir)
                    .ok()?
                    .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                    .find(|path| {
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(|name| name.ends_with(".response_reconstructed.txt"))
                    });
                if path.as_ref().is_some_and(|path| path.exists()) {
                    path
                } else {
                    std::thread::sleep(Duration::from_millis(10));
                    None
                }
            })
            .expect("reconstructed exchange response exists");
        assert_eq!(
            std::fs::read(&reconstructed_path).unwrap(),
            br#"{"error":{"message":"decoded upstream response"},"type":"error"}"#
        );

        let _ = shutdown_tx.send(());
        let _ = std::fs::remove_dir_all(log_dir);
    }

    #[test]
    fn maps_downstream_status_only_for_429_when_enabled() {
        let cfg = test_config(
            "provider_a",
            HashMap::from([(
                "provider_a".to_string(),
                Provider {
                    base_url: Url::parse("http://127.0.0.1:1/").unwrap(),
                    api_key: "token-a".to_string(),
                    authorization_header: None,
                    ..Provider::default()
                },
            )]),
        );

        assert_eq!(
            downstream_response_status(&cfg, StatusCode::TOO_MANY_REQUESTS),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            downstream_response_status(&cfg, StatusCode::BAD_GATEWAY),
            StatusCode::BAD_GATEWAY
        );
    }

    async fn build_retry_args(
        state: ProxyState,
        request_id: u64,
        peer: SocketAddr,
        pid: Option<u32>,
        transparent_retry_count: u32,
        transparent_retry_backoff_step: Duration,
    ) -> RetrySendArgs {
        let routing = state.runtime.routing_snapshot().await;
        let initial_attempt = resolve_upstream_attempt(ResolveUpstreamAttemptArgs {
            state: &state,
            cfg: &routing.config,
            default_provider: &routing.default_provider,
            pid,
            peer,
            request_id,
            forwarded_path: "/",
            incoming_query: None,
            base_headers: &HeaderMap::new(),
        })
        .await
        .unwrap();
        RetrySendArgs {
            state,
            request_id,
            peer,
            pid,
            transparent_retry_count,
            transparent_retry_backoff_step,
            idle_timeout: None,
            exchange_logger: None,
            statistics: None,
            request: RetryRequestTemplate {
                method: Method::POST,
                forwarded_path: "/".to_string(),
                incoming_query: None,
                base_headers: HeaderMap::new(),
                request_body: Bytes::from_static(b"retry-body"),
            },
            initial_attempt,
            initial_config: routing.config.clone(),
            initial_http_client: routing.http_client.clone(),
        }
    }

    #[tokio::test]
    async fn retries_non_2xx_status_until_success() {
        let (url, call_count, shutdown_tx) =
            spawn_retry_server(vec![StatusCode::INTERNAL_SERVER_ERROR, StatusCode::OK]).await;
        let mut providers = HashMap::new();
        providers.insert(
            "provider_a".to_string(),
            Provider {
                base_url: url,
                api_key: "token-a".to_string(),
                authorization_header: None,
                ..Provider::default()
            },
        );
        let state = test_proxy_state(test_config("provider_a", providers));
        let peer: SocketAddr = "127.0.0.1:50000".parse().unwrap();
        let args = build_retry_args(state, 1, peer, None, 2, Duration::ZERO).await;

        let (resp, _attempt_info, final_attempt, _final_attempt_latency_ms) =
            send_with_non_2xx_retries(args).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(final_attempt, 2);
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn does_not_retry_head_requests_by_default() {
        let (url, call_count, shutdown_tx) =
            spawn_retry_server(vec![StatusCode::INTERNAL_SERVER_ERROR, StatusCode::OK]).await;
        let mut providers = HashMap::new();
        providers.insert(
            "provider_a".to_string(),
            Provider {
                base_url: url,
                api_key: "token-a".to_string(),
                authorization_header: None,
                ..Provider::default()
            },
        );
        let mut cfg = test_config("provider_a", providers);
        cfg.transparent_retry_count = 1;
        let state = test_proxy_state(cfg);

        let req = Request::builder()
            .method(Method::HEAD)
            .uri("/")
            .body(Body::empty())
            .unwrap();
        let resp = handle_proxy_inner(state, "127.0.0.1:50019".parse().unwrap(), req)
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn retries_head_requests_when_enabled() {
        let (url, call_count, shutdown_tx) =
            spawn_retry_server(vec![StatusCode::INTERNAL_SERVER_ERROR, StatusCode::OK]).await;
        let mut providers = HashMap::new();
        providers.insert(
            "provider_a".to_string(),
            Provider {
                base_url: url,
                api_key: "token-a".to_string(),
                authorization_header: None,
                ..Provider::default()
            },
        );
        let mut cfg = test_config("provider_a", providers);
        cfg.transparent_retry_count = 1;
        cfg.transparent_retry_head_requests = true;
        let state = test_proxy_state(cfg);

        let req = Request::builder()
            .method(Method::HEAD)
            .uri("/")
            .body(Body::empty())
            .unwrap();
        let resp = handle_proxy_inner(state, "127.0.0.1:50020".parse().unwrap(), req)
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn returns_last_non_2xx_when_retry_limit_exhausted() {
        let (url, call_count, shutdown_tx) = spawn_retry_server(vec![
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BAD_GATEWAY,
            StatusCode::OK,
        ])
        .await;
        let mut providers = HashMap::new();
        providers.insert(
            "provider_a".to_string(),
            Provider {
                base_url: url,
                api_key: "token-a".to_string(),
                authorization_header: None,
                ..Provider::default()
            },
        );
        let state = test_proxy_state(test_config("provider_a", providers));
        let peer: SocketAddr = "127.0.0.1:50001".parse().unwrap();
        let args = build_retry_args(state, 2, peer, None, 1, Duration::ZERO).await;

        let (resp, _attempt_info, final_attempt, _final_attempt_latency_ms) =
            send_with_non_2xx_retries(args).await.unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(final_attempt, 2);
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn retries_send_error_before_downstream_response_until_success() {
        let (success_url, success_call_count, shutdown_tx) =
            spawn_retry_server(vec![StatusCode::OK]).await;
        let mut providers = HashMap::new();
        providers.insert(
            "provider_a".to_string(),
            Provider {
                base_url: unused_loopback_url(),
                api_key: "token-a".to_string(),
                authorization_header: None,
                ..Provider::default()
            },
        );
        providers.insert(
            "provider_b".to_string(),
            Provider {
                base_url: success_url,
                api_key: "token-b".to_string(),
                authorization_header: None,
                ..Provider::default()
            },
        );
        let state = test_proxy_state(test_config("provider_a", providers));
        let sleep_calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let peer: SocketAddr = "127.0.0.1:50014".parse().unwrap();
        let args =
            build_retry_args(state.clone(), 5, peer, None, 1, Duration::from_millis(25)).await;

        let (resp, final_attempt_info, final_attempt, _final_attempt_latency_ms) =
            send_with_non_2xx_retries_with_sleep(args, {
                let state = state.clone();
                let sleep_calls = sleep_calls.clone();
                move |duration| {
                    let state = state.clone();
                    let sleep_calls = sleep_calls.clone();
                    async move {
                        sleep_calls.lock().unwrap().push(duration);
                        state
                            .runtime
                            .set_default_provider("provider_b".to_string())
                            .await;
                    }
                }
            })
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(final_attempt_info.provider_name, "provider_b");
        assert_eq!(final_attempt, 2);
        assert_eq!(success_call_count.load(Ordering::SeqCst), 1);
        assert_eq!(
            sleep_calls.lock().unwrap().as_slice(),
            &[Duration::from_millis(25)]
        );
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn returns_send_error_when_retry_limit_exhausted() {
        let mut providers = HashMap::new();
        providers.insert(
            "provider_a".to_string(),
            Provider {
                base_url: unused_loopback_url(),
                api_key: "token-a".to_string(),
                authorization_header: None,
                ..Provider::default()
            },
        );
        let state = test_proxy_state(test_config("provider_a", providers));
        let sleep_calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let peer: SocketAddr = "127.0.0.1:50015".parse().unwrap();
        let args = build_retry_args(state, 6, peer, None, 1, Duration::from_millis(25)).await;

        let result = send_with_non_2xx_retries_with_sleep(args, {
            let sleep_calls = sleep_calls.clone();
            move |duration| {
                let sleep_calls = sleep_calls.clone();
                async move {
                    sleep_calls.lock().unwrap().push(duration);
                }
            }
        })
        .await;
        let err = match result {
            Ok(_) => panic!("expected upstream send error"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        assert_eq!(
            sleep_calls.lock().unwrap().as_slice(),
            &[Duration::from_millis(25)]
        );
    }

    #[tokio::test]
    async fn exchange_logs_include_request_and_response_files_for_each_retry_attempt() {
        let (url, call_count, shutdown_tx) =
            spawn_retry_server(vec![StatusCode::INTERNAL_SERVER_ERROR, StatusCode::OK]).await;
        let mut providers = HashMap::new();
        providers.insert(
            "provider_a".to_string(),
            Provider {
                base_url: url,
                api_key: "token-a".to_string(),
                authorization_header: None,
                ..Provider::default()
            },
        );
        let mut cfg = test_config("provider_a", providers);
        cfg.transparent_retry_count = 1;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos();
        let log_dir = std::env::temp_dir().join(format!(
            "codex-provider-proxy-retry-log-{}-{}",
            std::process::id(),
            unique
        ));
        cfg.logging.exchange_log_dir = Some(log_dir.clone());
        let state = test_proxy_state(cfg);

        let req = Request::builder()
            .method(Method::POST)
            .uri("/")
            .body(Body::from(Bytes::from_static(b"retry-body")))
            .unwrap();

        let resp = handle_proxy_inner(state, "127.0.0.1:50013".parse().unwrap(), req)
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body, Bytes::from_static(b"attempt-1"));
        assert_eq!(call_count.load(Ordering::SeqCst), 2);

        let meta_path = std::fs::read_dir(&log_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(".meta.json"))
            })
            .expect("meta file exists");
        let mut matched = false;
        for _ in 0..50 {
            let meta: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&meta_path).unwrap()).unwrap();
            let attempts = meta
                .get("attempts")
                .and_then(serde_json::Value::as_array)
                .unwrap();
            if attempts.len() != 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }

            let mut all_match = true;
            for (index, expected_response) in ["attempt-0", "attempt-1"].iter().enumerate() {
                let attempt = &attempts[index];
                let request_body = attempt
                    .get("request_body")
                    .and_then(serde_json::Value::as_str)
                    .unwrap();
                let response_body = attempt
                    .get("response_body")
                    .and_then(serde_json::Value::as_str)
                    .unwrap();
                all_match &= std::fs::read(request_body).is_ok_and(|body| body == b"retry-body");
                all_match &= std::fs::read(response_body)
                    .is_ok_and(|body| body == expected_response.as_bytes());
            }

            if all_match {
                matched = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            matched,
            "attempt request and response body files should match"
        );

        let _ = shutdown_tx.send(());
        let _ = std::fs::remove_dir_all(log_dir);
    }

    #[tokio::test]
    async fn records_linear_backoff_between_retries() {
        let (url, call_count, shutdown_tx) = spawn_retry_server(vec![
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BAD_GATEWAY,
            StatusCode::OK,
        ])
        .await;
        let sleep_calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut providers = HashMap::new();
        providers.insert(
            "provider_a".to_string(),
            Provider {
                base_url: url,
                api_key: "token-a".to_string(),
                authorization_header: None,
                ..Provider::default()
            },
        );
        let state = test_proxy_state(test_config("provider_a", providers));
        let peer: SocketAddr = "127.0.0.1:50002".parse().unwrap();
        let args = build_retry_args(state, 3, peer, None, 2, Duration::from_millis(50)).await;

        let (resp, _attempt_info, final_attempt, _final_attempt_latency_ms) =
            send_with_non_2xx_retries_with_sleep(args, {
                let sleep_calls = sleep_calls.clone();
                move |duration| {
                    let sleep_calls = sleep_calls.clone();
                    async move {
                        sleep_calls.lock().unwrap().push(duration);
                    }
                }
            })
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(final_attempt, 3);
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
        assert_eq!(
            sleep_calls.lock().unwrap().as_slice(),
            &[Duration::from_millis(50), Duration::from_millis(100)]
        );
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn reroutes_retry_attempts_when_provider_mapping_changes() {
        let (url_a, call_count_a, auth_headers_a, shutdown_tx_a) =
            spawn_auth_capture_server(StatusCode::INTERNAL_SERVER_ERROR, "provider-a").await;
        let (url_b, call_count_b, auth_headers_b, shutdown_tx_b) =
            spawn_auth_capture_server(StatusCode::OK, "provider-b").await;

        let mut providers = HashMap::new();
        providers.insert(
            "provider_a".to_string(),
            Provider {
                base_url: url_a,
                api_key: "token-a".to_string(),
                authorization_header: None,
                ..Provider::default()
            },
        );
        providers.insert(
            "provider_b".to_string(),
            Provider {
                base_url: url_b,
                api_key: "token-b".to_string(),
                authorization_header: None,
                ..Provider::default()
            },
        );

        let state = test_proxy_state(test_config("provider_a", providers));
        let pid = 4242u32;
        state
            .runtime
            .pid_routes()
            .insert(pid, "provider_a".to_string());

        let peer: SocketAddr = "127.0.0.1:50003".parse().unwrap();
        let args = build_retry_args(
            state.clone(),
            4,
            peer,
            Some(pid),
            1,
            Duration::from_millis(1),
        )
        .await;

        let (resp, final_attempt_info, final_attempt, _final_attempt_latency_ms) =
            send_with_non_2xx_retries_with_sleep(args, {
                let state = state.clone();
                move |_| {
                    let state = state.clone();
                    async move {
                        state
                            .runtime
                            .pid_routes()
                            .insert(pid, "provider_b".to_string());
                    }
                }
            })
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(final_attempt, 2);
        assert_eq!(final_attempt_info.provider_name, "provider_b");
        assert_eq!(call_count_a.load(Ordering::SeqCst), 1);
        assert_eq!(call_count_b.load(Ordering::SeqCst), 1);
        assert_eq!(
            auth_headers_a.lock().unwrap().as_slice(),
            &["Bearer token-a".to_string()]
        );
        assert_eq!(
            auth_headers_b.lock().unwrap().as_slice(),
            &["Bearer token-b".to_string()]
        );

        let _ = shutdown_tx_a.send(());
        let _ = shutdown_tx_b.send(());
    }

    #[test]
    fn identifies_overload_response_failed_events() {
        let slow_down = concat!(
            "event: response.failed\n",
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"slow_down\"}}}\n\n",
        );
        let overloaded =
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_is_overloaded\"}}}\n\n";
        let top_level_error = concat!(
            "event: error\n",
            "data: {\"type\":\"error\",\"error\":{\"type\":\"service_unavailable_error\",\"code\":\"server_is_overloaded\"}}\n\n",
        );
        let unrelated = concat!(
            "event: response.failed\n",
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"rate_limit_exceeded\"}}}\n\n",
        );
        let unrelated_error = concat!(
            "event: error\n",
            "data: {\"type\":\"error\",\"error\":{\"code\":\"rate_limit_exceeded\"}}\n\n",
        );

        assert_eq!(
            responses_slow_down_error_code(slow_down.as_bytes()),
            Some("slow_down")
        );
        assert_eq!(
            responses_slow_down_error_code(overloaded.as_bytes()),
            Some("server_is_overloaded")
        );
        assert_eq!(
            responses_slow_down_error_code(top_level_error.as_bytes()),
            Some("server_is_overloaded")
        );
        assert_eq!(responses_slow_down_error_code(unrelated.as_bytes()), None);
        assert_eq!(
            responses_slow_down_error_code(unrelated_error.as_bytes()),
            None
        );
    }

    #[test]
    fn only_filters_responses_sse_when_enabled() {
        let mut providers = HashMap::new();
        providers.insert(
            "provider_a".to_string(),
            Provider {
                base_url: Url::parse("http://127.0.0.1:1/").unwrap(),
                api_key: "token-a".to_string(),
                authorization_header: None,
                ..Provider::default()
            },
        );
        let mut cfg = test_config("provider_a", providers);
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "text/event-stream; charset=utf-8".parse().unwrap(),
        );

        assert!(is_text_event_stream(&headers));
        assert!(should_drop_responses_slow_down_errors(
            &cfg,
            "/v1/responses",
            &headers
        ));
        assert!(!should_drop_responses_slow_down_errors(
            &cfg,
            "/v1/messages",
            &headers
        ));

        cfg.drop_responses_slow_down_errors = false;
        assert!(!should_drop_responses_slow_down_errors(
            &cfg,
            "/v1/responses",
            &headers
        ));
    }

    #[tokio::test]
    async fn suppresses_slow_down_event_and_aborts_stream() {
        let payload_1 = concat!(
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n",
        );
        let payload_2 = concat!(
            "event: response.failed\n",
            "data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_1\",\"error\":{\"code\":\"slow_down\"}}}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\"}\n\n",
        );
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "text/event-stream".parse().unwrap());

        let stream = Box::pin(stream::iter(vec![
            Ok(Bytes::from_static(&payload_1.as_bytes()[..20])),
            Ok(Bytes::from_static(&payload_1.as_bytes()[20..])),
            Ok(Bytes::from_static(&payload_2.as_bytes()[..35])),
            Ok(Bytes::from_static(&payload_2.as_bytes()[35..])),
        ]));
        let mut stream = maybe_filter_responses_slow_down_stream(
            &test_config(
                "provider_a",
                HashMap::from([(
                    "provider_a".to_string(),
                    Provider {
                        base_url: Url::parse("http://127.0.0.1:1/").unwrap(),
                        api_key: "token-a".to_string(),
                        authorization_header: None,
                        ..Provider::default()
                    },
                )]),
            ),
            99,
            "/v1/responses",
            &headers,
            Some(4242),
            "provider_a",
            stream,
        );

        let mut delivered = BytesMut::new();
        while let Some(item) = stream.next().await {
            match item {
                Ok(chunk) => delivered.extend_from_slice(&chunk),
                Err(err) => {
                    assert_eq!(err.kind(), std::io::ErrorKind::ConnectionAborted);
                    assert!(err.to_string().contains("slow_down"));
                    break;
                }
            }
        }

        let delivered = String::from_utf8(delivered.to_vec()).unwrap();
        assert_eq!(delivered, payload_1);
        assert!(!delivered.contains("response.failed"));
        assert!(!delivered.contains("response.completed"));
    }

    #[tokio::test]
    async fn suppresses_top_level_error_event_and_aborts_stream() {
        let payload_1 = concat!(
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n",
        );
        let payload_2 = concat!(
            "event: error\n",
            "data: {\"type\":\"error\",\"error\":{\"type\":\"service_unavailable_error\",\"code\":\"server_is_overloaded\"}}\n\n",
            "event: response.failed\n",
            "data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_1\",\"error\":{\"code\":\"server_is_overloaded\"}}}\n\n",
        );
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "text/event-stream".parse().unwrap());

        let stream = Box::pin(stream::iter(vec![
            Ok(Bytes::from_static(&payload_1.as_bytes()[..20])),
            Ok(Bytes::from_static(&payload_1.as_bytes()[20..])),
            Ok(Bytes::from_static(&payload_2.as_bytes()[..35])),
            Ok(Bytes::from_static(&payload_2.as_bytes()[35..])),
        ]));
        let mut stream = maybe_filter_responses_slow_down_stream(
            &test_config(
                "provider_a",
                HashMap::from([(
                    "provider_a".to_string(),
                    Provider {
                        base_url: Url::parse("http://127.0.0.1:1/").unwrap(),
                        api_key: "token-a".to_string(),
                        authorization_header: None,
                        ..Provider::default()
                    },
                )]),
            ),
            101,
            "/v1/responses",
            &headers,
            Some(4242),
            "provider_a",
            stream,
        );

        let mut delivered = BytesMut::new();
        while let Some(item) = stream.next().await {
            match item {
                Ok(chunk) => delivered.extend_from_slice(&chunk),
                Err(err) => {
                    assert_eq!(err.kind(), std::io::ErrorKind::ConnectionAborted);
                    assert!(err.to_string().contains("server_is_overloaded"));
                    break;
                }
            }
        }

        let delivered = String::from_utf8(delivered.to_vec()).unwrap();
        assert_eq!(delivered, payload_1);
        assert!(!delivered.contains("event: error"));
        assert!(!delivered.contains("response.failed"));
    }

    #[tokio::test]
    async fn passes_through_non_matching_responses_sse() {
        let payload = concat!(
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\"}}\n\n",
        );
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "text/event-stream".parse().unwrap());

        let stream = Box::pin(stream::iter(vec![
            Ok(Bytes::from_static(&payload.as_bytes()[..30])),
            Ok(Bytes::from_static(&payload.as_bytes()[30..])),
        ]));
        let mut stream = maybe_filter_responses_slow_down_stream(
            &test_config(
                "provider_a",
                HashMap::from([(
                    "provider_a".to_string(),
                    Provider {
                        base_url: Url::parse("http://127.0.0.1:1/").unwrap(),
                        api_key: "token-a".to_string(),
                        authorization_header: None,
                        ..Provider::default()
                    },
                )]),
            ),
            100,
            "/v1/responses",
            &headers,
            None,
            "provider_a",
            stream,
        );

        let mut delivered = BytesMut::new();
        while let Some(item) = stream.next().await {
            delivered.extend_from_slice(&item.unwrap());
        }

        assert_eq!(String::from_utf8(delivered.to_vec()).unwrap(), payload);
    }

    // --- API conversion integration ---

    #[derive(Clone)]
    struct ScriptedServerState {
        bodies: Arc<Mutex<Vec<Bytes>>>,
        headers: Arc<Mutex<Vec<HeaderMap>>>,
        response: Arc<Mutex<Option<axum::http::Response<Body>>>>,
    }

    async fn scripted_server_handler(
        State(state): State<ScriptedServerState>,
        headers: HeaderMap,
        body: Bytes,
    ) -> axum::http::Response<Body> {
        state.bodies.lock().unwrap().push(body);
        state.headers.lock().unwrap().push(headers);
        let response = state.response.lock().unwrap().take().unwrap_or_else(|| {
            axum::http::Response::builder()
                .status(StatusCode::OK)
                .body(Body::from("ok"))
                .unwrap()
        });
        response
    }

    async fn spawn_scripted_server(
        path: &'static str,
    ) -> (
        Url,
        Arc<Mutex<Vec<Bytes>>>,
        Arc<Mutex<Vec<HeaderMap>>>,
        Arc<Mutex<Option<axum::http::Response<Body>>>>,
        oneshot::Sender<()>,
    ) {
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let headers = Arc::new(Mutex::new(Vec::new()));
        let response = Arc::new(Mutex::new(None));
        let state = ScriptedServerState {
            bodies: bodies.clone(),
            headers: headers.clone(),
            response: response.clone(),
        };
        let app = Router::new()
            .route(path, any(scripted_server_handler))
            .with_state(state);
        let (url, shutdown_tx) = spawn_test_server(app).await;
        (url, bodies, headers, response, shutdown_tx)
    }

    fn converting_provider(base_url: Url) -> Provider {
        Provider {
            base_url,
            api_key: "token-a".to_string(),
            authorization_header: None,
            upstream_api:
                codex_provider_proxy_api_conversion::dialect::UpstreamApi::OpenAiChatCompletions,
            accept_downstream_apis: vec![
                codex_provider_proxy_api_conversion::dialect::DownstreamApi::AnthropicMessages,
            ],
            ..Provider::default()
        }
    }

    fn messages_request(body: serde_json::Value) -> Request<Body> {
        let body = serde_json::to_vec(&body).unwrap();
        Request::builder()
            .method(Method::POST)
            .uri("/v1/messages?beta=true")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_LENGTH, body.len())
            .body(Body::from(body))
            .unwrap()
    }

    #[test]
    fn convert_non_streaming_body_parses_chat_json() {
        let body = Bytes::from_static(b"{\"id\":\"t\",\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\",\"content\":\"x\"},\"finish_reason\":\"stop\"}]}");
        let out = crate::api_conversion::convert_non_streaming_body(body, "/v1/messages").unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["type"], "message");
    }

    #[tokio::test]
    async fn non_streaming_conversion_stream_adapts_body() {
        let inner = futures_util::stream::iter(vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"{\"id\":\"t\",\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\",\"content\":\"x\"},\"finish_reason\":\"stop\"}]}")),
        ]);
        let mut stream = crate::api_conversion::NonStreamingConversionStream::with_recorder(
            inner,
            1024 * 1024,
            "/v1/messages".to_string(),
            None,
        );
        let mut delivered = BytesMut::new();
        while let Some(item) = stream.next().await {
            delivered.extend_from_slice(&item.unwrap());
        }
        let out: serde_json::Value = serde_json::from_slice(&delivered).unwrap();
        assert_eq!(out["type"], "message");
        assert_eq!(out["content"][0]["text"], "x");
    }

    #[tokio::test]
    async fn converts_messages_request_to_chat_completions_end_to_end() {
        let (url, bodies, headers, response, shutdown_tx) =
            spawn_scripted_server("/chat/completions").await;
        *response.lock().unwrap() = Some(
            axum::http::Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"id": "r1", "object": "chat.completion", "model": "deepseek-v4-pro", "choices": [{"index": 0, "message": {"role": "assistant", "content": "hi"}, "finish_reason": "stop"}], "usage": {"prompt_tokens": 1, "completion_tokens": 1}})
                        .to_string(),
                ))
                .unwrap(),
        );
        let mut providers = HashMap::new();
        providers.insert("provider_a".to_string(), converting_provider(url));
        let mut cfg = test_config("provider_a", providers);
        cfg.listen_base_path = "/v1".to_string();
        let state = test_proxy_state(cfg);

        let req = messages_request(json!({
            "model": "deepseek-v4-pro",
            "max_tokens": 100,
            "stream": false,
            "thinking": {"type": "adaptive"},
            "system": "You are helpful",
            "messages": [{"role": "user", "content": "hi"}]
        }));
        let resp = handle_proxy_inner(state, "127.0.0.1:50021".parse().unwrap(), req)
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        to_bytes(resp.into_body(), usize::MAX).await.unwrap();

        let captured_bodies = bodies.lock().unwrap();
        assert_eq!(captured_bodies.len(), 1);
        let captured: serde_json::Value = serde_json::from_slice(&captured_bodies[0]).unwrap();
        assert_eq!(captured["model"], "deepseek-v4-pro");
        assert_eq!(captured["max_tokens"], 100);
        assert_eq!(captured["messages"][0]["role"], "system");
        assert_eq!(captured["messages"][0]["content"], "You are helpful");
        assert_eq!(captured["messages"][1]["role"], "user");
        assert_eq!(captured["messages"][1]["content"], "hi");
        assert_eq!(captured["thinking"]["type"], "enabled");

        let captured_headers = headers.lock().unwrap();
        assert!(captured_headers[0].get("anthropic-beta").is_none());
        let _ = shutdown_tx.send(());
    }

    fn responses_provider(base_url: Url) -> Provider {
        Provider {
            accept_downstream_apis: vec![
                codex_provider_proxy_api_conversion::dialect::DownstreamApi::AnthropicMessages,
                codex_provider_proxy_api_conversion::dialect::DownstreamApi::OpenAiResponses,
            ],
            ..converting_provider(base_url)
        }
    }

    fn responses_request(body: serde_json::Value) -> Request<Body> {
        let body = serde_json::to_vec(&body).unwrap();
        Request::builder()
            .method(Method::POST)
            .uri("/v1/responses")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_LENGTH, body.len())
            .body(Body::from(body))
            .unwrap()
    }

    #[tokio::test]
    async fn responses_continuation_prepends_previous_transcript() {
        let (url, bodies, _headers, response, shutdown_tx) =
            spawn_scripted_server("/chat/completions").await;
        *response.lock().unwrap() = Some(
            axum::http::Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"id": "chatcmpl-1", "object": "chat.completion", "model": "m", "choices": [{"index": 0, "message": {"role": "assistant", "content": "hello"}, "finish_reason": "stop"}], "usage": {"prompt_tokens": 1, "completion_tokens": 1}})
                        .to_string(),
                ))
                .unwrap(),
        );
        let mut providers = HashMap::new();
        providers.insert("provider_a".to_string(), responses_provider(url));
        let mut cfg = test_config("provider_a", providers);
        cfg.listen_base_path = "/v1".to_string();
        let state = test_proxy_state(cfg);

        // First request starts the conversation; the response id is stored.
        let req = responses_request(json!({
            "model": "m",
            "input": "hi",
            "stream": false
        }));
        let resp = handle_proxy_inner(state.clone(), "127.0.0.1:50021".parse().unwrap(), req)
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let out = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let converted: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(converted["id"], "resp_chatcmpl-1");

        // Second request continues with previous_response_id; the stored assistant
        // turn is prepended to the messages the upstream sees.
        *response.lock().unwrap() = Some(
            axum::http::Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"id": "chatcmpl-2", "object": "chat.completion", "model": "m", "choices": [{"index": 0, "message": {"role": "assistant", "content": "hi there"}, "finish_reason": "stop"}], "usage": {"prompt_tokens": 1, "completion_tokens": 1}})
                        .to_string(),
                ))
                .unwrap(),
        );
        let req = responses_request(json!({
            "model": "m",
            "previous_response_id": "resp_chatcmpl-1",
            "input": "how are you?",
            "stream": false
        }));
        let resp = handle_proxy_inner(state, "127.0.0.1:50021".parse().unwrap(), req)
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        to_bytes(resp.into_body(), usize::MAX).await.unwrap();

        let captured = bodies.lock().unwrap();
        assert_eq!(captured.len(), 2);
        let second: serde_json::Value = serde_json::from_slice(&captured[1]).unwrap();
        let messages = second["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0]["role"], "user");
        assert_eq!(messages[0]["content"], "hi");
        assert_eq!(messages[1]["role"], "assistant");
        assert_eq!(messages[1]["content"], "hello");
        assert_eq!(messages[2]["role"], "user");
        assert_eq!(messages[2]["content"], "how are you?");
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn responses_continuation_rejects_unknown_previous_id() {
        let (url, _bodies, _headers, response, shutdown_tx) =
            spawn_scripted_server("/chat/completions").await;
        *response.lock().unwrap() = Some(
            axum::http::Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"id": "chatcmpl-1", "choices": [{"index": 0, "message": {"role": "assistant", "content": "x"}, "finish_reason": "stop"}]})
                        .to_string(),
                ))
                .unwrap(),
        );
        let mut providers = HashMap::new();
        providers.insert("provider_a".to_string(), responses_provider(url));
        let mut cfg = test_config("provider_a", providers);
        cfg.listen_base_path = "/v1".to_string();
        let state = test_proxy_state(cfg);

        let req = responses_request(json!({
            "model": "m",
            "previous_response_id": "resp_missing",
            "input": "hi",
            "stream": false
        }));
        let err = handle_proxy_inner(state, "127.0.0.1:50021".parse().unwrap(), req)
            .await
            .expect_err("unknown previous_response_id should fail");
        let chain: Vec<String> = err.chain().map(|cause| cause.to_string()).collect();
        assert!(
            chain
                .iter()
                .any(|msg| msg.contains("unknown or expired previous_response_id")),
            "unexpected error chain: {chain:?}"
        );
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn passthrough_provider_keeps_messages_path_upstream() {
        let (url, bodies, _headers, shutdown_tx) = spawn_body_capture_server("/messages").await;
        let mut providers = HashMap::new();
        providers.insert(
            "provider_a".to_string(),
            Provider {
                upstream_api:
                    codex_provider_proxy_api_conversion::dialect::UpstreamApi::Passthrough,
                ..converting_provider(url)
            },
        );
        let mut cfg = test_config("provider_a", providers);
        cfg.listen_base_path = "/v1".to_string();
        let state = test_proxy_state(cfg);

        let req = messages_request(
            json!({"model": "m", "max_tokens": 1, "messages": [{"role": "user", "content": "hi"}]}),
        );
        let resp = handle_proxy_inner(state, "127.0.0.1:50021".parse().unwrap(), req)
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        to_bytes(resp.into_body(), usize::MAX).await.unwrap();

        assert_eq!(bodies.lock().unwrap().len(), 1);
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn converts_non_streaming_chat_response_to_messages() {
        let (url, _bodies, _headers, response, shutdown_tx) =
            spawn_scripted_server("/chat/completions").await;
        *response.lock().unwrap() = Some(
            axum::http::Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "id": "abc123",
                        "object": "chat.completion",
                        "model": "deepseek-v4-pro",
                        "choices": [{
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": "Hello!",
                                "reasoning_content": "thinking here"
                            },
                            "finish_reason": "stop"
                        }],
                        "usage": {"prompt_tokens": 10, "completion_tokens": 5, "prompt_tokens_details": {"cached_tokens": 2}, "completion_tokens_details": {"reasoning_tokens": 3}}
                    })
                    .to_string(),
                ))
                .unwrap(),
        );
        let mut providers = HashMap::new();
        providers.insert("provider_a".to_string(), converting_provider(url));
        let mut cfg = test_config("provider_a", providers);
        cfg.listen_base_path = "/v1".to_string();
        let state = test_proxy_state(cfg);

        let req = messages_request(
            json!({"model": "m", "max_tokens": 1, "messages": [{"role": "user", "content": "hi"}]}),
        );
        let resp = handle_proxy_inner(state, "127.0.0.1:50021".parse().unwrap(), req)
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.headers().get(header::CONTENT_LENGTH).is_none());
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let out: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(out["id"], "msg_abc123");
        assert_eq!(out["type"], "message");
        assert_eq!(out["stop_reason"], "end_turn");
        assert_eq!(out["content"][0]["type"], "thinking");
        assert_eq!(out["content"][0]["thinking"], "thinking here");
        assert_eq!(out["content"][1]["type"], "text");
        assert_eq!(out["content"][1]["text"], "Hello!");
        assert_eq!(out["usage"]["input_tokens"], 10);
        assert_eq!(out["usage"]["output_tokens"], 5);
        assert_eq!(out["usage"]["cache_read_input_tokens"], 2);
        assert_eq!(out["usage"]["output_tokens_details"]["thinking_tokens"], 3);
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn converts_streaming_chat_response_to_messages_sse() {
        let sse_body = concat!(
            "data: {\"id\":\"x\",\"object\":\"chat.completion.chunk\",\"model\":\"deepseek-v4-pro\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":null,\"reasoning_content\":\"\"},\"finish_reason\":null}],\"usage\":null}\n\n",
            "data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":null,\"reasoning_content\":\"We\"},\"finish_reason\":null}],\"usage\":null}\n\n",
            "data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\",\"reasoning_content\":null},\"finish_reason\":null}],\"usage\":null}\n\n",
            "data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":null}\n\n",
            "data: {\"id\":\"x\",\"choices\":[],\"usage\":{\"prompt_tokens\":6,\"completion_tokens\":9}}\n\n",
            "data: [DONE]\n\n",
        );
        let (url, _bodies, _headers, response, shutdown_tx) =
            spawn_scripted_server("/chat/completions").await;
        *response.lock().unwrap() = Some(
            axum::http::Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/event-stream")
                .body(Body::from(sse_body))
                .unwrap(),
        );
        let mut providers = HashMap::new();
        providers.insert("provider_a".to_string(), converting_provider(url));
        let mut cfg = test_config("provider_a", providers);
        cfg.listen_base_path = "/v1".to_string();
        let state = test_proxy_state(cfg);

        let req = messages_request(
            json!({"model": "m", "max_tokens": 1, "stream": true, "messages": [{"role": "user", "content": "hi"}]}),
        );
        let resp = handle_proxy_inner(state, "127.0.0.1:50021".parse().unwrap(), req)
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("event: message_start"));
        assert!(text.contains("\"id\":\"msg_x\""));
        assert!(text.contains("\"type\":\"thinking_delta\""));
        assert!(text.contains("\"thinking\":\"We\""));
        assert!(text.contains("\"type\":\"text_delta\""));
        assert!(text.contains("\"text\":\"Hi\""));
        assert!(text.contains("\"stop_reason\":\"end_turn\""));
        assert!(text.contains("event: message_stop"));
        assert_eq!(text.matches("event: message_stop").count(), 1);
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn converts_upstream_error_to_anthropic_error_envelope() {
        let (url, _bodies, _headers, response, shutdown_tx) =
            spawn_scripted_server("/chat/completions").await;
        *response.lock().unwrap() = Some(
            axum::http::Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"error": {"message": "unknown tool type", "type": "invalid_request_error", "param": "tools[0].type"}})
                        .to_string(),
                ))
                .unwrap(),
        );
        let mut providers = HashMap::new();
        providers.insert("provider_a".to_string(), converting_provider(url));
        let mut cfg = test_config("provider_a", providers);
        cfg.listen_base_path = "/v1".to_string();
        let state = test_proxy_state(cfg);

        let req = messages_request(
            json!({"model": "m", "max_tokens": 1, "messages": [{"role": "user", "content": "hi"}]}),
        );
        let resp = handle_proxy_inner(state, "127.0.0.1:50021".parse().unwrap(), req)
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let out: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(out["type"], "error");
        assert_eq!(out["error"]["type"], "invalid_request_error");
        assert_eq!(out["error"]["message"], "unknown tool type");
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn rejects_unconvertible_request_with_anthropic_400() {
        let (url, _bodies, _headers, _response, shutdown_tx) =
            spawn_scripted_server("/chat/completions").await;
        let mut providers = HashMap::new();
        providers.insert("provider_a".to_string(), converting_provider(url));
        let mut cfg = test_config("provider_a", providers);
        cfg.listen_base_path = "/v1".to_string();
        let state = test_proxy_state(cfg);

        // Image input is unsupported by default capabilities.
        let req = messages_request(json!({
            "model": "m",
            "max_tokens": 1,
            "messages": [{"role": "user", "content": [{"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "aaa"}}]}]
        }));
        let resp = proxy_error_response(
            handle_proxy_inner(state, "127.0.0.1:50021".parse().unwrap(), req)
                .await
                .expect_err("unconvertible request should fail"),
        );
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let out: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(out["type"], "error");
        assert_eq!(out["error"]["type"], "invalid_request_error");
        assert!(out["error"]["message"].as_str().unwrap().contains("image"));
        let _ = shutdown_tx.send(());
    }

    #[test]
    fn provider_converts_messages_requires_both_config() {
        let provider = converting_provider(Url::parse("https://example.com/").unwrap());
        assert!(crate::api_conversion::provider_converts_path(
            &provider,
            "/v1/messages"
        ));
        let passthrough = Provider {
            upstream_api: codex_provider_proxy_api_conversion::dialect::UpstreamApi::Passthrough,
            ..provider.clone()
        };
        assert!(!crate::api_conversion::provider_converts_path(
            &passthrough,
            "/v1/messages"
        ));
    }
}
