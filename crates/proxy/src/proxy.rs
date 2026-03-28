use std::{
    future::Future,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{header, HeaderMap, Request, Response, StatusCode},
    Router,
};
use bytes::{Bytes, BytesMut};
use futures_util::{Stream, TryStreamExt};
use pin_project_lite::pin_project;
use tokio::sync::Notify;
use tokio::time::{Instant as TokioInstant, Sleep};
use tracing::{debug, info, warn};
use url::Url;

use crate::{
    config::Provider,
    exchange_log::{
        maybe_create_exchange_logger, ExchangeFileLogger, ExchangeLogContext,
        SharedExchangeFileLogger,
    },
    log_capture::{Capture, CaptureConfig, SharedCapture},
    runtime::RuntimeState,
};

const MAX_ANCESTOR_PID_DEPTH: usize = 64;

#[derive(Clone)]
pub struct ProxyState {
    pub listen_addr: SocketAddr,
    pub runtime: RuntimeState,
}

#[derive(Clone)]
struct ResolvedProviderRoute {
    pid: Option<u32>,
    route_pid: Option<u32>,
    provider_name: String,
    provider: Provider,
}

#[derive(Clone)]
struct RetryRequestTemplate {
    http_client: reqwest::Client,
    method: http::Method,
    url: Url,
    headers: HeaderMap,
    request_body: Bytes,
}

#[derive(Clone)]
struct RetrySendArgs {
    request_id: u64,
    transparent_retry_count: u32,
    idle_timeout: Option<Duration>,
    exchange_logger: Option<SharedExchangeFileLogger>,
    request: RetryRequestTemplate,
}

struct UpstreamSendRequest {
    request_id: u64,
    method: http::Method,
    url: Url,
    headers: HeaderMap,
    body: Body,
    exchange_logger: Option<SharedExchangeFileLogger>,
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
        Err(err) => {
            warn!(error = %err, "proxy error");
            Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
                .body(Body::from(format!("proxy error: {err}\n")))
                .unwrap()
        }
    }
}

async fn handle_proxy_inner(
    state: ProxyState,
    peer: SocketAddr,
    req: Request<Body>,
) -> Result<Response<Body>> {
    let request_id = state.runtime.next_request_id();
    let started = Instant::now();
    let cfg = state.runtime.config().await;
    let resolved = resolve_provider_for_request(&state, &cfg, peer, request_id).await?;
    let pid = resolved.pid;
    let route_pid = resolved.route_pid;
    let provider_name = resolved.provider_name;
    let provider = resolved.provider;

    let (parts, body) = req.into_parts();

    if cfg.logging.log_requests {
        info!(
            request_id,
            pid = ?pid,
            route_pid = ?route_pid,
            peer = %peer,
            provider = %provider_name,
            method = %parts.method,
            uri = %parts.uri,
            "request"
        );
        debug!(
            request_id,
            pid = ?pid,
            route_pid = ?route_pid,
            provider = %provider_name,
            headers = ?parts.headers,
            "request headers"
        );
    }

    let forwarded_path = match strip_listen_base_path(&cfg.listen_base_path, parts.uri.path()) {
        Some(p) => p,
        None => {
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
                .body(Body::from("not found\n"))?);
        }
    };
    let url = build_outgoing_url(&provider, forwarded_path, parts.uri.query())?;

    let exchange_logger = maybe_create_exchange_logger(
        &cfg.logging,
        ExchangeLogContext {
            request_id,
            peer,
            pid,
            route_pid,
            provider_name: &provider_name,
            method: &parts.method,
            uri: &parts.uri,
            upstream_url: &url,
            request_headers: &parts.headers,
        },
    );

    let mut headers = filtered_incoming_headers(&parts.headers);
    headers.insert(
        header::AUTHORIZATION,
        http::HeaderValue::from_str(&provider.authorization_value())
            .context("build Authorization header")?,
    );

    let send_result = send_upstream_request(
        &state,
        &cfg,
        UpstreamSendRequest {
            request_id,
            method: parts.method.clone(),
            url: url.clone(),
            headers,
            body,
            exchange_logger: exchange_logger.clone(),
        },
    )
    .await;

    let (resp, req_body_capture, _final_attempt, final_attempt_latency_ms) = match send_result {
        Ok(resp) => resp,
        Err(err) => {
            let error_latency_ms = started.elapsed().as_millis();
            let err_text = err.to_string();
            with_exchange_logger_blocking(
                exchange_logger.clone(),
                request_id,
                "mark upstream send error",
                move |logger| {
                    logger.mark_upstream_send_error(error_latency_ms, &err_text);
                    logger.finalize();
                },
            )
            .await;
            return Err(err).context("send upstream request");
        }
    };
    let status = resp.status();
    let resp_headers = resp.headers().clone();

    if cfg.logging.log_responses {
        if status.is_success() {
            info!(
                request_id,
                pid = ?pid,
                route_pid = ?route_pid,
                peer = %peer,
                provider = %provider_name,
                status = %status,
                latency_ms = started.elapsed().as_millis(),
                "response headers received"
            );
        } else {
            warn!(
                request_id,
                pid = ?pid,
                route_pid = ?route_pid,
                peer = %peer,
                provider = %provider_name,
                status = %status,
                latency_ms = started.elapsed().as_millis(),
                "response headers received with non-2xx upstream status"
            );
        }
        debug!(
            request_id,
            pid = ?pid,
            route_pid = ?route_pid,
            provider = %provider_name,
            headers = ?resp_headers,
            "response headers"
        );
    }

    let resp_headers_for_log = resp_headers.clone();
    let upstream_latency_ms = started.elapsed().as_millis();
    with_exchange_logger_blocking(
        exchange_logger.clone(),
        request_id,
        "record final upstream attempt",
        move |logger| {
            logger.record_attempt(
                status,
                &resp_headers_for_log,
                final_attempt_latency_ms,
                None,
                true,
            );
            logger.write_response_headers(status, &resp_headers_for_log, upstream_latency_ms);
        },
    )
    .await;

    if cfg.logging.log_bodies {
        if let Some(cap) = req_body_capture {
            let summary = cap.lock().unwrap().summary();
            debug!(
                request_id,
                pid = ?pid,
                route_pid = ?route_pid,
                provider = %provider_name,
                truncated = summary.truncated,
                body = %summary.as_lossy_utf8(),
                "request body"
            );
        }
    }

    let mut builder = Response::builder().status(status);
    copy_response_headers(&resp_headers, builder.headers_mut().unwrap())?;

    let (resp_stream, resp_capture) =
        response_stream_and_capture(&cfg, request_id, resp, exchange_logger.clone());
    let body = Body::from_stream(LogOnEndStream::new(resp_stream, move || {
        if let Some(capture) = resp_capture {
            let summary = capture.lock().unwrap().summary();
            debug!(
                request_id,
                pid = ?pid,
                route_pid = ?route_pid,
                provider = %provider_name,
                truncated = summary.truncated,
                body = %summary.as_lossy_utf8(),
                "response body"
            );
        }
        if let Some(exchange_logger) = exchange_logger {
            finalize_exchange_logger_nonblocking(exchange_logger, request_id);
        }
    }));

    Ok(builder.body(body)?)
}

async fn resolve_provider_for_request(
    state: &ProxyState,
    cfg: &crate::config::Config,
    peer: SocketAddr,
    request_id: u64,
) -> Result<ResolvedProviderRoute> {
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

    let default_provider = state.runtime.default_provider().await;
    let (provider_name, route_pid) = match pid {
        Some(pid) => match find_provider_for_pid_or_ancestors(state, pid).await {
            Ok(Some((route_pid, provider_name))) => (provider_name, Some(route_pid)),
            Ok(None) => (default_provider.clone(), None),
            Err(err) => {
                warn!(
                    request_id,
                    pid,
                    peer = %peer,
                    error = %err,
                    "ancestor pid route lookup failed; falling back to default provider"
                );
                (default_provider.clone(), None)
            }
        },
        None => (default_provider.clone(), None),
    };

    let provider = cfg
        .providers
        .get(&provider_name)
        .with_context(|| format!("provider {provider_name:?} missing from config"))?
        .clone();
    Ok(ResolvedProviderRoute {
        pid,
        route_pid,
        provider_name,
        provider,
    })
}

async fn send_upstream_request(
    state: &ProxyState,
    cfg: &crate::config::Config,
    request: UpstreamSendRequest,
) -> std::result::Result<(reqwest::Response, Option<SharedCapture>, u32, u128), std::io::Error> {
    let UpstreamSendRequest {
        request_id,
        method,
        url,
        headers,
        body,
        exchange_logger,
    } = request;
    if cfg.transparent_retry_count == 0 {
        let attempt_started = Instant::now();
        let upload_activity = cfg.upstream_idle_timeout.map(|_| Arc::new(Notify::new()));
        let (req_body, req_body_capture) = maybe_wrap_request_body_for_logging(
            cfg,
            body,
            exchange_logger.clone(),
            upload_activity.clone(),
            request_id,
        );
        let out = state
            .runtime
            .http_client()
            .request(method, url)
            .headers(headers)
            .body(req_body);

        let resp = send_with_optional_idle_timeout(
            request_id,
            cfg.upstream_idle_timeout,
            upload_activity,
            out.send(),
        )
        .await?;
        Ok((
            resp,
            req_body_capture,
            1,
            attempt_started.elapsed().as_millis(),
        ))
    } else {
        let (request_body, req_body_capture) =
            buffer_request_body_for_retry(cfg, body, exchange_logger.clone(), request_id).await?;
        let (resp, final_attempt, final_attempt_latency_ms) =
            send_with_non_2xx_retries(RetrySendArgs {
                request_id,
                transparent_retry_count: cfg.transparent_retry_count,
                idle_timeout: cfg.upstream_idle_timeout,
                exchange_logger,
                request: RetryRequestTemplate {
                    http_client: state.runtime.http_client(),
                    method,
                    url,
                    headers,
                    request_body,
                },
            })
            .await?;
        Ok((
            resp,
            req_body_capture,
            final_attempt,
            final_attempt_latency_ms,
        ))
    }
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

fn finalize_exchange_logger_nonblocking(
    exchange_logger: SharedExchangeFileLogger,
    request_id: u64,
) {
    if let Ok(runtime_handle) = tokio::runtime::Handle::try_current() {
        runtime_handle.spawn_blocking(move || {
            if let Ok(mut logger) = exchange_logger.lock() {
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
    // Preserve any path prefix in the base URL.
    let base = if base_path.is_empty() { "/" } else { base_path };
    let base = base.trim_end_matches('/');
    let incoming = incoming_path.strip_prefix('/').unwrap_or(incoming_path);
    if base.is_empty() || base == "/" {
        format!("/{}", incoming)
    } else if incoming.is_empty() {
        format!("{}/", base)
    } else {
        format!("{}/{}", base, incoming)
    }
}

fn filtered_incoming_headers(headers: &HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::new();
    for (name, value) in headers.iter() {
        if is_hop_by_hop(name) {
            continue;
        }
        if name == header::HOST || name == header::AUTHORIZATION {
            continue;
        }
        out.append(name, value.clone());
    }
    out
}

fn copy_response_headers(headers: &HeaderMap, out: &mut HeaderMap) -> Result<()> {
    for (name, value) in headers.iter() {
        if is_hop_by_hop(name) {
            continue;
        }
        out.append(name, value.clone());
    }
    Ok(())
}

fn is_hop_by_hop(name: &header::HeaderName) -> bool {
    // Minimal hop-by-hop list for a reverse proxy:
    // https://www.rfc-editor.org/rfc/rfc7230#section-6.1
    matches!(
        name.as_str().to_ascii_lowercase().as_str(),
        "connection"
            | "proxy-connection"
            | "keep-alive"
            | "transfer-encoding"
            | "upgrade"
            | "te"
            | "trailer"
    )
}

fn maybe_wrap_request_body_for_logging(
    cfg: &crate::config::Config,
    body: Body,
    exchange_logger: Option<SharedExchangeFileLogger>,
    upload_activity: Option<Arc<Notify>>,
    request_id: u64,
) -> (reqwest::Body, Option<SharedCapture>) {
    let stream = TryStreamExt::map_err(body.into_data_stream(), map_axum_body_err);

    let cap = if cfg.logging.log_bodies {
        Some(Arc::new(std::sync::Mutex::new(Capture::new(
            CaptureConfig {
                max_bytes: cfg.logging.max_body_log_bytes,
            },
        ))))
    } else {
        None
    };
    let cap2 = cap.clone();
    let exchange_logger2 = exchange_logger.clone();
    let upload_activity2 = upload_activity.clone();
    let stream = if cfg.logging.log_bodies || exchange_logger.is_some() || upload_activity.is_some()
    {
        Box::pin(stream.and_then(move |chunk| {
            let cap2 = cap2.clone();
            let exchange_logger2 = exchange_logger2.clone();
            let upload_activity2 = upload_activity2.clone();
            async move {
                if let Some(cap2) = &cap2 {
                    if let Ok(mut c) = cap2.lock() {
                        c.push_chunk(&chunk);
                    }
                }
                with_exchange_logger_blocking(
                    exchange_logger2,
                    request_id,
                    "append request body chunk",
                    {
                        let chunk = chunk.clone();
                        move |logger| logger.on_request_body_chunk(&chunk)
                    },
                )
                .await;
                if let Some(upload_activity) = &upload_activity2 {
                    upload_activity.notify_one();
                }
                Ok(chunk)
            }
        })) as BoxRespStream
    } else {
        Box::pin(stream)
    };
    (reqwest::Body::wrap_stream(stream), cap)
}

async fn buffer_request_body_for_retry(
    cfg: &crate::config::Config,
    body: Body,
    exchange_logger: Option<SharedExchangeFileLogger>,
    request_id: u64,
) -> std::result::Result<(Bytes, Option<SharedCapture>), std::io::Error> {
    let mut stream = TryStreamExt::map_err(body.into_data_stream(), map_axum_body_err);
    let mut buffered = BytesMut::new();

    let cap = if cfg.logging.log_bodies {
        Some(Arc::new(std::sync::Mutex::new(Capture::new(
            CaptureConfig {
                max_bytes: cfg.logging.max_body_log_bytes,
            },
        ))))
    } else {
        None
    };

    while let Some(chunk) = stream.try_next().await? {
        buffered.extend_from_slice(&chunk);

        if let Some(cap) = &cap {
            if let Ok(mut c) = cap.lock() {
                c.push_chunk(&chunk);
            }
        }
        with_exchange_logger_blocking(
            exchange_logger.clone(),
            request_id,
            "append buffered request body chunk",
            {
                let chunk = chunk.clone();
                move |logger| logger.on_request_body_chunk(&chunk)
            },
        )
        .await;
    }

    Ok((buffered.freeze(), cap))
}

type BoxRespStream =
    std::pin::Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static>>;

fn response_stream_and_capture(
    cfg: &crate::config::Config,
    request_id: u64,
    resp: reqwest::Response,
    exchange_logger: Option<SharedExchangeFileLogger>,
) -> (BoxRespStream, Option<SharedCapture>) {
    let stream = TryStreamExt::map_err(resp.bytes_stream(), map_reqwest_body_err);

    let cap = if cfg.logging.log_bodies {
        Some(Arc::new(std::sync::Mutex::new(Capture::new(
            CaptureConfig {
                max_bytes: cfg.logging.max_body_log_bytes,
            },
        ))))
    } else {
        None
    };
    let cap2 = cap.clone();
    let exchange_logger2 = exchange_logger.clone();
    let stream = if cfg.logging.log_bodies || exchange_logger.is_some() {
        Box::pin(stream.and_then(move |chunk| {
            let cap2 = cap2.clone();
            let exchange_logger2 = exchange_logger2.clone();
            async move {
                if let Some(cap2) = &cap2 {
                    if let Ok(mut c) = cap2.lock() {
                        c.push_chunk(&chunk);
                    }
                }
                with_exchange_logger_blocking(
                    exchange_logger2,
                    request_id,
                    "append response body chunk",
                    {
                        let chunk = chunk.clone();
                        move |logger| logger.on_response_body_chunk(&chunk)
                    },
                )
                .await;
                Ok(chunk)
            }
        })) as BoxRespStream
    } else {
        Box::pin(stream)
    };
    match cfg.upstream_idle_timeout {
        Some(idle_timeout) => {
            let stream = IdleTimeoutStream::new(
                stream,
                idle_timeout,
                request_id,
                "upstream response body download",
            );
            (Box::pin(stream), cap)
        }
        None => (stream, cap),
    }
}

fn map_axum_body_err(err: axum::Error) -> std::io::Error {
    std::io::Error::other(err)
}

fn map_reqwest_body_err(err: reqwest::Error) -> std::io::Error {
    std::io::Error::other(err)
}

async fn send_with_idle_timeout<F>(
    request_id: u64,
    idle_timeout: Duration,
    upload_activity: Arc<Notify>,
    send_fut: F,
) -> std::result::Result<reqwest::Response, std::io::Error>
where
    F: Future<Output = std::result::Result<reqwest::Response, reqwest::Error>>,
{
    tokio::pin!(send_fut);
    let sleep = tokio::time::sleep(idle_timeout);
    tokio::pin!(sleep);

    loop {
        tokio::select! {
            result = &mut send_fut => {
                return result.map_err(map_reqwest_body_err);
            }
            _ = &mut sleep => {
                warn!(
                    request_id,
                    idle_timeout_secs = idle_timeout.as_secs(),
                    "closing proxied connection after upstream idle timeout while sending request or waiting for response headers"
                );
                return Err(idle_timeout_error(
                    "sending request or waiting for upstream response headers",
                    idle_timeout,
                ));
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
            let upload_activity = upload_activity.unwrap_or_else(|| Arc::new(Notify::new()));
            send_with_idle_timeout(request_id, idle_timeout, upload_activity, send_fut).await
        }
        None => send_fut.await.map_err(map_reqwest_body_err),
    }
}

async fn send_with_non_2xx_retries(
    args: RetrySendArgs,
) -> std::result::Result<(reqwest::Response, u32, u128), std::io::Error> {
    for attempt in 0..=args.transparent_retry_count {
        let attempt_started = Instant::now();
        let out = args
            .request
            .http_client
            .request(args.request.method.clone(), args.request.url.clone())
            .headers(args.request.headers.clone())
            .body(args.request.request_body.clone());
        let resp =
            send_with_optional_idle_timeout(args.request_id, args.idle_timeout, None, out.send())
                .await?;

        let status = resp.status();
        let attempt_latency_ms = attempt_started.elapsed().as_millis();
        let attempt_number = attempt.saturating_add(1);
        if status.is_success() || attempt == args.transparent_retry_count {
            return Ok((resp, attempt_number, attempt_latency_ms));
        }

        let resp_headers = resp.headers().clone();
        let retry_body_bytes = drain_response_body_with_optional_idle_timeout(
            args.request_id,
            args.idle_timeout,
            resp,
            "reading non-final retry response body",
        )
        .await?;
        with_exchange_logger_blocking(
            args.exchange_logger.clone(),
            args.request_id,
            "record retry attempt",
            move |logger| {
                logger.record_attempt(
                    status,
                    &resp_headers,
                    attempt_latency_ms,
                    Some(retry_body_bytes),
                    false,
                );
            },
        )
        .await;

        warn!(
            args.request_id,
            status = %status,
            attempt = attempt_number,
            total_attempts = args.transparent_retry_count.saturating_add(1),
            retries_remaining = args.transparent_retry_count - attempt,
            "upstream returned non-2xx status; retrying transparently"
        );
    }

    unreachable!("retry loop always returns")
}

async fn drain_response_body_with_optional_idle_timeout(
    request_id: u64,
    idle_timeout: Option<Duration>,
    resp: reqwest::Response,
    phase: &'static str,
) -> std::result::Result<u64, std::io::Error> {
    let mut stream = TryStreamExt::map_err(resp.bytes_stream(), map_reqwest_body_err);
    let mut total_bytes = 0u64;

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
        total_bytes = total_bytes.saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
    }

    Ok(total_bytes)
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
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use axum::{
        body::Bytes,
        extract::State,
        http::{HeaderMap, Method, StatusCode},
        response::IntoResponse,
        routing::any,
        Router,
    };
    use tokio::sync::oneshot;
    use url::Url;

    use super::{
        join_paths, send_with_non_2xx_retries, strip_listen_base_path, RetryRequestTemplate,
        RetrySendArgs,
    };

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

    #[derive(Clone)]
    struct RetryServerState {
        statuses: Arc<Vec<StatusCode>>,
        call_count: Arc<AtomicUsize>,
    }

    async fn retry_server_handler(
        State(state): State<RetryServerState>,
        body: Bytes,
    ) -> impl IntoResponse {
        assert_eq!(body, Bytes::from_static(b"retry-body"));

        let call_index = state.call_count.fetch_add(1, Ordering::SeqCst);
        let status = state
            .statuses
            .get(call_index)
            .copied()
            .or_else(|| state.statuses.last().copied())
            .unwrap_or(StatusCode::OK);
        (status, format!("attempt-{call_index}"))
    }

    async fn spawn_retry_server(
        statuses: Vec<StatusCode>,
    ) -> (Url, Arc<AtomicUsize>, oneshot::Sender<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let call_count = Arc::new(AtomicUsize::new(0));
        let state = RetryServerState {
            statuses: Arc::new(statuses),
            call_count: call_count.clone(),
        };
        let app = Router::new()
            .route("/", any(retry_server_handler))
            .with_state(state);

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });

        (
            Url::parse(&format!("http://{addr}/")).unwrap(),
            call_count,
            shutdown_tx,
        )
    }

    #[tokio::test]
    async fn retries_non_2xx_status_until_success() {
        let (url, call_count, shutdown_tx) =
            spawn_retry_server(vec![StatusCode::INTERNAL_SERVER_ERROR, StatusCode::OK]).await;

        let (resp, final_attempt, _final_attempt_latency_ms) =
            send_with_non_2xx_retries(RetrySendArgs {
                request_id: 1,
                transparent_retry_count: 2,
                idle_timeout: None,
                exchange_logger: None,
                request: RetryRequestTemplate {
                    http_client: reqwest::Client::new(),
                    method: Method::POST,
                    url,
                    headers: HeaderMap::new(),
                    request_body: Bytes::from_static(b"retry-body"),
                },
            })
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(final_attempt, 2);
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

        let (resp, final_attempt, _final_attempt_latency_ms) =
            send_with_non_2xx_retries(RetrySendArgs {
                request_id: 2,
                transparent_retry_count: 1,
                idle_timeout: None,
                exchange_logger: None,
                request: RetryRequestTemplate {
                    http_client: reqwest::Client::new(),
                    method: Method::POST,
                    url,
                    headers: HeaderMap::new(),
                    request_body: Bytes::from_static(b"retry-body"),
                },
            })
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(final_attempt, 2);
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
        let _ = shutdown_tx.send(());
    }
}
