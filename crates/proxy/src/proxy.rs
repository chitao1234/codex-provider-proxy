use std::{net::SocketAddr, sync::Arc, time::Instant};

use anyhow::{Context, Result};
use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{header, HeaderMap, Request, Response, StatusCode},
    Router,
};
use bytes::Bytes;
use futures_util::{Stream, TryStreamExt};
use pin_project_lite::pin_project;
use tracing::{debug, info, warn};
use url::Url;

use crate::{
    config::Provider,
    exchange_log::{maybe_create_exchange_logger, SharedExchangeFileLogger},
    log_capture::{Capture, CaptureConfig, SharedCapture},
    runtime::RuntimeState,
};

const MAX_ANCESTOR_PID_DEPTH: usize = 64;

#[derive(Clone)]
pub struct ProxyState {
    pub listen_addr: SocketAddr,
    pub runtime: RuntimeState,
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
        Some(pid) => match find_provider_for_pid_or_ancestors(&state, pid).await {
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
        request_id,
        peer,
        pid,
        route_pid,
        &provider_name,
        &parts.method,
        &parts.uri,
        &url,
        &parts.headers,
    );

    let (req_body, req_body_capture) =
        maybe_wrap_request_body_for_logging(&cfg, body, exchange_logger.clone());
    let mut headers = filtered_incoming_headers(&parts.headers);
    headers.insert(
        header::AUTHORIZATION,
        http::HeaderValue::from_str(&provider.authorization_value())
            .context("build Authorization header")?,
    );

    let out = state
        .runtime
        .http_client()
        .request(parts.method.clone(), url)
        .headers(headers)
        .body(req_body);

    let resp = out.send().await.context("send upstream request")?;
    let status = resp.status();
    let resp_headers = resp.headers().clone();

    if cfg.logging.log_responses {
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
        debug!(
            request_id,
            pid = ?pid,
            route_pid = ?route_pid,
            provider = %provider_name,
            headers = ?resp_headers,
            "response headers"
        );
    }

    if let Some(exchange_logger) = &exchange_logger {
        if let Ok(mut logger) = exchange_logger.lock() {
            logger.write_response_headers(status, &resp_headers, started.elapsed().as_millis());
        }
    }

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
        response_stream_and_capture(&cfg, resp, exchange_logger.clone());
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
            if let Ok(mut logger) = exchange_logger.lock() {
                logger.finalize();
            }
        }
    }));

    Ok(builder.body(body)?)
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
) -> (reqwest::Body, Option<SharedCapture>) {
    if !cfg.logging.log_bodies && exchange_logger.is_none() {
        let stream = TryStreamExt::map_err(body.into_data_stream(), map_axum_body_err);
        return (reqwest::Body::wrap_stream(stream), None);
    }

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
    let stream = TryStreamExt::inspect_ok(
        TryStreamExt::map_err(body.into_data_stream(), map_axum_body_err),
        move |chunk| {
            if let Some(cap2) = &cap2 {
                if let Ok(mut c) = cap2.lock() {
                    c.push_chunk(chunk);
                }
            }
            if let Some(exchange_logger) = &exchange_logger2 {
                if let Ok(mut logger) = exchange_logger.lock() {
                    logger.on_request_body_chunk(chunk);
                }
            }
        },
    );
    (reqwest::Body::wrap_stream(stream), cap)
}

type BoxRespStream =
    std::pin::Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send + 'static>>;

fn response_stream_and_capture(
    cfg: &crate::config::Config,
    resp: reqwest::Response,
    exchange_logger: Option<SharedExchangeFileLogger>,
) -> (BoxRespStream, Option<SharedCapture>) {
    let stream = resp.bytes_stream();
    if !cfg.logging.log_bodies && exchange_logger.is_none() {
        return (Box::pin(stream), None);
    }

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
    let stream = stream.inspect_ok(move |chunk| {
        if let Some(cap2) = &cap2 {
            if let Ok(mut c) = cap2.lock() {
                c.push_chunk(chunk);
            }
        }
        if let Some(exchange_logger) = &exchange_logger2 {
            if let Ok(mut logger) = exchange_logger.lock() {
                logger.on_response_body_chunk(chunk);
            }
        }
    });
    (Box::pin(stream), cap)
}

fn map_axum_body_err(err: axum::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, err)
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
    use super::{join_paths, strip_listen_base_path};

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
}
