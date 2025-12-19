use std::{net::SocketAddr, sync::Arc};

use anyhow::{anyhow, Context, Result};
use axum::{
    extract::{ConnectInfo, Path, State},
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use codex_provider_proxy_rpc_types::{
    DeleteRouteResponse, ListRoutesResponse, ProvidersResponse, RouteEntry, SetDefaultProviderRequest,
    SetRouteRequest,
};
use dashmap::DashMap;
use tokio::sync::RwLock;

use crate::config::Config;

#[derive(Clone)]
pub struct RpcState {
    pub cfg: Arc<Config>,
    pub default_provider: Arc<RwLock<String>>,
    pub pid_routes: Arc<DashMap<u32, String>>,
}

pub fn router(state: RpcState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/rpc/v1/routes", get(list_routes).post(set_route))
        .route("/rpc/v1/routes/clear", post(clear_routes))
        .route("/rpc/v1/routes/:pid", delete(delete_route))
        .route("/rpc/v1/providers", get(list_providers))
        .route("/rpc/v1/default-provider", post(set_default_provider))
        .with_state(state)
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok\n")
}

fn ensure_loopback(peer: SocketAddr) -> Result<()> {
    if peer.ip().is_loopback() {
        Ok(())
    } else {
        Err(anyhow!("only loopback connections are supported"))
    }
}

fn ensure_auth(headers: &HeaderMap, cfg: &Config) -> Result<()> {
    let Some(token) = &cfg.rpc_token else {
        return Ok(());
    };

    let Some(value) = headers.get(header::AUTHORIZATION) else {
        return Err(anyhow!("missing Authorization header"));
    };
    let value = value.to_str().context("Authorization header not utf-8")?;
    let value = value
        .strip_prefix("Bearer ")
        .ok_or_else(|| anyhow!("Authorization must be Bearer"))?;

    if value == token {
        Ok(())
    } else {
        Err(anyhow!("invalid token"))
    }
}

async fn list_routes(
    State(state): State<RpcState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(err) = ensure_loopback(peer) {
        return (StatusCode::FORBIDDEN, format!("{err}\n")).into_response();
    }
    if let Err(err) = ensure_auth(&headers, &state.cfg) {
        return (StatusCode::UNAUTHORIZED, format!("{err}\n")).into_response();
    }

    let mut routes: Vec<RouteEntry> = state
        .pid_routes
        .iter()
        .map(|e| RouteEntry {
            pid: *e.key(),
            provider: e.value().clone(),
        })
        .collect();
    routes.sort_by_key(|r| r.pid);
    Json(ListRoutesResponse { routes }).into_response()
}

async fn set_route(
    State(state): State<RpcState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<SetRouteRequest>,
) -> impl IntoResponse {
    if let Err(err) = ensure_loopback(peer) {
        return (StatusCode::FORBIDDEN, format!("{err}\n")).into_response();
    }
    if let Err(err) = ensure_auth(&headers, &state.cfg) {
        return (StatusCode::UNAUTHORIZED, format!("{err}\n")).into_response();
    }

    if !state.cfg.providers.contains_key(&req.provider) {
        return (
            StatusCode::BAD_REQUEST,
            format!("unknown provider: {}\n", req.provider),
        )
            .into_response();
    }

    state.pid_routes.insert(req.pid, req.provider);
    StatusCode::NO_CONTENT.into_response()
}

async fn delete_route(
    State(state): State<RpcState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(pid): Path<u32>,
) -> impl IntoResponse {
    if let Err(err) = ensure_loopback(peer) {
        return (StatusCode::FORBIDDEN, format!("{err}\n")).into_response();
    }
    if let Err(err) = ensure_auth(&headers, &state.cfg) {
        return (StatusCode::UNAUTHORIZED, format!("{err}\n")).into_response();
    }

    let removed = state.pid_routes.remove(&pid).is_some();
    Json(DeleteRouteResponse { removed }).into_response()
}

async fn clear_routes(
    State(state): State<RpcState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(err) = ensure_loopback(peer) {
        return (StatusCode::FORBIDDEN, format!("{err}\n")).into_response();
    }
    if let Err(err) = ensure_auth(&headers, &state.cfg) {
        return (StatusCode::UNAUTHORIZED, format!("{err}\n")).into_response();
    }

    state.pid_routes.clear();
    StatusCode::NO_CONTENT.into_response()
}

async fn list_providers(
    State(state): State<RpcState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(err) = ensure_loopback(peer) {
        return (StatusCode::FORBIDDEN, format!("{err}\n")).into_response();
    }
    if let Err(err) = ensure_auth(&headers, &state.cfg) {
        return (StatusCode::UNAUTHORIZED, format!("{err}\n")).into_response();
    }

    let mut providers: Vec<String> = state.cfg.providers.keys().cloned().collect();
    providers.sort();

    let default_provider = state.default_provider.read().await.clone();
    Json(ProvidersResponse {
        default_provider,
        providers,
    })
    .into_response()
}

async fn set_default_provider(
    State(state): State<RpcState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<SetDefaultProviderRequest>,
) -> impl IntoResponse {
    if let Err(err) = ensure_loopback(peer) {
        return (StatusCode::FORBIDDEN, format!("{err}\n")).into_response();
    }
    if let Err(err) = ensure_auth(&headers, &state.cfg) {
        return (StatusCode::UNAUTHORIZED, format!("{err}\n")).into_response();
    }

    if !state.cfg.providers.contains_key(&req.provider) {
        return (
            StatusCode::BAD_REQUEST,
            format!("unknown provider: {}\n", req.provider),
        )
            .into_response();
    }

    *state.default_provider.write().await = req.provider;
    StatusCode::NO_CONTENT.into_response()
}
