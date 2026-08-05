use std::{net::SocketAddr, sync::Arc};

use anyhow::{anyhow, Context, Result};
use axum::{
    extract::{ConnectInfo, Path, Query, Request, State},
    http::{header, HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Extension, Json, Router,
};
use codex_provider_proxy_rpc_types::{
    DeleteRouteResponse, ListRoutesResponse, ProvidersResponse, RouteEntry,
    SetDefaultProviderRequest, SetRouteRequest,
};
use serde::Deserialize;

use crate::{config::Config, runtime::RuntimeState};

#[derive(Clone)]
pub struct RpcState {
    pub runtime: RuntimeState,
}

pub fn router(state: RpcState) -> Router {
    let protected_routes = Router::new()
        .route("/rpc/v1/routes", get(list_routes).post(set_route))
        .route("/rpc/v1/routes/clear", post(clear_routes))
        .route("/rpc/v1/routes/:pid", delete(delete_route))
        .route("/rpc/v1/providers", get(list_providers))
        .route("/rpc/v1/statistics", get(statistics))
        .route("/rpc/v1/default-provider", post(set_default_provider))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            authorize_rpc_request,
        ));

    Router::new()
        .route("/healthz", get(healthz))
        .merge(protected_routes)
        .with_state(state)
}

#[derive(Debug, Deserialize)]
struct StatisticsQuery {
    #[serde(default = "default_statistics_hours")]
    hours: u32,
}

fn default_statistics_hours() -> u32 {
    24
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok\n")
}

fn ensure_peer_allowed(peer: SocketAddr, rpc_listen_addr: SocketAddr) -> Result<()> {
    if !rpc_listen_addr.ip().is_loopback() || peer.ip().is_loopback() {
        Ok(())
    } else {
        Err(anyhow!(
            "only loopback RPC clients are allowed when rpc_listen_addr is loopback"
        ))
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

async fn authorize_rpc_request(
    State(state): State<RpcState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    mut request: Request,
    next: Next,
) -> Response {
    let cfg = state.runtime.config().await;
    if let Err(err) = ensure_peer_allowed(peer, cfg.rpc_listen_addr) {
        return (StatusCode::FORBIDDEN, format!("{err}\n")).into_response();
    }
    if let Err(err) = ensure_auth(request.headers(), &cfg) {
        return (StatusCode::UNAUTHORIZED, format!("{err}\n")).into_response();
    }

    request.extensions_mut().insert(cfg);
    next.run(request).await
}

async fn list_routes(State(state): State<RpcState>) -> impl IntoResponse {
    let mut routes: Vec<RouteEntry> = state
        .runtime
        .pid_routes()
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
    Extension(cfg): Extension<Arc<Config>>,
    Json(req): Json<SetRouteRequest>,
) -> impl IntoResponse {
    if !cfg.providers.contains_key(&req.provider) {
        return (
            StatusCode::BAD_REQUEST,
            format!("unknown provider: {}\n", req.provider),
        )
            .into_response();
    }

    state.runtime.pid_routes().insert(req.pid, req.provider);
    StatusCode::NO_CONTENT.into_response()
}

async fn delete_route(State(state): State<RpcState>, Path(pid): Path<u32>) -> impl IntoResponse {
    let removed = state.runtime.pid_routes().remove(&pid).is_some();
    Json(DeleteRouteResponse { removed }).into_response()
}

async fn clear_routes(State(state): State<RpcState>) -> impl IntoResponse {
    state.runtime.pid_routes().clear();
    StatusCode::NO_CONTENT.into_response()
}

async fn list_providers(State(state): State<RpcState>) -> impl IntoResponse {
    let routing = state.runtime.routing_snapshot().await;
    let mut providers: Vec<String> = routing.config.providers.keys().cloned().collect();
    providers.sort();

    Json(ProvidersResponse {
        default_provider: routing.default_provider,
        providers,
    })
    .into_response()
}

async fn statistics(
    State(state): State<RpcState>,
    Query(query): Query<StatisticsQuery>,
) -> impl IntoResponse {
    let hours = (query.hours != 0).then_some(query.hours);
    match state.runtime.statistics().query(hours).await {
        Ok(statistics) => Json(statistics).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("query statistics: {err:#}\n"),
        )
            .into_response(),
    }
}

async fn set_default_provider(
    State(state): State<RpcState>,
    Json(req): Json<SetDefaultProviderRequest>,
) -> impl IntoResponse {
    if !state
        .runtime
        .set_default_provider(req.provider.clone())
        .await
    {
        return (
            StatusCode::BAD_REQUEST,
            format!("unknown provider: {}\n", req.provider),
        )
            .into_response();
    }

    StatusCode::NO_CONTENT.into_response()
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::ensure_peer_allowed;

    #[test]
    fn loopback_rpc_listener_rejects_non_loopback_peer() {
        let peer: SocketAddr = "192.168.1.100:50000".parse().unwrap();
        let rpc_listen_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();
        assert!(ensure_peer_allowed(peer, rpc_listen_addr).is_err());
    }

    #[test]
    fn loopback_rpc_listener_allows_loopback_peer() {
        let peer: SocketAddr = "127.0.0.1:50000".parse().unwrap();
        let rpc_listen_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();
        assert!(ensure_peer_allowed(peer, rpc_listen_addr).is_ok());
    }

    #[test]
    fn non_loopback_rpc_listener_allows_non_loopback_peer() {
        let peer: SocketAddr = "192.168.1.100:50000".parse().unwrap();
        let rpc_listen_addr: SocketAddr = "0.0.0.0:8081".parse().unwrap();
        assert!(ensure_peer_allowed(peer, rpc_listen_addr).is_ok());
    }
}
