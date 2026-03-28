use std::{
    collections::{HashMap, HashSet},
    ffi::OsStr,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use anyhow::{Context, Result};
use dashmap::DashMap;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use pid_resolver::PidResolver;
use tokio::{
    net::TcpListener,
    sync::{oneshot, Mutex, RwLock},
    task::JoinHandle,
};
use tracing::{info, warn};
use tracing_subscriber::{reload, EnvFilter, Registry};

use crate::{
    config::Config,
    proxy::{self, ProxyState},
    rpc::{self, RpcState},
};

pub type LogReloadHandle = reload::Handle<EnvFilter, Registry>;

#[derive(Debug, Clone, Default)]
pub struct ConfigOverrides {
    pub log_requests: bool,
    pub log_responses: bool,
    pub log_bodies: bool,
    pub max_body_log_bytes: Option<usize>,
}

impl ConfigOverrides {
    pub fn apply(&self, cfg: &mut Config) {
        if self.log_requests {
            cfg.logging.log_requests = true;
        }
        if self.log_responses {
            cfg.logging.log_responses = true;
        }
        if self.log_bodies {
            cfg.logging.log_bodies = true;
        }
        if let Some(max) = self.max_body_log_bytes {
            cfg.logging.max_body_log_bytes = max;
        }
    }
}

#[derive(Clone)]
pub struct RuntimeState {
    inner: Arc<RuntimeInner>,
}

struct RuntimeInner {
    config: RwLock<Arc<Config>>,
    default_provider: RwLock<String>,
    pid_routes: Arc<DashMap<u32, String>>,
    pid_resolver: Arc<dyn PidResolver>,
    http_client: reqwest::Client,
    request_seq: AtomicU64,
    log_reload: LogReloadHandle,
}

pub struct ApplyConfigSummary {
    pub removed_pid_routes: usize,
}

impl RuntimeState {
    pub fn new(
        config: Arc<Config>,
        pid_resolver: Arc<dyn PidResolver>,
        http_client: reqwest::Client,
        log_reload: LogReloadHandle,
    ) -> Self {
        Self {
            inner: Arc::new(RuntimeInner {
                default_provider: RwLock::new(config.default_provider.clone()),
                config: RwLock::new(config),
                pid_routes: Arc::new(DashMap::new()),
                pid_resolver,
                http_client,
                request_seq: AtomicU64::new(1),
                log_reload,
            }),
        }
    }

    pub async fn config(&self) -> Arc<Config> {
        self.inner.config.read().await.clone()
    }

    pub async fn default_provider(&self) -> String {
        self.inner.default_provider.read().await.clone()
    }

    pub async fn set_default_provider(&self, provider: String) {
        *self.inner.default_provider.write().await = provider;
    }

    pub fn pid_routes(&self) -> Arc<DashMap<u32, String>> {
        self.inner.pid_routes.clone()
    }

    pub fn pid_resolver(&self) -> Arc<dyn PidResolver> {
        self.inner.pid_resolver.clone()
    }

    pub fn http_client(&self) -> reqwest::Client {
        self.inner.http_client.clone()
    }

    pub fn next_request_id(&self) -> u64 {
        self.inner.request_seq.fetch_add(1, Ordering::Relaxed)
    }

    pub async fn apply_config(&self, config: Arc<Config>) -> Result<ApplyConfigSummary> {
        let filter = config.logging.env_filter()?;
        self.inner
            .log_reload
            .reload(filter)
            .context("reload tracing filter")?;

        let removed_pids: Vec<u32> = self
            .inner
            .pid_routes
            .iter()
            .filter(|entry| !config.providers.contains_key(entry.value()))
            .map(|entry| *entry.key())
            .collect();

        {
            let mut cfg = self.inner.config.write().await;
            *cfg = config.clone();
        }
        {
            let mut default_provider = self.inner.default_provider.write().await;
            *default_provider = config.default_provider.clone();
        }

        for pid in &removed_pids {
            self.inner.pid_routes.remove(pid);
        }

        Ok(ApplyConfigSummary {
            removed_pid_routes: removed_pids.len(),
        })
    }
}

pub struct ServerManager {
    config_path: PathBuf,
    overrides: ConfigOverrides,
    runtime: RuntimeState,
    reconfigure_gate: Mutex<()>,
    listeners: Mutex<ListenerSet>,
}

struct ListenerSet {
    proxy: HashMap<SocketAddr, ListenerHandle>,
    rpc: Option<(SocketAddr, ListenerHandle)>,
}

struct ListenerHandle {
    local_addr: SocketAddr,
    shutdown_tx: oneshot::Sender<()>,
    task: JoinHandle<Result<()>>,
}

struct BoundListener {
    configured_addr: SocketAddr,
    local_addr: SocketAddr,
    listener: TcpListener,
}

impl ServerManager {
    pub fn new(config_path: PathBuf, overrides: ConfigOverrides, runtime: RuntimeState) -> Self {
        Self {
            config_path,
            overrides,
            runtime,
            reconfigure_gate: Mutex::new(()),
            listeners: Mutex::new(ListenerSet {
                proxy: HashMap::new(),
                rpc: None,
            }),
        }
    }

    pub fn config_path(&self) -> &Path {
        &self.config_path
    }

    pub async fn reload_from_disk(&self) -> Result<()> {
        let config = self.load_config_from_disk()?;
        self.reconfigure(config).await
    }

    pub async fn reconfigure(&self, config: Config) -> Result<()> {
        let _reconfigure_guard = self.reconfigure_gate.lock().await;
        let config = Arc::new(config);
        let desired_proxy_addrs: HashSet<SocketAddr> =
            config.listen_addrs.iter().copied().collect();

        let (existing_proxy_addrs, existing_rpc_addr) = {
            let listeners = self.listeners.lock().await;
            (
                listeners.proxy.keys().copied().collect::<HashSet<_>>(),
                listeners.rpc.as_ref().map(|(addr, _)| *addr),
            )
        };

        let mut new_proxy_listeners = Vec::new();
        for addr in config.listen_addrs.iter().copied() {
            if !existing_proxy_addrs.contains(&addr) {
                new_proxy_listeners.push(bind_listener(addr).await?);
            }
        }

        let new_rpc_listener = match existing_rpc_addr {
            Some(addr) if addr == config.rpc_listen_addr => None,
            _ => Some(bind_listener(config.rpc_listen_addr).await?),
        };

        let summary = self.runtime.apply_config(config.clone()).await?;

        let mut listeners = self.listeners.lock().await;
        for bound in new_proxy_listeners {
            if listeners.proxy.contains_key(&bound.configured_addr) {
                info!(
                    configured = %bound.configured_addr,
                    listen = %bound.local_addr,
                    "proxy listener already present; dropping newly bound listener"
                );
                continue;
            }
            info!(
                configured = %bound.configured_addr,
                listen = %bound.local_addr,
                "proxy listening"
            );
            listeners.proxy.insert(
                bound.configured_addr,
                spawn_proxy_listener(self.runtime.clone(), bound),
            );
        }

        if let Some(bound) = new_rpc_listener {
            if listeners
                .rpc
                .as_ref()
                .map(|(addr, _)| *addr == config.rpc_listen_addr)
                .unwrap_or(false)
            {
                info!(
                    configured = %bound.configured_addr,
                    rpc_listen = %bound.local_addr,
                    "rpc listener already present; dropping newly bound listener"
                );
            } else {
                info!(
                    configured = %bound.configured_addr,
                    rpc_listen = %bound.local_addr,
                    "rpc listening"
                );
                let new_handle = spawn_rpc_listener(self.runtime.clone(), bound);
                if let Some((old_addr, old_handle)) =
                    listeners.rpc.replace((config.rpc_listen_addr, new_handle))
                {
                    old_handle.stop("rpc", old_addr);
                }
            }
        }

        let to_remove: Vec<SocketAddr> = listeners
            .proxy
            .keys()
            .copied()
            .filter(|addr| !desired_proxy_addrs.contains(addr))
            .collect();
        for addr in to_remove {
            if let Some(handle) = listeners.proxy.remove(&addr) {
                handle.stop("proxy", addr);
            }
        }

        info!(
            listen_addrs = ?config.listen_addrs,
            rpc_listen_addr = %config.rpc_listen_addr,
            "config applied"
        );
        if summary.removed_pid_routes > 0 {
            warn!(
                removed_pid_routes = summary.removed_pid_routes,
                "removed PID routes for deleted providers"
            );
        }
        Ok(())
    }

    pub async fn shutdown_all(&self) {
        let _reconfigure_guard = self.reconfigure_gate.lock().await;
        let (proxy_handles, rpc_handle) = {
            let mut listeners = self.listeners.lock().await;
            let proxy_handles = std::mem::take(&mut listeners.proxy);
            let rpc_handle = listeners.rpc.take();
            (proxy_handles, rpc_handle)
        };

        for (addr, handle) in proxy_handles {
            handle.stop_and_wait("proxy", addr).await;
        }
        if let Some((addr, handle)) = rpc_handle {
            handle.stop_and_wait("rpc", addr).await;
        }
    }

    fn load_config_from_disk(&self) -> Result<Config> {
        let config_str = std::fs::read_to_string(&self.config_path)
            .with_context(|| format!("read config {}", self.config_path.display()))?;
        let mut config = Config::from_toml_str(&config_str)?;
        self.overrides.apply(&mut config);
        Ok(config)
    }
}

impl ListenerHandle {
    fn stop(self, kind: &'static str, configured_addr: SocketAddr) {
        let local_addr = self.local_addr;
        let _ = self.shutdown_tx.send(());
        tokio::spawn(async move {
            match self.task.await {
                Ok(Ok(())) => {
                    info!(
                        configured = %configured_addr,
                        listen = %local_addr,
                        kind,
                        "listener stopped"
                    );
                }
                Ok(Err(err)) => {
                    warn!(
                        configured = %configured_addr,
                        listen = %local_addr,
                        kind,
                        error = %err,
                        "listener exited with error"
                    );
                }
                Err(err) => {
                    warn!(
                        configured = %configured_addr,
                        listen = %local_addr,
                        kind,
                        error = %err,
                        "listener task failed"
                    );
                }
            }
        });
    }

    async fn stop_and_wait(self, kind: &'static str, configured_addr: SocketAddr) {
        let local_addr = self.local_addr;
        let _ = self.shutdown_tx.send(());
        match self.task.await {
            Ok(Ok(())) => {
                info!(
                    configured = %configured_addr,
                    listen = %local_addr,
                    kind,
                    "listener stopped"
                );
            }
            Ok(Err(err)) => {
                warn!(
                    configured = %configured_addr,
                    listen = %local_addr,
                    kind,
                    error = %err,
                    "listener exited with error"
                );
            }
            Err(err) => {
                warn!(
                    configured = %configured_addr,
                    listen = %local_addr,
                    kind,
                    error = %err,
                    "listener task failed"
                );
            }
        }
    }
}

pub fn spawn_config_watcher(manager: Arc<ServerManager>) -> Result<JoinHandle<()>> {
    let watch_dir = manager
        .config_path()
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let config_path = manager.config_path().to_path_buf();
    let target_name = config_path.file_name().map(OsStr::to_os_string);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<notify::Result<Event>>();

    let mut watcher: RecommendedWatcher = notify::recommended_watcher(move |event| {
        let _ = tx.send(event);
    })
    .context("create config watcher")?;
    watcher
        .watch(&watch_dir, RecursiveMode::NonRecursive)
        .with_context(|| format!("watch {}", watch_dir.display()))?;

    Ok(tokio::spawn(async move {
        let _watcher = watcher;
        while let Some(event) = rx.recv().await {
            match event {
                Ok(event) if event_targets_config(&event, &config_path, target_name.as_deref()) => {
                    match manager.reload_from_disk().await {
                        Ok(()) => info!(path = %config_path.display(), "config reloaded"),
                        Err(err) => warn!(
                            path = %config_path.display(),
                            error = %err,
                            "config reload failed"
                        ),
                    }
                }
                Ok(_) => {}
                Err(err) => warn!(
                    path = %config_path.display(),
                    error = %err,
                    "config watch error"
                ),
            }
        }
    }))
}

async fn bind_listener(addr: SocketAddr) -> Result<BoundListener> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    let local_addr = listener.local_addr().context("get local_addr")?;
    Ok(BoundListener {
        configured_addr: addr,
        local_addr,
        listener,
    })
}

fn spawn_proxy_listener(runtime: RuntimeState, bound: BoundListener) -> ListenerHandle {
    let BoundListener {
        local_addr,
        listener,
        ..
    } = bound;
    let app = proxy::router(ProxyState {
        listen_addr: local_addr,
        runtime,
    });
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.await;
        })
        .await
        .context("proxy server exited")
    });

    ListenerHandle {
        local_addr,
        shutdown_tx,
        task,
    }
}

fn spawn_rpc_listener(runtime: RuntimeState, bound: BoundListener) -> ListenerHandle {
    let BoundListener {
        local_addr,
        listener,
        ..
    } = bound;
    let app = rpc::router(RpcState { runtime });
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.await;
        })
        .await
        .context("rpc server exited")
    });

    ListenerHandle {
        local_addr,
        shutdown_tx,
        task,
    }
}

fn event_targets_config(event: &Event, config_path: &Path, target_name: Option<&OsStr>) -> bool {
    if !matches!(
        event.kind,
        EventKind::Any | EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
    ) {
        return false;
    }

    event.paths.iter().any(|path| {
        path == config_path
            || target_name
                .map(|name| path.file_name().is_some_and(|file_name| file_name == name))
                .unwrap_or(false)
    })
}
