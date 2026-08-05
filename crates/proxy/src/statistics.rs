use std::{
    fs,
    net::SocketAddr,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use bytes::Bytes;
use codex_provider_proxy_rpc_types::{
    HourlyStatistics, LatencySummary, StatisticsBreakdown, StatisticsResponse, StatisticsSummary,
    StatisticsWindow, TokenUsageSummary,
};
use http::{HeaderMap, StatusCode};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;
use tracing::warn;

use crate::{config::StatisticsConfig, content_encoding};

const SCHEMA_VERSION: i64 = 1;
const HOUR_MS: u64 = 60 * 60 * 1000;

#[derive(Clone)]
pub struct StatisticsManager {
    inner: Arc<RwLock<ManagerState>>,
}

struct ManagerState {
    config: StatisticsConfig,
    store: Option<Arc<StatisticsStore>>,
}

pub(crate) struct PreparedStatisticsConfig(Option<ManagerState>);

pub struct StatisticsRequestContext<'a> {
    pub peer: SocketAddr,
    pub pid: Option<u32>,
    pub method: &'a str,
    pub path: &'a str,
}

#[derive(Clone)]
pub struct StatisticsTracker {
    store: Arc<StatisticsStore>,
    pending: Arc<Mutex<PendingExchange>>,
    finalized: Arc<AtomicBool>,
    request_body_max_bytes: usize,
    response_body_max_bytes: usize,
}

struct PendingExchange {
    started_unix_ms: u64,
    started: Instant,
    client_ip: String,
    client_port: u16,
    pid: Option<u32>,
    route_pid: Option<u32>,
    provider: String,
    method: String,
    path: String,
    attempts: u32,
    upstream_status: Option<u16>,
    downstream_status: Option<u16>,
    upstream_latency_ms: Option<u64>,
    send_error: Option<String>,
    response_headers: HeaderMap,
    request_body: BoundedCapture,
    response_body: BoundedCapture,
}

#[derive(Default)]
struct BoundedCapture {
    bytes: Vec<u8>,
    total_bytes: u64,
    truncated: bool,
}

#[derive(Default)]
struct UsageSnapshot {
    input_tokens: u64,
    output_tokens: u64,
    total_tokens: u64,
    cached_tokens: u64,
    cache_creation_tokens: u64,
    reasoning_tokens: u64,
}

struct ExchangeRecord {
    started_unix_ms: u64,
    completed_unix_ms: u64,
    client_ip: String,
    client_port: u16,
    pid: Option<u32>,
    route_pid: Option<u32>,
    provider: String,
    model: Option<String>,
    method: String,
    path: String,
    upstream_status: Option<u16>,
    downstream_status: Option<u16>,
    success: bool,
    error: Option<String>,
    upstream_latency_ms: Option<u64>,
    total_duration_ms: u64,
    attempts: u32,
    request_bytes: u64,
    response_bytes: u64,
    usage: UsageSnapshot,
    usage_found: bool,
}

struct StatisticsStore {
    connection: Mutex<Connection>,
}

impl StatisticsManager {
    pub fn new(config: &StatisticsConfig) -> Result<Self> {
        let store = open_store_if_enabled(config)?;
        Ok(Self {
            inner: Arc::new(RwLock::new(ManagerState {
                config: config.clone(),
                store,
            })),
        })
    }

    pub(crate) fn prepare_reconfigure(
        &self,
        config: &StatisticsConfig,
    ) -> Result<PreparedStatisticsConfig> {
        let state = self.inner.read().expect("statistics manager lock poisoned");
        if state.config == *config {
            return Ok(PreparedStatisticsConfig(None));
        }
        let store = if config.enabled
            && state.config.database_path == config.database_path
            && state.store.is_some()
        {
            state.store.clone()
        } else {
            open_store_if_enabled(config)?
        };
        Ok(PreparedStatisticsConfig(Some(ManagerState {
            config: config.clone(),
            store,
        })))
    }

    pub(crate) fn apply_reconfigure(&self, prepared: PreparedStatisticsConfig) {
        let Some(next_state) = prepared.0 else {
            return;
        };
        *self
            .inner
            .write()
            .expect("statistics manager lock poisoned") = next_state;
    }

    pub fn begin_request(
        &self,
        context: StatisticsRequestContext<'_>,
    ) -> Option<StatisticsTracker> {
        let state = self.inner.read().expect("statistics manager lock poisoned");
        let store = state.store.clone()?;
        Some(StatisticsTracker {
            store,
            pending: Arc::new(Mutex::new(PendingExchange {
                started_unix_ms: unix_ms_now(),
                started: Instant::now(),
                client_ip: context.peer.ip().to_string(),
                client_port: context.peer.port(),
                pid: context.pid,
                route_pid: None,
                provider: "(unresolved)".to_string(),
                method: context.method.to_string(),
                path: context.path.to_string(),
                attempts: 0,
                upstream_status: None,
                downstream_status: None,
                upstream_latency_ms: None,
                send_error: None,
                response_headers: HeaderMap::new(),
                request_body: BoundedCapture::default(),
                response_body: BoundedCapture::default(),
            })),
            finalized: Arc::new(AtomicBool::new(false)),
            request_body_max_bytes: state.config.request_body_max_bytes,
            response_body_max_bytes: state.config.response_body_max_bytes,
        })
    }

    pub async fn query(&self, hours: Option<u32>) -> Result<StatisticsResponse> {
        let store = {
            let state = self.inner.read().expect("statistics manager lock poisoned");
            state.store.clone()
        };
        let generated_unix_ms = unix_ms_now();
        let Some(store) = store else {
            return Ok(empty_response(false, generated_unix_ms, hours));
        };
        tokio::task::spawn_blocking(move || store.query(hours, generated_unix_ms))
            .await
            .context("join statistics query task")?
    }
}

impl StatisticsTracker {
    pub fn begin_attempt(&self, provider: &str, route_pid: Option<u32>) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.attempts = pending.attempts.saturating_add(1);
            pending.provider = provider.to_string();
            pending.route_pid = route_pid;
            pending.send_error = None;
        }
    }

    pub fn capture_request_chunk(&self, chunk: &Bytes) {
        if let Ok(mut pending) = self.pending.lock() {
            pending
                .request_body
                .push(chunk, self.request_body_max_bytes);
        }
    }

    pub fn capture_response_chunk(&self, chunk: &Bytes) {
        if let Ok(mut pending) = self.pending.lock() {
            pending
                .response_body
                .push(chunk, self.response_body_max_bytes);
        }
    }

    pub fn record_attempt_send_error(&self, error: &str) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.send_error = Some(error.to_string());
        }
    }

    pub fn record_response_stream_error(&self, error: &str) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.send_error = Some(format!("response stream: {error}"));
        }
    }

    pub fn record_response(
        &self,
        upstream_status: StatusCode,
        downstream_status: StatusCode,
        headers: &HeaderMap,
        upstream_latency_ms: u128,
    ) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.upstream_status = Some(upstream_status.as_u16());
            pending.downstream_status = Some(downstream_status.as_u16());
            pending.upstream_latency_ms = Some(saturating_u64(upstream_latency_ms));
            pending.response_headers = headers.clone();
            pending.send_error = None;
        }
    }

    pub fn finalize(&self) {
        if self.finalized.swap(true, Ordering::AcqRel) {
            return;
        }
        let store = self.store.clone();
        let pending = self.pending.clone();
        let work = move || {
            let record = {
                let pending = pending.lock().expect("statistics tracker lock poisoned");
                pending.to_record()
            };
            if let Err(err) = store.insert(&record) {
                warn!(error = %err, "failed to persist request statistics");
            }
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn_blocking(work);
        } else {
            work();
        }
    }
}

impl PendingExchange {
    fn to_record(&self) -> ExchangeRecord {
        let request_json = serde_json::from_slice::<Value>(&self.request_body.bytes).ok();
        let response_bytes = content_encoding::decode_content_encoded_body(
            &self.response_headers,
            &self.response_body.bytes,
        )
        .unwrap_or_else(|_| self.response_body.bytes.clone());
        let response_usage = extract_usage_and_model(&response_bytes);
        let request_model = request_json
            .as_ref()
            .and_then(|value| value.get("model"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let model = request_model.or(response_usage.1);
        let usage_found = response_usage.0.is_some();
        let usage = response_usage.0.unwrap_or_default();
        let success = self
            .upstream_status
            .is_some_and(|status| (200..300).contains(&status))
            && self.send_error.is_none();
        ExchangeRecord {
            started_unix_ms: self.started_unix_ms,
            completed_unix_ms: unix_ms_now(),
            client_ip: self.client_ip.clone(),
            client_port: self.client_port,
            pid: self.pid,
            route_pid: self.route_pid,
            provider: self.provider.clone(),
            model,
            method: self.method.clone(),
            path: self.path.clone(),
            upstream_status: self.upstream_status,
            downstream_status: self.downstream_status,
            success,
            error: self.send_error.clone(),
            upstream_latency_ms: self.upstream_latency_ms,
            total_duration_ms: saturating_u64(self.started.elapsed().as_millis()),
            attempts: self.attempts.max(1),
            request_bytes: self.request_body.total_bytes,
            response_bytes: self.response_body.total_bytes,
            usage,
            usage_found,
        }
    }
}

impl BoundedCapture {
    fn push(&mut self, chunk: &Bytes, max_bytes: usize) {
        self.total_bytes = self
            .total_bytes
            .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        let remaining = max_bytes.saturating_sub(self.bytes.len());
        let write_len = remaining.min(chunk.len());
        self.bytes.extend_from_slice(&chunk[..write_len]);
        self.truncated |= write_len < chunk.len();
    }
}

impl StatisticsStore {
    fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)
                .with_context(|| format!("create statistics directory {}", parent.display()))?;
        }
        let connection = Connection::open(path)
            .with_context(|| format!("open statistics database {}", path.display()))?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL);
             CREATE TABLE IF NOT EXISTS exchanges (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 started_unix_ms INTEGER NOT NULL,
                 completed_unix_ms INTEGER NOT NULL,
                 client_ip TEXT NOT NULL,
                 client_port INTEGER NOT NULL,
                 pid INTEGER,
                 route_pid INTEGER,
                 provider TEXT NOT NULL,
                 model TEXT,
                 method TEXT NOT NULL,
                 path TEXT NOT NULL,
                 upstream_status INTEGER,
                 downstream_status INTEGER,
                 success INTEGER NOT NULL,
                 error TEXT,
                 upstream_latency_ms INTEGER,
                 total_duration_ms INTEGER NOT NULL,
                 attempts INTEGER NOT NULL,
                 request_bytes INTEGER NOT NULL,
                 response_bytes INTEGER NOT NULL,
                 usage_found INTEGER NOT NULL,
                 input_tokens INTEGER NOT NULL,
                 output_tokens INTEGER NOT NULL,
                 total_tokens INTEGER NOT NULL,
                 cached_tokens INTEGER NOT NULL,
                 cache_creation_tokens INTEGER NOT NULL,
                 reasoning_tokens INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS exchanges_started_idx ON exchanges(started_unix_ms);
             CREATE INDEX IF NOT EXISTS exchanges_provider_started_idx ON exchanges(provider, started_unix_ms);
             CREATE INDEX IF NOT EXISTS exchanges_model_started_idx ON exchanges(model, started_unix_ms);
             CREATE INDEX IF NOT EXISTS exchanges_client_ip_started_idx ON exchanges(client_ip, started_unix_ms);",
        )?;
        let version: Option<i64> = connection
            .query_row("SELECT version FROM schema_version LIMIT 1", [], |row| {
                row.get(0)
            })
            .optional()?;
        match version {
            None => {
                connection.execute(
                    "INSERT INTO schema_version(version) VALUES (?1)",
                    [SCHEMA_VERSION],
                )?;
            }
            Some(version) if version == SCHEMA_VERSION => {}
            Some(version) => anyhow::bail!(
                "unsupported statistics schema version {version}; expected {SCHEMA_VERSION}"
            ),
        }
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    fn insert(&self, record: &ExchangeRecord) -> Result<()> {
        let connection = self
            .connection
            .lock()
            .expect("statistics database lock poisoned");
        connection.execute(
            "INSERT INTO exchanges (
                started_unix_ms, completed_unix_ms, client_ip, client_port, pid, route_pid,
                provider, model, method, path, upstream_status, downstream_status, success, error,
                upstream_latency_ms, total_duration_ms, attempts, request_bytes, response_bytes,
                usage_found, input_tokens, output_tokens, total_tokens, cached_tokens,
                cache_creation_tokens, reasoning_tokens
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16,
                ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26
             )",
            params![
                to_i64(record.started_unix_ms),
                to_i64(record.completed_unix_ms),
                record.client_ip,
                i64::from(record.client_port),
                record.pid.map(i64::from),
                record.route_pid.map(i64::from),
                record.provider,
                record.model,
                record.method,
                record.path,
                record.upstream_status.map(i64::from),
                record.downstream_status.map(i64::from),
                record.success,
                record.error,
                record.upstream_latency_ms.map(to_i64),
                to_i64(record.total_duration_ms),
                i64::from(record.attempts),
                to_i64(record.request_bytes),
                to_i64(record.response_bytes),
                record.usage_found,
                to_i64(record.usage.input_tokens),
                to_i64(record.usage.output_tokens),
                to_i64(record.usage.total_tokens),
                to_i64(record.usage.cached_tokens),
                to_i64(record.usage.cache_creation_tokens),
                to_i64(record.usage.reasoning_tokens),
            ],
        )?;
        Ok(())
    }

    fn query(&self, hours: Option<u32>, generated_unix_ms: u64) -> Result<StatisticsResponse> {
        let from_unix_ms = hours.map(|hours| {
            generated_unix_ms.saturating_sub(u64::from(hours).saturating_mul(HOUR_MS))
        });
        let connection = self
            .connection
            .lock()
            .expect("statistics database lock poisoned");
        let summary = query_summary(&connection, from_unix_ms)?;
        let tokens = query_tokens(&connection, from_unix_ms)?;
        let providers = query_breakdown(&connection, "provider", from_unix_ms, 100)?;
        let models = query_breakdown(
            &connection,
            "COALESCE(model, '(unknown)')",
            from_unix_ms,
            100,
        )?;
        let client_ips = query_breakdown(&connection, "client_ip", from_unix_ms, 100)?;
        let pids = query_breakdown(
            &connection,
            "COALESCE(CAST(pid AS TEXT), '(unresolved)')",
            from_unix_ms,
            100,
        )?;
        let route_pids = query_breakdown(
            &connection,
            "COALESCE(CAST(route_pid AS TEXT), '(default)')",
            from_unix_ms,
            100,
        )?;
        let status_codes = query_breakdown(
            &connection,
            "COALESCE(CAST(upstream_status AS TEXT), '(send_error)')",
            from_unix_ms,
            100,
        )?;
        let methods = query_breakdown(&connection, "method", from_unix_ms, 32)?;
        let paths = query_breakdown(&connection, "path", from_unix_ms, 100)?;
        let hourly = query_hourly(&connection, from_unix_ms)?;
        Ok(StatisticsResponse {
            enabled: true,
            generated_unix_ms,
            window: StatisticsWindow {
                hours,
                from_unix_ms,
                to_unix_ms: generated_unix_ms,
            },
            summary,
            tokens,
            providers,
            models,
            client_ips,
            pids,
            route_pids,
            status_codes,
            methods,
            paths,
            hourly,
        })
    }
}

fn open_store_if_enabled(config: &StatisticsConfig) -> Result<Option<Arc<StatisticsStore>>> {
    if !config.enabled {
        return Ok(None);
    }
    Ok(Some(Arc::new(StatisticsStore::open(
        &config.database_path,
    )?)))
}

fn query_summary(connection: &Connection, from: Option<u64>) -> Result<StatisticsSummary> {
    let (where_clause, from_value) = time_filter(from);
    let sql = format!(
        "SELECT COUNT(*), COALESCE(SUM(success), 0), COALESCE(SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(request_bytes), 0), COALESCE(SUM(response_bytes), 0), COALESCE(SUM(attempts), 0)
         FROM exchanges {where_clause}"
    );
    let (requests, successful, errors, request_bytes, response_bytes, attempts): (
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
    ) = connection.query_row(&sql, [from_value], |row| {
        Ok((
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
        ))
    })?;
    let requests = nonnegative_u64(requests);
    Ok(StatisticsSummary {
        requests,
        successful_requests: nonnegative_u64(successful),
        error_requests: nonnegative_u64(errors),
        error_rate: rate(nonnegative_u64(errors), requests),
        request_bytes: nonnegative_u64(request_bytes),
        response_bytes: nonnegative_u64(response_bytes),
        attempts: nonnegative_u64(attempts),
        average_attempts: average(nonnegative_u64(attempts), requests),
        upstream_latency_ms: query_latency(connection, "upstream_latency_ms", from)?,
        total_duration_ms: query_latency(connection, "total_duration_ms", from)?,
    })
}

fn query_tokens(connection: &Connection, from: Option<u64>) -> Result<TokenUsageSummary> {
    let (where_clause, from_value) = time_filter(from);
    let sql = format!(
        "SELECT COALESCE(SUM(usage_found), 0), COALESCE(SUM(input_tokens), 0),
                COALESCE(SUM(output_tokens), 0), COALESCE(SUM(total_tokens), 0),
                COALESCE(SUM(cached_tokens), 0), COALESCE(SUM(cache_creation_tokens), 0),
                COALESCE(SUM(reasoning_tokens), 0)
         FROM exchanges {where_clause}"
    );
    connection
        .query_row(&sql, [from_value], |row| {
            Ok(TokenUsageSummary {
                responses_with_usage: nonnegative_u64(row.get(0)?),
                input_tokens: nonnegative_u64(row.get(1)?),
                output_tokens: nonnegative_u64(row.get(2)?),
                total_tokens: nonnegative_u64(row.get(3)?),
                cached_tokens: nonnegative_u64(row.get(4)?),
                cache_creation_tokens: nonnegative_u64(row.get(5)?),
                reasoning_tokens: nonnegative_u64(row.get(6)?),
            })
        })
        .map_err(Into::into)
}

fn query_latency(
    connection: &Connection,
    column: &str,
    from: Option<u64>,
) -> Result<LatencySummary> {
    let (where_clause, from_value) = time_filter(from);
    let sql = format!(
        "SELECT COUNT({column}), COALESCE(AVG({column}), 0), COALESCE(MIN({column}), 0), COALESCE(MAX({column}), 0)
         FROM exchanges {where_clause} AND {column} IS NOT NULL"
    );
    let (count, average_value, min, max): (i64, f64, i64, i64) =
        connection.query_row(&sql, [from_value], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?;
    let count_u64 = nonnegative_u64(count);
    Ok(LatencySummary {
        count: count_u64,
        average: average_value,
        min: nonnegative_u64(min),
        p50: query_percentile(connection, column, from, count_u64, 0.50)?,
        p95: query_percentile(connection, column, from, count_u64, 0.95)?,
        p99: query_percentile(connection, column, from, count_u64, 0.99)?,
        max: nonnegative_u64(max),
    })
}

fn query_percentile(
    connection: &Connection,
    column: &str,
    from: Option<u64>,
    count: u64,
    percentile: f64,
) -> Result<u64> {
    if count == 0 {
        return Ok(0);
    }
    let offset = ((percentile * count as f64).ceil() as u64).saturating_sub(1);
    let (where_clause, from_value) = time_filter(from);
    let sql = format!(
        "SELECT {column} FROM exchanges {where_clause} AND {column} IS NOT NULL
         ORDER BY {column} LIMIT 1 OFFSET ?2"
    );
    let value: i64 =
        connection.query_row(&sql, params![from_value, to_i64(offset)], |row| row.get(0))?;
    Ok(nonnegative_u64(value))
}

fn query_breakdown(
    connection: &Connection,
    expression: &str,
    from: Option<u64>,
    limit: u32,
) -> Result<Vec<StatisticsBreakdown>> {
    let (where_clause, from_value) = time_filter(from);
    let sql = format!(
        "SELECT {expression} AS dimension, COUNT(*), COALESCE(SUM(success), 0),
                COALESCE(SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END), 0),
                COALESCE(AVG(total_duration_ms), 0), COALESCE(SUM(request_bytes), 0),
                COALESCE(SUM(response_bytes), 0), COALESCE(SUM(input_tokens), 0),
                COALESCE(SUM(output_tokens), 0), COALESCE(SUM(total_tokens), 0),
                COALESCE(SUM(cached_tokens), 0)
         FROM exchanges {where_clause}
         GROUP BY dimension ORDER BY COUNT(*) DESC, dimension LIMIT ?2"
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(params![from_value, limit], |row| {
        let requests = nonnegative_u64(row.get(1)?);
        let errors = nonnegative_u64(row.get(3)?);
        Ok(StatisticsBreakdown {
            key: row.get(0)?,
            requests,
            successful_requests: nonnegative_u64(row.get(2)?),
            error_requests: errors,
            error_rate: rate(errors, requests),
            average_response_time_ms: row.get(4)?,
            request_bytes: nonnegative_u64(row.get(5)?),
            response_bytes: nonnegative_u64(row.get(6)?),
            input_tokens: nonnegative_u64(row.get(7)?),
            output_tokens: nonnegative_u64(row.get(8)?),
            total_tokens: nonnegative_u64(row.get(9)?),
            cached_tokens: nonnegative_u64(row.get(10)?),
        })
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn query_hourly(connection: &Connection, from: Option<u64>) -> Result<Vec<HourlyStatistics>> {
    let (where_clause, from_value) = time_filter(from);
    let sql = format!(
        "SELECT (started_unix_ms / {HOUR_MS}) * {HOUR_MS} AS bucket, COUNT(*),
                COALESCE(SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END), 0),
                COALESCE(AVG(total_duration_ms), 0), COALESCE(SUM(total_tokens), 0)
         FROM exchanges {where_clause}
         GROUP BY bucket ORDER BY bucket"
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([from_value], |row| {
        let requests = nonnegative_u64(row.get(1)?);
        let errors = nonnegative_u64(row.get(2)?);
        Ok(HourlyStatistics {
            bucket_start_unix_ms: nonnegative_u64(row.get(0)?),
            requests,
            error_requests: errors,
            error_rate: rate(errors, requests),
            average_response_time_ms: row.get(3)?,
            total_tokens: nonnegative_u64(row.get(4)?),
        })
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn time_filter(from: Option<u64>) -> (&'static str, i64) {
    (
        "WHERE (?1 = 0 OR started_unix_ms >= ?1)",
        from.map(to_i64).unwrap_or(0),
    )
}

fn extract_usage_and_model(bytes: &[u8]) -> (Option<UsageSnapshot>, Option<String>) {
    if bytes.is_empty() {
        return (None, None);
    }
    if let Ok(value) = serde_json::from_slice::<Value>(bytes) {
        return usage_and_model_from_value(&value);
    }

    let text = String::from_utf8_lossy(bytes);
    let mut usage: Option<UsageSnapshot> = None;
    let mut model = None;
    for line in text.lines() {
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data.is_empty() || data == "[DONE]" {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(data) else {
            continue;
        };
        let (candidate_usage, candidate_model) = usage_and_model_from_value(&value);
        if let Some(candidate_usage) = candidate_usage {
            merge_usage(
                usage.get_or_insert_with(UsageSnapshot::default),
                candidate_usage,
            );
        }
        if model.is_none() {
            model = candidate_model;
        }
    }
    (usage, model)
}

fn usage_and_model_from_value(value: &Value) -> (Option<UsageSnapshot>, Option<String>) {
    let candidate = value.get("response").unwrap_or(value);
    let model = candidate
        .get("model")
        .or_else(|| value.pointer("/message/model"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let usage = candidate
        .get("usage")
        .or_else(|| value.pointer("/message/usage"))
        .and_then(parse_usage);
    (usage, model)
}

fn parse_usage(usage: &Value) -> Option<UsageSnapshot> {
    let object = usage.as_object()?;
    let input_tokens = value_u64(object.get("input_tokens"))
        .or_else(|| value_u64(object.get("prompt_tokens")))
        .unwrap_or(0);
    let output_tokens = value_u64(object.get("output_tokens"))
        .or_else(|| value_u64(object.get("completion_tokens")))
        .unwrap_or(0);
    let total_tokens = value_u64(object.get("total_tokens"))
        .unwrap_or_else(|| input_tokens.saturating_add(output_tokens));
    let cached_tokens = object
        .get("input_tokens_details")
        .or_else(|| object.get("prompt_tokens_details"))
        .and_then(|details| details.get("cached_tokens"))
        .and_then(Value::as_u64)
        .or_else(|| value_u64(object.get("cache_read_input_tokens")))
        .unwrap_or(0);
    let cache_creation_tokens = value_u64(object.get("cache_creation_input_tokens")).unwrap_or(0);
    let reasoning_tokens = object
        .get("output_tokens_details")
        .or_else(|| object.get("completion_tokens_details"))
        .and_then(|details| details.get("reasoning_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    Some(UsageSnapshot {
        input_tokens,
        output_tokens,
        total_tokens,
        cached_tokens,
        cache_creation_tokens,
        reasoning_tokens,
    })
}

fn merge_usage(current: &mut UsageSnapshot, candidate: UsageSnapshot) {
    current.input_tokens = current.input_tokens.max(candidate.input_tokens);
    current.output_tokens = current.output_tokens.max(candidate.output_tokens);
    current.total_tokens = current.total_tokens.max(candidate.total_tokens);
    current.cached_tokens = current.cached_tokens.max(candidate.cached_tokens);
    current.cache_creation_tokens = current
        .cache_creation_tokens
        .max(candidate.cache_creation_tokens);
    current.reasoning_tokens = current.reasoning_tokens.max(candidate.reasoning_tokens);
    current.total_tokens = current
        .total_tokens
        .max(current.input_tokens.saturating_add(current.output_tokens));
}

fn value_u64(value: Option<&Value>) -> Option<u64> {
    value.and_then(Value::as_u64)
}

fn empty_response(enabled: bool, generated_unix_ms: u64, hours: Option<u32>) -> StatisticsResponse {
    StatisticsResponse {
        enabled,
        generated_unix_ms,
        window: StatisticsWindow {
            hours,
            from_unix_ms: hours.map(|hours| {
                generated_unix_ms.saturating_sub(u64::from(hours).saturating_mul(HOUR_MS))
            }),
            to_unix_ms: generated_unix_ms,
        },
        summary: StatisticsSummary::default(),
        tokens: TokenUsageSummary::default(),
        providers: Vec::new(),
        models: Vec::new(),
        client_ips: Vec::new(),
        pids: Vec::new(),
        route_pids: Vec::new(),
        status_codes: Vec::new(),
        methods: Vec::new(),
        paths: Vec::new(),
        hourly: Vec::new(),
    }
}

fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| saturating_u64(duration.as_millis()))
        .unwrap_or(0)
}

fn saturating_u64(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn nonnegative_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

fn rate(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn average(total: u64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        total as f64 / count as f64
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, time::Duration};

    use serde_json::json;

    use super::{
        extract_usage_and_model, parse_usage, ExchangeRecord, StatisticsStore, UsageSnapshot,
    };

    #[test]
    fn extracts_openai_usage() {
        let usage = parse_usage(&json!({
            "input_tokens": 100,
            "output_tokens": 20,
            "total_tokens": 120,
            "input_tokens_details": {"cached_tokens": 40},
            "output_tokens_details": {"reasoning_tokens": 7}
        }))
        .unwrap();
        assert_eq!(usage.input_tokens, 100);
        assert_eq!(usage.cached_tokens, 40);
        assert_eq!(usage.reasoning_tokens, 7);
    }

    #[test]
    fn extracts_chat_completion_usage() {
        let usage = parse_usage(&json!({
            "prompt_tokens": 10,
            "completion_tokens": 5,
            "total_tokens": 15,
            "prompt_tokens_details": {"cached_tokens": 3}
        }))
        .unwrap();
        assert_eq!(usage.input_tokens, 10);
        assert_eq!(usage.output_tokens, 5);
        assert_eq!(usage.cached_tokens, 3);
    }

    #[test]
    fn extracts_sse_completed_usage_and_model() {
        let body = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-test\",\"usage\":{\"input_tokens\":10,\"output_tokens\":2}}}\n\n"
        );
        let (usage, model) = extract_usage_and_model(body.as_bytes());
        assert_eq!(model.as_deref(), Some("gpt-test"));
        assert_eq!(usage.unwrap().total_tokens, 12);
    }

    #[test]
    fn statistics_survive_database_reopen() {
        let path = std::env::temp_dir().join(format!(
            "codex-provider-proxy-statistics-{}-{}.sqlite3",
            std::process::id(),
            super::unix_ms_now()
        ));
        let record = ExchangeRecord {
            started_unix_ms: super::unix_ms_now(),
            completed_unix_ms: super::unix_ms_now(),
            client_ip: "127.0.0.1".to_string(),
            client_port: 50000,
            pid: Some(42),
            route_pid: Some(42),
            provider: "provider_a".to_string(),
            model: Some("gpt-test".to_string()),
            method: "POST".to_string(),
            path: "/v1/responses".to_string(),
            upstream_status: Some(200),
            downstream_status: Some(200),
            success: true,
            error: None,
            upstream_latency_ms: Some(100),
            total_duration_ms: 125,
            attempts: 1,
            request_bytes: 50,
            response_bytes: 100,
            usage: UsageSnapshot {
                input_tokens: 10,
                output_tokens: 5,
                total_tokens: 15,
                cached_tokens: 3,
                cache_creation_tokens: 0,
                reasoning_tokens: 2,
            },
            usage_found: true,
        };

        StatisticsStore::open(&path)
            .unwrap()
            .insert(&record)
            .unwrap();
        std::thread::sleep(Duration::from_millis(10));
        let report = StatisticsStore::open(&path)
            .unwrap()
            .query(Some(24), super::unix_ms_now())
            .unwrap();

        assert_eq!(report.summary.requests, 1);
        assert_eq!(report.tokens.total_tokens, 15);
        assert_eq!(report.providers[0].key, "provider_a");
        assert_eq!(report.models[0].key, "gpt-test");
        assert_eq!(report.client_ips[0].key, "127.0.0.1");

        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = fs::remove_file(path.with_extension("sqlite3-shm"));
    }
}
