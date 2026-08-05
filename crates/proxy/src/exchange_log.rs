use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::http::{HeaderMap, Method, StatusCode, Uri};
use bytes::Bytes;
use serde::Serialize;
use serde_json::{Map, Value};
use tracing::warn;
use url::Url;

use crate::{
    config::{BodyLogCompression, LoggingConfig},
    content_encoding,
};

pub type SharedExchangeFileLogger = Arc<Mutex<ExchangeFileLogger>>;
const EXCHANGE_LOG_SCHEMA_VERSION: u32 = 4;

pub struct ExchangeLogContext<'a> {
    pub request_id: u64,
    pub peer: SocketAddr,
    pub pid: Option<u32>,
    pub route_pid: Option<u32>,
    pub provider_name: &'a str,
    pub method: &'a Method,
    pub uri: &'a Uri,
    pub upstream_url: &'a Url,
    pub request_headers: &'a HeaderMap,
}

#[derive(Clone, Copy)]
pub struct AttemptRouteContext<'a> {
    pub route_pid: Option<u32>,
    pub provider_name: &'a str,
    pub upstream_url: &'a Url,
}

pub fn maybe_create_exchange_logger(
    cfg: &LoggingConfig,
    ctx: ExchangeLogContext<'_>,
) -> Option<SharedExchangeFileLogger> {
    let root_dir = cfg.exchange_log_dir.as_ref()?;

    let should_reconstruct =
        cfg.reconstruct_responses && path_supports_reconstruction(ctx.uri.path());
    match ExchangeFileLogger::new(
        root_dir,
        &ctx,
        should_reconstruct,
        cfg.exchange_body_max_bytes,
        cfg.exchange_body_compression,
    ) {
        Ok(logger) => Some(Arc::new(Mutex::new(logger))),
        Err(err) => {
            warn!(
                request_id = ctx.request_id,
                dir = %root_dir.display(),
                error = %err,
                "failed to initialize exchange file logger"
            );
            None
        }
    }
}

pub struct ExchangeFileLogger {
    request_id: u64,
    root_dir: PathBuf,
    stem: String,
    metadata_path: PathBuf,
    metadata: ExchangeMetadata,
    request_body: BodyLogSink,
    response_body: BodyLogSink,
    response_headers_path: PathBuf,
    response_content_encodings: Vec<String>,
    response_content_encoding_error: Option<String>,
    reconstructed_path: PathBuf,
    should_reconstruct: bool,
    body_compression: BodyLogCompression,
    active_attempt: Option<ActiveAttemptLog>,
}

impl ExchangeFileLogger {
    fn new(
        root_dir: &Path,
        ctx: &ExchangeLogContext<'_>,
        should_reconstruct: bool,
        body_max_bytes: Option<u64>,
        body_compression: BodyLogCompression,
    ) -> std::io::Result<Self> {
        fs::create_dir_all(root_dir)?;

        let now = now_unix_ms();
        let stem = format!("{now}_req_{}", ctx.request_id);
        let body_suffix = body_file_suffix(body_compression);

        let metadata_path = root_dir.join(format!("{stem}.meta.json"));
        let request_headers_path = root_dir.join(format!("{stem}.request_headers.txt"));
        let request_body_path = root_dir.join(format!("{stem}.request_body{body_suffix}"));
        let response_headers_path = root_dir.join(format!("{stem}.response_headers.txt"));
        let response_body_path = root_dir.join(format!("{stem}.response_body{body_suffix}"));
        let reconstructed_path = root_dir.join(format!("{stem}.response_reconstructed.txt"));

        let metadata = ExchangeMetadata {
            schema_version: EXCHANGE_LOG_SCHEMA_VERSION,
            request_id: ctx.request_id,
            started_unix_ms: now,
            peer: ctx.peer.to_string(),
            pid: ctx.pid,
            route_pid: ctx.route_pid,
            provider: ctx.provider_name.to_string(),
            method: ctx.method.to_string(),
            uri: ctx.uri.to_string(),
            upstream_url: ctx.upstream_url.to_string(),
            body_max_bytes,
            body_compression: body_compression.as_str().to_string(),
            should_reconstruct,
            response_status_code: None,
            response_status_text: None,
            upstream_latency_ms: None,
            completed_unix_ms: None,
            total_duration_ms: None,
            upstream_error: None,
            request_body_bytes: 0,
            response_body_bytes: 0,
            request_body_logged_bytes: 0,
            response_body_logged_bytes: 0,
            request_body_truncated: false,
            response_body_truncated: false,
            reconstruction_attempted: false,
            reconstruction_succeeded: None,
            reconstruction_error: None,
            attempts: Vec::new(),
            files: ExchangeMetadataFiles {
                request_headers: request_headers_path.display().to_string(),
                request_body: request_body_path.display().to_string(),
                response_headers: response_headers_path.display().to_string(),
                response_body: response_body_path.display().to_string(),
                reconstructed_response: if should_reconstruct {
                    Some(reconstructed_path.display().to_string())
                } else {
                    None
                },
            },
        };
        let metadata_json = serde_json::to_vec_pretty(&metadata)?;
        fs::write(&metadata_path, metadata_json)?;
        write_request_headers_file(
            &request_headers_path,
            ctx.method,
            ctx.uri,
            ctx.request_headers,
        )?;

        Ok(Self {
            request_id: ctx.request_id,
            root_dir: root_dir.to_path_buf(),
            stem,
            metadata_path,
            metadata,
            request_body: BodyLogSink::new(
                request_body_path,
                body_compression,
                body_max_bytes,
                "request body",
            )?,
            response_body: BodyLogSink::new(
                response_body_path,
                body_compression,
                body_max_bytes,
                "response body",
            )?,
            response_headers_path,
            response_content_encodings: Vec::new(),
            response_content_encoding_error: None,
            reconstructed_path,
            should_reconstruct,
            body_compression,
            active_attempt: None,
        })
    }

    pub fn begin_attempt(
        &mut self,
        attempt_number: u32,
        route: AttemptRouteContext<'_>,
        method: &Method,
        headers: &HeaderMap,
        request_body: Option<&Bytes>,
    ) {
        if let Some(active_attempt) = self.active_attempt.take() {
            self.finish_attempt_body_writers(active_attempt);
        }

        let request_headers_path = self.root_dir.join(format!(
            "{}.attempt_{}.request_headers.txt",
            self.stem, attempt_number
        ));
        let request_body_path = self.root_dir.join(format!(
            "{}.attempt_{}.request_body{}",
            self.stem,
            attempt_number,
            body_file_suffix(self.body_compression)
        ));
        let response_headers_path = self.root_dir.join(format!(
            "{}.attempt_{}.response_headers.txt",
            self.stem, attempt_number
        ));
        let response_body_path = self.root_dir.join(format!(
            "{}.attempt_{}.response_body{}",
            self.stem,
            attempt_number,
            body_file_suffix(self.body_compression)
        ));

        if let Err(err) = write_attempt_request_headers_file(
            &request_headers_path,
            attempt_number,
            &route,
            method,
            headers,
        ) {
            warn!(
                request_id = self.request_id,
                attempt = attempt_number,
                path = %request_headers_path.display(),
                error = %err,
                "failed to write attempt request headers log"
            );
        }

        let request_body_sink = BodyLogSink::new_best_effort(
            self.request_id,
            attempt_number,
            request_body_path.clone(),
            self.body_compression,
            self.request_body.max_bytes,
            "attempt request body",
        );
        let response_body_sink = BodyLogSink::new_best_effort(
            self.request_id,
            attempt_number,
            response_body_path.clone(),
            self.body_compression,
            self.response_body.max_bytes,
            "attempt response body",
        );

        self.metadata.apply_route(route);
        self.metadata.attempts.push(ExchangeAttemptMetadata {
            attempt: attempt_number,
            is_final: false,
            route_pid: route.route_pid,
            provider: route.provider_name.to_string(),
            upstream_url: route.upstream_url.to_string(),
            response_status_code: None,
            response_status_text: None,
            upstream_latency_ms: None,
            upstream_error: None,
            request_body_bytes: None,
            request_body_logged_bytes: None,
            request_body_truncated: None,
            response_body_bytes: None,
            response_body_logged_bytes: None,
            response_body_truncated: None,
            request_headers: request_headers_path.display().to_string(),
            request_body: request_body_path.display().to_string(),
            response_headers: response_headers_path.display().to_string(),
            response_body: response_body_path.display().to_string(),
        });

        self.active_attempt = Some(ActiveAttemptLog {
            attempt_number,
            request_body: request_body_sink,
            response_body: response_body_sink,
        });

        if let Some(request_body) = request_body {
            self.on_active_attempt_request_body_chunk(request_body);
        }

        self.persist_metadata_best_effort("begin attempt metadata");
    }

    pub fn on_request_body_chunk(&mut self, chunk: &Bytes) {
        self.request_body.append(self.request_id, chunk);
        self.on_active_attempt_request_body_chunk(chunk);
    }

    pub fn on_response_body_chunk(&mut self, chunk: &Bytes) {
        self.response_body.append(self.request_id, chunk);
        self.on_active_attempt_response_body_chunk(chunk);
    }

    pub fn record_attempt(
        &mut self,
        attempt_number: u32,
        route: AttemptRouteContext<'_>,
        status: StatusCode,
        headers: &HeaderMap,
        latency_ms: u128,
        is_final: bool,
    ) {
        let status_text = status.canonical_reason().unwrap_or("unknown");
        let attempt_headers_path = self.root_dir.join(format!(
            "{}.attempt_{}.response_headers.txt",
            self.stem, attempt_number
        ));

        let mut body = format!(
            "attempt: {}\nprovider: {}\nupstream_url: {}\nstatus: {} {}\nlatency_ms: {}\n",
            attempt_number,
            route.provider_name,
            route.upstream_url,
            status.as_u16(),
            status_text,
            latency_ms
        );
        if let Some(route_pid) = route.route_pid {
            body.push_str(&format!("route_pid: {route_pid}\n"));
        }
        if let Some(attempt_meta) = self.attempt_mut(attempt_number) {
            if let Some(bytes) = attempt_meta.request_body_bytes {
                body.push_str(&format!("request_body_bytes: {bytes}\n"));
            }
            if let Some(bytes) = attempt_meta.response_body_bytes {
                body.push_str(&format!("response_body_bytes: {bytes}\n"));
            }
        }
        body.push_str(&format_headers(headers));

        if let Err(err) = fs::write(&attempt_headers_path, body.as_bytes()) {
            warn!(
                request_id = self.request_id,
                attempt = attempt_number,
                path = %attempt_headers_path.display(),
                error = %err,
                "failed to write retry attempt headers log"
            );
        }

        if let Some(attempt_meta) = self.attempt_mut(attempt_number) {
            attempt_meta.is_final = is_final;
            attempt_meta.apply_route(route);
            attempt_meta.response_status_code = Some(status.as_u16());
            attempt_meta.response_status_text = Some(status_text.to_string());
            attempt_meta.upstream_latency_ms = Some(latency_ms);
            attempt_meta.response_headers = attempt_headers_path.display().to_string();
        }

        self.persist_metadata_best_effort("write attempt metadata");
    }

    pub fn record_attempt_send_error(
        &mut self,
        attempt_number: u32,
        latency_ms: u128,
        err: &str,
        is_final: bool,
    ) {
        self.finish_active_attempt_if_matching(attempt_number);
        if let Some(attempt_meta) = self.attempt_mut(attempt_number) {
            attempt_meta.is_final = is_final;
            attempt_meta.upstream_latency_ms = Some(latency_ms);
            attempt_meta.upstream_error = Some(truncate_meta_error(err));
        }
        self.persist_metadata_best_effort("write attempt send error metadata");
    }

    pub fn on_attempt_response_body_chunk(&mut self, attempt_number: u32, chunk: &Bytes) {
        if self
            .active_attempt
            .as_ref()
            .is_some_and(|attempt| attempt.attempt_number == attempt_number)
        {
            self.on_active_attempt_response_body_chunk(chunk);
        }
    }

    pub fn finish_attempt(&mut self, attempt_number: u32) {
        self.finish_active_attempt_if_matching(attempt_number);
        self.persist_metadata_best_effort("finish attempt metadata");
    }

    pub fn update_upstream_target(&mut self, route: AttemptRouteContext<'_>) {
        self.metadata.apply_route(route);
        self.persist_metadata_best_effort("update upstream target metadata");
    }

    pub fn write_response_headers(
        &mut self,
        route: AttemptRouteContext<'_>,
        status: StatusCode,
        headers: &HeaderMap,
        latency_ms: u128,
    ) {
        match content_encoding::content_encodings(headers) {
            Ok(encodings) => {
                self.response_content_encodings = encodings;
                self.response_content_encoding_error = None;
            }
            Err(err) => {
                self.response_content_encodings.clear();
                self.response_content_encoding_error = Some(err.to_string());
            }
        }
        self.metadata.apply_route(route);
        let status_text = status.canonical_reason().unwrap_or("unknown");
        let mut body = format!(
            "status: {} {}\nlatency_ms: {}\n",
            status.as_u16(),
            status_text,
            latency_ms
        );
        body.push_str(&format_headers(headers));
        if let Err(err) = fs::write(&self.response_headers_path, body.as_bytes()) {
            warn!(
                request_id = self.request_id,
                path = %self.response_headers_path.display(),
                error = %err,
                "failed to write response headers log"
            );
        }
        self.metadata.response_status_code = Some(status.as_u16());
        self.metadata.response_status_text = Some(status_text.to_string());
        self.metadata.upstream_latency_ms = Some(latency_ms);
        self.metadata.upstream_error = None;
        self.persist_metadata_best_effort("write response metadata");
    }

    pub fn mark_upstream_send_error(&mut self, latency_ms: u128, err: &str) {
        if let Some(attempt_number) = self
            .active_attempt
            .as_ref()
            .map(|attempt| attempt.attempt_number)
        {
            self.finish_active_attempt_if_matching(attempt_number);
            if let Some(attempt_meta) = self.attempt_mut(attempt_number) {
                attempt_meta.is_final = true;
                attempt_meta.upstream_latency_ms = Some(latency_ms);
                attempt_meta.upstream_error = Some(truncate_meta_error(err));
            }
        }
        self.metadata.upstream_latency_ms = Some(latency_ms);
        self.metadata.upstream_error = Some(truncate_meta_error(err));
        self.persist_metadata_best_effort("write upstream error metadata");
    }

    pub fn finalize(&mut self) {
        if let Some(active_attempt) = self.active_attempt.take() {
            self.finish_attempt_body_writers(active_attempt);
        }
        self.request_body.finish(self.request_id);
        self.response_body.finish(self.request_id);

        let request_body = self.request_body.snapshot();
        let response_body = self.response_body.snapshot();
        self.metadata.request_body_bytes = request_body.bytes;
        self.metadata.response_body_bytes = response_body.bytes;
        self.metadata.request_body_logged_bytes = request_body.logged_bytes;
        self.metadata.response_body_logged_bytes = response_body.logged_bytes;
        self.metadata.request_body_truncated = request_body.truncated;
        self.metadata.response_body_truncated = response_body.truncated;
        let completed_unix_ms = now_unix_ms();
        self.metadata.completed_unix_ms = Some(completed_unix_ms);
        self.metadata.total_duration_ms =
            Some(completed_unix_ms.saturating_sub(self.metadata.started_unix_ms));
        if let Some(final_attempt) = self.metadata.attempts.iter_mut().rev().find(|a| a.is_final) {
            final_attempt.response_body_bytes = Some(response_body.bytes);
            final_attempt.response_body_logged_bytes = Some(response_body.logged_bytes);
            final_attempt.response_body_truncated = Some(response_body.truncated);
        }

        if self.should_reconstruct {
            self.metadata.reconstruction_attempted = true;
            if let Err(err) = self.reconstruct_and_write() {
                self.metadata.reconstruction_succeeded = Some(false);
                self.metadata.reconstruction_error = Some(truncate_meta_error(&err.to_string()));
                warn!(
                    request_id = self.request_id,
                    response_body = %self.response_body.path.display(),
                    reconstructed = %self.reconstructed_path.display(),
                    error = %err,
                    "response reconstruction failed; proxy response was unaffected"
                );
            } else {
                self.metadata.reconstruction_succeeded = Some(true);
                self.metadata.reconstruction_error = None;
            }
        }

        self.persist_metadata_best_effort("finalize exchange metadata");
    }

    fn attempt_mut(&mut self, attempt_number: u32) -> Option<&mut ExchangeAttemptMetadata> {
        self.metadata
            .attempts
            .iter_mut()
            .find(|attempt| attempt.attempt == attempt_number)
    }

    fn on_active_attempt_request_body_chunk(&mut self, chunk: &Bytes) {
        let Some(active_attempt) = self.active_attempt.as_mut() else {
            return;
        };
        active_attempt.request_body.append(self.request_id, chunk);
    }

    fn on_active_attempt_response_body_chunk(&mut self, chunk: &Bytes) {
        let Some(active_attempt) = self.active_attempt.as_mut() else {
            return;
        };
        active_attempt.response_body.append(self.request_id, chunk);
    }

    fn finish_active_attempt_if_matching(&mut self, attempt_number: u32) {
        let Some(active_attempt) = self.active_attempt.take() else {
            return;
        };
        if active_attempt.attempt_number == attempt_number {
            self.finish_attempt_body_writers(active_attempt);
        } else {
            self.active_attempt = Some(active_attempt);
        }
    }

    fn finish_attempt_body_writers(&mut self, mut active_attempt: ActiveAttemptLog) {
        active_attempt.request_body.finish(self.request_id);
        active_attempt.response_body.finish(self.request_id);
        let request_body = active_attempt.request_body.snapshot();
        let response_body = active_attempt.response_body.snapshot();

        if let Some(attempt_meta) = self.attempt_mut(active_attempt.attempt_number) {
            attempt_meta.request_body_bytes = Some(request_body.bytes);
            attempt_meta.request_body_logged_bytes = Some(request_body.logged_bytes);
            attempt_meta.request_body_truncated = Some(request_body.truncated);
            attempt_meta.response_body_bytes = Some(response_body.bytes);
            attempt_meta.response_body_logged_bytes = Some(response_body.logged_bytes);
            attempt_meta.response_body_truncated = Some(response_body.truncated);
        }
    }

    fn reconstruct_and_write(&self) -> std::io::Result<()> {
        let raw = read_logged_body_file(&self.response_body.path, self.body_compression)?;
        if raw.is_empty() {
            return Ok(());
        }
        if self.response_body.truncated && !self.response_content_encodings.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "cannot decode a truncated content-encoded response body",
            ));
        }
        if let Some(err) = &self.response_content_encoding_error {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("cannot decode response body: {err}"),
            ));
        }
        let decoded =
            content_encoding::decode_content_encodings(&raw, &self.response_content_encodings)?;
        let decoded_text = String::from_utf8(decoded).map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("decoded response body is not valid UTF-8: {err}"),
            )
        })?;
        let reconstructed = reconstruct_response_payload(&decoded_text);
        fs::write(&self.reconstructed_path, reconstructed.as_bytes())
    }

    fn persist_metadata_best_effort(&self, action: &'static str) {
        match serde_json::to_vec_pretty(&self.metadata) {
            Ok(bytes) => {
                if let Err(err) = fs::write(&self.metadata_path, bytes) {
                    warn!(
                        request_id = self.request_id,
                        path = %self.metadata_path.display(),
                        action,
                        error = %err,
                        "failed to write exchange metadata"
                    );
                }
            }
            Err(err) => {
                warn!(
                    request_id = self.request_id,
                    path = %self.metadata_path.display(),
                    action,
                    error = %err,
                    "failed to serialize exchange metadata"
                );
            }
        }
    }
}

#[derive(Debug, Serialize)]
struct ExchangeMetadata {
    schema_version: u32,
    request_id: u64,
    started_unix_ms: u128,
    peer: String,
    pid: Option<u32>,
    route_pid: Option<u32>,
    provider: String,
    method: String,
    uri: String,
    upstream_url: String,
    body_max_bytes: Option<u64>,
    body_compression: String,
    should_reconstruct: bool,
    response_status_code: Option<u16>,
    response_status_text: Option<String>,
    upstream_latency_ms: Option<u128>,
    completed_unix_ms: Option<u128>,
    total_duration_ms: Option<u128>,
    upstream_error: Option<String>,
    request_body_bytes: u64,
    response_body_bytes: u64,
    request_body_logged_bytes: u64,
    response_body_logged_bytes: u64,
    request_body_truncated: bool,
    response_body_truncated: bool,
    reconstruction_attempted: bool,
    reconstruction_succeeded: Option<bool>,
    reconstruction_error: Option<String>,
    attempts: Vec<ExchangeAttemptMetadata>,
    files: ExchangeMetadataFiles,
}

impl ExchangeMetadata {
    fn apply_route(&mut self, route: AttemptRouteContext<'_>) {
        self.route_pid = route.route_pid;
        self.provider = route.provider_name.to_string();
        self.upstream_url = route.upstream_url.to_string();
    }
}

#[derive(Debug, Serialize)]
struct ExchangeMetadataFiles {
    request_headers: String,
    request_body: String,
    response_headers: String,
    response_body: String,
    reconstructed_response: Option<String>,
}

#[derive(Debug, Serialize)]
struct ExchangeAttemptMetadata {
    attempt: u32,
    is_final: bool,
    route_pid: Option<u32>,
    provider: String,
    upstream_url: String,
    response_status_code: Option<u16>,
    response_status_text: Option<String>,
    upstream_latency_ms: Option<u128>,
    upstream_error: Option<String>,
    request_body_bytes: Option<u64>,
    request_body_logged_bytes: Option<u64>,
    request_body_truncated: Option<bool>,
    response_body_bytes: Option<u64>,
    response_body_logged_bytes: Option<u64>,
    response_body_truncated: Option<bool>,
    request_headers: String,
    request_body: String,
    response_headers: String,
    response_body: String,
}

impl ExchangeAttemptMetadata {
    fn apply_route(&mut self, route: AttemptRouteContext<'_>) {
        self.route_pid = route.route_pid;
        self.provider = route.provider_name.to_string();
        self.upstream_url = route.upstream_url.to_string();
    }
}

struct ActiveAttemptLog {
    attempt_number: u32,
    request_body: BodyLogSink,
    response_body: BodyLogSink,
}

#[derive(Clone, Copy)]
struct BodyLogSnapshot {
    bytes: u64,
    logged_bytes: u64,
    truncated: bool,
}

struct BodyLogSink {
    path: PathBuf,
    writer: Option<BodyLogWriter>,
    max_bytes: Option<u64>,
    bytes: u64,
    logged_bytes: u64,
    truncated: bool,
    kind: &'static str,
}

impl BodyLogSink {
    fn new(
        path: PathBuf,
        compression: BodyLogCompression,
        max_bytes: Option<u64>,
        kind: &'static str,
    ) -> std::io::Result<Self> {
        let writer = create_body_writer(&path, compression)?;
        Ok(Self {
            path,
            writer: Some(writer),
            max_bytes,
            bytes: 0,
            logged_bytes: 0,
            truncated: false,
            kind,
        })
    }

    fn new_best_effort(
        request_id: u64,
        attempt_number: u32,
        path: PathBuf,
        compression: BodyLogCompression,
        max_bytes: Option<u64>,
        kind: &'static str,
    ) -> Self {
        let mut sink = Self {
            path,
            writer: None,
            max_bytes,
            bytes: 0,
            logged_bytes: 0,
            truncated: false,
            kind,
        };
        match create_body_writer(&sink.path, compression) {
            Ok(writer) => sink.writer = Some(writer),
            Err(err) => {
                warn!(
                    request_id,
                    attempt = attempt_number,
                    path = %sink.path.display(),
                    error = %err,
                    kind,
                    "failed to create exchange body log"
                );
            }
        }
        sink
    }

    fn append(&mut self, request_id: u64, chunk: &Bytes) {
        self.bytes = self.bytes.saturating_add(chunk.len() as u64);
        let (write_len, truncated) =
            limited_chunk_len(self.max_bytes, self.logged_bytes, chunk.len());
        self.truncated |= truncated;
        self.logged_bytes = self.logged_bytes.saturating_add(write_len as u64);

        if write_len == 0 {
            return;
        }
        let Some(writer) = self.writer.as_mut() else {
            return;
        };
        if let Err(err) = writer
            .write_all(&chunk[..write_len])
            .and_then(|_| writer.flush())
        {
            warn!(
                request_id,
                path = %self.path.display(),
                kind = self.kind,
                error = %err,
                "failed to append or flush exchange log chunk"
            );
            self.writer = None;
        }
    }

    fn finish(&mut self, request_id: u64) {
        let Some(writer) = self.writer.take() else {
            return;
        };
        if let Err(err) = writer.finish() {
            warn!(
                request_id,
                path = %self.path.display(),
                kind = self.kind,
                error = %err,
                "failed to flush exchange log file"
            );
        }
    }

    fn snapshot(&self) -> BodyLogSnapshot {
        BodyLogSnapshot {
            bytes: self.bytes,
            logged_bytes: self.logged_bytes,
            truncated: self.truncated,
        }
    }
}

enum BodyLogWriter {
    Plain(BufWriter<File>),
    Zstd(zstd::stream::write::Encoder<'static, BufWriter<File>>),
}

impl BodyLogWriter {
    fn write_all(&mut self, chunk: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Plain(writer) => writer.write_all(chunk),
            Self::Zstd(writer) => writer.write_all(chunk),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(writer) => writer.flush(),
            Self::Zstd(writer) => writer.flush(),
        }
    }

    fn finish(self) -> std::io::Result<()> {
        match self {
            Self::Plain(mut writer) => writer.flush(),
            Self::Zstd(writer) => {
                let mut inner = writer.finish()?;
                inner.flush()
            }
        }
    }
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn truncate_meta_error(value: &str) -> String {
    const MAX_CHARS: usize = 512;
    if value.chars().count() <= MAX_CHARS {
        return value.to_string();
    }
    let truncated: String = value.chars().take(MAX_CHARS).collect();
    format!("{truncated}...[truncated]")
}

fn write_request_headers_file(
    path: &Path,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
) -> std::io::Result<()> {
    let mut body = format!("method: {method}\nuri: {uri}\n");
    body.push_str(&format_headers(headers));
    fs::write(path, body.as_bytes())
}

fn write_attempt_request_headers_file(
    path: &Path,
    attempt_number: u32,
    route: &AttemptRouteContext<'_>,
    method: &Method,
    headers: &HeaderMap,
) -> std::io::Result<()> {
    let mut body = format!(
        "attempt: {attempt_number}\nprovider: {}\nupstream_url: {}\nmethod: {method}\n",
        route.provider_name, route.upstream_url
    );
    if let Some(route_pid) = route.route_pid {
        body.push_str(&format!("route_pid: {route_pid}\n"));
    }
    body.push_str(&format_headers(headers));
    fs::write(path, body.as_bytes())
}

fn format_headers(headers: &HeaderMap) -> String {
    let mut body = String::new();
    for (name, value) in headers.iter() {
        let value = value
            .to_str()
            .map(str::to_string)
            .unwrap_or_else(|_| String::from_utf8_lossy(value.as_bytes()).to_string());
        body.push_str(name.as_str());
        body.push_str(": ");
        body.push_str(&value);
        body.push('\n');
    }
    body
}

fn create_body_writer(
    path: &Path,
    compression: BodyLogCompression,
) -> std::io::Result<BodyLogWriter> {
    let file = File::create(path)?;
    let writer = BufWriter::new(file);
    match compression {
        BodyLogCompression::None => Ok(BodyLogWriter::Plain(writer)),
        BodyLogCompression::Zstd => {
            let encoder = zstd::stream::write::Encoder::new(writer, 3)?;
            Ok(BodyLogWriter::Zstd(encoder))
        }
    }
}

fn body_file_suffix(compression: BodyLogCompression) -> &'static str {
    match compression {
        BodyLogCompression::None => ".bin",
        BodyLogCompression::Zstd => ".bin.zst",
    }
}

fn limited_chunk_len(max_bytes: Option<u64>, logged_bytes: u64, chunk_len: usize) -> (usize, bool) {
    let Some(max_bytes) = max_bytes else {
        return (chunk_len, false);
    };
    let remaining = max_bytes.saturating_sub(logged_bytes);
    let remaining_usize = remaining as usize;
    let write_len = remaining_usize.min(chunk_len);
    (write_len, write_len < chunk_len)
}

fn read_logged_body_file(path: &Path, compression: BodyLogCompression) -> std::io::Result<Vec<u8>> {
    match compression {
        BodyLogCompression::None => fs::read(path),
        BodyLogCompression::Zstd => {
            let file = File::open(path)?;
            zstd::stream::decode_all(file)
        }
    }
}

fn looks_like_sse(payload: &str) -> bool {
    payload.lines().any(|line| {
        let line = line.trim_start();
        line.starts_with("event:") || line.starts_with("data:")
    })
}

fn reconstruct_response_payload(payload: &str) -> String {
    if !looks_like_sse(payload) {
        return payload.to_string();
    }

    reconstruct_chat_completion_from_sse(payload)
        .or_else(|| reconstruct_openai_response_from_sse(payload))
        .or_else(|| reconstruct_anthropic_message_from_sse(payload))
        .unwrap_or_else(|| payload.to_string())
}

fn reconstruct_chat_completion_from_sse(payload: &str) -> Option<String> {
    let events = parse_sse_events(payload);
    if events.is_empty() {
        return None;
    }

    let mut has_chat_completion_chunks = false;
    let mut completion = Map::new();
    let mut choices: Vec<Option<Map<String, Value>>> = Vec::new();

    for event in events {
        let data = event.data.trim();
        if data.is_empty() || data == "[DONE]" {
            continue;
        }
        let Ok(json) = serde_json::from_str::<Value>(data) else {
            continue;
        };
        if !is_chat_completion_chunk(&json) {
            continue;
        }

        has_chat_completion_chunks = true;
        copy_chat_completion_metadata(&mut completion, &json);
        if let Some(usage) = json.get("usage") {
            completion.insert("usage".to_string(), usage.clone());
        }

        let Some(chunk_choices) = json.get("choices").and_then(Value::as_array) else {
            continue;
        };
        for chunk_choice in chunk_choices {
            let Some(index) = event_index(chunk_choice) else {
                continue;
            };
            ensure_slot(&mut choices, index);
            let choice = choices[index].get_or_insert_with(|| {
                let mut choice = Map::new();
                choice.insert("index".to_string(), Value::from(index));
                choice.insert("message".to_string(), Value::Object(Map::new()));
                choice
            });

            if let Some(delta) = chunk_choice.get("delta").and_then(Value::as_object) {
                merge_chat_completion_delta(choice, delta);
            }
            for field in ["finish_reason", "logprobs"] {
                if let Some(value) = chunk_choice.get(field) {
                    choice.insert(field.to_string(), value.clone());
                }
            }
        }
    }

    if !has_chat_completion_chunks {
        return None;
    }

    completion.insert(
        "object".to_string(),
        Value::String("chat.completion".to_string()),
    );
    completion.insert(
        "choices".to_string(),
        Value::Array(choices.into_iter().flatten().map(Value::Object).collect()),
    );

    let completion = Value::Object(completion);
    Some(serde_json::to_string_pretty(&completion).unwrap_or_else(|_| completion.to_string()))
}

fn is_chat_completion_chunk(json: &Value) -> bool {
    json.get("object").and_then(Value::as_str) == Some("chat.completion.chunk")
        || json
            .get("choices")
            .and_then(Value::as_array)
            .is_some_and(|choices| choices.iter().any(|choice| choice.get("delta").is_some()))
}

fn copy_chat_completion_metadata(completion: &mut Map<String, Value>, chunk: &Value) {
    for field in [
        "id",
        "created",
        "model",
        "service_tier",
        "system_fingerprint",
    ] {
        if let Some(value) = chunk.get(field) {
            completion.insert(field.to_string(), value.clone());
        }
    }
}

fn ensure_slot<T: Default>(vec: &mut Vec<T>, index: usize) {
    if vec.len() <= index {
        vec.resize_with(index + 1, T::default);
    }
}

fn merge_chat_completion_delta(choice: &mut Map<String, Value>, delta: &Map<String, Value>) {
    let message = choice
        .entry("message".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    let Some(message) = message.as_object_mut() else {
        return;
    };

    for (field, value) in delta {
        match field.as_str() {
            "content" | "refusal" | "reasoning_content" => {
                append_json_string(message, field, value);
            }
            "function_call" => merge_append_field(message, "function_call", value),
            "tool_calls" => merge_tool_calls(message, value),
            _ => {
                message.insert(field.clone(), value.clone());
            }
        }
    }
}

fn append_json_string(object: &mut Map<String, Value>, field: &str, value: &Value) {
    let Some(delta) = value.as_str() else {
        object.insert(field.to_string(), value.clone());
        return;
    };
    let current = object
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_default();
    let mut combined = String::with_capacity(current.len() + delta.len());
    combined.push_str(current);
    combined.push_str(delta);
    object.insert(field.to_string(), Value::String(combined));
}

fn merge_append_field(parent: &mut Map<String, Value>, key: &str, value: &Value) {
    let Some(delta) = value.as_object() else {
        parent.insert(key.to_string(), value.clone());
        return;
    };
    let object = parent
        .entry(key.to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    let Some(object) = object.as_object_mut() else {
        return;
    };
    for (field, value) in delta {
        if matches!(field.as_str(), "name" | "arguments") {
            append_json_string(object, field, value);
        } else {
            object.insert(field.clone(), value.clone());
        }
    }
}

fn merge_tool_calls(message: &mut Map<String, Value>, value: &Value) {
    let Some(delta_tool_calls) = value.as_array() else {
        message.insert("tool_calls".to_string(), value.clone());
        return;
    };
    let tool_calls = message
        .entry("tool_calls".to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
    let Some(tool_calls) = tool_calls.as_array_mut() else {
        return;
    };

    for (position, delta_tool_call) in delta_tool_calls.iter().enumerate() {
        let Some(delta_tool_call) = delta_tool_call.as_object() else {
            continue;
        };
        let index = delta_tool_call
            .get("index")
            .and_then(Value::as_u64)
            .and_then(|index| usize::try_from(index).ok())
            .unwrap_or(position);
        if tool_calls.len() <= index {
            tool_calls.resize_with(index + 1, || Value::Object(Map::new()));
        }
        let Some(tool_call) = tool_calls[index].as_object_mut() else {
            continue;
        };

        for (field, value) in delta_tool_call {
            if field == "function" {
                merge_tool_function(tool_call, value);
            } else if field != "index" {
                tool_call.insert(field.clone(), value.clone());
            }
        }
    }
}

fn merge_tool_function(tool_call: &mut Map<String, Value>, value: &Value) {
    merge_append_field(tool_call, "function", value);
}

fn reconstruct_openai_response_from_sse(payload: &str) -> Option<String> {
    let events = parse_sse_events(payload);
    if events.is_empty() {
        return None;
    }

    let mut completed_response: Option<Value> = None;
    let mut text_deltas = String::new();
    let mut plain_data_chunks = Vec::new();

    for event in events {
        let data = event.data.trim();
        if data.is_empty() || data == "[DONE]" {
            continue;
        }

        match serde_json::from_str::<Value>(data) {
            Ok(json) => {
                if let Some(response) = json.get("response") {
                    completed_response = Some(response.clone());
                    continue;
                }
                if event.event.as_deref() == Some("response.completed") {
                    completed_response = Some(json.clone());
                    continue;
                }
                if let Some(delta) = json.get("delta").and_then(Value::as_str) {
                    text_deltas.push_str(delta);
                }
                if let Some(output_text) = json.get("output_text").and_then(Value::as_str) {
                    text_deltas.push_str(output_text);
                }
            }
            Err(_) => plain_data_chunks.push(data.to_string()),
        }
    }

    if let Some(response) = completed_response {
        return Some(
            serde_json::to_string_pretty(&response).unwrap_or_else(|_| response.to_string()),
        );
    }
    if !text_deltas.is_empty() {
        return Some(text_deltas);
    }
    if !plain_data_chunks.is_empty() {
        return Some(plain_data_chunks.join("\n"));
    }

    None
}

fn reconstruct_anthropic_message_from_sse(payload: &str) -> Option<String> {
    let events = parse_sse_events(payload);
    if events.is_empty() {
        return None;
    }

    let mut has_message_events = false;
    let mut message: Option<Value> = None;
    let mut content_blocks: Vec<Option<Value>> = Vec::new();
    let mut input_json_deltas: Vec<String> = Vec::new();

    for event in events {
        let data = event.data.trim();
        if data.is_empty() || data == "[DONE]" {
            continue;
        }
        let Ok(json) = serde_json::from_str::<Value>(data) else {
            continue;
        };

        let event_type = json
            .get("type")
            .and_then(Value::as_str)
            .or(event.event.as_deref());

        match event_type {
            Some("message_start") => {
                has_message_events = true;
                if let Some(start_message) = json.get("message").cloned() {
                    message = Some(start_message);
                }
            }
            Some("content_block_start") => {
                has_message_events = true;
                let Some(index) = event_index(&json) else {
                    continue;
                };
                ensure_slot(&mut content_blocks, index);
                if let Some(content_block) = json.get("content_block").cloned() {
                    content_blocks[index] = Some(content_block);
                }
            }
            Some("content_block_delta") => {
                has_message_events = true;
                let Some(index) = event_index(&json) else {
                    continue;
                };
                ensure_slot(&mut content_blocks, index);
                ensure_slot(&mut input_json_deltas, index);
                let Some(delta) = json.get("delta").and_then(Value::as_object) else {
                    continue;
                };
                let Some(delta_type) = delta.get("type").and_then(Value::as_str) else {
                    continue;
                };

                if content_blocks[index].is_none() {
                    content_blocks[index] = Some(Value::Object(Map::new()));
                }
                let Some(content_block) = content_blocks[index].as_mut() else {
                    continue;
                };

                match delta_type {
                    "text_delta" => {
                        append_string_delta(content_block, "text", delta.get("text"));
                    }
                    "thinking_delta" => {
                        append_string_delta(content_block, "thinking", delta.get("thinking"));
                    }
                    "signature_delta" => {
                        append_string_delta(content_block, "signature", delta.get("signature"));
                    }
                    "input_json_delta" => {
                        if let Some(partial) = delta.get("partial_json").and_then(Value::as_str) {
                            input_json_deltas[index].push_str(partial);
                        }
                    }
                    _ => {}
                }
            }
            Some("content_block_stop") => {
                has_message_events = true;
            }
            Some("message_delta") => {
                has_message_events = true;
                let Some(message_obj) = ensure_message_object(&mut message) else {
                    continue;
                };
                if let Some(delta_obj) = json.get("delta").and_then(Value::as_object) {
                    for (k, v) in delta_obj {
                        message_obj.insert(k.clone(), v.clone());
                    }
                }
                if let Some(usage) = json.get("usage").cloned() {
                    message_obj.insert("usage".to_string(), usage);
                }
            }
            Some("message_stop") => {
                has_message_events = true;
            }
            Some("ping") | Some("error") => {}
            _ => {}
        }
    }

    if !has_message_events {
        return None;
    }

    let message_obj = ensure_message_object(&mut message)?;

    let mut reconstructed_content = Vec::new();
    for (index, maybe_block) in content_blocks.into_iter().enumerate() {
        let Some(mut block) = maybe_block else {
            continue;
        };
        if let Some(partial_json) = input_json_deltas.get(index) {
            let partial_json = partial_json.trim();
            if !partial_json.is_empty() {
                if let Some(obj) = block.as_object_mut() {
                    match serde_json::from_str::<Value>(partial_json) {
                        Ok(parsed) => {
                            obj.insert("input".to_string(), parsed);
                        }
                        Err(_) => {
                            obj.insert(
                                "input".to_string(),
                                Value::String(partial_json.to_string()),
                            );
                        }
                    }
                }
            }
        }
        reconstructed_content.push(block);
    }
    message_obj.insert("content".to_string(), Value::Array(reconstructed_content));

    let message = Value::Object(message_obj.clone());
    Some(serde_json::to_string_pretty(&message).unwrap_or_else(|_| message.to_string()))
}

fn event_index(json: &Value) -> Option<usize> {
    let raw = json.get("index")?.as_u64()?;
    usize::try_from(raw).ok()
}

fn append_string_delta(content_block: &mut Value, field: &str, delta: Option<&Value>) {
    let Some(delta) = delta.and_then(Value::as_str) else {
        return;
    };
    if delta.is_empty() {
        return;
    }
    let Some(obj) = content_block.as_object_mut() else {
        return;
    };
    let prev = obj
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let mut next = prev;
    next.push_str(delta);
    obj.insert(field.to_string(), Value::String(next));
}

fn ensure_message_object(message: &mut Option<Value>) -> Option<&mut Map<String, Value>> {
    if message.is_none() {
        *message = Some(Value::Object(Map::new()));
    }
    message.as_mut()?.as_object_mut()
}

#[derive(Debug)]
struct SseEvent {
    event: Option<String>,
    data: String,
}

fn flush_sse_event(
    events: &mut Vec<SseEvent>,
    current_event: &mut Option<String>,
    data_lines: &mut Vec<String>,
) {
    if current_event.is_some() || !data_lines.is_empty() {
        events.push(SseEvent {
            event: current_event.take(),
            data: data_lines.join("\n"),
        });
        data_lines.clear();
    }
}

fn parse_sse_events(payload: &str) -> Vec<SseEvent> {
    let mut events = Vec::new();
    let mut current_event: Option<String> = None;
    let mut data_lines: Vec<String> = Vec::new();

    for line in payload.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            flush_sse_event(&mut events, &mut current_event, &mut data_lines);
            continue;
        }

        if let Some(v) = line.strip_prefix("event:") {
            current_event = Some(v.trim().to_string());
            continue;
        }
        if let Some(v) = line.strip_prefix("data:") {
            data_lines.push(v.trim_start().to_string());
            continue;
        }
    }

    flush_sse_event(&mut events, &mut current_event, &mut data_lines);
    events
}

fn path_supports_reconstruction(path: &str) -> bool {
    path_ends_with_responses(path)
        || path_ends_with_messages(path)
        || path_is_chat_completions(path)
}

fn path_ends_with_responses(path: &str) -> bool {
    path_last_segment(path) == Some("responses")
}

fn path_ends_with_messages(path: &str) -> bool {
    path_last_segment(path) == Some("messages")
}

fn path_is_chat_completions(path: &str) -> bool {
    let trimmed = path.trim_end_matches('/');
    trimmed
        .strip_suffix("/completions")
        .is_some_and(|prefix| path_last_segment(prefix) == Some("chat"))
}

fn path_last_segment(path: &str) -> Option<&str> {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    trimmed.rsplit('/').next()
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, Method, StatusCode, Uri};
    use bytes::Bytes;
    use url::Url;

    use super::{
        path_supports_reconstruction, reconstruct_response_payload, AttemptRouteContext,
        BodyLogCompression, ExchangeFileLogger, ExchangeLogContext,
    };

    #[test]
    fn reconstructable_path_detection() {
        assert!(path_supports_reconstruction("/v1/responses"));
        assert!(path_supports_reconstruction("/v1/chat/responses/"));
        assert!(path_supports_reconstruction("/v1/messages"));
        assert!(path_supports_reconstruction("/v1/messages/"));
        assert!(path_supports_reconstruction("/v1/chat/completions"));
        assert!(path_supports_reconstruction("/v1/chat/completions/"));
        assert!(!path_supports_reconstruction("/v1/responses/stream"));
        assert!(!path_supports_reconstruction("/v1/messages/count_tokens"));
        assert!(!path_supports_reconstruction("/v1/chat/completions/stream"));
    }

    #[test]
    fn reconstructs_completed_response_event() {
        let payload = concat!(
            "event: response.created\n",
            "data: {\"id\":\"evt_1\",\"type\":\"response.created\"}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\"}}\n\n",
            "data: [DONE]\n\n"
        );
        let out = reconstruct_response_payload(payload);
        assert!(out.contains("\"id\": \"resp_1\""));
        assert!(out.contains("\"status\": \"completed\""));
    }

    #[test]
    fn reconstructs_delta_fallback() {
        let payload = concat!(
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello \"}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"world\"}\n\n",
            "data: [DONE]\n\n"
        );
        let out = reconstruct_response_payload(payload);
        assert_eq!(out, "hello world");
    }

    #[test]
    fn reconstructs_chat_completion_text_and_usage() {
        let payload = concat!(
            "data: {\"id\":\"chatcmpl_1\",\"object\":\"chat.completion.chunk\",\"created\":123,\"model\":\"gpt-test\",\"system_fingerprint\":\"fp_1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Hello \"},\"logprobs\":null,\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"chatcmpl_1\",\"object\":\"chat.completion.chunk\",\"created\":123,\"model\":\"gpt-test\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"world\"},\"logprobs\":null,\"finish_reason\":\"stop\"}]}\n\n",
            "data: {\"id\":\"chatcmpl_1\",\"object\":\"chat.completion.chunk\",\"created\":123,\"model\":\"gpt-test\",\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3,\"total_tokens\":5}}\n\n",
            "data: [DONE]\n\n"
        );
        let out = reconstruct_response_payload(payload);
        let completion: serde_json::Value = serde_json::from_str(&out).unwrap();

        assert_eq!(completion["object"], "chat.completion");
        assert_eq!(completion["id"], "chatcmpl_1");
        assert_eq!(completion["system_fingerprint"], "fp_1");
        assert_eq!(completion["choices"][0]["message"]["role"], "assistant");
        assert_eq!(
            completion["choices"][0]["message"]["content"],
            "Hello world"
        );
        assert_eq!(completion["choices"][0]["finish_reason"], "stop");
        assert_eq!(completion["usage"]["total_tokens"], 5);
    }

    #[test]
    fn reconstructs_chat_completion_function_and_tool_calls() {
        let payload = concat!(
            r#"data: {"object":"chat.completion.chunk","id":"chatcmpl_tool","created":456,"model":"gpt-test","choices":[{"index":0,"delta":{"function_call":{"name":"look","arguments":"{\"q\":\"ru"}},"finish_reason":null},{"index":1,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"wea","arguments":"{\"city\":\"San"}}]},"finish_reason":null}]}

"#,
            r#"data: {"object":"chat.completion.chunk","id":"chatcmpl_tool","created":456,"model":"gpt-test","choices":[{"index":0,"delta":{"function_call":{"name":"up","arguments":"st\"}"}},"finish_reason":"function_call"},{"index":1,"delta":{"tool_calls":[{"index":0,"function":{"name":"ther","arguments":" Francisco\"}"}}]},"finish_reason":"tool_calls"}]}

"#,
            "data: [DONE]\n\n",
        );
        let out = reconstruct_response_payload(payload);
        let completion: serde_json::Value = serde_json::from_str(&out).unwrap();

        assert_eq!(
            completion["choices"][0]["message"]["function_call"]["name"],
            "lookup"
        );
        assert_eq!(
            completion["choices"][0]["message"]["function_call"]["arguments"],
            "{\"q\":\"rust\"}"
        );
        assert_eq!(
            completion["choices"][1]["message"]["tool_calls"][0]["id"],
            "call_1"
        );
        assert_eq!(
            completion["choices"][1]["message"]["tool_calls"][0]["function"]["name"],
            "weather"
        );
        assert_eq!(
            completion["choices"][1]["message"]["tool_calls"][0]["function"]["arguments"],
            "{\"city\":\"San Francisco\"}"
        );
    }

    #[test]
    fn reconstructs_anthropic_message_text() {
        let payload = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"claude-sonnet-4-6\",\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":0,\"output_tokens\":0}}}\n\n",
            "event: content_block_start\n",
            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello \"}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"world\"}}\n\n",
            "event: content_block_stop\n",
            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"input_tokens\":12,\"output_tokens\":3}}\n\n",
            "event: message_stop\n",
            "data: {\"type\":\"message_stop\"}\n\n"
        );
        let out = reconstruct_response_payload(payload);
        assert!(out.contains("\"id\": \"msg_1\""));
        assert!(out.contains("\"model\": \"claude-sonnet-4-6\""));
        assert!(out.contains("\"text\": \"Hello world\""));
        assert!(out.contains("\"stop_reason\": \"end_turn\""));
        assert!(out.contains("\"input_tokens\": 12"));
    }

    #[test]
    fn reconstructs_anthropic_tool_input_json() {
        let payload = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_tool\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"claude-opus-4-6\",\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":0,\"output_tokens\":0}}}\n\n",
            "event: content_block_start\n",
            "data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tool_1\",\"name\":\"shell\",\"input\":{}}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"cmd\\\":\\\"echo\\\"\"}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\",\\\"arg\\\":\\\"hi\\\"}\"}}\n\n",
            "event: content_block_stop\n",
            "data: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\",\"stop_sequence\":null},\"usage\":{\"input_tokens\":10,\"output_tokens\":2}}\n\n",
            "event: message_stop\n",
            "data: {\"type\":\"message_stop\"}\n\n"
        );
        let out = reconstruct_response_payload(payload);
        assert!(out.contains("\"name\": \"shell\""));
        assert!(out.contains("\"cmd\": \"echo\""));
        assert!(out.contains("\"arg\": \"hi\""));
        assert!(out.contains("\"stop_reason\": \"tool_use\""));
    }

    #[test]
    fn keeps_plain_text_errors_unchanged() {
        let payload = "upstream error: invalid API key\n";
        let out = reconstruct_response_payload(payload);
        assert_eq!(out, payload);
    }

    #[test]
    fn reconstructs_zstd_encoded_response_and_retains_compressed_wire_body() {
        let root = std::env::temp_dir().join(format!(
            "codex-provider-proxy-zstd-reconstruction-{}-{}",
            std::process::id(),
            super::now_unix_ms()
        ));
        std::fs::create_dir_all(&root).unwrap();

        let method = Method::POST;
        let uri: Uri = "/v1/messages".parse().unwrap();
        let upstream_url = Url::parse("https://api.example.com/v1/messages").unwrap();
        let request_headers = HeaderMap::new();
        let ctx = ExchangeLogContext {
            request_id: 78,
            peer: "127.0.0.1:5001".parse().unwrap(),
            pid: None,
            route_pid: None,
            provider_name: "provider_a",
            method: &method,
            uri: &uri,
            upstream_url: &upstream_url,
            request_headers: &request_headers,
        };
        let mut logger =
            ExchangeFileLogger::new(&root, &ctx, true, None, BodyLogCompression::Zstd).unwrap();
        let route = AttemptRouteContext {
            route_pid: None,
            provider_name: "provider_a",
            upstream_url: &upstream_url,
        };
        let payload = br#"{"error":{"message":"decoded"},"type":"error"}"#;
        let compressed_payload = zstd::stream::encode_all(payload.as_slice(), 3).unwrap();
        let mut response_headers = HeaderMap::new();
        response_headers.insert("content-encoding", "zstd".parse().unwrap());

        logger.write_response_headers(route, StatusCode::NOT_FOUND, &response_headers, 12);
        logger.on_response_body_chunk(&Bytes::from(compressed_payload.clone()));
        logger.finalize();

        assert_eq!(logger.metadata.reconstruction_succeeded, Some(true));
        assert_eq!(
            std::fs::read(&logger.reconstructed_path).unwrap(),
            payload.as_slice()
        );
        assert_eq!(
            super::read_logged_body_file(&logger.response_body.path, BodyLogCompression::Zstd)
                .unwrap(),
            compressed_payload
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn flushes_body_chunks_to_disk_before_exchange_finalization() {
        let path = std::env::temp_dir().join(format!(
            "codex-provider-proxy-body-flush-{}-{}.bin",
            std::process::id(),
            super::now_unix_ms()
        ));
        let mut sink = super::BodyLogSink::new(
            path.clone(),
            BodyLogCompression::None,
            None,
            "response body",
        )
        .unwrap();
        sink.append(78, &Bytes::from_static(b"visible immediately"));

        assert_eq!(std::fs::read(&path).unwrap(), b"visible immediately");
        sink.finish(78);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn body_log_sink_tracks_total_logged_and_truncated_bytes() {
        let path = std::env::temp_dir().join(format!(
            "codex-provider-proxy-body-limit-{}-{}.bin",
            std::process::id(),
            super::now_unix_ms()
        ));
        let mut sink = super::BodyLogSink::new(
            path.clone(),
            BodyLogCompression::None,
            Some(5),
            "response body",
        )
        .unwrap();

        sink.append(79, &Bytes::from_static(b"abc"));
        sink.append(79, &Bytes::from_static(b"defgh"));
        sink.finish(79);

        let snapshot = sink.snapshot();
        assert_eq!(snapshot.bytes, 8);
        assert_eq!(snapshot.logged_bytes, 5);
        assert!(snapshot.truncated);
        assert_eq!(std::fs::read(&path).unwrap(), b"abcde");

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn writes_request_and_response_files_for_each_attempt() {
        let root = std::env::temp_dir().join(format!(
            "codex-provider-proxy-attempt-log-{}-{}",
            std::process::id(),
            super::now_unix_ms()
        ));
        std::fs::create_dir_all(&root).unwrap();

        let method = Method::POST;
        let uri: Uri = "/v1/responses".parse().unwrap();
        let upstream_url = Url::parse("https://api.example.com/v1/responses").unwrap();
        let mut request_headers = HeaderMap::new();
        request_headers.insert("x-test", "root".parse().unwrap());
        let ctx = ExchangeLogContext {
            request_id: 77,
            peer: "127.0.0.1:5000".parse().unwrap(),
            pid: Some(111),
            route_pid: Some(222),
            provider_name: "provider_a",
            method: &method,
            uri: &uri,
            upstream_url: &upstream_url,
            request_headers: &request_headers,
        };
        let mut logger =
            ExchangeFileLogger::new(&root, &ctx, false, None, BodyLogCompression::None).unwrap();

        let route = AttemptRouteContext {
            route_pid: Some(222),
            provider_name: "provider_a",
            upstream_url: &upstream_url,
        };
        let request_body = Bytes::from_static(br#"{"model":"test"}"#);
        logger.begin_attempt(1, route, &method, &request_headers, Some(&request_body));
        logger.record_attempt(
            1,
            route,
            StatusCode::INTERNAL_SERVER_ERROR,
            &HeaderMap::new(),
            12,
            false,
        );
        logger.on_attempt_response_body_chunk(1, &Bytes::from_static(b"first failure"));
        logger.finish_attempt(1);

        logger.begin_attempt(2, route, &method, &request_headers, Some(&request_body));
        logger.record_attempt(2, route, StatusCode::OK, &HeaderMap::new(), 34, true);
        logger.on_response_body_chunk(&Bytes::from_static(b"final success"));
        logger.finish_attempt(2);
        logger.finalize();

        assert_eq!(logger.metadata.schema_version, 4);
        assert_eq!(logger.metadata.attempts.len(), 2);
        let attempt_1 = &logger.metadata.attempts[0];
        let attempt_2 = &logger.metadata.attempts[1];
        assert!(!attempt_1.is_final);
        assert!(attempt_2.is_final);
        assert_eq!(
            attempt_1.request_body_bytes,
            Some(u64::try_from(request_body.len()).unwrap())
        );
        assert_eq!(
            attempt_2.request_body_bytes,
            Some(u64::try_from(request_body.len()).unwrap())
        );
        assert_eq!(attempt_1.response_body_bytes, Some(13));
        assert_eq!(attempt_2.response_body_bytes, Some(13));

        for path in [
            &attempt_1.request_headers,
            &attempt_1.request_body,
            &attempt_1.response_headers,
            &attempt_1.response_body,
            &attempt_2.request_headers,
            &attempt_2.request_body,
            &attempt_2.response_headers,
            &attempt_2.response_body,
        ] {
            assert!(std::path::Path::new(path).exists(), "{path} should exist");
        }

        assert_eq!(
            std::fs::read(&attempt_1.request_body).unwrap(),
            request_body
        );
        assert_eq!(
            std::fs::read(&attempt_1.response_body).unwrap(),
            b"first failure"
        );
        assert_eq!(
            std::fs::read(&attempt_2.request_body).unwrap(),
            request_body
        );
        assert_eq!(
            std::fs::read(&attempt_2.response_body).unwrap(),
            b"final success"
        );

        let _ = std::fs::remove_dir_all(root);
    }
}
