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

use crate::config::{BodyLogCompression, LoggingConfig};

pub type SharedExchangeFileLogger = Arc<Mutex<ExchangeFileLogger>>;
const EXCHANGE_LOG_SCHEMA_VERSION: u32 = 3;

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
        ctx.request_id,
        ctx.peer,
        ctx.pid,
        ctx.route_pid,
        ctx.provider_name,
        ctx.method,
        ctx.uri,
        ctx.upstream_url,
        ctx.request_headers,
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
    request_body_path: PathBuf,
    response_body_path: PathBuf,
    request_body_writer: Option<BodyLogWriter>,
    response_body_writer: Option<BodyLogWriter>,
    response_headers_path: PathBuf,
    reconstructed_path: PathBuf,
    should_reconstruct: bool,
    body_max_bytes: Option<u64>,
    body_compression: BodyLogCompression,
    request_body_bytes: u64,
    response_body_bytes: u64,
    request_body_logged_bytes: u64,
    response_body_logged_bytes: u64,
    request_body_truncated: bool,
    response_body_truncated: bool,
}

impl ExchangeFileLogger {
    #[allow(clippy::too_many_arguments)]
    fn new(
        root_dir: &Path,
        request_id: u64,
        peer: SocketAddr,
        pid: Option<u32>,
        route_pid: Option<u32>,
        provider_name: &str,
        method: &Method,
        uri: &Uri,
        upstream_url: &Url,
        request_headers: &HeaderMap,
        should_reconstruct: bool,
        body_max_bytes: Option<u64>,
        body_compression: BodyLogCompression,
    ) -> std::io::Result<Self> {
        fs::create_dir_all(root_dir)?;

        let now = now_unix_ms();
        let stem = format!("{now}_req_{request_id}");
        let body_suffix = body_file_suffix(body_compression);

        let metadata_path = root_dir.join(format!("{stem}.meta.json"));
        let request_headers_path = root_dir.join(format!("{stem}.request_headers.txt"));
        let request_body_path = root_dir.join(format!("{stem}.request_body{body_suffix}"));
        let response_headers_path = root_dir.join(format!("{stem}.response_headers.txt"));
        let response_body_path = root_dir.join(format!("{stem}.response_body{body_suffix}"));
        let reconstructed_path = root_dir.join(format!("{stem}.response_reconstructed.txt"));

        let metadata = ExchangeMetadata {
            schema_version: EXCHANGE_LOG_SCHEMA_VERSION,
            request_id,
            started_unix_ms: now,
            peer: peer.to_string(),
            pid,
            route_pid,
            provider: provider_name.to_string(),
            method: method.to_string(),
            uri: uri.to_string(),
            upstream_url: upstream_url.to_string(),
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
        write_request_headers_file(&request_headers_path, method, uri, request_headers)?;

        Ok(Self {
            request_id,
            root_dir: root_dir.to_path_buf(),
            stem,
            metadata_path,
            metadata,
            request_body_path: request_body_path.clone(),
            response_body_path: response_body_path.clone(),
            request_body_writer: Some(create_body_writer(&request_body_path, body_compression)?),
            response_body_writer: Some(create_body_writer(&response_body_path, body_compression)?),
            response_headers_path,
            reconstructed_path,
            should_reconstruct,
            body_max_bytes,
            body_compression,
            request_body_bytes: 0,
            response_body_bytes: 0,
            request_body_logged_bytes: 0,
            response_body_logged_bytes: 0,
            request_body_truncated: false,
            response_body_truncated: false,
        })
    }

    pub fn on_request_body_chunk(&mut self, chunk: &Bytes) {
        self.request_body_bytes = self
            .request_body_bytes
            .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        let (write_len, truncated) = limited_chunk_len(
            self.body_max_bytes,
            self.request_body_logged_bytes,
            chunk.len(),
        );
        self.request_body_truncated |= truncated;
        self.request_body_logged_bytes = self
            .request_body_logged_bytes
            .saturating_add(u64::try_from(write_len).unwrap_or(u64::MAX));

        let to_write = &chunk[..write_len];
        write_chunk_best_effort(
            self.request_id,
            &self.request_body_path,
            &mut self.request_body_writer,
            to_write,
            "request body",
        );
    }

    pub fn on_response_body_chunk(&mut self, chunk: &Bytes) {
        self.response_body_bytes = self
            .response_body_bytes
            .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        let (write_len, truncated) = limited_chunk_len(
            self.body_max_bytes,
            self.response_body_logged_bytes,
            chunk.len(),
        );
        self.response_body_truncated |= truncated;
        self.response_body_logged_bytes = self
            .response_body_logged_bytes
            .saturating_add(u64::try_from(write_len).unwrap_or(u64::MAX));

        let to_write = &chunk[..write_len];
        write_chunk_best_effort(
            self.request_id,
            &self.response_body_path,
            &mut self.response_body_writer,
            to_write,
            "response body",
        );
    }

    pub fn record_attempt(
        &mut self,
        route: AttemptRouteContext<'_>,
        status: StatusCode,
        headers: &HeaderMap,
        latency_ms: u128,
        response_body_bytes: Option<u64>,
        is_final: bool,
    ) {
        let attempt_number = u32::try_from(self.metadata.attempts.len())
            .unwrap_or(u32::MAX)
            .saturating_add(1);
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
        if let Some(bytes) = response_body_bytes {
            body.push_str(&format!("response_body_bytes: {bytes}\n"));
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

        self.metadata.attempts.push(ExchangeAttemptMetadata {
            attempt: attempt_number,
            is_final,
            route_pid: route.route_pid,
            provider: route.provider_name.to_string(),
            upstream_url: route.upstream_url.to_string(),
            response_status_code: status.as_u16(),
            response_status_text: status_text.to_string(),
            upstream_latency_ms: latency_ms,
            response_body_bytes,
            response_body_logged_bytes: None,
            response_body_truncated: None,
            response_headers: attempt_headers_path.display().to_string(),
            response_body: if is_final {
                Some(self.response_body_path.display().to_string())
            } else {
                None
            },
        });
        self.persist_metadata_best_effort("write attempt metadata");
    }

    pub fn update_upstream_target(&mut self, route: AttemptRouteContext<'_>) {
        self.metadata.route_pid = route.route_pid;
        self.metadata.provider = route.provider_name.to_string();
        self.metadata.upstream_url = route.upstream_url.to_string();
        self.persist_metadata_best_effort("update upstream target metadata");
    }

    pub fn write_response_headers(
        &mut self,
        route: AttemptRouteContext<'_>,
        status: StatusCode,
        headers: &HeaderMap,
        latency_ms: u128,
    ) {
        self.metadata.route_pid = route.route_pid;
        self.metadata.provider = route.provider_name.to_string();
        self.metadata.upstream_url = route.upstream_url.to_string();
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
        self.metadata.upstream_latency_ms = Some(latency_ms);
        self.metadata.upstream_error = Some(truncate_meta_error(err));
        self.persist_metadata_best_effort("write upstream error metadata");
    }

    pub fn finalize(&mut self) {
        flush_best_effort(
            self.request_id,
            &self.request_body_path,
            &mut self.request_body_writer,
            "request body",
        );
        flush_best_effort(
            self.request_id,
            &self.response_body_path,
            &mut self.response_body_writer,
            "response body",
        );

        self.metadata.request_body_bytes = self.request_body_bytes;
        self.metadata.response_body_bytes = self.response_body_bytes;
        self.metadata.request_body_logged_bytes = self.request_body_logged_bytes;
        self.metadata.response_body_logged_bytes = self.response_body_logged_bytes;
        self.metadata.request_body_truncated = self.request_body_truncated;
        self.metadata.response_body_truncated = self.response_body_truncated;
        self.metadata.completed_unix_ms = Some(now_unix_ms());
        self.metadata.total_duration_ms = Some(
            self.metadata
                .completed_unix_ms
                .unwrap_or(self.metadata.started_unix_ms)
                .saturating_sub(self.metadata.started_unix_ms),
        );
        if let Some(final_attempt) = self.metadata.attempts.iter_mut().rev().find(|a| a.is_final) {
            final_attempt.response_body_bytes = Some(self.response_body_bytes);
            final_attempt.response_body_logged_bytes = Some(self.response_body_logged_bytes);
            final_attempt.response_body_truncated = Some(self.response_body_truncated);
        }

        if self.should_reconstruct {
            self.metadata.reconstruction_attempted = true;
        }

        if self.should_reconstruct {
            if let Err(err) = self.reconstruct_and_write() {
                self.metadata.reconstruction_succeeded = Some(false);
                self.metadata.reconstruction_error = Some(truncate_meta_error(&err.to_string()));
                warn!(
                    request_id = self.request_id,
                    response_body = %self.response_body_path.display(),
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

    fn reconstruct_and_write(&self) -> std::io::Result<()> {
        let raw = read_logged_body_file(&self.response_body_path, self.body_compression)?;
        if raw.is_empty() {
            return Ok(());
        }
        let raw_text = String::from_utf8_lossy(&raw);
        let reconstructed = reconstruct_response_payload(&raw_text);
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
    response_status_code: u16,
    response_status_text: String,
    upstream_latency_ms: u128,
    response_body_bytes: Option<u64>,
    response_body_logged_bytes: Option<u64>,
    response_body_truncated: Option<bool>,
    response_headers: String,
    response_body: Option<String>,
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

fn format_headers(headers: &HeaderMap) -> String {
    let mut body = String::new();
    for (name, value) in headers.iter() {
        let value = value
            .to_str()
            .map(std::string::ToString::to_string)
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
    let remaining_usize = usize::try_from(remaining).unwrap_or(usize::MAX);
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

fn write_chunk_best_effort(
    request_id: u64,
    path: &Path,
    writer: &mut Option<BodyLogWriter>,
    chunk: &[u8],
    kind: &'static str,
) {
    if chunk.is_empty() {
        return;
    }
    let Some(writer_inner) = writer.as_mut() else {
        return;
    };
    if let Err(err) = writer_inner.write_all(chunk) {
        warn!(
            request_id,
            path = %path.display(),
            kind,
            error = %err,
            "failed to append exchange log chunk"
        );
        *writer = None;
    }
}

fn flush_best_effort(
    request_id: u64,
    path: &Path,
    writer: &mut Option<BodyLogWriter>,
    kind: &'static str,
) {
    let Some(writer_inner) = writer.take() else {
        return;
    };
    if let Err(err) = writer_inner.finish() {
        warn!(
            request_id,
            path = %path.display(),
            kind,
            error = %err,
            "failed to flush exchange log file"
        );
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

    reconstruct_openai_response_from_sse(payload)
        .or_else(|| reconstruct_anthropic_message_from_sse(payload))
        .unwrap_or_else(|| payload.to_string())
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
        return serde_json::to_string_pretty(&response)
            .ok()
            .or_else(|| Some(response.to_string()));
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
                ensure_content_slot(&mut content_blocks, index);
                if let Some(content_block) = json.get("content_block").cloned() {
                    content_blocks[index] = Some(content_block);
                }
            }
            Some("content_block_delta") => {
                has_message_events = true;
                let Some(index) = event_index(&json) else {
                    continue;
                };
                ensure_content_slot(&mut content_blocks, index);
                ensure_input_slot(&mut input_json_deltas, index);
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
    serde_json::to_string_pretty(&message)
        .ok()
        .or(Some(message.to_string()))
}

fn ensure_content_slot(content_blocks: &mut Vec<Option<Value>>, index: usize) {
    if content_blocks.len() <= index {
        content_blocks.resize_with(index + 1, || None);
    }
}

fn ensure_input_slot(input_json_deltas: &mut Vec<String>, index: usize) {
    if input_json_deltas.len() <= index {
        input_json_deltas.resize_with(index + 1, String::new);
    }
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

fn parse_sse_events(payload: &str) -> Vec<SseEvent> {
    let mut events = Vec::new();
    let mut current_event: Option<String> = None;
    let mut data_lines: Vec<String> = Vec::new();

    for line in payload.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            if current_event.is_some() || !data_lines.is_empty() {
                events.push(SseEvent {
                    event: current_event.take(),
                    data: data_lines.join("\n"),
                });
                data_lines.clear();
            }
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

    if current_event.is_some() || !data_lines.is_empty() {
        events.push(SseEvent {
            event: current_event,
            data: data_lines.join("\n"),
        });
    }

    events
}

fn path_supports_reconstruction(path: &str) -> bool {
    path_ends_with_responses(path) || path_ends_with_messages(path)
}

fn path_ends_with_responses(path: &str) -> bool {
    path_last_segment(path) == Some("responses")
}

fn path_ends_with_messages(path: &str) -> bool {
    path_last_segment(path) == Some("messages")
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
    use super::{path_supports_reconstruction, reconstruct_response_payload};

    #[test]
    fn reconstructable_path_detection() {
        assert!(path_supports_reconstruction("/v1/responses"));
        assert!(path_supports_reconstruction("/v1/chat/responses/"));
        assert!(path_supports_reconstruction("/v1/messages"));
        assert!(path_supports_reconstruction("/v1/messages/"));
        assert!(!path_supports_reconstruction("/v1/responses/stream"));
        assert!(!path_supports_reconstruction("/v1/messages/count_tokens"));
        assert!(!path_supports_reconstruction("/v1/chat/completions"));
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
}
