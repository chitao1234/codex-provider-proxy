use std::{
    fs::{self, File},
    io::{BufWriter, Read, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::http::{HeaderMap, Method, StatusCode, Uri};
use bytes::Bytes;
use serde::Serialize;
use serde_json::Value;
use tracing::warn;
use url::Url;

use crate::config::LoggingConfig;

pub type SharedExchangeFileLogger = Arc<Mutex<ExchangeFileLogger>>;

pub fn maybe_create_exchange_logger(
    cfg: &LoggingConfig,
    request_id: u64,
    peer: SocketAddr,
    pid: Option<u32>,
    route_pid: Option<u32>,
    provider_name: &str,
    method: &Method,
    uri: &Uri,
    upstream_url: &Url,
    request_headers: &HeaderMap,
) -> Option<SharedExchangeFileLogger> {
    let Some(root_dir) = cfg.exchange_log_dir.as_ref() else {
        return None;
    };

    let should_reconstruct = cfg.reconstruct_responses && path_ends_with_responses(uri.path());
    match ExchangeFileLogger::new(
        root_dir,
        request_id,
        peer,
        pid,
        route_pid,
        provider_name,
        method,
        uri,
        upstream_url,
        request_headers,
        should_reconstruct,
    ) {
        Ok(logger) => Some(Arc::new(Mutex::new(logger))),
        Err(err) => {
            warn!(
                request_id,
                dir = %root_dir.display(),
                error = %err,
                "failed to initialize exchange file logger"
            );
            None
        }
    }
}

#[derive(Debug)]
pub struct ExchangeFileLogger {
    request_id: u64,
    request_body_path: PathBuf,
    response_body_path: PathBuf,
    request_body_writer: Option<BufWriter<File>>,
    response_body_writer: Option<BufWriter<File>>,
    response_headers_path: PathBuf,
    reconstructed_path: PathBuf,
    should_reconstruct: bool,
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
    ) -> std::io::Result<Self> {
        fs::create_dir_all(root_dir)?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let stem = format!("{now}_req_{request_id}");

        let metadata_path = root_dir.join(format!("{stem}.meta.json"));
        let request_headers_path = root_dir.join(format!("{stem}.request_headers.txt"));
        let request_body_path = root_dir.join(format!("{stem}.request_body.bin"));
        let response_headers_path = root_dir.join(format!("{stem}.response_headers.txt"));
        let response_body_path = root_dir.join(format!("{stem}.response_body.bin"));
        let reconstructed_path = root_dir.join(format!("{stem}.response_reconstructed.txt"));

        let metadata = ExchangeMetadata {
            request_id,
            started_unix_ms: now,
            peer: peer.to_string(),
            pid,
            route_pid,
            provider: provider_name.to_string(),
            method: method.to_string(),
            uri: uri.to_string(),
            upstream_url: upstream_url.to_string(),
            should_reconstruct,
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
            request_body_path: request_body_path.clone(),
            response_body_path: response_body_path.clone(),
            request_body_writer: Some(BufWriter::new(File::create(&request_body_path)?)),
            response_body_writer: Some(BufWriter::new(File::create(&response_body_path)?)),
            response_headers_path,
            reconstructed_path,
            should_reconstruct,
        })
    }

    pub fn on_request_body_chunk(&mut self, chunk: &Bytes) {
        write_chunk_best_effort(
            self.request_id,
            &self.request_body_path,
            &mut self.request_body_writer,
            chunk,
            "request body",
        );
    }

    pub fn on_response_body_chunk(&mut self, chunk: &Bytes) {
        write_chunk_best_effort(
            self.request_id,
            &self.response_body_path,
            &mut self.response_body_writer,
            chunk,
            "response body",
        );
    }

    pub fn write_response_headers(
        &mut self,
        status: StatusCode,
        headers: &HeaderMap,
        latency_ms: u128,
    ) {
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

        if !self.should_reconstruct {
            return;
        }

        if let Err(err) = self.reconstruct_and_write() {
            warn!(
                request_id = self.request_id,
                response_body = %self.response_body_path.display(),
                reconstructed = %self.reconstructed_path.display(),
                error = %err,
                "response reconstruction failed; proxy response was unaffected"
            );
        }
    }

    fn reconstruct_and_write(&self) -> std::io::Result<()> {
        let mut raw = Vec::new();
        File::open(&self.response_body_path)?.read_to_end(&mut raw)?;
        if raw.is_empty() {
            return Ok(());
        }
        let raw_text = String::from_utf8_lossy(&raw);
        let reconstructed = reconstruct_response_payload(&raw_text);
        fs::write(&self.reconstructed_path, reconstructed.as_bytes())
    }
}

#[derive(Debug, Serialize)]
struct ExchangeMetadata {
    request_id: u64,
    started_unix_ms: u128,
    peer: String,
    pid: Option<u32>,
    route_pid: Option<u32>,
    provider: String,
    method: String,
    uri: String,
    upstream_url: String,
    should_reconstruct: bool,
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

fn write_chunk_best_effort(
    request_id: u64,
    path: &Path,
    writer: &mut Option<BufWriter<File>>,
    chunk: &Bytes,
    kind: &'static str,
) {
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
    writer: &mut Option<BufWriter<File>>,
    kind: &'static str,
) {
    let Some(mut writer_inner) = writer.take() else {
        return;
    };
    if let Err(err) = writer_inner.flush() {
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

    reconstruct_from_sse(payload).unwrap_or_else(|| payload.to_string())
}

fn reconstruct_from_sse(payload: &str) -> Option<String> {
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

fn path_ends_with_responses(path: &str) -> bool {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return false;
    }
    trimmed.rsplit('/').next() == Some("responses")
}

#[cfg(test)]
mod tests {
    use super::{path_ends_with_responses, reconstruct_response_payload};

    #[test]
    fn responses_path_detection() {
        assert!(path_ends_with_responses("/v1/responses"));
        assert!(path_ends_with_responses("/v1/chat/responses/"));
        assert!(!path_ends_with_responses("/v1/responses/stream"));
        assert!(!path_ends_with_responses("/v1/chat/completions"));
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
    fn keeps_plain_text_errors_unchanged() {
        let payload = "upstream error: invalid API key\n";
        let out = reconstruct_response_payload(payload);
        assert_eq!(out, payload);
    }
}
