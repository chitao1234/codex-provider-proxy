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
use serde_json::{Map, Value};
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

    let should_reconstruct = cfg.reconstruct_responses && path_supports_reconstruction(uri.path());
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
    metadata_path: PathBuf,
    metadata: ExchangeMetadata,
    request_body_path: PathBuf,
    response_body_path: PathBuf,
    request_body_writer: Option<BufWriter<File>>,
    response_body_writer: Option<BufWriter<File>>,
    response_headers_path: PathBuf,
    reconstructed_path: PathBuf,
    should_reconstruct: bool,
    request_body_bytes: u64,
    response_body_bytes: u64,
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

        let now = now_unix_ms();
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
            response_status_code: None,
            response_status_text: None,
            upstream_latency_ms: None,
            completed_unix_ms: None,
            total_duration_ms: None,
            upstream_error: None,
            request_body_bytes: 0,
            response_body_bytes: 0,
            reconstruction_attempted: false,
            reconstruction_succeeded: None,
            reconstruction_error: None,
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
            metadata_path,
            metadata,
            request_body_path: request_body_path.clone(),
            response_body_path: response_body_path.clone(),
            request_body_writer: Some(BufWriter::new(File::create(&request_body_path)?)),
            response_body_writer: Some(BufWriter::new(File::create(&response_body_path)?)),
            response_headers_path,
            reconstructed_path,
            should_reconstruct,
            request_body_bytes: 0,
            response_body_bytes: 0,
        })
    }

    pub fn on_request_body_chunk(&mut self, chunk: &Bytes) {
        self.request_body_bytes = self
            .request_body_bytes
            .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        write_chunk_best_effort(
            self.request_id,
            &self.request_body_path,
            &mut self.request_body_writer,
            chunk,
            "request body",
        );
    }

    pub fn on_response_body_chunk(&mut self, chunk: &Bytes) {
        self.response_body_bytes = self
            .response_body_bytes
            .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
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
        self.metadata.completed_unix_ms = Some(now_unix_ms());
        self.metadata.total_duration_ms = Some(
            self.metadata
                .completed_unix_ms
                .unwrap_or(self.metadata.started_unix_ms)
                .saturating_sub(self.metadata.started_unix_ms),
        );

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
        let mut raw = Vec::new();
        File::open(&self.response_body_path)?.read_to_end(&mut raw)?;
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
    response_status_code: Option<u16>,
    response_status_text: Option<String>,
    upstream_latency_ms: Option<u128>,
    completed_unix_ms: Option<u128>,
    total_duration_ms: Option<u128>,
    upstream_error: Option<String>,
    request_body_bytes: u64,
    response_body_bytes: u64,
    reconstruction_attempted: bool,
    reconstruction_succeeded: Option<bool>,
    reconstruction_error: Option<String>,
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

    let Some(message_obj) = ensure_message_object(&mut message) else {
        return None;
    };

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
        .or_else(|| Some(message.to_string()))
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
