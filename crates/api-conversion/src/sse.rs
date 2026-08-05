//! Server-Sent Events helpers: encoding downstream events and decoding upstream data lines.

use bytes::Bytes;

/// Encode one SSE event: `event: <type>\ndata: <json>\n\n`.
pub fn encode_sse_event(event_type: &str, data: &serde_json::Value) -> Bytes {
    let mut out = Vec::with_capacity(event_type.len() + data.to_string().len() + 16);
    out.extend_from_slice(b"event: ");
    out.extend_from_slice(event_type.as_bytes());
    out.extend_from_slice(b"\ndata: ");
    out.extend_from_slice(data.to_string().as_bytes());
    out.extend_from_slice(b"\n\n");
    Bytes::from(out)
}

/// The `data:` payload of a complete SSE event line (the part after `data: `, trimmed).
///
/// Returns `None` for non-data lines (`event:`, `:`, `id:`, `retry:`) and for empty data.
pub fn data_payload(line: &str) -> Option<&str> {
    let line = line.strip_prefix("data:")?;
    let payload = line.strip_prefix(' ').unwrap_or(line);
    if payload.is_empty() {
        None
    } else {
        Some(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_sse_event_with_trailing_blank_line() {
        let event = encode_sse_event("message_start", &serde_json::json!({"type": "message_start"}));
        assert_eq!(
            event,
            Bytes::from_static(b"event: message_start\ndata: {\"type\":\"message_start\"}\n\n")
        );
    }

    #[test]
    fn extracts_data_payload() {
        assert_eq!(data_payload("data: {\"a\":1}"), Some("{\"a\":1}"));
        assert_eq!(data_payload("data:{\"a\":1}"), Some("{\"a\":1}"));
        assert_eq!(data_payload("data: [DONE]"), Some("[DONE]"));
        assert_eq!(data_payload("event: message_start"), None);
        assert_eq!(data_payload(": keep-alive"), None);
        assert_eq!(data_payload("data:"), None);
    }
}
