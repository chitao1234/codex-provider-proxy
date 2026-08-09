//! Server-Sent Events helpers: encoding downstream events and decoding upstream data lines.

use std::borrow::Cow;

use bytes::Bytes;

/// Maximum buffered size of one not-yet-delimited SSE event.
pub const MAX_EVENT_BYTES: usize = 8 * 1024 * 1024;

/// The fields used by the conversion layer from one decoded SSE event.
#[derive(Debug, PartialEq, Eq)]
pub struct DecodedEvent<'a> {
    pub event_type: Option<&'a str>,
    pub data: Option<Cow<'a, str>>,
}

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

/// Decode the event type and complete data payload from one SSE event.
///
/// Multiple `data:` fields are joined with newlines as required by SSE. A single
/// data field remains borrowed, keeping the common path allocation-free.
pub fn decode_event(event: &[u8]) -> Result<DecodedEvent<'_>, std::str::Utf8Error> {
    let event = std::str::from_utf8(event)?;
    let mut event_type = None;
    let mut data_lines = Vec::new();
    let bytes = event.as_bytes();
    let mut line_start = 0;
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\r' | b'\n' => {
                decode_line(&event[line_start..index], &mut event_type, &mut data_lines);
                if bytes[index] == b'\r' && bytes.get(index + 1) == Some(&b'\n') {
                    index += 2;
                } else {
                    index += 1;
                }
                line_start = index;
            }
            _ => index += 1,
        }
    }
    if line_start < event.len() {
        decode_line(&event[line_start..], &mut event_type, &mut data_lines);
    }
    let data = match data_lines.as_slice() {
        [] => None,
        [data] => Some(Cow::Borrowed(*data)),
        _ => Some(Cow::Owned(data_lines.join("\n"))),
    };
    Ok(DecodedEvent { event_type, data })
}

fn decode_line<'a>(line: &'a str, event_type: &mut Option<&'a str>, data_lines: &mut Vec<&'a str>) {
    if line.starts_with(':') {
        return;
    }
    let (field, value) = line.split_once(':').unwrap_or((line, ""));
    let value = value.strip_prefix(' ').unwrap_or(value);
    match field {
        "event" => *event_type = Some(value),
        "data" => data_lines.push(value),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_sse_event_with_trailing_blank_line() {
        let event = encode_sse_event(
            "message_start",
            &serde_json::json!({"type": "message_start"}),
        );
        assert_eq!(
            event,
            Bytes::from_static(b"event: message_start\ndata: {\"type\":\"message_start\"}\n\n")
        );
    }

    #[test]
    fn decodes_complete_event_data() {
        assert_eq!(
            decode_event(b"event: message_start\ndata: {\"a\":1}\n\n").unwrap(),
            DecodedEvent {
                event_type: Some("message_start"),
                data: Some(Cow::Borrowed("{\"a\":1}")),
            }
        );
        assert_eq!(
            decode_event(b"data: {\ndata:   \"a\": 1\ndata: }\n\n").unwrap(),
            DecodedEvent {
                event_type: None,
                data: Some(Cow::Owned("{\n  \"a\": 1\n}".to_owned())),
            }
        );
        assert_eq!(
            decode_event(b"data:\ndata:\n\n").unwrap().data,
            Some(Cow::Owned("\n".to_owned()))
        );
        assert_eq!(decode_event(b": keep-alive\n\n").unwrap().data, None);
        assert_eq!(
            decode_event(b"event: test\rdata: first\rdata: second\r\r").unwrap(),
            DecodedEvent {
                event_type: Some("test"),
                data: Some(Cow::Owned("first\nsecond".to_owned())),
            }
        );
        assert!(decode_event(b"\xff\xfe garbage").is_err());
    }
}
