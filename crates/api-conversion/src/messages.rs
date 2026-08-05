//! Anthropic Messages request/response structure helpers shared by the converters.
//!
//! Conversion works on `serde_json::Value` trees (like the proxy's model-mapping rewrite) so
//! unknown fields are preserved or dropped explicitly rather than lost through typed structs.

use serde_json::{Map, Value};

/// Prefixes of Anthropic server-side tool types (web_search_20260209, web_fetch_20260209,
/// code_execution_*, tool_search_*, ...). These have no Chat Completions equivalent and are
/// handled by the configured `ServerToolPolicy`.
pub const SERVER_TOOL_TYPE_PREFIXES: &[&str] = &[
    "web_search_",
    "web_fetch_",
    "code_execution_",
    "tool_search_",
    "computer_",
    "memory_",
    "bash_",
    "text_editor_",
];

/// Claude Code billing attribution system text; never forwarded upstream.
pub const CLAUDE_BILLING_HEADER_PREFIX: &str = "x-anthropic-billing-header:";

/// A content block from an Anthropic message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentBlock<'a> {
    Text(&'a str),
    Image,
    Thinking(&'a str),
    RedactedThinking,
    ToolUse {
        id: &'a str,
        name: &'a str,
        input: Option<&'a Value>,
    },
    ToolResult {
        tool_use_id: Option<&'a str>,
        content: Option<&'a Value>,
        is_error: bool,
    },
    Document,
    ServerToolUse,
    Other,
}

/// Classify one content block object.
pub fn classify_block(block: &Value) -> ContentBlock<'_> {
    let Some(object) = block.as_object() else {
        return ContentBlock::Other;
    };
    match object.get("type").and_then(Value::as_str) {
        Some("text") => ContentBlock::Text(
            object
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        ),
        Some("image") => ContentBlock::Image,
        Some("thinking") => ContentBlock::Thinking(
            object
                .get("thinking")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        ),
        Some("redacted_thinking") => ContentBlock::RedactedThinking,
        Some("tool_use") => ContentBlock::ToolUse {
            id: object.get("id").and_then(Value::as_str).unwrap_or_default(),
            name: object
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default(),
            input: object.get("input"),
        },
        Some("tool_result") => ContentBlock::ToolResult {
            tool_use_id: object.get("tool_use_id").and_then(Value::as_str),
            content: object.get("content"),
            is_error: object
                .get("is_error")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        },
        Some("document") => ContentBlock::Document,
        Some("server_tool_use") => ContentBlock::ServerToolUse,
        _ => ContentBlock::Other,
    }
}

/// Whether a tool definition is an Anthropic server-side tool (has a versioned type or a
/// recognized server tool name).
pub fn is_server_tool(tool: &Value) -> bool {
    let Some(object) = tool.as_object() else {
        return false;
    };
    if let Some(tool_type) = object.get("type").and_then(Value::as_str) {
        if tool_type != "custom" && !tool_type.is_empty() {
            return true;
        }
    }
    object
        .get("name")
        .and_then(Value::as_str)
        .is_some_and(is_known_server_tool_name)
}

/// Well-known Anthropic server tool names as sent by Claude Code / Anthropic SDKs.
pub fn is_known_server_tool_name(name: &str) -> bool {
    matches!(
        name,
        "web_search"
            | "web_fetch"
            | "code_execution"
            | "bash"
            | "str_replace_editor"
            | "str_replace_based_edit_tool"
            | "memory"
            | "tool_search_tool_regex"
            | "tool_search_tool_bm25"
    )
}

/// Short name for an Anthropic server tool, used when mapping to a function tool.
pub fn server_tool_function_name(tool: &Value) -> Option<String> {
    let object = tool.as_object()?;
    let name = object.get("name").and_then(Value::as_str)?;
    if is_known_server_tool_name(name) {
        return Some(name.to_string());
    }
    if let Some(tool_type) = object.get("type").and_then(Value::as_str) {
        for prefix in SERVER_TOOL_TYPE_PREFIXES {
            if let Some(short) = tool_type.strip_prefix(prefix) {
                return Some(short.to_string());
            }
        }
    }
    None
}

/// Render a tool_result content (string or block array) as a single string for a Chat `tool`
/// message: text blocks joined with blank lines; image blocks replaced with an omission marker
/// when the upstream does not accept images.
pub fn tool_result_to_string(content: Option<&Value>, image_input: bool) -> String {
    let Some(content) = content else {
        return String::new();
    };
    match content {
        Value::String(text) => text.clone(),
        Value::Array(blocks) => {
            let mut parts = Vec::new();
            for block in blocks {
                match classify_block(block) {
                    ContentBlock::Text(text) if !text.is_empty() => parts.push(text.to_string()),
                    ContentBlock::Image => parts.push(if image_input {
                        "[image content]".to_string()
                    } else {
                        "[image omitted: upstream does not support image input]".to_string()
                    }),
                    _ => {}
                }
            }
            parts.join("\n\n")
        }
        _ => content.to_string(),
    }
}

/// Text from a content value that may be a plain string or a list of text/image blocks.
/// Returns (text, has_image).
pub fn content_text_and_has_image(content: &Value) -> (String, bool) {
    match content {
        Value::String(text) => (text.clone(), false),
        Value::Array(blocks) => {
            let mut parts = Vec::new();
            let mut has_image = false;
            for block in blocks {
                match classify_block(block) {
                    ContentBlock::Text(text) if !text.is_empty() => parts.push(text.to_string()),
                    ContentBlock::Image => has_image = true,
                    _ => {}
                }
            }
            (parts.join("\n\n"), has_image)
        }
        _ => (String::new(), false),
    }
}

/// A minimal owned system-prompt representation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SystemPrompt {
    /// Text blocks in order, with billing attribution and empty blocks filtered out.
    pub texts: Vec<String>,
}

impl SystemPrompt {
    /// Parse a Messages `system` value (string or text-block array), dropping Claude Code
    /// billing attribution and empty text.
    pub fn parse(system: Option<&Value>) -> Self {
        let Some(system) = system else {
            return Self::default();
        };
        match system {
            Value::String(text) => Self::from_single(text),
            Value::Array(blocks) => {
                let mut texts = Vec::new();
                for block in blocks {
                    let Some(object) = block.as_object() else {
                        continue;
                    };
                    let Some(text) = object.get("text").and_then(Value::as_str) else {
                        continue;
                    };
                    if text.trim().is_empty() || text.starts_with(CLAUDE_BILLING_HEADER_PREFIX) {
                        continue;
                    }
                    texts.push(text.to_string());
                }
                Self { texts }
            }
            _ => Self::default(),
        }
    }

    fn from_single(text: &str) -> Self {
        let mut texts = Vec::new();
        if !text.trim().is_empty() && !text.starts_with(CLAUDE_BILLING_HEADER_PREFIX) {
            texts.push(text.to_string());
        }
        Self { texts }
    }

    pub fn is_empty(&self) -> bool {
        self.texts.is_empty()
    }

    pub fn joined(&self) -> String {
        self.texts.join("\n\n")
    }
}

/// Insert a string field into an object, reporting whether the value changed.
pub fn set_string_field(object: &mut Map<String, Value>, field: &str, value: &str) -> bool {
    match object.get(field) {
        Some(Value::String(current)) if current == value => false,
        _ => {
            object.insert(field.to_string(), Value::String(value.to_string()));
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn parses_system_prompt_and_drops_billing_and_empty() {
        let system = json!([
            {"type": "text", "text": "x-anthropic-billing-header: cc_version=2;"},
            {"type": "text", "text": "You are Claude Code"},
            {"type": "text", "text": "   "},
            {"type": "text", "text": "Second part"}
        ]);
        let parsed = SystemPrompt::parse(Some(&system));
        assert_eq!(
            parsed.texts,
            vec!["You are Claude Code".to_string(), "Second part".to_string()]
        );
        assert_eq!(parsed.joined(), "You are Claude Code\n\nSecond part");
    }

    #[test]
    fn classifies_content_blocks() {
        let blocks = json!([
            {"type": "text", "text": "hello"},
            {"type": "thinking", "thinking": "hmm", "signature": "sig"},
            {"type": "redacted_thinking", "data": "x"},
            {"type": "tool_use", "id": "toolu_1", "name": "Bash", "input": {"command": "ls"}},
            {"type": "tool_result", "tool_use_id": "toolu_1", "content": "ok", "is_error": false},
            {"type": "image", "source": {"type": "base64", "data": "..."}},
            {"type": "server_tool_use", "id": "st_1", "name": "web_search", "input": {}}
        ]);
        let classified: Vec<_> = blocks
            .as_array()
            .unwrap()
            .iter()
            .map(classify_block)
            .collect();
        assert!(matches!(classified[0], ContentBlock::Text("hello")));
        assert!(matches!(classified[1], ContentBlock::Thinking("hmm")));
        assert!(matches!(classified[2], ContentBlock::RedactedThinking));
        assert!(matches!(
            classified[3],
            ContentBlock::ToolUse {
                id: "toolu_1",
                name: "Bash",
                ..
            }
        ));
        assert!(matches!(
            classified[4],
            ContentBlock::ToolResult {
                tool_use_id: Some("toolu_1"),
                ..
            }
        ));
        assert!(matches!(classified[5], ContentBlock::Image));
        assert!(matches!(classified[6], ContentBlock::ServerToolUse));
    }

    #[test]
    fn detects_server_tools() {
        assert!(is_server_tool(
            &json!({"type": "web_search_20260209", "name": "web_search"})
        ));
        assert!(is_server_tool(
            &json!({"type": "code_execution_20260120", "name": "code_execution"})
        ));
        assert!(is_server_tool(
            &json!({"name": "bash", "description": "d", "input_schema": {}})
        ));
        assert!(!is_server_tool(
            &json!({"name": "Read", "description": "d", "input_schema": {}})
        ));
        assert!(!is_server_tool(
            &json!({"type": "custom", "name": "my_tool", "input_schema": {}})
        ));
    }

    #[test]
    fn maps_server_tool_to_function_name() {
        assert_eq!(
            server_tool_function_name(
                &json!({"type": "web_search_20260209", "name": "web_search"})
            ),
            Some("web_search".to_string())
        );
        assert_eq!(
            server_tool_function_name(&json!({"type": "web_fetch_20260209", "name": "web_fetch"})),
            Some("web_fetch".to_string())
        );
        assert_eq!(
            server_tool_function_name(&json!({"name": "code_execution", "description": "d"})),
            Some("code_execution".to_string())
        );
        assert_eq!(
            server_tool_function_name(&json!({"name": "Read", "description": "d"})),
            None
        );
    }

    #[test]
    fn flattens_tool_result_content() {
        let content = json!([
            {"type": "text", "text": "stdout line"},
            {"type": "image", "source": {"type": "base64", "data": "x"}},
            {"type": "text", "text": "done"}
        ]);
        assert_eq!(
            tool_result_to_string(Some(&content), false),
            "stdout line\n\n[image omitted: upstream does not support image input]\n\ndone"
        );
        assert_eq!(
            tool_result_to_string(Some(&content), true),
            "stdout line\n\n[image content]\n\ndone"
        );
        assert_eq!(tool_result_to_string(None, false), "");
    }
}
