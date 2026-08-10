use serde_json::Value;

use super::{
    extension::Extension,
    tools::{ToolCall, ToolResult},
};

#[derive(Debug, Clone, PartialEq)]
pub enum ContentPart {
    Text(String),
    Refusal(String),
    Reasoning {
        text: String,
        signature: Option<String>,
    },
    Image {
        media_type: Option<String>,
        data: Option<String>,
        url: Option<String>,
    },
    Json(Value),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ContentItem {
    Message {
        role: String,
        content: Vec<ContentPart>,
        extensions: Vec<Extension>,
    },
    Refusal {
        id: Option<String>,
        content: Vec<ContentPart>,
        extensions: Vec<Extension>,
    },
    Reasoning {
        id: Option<String>,
        content: Vec<ContentPart>,
        extensions: Vec<Extension>,
    },
    ToolUse {
        id: Option<String>,
        call: ToolCall,
    },
    ToolResult {
        result: ToolResult,
    },
    ServerTool {
        id: Option<String>,
        kind: String,
        payload: Value,
        extensions: Vec<Extension>,
    },
    Unknown {
        id: Option<String>,
        kind: String,
        payload: Value,
        extensions: Vec<Extension>,
    },
}
