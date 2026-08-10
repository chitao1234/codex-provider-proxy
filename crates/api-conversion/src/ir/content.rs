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
    Extended {
        part: Box<ContentPart>,
        extensions: Vec<Extension>,
    },
    Json(Value),
}

impl ContentPart {
    pub fn with_extensions(self, extensions: Vec<Extension>) -> Self {
        if extensions.is_empty() {
            self
        } else {
            Self::Extended {
                part: Box::new(self),
                extensions,
            }
        }
    }

    pub fn extensions(&self) -> &[Extension] {
        match self {
            Self::Extended { extensions, .. } => extensions,
            _ => &[],
        }
    }

    pub fn semantic(&self) -> &Self {
        match self {
            Self::Extended { part, .. } => part.semantic(),
            part => part,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ContentItem {
    Message {
        id: Option<String>,
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

impl ContentItem {
    pub fn extensions(&self) -> &[Extension] {
        match self {
            Self::Message { extensions, .. }
            | Self::Refusal { extensions, .. }
            | Self::Reasoning { extensions, .. }
            | Self::ServerTool { extensions, .. }
            | Self::Unknown { extensions, .. } => extensions,
            Self::ToolUse { call, .. } => &call.extensions,
            Self::ToolResult { result } => &result.extensions,
        }
    }
}
