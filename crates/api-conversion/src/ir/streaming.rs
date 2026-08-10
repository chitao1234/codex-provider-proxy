use super::{
    content::ContentPart, extension::Extension, response::Usage, stop_reason::StopReason,
    tools::ToolCall,
};

#[derive(Debug, Clone, PartialEq)]
pub enum CanonicalResponseEvent {
    StreamStarted {
        response_id: Option<String>,
        model: Option<String>,
    },
    ItemStarted {
        item_id: String,
        item: ItemKind,
    },
    ItemMetadataUpdated {
        item_id: String,
        metadata: Vec<Extension>,
    },
    ContentDelta {
        item_id: String,
        content: ContentPart,
    },
    ToolCallStarted {
        item_id: String,
        call: ToolCall,
    },
    ToolArgumentsDelta {
        item_id: String,
        arguments: String,
    },
    UsageUpdated(Usage),
    ItemCompleted {
        item_id: String,
    },
    ResponseCompleted {
        stop_reason: StopReason,
        original_reason: Option<String>,
    },
    StreamError {
        message: String,
        code: Option<String>,
    },
    ProtocolExtension(Extension),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemKind {
    Message,
    Reasoning,
    Refusal,
    ToolCall,
    ToolResult,
    ServerTool,
    Unknown,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CoreUpdate {
    pub event: CanonicalResponseEvent,
    pub snapshot: super::response::ResponseIr,
    pub newly_completed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResponseSnapshot {
    pub response: super::response::ResponseIr,
    pub terminal: bool,
    pub failed: bool,
}
