use serde_json::Value;

use super::{
    content::ContentItem, extension::Extension, state::ConversationState, stop_reason::StopReason,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Usage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub cached_input_tokens: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResponseIr {
    pub response_id: Option<String>,
    pub model: Option<String>,
    pub output: Vec<ContentItem>,
    pub usage: Usage,
    pub stop_reason: Option<StopReason>,
    pub original_stop_reason: Option<String>,
    pub state: ConversationState,
    pub extensions: Vec<Extension>,
    pub opaque: Vec<Extension>,
    pub error: Option<Value>,
}
