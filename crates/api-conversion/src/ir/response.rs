use serde_json::Value;

use super::{
    content::ContentItem, extension::Extension, state::ConversationState, stop_reason::StopReason,
};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Usage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub cached_input_tokens: Option<u64>,
    pub cache_write_input_tokens: Option<u64>,
    pub cache_miss_input_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub extensions: Vec<Extension>,
}

impl Usage {
    pub fn normalize(&mut self) {
        if self.total_tokens.is_none() {
            self.total_tokens = self
                .input_tokens
                .zip(self.output_tokens)
                .and_then(|(input, output)| input.checked_add(output));
        }
    }
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
    pub error: Option<Value>,
}
