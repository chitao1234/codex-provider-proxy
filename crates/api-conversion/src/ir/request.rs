use serde_json::Value;

use super::{
    content::ContentItem, extension::Extension, state::ConversationState, tools::ToolDefinition,
};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct GenerationControls {
    pub temperature: Option<Value>,
    pub top_p: Option<Value>,
    pub max_output_tokens: Option<Value>,
    pub stop: Option<Value>,
    pub reasoning: Option<Value>,
    pub thinking: Option<Value>,
    pub response_format: Option<Value>,
    pub parallel_tools: Option<Value>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RequestIr {
    pub model: Option<String>,
    pub stream: Option<bool>,
    pub instructions: Vec<ContentItem>,
    pub transcript: Vec<ContentItem>,
    pub tools: Vec<ToolDefinition>,
    pub tool_choice: Option<Value>,
    pub generation: GenerationControls,
    pub state: ConversationState,
    pub extensions: Vec<Extension>,
    pub opaque: Vec<Extension>,
}
