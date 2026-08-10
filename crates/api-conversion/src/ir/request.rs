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
    pub stop_sequences: Option<Value>,
    pub reasoning: Option<Value>,
    pub reasoning_effort: Option<Value>,
    pub thinking: Option<Value>,
    pub text: Option<Value>,
    pub output_config: Option<Value>,
    pub parallel_tool_use: Option<Value>,
    pub parallel_tool_calls: Option<Value>,
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
