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

impl GenerationControls {
    pub(crate) fn effort(&self) -> Option<&str> {
        self.output_config
            .as_ref()
            .and_then(Value::as_object)
            .and_then(|config| config.get("effort"))
            .or_else(|| {
                self.reasoning
                    .as_ref()
                    .and_then(Value::as_object)
                    .and_then(|reasoning| reasoning.get("effort"))
            })
            .or(self.reasoning_effort.as_ref())
            .and_then(Value::as_str)
    }

    pub(crate) fn thinking_type(&self) -> Option<&str> {
        self.thinking
            .as_ref()
            .and_then(|thinking| thinking.get("type"))
            .and_then(Value::as_str)
    }

    pub(crate) fn output_format(&self) -> Option<&Value> {
        self.output_config
            .as_ref()
            .and_then(Value::as_object)
            .and_then(|config| config.get("format"))
            .or_else(|| {
                self.text
                    .as_ref()
                    .and_then(Value::as_object)
                    .and_then(|text| text.get("format"))
            })
    }

    pub(crate) fn stop_values(&self) -> Vec<Value> {
        let Some(stop) = self.stop_sequences.as_ref().or(self.stop.as_ref()) else {
            return Vec::new();
        };
        match stop {
            Value::String(value) if !value.is_empty() => vec![Value::String(value.clone())],
            Value::Array(values) => values
                .iter()
                .filter(|value| value.as_str().is_some_and(|value| !value.is_empty()))
                .cloned()
                .collect(),
            _ => Vec::new(),
        }
    }
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

impl RequestIr {
    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    pub(crate) fn last_user_text(&self) -> Option<String> {
        self.transcript.iter().rev().find_map(|item| {
            let ContentItem::Message { role, content, .. } = item else {
                return None;
            };
            (role == "user").then(|| {
                content
                    .iter()
                    .filter_map(|part| match part.semantic() {
                        super::ContentPart::Text(text) => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            })
        })
    }

    pub fn append_response(&mut self, response: &super::response::ResponseIr) {
        self.transcript.extend(response.output.clone());
        if self.state.source_response_id.is_none() {
            self.state
                .source_response_id
                .clone_from(&response.response_id);
        }
        self.extensions.extend(response.extensions.clone());
        self.opaque.extend(response.opaque.clone());
    }
}
