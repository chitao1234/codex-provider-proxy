use serde_json::Value;

use crate::{
    dialect::{ModelCapabilities, RequestConversionReport},
    error::ConversionError,
    protocol::Protocol,
};

#[derive(Debug, Clone, PartialEq)]
pub struct RenderedRequest {
    pub body: Value,
    pub report: RequestConversionReport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestParserKind {
    OpenAiResponses,
    OpenAiChatCompletions,
    AnthropicMessages,
}

impl RequestParserKind {
    pub fn for_protocol(protocol: Protocol) -> Self {
        match protocol {
            Protocol::OpenAiResponses => Self::OpenAiResponses,
            Protocol::OpenAiChatCompletions => Self::OpenAiChatCompletions,
            Protocol::AnthropicMessages => Self::AnthropicMessages,
        }
    }

    pub fn protocol(self) -> Protocol {
        match self {
            Self::OpenAiResponses => Protocol::OpenAiResponses,
            Self::OpenAiChatCompletions => Protocol::OpenAiChatCompletions,
            Self::AnthropicMessages => Protocol::AnthropicMessages,
        }
    }

    pub fn parse(self, body: &Value) -> Result<crate::ir::RequestIr, ConversionError> {
        match self {
            Self::OpenAiResponses => crate::protocols::openai_responses::parser::parse(body),
            Self::OpenAiChatCompletions => {
                crate::protocols::openai_chat_completions::parser::parse(body)
            }
            Self::AnthropicMessages => crate::protocols::anthropic_messages::parser::parse(body),
        }
    }

    pub fn parse_with_context(
        self,
        body: &Value,
        previous: Option<&crate::ir::RequestIr>,
        last_reasoning: Option<&str>,
    ) -> Result<crate::ir::RequestIr, ConversionError> {
        let mut request = self.parse(body)?;
        if self == Self::OpenAiResponses {
            if let Some(previous) = previous {
                request.transcript.splice(0..0, previous.transcript.clone());
                inherit_state(&mut request.state, &previous.state);
            }
            if let Some(reasoning) = last_reasoning.filter(|value| !value.trim().is_empty()) {
                let has_reasoning = request
                    .transcript
                    .iter()
                    .any(|item| matches!(item, crate::ir::ContentItem::Reasoning { .. }));
                if !has_reasoning {
                    // Reattach the reasoning at the position of the assistant turn it
                    // belongs to: immediately before the first function_call_output of
                    // the current continuation (the tool result of that turn). Appending
                    // at the end would leave an isolated thinking-only assistant message
                    // between turns (Messages upstreams).
                    let reasoning_item = crate::ir::ContentItem::Reasoning {
                        id: None,
                        content: vec![crate::ir::ContentPart::Reasoning {
                            text: reasoning.to_owned(),
                            signature: None,
                        }],
                        extensions: Vec::new(),
                    };
                    let insert_at = request
                        .transcript
                        .iter()
                        .position(|item| matches!(item, crate::ir::ContentItem::ToolResult { .. }))
                        .unwrap_or(request.transcript.len());
                    request.transcript.insert(insert_at, reasoning_item);
                }
            }
        }
        Ok(request)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestRendererKind {
    OpenAiResponses,
    OpenAiChatCompletions,
    AnthropicMessages,
}

impl RequestRendererKind {
    pub fn for_protocol(protocol: Protocol) -> Self {
        match protocol {
            Protocol::OpenAiResponses => Self::OpenAiResponses,
            Protocol::OpenAiChatCompletions => Self::OpenAiChatCompletions,
            Protocol::AnthropicMessages => Self::AnthropicMessages,
        }
    }

    pub fn protocol(self) -> Protocol {
        match self {
            Self::OpenAiResponses => Protocol::OpenAiResponses,
            Self::OpenAiChatCompletions => Protocol::OpenAiChatCompletions,
            Self::AnthropicMessages => Protocol::AnthropicMessages,
        }
    }

    pub fn render(
        self,
        request: &crate::ir::RequestIr,
        capabilities: &ModelCapabilities,
    ) -> Result<RenderedRequest, ConversionError> {
        let (body, report) = match self {
            Self::OpenAiResponses => {
                crate::protocols::openai_responses::request::render(request, capabilities)?
            }
            Self::AnthropicMessages => {
                crate::protocols::anthropic_messages::request::render(request, capabilities)?
            }
            Self::OpenAiChatCompletions => {
                crate::protocols::openai_chat_completions::request::render(request, capabilities)?
            }
        };
        Ok(RenderedRequest { body, report })
    }
}

fn inherit_state(
    current: &mut crate::ir::ConversationState,
    previous: &crate::ir::ConversationState,
) {
    if current.conversation.is_none() {
        current.conversation.clone_from(&previous.conversation);
    }
    if current.logical_conversation_id.is_none() {
        current
            .logical_conversation_id
            .clone_from(&previous.logical_conversation_id);
    }
    if current.identity.user_id.is_none() {
        current
            .identity
            .user_id
            .clone_from(&previous.identity.user_id);
    }
    if current.identity.session_id.is_none() {
        current
            .identity
            .session_id
            .clone_from(&previous.identity.session_id);
    }
    if current.cache.key.is_none() {
        current.cache.key.clone_from(&previous.cache.key);
    }
    current.extensions = previous
        .extensions
        .iter()
        .chain(&current.extensions)
        .cloned()
        .collect();
    let mut provenance = previous.provenance.clone();
    for entry in &current.provenance {
        if !provenance.contains(entry) {
            provenance.push(entry.clone());
        }
    }
    current.provenance = provenance;
}
