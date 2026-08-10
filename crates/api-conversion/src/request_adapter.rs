//! Protocol-owned request adapter boundaries.
//!
//! The adapters keep wire parsing/rendering dispatch protocol-local. Their implementation still
//! delegates to the established request semantic representation until the v2 field migration is
//! complete; callers must not dispatch by pairing protocol modules directly.

use serde_json::Value;

use crate::{
    dialect::{ModelCapabilities, RequestConversionReport},
    error::ConversionError,
    request_ir::{Protocol, RequestIr},
};

#[derive(Debug, Clone, PartialEq)]
pub struct RenderedRequest {
    pub body: Value,
    pub report: RequestConversionReport,
}

pub trait RequestAdapter {
    fn protocol(&self) -> Protocol;

    fn parse(&self, body: &Value) -> Result<RequestIr, ConversionError> {
        RequestIr::parse(self.protocol(), body)
    }

    fn parse_v2(&self, body: &Value) -> Result<crate::ir::RequestIr, ConversionError> {
        match self.protocol() {
            Protocol::OpenAiResponses => crate::protocols::openai_responses::parser::parse(body),
            Protocol::OpenAiChatCompletions => {
                crate::protocols::openai_chat_completions::parser::parse(body)
            }
            Protocol::AnthropicMessages => {
                crate::protocols::anthropic_messages::parser::parse(body)
            }
        }
    }

    fn parse_with_context(
        &self,
        body: &Value,
        previous: Option<&crate::ir::RequestIr>,
        _last_reasoning: Option<&str>,
    ) -> Result<RequestIr, ConversionError> {
        if self.protocol() == Protocol::OpenAiResponses {
            let _ = previous;
            self.parse(body)
        } else {
            self.parse(body)
        }
    }

    fn parse_v2_with_context(
        &self,
        body: &Value,
        previous: Option<&crate::ir::RequestIr>,
        last_reasoning: Option<&str>,
    ) -> Result<crate::ir::RequestIr, ConversionError> {
        let mut request = self.parse_v2(body)?;
        if self.protocol() == Protocol::OpenAiResponses {
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
                    request.transcript.push(crate::ir::ContentItem::Reasoning {
                        id: None,
                        content: vec![crate::ir::ContentPart::Reasoning {
                            text: reasoning.to_owned(),
                            signature: None,
                        }],
                        extensions: Vec::new(),
                    });
                }
            }
        }
        Ok(request)
    }

    fn render(
        &self,
        request: &RequestIr,
        target: Protocol,
        capabilities: &ModelCapabilities,
    ) -> Result<RenderedRequest, ConversionError> {
        let (body, report) = request.render_with_report(target, capabilities)?;
        Ok(RenderedRequest { body, report })
    }

    fn render_v2(
        &self,
        request: &crate::ir::RequestIr,
        target: Protocol,
        capabilities: &ModelCapabilities,
    ) -> Result<RenderedRequest, ConversionError> {
        let (body, report) = match target {
            Protocol::OpenAiResponses => {
                crate::protocols::openai_responses::request::render(request, capabilities)?
            }
            Protocol::AnthropicMessages => {
                crate::protocols::anthropic_messages::request::render(request, capabilities)?
            }
            Protocol::OpenAiChatCompletions => {
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
}

#[derive(Debug, Clone, Copy, Default)]
pub struct OpenAiResponsesRequestAdapter;

impl RequestAdapter for OpenAiResponsesRequestAdapter {
    fn protocol(&self) -> Protocol {
        Protocol::OpenAiResponses
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct OpenAiChatCompletionsRequestAdapter;

impl RequestAdapter for OpenAiChatCompletionsRequestAdapter {
    fn protocol(&self) -> Protocol {
        Protocol::OpenAiChatCompletions
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AnthropicMessagesRequestAdapter;

impl RequestAdapter for AnthropicMessagesRequestAdapter {
    fn protocol(&self) -> Protocol {
        Protocol::AnthropicMessages
    }
}

#[derive(Debug, Clone, Copy)]
pub enum RequestAdapterKind {
    OpenAiResponses(OpenAiResponsesRequestAdapter),
    OpenAiChatCompletions(OpenAiChatCompletionsRequestAdapter),
    AnthropicMessages(AnthropicMessagesRequestAdapter),
}

impl RequestAdapterKind {
    pub fn for_protocol(protocol: Protocol) -> Self {
        match protocol {
            Protocol::OpenAiResponses => Self::OpenAiResponses(Default::default()),
            Protocol::OpenAiChatCompletions => Self::OpenAiChatCompletions(Default::default()),
            Protocol::AnthropicMessages => Self::AnthropicMessages(Default::default()),
        }
    }
}

impl RequestAdapter for RequestAdapterKind {
    fn protocol(&self) -> Protocol {
        match self {
            Self::OpenAiResponses(adapter) => adapter.protocol(),
            Self::OpenAiChatCompletions(adapter) => adapter.protocol(),
            Self::AnthropicMessages(adapter) => adapter.protocol(),
        }
    }

    fn parse(&self, body: &Value) -> Result<RequestIr, ConversionError> {
        match self {
            Self::OpenAiResponses(adapter) => adapter.parse(body),
            Self::OpenAiChatCompletions(adapter) => adapter.parse(body),
            Self::AnthropicMessages(adapter) => adapter.parse(body),
        }
    }

    fn parse_v2(&self, body: &Value) -> Result<crate::ir::RequestIr, ConversionError> {
        match self {
            Self::OpenAiResponses(adapter) => adapter.parse_v2(body),
            Self::OpenAiChatCompletions(adapter) => adapter.parse_v2(body),
            Self::AnthropicMessages(adapter) => adapter.parse_v2(body),
        }
    }

    fn parse_with_context(
        &self,
        body: &Value,
        previous: Option<&crate::ir::RequestIr>,
        last_reasoning: Option<&str>,
    ) -> Result<RequestIr, ConversionError> {
        match self {
            Self::OpenAiResponses(adapter) => {
                adapter.parse_with_context(body, previous, last_reasoning)
            }
            Self::OpenAiChatCompletions(adapter) => {
                adapter.parse_with_context(body, previous, last_reasoning)
            }
            Self::AnthropicMessages(adapter) => {
                adapter.parse_with_context(body, previous, last_reasoning)
            }
        }
    }

    fn render_v2(
        &self,
        request: &crate::ir::RequestIr,
        target: Protocol,
        capabilities: &ModelCapabilities,
    ) -> Result<RenderedRequest, ConversionError> {
        match self {
            Self::OpenAiResponses(adapter) => adapter.render_v2(request, target, capabilities),
            Self::OpenAiChatCompletions(adapter) => {
                adapter.render_v2(request, target, capabilities)
            }
            Self::AnthropicMessages(adapter) => adapter.render_v2(request, target, capabilities),
        }
    }

    fn parse_v2_with_context(
        &self,
        body: &Value,
        previous: Option<&crate::ir::RequestIr>,
        last_reasoning: Option<&str>,
    ) -> Result<crate::ir::RequestIr, ConversionError> {
        match self {
            Self::OpenAiResponses(adapter) => {
                adapter.parse_v2_with_context(body, previous, last_reasoning)
            }
            Self::OpenAiChatCompletions(adapter) => {
                adapter.parse_v2_with_context(body, previous, last_reasoning)
            }
            Self::AnthropicMessages(adapter) => {
                adapter.parse_v2_with_context(body, previous, last_reasoning)
            }
        }
    }
}
