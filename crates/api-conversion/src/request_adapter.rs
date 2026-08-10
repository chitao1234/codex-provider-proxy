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

    fn parse_with_context(
        &self,
        body: &Value,
        previous: Option<&RequestIr>,
        last_reasoning: Option<&str>,
    ) -> Result<RequestIr, ConversionError> {
        if self.protocol() == Protocol::OpenAiResponses {
            RequestIr::parse_with_context(self.protocol(), body, previous, last_reasoning)
        } else {
            self.parse(body)
        }
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

    fn parse_with_context(
        &self,
        body: &Value,
        previous: Option<&RequestIr>,
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
}
