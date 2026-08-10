pub mod anthropic_messages;
pub mod openai_chat_completions;
pub mod openai_responses;
pub(crate) mod request_support;

use bytes::Bytes;
use serde_json::Value;

use crate::{
    error::ConversionError,
    ir::{CanonicalResponseEvent, CoreUpdate, ResponseIr},
    protocol::Protocol,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseParserKind {
    OpenAiResponses,
    OpenAiChatCompletions,
    AnthropicMessages,
}

impl ResponseParserKind {
    pub fn for_protocol(protocol: Protocol) -> Self {
        match protocol {
            Protocol::OpenAiResponses => Self::OpenAiResponses,
            Protocol::OpenAiChatCompletions => Self::OpenAiChatCompletions,
            Protocol::AnthropicMessages => Self::AnthropicMessages,
        }
    }

    pub fn parse(self, body: &Value) -> Result<ResponseIr, ConversionError> {
        match self {
            Self::OpenAiResponses => openai_responses::response::parse(body),
            Self::OpenAiChatCompletions => openai_chat_completions::response::parse(body),
            Self::AnthropicMessages => anthropic_messages::response::parse(body),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseRendererKind {
    OpenAiResponses,
    AnthropicMessages,
}

impl ResponseRendererKind {
    pub fn for_protocol(protocol: Protocol) -> Result<Self, ConversionError> {
        match protocol {
            Protocol::OpenAiResponses => Ok(Self::OpenAiResponses),
            Protocol::AnthropicMessages => Ok(Self::AnthropicMessages),
            Protocol::OpenAiChatCompletions => Err(ConversionError::unsupported(
                "response",
                "Chat Completions is not a downstream response format",
            )),
        }
    }

    pub fn render(self, response: &ResponseIr) -> Value {
        match self {
            Self::OpenAiResponses => openai_responses::renderer::render(response),
            Self::AnthropicMessages => anthropic_messages::renderer::render(response),
        }
    }
}

pub enum StreamParserKind {
    OpenAiResponses(openai_responses::stream::Parser),
    OpenAiChatCompletions(openai_chat_completions::stream::Parser),
    AnthropicMessages(anthropic_messages::stream::Parser),
}

impl StreamParserKind {
    pub fn for_protocol(protocol: Protocol) -> Self {
        match protocol {
            Protocol::OpenAiResponses => Self::OpenAiResponses(Default::default()),
            Protocol::OpenAiChatCompletions => Self::OpenAiChatCompletions(Default::default()),
            Protocol::AnthropicMessages => Self::AnthropicMessages(Default::default()),
        }
    }

    pub fn on_event(
        &mut self,
        event_type: &str,
        payload: &str,
        out: &mut Vec<CanonicalResponseEvent>,
    ) -> Result<(), ConversionError> {
        match self {
            Self::OpenAiResponses(parser) => parser.on_event(event_type, payload, out),
            Self::OpenAiChatCompletions(parser) => parser.on_chunk(payload, out),
            Self::AnthropicMessages(parser) => parser.on_event(event_type, payload, out),
        }
    }

    pub fn finish(&mut self, out: &mut Vec<CanonicalResponseEvent>) {
        match self {
            Self::OpenAiResponses(parser) => parser.finish(out),
            Self::OpenAiChatCompletions(parser) => parser.finish(out),
            Self::AnthropicMessages(parser) => parser.finish(out),
        }
    }
}

pub enum StreamRendererKind {
    OpenAiResponses(openai_responses::stream::Renderer),
    AnthropicMessages(anthropic_messages::stream::Renderer),
}

impl StreamRendererKind {
    pub fn for_protocol(protocol: Protocol) -> Result<Self, ConversionError> {
        match protocol {
            Protocol::OpenAiResponses => Ok(Self::OpenAiResponses(Default::default())),
            Protocol::AnthropicMessages => Ok(Self::AnthropicMessages(Default::default())),
            Protocol::OpenAiChatCompletions => Err(ConversionError::unsupported(
                "response",
                "Chat Completions is not a downstream response format",
            )),
        }
    }

    pub fn on_update(&mut self, update: &CoreUpdate, out: &mut Vec<Bytes>) {
        match self {
            Self::OpenAiResponses(renderer) => renderer.on_update(update, out),
            Self::AnthropicMessages(renderer) => renderer.on_update(update, out),
        }
    }

    pub fn finish(&mut self, out: &mut Vec<Bytes>) {
        match self {
            Self::OpenAiResponses(renderer) => renderer.finish(out),
            Self::AnthropicMessages(renderer) => renderer.finish(out),
        }
    }

    pub fn response_id(&self) -> Option<&str> {
        match self {
            Self::OpenAiResponses(renderer) => renderer.response_id(),
            Self::AnthropicMessages(_) => None,
        }
    }
}

pub fn parse_response(protocol: Protocol, body: &Value) -> Result<ResponseIr, ConversionError> {
    let mut response = ResponseParserKind::for_protocol(protocol).parse(body)?;
    response
        .state
        .source_response_id
        .clone_from(&response.response_id);
    response.usage.normalize();
    Ok(response)
}

pub fn render_response(
    protocol: Protocol,
    response: &ResponseIr,
) -> Result<Value, ConversionError> {
    Ok(ResponseRendererKind::for_protocol(protocol)?.render(response))
}
