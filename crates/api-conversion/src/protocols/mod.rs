pub mod anthropic_messages;
pub mod openai_chat_completions;
pub mod openai_responses;
pub(crate) mod request_support;

use serde_json::Value;

use crate::{error::ConversionError, ir::ResponseIr, protocol::Protocol};

pub fn parse_response(protocol: Protocol, body: &Value) -> Result<ResponseIr, ConversionError> {
    let mut response = match protocol {
        Protocol::OpenAiResponses => openai_responses::response::parse(body),
        Protocol::AnthropicMessages => anthropic_messages::response::parse(body),
        Protocol::OpenAiChatCompletions => openai_chat_completions::response::parse(body),
    }?;
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
    match protocol {
        Protocol::OpenAiResponses => Ok(openai_responses::renderer::render(response)),
        Protocol::AnthropicMessages => Ok(anthropic_messages::renderer::render(response)),
        Protocol::OpenAiChatCompletions => Err(ConversionError::unsupported(
            "response",
            "Chat Completions is not a downstream response format",
        )),
    }
}
