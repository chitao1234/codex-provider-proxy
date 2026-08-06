//! Cross-protocol request rendering through the protocol-neutral request IR.

use serde_json::Value;

use crate::{
    dialect::ModelCapabilities,
    error::ConversionError,
    request_ir::{Protocol, RequestIr},
};

pub fn convert_chat_request_to_responses(body: &Value) -> Result<Value, ConversionError> {
    let request = RequestIr::parse(Protocol::OpenAiChatCompletions, body)?;
    request.render(Protocol::OpenAiResponses, &ModelCapabilities::default())
}

pub fn convert_chat_request_to_messages(body: &Value) -> Result<Value, ConversionError> {
    let request = RequestIr::parse(Protocol::OpenAiChatCompletions, body)?;
    request.render(Protocol::AnthropicMessages, &ModelCapabilities::default())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn renders_chat_request_as_responses_request() {
        let chat = json!({
            "model": "m",
            "max_tokens": 100,
            "stream": true,
            "messages": [
                {"role": "system", "content": "be concise"},
                {"role": "user", "content": "hello"},
                {"role": "assistant", "tool_calls": [{"id": "call_1", "function": {"name": "lookup", "arguments": "{}"}}]},
                {"role": "tool", "tool_call_id": "call_1", "content": "done"}
            ],
            "tools": [{"type": "function", "function": {"name": "lookup", "description": "look up", "parameters": {"type": "object"}}}]
        });
        let responses = convert_chat_request_to_responses(&chat).unwrap();
        assert_eq!(responses["instructions"], "be concise");
        assert_eq!(responses["max_output_tokens"], 100);
        assert_eq!(responses["input"][0]["type"], "message");
        assert_eq!(responses["input"][1]["type"], "function_call");
        assert_eq!(responses["input"][2]["type"], "function_call_output");
        assert_eq!(responses["tools"][0]["type"], "function");
    }

    #[test]
    fn renders_chat_request_as_messages_request() {
        let chat = json!({
            "model": "m",
            "max_completion_tokens": 100,
            "messages": [
                {"role": "system", "content": "be concise"},
                {"role": "user", "content": "hello"},
                {"role": "assistant", "tool_calls": [{"id": "call_1", "function": {"name": "lookup", "arguments": "{\"q\":\"x\"}"}}]},
                {"role": "tool", "tool_call_id": "call_1", "content": "done"}
            ],
            "tools": [{"type": "function", "function": {"name": "lookup", "parameters": {"type": "object"}}}]
        });
        let messages = convert_chat_request_to_messages(&chat).unwrap();
        assert_eq!(messages["system"], "be concise");
        assert_eq!(messages["max_tokens"], 100);
        assert_eq!(messages["messages"][0]["role"], "user");
        assert_eq!(messages["messages"][1]["content"][0]["type"], "tool_use");
        assert_eq!(messages["messages"][2]["content"][0]["type"], "tool_result");
        assert_eq!(messages["tools"][0]["input_schema"]["type"], "object");
    }

    #[test]
    fn preserves_unknown_chat_parameters() {
        let chat = json!({
            "model": "m",
            "messages": [],
            "provider_custom": {"budget": 42}
        });
        let responses = convert_chat_request_to_responses(&chat).unwrap();
        assert_eq!(responses["provider_custom"]["budget"], 42);
    }
}
