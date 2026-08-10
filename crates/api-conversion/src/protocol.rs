#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    AnthropicMessages,
    OpenAiChatCompletions,
    OpenAiResponses,
}
