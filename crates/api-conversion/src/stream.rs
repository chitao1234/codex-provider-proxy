//! Protocol-independent streaming response events (the semantic IR).
//!
//! Upstream protocol parsers (Chat Completions, Anthropic Messages, third-party
//! Responses) translate their SSE streams into `StreamEvent`s; downstream renderers
//! (Anthropic Messages, official OpenAI Responses) translate the event stream back
//! into their protocol's SSE. This decouples N upstream protocols from M downstream
//! protocols: N parsers + M renderers instead of N×M converters, and the semantic
//! IR preserves meaning that a Chat-Completions-shaped intermediate would lose
//! (thinking signatures, item ids, server-tool calls, usage granularity).

use crate::chat::ChatUsage;

/// Why the response ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// Natural end (Anthropic `end_turn` / chat `stop`).
    Stop,
    /// The model requested tool calls (Anthropic `tool_use` / chat `tool_calls`).
    ToolUse,
    /// Output token budget exhausted (Anthropic `max_tokens` / chat `length`).
    MaxTokens,
    /// Content filter / safety stop.
    ContentFilter,
    /// Any other reason.
    Other,
}

/// One protocol-independent streaming response event.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    /// The response started; carries the upstream id/model for downstream id synthesis.
    Start { id: String, model: String },
    /// Increment of reasoning/thinking text. `signature` is present when the upstream
    /// protocol carries one (Anthropic Messages thinking blocks).
    ReasoningDelta {
        text: String,
        signature: Option<String>,
    },
    /// Increment of visible output text.
    TextDelta { text: String },
    /// A tool call began (id + name known; arguments may follow incrementally).
    ToolCallStart {
        index: usize,
        id: String,
        name: String,
    },
    /// Increment of a tool call's arguments (never cumulative).
    ToolCallArgsDelta { index: usize, args: String },
    /// The response ended, with the stop reason and final usage.
    End {
        stop_reason: StopReason,
        usage: ChatUsage,
    },
    /// An upstream stream error; the renderer terminates without faking a completion.
    Error {
        message: String,
        code: Option<String>,
    },
}

impl StreamEvent {
    /// The upstream id of a Start event (default empty).
    pub fn start_id(&self) -> &str {
        match self {
            StreamEvent::Start { id, .. } => id,
            _ => "",
        }
    }
}

/// Map a Chat Completions `finish_reason` to the semantic stop reason.
pub fn map_chat_finish_reason(reason: Option<&str>, saw_tool_call: bool) -> StopReason {
    match reason {
        Some("stop") => StopReason::Stop,
        Some("tool_calls") if saw_tool_call => StopReason::ToolUse,
        Some("tool_calls") => StopReason::ToolUse,
        Some("length") => StopReason::MaxTokens,
        Some("content_filter") => StopReason::ContentFilter,
        _ => StopReason::Other,
    }
}
