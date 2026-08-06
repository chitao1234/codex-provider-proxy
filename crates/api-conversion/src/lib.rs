//! API format conversion between downstream client dialects and upstream provider dialects.
//!
//! The first supported conversion is Anthropic Messages (downstream, e.g. Claude Code) to
//! OpenAI Chat Completions (upstream). Providers opt in by declaring an upstream API and the
//! downstream APIs they accept; providers without conversion configuration keep the existing
//! transparent passthrough path.
//!
//! This crate is intentionally pure: no axum, no reqwest, no proxy runtime state. The proxy
//! crate owns networking, routing, configuration, and logging.

pub mod chat;
pub mod chat_parser;
pub mod dialect;
pub mod error;
pub mod messages;
pub mod messages_renderer;
pub mod messages_to_chat;
pub mod responses;
pub mod responses_renderer;
pub mod sse;
pub mod stream;

pub use dialect::{
    DownstreamApi, MaxTokensField, ModelCapabilities, ReasoningEffortConfig, ResponseFormatCap,
    ServerToolPolicy, ThinkingParam, UpstreamApi,
};
pub use error::ConversionError;
pub use messages_to_chat::{convert_chat_response, convert_messages_request};
pub use responses::{convert_chat_response_to_responses, convert_responses_request};
