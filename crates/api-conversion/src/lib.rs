//! API format conversion between downstream client dialects and upstream provider dialects.
//!
//! Requests are parsed into a protocol-neutral IR and rendered as Anthropic Messages, OpenAi
//! Chat Completions, or OpenAi Responses. Unknown top-level/provider fields remain attached to
//! the request, while protocol-owned fields, ordered conversation items, cache affinity, and
//! continuation state are mapped explicitly at protocol boundaries.
//!
//! This crate is intentionally pure: no axum, no reqwest, no proxy runtime state. The proxy
//! crate owns networking, routing, configuration, and logging.

pub(crate) mod chat;
pub mod chat_parser;
pub mod chat_to_messages;
pub(crate) mod conversion_utils;
pub mod dialect;
pub mod error;
pub mod messages_parser;
pub mod messages_renderer;
pub mod request_ir;
pub mod response_ir;
pub mod responses;
pub mod responses_parser;
pub mod responses_renderer;
pub(crate) mod server_tools;
pub mod sse;
pub mod stream;
