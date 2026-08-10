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
pub(crate) mod conversion_utils;
pub mod dialect;
pub mod error;
pub mod ir;
pub mod protocols;
pub(crate) mod provider_tools;
pub mod request_adapter;
pub mod request_ir;
pub(crate) mod server_tools;
pub mod sse;
