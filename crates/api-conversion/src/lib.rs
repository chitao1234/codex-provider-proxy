//! API format conversion between downstream client dialects and upstream provider dialects.
//!
//! Requests are parsed into a protocol-neutral IR and rendered as Anthropic Messages, OpenAi
//! Chat Completions, or OpenAi Responses. Unknown top-level/provider fields remain attached to
//! the request, while protocol-owned fields, ordered conversation items, cache affinity, and
//! continuation state are mapped explicitly at protocol boundaries.
//!
//! This crate is intentionally pure: no axum, no reqwest, no proxy runtime state. The proxy
//! crate owns networking, routing, configuration, and logging.

pub mod dialect;
pub mod error;
pub mod ir;
pub mod plan;
pub mod protocol;
pub mod protocols;
pub mod provider_normalizer;
pub(crate) mod provider_tools;
pub mod request_adapter;
pub mod sse;
