# API Conversion Layer Plan

## Goal

Add a universal API conversion layer to `codex-provider-proxy` so downstream clients can speak one model API dialect while selected upstream providers speak another. The first supported conversion is:

```text
OpenAI Responses API consumer -> Chat Completions API provider
```

The conversion layer should be reusable for future API bridges, such as Chat Completions to Responses, Anthropic Messages to OpenAI Responses, or OpenAI-compatible dialect normalization.

## Design Decision

Implement conversion logic in a new workspace library crate and consume it from the main proxy crate.

Recommended crate name:

```text
crates/api-conversion/
```

Recommended package name:

```text
codex-provider-proxy-api-conversion
```

This keeps the proxy responsible for networking, routing, retries, configuration, auth rewriting, and logging, while the new crate owns API-shape translation and conversion-specific error formatting.

## Responsibilities

### New Conversion Crate

The new crate should own:

- API dialect enums and conversion kinds.
- Minimal OpenAI Responses request/response structs.
- Minimal Chat Completions request/response structs.
- Responses-to-Chat request conversion.
- Chat-to-Responses response conversion.
- Chat SSE-to-Responses SSE streaming conversion.
- Conversion-specific error types and OpenAI-style error payloads.
- Shared SSE parsing and formatting utilities.
- Unit tests for all mapping rules.

The crate should avoid dependencies on:

- `axum`
- `reqwest`
- proxy runtime state
- PID routing
- exchange logging
- provider config types

Keeping this crate mostly pure makes it easy to test conversion behavior without running a local HTTP server.

### Proxy Crate

The existing `crates/proxy` crate should continue to own:

- PID-based provider selection.
- Default provider selection.
- Provider URL construction.
- Authorization header rewrite.
- Incoming and outgoing header filtering.
- Transparent retries.
- Idle timeouts.
- Request and response body capture.
- Exchange logs.
- Config loading and hot reload.

The proxy should call into the conversion crate only after it has selected a provider and knows that provider's upstream API dialect.

## High-Level Flow

Current transparent flow:

```text
incoming request
  -> strip listen_base_path
  -> resolve client PID
  -> resolve provider
  -> build upstream URL
  -> rewrite Authorization
  -> stream request upstream
  -> stream response downstream
```

Converted flow:

```text
incoming request
  -> strip listen_base_path
  -> resolve client PID
  -> resolve provider
  -> decide whether selected provider needs conversion
  -> buffer downstream JSON request body
  -> convert downstream request to upstream request
  -> build upstream URL using converted path
  -> rewrite Authorization
  -> send converted request upstream
  -> convert upstream response body or stream
  -> return converted response downstream
```

The transparent flow must remain unchanged for providers that do not opt into conversions.

## Workspace Changes

Add a new member to the root `Cargo.toml`:

```toml
[workspace]
members = [
  "crates/pid-resolver",
  "crates/proxy",
  "crates/rpc-types",
  "crates/proxyctl",
  "crates/api-conversion",
]
```

Add the dependency to `crates/proxy/Cargo.toml`:

```toml
[dependencies]
codex-provider-proxy-api-conversion = { path = "../api-conversion" }
```

The new crate should initially depend only on shared lightweight dependencies:

```toml
[dependencies]
bytes.workspace = true
futures-util.workspace = true
http.workspace = true
serde.workspace = true
serde_json.workspace = true
```

Add `tokio` only if streaming tests or async stream utilities require it.

## Suggested Crate Layout

```text
crates/api-conversion/
  Cargo.toml
  src/
    lib.rs
    dialect.rs
    error.rs
    http.rs
    sse.rs
    openai/
      mod.rs
      responses.rs
      chat_completions.rs
      responses_to_chat.rs
      chat_to_responses.rs
```

### Module Purpose

- `dialect.rs` defines `ApiDialect`, `ConversionKind`, and provider-independent conversion selection types.
- `error.rs` defines `ConversionError`, unsupported-feature errors, invalid-request errors, and JSON error rendering.
- `http.rs` defines small generic HTTP request/response containers that use `http`, `bytes`, and `HeaderMap`.
- `sse.rs` defines incremental Server-Sent Events parsing and encoding.
- `openai/responses.rs` defines minimal Responses API serde models needed by conversion.
- `openai/chat_completions.rs` defines minimal Chat Completions serde models needed by conversion.
- `openai/responses_to_chat.rs` maps downstream Responses requests into upstream Chat requests.
- `openai/chat_to_responses.rs` maps upstream Chat responses or streams back into Responses responses or events.

## Public API Sketch

The first public API should stay small and evolve as streaming/state is added:

```rust
pub enum ApiDialect {
    OpenAIResponses,
    OpenAiChatCompletions,
}

pub enum ConversionKind {
    OpenAIResponsesToChatCompletions,
}

pub struct ConversionOptions {
    pub instruction_role: InstructionRole,
    pub max_tokens_field: MaxTokensField,
    pub strict_tools_default: bool,
}

pub struct DownstreamRequest {
    pub method: http::Method,
    pub path: String,
    pub query: Option<String>,
    pub headers: http::HeaderMap,
    pub body: bytes::Bytes,
}

pub struct UpstreamRequest {
    pub method: http::Method,
    pub path: String,
    pub query: Option<String>,
    pub headers: http::HeaderMap,
    pub body: bytes::Bytes,
    pub response_conversion: ResponseConversion,
}

pub enum ResponseConversion {
    None,
    ChatCompletionsToResponses {
        stream: bool,
    },
}
```

Initial pure conversion functions:

```rust
pub fn responses_to_chat_request(
    request: DownstreamRequest,
    options: &ConversionOptions,
) -> Result<UpstreamRequest, ConversionError>;

pub fn chat_response_to_responses_body(
    body: bytes::Bytes,
) -> Result<bytes::Bytes, ConversionError>;
```

Streaming can be added after non-streaming conversion works:

```rust
pub fn chat_sse_to_responses_sse<S>(
    stream: S,
) -> impl futures_util::Stream<Item = Result<bytes::Bytes, ConversionError>>
where
    S: futures_util::Stream<Item = Result<bytes::Bytes, ConversionError>>;
```

The exact stream error bound may need adjustment during implementation so it composes cleanly with `reqwest::Response::bytes_stream()`.

## Provider Configuration

Add provider-level API dialect configuration while preserving existing config compatibility.

Suggested initial config:

```toml
[providers.default]
base_url = "https://api.openai.com/v1"
api_key = "..."
upstream_api = "openai_responses"

[providers.chat_provider]
base_url = "https://example.com/v1"
api_key = "..."
upstream_api = "openai_chat_completions"
accept_downstream_apis = ["openai_responses"]
instruction_role = "system"
max_tokens_field = "max_tokens"
```

Defaults:

- `upstream_api = "passthrough"` or equivalent behavior if omitted.
- `accept_downstream_apis = []` if omitted.
- Existing providers continue to proxy transparently.

Recommended Rust config additions:

```rust
pub struct Provider {
    pub base_url: Url,
    pub api_key: String,
    pub authorization_header: Option<String>,
    pub upstream_api: ProviderApi,
    pub accept_downstream_apis: Vec<DownstreamApi>,
    pub conversion_options: ConversionOptions,
}
```

Keep proxy-local config enums in `crates/proxy` and convert them into conversion-crate option types at the call site. This avoids making the conversion crate depend on proxy config parsing.

## Conversion Selection

The proxy should decide conversion based on:

- Incoming method.
- Incoming path after `listen_base_path` stripping.
- Selected provider upstream API.
- Selected provider accepted downstream APIs.

Initial rule:

```text
if method == POST
and forwarded_path == "/responses" or "responses"
and selected_provider.upstream_api == openai_chat_completions
and selected_provider.accept_downstream_apis contains openai_responses
then use OpenAIResponsesToChatCompletions
else pass through
```

The exact forwarded path comparison must account for current `strip_listen_base_path` behavior, which can return either `"/"` or a path without a leading slash for nested paths.

## Request Mapping: Responses to Chat

### Endpoint

Map:

```text
POST /v1/responses
```

to:

```text
POST /v1/chat/completions
```

The upstream URL must still be joined with provider `base_url` exactly like transparent proxying.

### Body Fields

Recommended mapping:

| Responses request field | Chat Completions request field | Notes |
|---|---|---|
| `model` | `model` | Add model mapping later if needed. |
| `instructions` | leading `developer` or `system` message | Provider-configurable. |
| `input` string | `messages: [{ role: "user", content: input }]` | MVP path. |
| `input` message array | `messages` | Preserve order. |
| `input_text` content part | text content part or plain text | Prefer preserving content parts. |
| `input_image` content part | image content part | Only if provider supports image inputs. |
| `function_call_output` item | `tool` role message | `call_id` maps to `tool_call_id`. |
| `tools` function definitions | `tools` function definitions | Wrap as Chat function tools. |
| `tool_choice` | `tool_choice` | Map `auto`, `none`, `required`, and named functions. |
| `parallel_tool_calls` | `parallel_tool_calls` | Capability-gated if provider compatibility is uncertain. |
| `text.format` | `response_format` | Map JSON schema and JSON object formats. |
| `max_output_tokens` | `max_completion_tokens` or `max_tokens` | Provider-configurable. |
| `temperature` | `temperature` | Copy if present. |
| `top_p` | `top_p` | Copy if present. |
| `stop` | `stop` | Copy if present. |
| `seed` | `seed` | Copy if present. |
| `user` | `user` | Copy if present. |
| `metadata` | `metadata` | Copy if provider supports it. |
| `service_tier` | `service_tier` | Copy if provider supports it. |
| `reasoning.effort` | `reasoning_effort` | Copy only when compatible. |

### Unsupported Fields

The converter should reject unsupported features explicitly rather than silently dropping them.

Examples:

- hosted tools such as web search, file search, or computer use
- `background`
- unsupported `include` values
- unsupported `conversation` references
- unsupported encrypted reasoning fields
- unsupported multimodal content for a text-only provider

Recommended error status:

- `400 Bad Request` when a known field cannot be represented for the selected conversion.
- `501 Not Implemented` when the conversion as a whole is not implemented.

Recommended error payload:

```json
{
  "error": {
    "message": "This proxy cannot convert Responses field `tools[0].type` to Chat Completions for this provider.",
    "type": "unsupported_conversion_feature",
    "param": "tools[0].type",
    "code": "responses_to_chat_unsupported_tool"
  }
}
```

## Response Mapping: Chat to Responses

### Non-Streaming

Map upstream Chat response JSON into downstream Responses response JSON.

Recommended mapping:

| Chat response field | Responses response field | Notes |
|---|---|---|
| `id` | generated `resp_*` ID | Store upstream ID in metadata if state is implemented. |
| `object: "chat.completion"` | `object: "response"` | Synthetic Responses object. |
| `created` | `created_at` | Preserve timestamp. |
| `model` | `model` | Preserve upstream model. |
| `choices[0].message.content` | `output[].content[].text` | Use `output_text` content item. |
| `choices[0].message.tool_calls[]` | `output[]` function call items | Preserve function name and arguments. |
| `tool_call.id` | `call_id` | Important for tool result correlation. |
| `finish_reason: "stop"` | `status: "completed"` | Normal completion. |
| `finish_reason: "length"` | `status: "incomplete"` | Include an incomplete reason. |
| `finish_reason: "tool_calls"` | `status: "completed"` | Output contains function calls. |
| `usage.prompt_tokens` | `usage.input_tokens` | Rename. |
| `usage.completion_tokens` | `usage.output_tokens` | Rename. |
| `usage.total_tokens` | `usage.total_tokens` | Preserve. |

When converting the response, the proxy must not transparently copy incompatible upstream headers such as `content-length` from the original Chat response.

### Streaming

Map upstream Chat Completions SSE chunks into downstream Responses SSE events.

Required behavior:

- Parse upstream SSE incrementally.
- Treat upstream `data: [DONE]` as an upstream terminator, not as a downstream payload.
- Emit `response.created` before output deltas.
- Map `choices[].delta.content` into `response.output_text.delta`.
- Accumulate streamed tool-call fragments by tool call index.
- Map streamed function-call argument fragments into Responses function-call argument delta events.
- Emit final output item completion events where needed.
- Emit `response.completed` when upstream generation finishes.
- Preserve usage from final stream chunks when upstream provides it.

The existing exchange-log SSE parser should not remain private to `exchange_log.rs` long-term. Move or duplicate the useful pieces into the conversion crate's `sse.rs`, then optionally update exchange logging to reuse them.

## Stateful Responses Compatibility

Responses API clients may use `previous_response_id`. Chat Completions providers require the complete conversation message list.

For a practical bridge, add a bounded local state store after the stateless conversion works.

Suggested state record:

```rust
pub struct ResponseState {
    pub response_id: String,
    pub provider_name: String,
    pub model: String,
    pub messages_for_chat: Vec<ChatMessage>,
    pub output_items: Vec<ResponseOutputItem>,
    pub created_at_unix: u64,
    pub expires_at_unix: u64,
}
```

Initial state behavior:

- Generate synthetic Responses IDs.
- Store the chat transcript needed to continue a conversation.
- On `previous_response_id`, load prior transcript and append new input.
- Store assistant tool calls so later `function_call_output` items can map to Chat `tool` messages.
- Evict old state by TTL and maximum entry count.

Recommended routing semantics:

- Bind a response chain to the provider that created the previous response.
- Reject continuation if the current PID route selects a different provider, unless a config option explicitly allows cross-provider continuation.

This avoids accidental provider drift within one conversation.

## Retry Behavior

Current transparent retries re-resolve provider/default routing before each retry. Converted requests should preserve that behavior.

Recommended approach:

- Store a downstream request template for retries.
- On each retry, re-resolve the provider as today.
- Re-run conversion for the provider selected on that attempt.
- If a later provider does not support the needed conversion, stop and return a conversion error.

This keeps retry semantics consistent with the existing proxy.

## Header Behavior

### Downstream to Upstream

Continue filtering hop-by-hop headers and downstream `Authorization`.

For converted requests:

- Set upstream `content-type: application/json`.
- Recompute or omit `content-length`.
- Rewrite `Authorization` using selected provider configuration.
- Avoid forwarding downstream OpenAI-specific beta headers unless intentionally supported.

### Upstream to Downstream

For converted responses:

- Set downstream `content-type` to `application/json` for non-streaming.
- Set downstream `content-type` to `text/event-stream` for streaming.
- Drop upstream `content-length`.
- Drop upstream content encoding headers if body bytes are changed.
- Continue filtering hop-by-hop headers.

## Logging Plan

Exchange logs should make conversions visible.

Add metadata fields:

- `downstream_api`
- `upstream_api`
- `conversion`
- `downstream_uri`
- `upstream_uri`
- `converted_request_body_bytes`
- `converted_response_body_bytes`

Recommended file semantics:

- Keep existing request body logs as the downstream client request body.
- Add converted upstream request body logs only for converted requests.
- Keep existing response body logs as the downstream client response body.
- Optionally add raw upstream response body logs for debugging converted requests.

This avoids confusing users by replacing client-visible logs with provider-internal converted payloads.

## Implementation Phases

### Phase 1: Crate and Config Plumbing

- Add `crates/api-conversion`.
- Add dialect enums and conversion option structs.
- Add provider config fields with backward-compatible defaults.
- Add conversion selection in the proxy.
- Keep pass-through behavior unchanged.
- Add unit tests for config parsing defaults.

### Phase 2: Stateless Non-Streaming Text Conversion

- Convert `POST /v1/responses` non-streaming text requests to `POST /v1/chat/completions`.
- Convert Chat text responses back to Responses JSON.
- Add unit tests for request and response conversion.
- Add integration test with a mock Chat upstream.

### Phase 3: Structured Outputs and Basic Parameters

- Map `text.format` to `response_format`.
- Map `max_output_tokens` to configured Chat token field.
- Copy common sampling and metadata parameters.
- Add unsupported-feature errors for known unsupported fields.

### Phase 4: Streaming Text Conversion

- Implement incremental SSE parsing.
- Convert Chat content deltas to Responses text delta events.
- Emit `response.created` and `response.completed`.
- Add streaming integration tests.

### Phase 5: Function Tools and Tool Outputs

- Map Responses function tools to Chat function tools.
- Map Chat tool calls back to Responses function call output items.
- Map Responses `function_call_output` items to Chat `tool` messages.
- Support streamed tool-call argument deltas.
- Add tests for tool-call loops.

### Phase 6: Local Responses State

- Add bounded local `previous_response_id` state.
- Generate synthetic `resp_*` IDs.
- Store transcripts for continuation.
- Add state TTL and maximum entry count.
- Add continuation tests.

### Phase 7: Logging and Diagnostics

- Add conversion metadata to exchange logs.
- Add converted upstream body logs when enabled.
- Improve log reconstruction to use shared SSE utilities.
- Add clear trace messages for conversion selection and unsupported features.

## Test Strategy

### Conversion Crate Unit Tests

- String input maps to a user chat message.
- Instructions map to configured system/developer role.
- Message-array input preserves ordering.
- Function tool definitions map correctly.
- Function call outputs map to Chat tool messages.
- JSON schema output format maps to Chat response format.
- Chat text response maps to Responses output text.
- Chat tool-call response maps to Responses function calls.
- Usage fields are renamed correctly.
- Unsupported features return structured errors.

### Proxy Integration Tests

- Converted request reaches upstream as `/chat/completions`.
- Provider auth rewrite still works.
- PID-selected provider determines whether conversion is used.
- Providers without conversion config still pass through unchanged.
- Converted responses do not forward stale `content-length`.
- Retry path re-runs conversion for the retried provider.
- Streaming conversion returns valid Responses SSE events.

## Compatibility Risks

- OpenAI-compatible Chat providers may not support all official Chat fields.
- Some providers expect `system` rather than `developer` messages.
- Token limit field names vary between providers.
- Streaming tool-call chunks vary across OpenAI-compatible implementations.
- Responses state features cannot be perfectly represented without a local state store.
- Hosted Responses tools cannot be converted to Chat Completions without local tool implementations.

Provider capability knobs should be added incrementally rather than silently dropping unsupported fields.

## First Milestone Definition

The first useful milestone should include:

- New conversion crate.
- Backward-compatible provider config.
- Non-streaming `POST /v1/responses` text request conversion.
- Non-streaming Chat text response conversion.
- Clear unsupported-feature errors.
- Mock-upstream integration test.

If the intended downstream consumer is an agentic client such as Codex, the first production-ready milestone should also include:

- Streaming text conversion.
- Function tools.
- Function-call outputs.
- `previous_response_id` local state.

Without those, trivial prompts may work, but real agent loops are likely to fail.

## Reference Documentation

- OpenAI Responses vs Chat Completions guide: https://developers.openai.com/api/docs/guides/responses-vs-chat-completions
- OpenAI Responses API reference: https://developers.openai.com/api/reference/resources/responses/methods/create
- OpenAI Chat Completions API reference: https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create
- OpenAI Streaming Responses guide: https://developers.openai.com/api/docs/guides/streaming-responses
- OpenAI Function Calling guide: https://developers.openai.com/api/docs/guides/function-calling
