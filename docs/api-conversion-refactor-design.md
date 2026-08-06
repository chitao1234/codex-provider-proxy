# API Conversion Refactor Design

## Status

Proposed design for the next refactor of the API conversion layer. This document describes the intended architecture and migration order; it does not expand the supported protocol matrix by itself.

## Motivation

The response side now uses a semantic streaming IR, but request conversion and proxy dispatch grew incrementally across several commits. The result has three important design problems:

1. Request and response routing decisions are repeated in several proxy functions.
2. Request conversion is not actually symmetric with the response conversion matrix.
3. Responses continuation state stores the last reasoning text per provider, which can mix concurrent conversations using the same provider.

The refactor must preserve current working behavior while making unsupported combinations explicit rather than forwarding a body in the wrong protocol shape.

## Current Protocol Matrix

The configured downstream API is inferred from the request path. The upstream API is declared by the selected provider.

### Request side today

| Downstream | Upstream Chat | Upstream Messages | Upstream Responses |
| --- | --- | --- | --- |
| Messages | Supported: Messages -> Chat | Supported: same-protocol passthrough | **Not implemented** |
| Responses | Supported: Responses -> Chat | **Not implemented** | Supported: same-protocol passthrough/Ark normalization |

The two cross-protocol cells marked **Not implemented** currently fall through to converters that produce Chat Completions request bodies. Because the upstream path is nevertheless changed to `/v1/messages` or `/v1/responses`, those combinations can send a Chat-shaped body to a non-Chat endpoint.

### Response side today

The response side supports all six combinations for both streaming and non-streaming responses through protocol parsers/renderers or direct same-protocol handling.

This asymmetry is the main reason conversion selection must become an explicit plan.

## Goals

- Resolve conversion behavior once per prepared upstream attempt.
- Prevent unsupported protocol pairs from reaching an upstream endpoint.
- Make request, response, error, path, header, buffering, and recorder behavior derive from the same plan.
- Isolate Responses continuation state per response chain.
- Preserve passthrough streaming and bounded buffering invariants.
- Keep protocol conversion code independent of proxy networking and PID routing.
- Provide a migration path toward an N parsers + M renderers request architecture.

## Non-goals

- Implement every cross-protocol request converter in the first patch.
- Introduce unbounded request or response buffering.
- Change provider selection, retry semantics, auth rewriting, logging, or statistics.
- Persist Responses continuation state across proxy restarts.
- Support multiple Chat Completions choices; the proxy will explicitly require one logical output.

## Invariants

1. A request body must never be sent to an endpoint that expects a different protocol shape.
2. The conversion decision must be stable for one attempt, but retries must resolve a new provider and therefore a new plan.
3. Same-protocol passthrough remains byte-preserving unless an explicitly configured normalizer is active.
4. Streaming responses remain streaming. Only already-required rewrite/replay paths may buffer, and they remain bounded by `request_body_buffer_max_bytes`.
5. A `previous_response_id` may only resolve state produced by the same provider and model-compatible conversation chain.
6. Reasoning reattachment must never use state from another response chain.
7. Only the final retry attempt may record Responses continuation state.
8. Unsupported configurations fail during config validation when possible and fail locally before upstream I/O otherwise.

## Part 1: Unified Conversion Plan

### Plan model

The proxy crate owns plan resolution because it combines provider configuration, HTTP method, forwarded path, and runtime routing. The pure conversion crate continues to own protocol transformations.

```rust
pub struct ConversionPlan {
    pub downstream: DownstreamApi,
    pub upstream: UpstreamApi,
    pub upstream_path: UpstreamPath,
    pub request: RequestAction,
    pub response: ResponseAction,
    pub error: ErrorAction,
    pub needs_request_buffer: bool,
    pub records_response_state: bool,
}

pub enum RequestAction {
    Passthrough,
    NormalizeResponsesArk,
    MessagesToChat,
    ResponsesToChat,
    MessagesToResponses,
    ResponsesToMessages,
}

pub enum ResponseAction {
    Passthrough,
    ParseRender {
        parser: ResponseParserKind,
        renderer: ResponseRendererKind,
    },
}

pub enum ResponseParserKind {
    Chat,
    Messages,
    Responses,
}

pub enum ResponseRendererKind {
    Messages,
    Responses,
}
```

`ConversionPlan::resolve(provider, method, forwarded_path)` returns one of:

- `Ok(None)` for transparent proxying.
- `Ok(Some(plan))` for a supported conversion/normalization.
- `Err(UnsupportedProtocolPair)` for an accepted downstream API that cannot yet be rendered to the configured upstream API.

### Single source of truth

The following existing decisions should consume the plan rather than re-evaluating the protocol pair:

- request buffering eligibility;
- upstream path selection;
- request body conversion;
- request header cleanup;
- non-2xx error conversion;
- streaming parser/renderer construction;
- non-streaming response conversion;
- Responses transcript recorder creation;
- conversion logging labels.

This removes the current parallel decision trees in `request_conversion_enabled`, `response_conversion_enabled`, `conversion_upstream_path`, `convert_request_body`, `convert_non_streaming_body`, and the response-stream branch in `proxy.rs`.

### Immediate safety behavior

Before implementing new request converters, plan resolution must reject:

- downstream Messages -> upstream Responses;
- downstream Responses -> upstream Messages.

Config validation should reject providers that advertise these pairs. Runtime plan resolution remains defensive because hot reloads and future config changes must not rely on validation alone.

## Part 2: Request Semantic IR

Direct Messages -> Chat and Responses -> Chat conversion duplicates policy and makes additional upstream protocols expensive. The request side should eventually mirror the response-side parser/renderer architecture.

### Proposed IR

```rust
pub struct RequestIr {
    pub model: String,
    pub instructions: Vec<InstructionBlock>,
    pub turns: Vec<Turn>,
    pub tools: Vec<ToolDefinition>,
    pub tool_choice: ToolChoice,
    pub generation: GenerationOptions,
    pub reasoning: ReasoningOptions,
    pub output_format: OutputFormat,
    pub stream: bool,
    pub extensions: RequestExtensions,
}

pub enum Turn {
    User(Vec<InputBlock>),
    Assistant(Vec<AssistantBlock>),
    ToolResult(ToolResult),
}
```

The IR should represent semantics, not vendor JSON field names. Provider-specific knobs remain renderer inputs through `ModelCapabilities`.

### Parsers and renderers

```text
Messages JSON  -> MessagesRequestParser  -> RequestIr
Responses JSON -> ResponsesRequestParser -> RequestIr

RequestIr -> ChatRequestRenderer      -> Chat Completions JSON
RequestIr -> MessagesRequestRenderer  -> Anthropic Messages JSON
RequestIr -> ResponsesRequestRenderer -> OpenAI Responses JSON
```

Migration must be incremental:

1. Introduce IR types and golden tests without changing public behavior.
2. Move the existing Messages -> Chat logic behind parser + renderer.
3. Move the existing Responses -> Chat logic behind parser + renderer.
4. Implement Messages -> Responses and Responses -> Messages renderers.
5. Remove old direct-conversion helpers only after parity tests pass.

### Extension policy

Unknown fields cannot all be preserved across protocols. Each parser classifies fields as:

- semantic and represented in the IR;
- same-protocol extension, preserved only for same-protocol normalization;
- unsupported, producing a structured conversion error;
- intentionally dropped, recorded in `RequestConversionReport`.

Silent dropping should be limited to explicitly documented compatibility fields.

## Part 3: Responses State Isolation

### Current issue

The store keeps `last_reasoning` in a provider-keyed map. Two independent conversations using the same provider can overwrite each other's reasoning and cause the next empty reasoning echo to receive text from the wrong conversation.

### Proposed state

Reasoning belongs to the response state that produced it:

```rust
pub struct ResponseState {
    pub response_id: String,
    pub provider_name: String,
    pub model: String,
    pub chat_messages: Vec<Value>,
    pub last_reasoning: Option<String>,
    pub created_at_unix: u64,
    pub expires_at_unix: u64,
}
```

Remove the global/provider-scoped `last_reasoning` map.

When a request contains `previous_response_id`:

1. Load the referenced `ResponseState`.
2. Validate provider binding.
3. Validate model compatibility after model mapping.
4. Pass both `chat_messages` and `last_reasoning` from that exact state into request conversion.

When there is no `previous_response_id`, no remembered reasoning is attached. This is safer than guessing from the last request handled by the provider.

### Recorder behavior

`ResponseRecorder` should receive the parent state identity and write a child state only after the final downstream response is complete. The recorded `last_reasoning` is extracted from the assistant turn stored in that same child state.

A failed stream, retry attempt, or downstream disconnect before a valid response identity is available must not publish continuation state.

### State lookup result

Use a typed result instead of `Option`:

```rust
pub enum ResponseStateLookup {
    Found(ResponseState),
    Missing,
    Expired,
    ProviderMismatch,
    ModelMismatch,
}
```

This allows the proxy to return stable downstream errors and emit useful logs without exposing stored content.

## Part 4: Streaming IR Hardening

The current `StreamEvent` is a sound base, but the contract should be made explicit before adding more provider dialects.

Planned changes:

- add a stable generated response identity when the upstream omits one;
- define whether reasoning signatures are preserved, synthesized, or dropped per renderer;
- represent citation/source metadata structurally instead of converting it to text inside a parser;
- preserve structured upstream error metadata in an extension map;
- reject or explicitly select the first result when Chat `choices.len() > 1`;
- add parser conformance tests for duplicate start/end events, missing IDs, usage on finish frames, truncated SSE events, and interleaved tool calls.

Do not add a generic `serde_json::Value` bag to every event without a documented ownership policy; that would recreate protocol coupling inside the IR.

## Validation and Testing Strategy

### Plan matrix tests

Table-driven tests cover every `(method, path, upstream_api, accepted_downstream_api)` combination and assert:

- selected request action;
- selected response parser/renderer;
- upstream path;
- buffering requirement;
- recorder requirement;
- unsupported-pair error.

### Request parity tests

For existing supported conversions, run old and new implementations over the current fixtures and compare normalized JSON output before removing the old implementation.

### State isolation tests

- two response chains on the same provider retain different reasoning;
- continuation loads reasoning only from its own `previous_response_id`;
- provider mismatch is rejected;
- model mismatch behavior is explicit;
- expired state cannot provide transcript or reasoning;
- retries record only the final attempt;
- concurrent store access cannot cross-link state.

### End-to-end tests

For every supported request cell, test both streaming and non-streaming responses. Each test should assert the actual upstream request path and body shape, not only the downstream response.

## Migration Plan

### Phase 0: Safety and plan resolution

- Add `ConversionPlan` and table-driven matrix tests.
- Reject the two unsupported cross-protocol request pairs.
- Route existing supported behavior through the plan.
- Keep existing converters and response streams unchanged internally.

### Phase 1: State isolation

- Move reasoning into `ResponseState`.
- Resolve transcript and reasoning through the same `previous_response_id` lookup.
- Remove provider-global reasoning state.
- Add concurrency and cross-conversation tests.

### Phase 2: Request IR extraction

- Add request IR types.
- Adapt Messages -> Chat with parity tests.
- Adapt Responses -> Chat with parity tests.

### Phase 3: Complete request matrix

- Implement Messages -> Responses.
- Implement Responses -> Messages.
- Enable the corresponding plan cells only after end-to-end tests pass.

### Phase 4: Streaming IR hardening

- Add structured citations/errors and generated IDs.
- Define multi-choice behavior.
- Expand parser/renderer conformance tests.

### Phase 5: Documentation and cleanup

- Update `README.md` and `config.example.toml` with the real supported matrix.
- Replace outdated Ark capability comments.
- Remove obsolete direct dispatch helpers and superseded design notes.

## Recommended First Implementation Commit

The first code change after this design should be Phase 0 only: introduce `ConversionPlan`, add the complete matrix test, and reject unsupported request pairs. This is the smallest change that prevents invalid upstream requests while creating the foundation for subsequent state and IR work.
