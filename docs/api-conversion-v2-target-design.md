# API Conversion V2 Target Design

## Status

Design-only document. It is intentionally not committed with the implementation changes.

This document defines the target architecture for the three supported protocols:

- `OpenAiResponses`
- `OpenAiChatCompletions`
- `AnthropicMessages`

The protocols remain separate. They are not merged into one protocol state machine, and none of
them is used as the intermediate representation for the others.

## 1. Target Architecture

The final conversion pipeline is:

```text
protocol wire parser
        -> protocol-neutral semantic IR
        -> state and identity resolver
        -> provider dialect normalizer
        -> protocol wire renderer
```

Each protocol owns its own request parser, response parser, streaming parser, request renderer,
response renderer, streaming renderer, protocol lifecycle state machine, termination semantics, and
opaque field rules. The protocol-specific non-streaming and streaming boundaries must feed the same
semantic response core; they are not two unrelated implementations.

Only genuinely protocol-neutral infrastructure is shared:

- SSE framing and line-ending handling;
- UTF-8 and JSON validation;
- bounded event and body buffers;
- transport-error delivery;
- generic event queues;
- protocol-neutral IR data structures.

The proxy owns routing, retries, buffering, and state-store access. The conversion crate owns
protocol parsing, semantic conversion, rendering, and provider dialect normalization.

## 2. Protocol Boundaries

The target module layout is:

```text
api-conversion/
├── ir/
│   ├── request.rs
│   ├── response.rs
│   ├── streaming.rs
│   ├── response_core.rs
│   ├── response_accumulator.rs
│   ├── conversation.rs
│   ├── content.rs
│   ├── tools.rs
│   ├── usage.rs
│   ├── stop_reason.rs
│   ├── state.rs
│   ├── identity.rs
│   ├── cache.rs
│   └── extension.rs
├── protocols/
│   ├── openai_responses/
│   ├── openai_chat_completions/
│   └── anthropic_messages/
├── providers/
│   ├── ark/
│   ├── openai/
│   ├── anthropic/
│   └── generic/
├── transport/
│   ├── sse.rs
│   ├── bounded_buffer.rs
│   ├── stream_error.rs
│   └── passthrough.rs
└── plan.rs
```

The current implementation may be migrated into this layout incrementally, but the dependency
direction must converge to:

```text
protocols -> ir
providers  -> ir
renderers  -> ir
transport  -> no protocol semantics
proxy      -> plan + transport + state store
```

No protocol module may import another protocol module's parser or renderer.

## 3. Request Semantic IR

The request IR is semantic and protocol-neutral. It is not a Chat Completions `messages` array,
an Anthropic request, or an OpenAI Responses request.

```text
RequestIr
├── model
├── generation controls
├── ordered conversation transcript
├── instructions
├── content items
├── tool definitions
├── tool choice
├── state context
├── cache identity
├── semantic extensions
└── opaque wire extensions
```

Content items must represent at least:

- text;
- refusal;
- reasoning/thinking;
- image and other multimodal content;
- tool use;
- tool result;
- server-tool items;
- future/unknown output items without coercing them into text.

Tool calls have independent call-level and function-level metadata, identifiers, status, and
arguments. Tool definitions have independent definition-level and function-level metadata.

Structural fields are parsed into typed IR fields. Unknown fields are retained in a scoped opaque
container and are never silently folded into a generic `extra` object.

### 3.1 Request adapter contract

Protocol request implementations converge on these boundaries:

```text
RequestParser::parse(wire_request) -> RequestIr
RequestRenderer::render(request_ir, target_capabilities) -> RenderedRequest
```

`RenderedRequest` contains the target wire body plus a conversion report. The report records
dropped, mapped, normalized, and provider-only fields; a renderer must never silently discard a
field that was not classified by policy.

Request parsing is pure and does not access proxy state. Stateful continuation is applied by a
separate resolver before rendering:

```text
wire request -> parse -> resolve state/identity -> render -> normalize provider dialect
```

The resolver may inherit transcript and identities from a stored state record, but it must not
mutate the stored ancestor. Each request receives a new immutable state snapshot.

## 4. Response Semantic IR

```text
ResponseIr
├── response identity
├── ordered output items
├── usage
├── normalized stop reason
├── original protocol stop reason
├── response state
├── cache metadata
├── semantic extensions
└── protocol extensions
```

Output items carry stable identities and preserve their original order. Standard item kinds include
text, refusal, reasoning, tool call, tool result, server-tool calls, and unknown future items.

Stop reasons must preserve both normalized meaning and the original provider value:

```text
Natural
ToolUse
MaxTokens
ContentFilter
Cancelled
Error
Unknown(String)
```

An unknown provider stop reason must not be silently converted to natural completion.

## 5. Shared Response Core

Streaming and non-streaming conversion share one response semantic core. The difference is only at
the wire boundary:

```text
non-streaming wire body ──┐
                          ├── protocol response events ──> shared response core
streaming wire events ────┘
                                                   ├── ResponseIr snapshot
                                                   ├── continuation state
                                                   └── streaming output events
```

Every protocol parser should first translate its wire representation into protocol-local response
events. The shared response core then owns:

- ordered output-item accumulation;
- item identity and correlation;
- text, reasoning, refusal, and tool argument accumulation;
- metadata and opaque extension merging;
- usage accumulation;
- stop-reason normalization;
- completion/error state;
- continuation eligibility;
- response snapshots used by both streaming and non-streaming renderers.

The non-streaming path is not a second semantic implementation. It feeds the same core with a
finite event sequence and renders one final snapshot. The streaming path feeds the same core
incrementally and additionally renders intermediate events.

The shared core must expose operations equivalent to:

```text
apply(event) -> CoreUpdate
snapshot() -> ResponseIr
continuation_state() -> Option<ConversationState>
is_terminal() -> bool
```

`CoreUpdate` describes newly accumulated semantic material. A streaming renderer may emit the
update immediately; a non-streaming renderer may ignore intermediate updates and render the final
snapshot. Both paths therefore use identical accumulation and normalization logic.

Protocol-specific differences remain outside the core:

- wire event names and JSON shapes;
- protocol item lifecycle rules;
- protocol-specific event ordering validation;
- protocol-specific stream terminators;
- output framing and response envelopes;
- same-protocol passthrough decisions.

The following implementations are explicitly forbidden:

- parsing non-streaming JSON into a separate semantic model from streaming events;
- duplicating usage/stop-reason/tool accumulation in both paths;
- reconstructing a non-streaming response by scraping rendered streaming bytes;
- making a streaming renderer the source of truth for continuation state.

The response core is the source of truth. Renderers consume the core snapshot or its incremental
updates; they do not own semantic accumulation.

### 5.4 Response adapter contract

Each protocol has two wire adapters but one semantic response core:

```text
NonStreamingResponseParser::parse(body) -> Vec<CanonicalResponseEvent>
StreamingResponseParser::on_wire_event(event) -> Vec<CanonicalResponseEvent>
ResponseAccumulator::apply(event) -> CoreUpdate
ResponseRenderer::render(snapshot) -> wire response
StreamingRenderer::render_update(update) -> wire events
```

The parser may emit more than one canonical event for a single wire event, and a single canonical
event may be represented by multiple target wire events. This preserves protocol-specific framing
without duplicating semantic accumulation.

The non-streaming adapter must use the same canonical event constructors as the streaming adapter.
If a complete response contains fields that have no streaming equivalent, the parser emits an
explicit `CanonicalResponseEvent::MetadataUpdated` or `ProtocolExtension`; it must not bypass the
core by writing directly to a renderer-specific response object.

### 5.1 Core data flow

The shared core receives a canonical response event stream, not raw protocol JSON:

```text
protocol response parser
        -> CanonicalResponseEvent
        -> ResponseAccumulator
        -> CoreUpdate + ResponseSnapshot
```

`CanonicalResponseEvent` is the internal input to the shared core. It contains stable item
identity, item kind, ordered content operations, usage updates, lifecycle transitions, and scoped
extensions. A protocol parser may reject invalid protocol ordering before creating a canonical
event, but it must not accumulate semantic content itself.

The accumulator owns all mutable semantic state. It must be usable in both modes:

```text
NonStreamingAdapter:
    parse complete body -> canonical events -> apply all -> snapshot -> render response

StreamingAdapter:
    parse one wire event -> canonical events -> apply -> render CoreUpdate -> continue
```

The same canonical event sequence for equivalent protocol responses must produce the same
`ResponseSnapshot`, regardless of whether it arrived as one body or many stream events.

### 5.2 Core invariants

The accumulator enforces these invariants centrally rather than in individual renderers:

- item indexes and provider ids resolve to one canonical item identity;
- content deltas are append-only and ordered;
- final values reconcile with already emitted prefixes without duplication;
- metadata updates merge by scope and preserve unknown fields;
- usage fields merge monotonically by known semantic category;
- terminal state is idempotent;
- failed state cannot become successful terminal state;
- continuation is created only from a successful terminal snapshot;
- protocol extensions are recorded separately from replayable continuation items.

### 5.3 Renderer responsibilities

Renderers do not accumulate semantic state. They only:

- translate a `ResponseSnapshot` into a non-streaming response;
- translate `CoreUpdate` into protocol streaming events;
- maintain protocol-local wire framing state;
- enforce target protocol lifecycle and ordering rules;
- apply target-specific extension and capability policies.

For streaming output, a renderer may maintain wire bookkeeping such as content-block indexes or
sequence numbers, but it must not become the owner of text, tool arguments, usage, stop reasons,
or continuation data.

## 6. Streaming Semantic IR

Streaming IR is an ordered event stream with item identity and lifecycle boundaries:

```text
StreamEvent
├── StreamStarted
├── ItemStarted
├── ItemMetadataUpdated
├── ContentDelta
├── ReasoningDelta
├── RefusalDelta
├── ToolCallStarted
├── ToolCallMetadataUpdated
├── ToolArgumentsDelta
├── UsageUpdated
├── ItemCompleted
├── ResponseCompleted
├── StreamError
└── ProtocolExtension
```

The existing `ToolCallExtensions` behavior is a transitional implementation of
`ItemMetadataUpdated`. The target model should generalize it instead of adding another special
event for every item kind.

Streaming invariants:

1. Every delta is associated with an item identity or an explicitly selected active item.
2. Metadata can arrive after item start and must update the semantic state.
3. A done event may complete missing content but must not duplicate already emitted content.
4. EOF never invents protocol completion.
5. A stream error never emits a successful completion.
6. Converted events already produced remain deliverable before a later stream error.
7. Same-protocol passthrough remains byte-transparent.

## 7. Extensions and Opaque Data

Extensions are scoped and namespaced:

```text
Extension
├── namespace
├── key or event kind
├── value
├── scope
├── source protocol
├── provider
└── replay policy
```

Supported scopes include `root`, `request`, `response`, `message`, `content_item`, `tool_call`,
`tool_definition`, `usage`, `state`, and `provider`.

Replay policy must be explicit:

```text
NeverReplay
ReplayAsSameProtocolItem
ReplayAsTargetOpaque
ReplayIfSupported
ProviderOnly
```

Unknown stream events may be recorded for diagnostics and continuation inspection, but they must
not automatically become the next Responses request's `input`. Only explicitly replayable output
items may enter a continuation transcript.

Rendering precedence is fixed:

```text
proxy safety fields
> target structural fields
> represented semantic fields
> compatible protocol extensions
> opaque fields
```

An opaque field may never override a required target structural field or a proxy safety limit.

When multiple sources provide the same field, precedence is deterministic:

```text
proxy safety override
> explicit request field
> inherited state field
> normalized semantic field
> provider default
> opaque source field
```

Every conflict that removes or replaces a source value is recorded in the conversion report.

## 8. Stateful Conversation and Identity

Stateful behavior is an IR concern, not a renderer-side afterthought.

```text
ConversationState
├── logical conversation id
├── source response id
├── target response id
├── previous-response relationship
├── transcript
├── user identity
├── session identity
├── provider session identity
├── cache identity
└── replay policy
```

Identity values are normalized into canonical slots before target rendering:

```text
IdentitySet
├── conversation_id
├── response_id
├── previous_response_id
├── user_id
├── session_id
├── prompt_cache_key
├── safety_identifier
└── provider_opaque_identity
```

Each mapping records source protocol, source field, target field, whether it is lossy, and whether
it is replayable.

`previous_response_id` is not governed by a blanket conversion rule. It is parsed into state and
may be rendered when the target protocol and provider dialect support it. When the target protocol
does not support it, the transcript/state representation is used instead. The conversion flag alone
must never decide whether the field is retained or removed.

Cache identity is semantic, not a field alias. Examples include:

```text
Responses.prompt_cache_key -> canonical cache key -> Messages.metadata.user_id
Chat.user                 -> canonical end-user identity
Messages.metadata.user_id -> canonical cache/cache-affinity identity when configured
```

Original values and mapping provenance remain available for diagnostics and continuation.

Ark explicit cache behavior remains provider-specific and is applied only after rendering an
`OpenAiResponses` request:

```text
Request IR -> OpenAiResponses renderer -> Ark Responses normalizer
```

Ark cache behavior must not become a general IR rule.

## 9. Provider Dialects

Provider compatibility is split into capabilities, dialect normalization, and wire transport.

```text
ProviderCapabilities
├── supported protocols
├── supported content kinds
├── supported tools
├── supported stop reasons
├── supported cache identities
├── supported extensions
└── stream behavior
```

Provider normalizers may transform semantic IR or protocol-targeted IR, but may not directly
rewrite arbitrary request JSON outside the renderer/normalizer boundary.

Provider wire adapters handle URL, authentication, headers, endpoint-specific event names, and
provider error envelopes. They do not perform protocol conversion.

Provider normalizers are pure with respect to their input IR. They return a new normalized value
or a rejected conversion; they do not mutate global provider state. Runtime cache/session stores
remain owned by the proxy and are accessed through an explicit state-store interface.

## 10. Conversion Plan

Each prepared attempt gets a complete immutable plan because retries may select a different
provider:

```text
ConversionPlan
├── source protocol
├── target protocol
├── provider dialect
├── request parser
├── request renderer
├── response parser
├── response renderer
├── stream parser
├── stream renderer
├── state policy
└── extension policy
```

The proxy must not independently reconstruct conversion matrices outside this plan.

## 11. Migration Plan

### Phase 0: Freeze behavior

Create local-only golden fixtures for all three protocols covering requests, non-streaming
responses, streaming responses, tool calls, reasoning, refusals, cache identities, stateful
continuations, opaque fields, malformed SSE, incomplete streams, and provider dialects.

Phase 0 produces two related fixture families:

```text
wire fixtures
├── request bodies
├── complete response bodies
└── streaming event sequences

semantic fixtures
├── parsed request IR snapshots
├── canonical response event sequences
├── final ResponseIr snapshots
└── continuation/state snapshots
```

For every response scenario, the complete-body and streaming forms must be paired. Their final
semantic snapshots must compare equal even when their wire envelopes and event boundaries differ.

Phase 0 does not introduce IR v2 or change production behavior. It records current supported
behavior, identifies intentional lossy mappings, and marks every known gap before implementation
work begins.

Phase 0 exits only when:

- every request protocol has fixtures for native rendering and all supported cross-protocol targets;
- every response protocol has complete-body fixtures and paired streaming fixtures;
- each paired complete/streaming scenario has a documented semantic comparison;
- stateful continuation fixtures cover transcript, `previous_response_id`, user/session identity,
  and cache identity inheritance;
- opaque/extension fixtures classify replay behavior explicitly;
- malformed, incomplete, error, and oversized stream fixtures cover terminal behavior;
- each known lossy mapping is listed as intentional or filed as a blocking design gap;
- the baseline suite is deterministic and runs without network access.

### Phase 1: Introduce IR v2

Add the semantic request, response, streaming, state, identity, cache, and extension types beside
the existing implementation. No production path is switched until fixture output is comparable.

### Phase 2: Migrate request conversion

Migrate Responses, Chat, and Messages request parsing/rendering independently. Then migrate state
and cache mapping, followed by provider normalizers. Remove request pair-wise helpers only after all
routes use IR v2.

### Phase 3: Build the shared response core

Implement the response accumulator and event model before switching either response path. Port
non-streaming and streaming fixtures to the same core and assert that they produce identical
`ResponseIr` snapshots for equivalent provider responses.

### Phase 4: Migrate non-streaming responses

Use the same response semantic model for all three protocols. Preserve output ordering, refusal,
reasoning, tool metadata, usage granularity, stop reasons, and continuation items.

### Phase 5: Migrate streaming

For each protocol separately, migrate wire parser, protocol-local state machine, semantic event
emission, event recording, and renderer. Share only framing and transport infrastructure.

### Phase 6: Migrate provider dialects

Migrate Ark Responses normalization first, then cache identities, reasoning controls, server tools,
usage/error mappings, and provider extension replay policies.

### Phase 7: Delete transitional code

Delete the old pipeline, `messages_to_chat`, pair-wise converters, unscoped `extra` aggregation,
direct JSON conversion paths, and unexplained provider special cases.

## 12. Acceptance Criteria

The final architecture is accepted only when:

- all cross-protocol conversion passes through protocol-neutral IR;
- the three protocol parsers/renderers/state machines remain independent;
- the IR is not shaped as Chat Completions messages;
- unknown client/provider fields do not break proxy operation;
- unknown stream events cannot pollute continuation input;
- refusal, reasoning, tool/server-tool, future items, and unknown stop reasons are representable;
- conversation, user, session, cache, and response identities remain traceable;
- `previous_response_id` is target/provider-policy driven rather than conversion-flag driven;
- Ark normalization remains attached to `OpenAiResponses`;
- same-protocol passthrough remains byte-transparent;
- stream errors cannot fabricate successful completion;
- retries re-resolve the complete conversion plan;
- `cargo fmt --all --check` passes;
- `cargo clippy --workspace --all-targets -- -D warnings` passes;
- `cargo test --workspace` passes;
- all golden fixtures pass.

This document is design input for the refactor only. It does not itself change runtime behavior.
