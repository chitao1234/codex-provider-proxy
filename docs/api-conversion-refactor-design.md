# Protocol-Neutral API Conversion Design

## Status

Implemented for request routing and streaming response conversion. The supported protocols are:

- Anthropic Messages
- OpenAI Chat Completions
- OpenAI Responses

No protocol pair uses Chat Completions JSON as the shared cross-protocol request representation.

## Core Rule

Conversion is split into four independent stages:

```text
protocol parser -> protocol-neutral IR -> optional provider normalizers -> protocol renderer
```

- Parsers understand client protocol syntax.
- The IR stores protocol-independent semantics and lossless extension data.
- Normalizers apply provider/model-specific compatibility rules.
- Renderers produce the selected upstream protocol.

The proxy owns routing and buffering. The conversion crate remains independent of networking,
provider selection, retries, and PID routing.

## Request IR

`request_ir::RequestIr` is the canonical request boundary. It stores:

- source protocol;
- original top-level request object;
- model and streaming mode;
- instructions;
- ordered semantic messages and reasoning items;
- tool calls and tool results;
- tool definitions and tool choice;
- continuation data;
- stateful request context (conversation, end-user identity, safety identity, metadata, storage,
  truncation, include selectors, prompt-cache identity and retention);
- arbitrary top-level parameters.

Messages, tool calls, and tool definitions retain their original object data in addition to the
typed semantic fields. Renderers clone the retained extension data, remove only source structural
fields, and overlay the target protocol representation.

This is intentionally different from treating a Chat Completions `messages` array as an IR.
Chat-specific fields are produced only by the Chat renderer.

## Extension Preservation

The converter must tolerate clients and providers adding fields that the proxy does not know yet.

The preservation policy is:

1. Same-protocol requests without a normalizer remain byte-preserving passthrough.
2. Unknown top-level parameters survive cross-protocol conversion unchanged.
3. Unknown message/tool fields are retained when the target object has a corresponding semantic
   object.
4. Known source-protocol transport fields are consumed rather than copied blindly.
5. Known incompatible fields may be removed only by an explicit parser, capability rule, or
   normalizer.
6. Provider-generated extension parameters override neither required target structural fields nor
   proxy safety limits.

Examples of extension parameters that must survive include custom sampling controls, vendor token
budgets, experimental decoding modes, and provider-specific request switches.

## Request Protocol Flow

```text
Anthropic Messages parser -----+
OpenAI Chat parser ------------+--> RequestIr --> target renderer
OpenAI Responses parser -------+
```

The target renderer is selected independently:

```text
RequestIr --> Anthropic Messages renderer
RequestIr --> OpenAI Chat Completions renderer
RequestIr --> OpenAI Responses renderer
```

The mature Messages/Responses-to-Chat capability policies are exposed as the Chat renderer's
compatibility implementation during migration. They consume the IR's lossless source object, so
existing server-tool, reasoning, image, response-format, and provider-native behavior remains
available while the semantic renderer is expanded.

## Response IR

Streaming responses use `stream::StreamEvent` as the protocol-neutral semantic IR:

```text
ChatParser --------+
MessagesParser ----+--> StreamEvent --> MessagesRenderer / ResponsesRenderer
ResponsesParser ---+
```

The response parser and renderer pair is resolved once in `ConversionPlan.response`. The proxy no
longer rebuilds the matrix separately from the plan.

Non-streaming conversion consumes the same response pipeline identity. Same-protocol Messages
responses remain passthrough; Responses responses may be normalized through the Responses parser
and renderer when conversion is enabled.

## Conversion Plan

`ConversionPlan` is resolved for each prepared attempt because retries may select a different
provider. It contains:

- downstream protocol;
- upstream protocol;
- request parser and renderer;
- optional request normalizer;
- response parser and renderer.

The plan controls upstream path selection, request conversion, response parser/renderer selection,
non-streaming response conversion, and continuation recording.

## Ark Normalization

Ark normalization is optional and target-specific:

```text
RequestIr -> OpenAI Responses renderer -> Ark Responses normalizer
```

It applies to both native Responses requests and requests converted from Messages. It is enabled by
`capabilities.ark_style = true`, with model capabilities overriding provider defaults.

The normalizer currently maps reasoning effort to Ark thinking controls and normalizes supported
web-search parameters. It does not change the source parser or make Ark a separate protocol.

## Continuation State

Responses continuation state is scoped to a response chain. `previous_response_id` restores both
the previous transcript and the reasoning associated with that exact response state. It never uses
provider-global reasoning state.

The stored transcript is `RequestIr`, not Chat Completions messages. A continued request inherits
stateful context from that IR when the new request does not override it. This includes:

- OpenAI `conversation` and `previous_response_id` chain semantics;
- OpenAI `user` / `safety_identifier` and Anthropic `metadata.user_id` identity mapping;
- `metadata` and recognized provider session identifiers such as `session_id`;
- `prompt_cache_key`, `prompt_cache_retention`, and Anthropic `cache_control` directives;
- Responses `store`, `include`, and `truncation` state-affecting options.

Nested cache directives remain attached to their semantic instruction, content, or tool objects.
Unknown provider-specific identity/cache fields remain in the lossless extension data. Provider
normalizers may add explicit spellings later, but a missing mapping must not make parsing fail or
erase the field from continuation state.

Where a target protocol has no separate prompt-cache key, the renderer applies a deliberate
identity mapping instead of leaking an unsupported source field: a Responses `prompt_cache_key`
becomes Anthropic `metadata.user_id` (or the Chat `user` field) when no explicit end-user identity
is present. The source-only cache-key fields are then omitted from that target request.

`previous_response_id` itself is not blindly forwarded during cross-protocol rendering because a
proxy-synthesized response id is meaningful only to the proxy's local state store. The restored
transcript and context are rendered instead.

## Validation Requirements

Changes to the conversion layer must keep these checks passing:

- all configured protocol pairs resolve to one request and response plan;
- unknown top-level parameters survive each cross-protocol route;
- tool-call identifiers remain correlated with tool results;
- conversation, user, session, and prompt-cache identifiers survive conversion and continuation;
- exact-limit request buffering succeeds and over-limit buffering returns 413;
- retries re-resolve the conversion plan with the selected provider;
- Ark normalization works for native and converted Responses requests;
- streaming and non-streaming responses use the same parser/renderer identity;
- `cargo clippy --workspace --all-targets -- -D warnings` remains clean.
