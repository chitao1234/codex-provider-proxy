# Protocol-neutral IR cleanup

## Goals

- Every request conversion remains `protocol parser -> RequestIr -> protocol renderer`.
- Every non-streaming response conversion becomes
  `protocol parser -> ResponseIr -> protocol renderer`.
- Protocol-owned fields never survive inside an untyped catch-all map.
- Understood extensions are represented explicitly; unknown extensions remain opaque and
  are preserved only where the target boundary can carry them safely.
- Existing proxy behavior and conversion coverage remain locked by tests.
- Ark explicit-cache injection is outside this cleanup and remains unchanged.
- Cache identity headers are opt-in capabilities, not implied by a generic cache key.

## Request IR extension boundary

The former `extra` maps cloned complete source objects, including fields already represented by
the IR. That convention has now been removed: protocol-owned fields are parsed into typed IR
members, while only genuinely unknown fields remain in `opaque_extensions`.

The resulting rules are:

1. Parsers extract every protocol-owned field before constructing an IR value.
2. `cache_control` is a represented content extension, not an opaque field.
3. Remaining unknown fields are stored as `opaque_extensions`.
4. Renderers start from `opaque_extensions` and overlay the target protocol shape. They do not
   remove source-protocol structural keys because those keys never enter the extension bag.
5. Raw protocol objects may exist only for genuinely unknown/custom conversation items whose
   semantics cannot be represented without dropping information.

The migration covers message, text/thinking/tool-result content, tool calls, and tools.
Image normalization and unknown custom item normalization remain separate future work.

## Final response IR

Introduce `response_ir.rs` with one protocol-neutral final response model:

```text
ResponseIr
  id
  model
  status / stop reason
  output[]
    reasoning(text, signature?)
    text(text, annotations/citations)
    tool_call(id, call_id, name, arguments)
    opaque custom item
  usage
    input/output/reasoning/cache read/cache write/cache miss
  opaque_extensions
```

Parsers:

- `ResponseIr::parse(OpenAiChatCompletions, body)`
- `ResponseIr::parse(AnthropicMessages, body)`
- `ResponseIr::parse(OpenAiResponses, body)`

Renderers:

- `render(AnthropicMessages)`
- `render(OpenAiResponses)`

Chat is not currently a downstream response format, so no Chat renderer is added. The proxy owns
only parser/renderer selection and no longer contains a pair-wise match of converter functions.

## Behavioral compatibility

- Chat reasoning remains an Anthropic thinking block and a Responses reasoning item.
- MiMo URL citations remain visible when rendering Messages.
- Tool calls retain their upstream call identifiers and JSON arguments.
- Usage retains cached, cache-write, cache-miss, and reasoning token detail.
- Responses -> Responses normalization preserves unknown top-level and usage fields currently
  passed through by the compatibility renderer.
- Existing continuation transcript recording remains separate from response rendering.

## Migration sequence

1. Add opaque-extension extraction helpers and migrate request IR fields.
2. Add `ResponseIr` parsers/renderers with focused cross-protocol tests.
3. Route the proxy non-streaming pipeline through `ResponseIr`.
4. Delete pair-wise non-streaming response converters and the misleading
   `chat_to_messages.rs` module.
5. Run formatting, Clippy, and all workspace tests after each structural commit.
