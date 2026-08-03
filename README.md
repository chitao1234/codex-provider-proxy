# codex-provider-proxy

Local HTTP reverse proxy that routes each incoming request to an upstream "provider" (base URL + API key).

The special behavior: the provider is selected based on the **PID of the client process** that opened the TCP
connection. This is implemented using:
- Linux `/proc` inspection (`/proc/net/tcp` + `/proc/<pid>/fd`)
- Windows IP Helper APIs (`GetExtendedTcpTable`)

## Quickstart

1. Create a config file:

```bash
cp config.example.toml config.toml
```

2. Run:

```bash
cargo run -p codex-provider-proxy -- --config config.toml
```

The proxy watches its config file and hot-reloads changes automatically. Updating providers, proxy listen
addresses, `rpc_listen_addr`, `rpc_token`, `upstream_connect_timeout_secs`, `upstream_idle_timeout_secs`,
`drop_responses_slow_down_errors`, `convert_429_to_503`, `transparent_retry_count`,
`transparent_retry_head_requests`, `transparent_retry_backoff_step_ms`, `rewrite.model_mappings`, and all
`[logging]` options takes effect without restarting the process.

To print an example config:

```bash
cargo run -p codex-provider-proxy -- --print-example-config
```

3. (Optional) Set a PID route via the RPC client:

```bash
pid=$$
cargo run -p codex-provider-proxyctl -- set -p "$pid" -P provider_b
```

If `rpc_token` is set in the proxy config, pass `--token` to the client.

You can also interactively set PID routes for multiple processes by matching a regex against each process
cmdline (Linux `/proc`, Windows Toolhelp + Win32 process APIs):

```bash
# Prompts you per match to enter a provider name (or press enter to skip).
cargo run -p codex-provider-proxyctl -- match 'python|node'
```

If you omit the regex, `proxyctl` uses its built-in default match regex.

You can also change the runtime default provider (used when no PID route matches):

```bash
cargo run -p codex-provider-proxyctl -- set-default -p provider_b
```

You can also run a command under a specific provider route:

```bash
cargo run -p codex-provider-proxyctl -- exec -p provider_b -- \
  curl -sS http://127.0.0.1:8080/v1/models
```

By default, `exec` removes the PID route when the command exits. Use `--keep-route` to keep it.
To avoid a startup race, `exec` pre-binds a temporary route on the controller process before spawning
the child, then transfers routing to the child PID.

4. Send a request from that same local process; it will be routed to the provider assigned to the PID.

## Notes

- The proxy can accept non-loopback connections (depending on `listen_addrs` / legacy `listen_addr`), but non-loopback clients
  always route to `default_provider` for now.
- RPC access is loopback-only when `rpc_listen_addr` is loopback (the default). If you set
  `rpc_listen_addr` to a non-loopback address, non-loopback RPC clients are allowed.
- The proxy rewrites:
  - Destination URL to `provider.base_url + (incoming_path_minus_listen_base_path) + incoming_query`
  - `Authorization` header to `Bearer <provider.api_key>` (or `provider.authorization_header` if set)
- If `[[rewrite.model_mappings]]` entries are configured, eligible JSON request bodies are rewritten before
  forwarding. With no mappings configured, request bodies use the existing streaming passthrough path.
- If `reject_messages_count_tokens = true` (the default), requests whose routed path is `/messages/count_tokens`
  or ends with that path segment suffix return a local `404` and are not forwarded upstream. Query strings such
  as `?beta=true` do not bypass this check.
- If no upstream bytes are observed for `upstream_idle_timeout_secs` (default `120`), the proxy aborts the proxied
  exchange and closes both sides. Set `upstream_idle_timeout_secs = 0` to disable this behavior.
- New upstream connection setup, including TCP connect and TLS handshake, is bounded by
  `upstream_connect_timeout_secs` (default `10`). Set `upstream_connect_timeout_secs = 0` to disable this specific
  connect timeout; `upstream_idle_timeout_secs` can still bound the broader send/header-wait phase.
- If `drop_responses_slow_down_errors = true` (the default), `*/responses` SSE streams are inspected event-by-event.
  When the proxy sees `response.failed` with `response.error.code`, or `error` with `error.code`, of `slow_down` or
  `server_is_overloaded`, it suppresses that SSE event, logs a warning, and aborts the downstream response so the
  client can reconnect and retry.
- If `convert_429_to_503 = true` (the default), final upstream HTTP `429 Too Many Requests` responses are returned
  downstream as HTTP `503 Service Unavailable`. Disable it to preserve upstream `429` responses unchanged.
- If `transparent_retry_count > 0`, non-2xx upstream responses and upstream request-send failures that occur before
  any downstream response is started are retried transparently up to that many additional attempts before returning
  the final upstream response or error.
- Connection setup timeouts, including TLS handshake timeouts, are upstream request-send failures. With transparent
  retries enabled, each timeout consumes one attempt and then the proxy continues to the next retry.
- `HEAD` requests are excluded from transparent retries by default, even when `transparent_retry_count > 0`. Set
  `transparent_retry_head_requests = true` to opt in.
- Each transparent retry re-resolves the current provider/default route before sending and reapplies request
  rewrites, so PID route changes, default-provider changes, provider config reloads, and rewrite config reloads can
  affect later attempts within the same proxied request.
- `transparent_retry_backoff_step_ms` adds linear delay between those retries. A value of `250` waits 250 ms before
  retry 2, 500 ms before retry 3, 750 ms before retry 4, and so on.
- PID routing lookup checks the client PID first; if no route exists it walks up the process tree
  (parent PID, grandparent PID, etc.) and uses the first ancestor with a defined route.
- By default, the runtime PID routing table is empty, so all requests go to `default_provider`.
- When a config reload removes a provider, any PID routes pointing at that provider are dropped automatically.

## Path Prefix Example

If the proxy config sets `listen_base_path = "/v1"` and the selected provider has `base_url = "https://example.com/v2"`:

- Incoming: `http://127.0.0.1:8080/v1/models`
- Upstream: `https://example.com/v2/models`

## Multiple Listen Addresses

You can bind the proxy to more than one address at once:

```toml
listen_addrs = ["127.0.0.1:8080", "127.0.0.1:8082"]
```

Editing that list while the proxy is running adds new listeners and gracefully shuts down listeners that were
removed from the config.

## Request Rewrites

The request rewrite layer is generic internally, but the only user-visible rewrite feature today is model mapping.
It is disabled unless `[[rewrite.model_mappings]]` contains at least one entry:

```toml
[[rewrite.model_mappings]]
provider = "provider_a"              # optional
from_model = "gpt-5.5"
from_reasoning_effort = "xhigh"      # optional
to_model = "grok-4.5"
to_reasoning_effort = "high"         # optional; omit to preserve the current effort field
```

Model matching is exact. `provider` limits a mapping to one configured provider. `from_reasoning_effort` limits a
mapping to requests whose current effort value matches. If several entries match a request, the most specific
mapping wins: provider-specific beats global, and effort-specific beats model-only. Ties keep config order.

Model mappings apply to JSON `POST` request bodies whose routed path ends with one of these API shapes:
- `responses`
- `messages`
- `chat/completions`

The mapper rewrites top-level `model`. For effort matching and rewriting, it recognizes the request shapes seen in
captured real exchanges:
- OpenAI-style responses: `reasoning.effort`
- Claude Code / Anthropic messages: `output_config.effort`; `thinking.type` is still recognized for older captured
  shapes
- Chat completions: `reasoning_effort`; if an existing `reasoning` object is present, that shape is preserved

For Claude Code 2.1.137 with `--model sonnet`, `--effort low|medium|high|max` sends
`output_config.effort = "low"|"medium"|"high"|"max"` and keeps `thinking.type = "adaptive"`. `--effort xhigh`
is accepted by the CLI but sends `output_config.effort = "high"` for `claude-sonnet-4-6`. When a Messages effort
rewrite is applied, the proxy also ensures `anthropic-beta` contains an effort beta marker, matching the captured
Claude Code request header shape without depending on a specific dated beta value.

Compressed request bodies are not rewritten unless their `Content-Encoding` is absent or `identity`. When a rewrite
changes the JSON body, the proxy removes the downstream `Content-Length` header before forwarding so the upstream
client can send the correct length for the rewritten body.

## Runtime Logging

The `[logging]` section controls both request/response/body capture and the tracing filter:

```toml
[logging]
level = "info"
rule = "codex_provider_proxy=debug,hyper=warn"
log_requests = true
log_responses = true
log_bodies = false
max_body_log_bytes = 8192
# exchange_log_dir = "./logs/exchanges"
exchange_body_max_bytes = 0
exchange_body_compression = "none"
reconstruct_responses = true
```

Changing any of those values in `config.toml` updates the running process without a restart.

If `logging.exchange_log_dir` is set, the proxy writes per-exchange files:
- `<timestamp>_req_<id>.meta.json`
- `<timestamp>_req_<id>.request_headers.txt`
- `<timestamp>_req_<id>.request_body.bin` (or `.bin.zst` when `exchange_body_compression = "zstd"`)
- `<timestamp>_req_<id>.response_headers.txt`
- `<timestamp>_req_<id>.response_body.bin` (or `.bin.zst` when `exchange_body_compression = "zstd"`)
- `<timestamp>_req_<id>.attempt_<n>.request_headers.txt`
- `<timestamp>_req_<id>.attempt_<n>.request_body.bin` (or `.bin.zst`)
- `<timestamp>_req_<id>.attempt_<n>.response_headers.txt`
- `<timestamp>_req_<id>.attempt_<n>.response_body.bin` (or `.bin.zst`)

When `transparent_retry_count > 0`, `*.meta.json` includes an `attempts` array with per-attempt provider, upstream
URL, status/latency, body-byte details, and paths for each attempt's request/response files. Non-final retry
responses are saved before being discarded for the next transparent retry; the final response is saved while it is
streamed to the downstream client.

Exchange body chunks are flushed after each write, so active body files grow during a streamed exchange. Metadata
and compressed body frames are finalized when the exchange completes.

When `logging.reconstruct_responses = true`, requests whose URL path ends in `responses` or `messages`, or is
`chat/completions`,
additionally produce:
- `<timestamp>_req_<id>.response_reconstructed.txt`

Reconstruction is best-effort for OpenAI `v1/responses` and `v1/chat/completions` SSE streams and Anthropic
`v1/messages` SSE streams,
with plain-text error fallback. Before reconstructing, it decodes `gzip`, `deflate`, `br`, and `zstd` content
codings from the upstream response. Exchange response-body files always retain the original wire bytes, so an
upstream `zstd` response with `exchange_body_compression = "zstd"` has two distinct zstd layers. Any
reconstruction failure is logged as a warning and does not affect proxy forwarding behavior.

`*.meta.json` also records machine-readable exchange status fields such as:
- `response_status_code`
- `upstream_latency_ms`
- `completed_unix_ms`
- `total_duration_ms`
- `request_body_bytes`
- `response_body_bytes`
- `upstream_error` (when upstream send fails before response headers)

## Log Analysis Utility

You can analyze captured exchange logs (token usage, token categories, cache ratio, and latency stats):

```bash
cargo run -p codex-provider-proxyctl --bin log_analyze -- -d logs/exchanges
```

The analyzer decodes the exchange body-file compression first and then any upstream response
`Content-Encoding` (`gzip`, `deflate`, `br`, or `zstd`) recorded in the response headers.

Filter examples (filters can be combined; combination is AND):

```bash
# Time range by started_unix_ms (inclusive)
cargo run -p codex-provider-proxyctl --bin log_analyze -- \
  -d logs/exchanges \
  -f 1773756500000 \
  -t 1773756800000

# Provider filter
cargo run -p codex-provider-proxyctl --bin log_analyze -- \
  -d logs/exchanges \
  -p packycode,rightcode

# Model + provider + time range together
cargo run -p codex-provider-proxyctl --bin log_analyze -- \
  -d logs/exchanges \
  -p packycode \
  -m gpt-5,gpt-5-codex \
  -f 1773756500000 \
  -t 1773756800000
```

The utility scans `*.meta.json` and corresponding response logs, extracts `response.completed` usage from SSE
payloads, and prints aggregate metrics:
- Input/output/total tokens
- Token detail categories (`input_tokens_details.*`, `output_tokens_details.*`)
- Cache ratio (`cached_tokens / input_tokens`)
- Upstream latency and total-duration statistics (`avg`, `p50`, `p95`, `min`, `max`)

## Log Pruning Utility

You can prune captured exchange log files older than a local datetime cutoff:

```bash
cargo run -p codex-provider-proxyctl -- prune-logs \
  -d logs/exchanges \
  --before-local-datetime 2026-01-01T00:00:00.000 \
  -n
```

`prune-logs` only deletes files whose exchange stem timestamp is older than `--before-local-datetime`
interpreted in the machine's current local timezone.
Use `--dry-run` (`-n`) to preview first. Without `-y`, `prune-logs` prompts for interactive confirmation before deletion:

```bash
cargo run -p codex-provider-proxyctl -- prune-logs \
  -d logs/exchanges \
  --before-local-datetime 2026-01-01T00:00:00
```

Skip the prompt by passing `-y` / `--yes`:

```bash
cargo run -p codex-provider-proxyctl -- prune-logs \
  -d logs/exchanges \
  --before-local-datetime 2026-01-01T00:00:00 \
  -y
```
