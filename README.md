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
addresses, `rpc_listen_addr`, `rpc_token`, and all `[logging]` options takes effect without restarting the
process.

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

4. Send a request from that same local process; it will be routed to the provider assigned to the PID.

## Notes

- The proxy can accept non-loopback connections (depending on `listen_addrs` / legacy `listen_addr`), but non-loopback clients
  always route to `default_provider` for now.
- RPC access is loopback-only when `rpc_listen_addr` is loopback (the default). If you set
  `rpc_listen_addr` to a non-loopback address, non-loopback RPC clients are allowed.
- The proxy rewrites:
  - Destination URL to `provider.base_url + (incoming_path_minus_listen_base_path) + incoming_query`
  - `Authorization` header to `Bearer <provider.api_key>` (or `provider.authorization_header` if set)
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
reconstruct_responses = true
```

Changing any of those values in `config.toml` updates the running process without a restart.

If `logging.exchange_log_dir` is set, the proxy writes per-exchange files:
- `<timestamp>_req_<id>.meta.json`
- `<timestamp>_req_<id>.request_headers.txt`
- `<timestamp>_req_<id>.request_body.bin`
- `<timestamp>_req_<id>.response_headers.txt`
- `<timestamp>_req_<id>.response_body.bin`

When `logging.reconstruct_responses = true`, requests whose URL path ends in `responses` or `messages`
additionally produce:
- `<timestamp>_req_<id>.response_reconstructed.txt`

Reconstruction is best-effort for OpenAI `v1/responses` SSE streams and Anthropic `v1/messages` SSE streams,
with plain-text error fallback. Any reconstruction failure is logged as a warning and does not affect proxy
forwarding behavior.

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
