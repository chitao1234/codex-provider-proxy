# codex-provider-proxy

Local HTTP reverse proxy that routes each incoming request to an upstream "provider" (base URL + API key).

The special behavior: the provider is selected based on the **PID of the client process** that opened the TCP
connection. For now this is implemented using Linux `/proc` inspection (`/proc/net/tcp` + `/proc/<pid>/fd`),
so only Linux is supported for PID-based routing.

## Quickstart

1. Create a config file:

```bash
cp config.example.toml config.toml
```

2. Run:

```bash
cargo run -p codex-provider-proxy -- --config config.toml
```

To print an example config:

```bash
cargo run -p codex-provider-proxy -- --print-example-config
```

3. (Optional) Set a PID route via the RPC client:

```bash
pid=$$
cargo run -p codex-provider-proxyctl -- set --pid "$pid" --provider provider_b
```

If `rpc_token` is set in the proxy config, pass `--token` to the client.

4. Send a request from that same local process; it will be routed to the provider assigned to the PID.

## Notes

- Only loopback (localhost) connections are accepted.
- The proxy rewrites:
  - Destination URL to `provider.base_url + (incoming_path_minus_listen_base_path) + incoming_query`
  - `Authorization` header to `Bearer <provider.api_key>` (or `provider.authorization_header` if set)
- By default, the runtime PID routing table is empty, so all requests go to `default_provider`.

## Path Prefix Example

If the proxy config sets `listen_base_path = "/v1"` and the selected provider has `base_url = "https://example.com/v2"`:

- Incoming: `http://127.0.0.1:8080/v1/models`
- Upstream: `https://example.com/v2/models`
