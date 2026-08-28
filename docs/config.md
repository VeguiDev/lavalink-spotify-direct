# Configuration Reference

The plugin is configured under the `plugins.golibrespot` namespace in
Lavalink's `application.yml`. Binding is strict: unknown keys and unparseable
values are rejected at startup with an error naming the offending field, and
semantic problems (duplicate backend names, malformed URLs, non-absolute FIFO
paths, non-positive timeouts, empty `backends` while `enabled: true`) are
startup-fatal.

This reference mirrors the binding implemented in
`config/GoLibrespotConfig.java`, `config/BackendConfig.java`, and
`config/GoLibrespotConfigValidator.java`.

## Minimal configuration

```yaml
plugins:
  golibrespot:
    enabled: true
    backends:
      - name: gb-1
        restBaseUrl: http://golibrespot:3678
        fifoPath: /spdirect/spdirect-1.fifo
```

That is all you need for one daemon. The events WebSocket URL (`wsUrl`) is
optional: when omitted it is derived from `restBaseUrl` by swapping the
scheme (`http` -> `ws`, `https` -> `wss`) and appending `/events`.

## Backend entries

A backend is one go-librespot daemon plus one FIFO. Fields:

| Key | Required | Description |
| --- | --- | --- |
| `name` | yes | Unique backend name. Duplicates are startup-fatal. |
| `restBaseUrl` | yes | Daemon REST base URL, e.g. `http://golibrespot:3678`. Must be a valid `http(s)://` URL with a host. |
| `wsUrl` | no | Events WebSocket URL. Defaults to a derived value (see above). If set, it must be a valid ws(s) URL. |
| `fifoPath` | yes | Absolute path to the named pipe the daemon writes PCM into, e.g. `/spdirect/spdirect-1.fifo`. Absolute paths only; the pipe backend is Linux-only. |

## Global keys

All keys below are optional; they take the documented defaults when absent.

| Key | Default | Description |
| --- | --- | --- |
| `enabled` | `true` | Master switch for the plugin. When `true`, `backends` must not be empty. |
| `fifoCheck` | `warn` | FIFO existence policy. `warn` marks a backend degraded until its FIFO appears (recommended for compose, where the FIFO init may run after Lavalink boots). `fail` refuses to start the plugin if a configured FIFO is missing at startup. |
| `restTimeoutMs` | `5000` | Budget for one daemon REST request. |
| `metadataTimeoutMs` | `5000` | Budget for the load-time `GET /web-api/v1/tracks/{id}` metadata fetch. |
| `activationTimeoutMs` | `15000` | Bound on `POST /player/play` reaching the matching `playing`/`will_play` event of the current generation. |
| `seekTimeoutMs` | `10000` | Bound on an absolute seek being confirmed by the matching `seek` event or `/status` position. |
| `drainTimeoutMs` | `5000` | Time bound for draining and discarding pre-seek FIFO PCM. |
| `drainByteCap` | `4194304` | Byte bound (4 MiB) for the same pre-seek drain. |
| `wsReconnectInitialMs` | `1000` | Initial WebSocket reconnect backoff. |
| `wsReconnectMaxMs` | `30000` | Maximum WebSocket reconnect backoff (exponential with jitter between the two). |
| `wsFailuresBeforeQuarantine` | `5` | Consecutive WebSocket connect failures or read errors that quarantine a backend. |
| `poolAcquireTimeoutMs` | `30000` | Bound on waiting for an exclusive backend lease at play time. |

The pause-ack budget (5 s) and FIFO read buffer size (16 KiB) are internal
constants, not configuration keys.

## Per-backend overrides

Every timeout key above can be repeated inside a `backends[]` entry to
override that backend's value. The per-backend value wins; absent keys fall
back to the global value.

```yaml
plugins:
  golibrespot:
    enabled: true
    restTimeoutMs: 5000
    backends:
      - name: gb-1
        restBaseUrl: http://golibrespot:3678
        fifoPath: /spdirect/spdirect-1.fifo
        restTimeoutMs: 8000          # per-backend override
        activationTimeoutMs: 20000   # per-backend override
      - name: gb-2
        restBaseUrl: http://golibrespot-2:3678
        fifoPath: /spdirect/spdirect-2.fifo
```

## Startup validation summary

Failures reported at startup (the plugin does not start when they occur):

- `backends` empty while `enabled: true`
- duplicate `name` across backends
- missing or blank `name`
- missing, blank, or non-`http(s)` `restBaseUrl`
- explicit `wsUrl` that is not a valid WebSocket URL
- missing or non-absolute `fifoPath`
- any timeout key that is not a positive number

FIFO *existence* is deliberately not checked by the validator (deployment
race); the `fifoCheck` policy handles it.

## Required external daemon configuration

The daemon side must be configured so the plugin can drive it. These are the
mandatory go-librespot v0.9.0 options (pinned in
[`API_CONTRACT.md`](./API_CONTRACT.md) section 4):

| Option | Required value | Why |
| --- | --- | --- |
| `audio_backend` | `pipe` | The only backend the plugin can consume. Linux-only. |
| `audio_output_pipe` | absolute path to an existing FIFO | Must match the plugin's `fifoPath`. |
| `audio_output_pipe_format` | `s16le` | Interleaved little-endian signed 16-bit PCM. |
| `audio_output_pipe_wait_for_reader` | `true` | Blocks the daemon's write-open until the plugin's reader connects. |
| `disable_autoplay` | `true` | Prevents the daemon from loading an autoplay radio station after a context ends. |
| `external_volume` | `true` | Keeps volume in the Lavaplayer pipeline only; the daemon stops pre-multiplying samples. |
| `crossfade_duration` | `0` | Disables crossfade. |
| `server.enabled` | `true` | Turns the API server on. |
| `server.address` / `server.port` | explicit internal bind | The default port is `0`, which means an ephemeral random port the plugin cannot reach. |
| `credentials.type` | `interactive` | v0.9.0 values are `interactive`, `spotify_token`, `zeroconf`. `device_auth` is **not** a valid v0.9.0 value. |

The `deploy/` directory in this repository ships a compose layout that sets
all of these for you.
