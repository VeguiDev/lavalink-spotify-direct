# go-librespot v0.9.0 API Contract (pinned)

> **Source of truth:** the go-librespot repository at tag **`v0.9.0`**
> (commit `cddcada819ba01966330ce5f0b10535494443cf3`). The REST API server is
> **codegen'd from the OpenAPI spec [`api-spec.yml`](https://github.com/devgianlu/go-librespot/blob/cddcada819ba01966330ce5f0b10535494443cf3/api-spec.yml)**
> into `daemon/api_gen.go`; the spec comment states: *"This spec is the source
> of truth for the API: the request/response models and the HTTP routing in
> `daemon/api_gen.go` are generated from it by oapi-codegen."*
>
> Every claim below traces to one of: `api-spec.yml`, `daemon/api_gen.go`,
> `daemon/api_server.go`, `daemon/controls.go`, `daemon/player.go`,
> `daemon/player_state.go`, `daemon/app.go`, `output/driver-pipe.go`,
> `output/driver-pipe-unix.go`, `output/driver-pipe-stub.go`,
> `player/player.go`, `player/events.go`, `spclient/spclient.go`,
> `config_schema.json`, `API.md`. Do **not** cross-reference go-librespot
> `master` — `device_auth` and other master-only changes are NOT part of this
> contract.
>
> **Compatibility rule for the plugin:** every consumer of this contract
> (REST client, WS client, metadata resolver) MUST tolerate unknown JSON
> fields and unknown WS event `type` values in every response/event. This
> contract pins the shapes v0.9.0 emits today; the daemon may add fields in
> patch releases without bumping the API version.

---

## 1. Transport & base

| Item | Value |
| --- | --- |
| Base URL | `http://<host>:<port>` (TLS only if `server.cert_file` + `server.key_file` configured; plain HTTP otherwise) |
| Content type | `application/json` for all JSON request/response bodies |
| Protocol | HTTP/1.1, WebSocket upgrade for `/events` |
| Auth | **None.** The REST/WS API has no authentication. The only bearer token in play is attached **inside** the daemon to outbound `/web-api/` proxied requests (see §3.6). Never expose the port to anything but the plugin's internal network. |

### 1.1 How requests are processed (important for timeouts)

Every REST request is funneled through a single **serialized request lane**
per active session:

1. `handleRequest` (HTTP handler goroutine) pushes `ApiRequest` into
   `s.requests` and blocks on the reply.
2. The daemon's `Run` loop (a single goroutine that also drains player
   events and dealer messages) pops the request, calls
   `handleApiRequest`, and replies.
3. `handleApiRequest` itself is bounded to **30 s** (`context.WithTimeout`).

Consequences the plugin must design around:

- The request lane is the **same goroutine that emits WS events**. A slow
  WS client blocks event emission (`Emit` is synchronous per client,
  bounded by a **10 s** write timeout, see §3.5).
- Errors raised by the player internals are **swallowed** in most handlers
  (e.g. `_ = p.play(ctx)`). **HTTP 200 therefore does NOT prove the
  command succeeded** — the caller must reconcile with `/status` and WS
  events.
- When the session is gone, requests receive 204 (see per-endpoint).

---

## 2. REST endpoints

### 2.1 `GET /` — reachability & playback readiness

| | |
| --- | --- |
| Response 200 | `application/json` body: |

```json
{
  "playback_ready": true
}
```

- `playback_ready` (`bool`, required): the daemon is fully bootstrapped —
  it has a Spotify connection id, has PUT its initial connect state, and
  knows its country code — and is ready to accept `/player/play`
  (`daemon/player.go` `playbackReady()`). Until those three conditions are
  met the daemon returns `false`.
- Always **200**. With **no active session** the daemon replies 200 with
  `{"playback_ready": false}` (`daemon/app.go` — the no-session branch
  answers `&ApiRoot{}` for `ApiRequestTypeRoot`).
- Used by the plugin as the liveness + readiness probe.

### 2.2 `GET /status` — full player status

| | |
| --- | --- |
| Response 200 | `application/json` — full `ApiStatus` (schema `status`); `track` is nullable |
| Response 204 | **No active session** (no content, empty body). |

```json
{
  "username": "string",
  "device_id": "string",
  "device_type": "string",
  "device_name": "string",
  "play_origin": "string",
  "stopped": true,
  "paused": false,
  "buffering": false,
  "volume": 100,
  "volume_steps": 100,
  "repeat_context": false,
  "repeat_track": false,
  "shuffle_context": false,
  "track": {
    "uri": "spotify:track:...",
    "name": "Track name",
    "artist_names": ["Artist A", "Artist B"],
    "album_name": "Album",
    "album_cover_url": "https://i.scdn.co/image/...",
    "position": 12345,
    "duration": 240000,
    "release_date": "2020-01-01",
    "track_number": 3,
    "disc_number": 1,
    "format": "OGG_VORBIS_160",
    "codec": "vorbis",
    "bitrate": 160,
    "sample_rate": 44100,
    "bit_depth": null
  }
}
```

Field-by-field (all top-level fields required per spec):

| Field | Type | Meaning |
| --- | --- | --- |
| `username` | string | Active account username |
| `device_id` | string | Player device id (hex) |
| `device_type` | string | e.g. `COMPUTER`, `SPEAKER` |
| `device_name` | string | Configured device name |
| `play_origin` | string | Who started playback; **`go-librespot` when started via the REST API**, anything else is Spotify-originated |
| `stopped` | bool | **`!IsPlaying`** — true when idle (incl. right after a logical stop) |
| `paused` | bool | Paused state |
| `buffering` | bool | Buffering state |
| `volume` | uint32 | Current volume, 0..`volume_steps` |
| `volume_steps` | uint32 | Max volume value (default 100) |
| `repeat_context` / `repeat_track` / `shuffle_context` | bool | Current options |
| `track` | object \| **null** | Track + stream info; **`null` when no stream is loaded** (e.g. idle after stop), see below |

`track` object (schema `track`; **all fields required** when present,
several nullable):

| Field | Type | Meaning |
| --- | --- | --- |
| `uri` | string | Track/episode URI (`spotify:track:...` / `spotify:episode:...`) |
| `name` | string | Track name |
| `artist_names` | string[] | Artist names (for episodes: the show name) |
| `album_name` | string | Album name (for episodes: the show name) |
| `album_cover_url` | string \| **null** | Album cover URL |
| `position` | int64 | Playback position in ms |
| `duration` | integer | Duration in ms |
| `release_date` | string | Album release date; **empty for episodes** |
| `track_number` | integer | Track number within disc; **zero for episodes** |
| `disc_number` | integer | Disc number; **zero for episodes** |
| `format` | string | Spotify name of the audio file decoded, e.g. `OGG_VORBIS_160` |
| `codec` | string enum | `vorbis` \| `flac` \| `mp3` \| `aac` \| `unknown` \| `""` |
| `bitrate` | integer \| **null** | Nominal bitrate kbps; null for e.g. FLAC |
| `sample_rate` | integer \| **null** | **Actual decoder sample rate in Hz** (always 44100 in v0.9.0; reported from the decoder, not the format name) |
| `bit_depth` | integer \| **null** | Source bits-per-sample; null for lossy formats |

Notes:

- `track` is present only when `primaryStream != nil && prodInfo != nil`
  (`daemon/player.go` `ApiRequestTypeStatus`). A session that is alive but
  idle reports `track: null` with `stopped: true`.
- `position` is the daemon-authoritative position (dynamic when playing,
  frozen when paused/stopped). **The plugin treats position as
  daemon-authoritative and never derives it from PCM.**
- `stopped` is derived (`!IsPlaying`), so it is true both after a natural
  context end and after an explicit stop — distinguish by events + track
  presence, not by this flag alone.

### 2.3 `POST /player/play` — start playback

Request body (`play` schema):

```json
{
  "uri": "spotify:track:4uLU6hMCjMI75M1A2tKUQC",
  "skip_to_uri": "spotify:track:...",
  "paused": false,
  "position": 0
}
```

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `uri` | string | **yes** | Spotify URI to start playing (track or context) |
| `skip_to_uri` | string | no (default `""`) | URI to skip to when playing a playlist/context |
| `paused` | bool | no (default `false`) | Start playback as paused |
| `position` | int64 | no (default `0`) | Start position in ms within the selected track (0 = beginning) |

Responses:

| Status | Meaning |
| --- | --- |
| **200** | Accepted for processing — **NOT proof of success** (internal errors are swallowed; `_ = p.loadContext(...)`). Reconcile with `/status` and the `playing`/`will_play` events. |
| **400** | `uri` missing or empty (`len(data.Uri) == 0`), or body is not valid JSON. |

Behavior details (from `handleApiRequest` → `ApiRequestTypePlay`):

- Sets `play_origin` to `"go-librespot"` (so `playing`/`paused`/etc. events
  and `/status.play_origin` identify API-initiated playback).
- `paused: true` → context loads paused; **no `playing` event is emitted
  until resumed** (you get `will_play` + `metadata`).
- `position > 0` → loads the context **paused**, issues an internal seek to
  `position`, then resumes unless `paused: true`. So `play {position}` and
  `play {paused:true, position}` both start the track at `position`
  (paused in the latter case).
- The `uri` may be a **context** (playlist/album/artist). With
  `disable_autoplay: true` and a single-track context this is safe to use
  for one-track playback; prefer passing the bare track URI for
  `spdirect:` playback.

### 2.4 `POST /player/pause` — pause (NO body)

| Status | Meaning |
| --- | --- |
| **200** | Accepted. Errors swallowed (`_ = p.pause(ctx)`); if no primary stream is loaded the handler still returns 200. Confirm with the `paused` event and/or `/status`. |
| — | No 400 (no body to validate). |

### 2.5 `POST /player/resume` — resume (NO body)

| Status | Meaning |
| --- | --- |
| **200** | Accepted. Errors swallowed; confirm with the `playing` event (`resume: true`) and/or `/status`. |
| — | No 400. |

### 2.6 `POST /player/stop` — stop & disconnect session (NO body)

> **⚠️ FORBIDDEN FOR THIS PLUGIN — never called from `src/main`.**
> See §5 for the v0.9.0 stop-race that makes `/player/stop` unsafe.

| Status | Meaning |
| --- | --- |
| **200** | Stop processed. Errors swallowed. |

What it does (`stopPlayback`): stops the player, clears streams, **resets
`state.player`** (`state.reset()`), PUTs inactive connect state, and — in
**zeroconf mode only** — pushes a logout that tears the session down and
restores a fresh one. In **non-zeroconf mode** (e.g. `credentials.type:
interactive`/`spotify_token`) the logout is skipped: the old player object
stays alive with the live buffered-event race (§5).

### 2.7 `POST /player/seek` — seek

Request body (`seek` schema):

```json
{ "position": 120000, "relative": false }
```

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `position` | int64 | **yes** | Absolute ms position; **must not be negative unless `relative`** |
| `relative` | bool | no (default `false`) | Interpret `position` relative to the current position |

Responses:

| Status | Meaning |
| --- | --- |
| **200** | Accepted. Errors swallowed; confirm with the `seek` event (position) and/or `/status`. |
| **400** | `position < 0` and `relative == false`, or body not valid JSON. |

Behavior: absolute seek is clamped to `[0, duration]`. A `seek` event is
emitted with the resulting absolute `position`.

### 2.8 `GET /events` — WebSocket event stream

| | |
| --- | --- |
| Response **101** | `Upgrade: websocket` (protocol upgrade, not a plain HTTP response) |
| Frame format | JSON text frames of the form `{"type": "<event_type>", "data": {...}}` |

- Implemented with `github.com/coder/websocket` (`daemon/api_server.go`
  `GetEvents`).
- The server keeps the connection open and **only reads**; it pushes events
  to every connected client.
- The plugin MUST treat the connection as authoritative state delivery and
  must never block on it (see §3.5 — daemon-side write timeout).
- Full event catalog and field shapes: **§3**.

### 2.9 `GET/POST/PUT/DELETE /web-api/{path}` — Spotify Web API passthrough

Catch-all proxy to `https://api.spotify.com/` for **any** number of path
segments (e.g. `/web-api/v1/tracks/{id}`, `/web-api/v1/me/player`). All four
methods forward. **The plugin uses it for load-time metadata**
(`GET /web-api/v1/tracks/{id}`).

**⚠️ NOT byte-for-byte transparent.** The passthrough re-shapes the request
and response:

1. **Path**: everything after `/web-api/` is joined onto
   `https://api.spotify.com/`.
2. **Query string is parsed and re-encoded.** The daemon takes
   `r.URL.Query()` (a `url.Values`) and rebuilds with `query.Encode()`
   (`spclient.go` `innerRequestWith`): keys sorted, ` ` encoded as `+`.
   Original query byte order/encoding is not preserved.
3. **Request body is NOT forwarded.** The handler extracts only
   method + path + query (`ApiRequestDataWebApi`); the body argument to
   `sess.WebApi` is always `nil`. A POST/PUT request body from the caller is
   **dropped**.
4. **Session bearer token is attached by the daemon**: `Authorization:
   Bearer <access_token>` for the active session (plus `Client-Token` when
   configured). Callers never send credentials.
5. **Only a fixed set of upstream statuses are propagated** as the daemon's
   own HTTP status:
   | Upstream status | Daemon response |
   | --- | --- |
   | 400 | 400 |
   | 403 | 403 |
   | 404 | 404 |
   | 405 | 405 |
   | 429 | 429 |
   | **any other status** (incl. 200, 201, 204, 500, 502…) | **HTTP 200** with the body, or an **empty 200** for no-content responses |
   Because of this, an upstream `204`/`500` arrives at the client as `200`
   with empty body — **the plugin's REST client must not infer success from
   status alone** and must treat the passthrough result as "raw Spotify
   payload or empty", validated by content.
6. **Response body re-encoding**:
   - If upstream `Content-Type` does **not** contain `application/json`,
     the body bytes are returned verbatim under
     `Content-Type: application/octet-stream`.
   - If it **is** JSON, the body is decoded to a generic `any` and
     **re-encoded** by the daemon (`json.NewEncoder`) — semantically equal
     but not byte-for-byte (number representation / key order may change).
7. **No active session** → `GET /web-api/...` replies **204** (no content,
   `ErrNoSession`).

> Practical consequence for `GET /web-api/v1/tracks/{id}`: a healthy track
> returns HTTP 200 + JSON (`application/json`), missing/invalid id → 404,
> unauthenticated/stale session → the daemon retries token acquisition and
> may surface 401 → 403, and "no session at all" → 204. Any of
> 404/403/429/204/empty-200 must be treated as a metadata failure by the
> resolver (see DECISIONS.md — metadata fetch never fabricates data).

### 2.10 Other endpoints (documented for completeness; NOT used by the plugin)

| Endpoint | Method | Request body | Responses |
| --- | --- | --- | --- |
| `/token` | POST | — | 200 `{"token": "<access_token>"}`; 204 no session |
| `/player/playpause` | POST | — | 200 (resume when paused, pause when playing) |
| `/player/next` | POST | `{"uri": "spotify:track:..."}` (optional; omit → next in context) | 200 |
| `/player/prev` | POST | — | 200 (rewinds current track when >3s in, else previous) |
| `/player/volume` | GET | — | 200 `{"value": int, "max": int}` |
| `/player/volume` | POST | `{"volume": int32, "relative": false}` | 200; 400 when negative & not relative |
| `/player/repeat_context` | POST | `{"repeat_context": bool}` | 200 |
| `/player/repeat_track` | POST | `{"repeat_track": bool}` | 200 |
| `/player/shuffle_context` | POST | `{"shuffle_context": bool}` | 200 |
| `/player/add_to_queue` | POST | `{"uri": "..."}` | 200; 400 missing/empty uri |
| `/set_device_name` | POST | `{"name": "..."}` | 200; 400 missing/empty name |
| `/player/output` | POST | `{"device": "..."}` (empty = default) | 200 |

---

## 3. WebSocket events (`GET /events`)

Frame envelope (fixed, `daemon/api_server.go`):

```json
{ "type": "<event_type>", "data": { ... } }
```

- `type` (`string`) — one of the values below.
- `data` — an object whose fields depend on `type`; for `playback_ready`,
  `active`, `inactive` the `data` member is **`null`**.

**Events carry NO id, NO sequence number, NO timestamp, NO session id.** The
plugin correlates events by `uri` + local lease generation + expected phase
(never by a daemon-provided sequence). Unknown `type` values and unknown
fields inside `data` **MUST be ignored** (forward compatibility).

### 3.1 Event catalog

| `type` | Emitted when | `data` shape |
| --- | --- | --- |
| `playback_ready` | Daemon fully bootstrapped (has spot conn id + initial connect state + country code); emitted **once** per session | `null` |
| `active` | Device became active — Spotify transfer command (`transfer`) or REST `/player/play` (`setActive(true)`) | `null` |
| `inactive` | Device became inactive (`stopPlayback`) | `null` |
| `metadata` | A new track was loaded into the stream | full track object (§3.2) |
| `will_play` | Player is about to play the specified track (emitted at track load, before stream is set) | `{context_uri, uri, play_origin}` |
| `playing` | Current track is playing. `resume:false` = fresh play, `resume:true` = resumed from pause | `{context_uri, uri, resume, play_origin}` |
| `not_playing` | Current track has finished playing (natural end) | `{context_uri, uri, play_origin}` |
| `paused` | Current track is paused | `{context_uri, uri, play_origin}` |
| `stopped` | Context is empty / nothing more to play (or player stop) — **NO `uri`** | `{play_origin}` (may be `""`) |
| `seek` | Current track seeked | `{context_uri, uri, position, duration, play_origin}` |
| `volume` | Player volume changed (from anywhere: REST, Connect, mixer) | `{value, max}` |
| `repeat_track` | Repeating-track option toggled | `{value: bool}` |
| `repeat_context` | Repeating-context option toggled | `{value: bool}` |
| `shuffle_context` | Shuffling-context option toggled | `{value: bool}` |

### 3.2 `metadata` data — the only event carrying full track info

`metadata` is emitted by `ApiEventDataMetadata`, which is **exactly the
`track` schema** (§2.2) — JSON field names:

```json
{
  "uri": "spotify:track:4uLU6hMCjMI75M1A2tKUQC",
  "name": "Track name",
  "artist_names": ["Artist A", "Artist B"],
  "album_name": "Album name",
  "album_cover_url": "https://i.scdn.co/image/...",
  "position": 0,
  "duration": 240000,
  "release_date": "2020-01-01",
  "track_number": 1,
  "disc_number": 1,
  "format": "OGG_VORBIS_160",
  "codec": "vorbis",
  "bitrate": 160,
  "sample_rate": 44100,
  "bit_depth": null
}
```

> Note: the repo's `API.md` documents a `context_uri` field for `metadata`,
> but the **emitted struct has no `context_uri`** — `ApiEventDataMetadata`
> is a plain `ApiTrack`. The code (`controls.go` `loadCurrentTrack`) is
> authoritative; consumers must not require `context_uri` on `metadata`.

### 3.3 `playing` / `paused` / `not_playing` / `will_play` / `seek` data

```json
{ "context_uri": "spotify:playlist:...", "uri": "spotify:track:...", "play_origin": "go-librespot" }
```

- `playing` adds `"resume": true|false`.
- `seek` adds `"position": <ms int>` and `"duration": <ms int>` (plus the
  three shared fields).
- `play_origin` = `"go-librespot"` for API-initiated playback; anything else
  means Spotify-side origin (transfer/cast).

### 3.4 Events WITHOUT track data (summary)

| Event | Has `uri`? | Has other data? |
| --- | --- | --- |
| `playback_ready` | no | none (`data: null`) |
| `active` | no | none |
| `inactive` | no | none |
| `stopped` | **no** | `play_origin` only (may be `""`) |
| `volume` | no | `value`, `max` |
| `repeat_track` / `repeat_context` / `shuffle_context` | no | `value` only |

### 3.5 Daemon-side emission behavior (WS client design input)

- `Emit` iterates all connected clients **synchronously** on the caller's
  goroutine and writes with a **10 s per-client timeout**
  (`daemon/api_server.go` `const timeout = 10 * time.Second`). A client that
  stops reading will, after ~10 s, be skipped (write error logged, client
  dropped only when the connection errors on the server read loop).
- The daemon **drops the client from its list when the connection errors**
  (its read loop gets an error). Clean client-side close with a close frame
  is the reliable path.
- The plugin's WS client must **drain promptly** (never let the socket
  buffer grow unbounded and never block event processing) so it never causes
  the daemon's synchronous emit to stall; reconnect with bounded exponential
  backoff (§ DECISIONS.md constants).

---

## 4. Mandatory daemon configuration (v0.9.0)

From `config_schema.json` + `output/driver-pipe*.go`. These are the options
the plugin's deployment **requires** (Compose/README/runbook must set them):

| Option | Value | Why (source) |
| --- | --- | --- |
| `audio_backend` | `pipe` | Pipe backend. Enum: `alsa`, `pulseaudio`, `pipe`, `audio-toolbox`, `wasapi` (default `alsa`). **Linux-only:** `output/driver-pipe-stub.go` (`//go:build windows`) returns `"pipe output is not supported on Windows"`. |
| `audio_output_pipe` | **absolute** path to an **existing** FIFO | `config_schema.json`: "The path to an existing FIFO for the pipe audio backend". The daemon opens it with `os.OpenFile(path, O_WRONLY[, O_NONBLOCK])`. |
| `audio_output_pipe_format` | `s16le` | Interleaved little-endian signed 16-bit PCM (enum `s16le`, `s32le`, `f32le`; default `s16le`). |
| `audio_output_pipe_wait_for_reader` | `true` | Block the FIFO open until a reader connects (`driver-pipe-unix.go`). With `false` the daemon opens `O_WRONLY\|O_NONBLOCK`, fails with `ENXIO` when no reader exists, and the player errors at first play. (Option added in v0.9.0, commit `ac0c881`.) |
| `disable_autoplay` | `true` | Else, when a context ends with no next track, the daemon resolves and loads an **autoplay radio station** (`controls.go` `advanceNext` → `ContextResolveAutoplay`). The plugin wants deterministic track end. |
| `external_volume` | `true` | Else the pipe output loop **pre-multiplies samples by `volume²`** (squared curve, `driver-pipe.go`). Plugin applies volume in the Lavaplayer pipeline only. |
| `crossfade_duration` | `0` | 0 disables crossfade (default 0). |
| `server.enabled` | `true` | Turns the API server on (default `false`). |
| `server.address` | explicit internal bind address | Default `localhost`. |
| `server.port` | explicit port ≠ 0 | **Default `0` = EPHEMERAL** — the daemon binds a random port (`net.Listen("tcp", addr:0)`). The plugin's `restBaseUrl`/`wsUrl` must point at the real configured port. |
| `credentials.type` | `interactive` (recommended for compose) | v0.9.0 enum: `interactive`, `spotify_token`, `zeroconf` (default `zeroconf`). **`device_auth` is MASTER-ONLY — it is NOT a valid v0.9.0 value** and must not be configured. |

Related options worth pinning in the runbook (not strictly required):

- `server.allow_origin` (default `""`; used for WS origin matching and CORS).
- `server.image_size` (`default`|`small`|`large`|`xlarge`) — album art served in `album_cover_url`.
- `server.cert_file` / `server.key_file` — empty = plain HTTP.
- `bitrate` (96|160|320, default 160), `volume_steps` (default 100; the `max` reported by `volume` events and `/status`).
- `normalisation_disabled` (default `false`), `normalisation_pregain`.
- `zeroconf_enabled` (default `false`), `zeroconf_backend` (`builtin`|`avahi`), `zeroconf_port` (default 0 = random).
- `log_level`, `device_id`, `device_name`, `device_type`.

---

## 5. The v0.9.0 stop-race (why `/player/stop` is forbidden)

### 5.1 Mechanism

1. Player events are delivered to the daemon over a **buffered channel with
   capacity 128** (`player/player.go` `ev: make(chan Event, 128)`) and the
   daemon consumes them **on the same goroutine** as API requests
   (`daemon/player.go` `Run` select loop).
2. Player-level events (`player/events.go`) carry **only a type**
   (`Event{Type EventType}` — no data, no sequence). The daemon's
   `handlePlayerEvent` (`controls.go`) re-reads the **current shared state**
   to build WS events, e.g. `Uri: p.state.player.Track.Uri` for
   `playing`/`paused`/`not_playing`.
3. `/player/stop` → `stopPlayback` (`controls.go`) → `state.reset()`
   (`player_state.go`) which **replaces `state.player` with a fresh
   `connectpb.PlayerState` whose `Track` is `nil`**.
4. If any `playing`/`paused`/`not_playing` event was still buffered (cap
   128) when the stop was processed, draining it **nil-dereferences
   `p.state.player.Track.Uri`** → Go panic → the `Run` loop dies.
5. The API listener goroutine (`serve()`) survives (the port stays open),
   but `handleRequest` blocks forever on a request channel with **no
   consumer** → **every subsequent API call hangs indefinitely** (no
   timeout, no error response).

### 5.2 Zeroconf vs non-zeroconf

- **zeroconf mode**: `stopPlayback` also pushes the player onto the logout
  channel, which tears the session down and restores a fresh one — the stale
  events may never be drained against the reset state, but the race is
  still live if events are in flight before teardown.
- **non-zeroconf mode** (`interactive`/`spotify_token`): logout is skipped;
  the old player object stays alive with the live race.

### 5.3 Consequence for the plugin

- `POST /player/stop` is **never** issued from `src/main` (grep gate:
  `rg "/player/stop" src/main` must return nothing).
- **Logical stop** = remote `pause` + confirmation + generation retirement.
- **Replacement** = direct `play` over the held backend (play-over-play).
- Any contradictory/stale event, failed barrier, hung call, WS loss, or
  FIFO death quarantines the backend; a stop-tainted backend is never
  reused by the plugin process (external orchestration restarts daemons).
- No finite "event drain delay" is ever treated as proof the race cleared.

---

## 6. Pipe backend data contract (audio plane)

Source: `output/driver-pipe.go`, `output/driver-pipe-unix.go`,
`player/player.go`.

- **Format**: interleaved, little-endian, **signed 16-bit** PCM
  (`s16le` transform: `int16(clamp(sample) * 32768)` written via
  `binary.LittleEndian.PutUint16`). A **stereo frame is 4 bytes**
  (L then R, little-endian).
- **Rate/channels**: hardcoded **44100 Hz, 2 channels**
  (`player/player.go` `SampleRate = 44100`, `Channels = 2`; the decoder
  refuses other rates/channels, so pipe output is always 44.1 kHz stereo).
  `metadata.sample_rate` reports this same value from the decoder.
- **Writer loop**: `outputLoop` reads up to `4*1024` float samples, applies
  volume unless `external_volume`, transforms to s16le bytes, and
  `file.Write`s them (single blocking write per chunk).
- **`Pause()`** sets a `paused` flag and the loop waits on a condvar —
  **stops issuing new writes only**; anything already in the FIFO/kernel
  pipe buffer stays there.
- **`Drop()` is a NO-OP** (returns `nil`) — the pipe backend **cannot flush
  stale PCM**. After a seek, pre-seek bytes already buffered in the kernel
  FIFO **will arrive after** the new position's audio. ⇒ The plugin's seek
  handshake must **drain and discard** FIFO bytes under caps before seeking
  (DECISIONS.md constants), and position must never be derived from PCM.
- **Reader disconnect** → `file.Write` returns `EPIPE` → the loop reports
  the error, sets `closed`, closes the file → the player emits
  `EventTypeStop`. **`resume` will NOT recreate the pipe output** — a fresh
  `/player/play` is required. (This is why the plugin's FIFO reader must
  survive/reopen and why a dead reader ⇒ backend quarantine.)
- **`wait_for_reader: true`** ⇒ `os.OpenFile(path, O_WRONLY)` **blocks**
  until a reader opens the FIFO read-end (`driver-pipe-unix.go`). With
  `wait_for_reader: false` the open is `O_WRONLY|O_NONBLOCK`, fails `ENXIO`
  without a reader, then falls back to blocking.
- **EOF** on the sample source sets `paused = true` (no more writes), not
  `closed`.
- The daemon writes **only while playing** (the loop waits while paused), so
  a paused plugin must keep draining the FIFO to avoid write backpressure
  when playback resumes (see DECISIONS.md — reader stays open and draining
  during active playback/pause).

---

## 7. Quick reference: JSON field-name index

| Shape | Fields |
| --- | --- |
| `root` (`GET /`) | `playback_ready` |
| `status` (`GET /status`) | `username device_id device_type device_name play_origin stopped paused buffering volume volume_steps repeat_context repeat_track shuffle_context track` |
| `track` (status + `metadata` event) | `uri name artist_names album_name album_cover_url position duration release_date track_number disc_number format codec bitrate sample_rate bit_depth` |
| `play` (req) | `uri skip_to_uri paused position` |
| `seek` (req) | `position relative` |
| `volume` (event + `GET /player/volume`) | `value max` |
| event `playing` data | `context_uri uri resume play_origin` |
| event `paused`/`not_playing`/`will_play` data | `context_uri uri play_origin` |
| event `seek` data | `context_uri uri position duration play_origin` |
| event `stopped` data | `play_origin` (only) |
| event `repeat_track`/`repeat_context`/`shuffle_context` data | `value` |
| event `playback_ready`/`active`/`inactive` data | `null` |
