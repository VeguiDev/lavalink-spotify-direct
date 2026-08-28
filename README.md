# lavalink-go-librespot

A Lavalink v4 plugin that plays Spotify tracks by controlling an **external**
[go-librespot](https://github.com/devgianlu/go-librespot) daemon: REST control,
an `/events` WebSocket for state, and the pipe-backend raw PCM FIFO fed into
Lavaplayer.

```
┌──────────────┐   REST + /events WS    ┌─────────────────┐
│   Lavalink   │ ─────────────────────▶ │ go-librespot    │
│   (plugin)   │                        │  daemon v0.9.0  │
│              │ ◀───────────────────── │  (external)     │
└──────┬───────┘   state + metadata     └────────┬────────┘
       │  s16le PCM over a named pipe (FIFO)      │
       └──────────────────────────────────────────┘
```

The plugin **does not implement any part of the Spotify protocol**. No audio
decryption, no authentication, no token management, no CDN access. The daemon
does all of that; the plugin only tells it what to play, watches its events,
and reads the PCM it writes. The daemon is a **separate process** and stays
that way: it is never embedded, copied, vendored, linked, or forked inside
this project.

## What it is

- A **Lavalink 4.2.x audio source** named `spdirect`. It claims exactly
  `spdirect:<22-char-track-id>` and `spdirect:spotify:track:<id>` identifiers.
- A **remote player controller** that drives one or more go-librespot daemons
  over their unauthenticated REST API and reconciles state through the
  `/events` WebSocket.
- A **PCM transport** that reads the daemon's pipe backend output
  (interleaved s16le, 44.1 kHz stereo) from a named pipe and feeds it into the
  Lavaplayer pipeline, so the rest of your Lavalink stack (filters, mixing,
  forwarding) works as usual.
- A **lifecycle manager** that serializes every backend operation, quarantines
  a daemon when it behaves inconsistently, and never leaves a session half-open.

## What it is NOT

- **Not a Spotify implementation.** There is no Spotify protocol, decryption,
  auth, or CDN code in this project. Everything Spotify-related happens inside
  the go-librespot daemon.
- **Not a go-librespot bundle.** go-librespot (GPL-3.0) is a separate external
  process you run yourself. See [THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES)
  and [docs/security.md](docs/security.md).
- **Not a search source.** The plugin cannot search Spotify and does not claim
  `open.spotify.com` URLs or `spsearch:`-style identifiers. Pair it with
  [LavaSrc](https://github.com/topi314/LavaSrc) for search and discovery; see
  [docs/lavasrc-coexistence.md](docs/lavasrc-coexistence.md).
- **Not a Spotify Connect player.** Other devices cannot control playback
  through this plugin. Playback is initiated and controlled by Lavalink only.

## Requirements

| Requirement | Notes |
| --- | --- |
| Java 17+ | The plugin is compiled to Java 17 bytecode and runs inside the Lavalink server JVM. |
| Lavalink 4.2.2 | Built against `dev.arbjerg.lavalink:plugin-api:4.2.2`. |
| Linux host | The pipe audio backend is Linux-only (`audio_backend: pipe`). Windows and macOS are not supported. |
| go-librespot v0.9.0 daemon | External process, run as a separate container or service. Pinned to tag `v0.9.0`; see [docs/API_CONTRACT.md](docs/API_CONTRACT.md). |
| Spotify Premium account | Required by go-librespot. Authentication is interactive/zeroconf inside the daemon; the plugin never sees your credentials. |
| Network access | The daemon needs outbound access to the Spotify API/CDN. The plugin and the daemon must reach each other over an internal network. |

## Install

1. Download the plugin JAR from the project's GitHub Release page (artifact
   `lavalink-go-librespot-1.0.0.jar`).
2. Place the JAR into Lavalink's `plugins/` directory (create it if it does
   not exist).
3. Restart Lavalink.

Lavalink 4.2.x loads every `.jar` it finds in `plugins/`. The plugin is
identified by the `lavalink-plugin.yml` descriptor embedded at the JAR root
(plugin name `golibrespot`, main class
`dev.lavalinkplugins.golibrespot.GoLibrespotPlugin`). During development the
Gradle build also emits a `lavalink-plugins/golibrespot.properties` descriptor
for the bundled test server, but a release install only needs the JAR in
`plugins/`.

The release is distributed as a plain JAR with **zero runtime dependencies**:
REST and WebSocket traffic use the JDK's `java.net.http` client, so no extra
jars are required.

## Quickstart (Docker Compose)

A ready-to-run compose layout lives in the `deploy/` directory (created in
parallel with this documentation). It runs Lavalink plus a pinned
`ghcr.io/devgianlu/go-librespot:v0.9.0` container on an internal-only bridge
network, creates the named pipes with a one-shot init container, and keeps the
daemon's REST/WS port off the host.

High-level shape:

```yaml
services:
  lavalink:
    image: ghcr.io/lavalink-devs/lavalink:4.2.2
    volumes:
      - ./application.yml:/opt/Lavalink/application.yml
      - ./plugins:/opt/Lavalink/plugins
  golibrespot:
    image: ghcr.io/devgianlu/go-librespot:v0.9.0   # pinned, never latest
    # internal-only network, no published ports (the daemon API has no auth)
```

Both containers share a volume holding one FIFO per backend
(e.g. `spdirect-1.fifo`). The go-librespot configuration inside the compose
layout sets the mandatory options documented in
[docs/config.md](docs/config.md).

See the `deploy/` directory for the full files.

## Configuration

The plugin is configured under the `plugins.golibrespot` namespace in
Lavalink's `application.yml`. Minimal example:

```yaml
plugins:
  golibrespot:
    enabled: true
    backends:
      - name: gb-1
        restBaseUrl: http://golibrespot:3678
        fifoPath: /spdirect/spdirect-1.fifo
```

The `wsUrl` is optional: when omitted it is derived from `restBaseUrl`
(`http` -> `ws`, `https` -> `wss`, `/events` appended). A backend is one
daemon plus one FIFO. Global timeouts (activation, seek, drain, reconnect,
and so on) have defaults that mirror the project's decision record; every
timeout can be overridden per backend.

See [docs/config.md](docs/config.md) for the complete reference, including the
mandatory go-librespot daemon options (`audio_backend: pipe`,
`audio_output_pipe_format: s16le`, `disable_autoplay: true`,
`external_volume: true`, an explicit `server.port`, and
`credentials.type: interactive` for v0.9.0).

## Loading and playing tracks

`spdirect` identifiers are two forms:

- `spdirect:4uLU6hMCjMI75M1A2tKUQC`
- `spdirect:spotify:track:4uLU6hMCjMI75M1A2tKUQC`

Load a track the usual way: `GET /v4/loadtracks?identifier=spdirect:<id>`.
Ordinary Spotify URIs (`spotify:track:...`) and `open.spotify.com` URLs are
**not** claimed by this source; they fall through to other sources (or load
nothing). Metadata is resolved at load time through the daemon's
`/web-api/v1/tracks/{id}` passthrough, so a track only loads when a backend is
healthy.

To *find* tracks, use a search source such as LavaSrc, then take the returned
track's `info.identifier` and load it as `spdirect:<id>`. The full workflow is
described in [docs/lavasrc-coexistence.md](docs/lavasrc-coexistence.md).

## Lifecycle and quarantine expectations

Playback is coordinated against each daemon's real state. The important
operational facts:

- **The plugin never issues the daemon's stop endpoint.** go-librespot
  v0.9.0 has a known race where a stop during buffered event delivery can
  crash the daemon's request loop. A logical stop is a remote pause, a
  confirmed state, and session retirement instead.
- **Replacement is play-over-play.** Skipping from one spdirect track to the
  next reuses the held backend rather than stop-and-restart.
- **Position is daemon-authoritative.** The plugin does not track time from
  the PCM stream.
- **A misbehaving daemon is quarantined, not worked around.** Repeated
  WebSocket failures, hung HTTP calls, contradictory events, or a dead FIFO
  put the backend into `QUARANTINING`/`DEGRADED` state. The plugin never
  reuses a quarantined daemon and never retries forever.
- **Recovery is external orchestration.** Only a daemon restart (for example
  a container restart) recovers a quarantined backend. See
  [docs/troubleshooting.md](docs/troubleshooting.md).

## Security

- The go-librespot REST and WebSocket API has **no authentication**. Keep it
  on an internal network and never publish the daemon's port to the host or
  the internet. The compose layout in `deploy/` does exactly this.
- The plugin redacts secrets in its logs (bearer tokens, credentials, query
  strings) via a log sanitizer.
- Credentials for the Spotify account live in the daemon's configuration and
  are never handled by this plugin. The repository's `.gitignore` excludes
  `.env`, `secrets*.yml`, and `application.yml` so local secrets do not get
  committed.

See [docs/security.md](docs/security.md) for the full threat model.

## Documentation

- [docs/architecture.md](docs/architecture.md) - component layout and data flow
- [docs/config.md](docs/config.md) - configuration reference
- [docs/lavasrc-coexistence.md](docs/lavasrc-coexistence.md) - LavaSrc workflow
- [docs/troubleshooting.md](docs/troubleshooting.md) - common problems
- [docs/security.md](docs/security.md) - security and threat model
- [docs/API_CONTRACT.md](docs/API_CONTRACT.md) - the pinned go-librespot v0.9.0 API contract
- [docs/DECISIONS.md](docs/DECISIONS.md) - design decisions and constants

## License

Apache-2.0. go-librespot is a separate GPL-3.0 process that is not bundled
with or linked into this project. See [LICENSE](LICENSE) and
[THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES).

This is an unofficial project. It is not affiliated with, endorsed by, or
sponsored by Spotify, the Lavalink project, or go-librespot.
