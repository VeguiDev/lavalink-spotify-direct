# Changelog

All notable changes to lavalink-go-librespot are documented here. This
project follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 2.0.0 (unreleased)

### Added

- **Collection (playlist/album) loading via the Spotify Web API**: playlists
  and albums now resolve directly through Spotify's client-credentials API
  with pagination (up to 500 tracks / 10 pages), instead of relying on the
  go-librespot daemon passthrough, which Spotify rate-limits (HTTP 429 →
  "Unknown file format").
- **Audio source renamed to `spotify`** (was `spdirect`). The `spdirect:<id>`
  identifier syntax is unchanged.
- **New optional config keys** under `plugins.golibrespot`:
  `spotifyClientId`, `spotifyClientSecret`, and `spotifyMarket` (default
  `"AR"`). Collection loading requires the client credentials.

## 1.0.0 (unreleased)

Initial public release of lavalink-go-librespot, a Lavalink v4 plugin that
plays Spotify tracks by controlling an external go-librespot v0.9.0 daemon.

### Added

- **`spdirect` audio source**: claims exactly `spdirect:<22-char-base62>` and
  `spdirect:spotify:track:<id>`; ordinary Spotify URIs and URLs are not
  claimed, keeping LavaSrc coexistence deterministic.
- **Strict plugin configuration** under `plugins.golibrespot` with
  fail-on-unknown key binding and startup-fatal semantic validation
  (`config.GoLibrespotConfig`, `config.BackendConfig`,
  `config.GoLibrespotConfigValidator`).
- **Typed, time-bounded REST client** for the daemon control plane
  (`backend.rest.GoLibrespotRestClient`), with typed timeouts and no
  success-inference from HTTP 200.
- **Tolerant reconnecting events WebSocket client**
  (`backend.ws.EventsWebSocketClient`) with bounded exponential backoff,
  quarantine thresholds, and a generation filter for stale connections.
- **Exclusive backend pool** (`pool.ExclusivePool`): fair, generation-stamped
  leases acquired at play time, never at load time; READY / LEASED /
  QUARANTINING / DEGRADED / DEAD backend states.
- **FIFO/PCM transport**: cancellable FIFO opener with dummy-writer
  rendezvous (`fifo.FifoOpener`), always-draining reader
  (`fifo.FifoReader`), and s16le stereo decoder (`fifo.PcmDecoder`) with
  partial-frame preservation and discard mode.
- **Lifecycle coordination**: activation barriers, a serialized backend state
  machine, a strict pause/drain/seek/resume handshake, logical stop, and
  exactly-once cleanup (`lifecycle.*`). The daemon stop endpoint is never
  issued (v0.9.0 stop-race).
- **Lease-free load-time metadata** via the daemon's
  `GET /web-api/v1/tracks/{id}` passthrough (`metadata.MetadataResolver`);
  metadata is never fabricated.
- **Secret-redacting log sanitizer** (`logging.LogSanitizer`).
- **Zero runtime dependencies**: REST and WebSocket use the JDK's
  `java.net.http` client.
- **Documentation**: architecture, configuration reference, LavaSrc
  coexistence, troubleshooting, security model, API contract, and design
  decisions.
- **Compose deployment layout** (`deploy/`): pinned go-librespot v0.9.0
  container, internal-only network, FIFO init, and mandatory daemon options.
- **Apache-2.0 license** and third-party notices (go-librespot GPL-3.0
  external process, not bundled).
