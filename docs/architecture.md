# Architecture

This document describes how lavalink-go-librespot is built and how audio and
control data flow through it. It is a companion to
[`API_CONTRACT.md`](./API_CONTRACT.md) (the pinned go-librespot v0.9.0 wire
contract) and [`DECISIONS.md`](./DECISIONS.md) (the plugin's constants,
configuration schema, and lifecycle rules). Every class named here lives
under `src/main/java/dev/lavalinkplugins/golibrespot`.

## Big picture

The plugin adapts a Lavalink v4 server to one or more **external**
go-librespot daemons. Three planes connect them:

1. **Control plane**: REST calls to the daemon (`/player/play`, `/player/pause`,
   `/player/resume`, `/player/seek`, `GET /status`, `GET /`), issued by a
   typed, time-bounded HTTP client.
2. **State plane**: a persistent `/events` WebSocket that streams daemon state
   (`playing`, `paused`, `not_playing`, `seek`, `metadata`, and so on). Events
   carry no sequence number, so the plugin correlates them by URI, generation,
   and expected phase.
3. **Audio plane**: the daemon's pipe audio backend writes interleaved s16le
   44.1 kHz stereo PCM into a named pipe (FIFO). A per-backend reader drains
   it, a decoder turns bytes into `short[]` frames, and the frames are fed
   into the Lavaplayer pipeline.

The daemon's REST/WS API has no authentication, so all three planes travel
over an internal-only network. See [`security.md`](./security.md).

## The six layers

### 1. Plugin / source layer

This is what Lavalink sees.

- `GoLibrespotPlugin` (`dev.lavalinkplugins.golibrespot`) is the plugin entry
  point; it extends `PluginEventHandler` and is registered as a Spring bean.
- `GoLibrespotAudioSourceManager` is the `AudioSourceManager` for the source
  named `spdirect`. `loadItem` parses the identifier, resolves metadata, and
  returns a `GoLibrespotAudioTrack`, or `null` so Lavalink falls through to
  other sources. It never acquires a backend lease at load time.
- `TrackIdParser` (`identifier` package) decides what is claimed: exactly
  `spdirect:<22-char-base62>` and `spdirect:spotify:track:<id>`. Ordinary
  Spotify URIs and URLs are recognized only to suggest a conversion hint and
  are never claimed.
- `AudioTrackInfoMapper` + `TrackMetadata` map daemon metadata to a Lavaplayer
  `AudioTrackInfo` (`identifier = spdirect:<id>`, `isStream = true`). A track
  without a positive duration is never represented; nothing is fabricated.
- `GoLibrespotAudioTrack` is the playable track. Its `process()` loop blocks
  bounded on the coordinator's activation barrier, then feeds decoded frames
  through the pipeline until end of stream.
- `PlayerLifecycleBridge` is an `AudioEventAdapter` attached per Lavalink
  player. It routes only events whose track is a `GoLibrespotAudioTrack` and
  maps them to coordinator actions: start, pause, resume, seek, end, replace,
  destroy, quarantine.

### 2. Backend control layer

Everything that talks to a daemon.

- `GoLibrespotRestClient` (`backend.rest`) is the time-bounded REST client.
  Every call has a request timeout; a hang surfaces as a typed
  `RestException`. It never infers success from an HTTP 200, because the
  daemon swallows many internal errors (see `API_CONTRACT.md` section 1).
- `EventsWebSocketClient` (`backend.ws`) is the persistent, reconnecting
  `/events` client. It parses frames tolerantly, drains them on a dedicated
  thread so the socket never backs up, and reports connect/disconnect and
  quarantine thresholds. `EventType` and `PlayerEvent` model the event
  envelope.
- `backend.model` holds the small DTOs (`StatusDto`, `TrackDto`, `RootDto`,
  `PlayerCommandResult`, `StatusResult`, `WebApiResult`, ...) plus a minimal
  tolerant JSON parser.
- `MetadataResolver` (`metadata`) resolves a track's `AudioTrackInfo` at load
  time via the daemon's `GET /web-api/v1/tracks/{id}` passthrough, walking
  READY backends without leasing any of them.
- `LogSanitizer` (`logging`) redacts bearer tokens, credentials, and query
  secrets from every log line the plugin writes.
- `BackendStateMachine` (`lifecycle`) is the serialized state machine for one
  backend: one command in flight at a time, URI + generation + phase
  correlation, `/status` reconciliation, and the quarantine decision table.

### 3. Exclusive pool layer

Play-time backend allocation, never load-time.

- `ExclusivePool` (`pool`) hands out fair, generation-stamped, exclusive
  leases. `Lease` is the handle (release is idempotent and exactly-once).
  `BackendHandle` wraps a backend's config. `BackendState` models `READY`,
  `LEASED`, `QUARANTINING`, `DEGRADED`, and `DEAD`.

### 4. FIFO / PCM layer

The audio plane's plumbing.

- `FifoOpener` opens a FIFO read-end with a bounded timeout. Because a
  blocking open is not interrupt-cancellable, cancellation uses a
  `DummyWriterCancellation` rendezvous: a temporary blocking writer completes
  the open, both ends close, and the opener executor is proven drained.
- `FifoReader` is the always-draining reader (16 KiB chunks into a bounded
  queue, drop-oldest) that keeps the daemon's writes from backing up.
- `PcmDecoder` converts interleaved little-endian s16le stereo bytes into
  `short[]` frames, preserving partial frames, and supports a discard mode
  used to drop pre-seek stale PCM.

### 5. Lifecycle coordinator

The per-session brain.

- `LifecycleCoordinator` owns one backend session: acquire lease, open the
  FIFO (submitted before the play command, awaited after it), issue
  `POST /player/play`, wait on the activation barrier, then stream frames.
  `replace()` plays over the held lease for track-to-track transitions.
- `ActivationBarrier` gates the track's read loop until playback is confirmed
  or fails with a typed `ActivationException`.
- `SeekHandshake` implements the strict seek order: remote pause, confirm,
  drain + discard FIFO bytes under caps, clear the partial frame, absolute
  seek, confirm, resume if the player wants to be playing.
- `StopSequence` implements the logical stop and the destroy path. The daemon
  stop endpoint is never issued (v0.9.0 stop-race, `API_CONTRACT.md` section
  5); a logical stop is a remote pause with confirmation and generation
  retirement, and cleanup releases the lease exactly once.

### 6. Deployment / release surface

- The `deploy/` directory holds the compose layout: a Lavalink container, one
  pinned go-librespot v0.9.0 container per backend, an internal-only bridge
  network, a shared FIFO volume with an init step, and the daemon's mandatory
  configuration.
- This documentation set (README, `docs/`), the license and third-party
  notices, and the GitHub templates.
- The release artifact is a self-contained JAR (zero runtime dependencies)
  distributed from GitHub Releases.

## Load path (no lease)

```
/client -> GET /v4/loadtracks?identifier=spdirect:<id>
  GoLibrespotAudioSourceManager.loadItem
    TrackIdParser.parse(identifier)        -> TrackId, or null (NotClaimed/Malformed)
    MetadataResolver.resolve(id)           -> GET /web-api/v1/tracks/<id> on a READY backend
    AudioTrackInfoMapper.map(metadata)     -> AudioTrackInfo (never fabricated)
  -> GoLibrespotAudioTrack(id, info, manager)
```

A missing identifier, an unclaimed form, or a metadata failure all return
`null` from `loadItem`. Lavalink stays healthy; the load simply fails.

## Playback path

```
Lavaplayer fires onTrackStart
  PlayerLifecycleBridge -> coordinator.start(daemonUri, positionMs)
    acquire exclusive lease (bounded, fair)
    submit FIFO read-end open (async, cancellable)
    BackendStateMachine.activate -> POST /player/play
    await FIFO open (rendezvous with the daemon's writer)
    start FifoReader
    await activation barrier (playing/will_play of this generation)
  GoLibrespotAudioTrack.process()
    awaitActivated()                         -> frames short[] | null(EOS)
    nextFrame(50ms) -> PcmDecoder -> pipeline -> Lavaplayer
```

## Pause / resume / seek / end

| Event | Action |
| --- | --- |
| `onPlayerPause` | remote pause, confirm with `paused` event or `/status` |
| `onPlayerResume` | remote resume, confirm with `playing` (`resume: true`) |
| `onTrackSeek` | `SeekHandshake` (pause -> drain FIFO -> seek -> confirm -> resume) |
| `onTrackEnd` FINISHED | natural completion: wait for `not_playing`, reconcile `/status`, release lease |
| `onTrackEnd` STOPPED | logical stop: remote pause + confirmation + retire generation |
| `onTrackEnd` REPLACED (spdirect next) | play-over-play on the held lease |
| `onTrackEnd` REPLACED (foreign next) | logical stop |
| `onTrackEnd` CLEANUP | destroy: async, exactly-once release |
| `onTrackException` / `onTrackStuck` | abort the track and quarantine the backend |

The full mapping table, including the correlation rule, is in
`DECISIONS.md` section 3.

## Quarantine model

A backend that contradicts its own state, fails an activation barrier, hangs,
loses its WebSocket too many times, or loses its FIFO is quarantined rather
than retried. `BackendState.QUARANTINING` is transient and can be re-admitted
(a fresh WebSocket, an idle `/status`, and a reopened FIFO prove health).
`DEGRADED` is process-permanent (stop-taint or contradictory state) and
`DEAD` is terminal. The plugin never reuses a quarantined backend in the same
process; recovering one requires an external daemon restart, which is why the
compose layout treats daemons as restartable units.

## Secrets handling

Credentials and tokens never enter the plugin: authentication happens inside
the daemon. The plugin's own REST/WS traffic carries no credentials of its
own (the daemon attaches the Spotify bearer token only to its outbound
`/web-api/` proxy calls). Every log line passes through `LogSanitizer`, which
redacts `Authorization` headers, query-string secrets, and credential fields.
