# Design Decisions — lavalink-go-librespot

> Pins for the plugin that adapts an **external** go-librespot **v0.9.0**
> daemon (tag `cddcada819ba01966330ce5f0b10535494443cf3`) via REST +
> `/events` WebSocket + s16le FIFO. API shapes are pinned in
> [`API_CONTRACT.md`](./API_CONTRACT.md); this file records the plugin's own
> constants, configuration schema, Lavaplayer event mapping, and the verified
> dependency coordinates.

---

## 1. Constants (canonical — owned by this table)

| Constant | Value | Rationale / source |
| --- | --- | --- |
| REST request timeout | **5 s** (`restTimeoutMs`) | Time-bounded control plane; daemon handlers swallow errors so we time-bind instead. |
| Metadata fetch timeout | **5 s** (`metadataTimeoutMs`) | Best-effort `/web-api/v1/tracks/{id}` at load time, never blocks a track load path past this. |
| Activation barrier | **15 s** (`activationTimeoutMs`) | Bound on `POST /player/play` → matching `playing`/`will_play` of the current generation. |
| Pause-ack | **5 s** (`pauseAckTimeoutMs`) | Bound on remote `pause` → matching `paused` event/status during logical stop and seek handshake. |
| Seek-ack | **10 s** (`seekAckTimeoutMs`) | Bound on absolute `seek` → matching `seek` event/status position. |
| Drain caps | **5 s / 4 MiB** (`drainTimeoutMs` / `drainByteCap`) | Bound for discarding pre-seek FIFO PCM (`Drop()` is a no-op in v0.9.0 pipe, so stale bytes must be drained and dropped). |
| WS reconnect backoff | **1 s → 30 s** (`wsReconnectInitialMs`→`wsReconnectMaxMs`), exponential with jitter | Bounded reconnect; never reconnect-forever without quarantining. |
| Quarantine threshold | **5 consecutive WS failures** (`wsFailuresBeforeQuarantine`) | 5 failed connect attempts / consecutive read errors ⇒ backend quarantined. |
| Pool acquire timeout | **30 s** (`poolAcquireTimeoutMs`) | Fair FIFO wait for an exclusive lease; bounded, no indefinite waits. |
| FIFO read buffer | **16 KiB** (`fifoReadBufferBytes`) | Chunked reads off the named pipe; never `readAllBytes`/`available()`. |
| s16le stereo frame | **4 bytes** | 2 channels × 16-bit little-endian (`player.SampleRate = 44100`, `Channels = 2`). |
| Daemon-side WS write timeout | **10 s** (observed, not a plugin constant) | `api_server.go` `const timeout = 10 * time.Second` per client per `Emit`; the plugin's WS client must drain promptly and reconnect cleanly. |
| Daemon handler timeout | **30 s** (observed) | `handleApiRequest` per-request budget. |
| Player event buffer | **128** (observed) | `player/player.go` `make(chan Event, 128)`; the root of the stop-race (API_CONTRACT.md §5). |

Quarantine policy (from decisions notepad):

- **Re-admissible** after: fresh WS connection + idle `/status` + FIFO reopened, for failures like WS loss, HTTP hang, barrier timeout.
- **Process-permanent**: stop-taint (any `/player/stop` ever issued) and contradictory-state (events that contradict `/status` or a completed phase). External orchestration restarts the daemon.

---

## 2. Configuration schema (plugin)

Namespace: `plugins.golibrespot` (Spring `@ConfigurationProperties`, Jakarta
validation, `fail-on-unknown` keys).

### 2.1 Backends

```yaml
plugins:
  golibrespot:
    backends:
      - name: gb-1                    # unique, required
        restBaseUrl: http://golibrespot:3678   # required, valid URL
        wsUrl: ws://golibrespot:3678/events    # optional — default DERIVED from restBaseUrl (http→ws, https→wss, append /events)
        fifoPath: /spdirect/spdirect-1.fifo    # required, ABSOLUTE path
```

Validation rules (startup-fatal):

- duplicate `name` across backends
- `restBaseUrl` not a valid `http(s)://` URL
- `wsUrl` present but invalid, or scheme mismatch
- `fifoPath` not absolute (and not on Windows — Linux-only deployment)
- unsupported enum values / non-numeric timeouts
- empty `backends` with the plugin `enabled: true`

FIFO existence is **NOT** validated at startup (deployment race — the FIFO
init container may run after Lavalink). `fifoCheck: warn` (default) marks the
backend degraded until the FIFO appears; `fifoCheck: fail` refuses to start.

### 2.2 Global timeouts with per-backend override

```yaml
plugins:
  golibrespot:
    enabled: true
    restTimeoutMs: 5000
    metadataTimeoutMs: 5000
    activationTimeoutMs: 15000
    pauseAckTimeoutMs: 5000
    seekAckTimeoutMs: 10000
    drainTimeoutMs: 5000
    drainByteCap: 4194304          # 4 MiB
    wsReconnectInitialMs: 1000
    wsReconnectMaxMs: 30000
    wsFailuresBeforeQuarantine: 5
    poolAcquireTimeoutMs: 30000
    fifoReadBufferBytes: 16384
    fifoCheck: warn                # warn | fail
    backends:
      - name: gb-1
        restBaseUrl: http://golibrespot:3678
        fifoPath: /spdirect/spdirect-1.fifo
        # optional per-backend override (any global key):
        restTimeoutMs: 8000
```

Every global timeout key may be repeated inside a `backends[]` entry to
override that backend's value; the per-backend value wins.

### 2.3 Required external daemon configuration (deployment)

Mirrored from API_CONTRACT.md §4 — the operator's go-librespot `config.yml`
must set: `audio_backend: pipe`, `audio_output_pipe: <absolute FIFO path>`,
`audio_output_pipe_format: s16le`, `audio_output_pipe_wait_for_reader: true`,
`disable_autoplay: true`, `external_volume: true`, `crossfade_duration: 0`,
`server.enabled: true` with explicit `server.address`/`server.port` (default
port 0 = ephemeral), `credentials.type: interactive` (v0.9.0 enum:
`interactive` | `spotify_token` | `zeroconf`; **`device_auth` is
MASTER-ONLY and must not be used**).

---

## 3. Lavaplayer player-event → coordinator-action mapping

Correlation rule: an event is accepted only when
`event.uri == expected-uri && generation == current-generation &&
phase == expected-phase`; everything else is tolerated-ignored, and a
contradiction (event contradicts `/status` or a completed phase) routes to
quarantine.

| Lavaplayer event / lifecycle | Coordinator action | Confirmation |
| --- | --- | --- |
| `onTrackStart` (play) | Acquire exclusive lease (bounded 30 s) → open FIFO reader (async, cancellable, dummy-writer rendezvous) → confirm reader open → `POST /player/play` → await current-generation `playing`/`will_play` (barrier ≤ 15 s; on timeout ⇒ quarantine + fail track) | `playing` (or `will_play` then `playing` after activation) |
| `onPlayerPause` | Remote `POST /player/pause` (async; never block the player thread) | `paused` event / `/status.paused` within 5 s |
| `onPlayerResume` | Remote `POST /player/resume` | `playing` with `resume: true` |
| `onTrackSeek` | Seek handshake (strict order, see below) | `seek` event / `/status.position` within 10 s |
| `onTrackEnd(FINISHED)` | Natural completion: await current-generation `not_playing` **after** activation (a single event never proves completion), reconcile `/status`, then release lease + retire generation | `not_playing` + `/status` idle |
| `onTrackEnd(STOPPED)` | **Logical stop (never `/player/stop`)**: remote pause → pause-ack (5 s) → retire generation → release lease | `paused` + generation retired |
| `onTrackEnd(REPLACED)` | Play-over-play on the held backend: generation bump + URI guard, re-issue `play` on stale-advance `not_playing` mismatch (never release-then-acquire) | `will_play`/`playing` of new generation |
| `onTrackStuck` | Abort track, quarantine backend | — |
| `onTrackException` | Abort track, quarantine backend, clear load failure | — |
| `player destroy` | Async release of lease (exactly-once, idempotent) | — |
| Plugin shutdown | Cancel FIFO opens (dummy writer), close WS, drain executors, bounded join; zero blocked opener threads | — |

### 3.1 Seek handshake (strict order — from decisions draft)

1. Remote `pause` → await matching `paused`/`/status` (5 s).
2. Drain + discard FIFO pre-seek PCM under caps (5 s / 4 MiB) via the PCM
   decoder's discard mode (never `Thread.sleep` as the drain mechanism —
   non-blocking reads + timeouts).
3. Clear partial PCM frame.
4. `POST /player/seek` absolute (`{"position": <ms>, "relative": false}`) →
   await matching `seek` event/`/status` position (10 s).
5. Pipeline `seekPerformed`.
6. Resume iff the desired player state is playing.

Any failure ⇒ abort the track + quarantine the backend. Position is
daemon-authoritative, never PCM-derived. Two rapid seeks are serialized.

---

## 4. Verified dependency coordinates (checked 2026-08-27)

Repository: `https://maven.lavalink.dev/releases` (Reposilite; **directory
listing disabled** — every artifact path was fetched directly). Maven Central
`https://repo1.maven.org/maven2`. Plugin portal
`https://plugins.gradle.org/m2`.

### 4.1 Lavalink plugin API

| Check | URL fetched | Result |
| --- | --- | --- |
| metadata | `https://maven.lavalink.dev/releases/dev/arbjerg/lavalink/plugin-api/maven-metadata.xml` | **HTTP 200**; versions incl. `4.2.0`, `4.2.1`, `4.2.2` |
| POM | `https://maven.lavalink.dev/releases/dev/arbjerg/lavalink/plugin-api/4.2.2/plugin-api-4.2.2.pom` | **HTTP 200**; `groupId=dev.arbjerg.lavalink`, `artifactId=plugin-api`, `version=4.2.2`; depends on `dev.arbjerg.lavalink:protocol-jvm:4.2.2` + Spring Boot + lavaplayer |
| wrong path | `.../dev/arbjerg/lavalink/lavalink-plugin-api/maven-metadata.xml` | **HTTP 404** — the artifact is `plugin-api`, not `lavalink-plugin-api` |

**→ GAV: `dev.arbjerg.lavalink:plugin-api:4.2.2`** (scope `compileOnly`).

### 4.2 Test/run server (for `runLavaLink` + integration smoke)

| Check | URL fetched | Result |
| --- | --- | --- |
| run server metadata | `https://maven.lavalink.dev/releases/dev/arbjerg/lavalink/Lavalink-Server/maven-metadata.xml` | **HTTP 200**; versions incl. `4.2.2` |
| `lavalink-testserver` | `.../dev/arbjerg/lavalink/lavalink-testserver/maven-metadata.xml` | **HTTP 404 — does not exist** |
| `lavalink-server` | `.../dev/arbjerg/lavalink/lavalink-server/maven-metadata.xml` | **HTTP 404 — does not exist** |
| `Lavalink` (archivesName) | `.../dev/arbjerg/lavalink/Lavalink/maven-metadata.xml` | **HTTP 404 — not published under this name** |

**→ GAV: `dev.arbjerg.lavalink:Lavalink-Server:4.2.2@jar`** — the executable
Spring Boot run server. Cross-checked in the released Gradle plugin 1.1.2
source (`LavalinkGradlePlugin.kt`): `dependencies.create("dev.arbjerg.lavalink:Lavalink-Server:$serverVersion@jar") { isTransitive = false }`.
Use `@jar`, non-transitive, for integration smoke tests and the `runLavaLink`
task.

### 4.3 Lavaplayer fork

| Check | URL fetched | Result |
| --- | --- | --- |
| `dev.arbjerg:lavaplayer` | `https://maven.lavalink.dev/releases/dev/arbjerg/lavaplayer/maven-metadata.xml` | **HTTP 200**; versions incl. `2.2.6`, **`2.2.7`** |
| `com.github.walkyst:lavaplayer-fork` | `https://repo1.maven.org/maven2/com/github/walkyst/` and `.../com/github/walkyst/lavaplayer-fork/maven-metadata.xml` | **HTTP 404 — not on Maven Central** (`com.github.*` is the JitPack convention; the walkyst fork is a legacy JitPack-only coordinate) |
| Lavalink 4.2.2 server catalog | local clone `lavalink-4.2.2` `settings.gradle.kts` | uses `dev.arbjerg:lavaplayer:2.2.6` |
| LavaSrc 4.8.3 | local clone `main/build.gradle.kts` | uses `dev.arbjerg:lavaplayer:2.0.4` |

**→ GAV: `dev.arbjerg:lavaplayer:2.2.7`** (replaces the obsolete
`com.github.walkyst:lavaplayer-fork` assumption; 2.2.7 exists in the same
family Lavalink itself uses).

### 4.4 Lavalink Gradle plugin

| Check | URL fetched | Result |
| --- | --- | --- |
| marker | `https://plugins.gradle.org/m2/dev/arbjerg/lavalink/gradle-plugin/dev.arbjerg.lavalink.gradle-plugin.gradle.plugin/maven-metadata.xml` | **HTTP 200**; **`latest = 1.1.2`** (range 1.0.3–1.1.2) |
| stale candidate | `.../io/github/arbjerg/lavalink/lavalink-plugin/maven-metadata.xml` | HTTP 303 (redirect) — the real id is `dev.arbjerg.lavalink.gradle-plugin` |
| task name | released source `lavalink-gradle-plugin` (1.1.2) `LavalinkGradlePlugin.kt` | `register<RunLavalinkTask>("runLavaLink")` — **task is `runLavaLink`** (capital L), not `runLavalink` |

**→ Plugin id: `dev.arbjerg.lavalink.gradle-plugin` version `1.1.2`** from
`pluginManagement { plugins { id("dev.arbjerg.lavalink.gradle-plugin") version "1.1.2" } }`.
The exact Gradle task name `runLavaLink` must be re-verified with
`./gradlew tasks` at build time (official prose says `runLavalink`; the
released source and the actual task say `runLavaLink`).

### 4.5 Test dependencies (Maven Central)

| Dependency | URL fetched | Latest overall | **Pin** |
| --- | --- | --- | --- |
| `org.junit.jupiter:junit-jupiter` | `https://repo1.maven.org/maven2/org/junit/jupiter/junit-jupiter/maven-metadata.xml` | `6.1.3` (JUnit 6 line) | **`5.11.4`** (latest 5.11.x; plan pins 5.11.x) |
| `org.assertj:assertj-core` | `https://repo1.maven.org/maven2/org/assertj/assertj-core/maven-metadata.xml` | `3.27.7` (stable 3.x; `4.0.0-M1` milestone) | **`3.26.3`** (latest 3.26.x; plan pins 3.26.x) |
| `org.awaitility:awaitility` | `https://repo1.maven.org/maven2/org/awaitility/awaitility/maven-metadata.xml` | `4.3.0` | **`4.2.2`** (latest 4.2.x; plan pins 4.2.x) |
| `org.java-websocket:Java-WebSocket` | `https://repo1.maven.org/maven2/org/java-websocket/Java-WebSocket/maven-metadata.xml` | `1.6.0` | **`1.5.7`** (latest 1.5.x; test-only, fake daemon) |

All four test deps are `testImplementation` only. Java-WebSocket is used
solely by the in-JVM fake daemon fixture — the product has **zero runtime
dependencies** (`java.net.http` HttpClient + WebSocket cover REST and WS).

---

## 5. Lifecycle & risk decisions (summary)

- **No-normal-`/player/stop` policy** — `/player/stop` never appears in
  `src/main` (grep gate). Logical stop = remote pause + ack + generation
  retirement; replacement = play-over-play; contradiction → quarantine.
- **Lease at play, never at load** — `loadItem` acquires no backend;
  metadata is a best-effort no-lease `/web-api` fetch; failure = clear typed
  load failure (never fake metadata/duration).
- **Claim namespace** — exactly `spdirect:<22-char-base62>` and
  `spdirect:spotify:track:<id>`; ordinary Spotify URIs/URLs are understood
  only for diagnostics/conversion hints; source name `spotify`.
- **Collection resolution may exceed the track-metadata budget** — resolving a
  playlist/album through the Spotify Web API may paginate up to 500 tracks /
  10 pages and can therefore run longer than the 5 s `metadataTimeoutMs`
  track-metadata budget. This is a deliberate, bounded deviation: single-track
  metadata stays within 5 s; collection resolution is capped at 500 tracks and
  10 pages.
- **Volume is pipeline-only** (`external_volume: true`; no daemon volume
  forwarding).
- **Events have no sequence/session id** — correlation is URI + generation
  + phase only; unknown event types/fields are tolerated.
- **FIFO hygiene** — reader stays open and draining during active playback
  and pause (no daemon write backpressure); never unlink at runtime; pure-Java
  cancellation via dummy-writer rendezvous; never `Thread.sleep` for
  handshakes.
- **Distribution** — GitHub-Release-first JAR + SHA-256 + CycloneDX SBOM +
  attestation; Maven Central deferred (no owner namespace).
- **License** — Apache-2.0 adapter + THIRD_PARTY_NOTICES (go-librespot is a
  separate GPL-3.0 process, not bundled).
