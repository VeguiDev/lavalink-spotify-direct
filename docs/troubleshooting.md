# Troubleshooting

This guide covers the problems operators actually hit with
lavalink-go-librespot. It assumes you are running a go-librespot v0.9.0
daemon with the mandatory configuration from
[`config.md`](./config.md) and the pinned API behavior in
[`API_CONTRACT.md`](./API_CONTRACT.md).

The single most useful diagnostic is the daemon's log plus Lavalink's log.
The plugin logs every coordinator action and every quarantine with a reason,
and all log lines are redacted by the log sanitizer.

## Load fails, no track is returned

`spdirect:<id>` loads return nothing when:

- **The identifier is not claimed.** The plugin claims exactly
  `spdirect:<22-char-base62>` and `spdirect:spotify:track:<id>`. `spotify:track:`
  URIs and `open.spotify.com` URLs are not claimed; they fall through to other
  sources. Use the LavaSrc workflow described in
  [`lavasrc-coexistence.md`](./lavasrc-coexistence.md).
- **No backend is READY.** Metadata resolution walks READY backends only. If
  every daemon is quarantined, degraded, or down, loads fail. Check the pool
  state in the logs and the daemon's health.
- **The daemon has no active Spotify session.** The `/web-api` passthrough
  returns 204 without a session, which is a metadata failure. Authenticate the
  daemon (`credentials.type: interactive`) and confirm it is connected.
- **The metadata payload is unusable.** A missing title or a non-positive
  `duration_ms` makes the load fail rather than fabricate metadata. This is
  intentional.

## Track loads but no audio plays

Check, in order:

1. **FIFO path mismatch.** `audio_output_pipe` in the daemon config must be
   the same absolute path as the plugin's `fifoPath`.
2. **FIFO permissions.** The daemon process and the Lavalink process must both
   be able to open the named pipe. Use a shared group with group-write access;
   never world-writable if a shared user/group works.
3. **`audio_output_pipe_wait_for_reader`.** It must be `true`. With `false`,
   the daemon opens the pipe non-blocking and fails with `ENXIO` when no
   reader is attached.
4. **The FIFO actually exists.** If the FIFO init runs after Lavalink boots,
   the backend is degraded until the pipe appears. With `fifoCheck: fail` the
   plugin refuses to start instead.

## A backend is quarantined (`QUARANTINING` / `DEGRADED`)

The plugin quarantines a backend instead of working around it. Common causes:

- **Repeated WebSocket failures.** After `wsFailuresBeforeQuarantine`
  consecutive connect failures or read errors, the backend quarantines.
  Check that `wsUrl` (or the derived URL) is reachable. A wrong port, or the
  daemon's default ephemeral port, is the usual culprit.
- **Hung HTTP calls.** A REST call that exceeds `restTimeoutMs` marks the
  backend transiently quarantined. A daemon in the v0.9.0 stop-race state
  accepts connections but never answers; nothing the plugin does can recover
  it.
- **Contradictory state.** Events that contradict `/status` or a completed
  phase, or a stop-taint, produce a `DEGRADED` state that is
  process-permanent.

Recovery is **external orchestration**: restart the daemon (a container
restart). The plugin never reuses a quarantined daemon in the same process.
A transient quarantine can re-admit itself after a fresh WebSocket, an idle
`/status`, and a reopened FIFO prove health.

## Playback stops or the daemon appears wedged

If the daemon accepts TCP connections but every API call hangs forever, it
is likely in the v0.9.0 stop-race state: the plugin's request lane has no
consumer. This is exactly why the plugin never issues the daemon's stop
endpoint. Restart the daemon container.

## Seek lands in the wrong place, or audio glitches after a seek

Position is daemon-authoritative. Pre-seek PCM already buffered in the kernel
pipe can arrive after a seek; the plugin drains and discards FIFO bytes under
caps before seeking. If you hear a glitch:

- Confirm `drainTimeoutMs` / `drainByteCap` are not absurdly small for the
  pipe's buffered content.
- Confirm you are not also running a second reader on the same FIFO (a second
  spdirect source instance, or a leftover process). Two readers on one pipe
  corrupt delivery.

## On Windows or macOS

The pipe audio backend is Linux-only (`//go:build windows` stub returns
"pipe output is not supported on Windows"). This plugin requires Linux for
the daemon and the FIFO.

## The plugin refuses to start

Startup-fatal config errors name the offending field. Common ones:

- `backends` empty while `enabled: true`
- duplicate backend `name`
- `restBaseUrl` not a valid `http(s)://` URL
- `fifoPath` not absolute
- a non-positive timeout value

If Lavalink starts but spdirect loads all fail, the plugin is running
degraded (no READY backend). That is a healthy design: Lavalink and every
other source keep working.
