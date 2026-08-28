# Deployment — Docker Compose (lavalink-go-librespot)

> **Linux only.** The go-librespot `pipe` audio backend is a stub on Windows
> ("pipe output is not supported on Windows"). This stack must run on a Linux
> Docker host.
>
> **Requires a Spotify Premium account** — one login per daemon backend.

## Topology

```
fifo-init (one-shot) ── mkfifo /spdirect/spdirect-<n>.fifo (0660, root:<PGID>)
   │                    + seed per-daemon /config volume (config.yml, chown)
   ▼
golibrespot-1 / golibrespot-2   go-librespot v0.9.0, credentials.type=interactive
   │  REST/WS API (UNAUTHENTICATED) — internal network ONLY, never published
   ▼
lavalink 4.2.2 + plugin JAR      publishes ONLY port 2333 (your bot)
```

All containers share one bridge network (`spdirect`). The go-librespot daemon
**requires outbound internet at startup** (clienttoken.spotify.com, dealer,
web-api — verified 2026-08-28: on an `internal: true` network it exits with a
DNS failure before even printing the login link), so the network is **not**
`internal`. Isolation comes from publishing **zero** daemon ports: the
unauthenticated REST/WS API is reachable only from containers on this network
and from the Docker host through the bridge interface — never from the outside.
The only host-published port is Lavalink's `2333`. If your threat model also
requires host-side isolation, firewall the daemon ports or bind
`server.address` to the daemons' bridge IPs on a dedicated network.

## Quickstart

```bash
# 1. Configure
cp .env.example .env                # then set a real LAVALINK_PASSWORD

# 2. Build the plugin JAR (mounted into the Lavalink container)
./gradlew build

# 3. Start (FIFOs are created by fifo-init before anything else starts)
docker compose up -d

# 4. First-login for each daemon (interactive OAuth):
docker compose logs -f golibrespot-1   # prints the Spotify authorize link
```

Each daemon prints one authorize link, then **blocks waiting for the OAuth
callback**. The callback server binds `0.0.0.0:<callback_port>` (3679), but the
printed link points at `127.0.0.1:3679`, so for the **first login only**:

1. Uncomment the `# ports: ["3679:3679"]` mapping in `compose.yaml`
   (`golibrespot-1`; the same applies to `golibrespot-2`).
2. `docker compose up -d golibrespot-1` and open the link from the logs in a
   browser **on the Docker host** (the callback server serves only the OAuth
   redirect — never the daemon API — but remove the mapping afterwards anyway).
3. `docker compose up -d` again to drop the mapping.

The session is persisted in the `config-<n>` volume (`state.json` +
`credentials.json` in the config dir), so later restarts skip the login.

## What the stack does (and why)

- **Separate containers** — Lavalink and go-librespot are never bundled; the
  daemon is an external GPL-3.0 process (see THIRD_PARTY_NOTICES).
- **Shared FIFO volume** — named volume `fifos` mounted at `/spdirect` in
  every container; the FIFOs `/spdirect/spdirect-1.fifo` and
  `/spdirect/spdirect-2.fifo` exist at identical absolute paths in the daemon
  (`audio_output_pipe`) and the plugin (`plugins.golibrespot.backends[].fifoPath`).
- **Group-writable FIFOs, never world-writable** — `mkfifo` + `chgrp <PGID>` +
  `chmod 0660`. The daemon writes as gid `PGID`; the Lavalink plugin JVM reads
  as the image's `lavalink` user (gid **322** — the default PGID, verified on
  the `ghcr.io/lavalink-devs/lavalink:4.2.2` image). Change `PGID` only if you
  also change the Lavalink container user.
- **Unprivileged daemon** — the go-librespot image has no USER directive; it is
  run as `${GO_LIBRESPOT_PUID}:${GO_LIBRESPOT_PGID}` (default 1000:322). The
  config volumes are chowned to that pair (mode 0700) so `state.json`,
  `credentials.json` and the config-dir `lockfile` are writable. Lavalink runs
  as its image user `lavalink` (uid 322). If an image ever forces root,
  prefer user namespaces (`/etc/docker/daemon.json` → `userns-remap`).
- **Pinned images with digests** — see `compose.yaml`; digests were verified
  with `docker buildx imagetools inspect` on 2026-08-28 (index digests;
  resolved per host platform by `docker compose pull`). Never switch to
  `latest`/floating tags.
- **Daemon healthcheck** — the image ships busybox `wget` only (no `curl`), and
  busybox `wget` **times out** against the daemon API (keep-alive responses
  without content-length; verified 2026-08-28). The healthcheck therefore sends
  a raw `GET /` with `Connection: close` via `nc` (bounded by `timeout 5`;
  busybox `nc -w` is connect-only) and greps for ` 200 `. Important: while the
  interactive login is pending, the API server does **not** answer at all (the
  request lane is only drained after login — `daemon/app.go`), so a daemon
  container reports **unhealthy until the operator completes the OAuth flow**.
  That is expected, not a fault; restart the daemon after login is not needed —
  it becomes healthy on its own.

## Mandatory daemon settings (all set in `deploy/go-librespot/config.yml`)

| Setting | Value | Why |
| --- | --- | --- |
| `audio_backend` | `pipe` | pipe backend (Linux-only) |
| `audio_output_pipe` | `/spdirect/spdirect-<n>.fifo` | absolute path of the existing FIFO created by `fifo-init` |
| `audio_output_pipe_format` | `s16le` | interleaved LE signed 16-bit PCM |
| `audio_output_pipe_wait_for_reader` | `true` | daemon blocks its write-open until the plugin reader rendezvouses at play time |
| `disable_autoplay` | `true` | no autoplay radio after a context ends |
| `external_volume` | `true` | volume is applied in the Lavaplayer pipeline only (daemon would otherwise pre-multiply samples by volume²) |
| `crossfade_duration` | `0` | no crossfade |
| `server.enabled` / `server.address` / `server.port` | `true` / `0.0.0.0` / `3678` | API on; default port 0 is EPHEMERAL — must be pinned to the port `restBaseUrl` uses |
| `credentials.type` | `interactive` | v0.9.0 enum `interactive \| spotify_token \| zeroconf`; `device_auth` is **master-only**, invalid in v0.9.0 |

The v0.9.0 `config_schema.json` sets `additionalProperties: false` — a typo in
any key makes the daemon refuse the config.

## Plugin config (`deploy/lavalink/application.yml`)

`plugins.golibrespot.backends[]` binds strictly (fail-on-unknown keys):
`name`, `restBaseUrl`, optional `wsUrl` (defaults derived: http→ws + `/events`),
`fifoPath` (absolute). The two entries point at the two daemons and the two
FIFOs. `wsUrl` is derived from `restBaseUrl`, so it is not set here.

## Files

```
compose.yaml                          services, networks, volumes, digests
.env.example                          operator variables (NO secrets)
deploy/go-librespot/config.yml        daemon config, backend 1 (spdirect-1.fifo)
deploy/go-librespot/config.2.yml      daemon config, backend 2 (spdirect-2.fifo)
deploy/go-librespot/init-fifos.sh     one-shot: mkfifo + chown + config seed
deploy/lavalink/application.yml       Lavalink + plugin config
```

## Operations notes

- **Restart**: `docker compose up -d` — `fifo-init` re-runs idempotently
  (`restart: "no"`); FIFOs and config volumes are left intact.
- **Reset everything** (also destroys saved sessions): `docker compose down -v`.
- **Editing the daemon config**: the volumes are seeded once from the
  templates in `deploy/go-librespot/`; edit the volume copy afterwards, e.g.
  `docker compose cp ./my-config.yml golibrespot-1:/config/config.yml` and
  recreate the service. Editing the templates alone does not update existing
  volumes.
- **Bot client** — point it at `http://<host>:${LAVALINK_PORT}` (default 2333).
  Lavalink 4.2.2's auth filter compares the `Authorization` header **verbatim**
  against the configured password — send `Authorization: <password>` with no
  `Bearer ` prefix (verified against the 4.2.2 source and a live container:
  raw password → 200, `Bearer <password>` → 403). Load
  `spdirect:<spotify-track-id>` tracks.
