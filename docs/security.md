# Security

This document describes the security model of lavalink-go-librespot, the
assumptions it makes about the deployment, and how the project handles
secrets. It is not a security audit and not a guarantee.

## Threat model

The deployment has three actors:

- **Lavalink** (the plugin's host), which the operator already trusts.
- **The go-librespot daemon**, a separate process.
- **Everything else**, which must not reach the daemon.

The go-librespot v0.9.0 REST and WebSocket API has **no authentication**.
Any client that can reach the daemon's port can start playback, pause it,
seek, read `/status`, and use the `/web-api` passthrough with the daemon's
Spotify credentials. The plugin's own security posture is therefore
structural:

- The daemon API must live on an **internal-only network**. The compose
  layout in `deploy/` never publishes the daemon's port to the host or the
  internet.
- The plugin sends no credentials of its own. The daemon attaches its Spotify
  bearer token only to outbound `/web-api/` proxied requests; that token never
  travels toward Lavalink.

## Config and secret hygiene

- **Configuration binding is strict.** Unknown keys are rejected at startup.
  A typo cannot silently weaken a setting.
- **Credentials stay in the daemon.** The Spotify account credentials are
  configured inside the daemon's own configuration and data volume, never in
  Lavalink's `application.yml` and never in this plugin.
- **Logs are redacted.** Every log line the plugin writes passes through the
  log sanitizer, which redacts `Authorization` header values, query-string
  secrets (`token`, `refresh_token`, `access_token`, `code`), and credential
  form/JSON fields (`client_id`, `client_secret`, `username`, `password`).
- **Secrets are not committed.** The repository's `.gitignore` excludes
  `.env`, `.env.*`, `secrets*.yml`, `secrets*.yaml`, `application.yml`, and
  keystore files. Keep operator secrets in ignored local files or a secrets
  store.
- **FIFO permissions.** The shared FIFO volume should use a dedicated
  non-root UID/GID with group-write access shared by the Lavalink and daemon
  containers. Avoid world-writable pipes where a shared group works.

## The daemon is a separate process (GPL-3.0)

go-librespot is licensed GPL-3.0 and runs as an **external process**. This
project does not bundle, vendor, copy, or link go-librespot code, so GPL
obligations for go-librespot do not attach to this project's Apache-2.0 code
base through this plugin. Running a separate GPL program alongside an
Apache-2.0 program is the standard, license-compatible arrangement.

This is a summary for operators, not legal advice. If you redistribute or
modify either project, review the license texts yourself and consult counsel
where your situation is unusual. See
[`THIRD_PARTY_NOTICES`](../THIRD_PARTY_NOTICES) and
[`LICENSE`](../LICENSE).

## Network assumptions to verify

- The daemon's `server.address` / `server.port` must be an explicit internal
  bind. The go-librespot default port is `0`, which binds an ephemeral port
  the plugin could not reliably reach and a firewall could not reliably
  describe.
- TLS is optional in the daemon (`server.cert_file` / `server.key_file`). If
  you enable it, use `https://` / `wss://` URLs; the plugin derives the
  WebSocket scheme from the REST scheme.
- The daemon needs outbound access to Spotify's API and CDN to authenticate
  and stream. That is expected and does not require inbound exposure.

## Reporting a vulnerability

If you find a security issue, do not open a public issue with exploit
details. Report it privately through the repository's security advisories or
to the maintainers, and include the deployment shape, the config in use
(secrets redacted), and a minimal reproduction.
