---
name: Bug report
about: Report a reproducible problem with lavalink-go-librespot
title: "[bug] "
labels: bug
assignees: ""
---

## Description

What happened, and what did you expect to happen instead?

## Environment

- Plugin version (from the release JAR, e.g. `1.0.0`):
- Lavalink version:
- go-librespot version / image tag (must be v0.9.0):
- Host OS (Linux distribution and kernel):
- Deployment (Docker Compose, bare processes, systemd):

## Configuration

Paste your `plugins.golibrespot` block **with secrets and internal hostnames
redacted**:

```yaml
plugins:
  golibrespot:
    ...
```

Also confirm the daemon's mandatory options
([docs/config.md](../../docs/config.md)):
- `audio_backend: pipe`?
- `audio_output_pipe_wait_for_reader: true`?
- `disable_autoplay: true`?
- `external_volume: true`?
- explicit `server.port` (not 0)?

## Steps to reproduce

1.
2.
3.

## Expected behavior

## Actual behavior

## Logs

Redact tokens, credentials, and IPs before pasting. Include both Lavalink and
go-librespot logs, especially any lines containing `quarantin`, `degraded`,
`spdirect load failed`, or coordinator results.

```
```

## Additional context

Anything else relevant: FIFO path and permissions, network layout, how many
backends, whether the daemon was recently restarted.
