# Contributing

Thanks for considering a contribution to lavalink-go-librespot. This project
follows a few hard rules that keep it safe to run next to an unauthenticated
daemon and inside a memory-constrained Lavalink server.

## Before you start

Open an issue or discussion first for anything larger than a small fix.
The project has a documented architecture
([`docs/architecture.md`](docs/architecture.md)) and a pinned upstream
contract ([`docs/API_CONTRACT.md`](docs/API_CONTRACT.md)); a change that
contradicts either needs a written rationale.

## Building and testing

Requires JDK 17+ to compile (the wrapper needs a newer JDK; see the Gradle
wrapper properties) and Linux for the FIFO-backed tests.

```
./gradlew clean build
```

Run a single test class:

```
./gradlew test --tests '*GoLibrespotAudioTrackTest*'
```

The test suite includes real `mkfifo` FIFO tests that are skipped on Windows
and real on Linux. Keep them.

## Hard rules

These are enforced by design and by CI-friendly gates. A change that breaks
one will be sent back.

1. **No daemon stop endpoint.** The v0.9.0 stop-race (see
   `docs/API_CONTRACT.md` section 5) makes the daemon's stop endpoint unsafe.
   It must never appear in `src/main`, not even in a comment. Logical stop is
   a remote pause with confirmation and generation retirement; replacement is
   play-over-play. Gate: the string for that endpoint's path must not appear
   anywhere under `src/main`.
2. **No arbitrary sleeps in main code.** Handshakes use bounded waits and
   timeouts, never `Thread.sleep`. Gate:
   `rg "Thread.sleep" src/main` must return nothing.
3. **Claim only the `spdirect:` namespace.** The source manager claims exactly
   `spdirect:<22-char-base62>` and `spdirect:spotify:track:<id>`. Ordinary
   Spotify URIs/URLs are never claimed. The source name is `spotify`.
4. **No fake metadata.** A load without real metadata fails clearly. Never
   fabricate a duration, title, or artist.
5. **Zero runtime dependencies.** The shipped JAR uses only the JDK
   (`java.net.http` for REST and WebSocket). New main-code dependencies need
   approval and a justification.
6. **Lease at play, never at load.** `loadItem` must not acquire a backend
   lease; metadata resolution is lease-free.
7. **Redact logs.** Every log line goes through `LogSanitizer`. Bearer
   tokens, credentials, and query secrets must never reach a log.
8. **No stop-endpoint recovery shortcuts.** A quarantined backend is
   recovered by external orchestration (a daemon restart), never by unsafe
   reuse inside the plugin process.
9. **Tests first.** New behavior ships with tests that fail against the old
   code. Concurrency and lifecycle changes need tests that exercise the fake
   daemon fixture and, where relevant, the real FIFO path.

## Code style

- 2-space indentation in the `lifecycle` and `source` packages, 4-space
  elsewhere (match the file you are editing).
- Java 17, `--release 17`. Prefer records, sealed interfaces, and immutable
  config. No external JSON library: the project ships small tolerant parsers
  on purpose.
- Public API is documented in Javadoc with the contracts it must honor.
- Keep files small and focused. If a class grows past a few hundred lines,
  split it.

## Committing

Write conventional commit messages (`feat:`, `fix:`, `refactor:`, `docs:`,
`test:`, `build:`, `ci:`). Keep commits atomic: one logical change per
commit, implementation with its tests. Never commit secrets, `.env` files,
`application.yml`, or local configuration (`.gitignore` already excludes
them).

## Pull requests

See the [pull request template](.github/pull_request_template.md). Every PR
should state what changed, what was tested, and confirm the hard rules above
still hold.
