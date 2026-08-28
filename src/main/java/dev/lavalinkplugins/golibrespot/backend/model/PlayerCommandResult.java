package dev.lavalinkplugins.golibrespot.backend.model;

/**
 * Raw result of a player command ({@code POST /player/play|pause|resume|seek}).
 *
 * <p><b>Never infer success from HTTP 200 alone.</b> The v0.9.0 daemon swallows
 * internal player errors in most handlers ({@code _ = p.play(ctx)},
 * docs/API_CONTRACT.md §1.1) — a 200 only means "accepted for processing".
 * Callers reconcile with {@code /status} and WS events. This record carries the
 * raw status + body and exposes only status-code semantics.</p>
 */
public record PlayerCommandResult(int status, String body) {

    public PlayerCommandResult {
        body = body == null ? "" : body;
    }

    public static PlayerCommandResult of(int status, String body) {
        return new PlayerCommandResult(status, body);
    }

    /** {@code 200..299}. */
    public boolean is2xx() {
        return status >= 200 && status < 300;
    }

    /** {@code >= 400} — a typed failure surfaced by the daemon. */
    public boolean isError() {
        return status >= 400;
    }

    /** {@code 204} — no active session sentinel. */
    public boolean isNoSession() {
        return status == 204;
    }
}
