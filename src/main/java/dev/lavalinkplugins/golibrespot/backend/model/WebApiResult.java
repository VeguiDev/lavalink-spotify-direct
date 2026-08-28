package dev.lavalinkplugins.golibrespot.backend.model;

/**
 * Raw result of a {@code /web-api/{path}} passthrough call.
 *
 * <p>The passthrough is <b>not byte-for-byte transparent</b> (API_CONTRACT.md
 * §2.9): only upstream 400/403/404/405/429 map to the daemon's own status —
 * every other upstream status (incl. 204/500) arrives as HTTP 200, possibly
 * with an empty body. No active session → 204. Consumers must therefore treat
 * this as "raw Spotify payload or empty", validated by content, never by
 * status alone.</p>
 */
public record WebApiResult(int status, String body) {

    public WebApiResult {
        body = body == null ? "" : body;
    }

    public static WebApiResult of(int status, String body) {
        return new WebApiResult(status, body);
    }

    /** {@code 200..299} (meaningless for this endpoint — see class javadoc). */
    public boolean is2xx() {
        return status >= 200 && status < 300;
    }

    /** {@code >= 400} — a typed failure surfaced by the daemon. */
    public boolean isError() {
        return status >= 400;
    }

    /** {@code 204} — no active session. */
    public boolean isNoSession() {
        return status == 204;
    }

    public boolean isNotFound() {
        return status == 404;
    }

    public boolean isForbidden() {
        return status == 403;
    }

    public boolean isUnauthorized() {
        return status == 401;
    }

    public boolean isRateLimited() {
        return status == 429;
    }
}
