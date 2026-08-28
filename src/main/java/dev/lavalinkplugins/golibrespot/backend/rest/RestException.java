package dev.lavalinkplugins.golibrespot.backend.rest;

/**
 * Typed transport failure of a REST call — distinct from an HTTP error status.
 *
 * <p>HTTP 4xx/5xx responses are <b>not</b> exceptions: they are returned as raw
 * status + body results (callers reconcile; the v0.9.0 daemon swallows internal
 * errors, so 200 never proves success). {@link RestException} is reserved for
 * the request never producing a usable response: request/connect timeout, I/O
 * failure (e.g. connection refused), cancellation, or interruption.</p>
 */
public final class RestException extends RuntimeException {

    public enum Kind {
        /** {@link java.net.http.HttpTimeoutException} — request or connect timeout elapsed. */
        TIMEOUT,
        /** {@link java.io.IOException} — connection refused, reset, DNS, etc. */
        IO,
        /** The in-flight request was canceled (caller or {@code close()}). */
        CANCELED,
        /** The calling thread was interrupted. */
        INTERRUPTED
    }

    private final Kind kind;

    private RestException(Kind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
    }

    public Kind kind() {
        return kind;
    }

    public static RestException timeout(String url, long timeoutMs, Throwable cause) {
        return new RestException(
                Kind.TIMEOUT,
                "REST request to " + url + " timed out after " + timeoutMs + "ms",
                cause);
    }

    public static RestException io(String url, Throwable cause) {
        return new RestException(Kind.IO, "REST request to " + url + " failed: " + cause, cause);
    }

    public static RestException canceled(String url) {
        return new RestException(Kind.CANCELED, "REST request to " + url + " canceled", null);
    }

    public static RestException interrupted(String url) {
        return new RestException(Kind.INTERRUPTED, "REST request to " + url + " interrupted", null);
    }
}
