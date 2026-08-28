package dev.lavalinkplugins.golibrespot.backend.rest;

import dev.lavalinkplugins.golibrespot.backend.model.PlayerCommandResult;
import dev.lavalinkplugins.golibrespot.backend.model.RootResult;
import dev.lavalinkplugins.golibrespot.backend.model.StatusResult;
import dev.lavalinkplugins.golibrespot.backend.model.WebApiResult;
import dev.lavalinkplugins.golibrespot.config.BackendConfig;
import dev.lavalinkplugins.golibrespot.config.GoLibrespotConfig;
import dev.lavalinkplugins.golibrespot.logging.LogSanitizer;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Time-bounded typed REST client for the go-librespot v0.9.0 HTTP API
 * (docs/API_CONTRACT.md §2) over {@link java.net.http.HttpClient}.
 *
 * <p>Design contract:</p>
 * <ul>
 *   <li><b>Time-bounded control plane.</b> Connect + request timeouts both come
 *       from the effective REST timeout (DECISIONS.md — 5 s default, per-backend
 *       override via {@link GoLibrespotConfig#effectiveRestTimeoutMs(BackendConfig)}).
 *       A hung daemon surfaces as {@link RestException} with
 *       {@link RestException.Kind#TIMEOUT}, never an unbounded wait.</li>
 *   <li><b>Never infer success from HTTP 200.</b> v0.9.0 handlers swallow
 *       internal errors ({@code _ = p.play(ctx)}); every call returns the raw
 *       status + body (typed DTOs in {@code backend.model}) and callers
 *       reconcile with {@code /status} and WS events. {@code 204} is the
 *       no-session sentinel.</li>
 *   <li><b>The stop endpoint is never used.</b> This client implements only the
 *       endpoints the plugin may use (API_CONTRACT.md §5 — the v0.9.0 stop-race:
 *       stopping is a logical remote pause + confirmation, never a stop call).</li>
 *   <li><b>Cancellation.</b> Every call has an async variant; the returned
 *       future can be canceled (aborting the underlying HTTP exchange), and
 *       {@link #close()} cancels all in-flight requests and shuts the client
 *       down. Idempotent.</li>
 *   <li><b>Redacted logging.</b> All diagnostic lines pass through the
 *       {@link LogSanitizer} (URLs via {@code sanitizeUrl}, bodies via
 *       {@code sanitize}) before reaching the {@link Consumer} log sink
 *       (default no-op) — tokens are never logged.</li>
 *   <li><b>Zero runtime dependencies.</b> {@code java.net.http} + hand-built
 *       JSON request bodies (the responses are parsed by the tiny
 *       {@code backend.model} parser).</li>
 * </ul>
 */
public final class GoLibrespotRestClient implements AutoCloseable {

    private static final String ROOT = "/";
    private static final String STATUS = "/status";
    private static final String PLAY = "/player/play";
    private static final String PAUSE = "/player/pause";
    private static final String RESUME = "/player/resume";
    private static final String SEEK = "/player/seek";
    private static final String WEB_API = "/web-api/";

    private final String baseUrl;
    private final int requestTimeoutMs;
    private final LogSanitizer sanitizer;
    private final Consumer<String> logSink;
    private final HttpClient httpClient;
    private final ExecutorService executor;
    private final Set<CompletableFuture<?>> inFlight = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /** @param timeoutMs connect + request timeout in ms (effective REST timeout, default 5000) */
    public GoLibrespotRestClient(String baseUrl, int timeoutMs) {
        this(baseUrl, timeoutMs, LogSanitizer.defaults(), s -> {});
    }

    public GoLibrespotRestClient(String baseUrl, int timeoutMs, LogSanitizer sanitizer) {
        this(baseUrl, timeoutMs, sanitizer, s -> {});
    }

    /**
     * @param logSink receives sanitized diagnostic lines; every line has passed
     *                through the {@link LogSanitizer} before being accepted
     */
    public GoLibrespotRestClient(String baseUrl, int timeoutMs, LogSanitizer sanitizer, Consumer<String> logSink) {
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("timeoutMs must be positive, got " + timeoutMs);
        }
        this.baseUrl = trimTrailingSlash(Objects.requireNonNull(baseUrl, "baseUrl"));
        this.requestTimeoutMs = timeoutMs;
        this.sanitizer = Objects.requireNonNull(sanitizer, "sanitizer");
        this.logSink = Objects.requireNonNull(logSink, "logSink");
        this.executor = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "golibrespot-rest");
            thread.setDaemon(true);
            return thread;
        });
        this.httpClient = HttpClient.newBuilder()
                .executor(executor)
                .connectTimeout(Duration.ofMillis(timeoutMs))
                .build();
    }

    /** Builds a client for a configured backend; connect+request timeout = {@code effectiveRestTimeoutMs}. */
    public static GoLibrespotRestClient fromConfig(GoLibrespotConfig config, BackendConfig backend) {
        return new GoLibrespotRestClient(
                backend.getRestBaseUrl(),
                config.effectiveRestTimeoutMs(backend),
                LogSanitizer.defaults(),
                s -> {});
    }

    // ------------------------------------------------------------ player commands

    /**
     * {@code POST /player/play} with body {@code {"uri", "paused", "position"}}.
     * 200 means accepted-for-processing, NOT success — reconcile with
     * {@code /status} / WS events.
     */
    public PlayerCommandResult play(String uri, long positionMs, boolean paused) {
        return sync(playAsync(uri, positionMs, paused));
    }

    public CompletableFuture<PlayerCommandResult> playAsync(String uri, long positionMs, boolean paused) {
        Objects.requireNonNull(uri, "uri");
        String body = "{\"uri\":" + jsonString(uri)
                + ",\"paused\":" + paused
                + ",\"position\":" + positionMs + "}";
        return send("POST", PLAY, body)
                .thenApply(r -> PlayerCommandResult.of(r.statusCode(), r.body()));
    }

    /** {@code POST /player/pause} — no body. */
    public PlayerCommandResult pause() {
        return sync(pauseAsync());
    }

    public CompletableFuture<PlayerCommandResult> pauseAsync() {
        return send("POST", PAUSE, null)
                .thenApply(r -> PlayerCommandResult.of(r.statusCode(), r.body()));
    }

    /** {@code POST /player/resume} — no body. */
    public PlayerCommandResult resume() {
        return sync(resumeAsync());
    }

    public CompletableFuture<PlayerCommandResult> resumeAsync() {
        return send("POST", RESUME, null)
                .thenApply(r -> PlayerCommandResult.of(r.statusCode(), r.body()));
    }

    /**
     * {@code POST /player/seek} with body {@code {"position", "relative": false}}
     * — absolute seek, clamped to {@code [0, duration]} daemon-side.
     */
    public PlayerCommandResult seek(long positionMs) {
        return sync(seekAsync(positionMs));
    }

    public CompletableFuture<PlayerCommandResult> seekAsync(long positionMs) {
        String body = "{\"position\":" + positionMs + ",\"relative\":false}";
        return send("POST", SEEK, body)
                .thenApply(r -> PlayerCommandResult.of(r.statusCode(), r.body()));
    }

    // ------------------------------------------------------------ status / readiness

    /** {@code GET /status} — full player status; 204 = no session ({@link StatusResult#isNoSession()}). */
    public StatusResult status() {
        return sync(statusAsync());
    }

    public CompletableFuture<StatusResult> statusAsync() {
        return send("GET", STATUS, null)
                .thenApply(r -> {
                    StatusResult result = StatusResult.of(r.statusCode(), r.body());
                    if (r.statusCode() == 200 && !r.body().isBlank() && result.parsed().isEmpty()) {
                        log("REST GET " + sanitizer.sanitizeUrl(baseUrl + STATUS) + " -> 200 with unparseable body");
                    }
                    return result;
                });
    }

    /** {@code GET /} — liveness + readiness probe ({@code playback_ready}). */
    public RootResult playbackReady() {
        return sync(playbackReadyAsync());
    }

    public CompletableFuture<RootResult> playbackReadyAsync() {
        return send("GET", ROOT, null)
                .thenApply(r -> RootResult.of(r.statusCode(), r.body()));
    }

    // ------------------------------------------------------------ /web-api passthrough

    /**
     * {@code GET /web-api/{path}} — raw Spotify payload or empty, validated by
     * content never by status alone (API_CONTRACT.md §2.9: non-listed upstream
     * statuses arrive as HTTP 200; no session → 204).
     *
     * @param path everything after {@code /web-api/} (e.g. {@code v1/tracks/<id>});
     *             may include a query string
     */
    public WebApiResult metadataViaWebApi(String path) {
        return sync(metadataViaWebApiAsync(path));
    }

    public CompletableFuture<WebApiResult> metadataViaWebApiAsync(String path) {
        Objects.requireNonNull(path, "path");
        String trimmed = path.startsWith("/") ? path.substring(1) : path;
        if (trimmed.isBlank()) {
            throw new IllegalArgumentException("web-api path must not be blank");
        }
        return send("GET", URI.create(baseUrl + WEB_API + trimmed), null)
                .thenApply(r -> WebApiResult.of(r.statusCode(), r.body()));
    }

    // ------------------------------------------------------------ lifecycle

    /** True once {@link #close()} has been called. */
    public boolean isClosed() {
        return closed.get();
    }

    /**
     * Cancels every in-flight request and shuts the client down. Idempotent;
     * subsequent calls throw {@link IllegalStateException}.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (CompletableFuture<?> future : inFlight) {
            future.cancel(true);
        }
        inFlight.clear();
        executor.shutdownNow();
    }

    // ------------------------------------------------------------ plumbing

    private CompletableFuture<HttpResponse<String>> send(String method, String path, String body) {
        return send(method, URI.create(baseUrl + path), body);
    }

    private CompletableFuture<HttpResponse<String>> send(String method, URI uri, String body) {
        if (closed.get()) {
            throw new IllegalStateException("GoLibrespotRestClient is closed");
        }
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofMillis(requestTimeoutMs));
        if (body == null) {
            builder.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            builder.method(method, HttpRequest.BodyPublishers.ofString(body));
            builder.header("Content-Type", "application/json");
        }
        HttpRequest request = builder.build();
        log("REST " + method + " " + sanitizer.sanitizeUrl(uri.toString()));

        CompletableFuture<HttpResponse<String>> raw = httpClient.sendAsync(
                request, HttpResponse.BodyHandlers.ofString());
        inFlight.add(raw);
        raw.whenComplete((r, e) -> inFlight.remove(raw));
        return raw.handle((response, error) -> {
            if (error != null) {
                throw mapError(method, uri, error);
            }
            log("REST " + method + " " + sanitizer.sanitizeUrl(uri.toString())
                    + " -> " + response.statusCode());
            return response;
        });
    }

    private RestException mapError(String method, URI uri, Throwable error) {
        String url = sanitizer.sanitizeUrl(uri.toString());
        Throwable cause = error instanceof CompletionException ce && ce.getCause() != null
                ? ce.getCause()
                : error;
        if (cause instanceof HttpTimeoutException) {
            return RestException.timeout(url, requestTimeoutMs, cause);
        }
        if (cause instanceof CancellationException) {
            return RestException.canceled(url);
        }
        if (cause instanceof ConnectException) {
            return RestException.io(url, cause);
        }
        if (cause instanceof IOException) {
            return RestException.io(url, cause);
        }
        if (cause instanceof InterruptedException) {
            return RestException.interrupted(url);
        }
        return RestException.io(url, cause);
    }

    private static <T> T sync(CompletableFuture<T> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RestException re) {
                throw re;
            }
            throw e;
        } catch (CancellationException e) {
            throw RestException.canceled("(canceled)");
        }
    }

    private void log(String line) {
        logSink.accept(sanitizer.sanitize(line));
    }

    private static String trimTrailingSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    private static String jsonString(String value) {
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }
}
