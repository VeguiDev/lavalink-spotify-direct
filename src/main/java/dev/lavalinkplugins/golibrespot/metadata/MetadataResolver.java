package dev.lavalinkplugins.golibrespot.metadata;

import com.sedmelluq.discord.lavaplayer.track.AudioTrackInfo;
import dev.lavalinkplugins.golibrespot.logging.LogSanitizer;
import dev.lavalinkplugins.golibrespot.source.AudioTrackInfoMapper;
import dev.lavalinkplugins.golibrespot.source.TrackMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Best-effort, lease-free track metadata resolver.
 *
 * <p>Resolves a Spotify track id to a Lavaplayer {@link AudioTrackInfo} via the
 * daemon's {@code GET /web-api/v1/tracks/{id}} passthrough (API_CONTRACT.md
 * §2.9), mapped through {@link TrackMetadata} + {@link AudioTrackInfoMapper}.
 * The backend is picked from the injectable {@link ReadyBackendSelector} —
 * the selector returns READY backends WITHOUT acquiring an exclusive lease
 * (DECISIONS.md §5: lease at play, never at load).</p>
 *
 * <p>Failure contract: the resolver NEVER fabricates metadata. Every failure
 * mode — non-200 status (incl. the 400/403/404/405/429 the daemon propagates),
 * 204 no-session, an empty 200 (upstream 204/5xx arrives as 200 empty), a
 * malformed body, a missing or non-positive {@code duration_ms}, a dead
 * daemon, or the metadata timeout — yields {@link Optional#empty()}. The
 * caller (source manager) decides the load error from the empty result.</p>
 *
 * <p>Multi-backend: {@link #resolve(String)} walks {@link ReadyBackendSelector#next()}
 * until one backend succeeds or the selector is exhausted; a failing backend
 * is simply skipped. All log output passes through {@link LogSanitizer} so
 * tokens/secrets never reach the log. The fetch is bounded by the metadata
 * timeout (DECISIONS.md: {@code metadataTimeoutMs} = 5 s default).</p>
 */
public final class MetadataResolver {

    /** Daemon passthrough path prefix (API_CONTRACT.md §2.9). */
    private static final String TRACKS_PATH_PREFIX = "/web-api/v1/tracks/";

    /** DECISIONS.md {@code metadataTimeoutMs} — never block a track load past this. */
    private static final long DEFAULT_METADATA_TIMEOUT_MS = 5_000L;

    /** A ready daemon backend, addressable by its REST base URL. */
    public record ReadyBackend(String restBaseUrl) {

        /** @throws NullPointerException when {@code restBaseUrl} is {@code null} */
        public ReadyBackend {
            Objects.requireNonNull(restBaseUrl, "restBaseUrl");
        }

        /** Base URL with any trailing slashes removed, for safe path joining. */
        public String apiBase() {
            int end = restBaseUrl.length();
            while (end > 0 && restBaseUrl.charAt(end - 1) == '/') {
                end--;
            }
            return restBaseUrl.substring(0, end);
        }
    }

    /**
     * Supplies READY backends for the resolver to try, one at a time.
     *
     * <p>Contract: each call returns the next ready backend or
     * {@link Optional#empty()} once exhausted. Implementations MUST NOT
     * lease/reserve the backend — the metadata fetch is best-effort and
     * lease-free. The pool (T13) supplies this selector.
     */
    @FunctionalInterface
    public interface ReadyBackendSelector {

        /** @return the next ready backend to try, or empty when exhausted */
        Optional<ReadyBackend> next();
    }

    private final ReadyBackendSelector selector;
    private final long timeoutMs;
    private final Logger log;
    private final LogSanitizer sanitizer;
    private final HttpClient httpClient;
    private final AudioTrackInfoMapper mapper = new AudioTrackInfoMapper();

    /** Creates a resolver with the DECISIONS.md default metadata timeout (5 s). */
    public MetadataResolver(ReadyBackendSelector selector) {
        this(selector, DEFAULT_METADATA_TIMEOUT_MS);
    }

    /**
     * Creates a resolver with a custom metadata fetch timeout.
     *
     * @param selector source of READY backends (never leased)
     * @param timeoutMs per-request budget for each backend's metadata fetch
     */
    public MetadataResolver(ReadyBackendSelector selector, long timeoutMs) {
        this.selector = Objects.requireNonNull(selector, "selector");
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("timeoutMs must be positive: " + timeoutMs);
        }
        this.timeoutMs = timeoutMs;
        this.log = LoggerFactory.getLogger(MetadataResolver.class);
        this.sanitizer = LogSanitizer.defaults();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(timeoutMs))
                .build();
    }

    /**
     * Best-effort {@code GET /web-api/v1/tracks/{id}} metadata resolution.
     *
     * <p>Walks the selector's READY backends in order, returning the first
     * successful {@link AudioTrackInfo} or {@link Optional#empty()} when no
     * backend resolves the track. Never fabricates metadata and never acquires
     * a lease. {@code trackId} must be a path-safe Spotify id (callers use
     * {@code TrackIdParser}); a blank id is rejected without any HTTP call.
     *
     * @param trackId the Spotify track id (22-char base62)
     * @return the resolved track info, or empty on any failure
     */
    public Optional<AudioTrackInfo> resolve(String trackId) {
        if (trackId == null || trackId.isBlank()) {
            log.warn("Metadata resolve skipped: blank track id");
            return Optional.empty();
        }
        Optional<ReadyBackend> next;
        boolean anyBackend = false;
        while ((next = selector.next()).isPresent()) {
            ReadyBackend backend = next.get();
            anyBackend = true;
            String url = backend.apiBase() + TRACKS_PATH_PREFIX + trackId;
            Optional<AudioTrackInfo> resolved = tryResolve(url, trackId);
            if (resolved.isPresent()) {
                log.info("Resolved metadata for track {} via {}", trackId, sanitizer.sanitizeUrl(url));
                return resolved;
            }
        }
        if (!anyBackend) {
            log.warn("Metadata resolve for track {} skipped: no ready backend", trackId);
        }
        return Optional.empty();
    }

    /**
     * Fetches and maps one backend's answer. Logs (sanitized) and returns empty
     * on every failure; only an HTTP 200 carrying a parseable track body with a
     * positive {@code duration_ms} can produce a result.
     */
    private Optional<AudioTrackInfo> tryResolve(String url, String trackId) {
        String safeUrl = sanitizer.sanitizeUrl(url);
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofMillis(timeoutMs))
                    .GET()
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            int status = response.statusCode();
            if (status == HttpURLConnection.HTTP_NO_CONTENT) {
                log.warn("Metadata {} returned 204 (no session)", safeUrl);
                return Optional.empty();
            }
            if (status != HttpURLConnection.HTTP_OK) {
                // only 400/403/404/405/429 are propagated by the daemon; any other
                // non-200 (incl. 204) is a failure for metadata purposes.
                log.warn("Metadata {} returned HTTP {}", safeUrl, status);
                return Optional.empty();
            }
            String body = response.body();
            if (body == null || body.isBlank()) {
                // an upstream 204/5xx arrives as an empty 200 (API_CONTRACT §2.9.5)
                log.warn("Metadata {} returned empty 200 body", safeUrl);
                return Optional.empty();
            }
            Json parsed = Json.parse(body);
            if (!(parsed instanceof JsonObject root)) {
                log.warn("Metadata {} body is not a JSON object", safeUrl);
                return Optional.empty();
            }
            return toTrackMetadata(trackId, root).flatMap(mapper::map);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Metadata {} fetch interrupted", safeUrl);
            return Optional.empty();
        } catch (IOException e) {
            log.warn("Metadata {} fetch failed: {}", safeUrl,
                    sanitizer.sanitize(String.valueOf(e.getMessage())));
            return Optional.empty();
        } catch (RuntimeException e) {
            log.warn("Metadata {} malformed payload: {}", safeUrl,
                    sanitizer.sanitize(String.valueOf(e.getMessage())));
            return Optional.empty();
        }
    }

    /**
     * Extracts the Spotify Web API track fields into {@link TrackMetadata},
     * tolerating absent optional fields. A missing title, or a missing /
     * non-positive {@code duration_ms}, is a hard failure — duration is never
     * invented.
     */
    private Optional<TrackMetadata> toTrackMetadata(String trackId, JsonObject root) {
        String name = root.string("name");
        Long durationMs = root.longValue("duration_ms");
        if (name == null || durationMs == null || durationMs <= 0) {
            log.warn("Metadata for track {} lacks name or a valid duration_ms; not fabricating", trackId);
            return Optional.empty();
        }
        return Optional.of(new TrackMetadata(
                trackId,
                name,
                root.artists(),
                root.albumName(),
                durationMs,
                root.albumArtwork(),
                root.isrc()));
    }

    // ------------------------------------------------------------------
    // Minimal tolerant JSON model + parser (no JSON library on the
    // classpath). Unknown fields are ignored; the daemon re-encodes the
    // upstream payload so key order/number representation must not matter.
    // ------------------------------------------------------------------

    /** Base of the tiny JSON value model. */
    private abstract static class Json {

        static Json parse(String text) {
            return new Parser(text).parse();
        }
    }

    /** JSON {@code null}. */
    private static final class JsonNull extends Json {
    }

    /** JSON boolean. */
    private static final class JsonBool extends Json {
        final boolean value;

        JsonBool(boolean value) {
            this.value = value;
        }
    }

    /** JSON number — the only numeric field consumed is {@code duration_ms} (a long). */
    private static final class JsonNumber extends Json {
        final long value;

        JsonNumber(long value) {
            this.value = value;
        }
    }

    /** JSON string (unescaped). */
    private static final class JsonString extends Json {
        final String value;

        JsonString(String value) {
            this.value = value;
        }
    }

    /** JSON array. */
    private static final class JsonArray extends Json {
        final List<Json> value = new ArrayList<>();
    }

    /** JSON object with tolerant field accessors for the Web API track shape. */
    private static final class JsonObject extends Json {
        final Map<String, Json> fields = new HashMap<>();

        /** String field value, or {@code null} when absent or not a string. */
        String string(String key) {
            Json v = fields.get(key);
            return v instanceof JsonString s ? s.value : null;
        }

        /** Long field value, or {@code null} when absent or not a number. */
        Long longValue(String key) {
            Json v = fields.get(key);
            return v instanceof JsonNumber n ? n.value : null;
        }

        /** {@code artists[].name}, in order (empty when absent/malformed). */
        List<String> artists() {
            Json v = fields.get("artists");
            if (!(v instanceof JsonArray arr)) {
                return List.of();
            }
            List<String> names = new ArrayList<>();
            for (Json item : arr.value) {
                if (item instanceof JsonObject o) {
                    String name = o.string("name");
                    if (name != null) {
                        names.add(name);
                    }
                }
            }
            return names;
        }

        /** {@code album.name}, or {@code ""} when absent (non-null contract). */
        String albumName() {
            Json v = fields.get("album");
            if (!(v instanceof JsonObject album)) {
                return "";
            }
            String name = album.string("name");
            return name == null ? "" : name;
        }

        /** {@code album.images[0].url}, or {@code null} when absent. */
        String albumArtwork() {
            Json v = fields.get("album");
            if (!(v instanceof JsonObject album)) {
                return null;
            }
            Json images = album.fields.get("images");
            if (!(images instanceof JsonArray arr) || arr.value.isEmpty()) {
                return null;
            }
            Json first = arr.value.get(0);
            return first instanceof JsonObject o ? o.string("url") : null;
        }

        /** {@code external_ids.isrc}, or {@code null} when absent. */
        String isrc() {
            Json v = fields.get("external_ids");
            return v instanceof JsonObject o ? o.string("isrc") : null;
        }
    }

    /** Recursive-descent parser over the JSON value model. */
    private static final class Parser {

        private static final String LITERAL_TRUE = "true";
        private static final String LITERAL_FALSE = "false";
        private static final String LITERAL_NULL = "null";

        private final String text;
        private int pos;

        Parser(String text) {
            this.text = text;
        }

        Json parse() {
            Json value = parseValue();
            skipWhitespace();
            if (pos != text.length()) {
                throw new JsonParseException("trailing content at index " + pos);
            }
            return value;
        }

        private Json parseValue() {
            skipWhitespace();
            if (pos >= text.length()) {
                throw new JsonParseException("unexpected end of input");
            }
            char c = text.charAt(pos);
            switch (c) {
                case '{' -> {
                    return parseObject();
                }
                case '[' -> {
                    return parseArray();
                }
                case '"' -> {
                    return parseString();
                }
                case 't' -> {
                    if (expectLiteral(LITERAL_TRUE)) {
                        return new JsonBool(true);
                    }
                    throw new JsonParseException("invalid literal at " + pos);
                }
                case 'f' -> {
                    if (expectLiteral(LITERAL_FALSE)) {
                        return new JsonBool(false);
                    }
                    throw new JsonParseException("invalid literal at " + pos);
                }
                case 'n' -> {
                    if (expectLiteral(LITERAL_NULL)) {
                        return new JsonNull();
                    }
                    throw new JsonParseException("invalid literal at " + pos);
                }
                default -> {
                    if (c == '-' || Character.isDigit(c)) {
                        return parseNumber();
                    }
                    throw new JsonParseException("unexpected character '" + c + "' at " + pos);
                }
            }
        }

        private JsonObject parseObject() {
            expect('{');
            JsonObject obj = new JsonObject();
            skipWhitespace();
            if (peek() == '}') {
                pos++;
                return obj;
            }
            while (true) {
                skipWhitespace();
                String key = parseString().value;
                skipWhitespace();
                expect(':');
                obj.fields.put(key, parseValue());
                skipWhitespace();
                char c = next();
                if (c == '}') {
                    return obj;
                }
                if (c != ',') {
                    throw new JsonParseException("expected ',' or '}' at " + (pos - 1));
                }
            }
        }

        private JsonArray parseArray() {
            expect('[');
            JsonArray arr = new JsonArray();
            skipWhitespace();
            if (peek() == ']') {
                pos++;
                return arr;
            }
            while (true) {
                arr.value.add(parseValue());
                skipWhitespace();
                char c = next();
                if (c == ']') {
                    return arr;
                }
                if (c != ',') {
                    throw new JsonParseException("expected ',' or ']' at " + (pos - 1));
                }
            }
        }

        private JsonString parseString() {
            expect('"');
            StringBuilder sb = new StringBuilder();
            while (true) {
                if (pos >= text.length()) {
                    throw new JsonParseException("unterminated string");
                }
                char c = text.charAt(pos++);
                if (c == '"') {
                    return new JsonString(sb.toString());
                }
                if (c == '\\') {
                    if (pos >= text.length()) {
                        throw new JsonParseException("unterminated escape");
                    }
                    char e = text.charAt(pos++);
                    switch (e) {
                        case '"' -> sb.append('"');
                        case '\\' -> sb.append('\\');
                        case '/' -> sb.append('/');
                        case 'b' -> sb.append('\b');
                        case 'f' -> sb.append('\f');
                        case 'n' -> sb.append('\n');
                        case 'r' -> sb.append('\r');
                        case 't' -> sb.append('\t');
                        case 'u' -> {
                            if (pos + 4 > text.length()) {
                                throw new JsonParseException("truncated \\u escape");
                            }
                            try {
                                sb.append((char) Integer.parseInt(text.substring(pos, pos + 4), 16));
                            } catch (NumberFormatException nfe) {
                                throw new JsonParseException("invalid \\u escape");
                            }
                            pos += 4;
                        }
                        default -> throw new JsonParseException("invalid escape '\\" + e + "'");
                    }
                } else {
                    sb.append(c);
                }
            }
        }

        private JsonNumber parseNumber() {
            int start = pos;
            if (peek() == '-') {
                pos++;
            }
            boolean hasDigits = false;
            while (pos < text.length() && Character.isDigit(text.charAt(pos))) {
                pos++;
                hasDigits = true;
            }
            if (!hasDigits) {
                throw new JsonParseException("invalid number at " + start);
            }
            final long value;
            try {
                value = Long.parseLong(text.substring(start, pos));
            } catch (NumberFormatException e) {
                throw new JsonParseException("number overflow at " + start);
            }
            skipFractionOrExponent();
            return new JsonNumber(value);
        }

        /** Tolerantly skips a trailing {@code .5} / {@code e3} so only the integer part is used. */
        private void skipFractionOrExponent() {
            if (pos < text.length() && text.charAt(pos) == '.') {
                pos++;
                while (pos < text.length() && Character.isDigit(text.charAt(pos))) {
                    pos++;
                }
            }
            if (pos < text.length() && (text.charAt(pos) == 'e' || text.charAt(pos) == 'E')) {
                pos++;
                if (pos < text.length() && (text.charAt(pos) == '+' || text.charAt(pos) == '-')) {
                    pos++;
                }
                while (pos < text.length() && Character.isDigit(text.charAt(pos))) {
                    pos++;
                }
            }
        }

        private boolean expectLiteral(String literal) {
            if (text.startsWith(literal, pos)) {
                pos += literal.length();
                return true;
            }
            return false;
        }

        private void expect(char expected) {
            if (pos >= text.length() || text.charAt(pos) != expected) {
                throw new JsonParseException("expected '" + expected + "' at " + pos);
            }
            pos++;
        }

        private char next() {
            if (pos >= text.length()) {
                throw new JsonParseException("unexpected end of input");
            }
            return text.charAt(pos++);
        }

        private char peek() {
            if (pos >= text.length()) {
                throw new JsonParseException("unexpected end of input");
            }
            return text.charAt(pos);
        }

        private void skipWhitespace() {
            while (pos < text.length()) {
                char c = text.charAt(pos);
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                    pos++;
                } else {
                    break;
                }
            }
        }
    }

    /** Thrown for any malformed JSON; surfaces as a typed (empty) metadata failure. */
    private static final class JsonParseException extends RuntimeException {
        JsonParseException(String message) {
            super(message);
        }
    }
}
