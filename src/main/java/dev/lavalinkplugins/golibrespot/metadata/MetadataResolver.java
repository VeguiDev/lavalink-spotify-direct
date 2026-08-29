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
import java.util.Base64;
import java.nio.charset.StandardCharsets;
import java.net.URLEncoder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

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
 * <p>Multi-backend: {@link #resolve(String)} walks the finite snapshot returned
 * by {@link ReadyBackendSelector#readyBackends()} until one backend succeeds; a failing backend
 * is simply skipped. All log output passes through {@link LogSanitizer} so
 * tokens/secrets never reach the log. The fetch is bounded by the metadata
 * timeout (DECISIONS.md: {@code metadataTimeoutMs} = 5 s default).</p>
 */
public final class MetadataResolver {

    public record CollectionMetadata(
            String name, String author, String artworkUrl, List<AudioTrackInfo> tracks) {}

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
     * Supplies a finite snapshot of READY backends for one resolve operation.
     *
     * <p>Implementations MUST NOT lease/reserve a backend. Each returned backend
     * is attempted at most once by a resolve call, even when metadata fails.
     */
    @FunctionalInterface
    public interface ReadyBackendSelector {

        /** @return an ordered, finite snapshot of ready backends */
        List<ReadyBackend> readyBackends();
    }

    private final ReadyBackendSelector selector;
    private final long timeoutMs;
    private final Logger log;
    private final LogSanitizer sanitizer;
    private final HttpClient httpClient;
    private final AudioTrackInfoMapper mapper = new AudioTrackInfoMapper();
    private final String spotifyClientId;
    private final String spotifyClientSecret;
    private final String spotifyMarket;
    private final String tokenUrl;
    private final String apiBaseUrl;
    private final Map<String, CachedTrack> trackCache = new ConcurrentHashMap<>();
    private final Map<String, CachedCollection> collectionCache = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<Optional<CollectionMetadata>>> collectionInFlight =
            new ConcurrentHashMap<>();
    private volatile AccessToken accessToken;
    private volatile long rateLimitedUntilMs;
    private static final long CACHE_TTL_MS = 6 * 60 * 60 * 1000L;
    private static final long COLLECTION_CACHE_TTL_MS = 10 * 60 * 1000L;
    private static final int MAX_COLLECTION_TRACKS = 500;
    private static final int MAX_COLLECTION_PAGES = 10;

    private record AccessToken(String value, long expiresAtMs) {}
    private record CachedTrack(AudioTrackInfo info, long expiresAtMs) {}
    private record CachedCollection(CollectionMetadata metadata, long expiresAtMs) {}

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
        this(selector, timeoutMs, "", "", "AR");
    }

    public MetadataResolver(ReadyBackendSelector selector, long timeoutMs,
                            String spotifyClientId, String spotifyClientSecret, String spotifyMarket) {
        this(selector, timeoutMs, spotifyClientId, spotifyClientSecret, spotifyMarket,
                HttpClient.newBuilder().connectTimeout(Duration.ofMillis(timeoutMs)).build(),
                "https://accounts.spotify.com/api/token",
                "https://api.spotify.com");
    }

    /**
     * Root constructor: adds the test seam over the 5-arg form.
     *
     * <p>Injects the {@link HttpClient} used for every direct Spotify Web API
     * call, the client-credentials token endpoint URL, and the Spotify Web API
     * base URL. Tests point these at a local fake; production callers get the
     * same defaults via the delegating ctors (token
     * {@code https://accounts.spotify.com/api/token}, API
     * {@code https://api.spotify.com}), so production behavior is identical.
     *
     * @param selector source of READY backends (never leased)
     * @param timeoutMs per-request budget for each backend's metadata fetch
     * @param spotifyClientId client-credentials id (blank disables the direct path)
     * @param spotifyClientSecret client-credentials secret (blank disables the direct path)
     * @param spotifyMarket ISO 3166-1 alpha-2 market, default {@code AR} when blank
     * @param httpClient HTTP client for direct Spotify Web API calls (never null)
     * @param tokenUrl client-credentials token endpoint
     * @param apiBaseUrl Spotify Web API base (path prefix {@code /v1} is appended)
     */
    public MetadataResolver(ReadyBackendSelector selector, long timeoutMs,
                            String spotifyClientId, String spotifyClientSecret, String spotifyMarket,
                            HttpClient httpClient, String tokenUrl, String apiBaseUrl) {
        this.selector = Objects.requireNonNull(selector, "selector");
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("timeoutMs must be positive: " + timeoutMs);
        }
        this.timeoutMs = timeoutMs;
        this.log = LoggerFactory.getLogger(MetadataResolver.class);
        this.sanitizer = LogSanitizer.defaults();
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
        this.spotifyClientId = spotifyClientId == null ? "" : spotifyClientId.trim();
        this.spotifyClientSecret = spotifyClientSecret == null ? "" : spotifyClientSecret.trim();
        this.spotifyMarket = spotifyMarket == null || spotifyMarket.isBlank() ? "AR" : spotifyMarket.trim();
        this.tokenUrl = tokenUrl;
        this.apiBaseUrl = apiBaseUrl;
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
        CachedTrack cached = trackCache.get(trackId);
        long now = System.currentTimeMillis();
        if (cached != null && cached.expiresAtMs() > now) {
            return Optional.of(cached.info());
        }
        Optional<AudioTrackInfo> direct = resolveViaSpotifyWebApi(trackId, false);
        if (direct.isPresent()) {
            trackCache.put(trackId, new CachedTrack(direct.get(), now + CACHE_TTL_MS));
            return direct;
        }
        List<ReadyBackend> backends = List.copyOf(selector.readyBackends());
        for (ReadyBackend backend : backends) {
            String url = backend.apiBase() + TRACKS_PATH_PREFIX + trackId;
            Optional<AudioTrackInfo> resolved = tryResolve(url, trackId);
            if (resolved.isPresent()) {
                trackCache.put(trackId, new CachedTrack(resolved.get(), now + CACHE_TTL_MS));
                log.info("Resolved metadata for track {} via {}", trackId, sanitizer.sanitizeUrl(url));
                return resolved;
            }
        }
        if (backends.isEmpty()) {
            log.warn("Metadata resolve for track {} skipped: no ready backend", trackId);
        }
        if (cached != null) {
            log.warn("Using stale cached Spotify metadata for track {}", trackId);
            return Optional.of(cached.info());
        }
        return Optional.empty();
    }

    private Optional<AudioTrackInfo> resolveViaSpotifyWebApi(String trackId, boolean retriedAfterUnauthorized) {
        if (spotifyClientId.isBlank() || spotifyClientSecret.isBlank()
                || System.currentTimeMillis() < rateLimitedUntilMs) {
            return Optional.empty();
        }
        Optional<String> token = accessToken();
        if (token.isEmpty()) return Optional.empty();
        String url = apiBaseUrl + "/v1/tracks/" + trackId + "?market="
                + URLEncoder.encode(spotifyMarket, StandardCharsets.UTF_8);
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofMillis(timeoutMs))
                    .header("Authorization", "Bearer " + token.get()).GET().build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == HttpURLConnection.HTTP_UNAUTHORIZED && !retriedAfterUnauthorized) {
                accessToken = null;
                return resolveViaSpotifyWebApi(trackId, true);
            }
            if (response.statusCode() == 429) {
                long retrySeconds = response.headers().firstValue("Retry-After")
                        .flatMap(MetadataResolver::parsePositiveLong).orElse(30L);
                rateLimitedUntilMs = System.currentTimeMillis() + Math.min(retrySeconds, 300L) * 1000L;
                log.warn("Spotify Web API rate limited metadata requests for {} seconds", retrySeconds);
                return Optional.empty();
            }
            if (response.statusCode() != HttpURLConnection.HTTP_OK || response.body().isBlank()) {
                log.warn("Spotify Web API track metadata returned HTTP {}", response.statusCode());
                return Optional.empty();
            }
            Json json = Json.parse(response.body());
            return json instanceof JsonObject root
                    ? toTrackMetadata(trackId, root).flatMap(mapper::map) : Optional.empty();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException | RuntimeException e) {
            log.warn("Spotify Web API track metadata failed: {}",
                    sanitizer.sanitize(String.valueOf(e.getMessage())));
        }
        return Optional.empty();
    }

    private synchronized Optional<String> accessToken() {
        long now = System.currentTimeMillis();
        if (accessToken != null && accessToken.expiresAtMs() - 30_000L > now) {
            return Optional.of(accessToken.value());
        }
        try {
            String basic = Base64.getEncoder().encodeToString(
                    (spotifyClientId + ":" + spotifyClientSecret).getBytes(StandardCharsets.UTF_8));
            HttpRequest request = HttpRequest.newBuilder(URI.create(tokenUrl))
                    .timeout(Duration.ofMillis(timeoutMs))
                    .header("Authorization", "Basic " + basic)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString("grant_type=client_credentials")).build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != HttpURLConnection.HTTP_OK || response.body().isBlank()) {
                log.warn("Spotify token request returned HTTP {}", response.statusCode());
                return Optional.empty();
            }
            Json json = Json.parse(response.body());
            if (!(json instanceof JsonObject root)) return Optional.empty();
            String value = root.string("access_token");
            Long expiresIn = root.longValue("expires_in");
            if (value == null || expiresIn == null || expiresIn <= 0) return Optional.empty();
            accessToken = new AccessToken(value, now + expiresIn * 1000L);
            return Optional.of(value);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException | RuntimeException e) {
            log.warn("Spotify token request failed: {}", sanitizer.sanitize(String.valueOf(e.getMessage())));
        }
        return Optional.empty();
    }

    private static Optional<Long> parsePositiveLong(String value) {
        try {
            long parsed = Long.parseLong(value.trim());
            return parsed > 0 ? Optional.of(parsed) : Optional.empty();
        } catch (RuntimeException ignored) {
            return Optional.empty();
        }
    }

    public Optional<CollectionMetadata> resolveCollection(String kind, String id) {
        if (!("album".equals(kind) || "playlist".equals(kind)) || id == null || id.isBlank()) {
            return Optional.empty();
        }
        String cacheKey = kind + ":" + id;
        CachedCollection cached = collectionCache.get(cacheKey);
        long now = System.currentTimeMillis();
        if (cached != null && cached.expiresAtMs() > now) {
            return Optional.of(cached.metadata());
        }

        CompletableFuture<Optional<CollectionMetadata>> flight = new CompletableFuture<>();
        CompletableFuture<Optional<CollectionMetadata>> existing = collectionInFlight.putIfAbsent(cacheKey, flight);
        if (existing != null) {
            return existing.join();
        }

        try {
            CachedCollection latest = collectionCache.get(cacheKey);
            if (latest != null && latest.expiresAtMs() > System.currentTimeMillis()) {
                Optional<CollectionMetadata> result = Optional.of(latest.metadata());
                flight.complete(result);
                return result;
            }

            Optional<CollectionMetadata> resolved = Optional.empty();
            if (System.currentTimeMillis() >= rateLimitedUntilMs) {
                resolved = resolveCollectionViaSpotifyWebApi(kind, id, false);
            }
            if (resolved.isEmpty()) {
                resolved = resolveCollectionViaDaemon(kind, id);
            }
            if (resolved.isPresent()) {
                collectionCache.put(cacheKey, new CachedCollection(
                        resolved.get(), System.currentTimeMillis() + COLLECTION_CACHE_TTL_MS));
            } else if (latest != null) {
                log.warn("Using stale cached Spotify collection metadata for {}", cacheKey);
                resolved = Optional.of(latest.metadata());
            }
            flight.complete(resolved);
            return resolved;
        } catch (RuntimeException e) {
            CachedCollection stale = collectionCache.get(cacheKey);
            Optional<CollectionMetadata> fallback = stale == null
                    ? Optional.empty() : Optional.of(stale.metadata());
            flight.complete(fallback);
            log.warn("Spotify collection metadata {} failed: {}", cacheKey,
                    sanitizer.sanitize(String.valueOf(e.getMessage())));
            return fallback;
        } finally {
            collectionInFlight.remove(cacheKey, flight);
        }
    }

    private Optional<CollectionMetadata> resolveCollectionViaSpotifyWebApi(
            String kind, String id, boolean retriedAfterUnauthorized) {
        if (spotifyClientId.isBlank() || spotifyClientSecret.isBlank()) {
            return Optional.empty();
        }
        Optional<String> token = accessToken();
        if (token.isEmpty()) {
            return Optional.empty();
        }

        String url = "album".equals(kind)
                ? apiBaseUrl + "/v1/albums/" + id + "?market="
                        + URLEncoder.encode(spotifyMarket, StandardCharsets.UTF_8) + "&limit=50"
                : apiBaseUrl + "/v1/playlists/" + id;
        List<AudioTrackInfo> result = new ArrayList<>();
        String name = null;
        String author = null;
        String artworkUrl = null;

        for (int pageNumber = 1; pageNumber <= MAX_COLLECTION_PAGES && url != null; pageNumber++) {
            try {
                HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                        .timeout(Duration.ofMillis(timeoutMs))
                        .header("Authorization", "Bearer " + token.get()).GET().build();
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                int status = response.statusCode();
                if (status == HttpURLConnection.HTTP_UNAUTHORIZED) {
                    if (!retriedAfterUnauthorized) {
                        accessToken = null;
                        return resolveCollectionViaSpotifyWebApi(kind, id, true);
                    }
                    return Optional.empty();
                }
                if (status == 429) {
                    long retrySeconds = response.headers().firstValue("Retry-After")
                            .flatMap(MetadataResolver::parsePositiveLong).orElse(30L);
                    long backoffSeconds = Math.min(retrySeconds, 300L);
                    rateLimitedUntilMs = Math.max(
                            rateLimitedUntilMs, System.currentTimeMillis() + backoffSeconds * 1000L);
                    log.warn("Spotify Web API rate limited collection metadata for {} seconds", backoffSeconds);
                    return pageNumber == 1 ? Optional.empty()
                            : Optional.of(collectionMetadata(kind, name, author, artworkUrl, result));
                }
                if (status == HttpURLConnection.HTTP_NOT_FOUND) {
                    return pageNumber == 1 ? Optional.empty()
                            : Optional.of(collectionMetadata(kind, name, author, artworkUrl, result));
                }
                if (status != HttpURLConnection.HTTP_OK
                        || response.body() == null || response.body().isBlank()) {
                    log.warn("Spotify Web API collection metadata returned HTTP {}", status);
                    return pageNumber == 1 ? Optional.empty()
                            : Optional.of(collectionMetadata(kind, name, author, artworkUrl, result));
                }

                Json parsed = Json.parse(response.body());
                if (!(parsed instanceof JsonObject root)) {
                    return pageNumber == 1 ? Optional.empty()
                            : Optional.of(collectionMetadata(kind, name, author, artworkUrl, result));
                }
                JsonObject page;
                if (pageNumber == 1) {
                    name = root.string("name");
                    author = "playlist".equals(kind)
                            ? root.ownerDisplayName()
                            : root.artists().stream().findFirst().orElse(null);
                    artworkUrl = root.imagesFirstUrl();
                    Json tracksValue = root.fields.get("tracks");
                    if (!(tracksValue instanceof JsonObject tracks)) {
                        return Optional.empty();
                    }
                    page = tracks;
                } else {
                    page = root;
                }

                Json itemsValue = page.fields.get("items");
                if (!(itemsValue instanceof JsonArray items)) {
                    return pageNumber == 1 ? Optional.empty()
                            : Optional.of(collectionMetadata(kind, name, author, artworkUrl, result));
                }
                appendCollectionTracks(kind, items.value, result, MAX_COLLECTION_TRACKS);
                String next = page.string("next");
                if (result.size() >= MAX_COLLECTION_TRACKS) {
                    log.warn("Collection '{}:{}' truncated at 500 tracks", kind, id);
                    return Optional.of(collectionMetadata(kind, name, author, artworkUrl, result));
                }
                url = next == null ? null : rewriteCollectionPageUrl(next);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return pageNumber == 1 ? Optional.empty()
                        : Optional.of(collectionMetadata(kind, name, author, artworkUrl, result));
            } catch (IOException | RuntimeException e) {
                log.warn("Spotify Web API collection metadata failed: {}",
                        sanitizer.sanitize(String.valueOf(e.getMessage())));
                return pageNumber == 1 ? Optional.empty()
                        : Optional.of(collectionMetadata(kind, name, author, artworkUrl, result));
            }
        }
        return Optional.of(collectionMetadata(kind, name, author, artworkUrl, result));
    }

    private Optional<CollectionMetadata> resolveCollectionViaDaemon(String kind, String id) {
        for (ReadyBackend backend : List.copyOf(selector.readyBackends())) {
            String url = backend.apiBase() + "/web-api/v1/" + kind + "s/" + id;
            Optional<JsonObject> response = fetchObject(url);
            if (response.isEmpty()) {
                continue;
            }
            JsonObject root = response.get();
            Json tracksValue = root.fields.get("tracks");
            if (!(tracksValue instanceof JsonObject tracksObject)) {
                continue;
            }
            Json itemsValue = tracksObject.fields.get("items");
            if (!(itemsValue instanceof JsonArray items)) {
                continue;
            }
            List<AudioTrackInfo> result = new ArrayList<>();
            appendCollectionTracks(kind, items.value, result, Integer.MAX_VALUE);
            String author = "playlist".equals(kind)
                    ? root.ownerDisplayName()
                    : root.artists().stream().findFirst().orElse(null);
            return Optional.of(collectionMetadata(
                    kind, root.string("name"), author, root.imagesFirstUrl(), result));
        }
        return Optional.empty();
    }

    private void appendCollectionTracks(
            String kind, List<Json> values, List<AudioTrackInfo> result, int maximumSize) {
        for (Json value : values) {
            if (result.size() >= maximumSize) {
                return;
            }
            if (!(value instanceof JsonObject item)) {
                continue;
            }
            Json candidate = "playlist".equals(kind) ? item.fields.get("track") : item;
            if (!(candidate instanceof JsonObject track)
                    || item.booleanValue("is_local") || track.booleanValue("is_local")) {
                continue;
            }
            String trackId = track.string("id");
            String title = track.string("name");
            Long duration = track.longValue("duration_ms");
            if (trackId == null || title == null || duration == null || duration <= 0) {
                continue;
            }
            result.add(new AudioTrackInfo(title, String.join(", ", track.artists()), duration,
                    "spdirect:" + trackId, true,
                    "https://open.spotify.com/track/" + trackId,
                    track.albumArtwork(), track.isrc()));
        }
    }

    private CollectionMetadata collectionMetadata(
            String kind, String name, String author, String artworkUrl, List<AudioTrackInfo> tracks) {
        return new CollectionMetadata(
                name == null ? "Spotify " + kind : name, author, artworkUrl, List.copyOf(tracks));
    }

    private String rewriteCollectionPageUrl(String next) {
        URI nextUri = URI.create(next);
        String path = nextUri.getRawPath();
        String query = nextUri.getRawQuery();
        return apiBaseUrl + path + (query == null ? "" : "?" + query);
    }

    private Optional<JsonObject> fetchObject(String url) {
        String safeUrl = sanitizer.sanitizeUrl(url);
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofMillis(timeoutMs)).GET().build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != HttpURLConnection.HTTP_OK
                    || response.body() == null || response.body().isBlank()) {
                log.warn("Spotify collection metadata {} returned HTTP {}", safeUrl, response.statusCode());
                return Optional.empty();
            }
            Json json = Json.parse(response.body());
            return json instanceof JsonObject object ? Optional.of(object) : Optional.empty();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (IOException | RuntimeException e) {
            log.warn("Spotify collection metadata {} failed: {}", safeUrl,
                    sanitizer.sanitize(String.valueOf(e.getMessage())));
            return Optional.empty();
        }
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

        boolean booleanValue(String key) {
            Json v = fields.get(key);
            return v instanceof JsonBool b && b.value;
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

        String imagesFirstUrl() {
            Json images = fields.get("images");
            if (!(images instanceof JsonArray arr) || arr.value.isEmpty()) {
                return null;
            }
            Json first = arr.value.get(0);
            return first instanceof JsonObject o ? o.string("url") : null;
        }

        String ownerDisplayName() {
            Json owner = fields.get("owner");
            return owner instanceof JsonObject o ? o.string("display_name") : null;
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
