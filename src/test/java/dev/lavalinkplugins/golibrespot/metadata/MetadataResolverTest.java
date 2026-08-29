package dev.lavalinkplugins.golibrespot.metadata;

import com.sedmelluq.discord.lavaplayer.track.AudioTrackInfo;
import dev.lavalinkplugins.golibrespot.metadata.MetadataResolver.ReadyBackend;
import dev.lavalinkplugins.golibrespot.metadata.MetadataResolver.ReadyBackendSelector;
import dev.lavalinkplugins.golibrespot.testfixtures.FakeLibrespotDaemon;
import dev.lavalinkplugins.golibrespot.testfixtures.FakeSpotifyWebApi;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link MetadataResolver} — the no-lease, best-effort
 * {@code GET /web-api/v1/tracks/{id}} metadata fetch.
 *
 * <p>Contract under test: a READY backend is selected WITHOUT acquiring a
 * lease (no {@code /player/play} is ever issued); the Spotify Web API track
 * response is mapped through {@link TrackMetadata} + {@code AudioTrackInfoMapper}
 * into an {@link AudioTrackInfo}; every failure mode (non-2xx, 204 no-session,
 * empty 200, missing/non-positive {@code duration_ms}, malformed body, dead
 * daemon, timeout) yields an empty result — metadata is never fabricated.</p>
 */
class MetadataResolverTest {

    private static final String TRACK_ID = "4uLU6hMCjMI75M1A2tKUQC";
    private static final String TRACK_PATH = "/web-api/v1/tracks/" + TRACK_ID;
    private static final String FAKE_TRACK_PATH = "/v1/tracks/" + TRACK_ID;
    private static final String PLAYLIST_ID = "37i9dQZF1DXcBWIGoYBM5M";
    private static final String PLAYLIST_PATH = "/v1/playlists/" + PLAYLIST_ID;
    private static final String PLAYLIST_TRACKS_PATH = PLAYLIST_PATH + "/tracks";
    private static final String DAEMON_PLAYLIST_PATH = "/web-api/v1/playlists/" + PLAYLIST_ID;
    private static final String ALBUM_ID = "4aawyAB9vmqN3uQ7FjRGTy";
    private static final String ALBUM_PATH = "/v1/albums/" + ALBUM_ID;

    private static ReadyBackend backend(FakeLibrespotDaemon daemon) {
        return new ReadyBackend(daemon.getHttpUrl());
    }

    private static ReadyBackendSelector selector(ReadyBackend... backends) {
        return () -> List.of(backends);
    }

    /**
     * Builds a resolver pointed at a fake Spotify Web API via the root
     * constructor's injected HttpClient + tokenUrl + apiBaseUrl seam.
     */
    private static MetadataResolver seamResolver(FakeSpotifyWebApi fake, ReadyBackendSelector selector) {
        return new MetadataResolver(selector, 5_000L, "seam-client-id", "seam-client-secret", "AR",
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build(),
                fake.getTokenUrl(), fake.getApiBaseUrl());
    }

    /**
     * Builds a Spotify Web API {@code track} object (daemon passthrough shape):
     * {@code name}, {@code artists[].name}, {@code album.name} +
     * {@code album.images[0].url}, {@code duration_ms}, {@code external_ids.isrc}.
     * Null fields are omitted so the resolver must tolerate their absence.
     */
    private static String webApiTrackJson(
            String name, List<String> artists, String album, String artworkUrl, Long durationMs, String isrc) {
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"name\":").append(FakeLibrespotDaemon.jsonString(name));
        if (artists != null) {
            sb.append(",\"artists\":[");
            for (int i = 0; i < artists.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append("{\"name\":").append(FakeLibrespotDaemon.jsonString(artists.get(i))).append('}');
            }
            sb.append(']');
        }
        if (album != null) {
            sb.append(",\"album\":{\"name\":").append(FakeLibrespotDaemon.jsonString(album));
            if (artworkUrl != null) {
                sb.append(",\"images\":[{\"url\":").append(FakeLibrespotDaemon.jsonString(artworkUrl)).append("}]");
            }
            sb.append('}');
        }
        if (durationMs != null) {
            sb.append(",\"duration_ms\":").append(durationMs);
        }
        if (isrc != null) {
            sb.append(",\"external_ids\":{\"isrc\":").append(FakeLibrespotDaemon.jsonString(isrc)).append('}');
        }
        return sb.append('}').toString();
    }

    private static String fullTrackJson() {
        return webApiTrackJson(
                "Livin' on a Prayer",
                List.of("Bon Jovi", "Jon Bon Jovi"),
                "Slippery When Wet",
                "https://i.scdn.co/image/abc",
                251_000L,
                "USRC18204510");
    }

    private static String collectionTrackJson(String id, String name) {
        return FakeSpotifyWebApi.trackObjectJson(
                id, name, "Collection Artist", "Collection Album",
                "https://i.scdn.co/image/track", 180_000L, "USRC10000001");
    }

    private static String playlistJson(String itemsJson, String nextUrl) {
        return "{\"name\":\"Fixture Playlist\","
                + "\"owner\":{\"display_name\":\"Fixture Owner\"},"
                + "\"images\":[{\"url\":\"https://i.scdn.co/image/playlist\"}],"
                + "\"tracks\":" + FakeSpotifyWebApi.pageJson(itemsJson, nextUrl) + "}";
    }

    private static String albumJson(String itemsJson, String nextUrl) {
        return "{\"name\":\"Fixture Album\","
                + "\"artists\":[{\"name\":\"Fixture Album Artist\"}],"
                + "\"images\":[{\"url\":\"https://i.scdn.co/image/album\"}],"
                + "\"tracks\":" + FakeSpotifyWebApi.pageJson(itemsJson, nextUrl) + "}";
    }

    // ------------------------------------------------------------------
    // Success path
    // ------------------------------------------------------------------

    @Test
    void resolvesFullTrackFromWebApiWithoutLease() throws Exception {
        try (FakeLibrespotDaemon daemon = new FakeLibrespotDaemon()) {
            daemon.scriptGet(TRACK_PATH, FakeLibrespotDaemon.Response.ok(fullTrackJson()));
            daemon.start();

            MetadataResolver resolver = new MetadataResolver(selector(backend(daemon)));

            Optional<AudioTrackInfo> result = resolver.resolve(TRACK_ID);

            assertThat(result).isPresent();
            AudioTrackInfo info = result.orElseThrow();
            assertThat(info.title).isEqualTo("Livin' on a Prayer");
            assertThat(info.author).isEqualTo("Bon Jovi, Jon Bon Jovi");
            assertThat(info.length).isEqualTo(251_000L);
            assertThat(info.identifier).isEqualTo("spdirect:" + TRACK_ID);
            assertThat(info.isStream).isTrue();
            assertThat(info.uri).isNull();
            assertThat(info.artworkUrl).isEqualTo("https://i.scdn.co/image/abc");
            assertThat(info.isrc).isEqualTo("USRC18204510");
        }
    }

    @Test
    void requestsOnlyTheWebApiTrackPathAndNeverPlayerPlay() throws Exception {
        try (FakeLibrespotDaemon daemon = new FakeLibrespotDaemon()) {
            daemon.scriptGet(TRACK_PATH, FakeLibrespotDaemon.Response.ok(fullTrackJson()));
            daemon.start();

            new MetadataResolver(selector(backend(daemon))).resolve(TRACK_ID);

            List<FakeLibrespotDaemon.RecordedCommand> commands = daemon.getReceivedCommands();
            assertThat(commands).isNotEmpty();
            assertThat(commands).allMatch(c -> c.path().equals(TRACK_PATH));
            assertThat(commands).extracting(FakeLibrespotDaemon.RecordedCommand::method)
                    .containsOnly("GET");
            assertThat(commands).noneMatch(c -> c.path().equals("/player/play"));
        }
    }

    @Test
    void isrcAndArtworkAreOptional() throws Exception {
        String body = webApiTrackJson("No ISRC", List.of("Artist"), "Album", null, 180_000L, null);
        try (FakeLibrespotDaemon daemon = new FakeLibrespotDaemon()) {
            daemon.scriptGet(TRACK_PATH, FakeLibrespotDaemon.Response.ok(body));
            daemon.start();

            AudioTrackInfo info = new MetadataResolver(selector(backend(daemon)))
                    .resolve(TRACK_ID).orElseThrow();

            assertThat(info.isrc).isNull();
            assertThat(info.artworkUrl).isNull();
            assertThat(info.title).isEqualTo("No ISRC");
        }
    }

    @Test
    void missingAlbumAndArtistsStillResolveWithEmptyDefaults() throws Exception {
        String body = webApiTrackJson("Bare track", null, null, null, 120_000L, null);
        try (FakeLibrespotDaemon daemon = new FakeLibrespotDaemon()) {
            daemon.scriptGet(TRACK_PATH, FakeLibrespotDaemon.Response.ok(body));
            daemon.start();

            AudioTrackInfo info = new MetadataResolver(selector(backend(daemon)))
                    .resolve(TRACK_ID).orElseThrow();

            assertThat(info.author).isEmpty();
            assertThat(info.length).isEqualTo(120_000L);
        }
    }

    // ------------------------------------------------------------------
    // Typed failure surface
    // ------------------------------------------------------------------

    @Test
    void emptyWhenNoReadyBackend() throws Exception {
        MetadataResolver resolver = new MetadataResolver(List::of);

        assertThat(resolver.resolve(TRACK_ID)).isEmpty();
    }

    @Test
    void failsOnNon200Status() throws Exception {
        try (FakeLibrespotDaemon daemon = new FakeLibrespotDaemon()) {
            daemon.scriptGet(TRACK_PATH, FakeLibrespotDaemon.Response.error(404));
            daemon.start();

            assertThat(new MetadataResolver(selector(backend(daemon))).resolve(TRACK_ID)).isEmpty();
        }
    }

    @Test
    void failsOnNoSession204() throws Exception {
        try (FakeLibrespotDaemon daemon = new FakeLibrespotDaemon()) {
            daemon.scriptGet(TRACK_PATH, FakeLibrespotDaemon.Response.noContent());
            daemon.start();

            assertThat(new MetadataResolver(selector(backend(daemon))).resolve(TRACK_ID)).isEmpty();
        }
    }

    @Test
    void failsOnDefaultNoSessionMode() throws Exception {
        try (FakeLibrespotDaemon daemon = new FakeLibrespotDaemon()) {
            daemon.hasSession(false);
            daemon.start();

            assertThat(new MetadataResolver(selector(backend(daemon))).resolve(TRACK_ID)).isEmpty();
        }
    }

    @Test
    void failsOnEmpty200() throws Exception {
        try (FakeLibrespotDaemon daemon = new FakeLibrespotDaemon()) {
            daemon.scriptGet(TRACK_PATH, FakeLibrespotDaemon.Response.ok());
            daemon.start();

            assertThat(new MetadataResolver(selector(backend(daemon))).resolve(TRACK_ID)).isEmpty();
        }
    }

    @Test
    void failsOnMissingDurationMs() throws Exception {
        String body = webApiTrackJson("No duration", List.of("Artist"), "Album", null, null, null);
        try (FakeLibrespotDaemon daemon = new FakeLibrespotDaemon()) {
            daemon.scriptGet(TRACK_PATH, FakeLibrespotDaemon.Response.ok(body));
            daemon.start();

            assertThat(new MetadataResolver(selector(backend(daemon))).resolve(TRACK_ID)).isEmpty();
        }
    }

    @Test
    void failsOnZeroOrNegativeDurationMs() throws Exception {
        for (long duration : new long[] {0L, -1L, -100L}) {
            String body = webApiTrackJson("Bad duration", List.of("Artist"), "Album", null, duration, null);
            try (FakeLibrespotDaemon daemon = new FakeLibrespotDaemon()) {
                daemon.scriptGet(TRACK_PATH, FakeLibrespotDaemon.Response.ok(body));
                daemon.start();

                assertThat(new MetadataResolver(selector(backend(daemon))).resolve(TRACK_ID))
                        .as("duration %d must not resolve", duration)
                        .isEmpty();
            }
        }
    }

    @Test
    void failsOnMalformedJson() throws Exception {
        try (FakeLibrespotDaemon daemon = new FakeLibrespotDaemon()) {
            daemon.scriptGet(TRACK_PATH, FakeLibrespotDaemon.Response.ok("{not json at all"));
            daemon.start();

            assertThat(new MetadataResolver(selector(backend(daemon))).resolve(TRACK_ID)).isEmpty();
        }
    }

    @Test
    void failsWhenDaemonIsDown() throws Exception {
        FakeLibrespotDaemon daemon = new FakeLibrespotDaemon();
        daemon.start();
        int port = daemon.getPort();
        daemon.stop();

        MetadataResolver resolver = new MetadataResolver(
                selector(new ReadyBackend("http://127.0.0.1:" + port)), 2_000L);

        assertThat(resolver.resolve(TRACK_ID)).isEmpty();
    }

    @Test
    void honorsMetadataTimeoutWhenBackendHangs() throws Exception {
        try (FakeLibrespotDaemon daemon = new FakeLibrespotDaemon()) {
            daemon.scriptGet(TRACK_PATH, FakeLibrespotDaemon.Response.hang());
            daemon.start();

            long start = System.nanoTime();
            Optional<AudioTrackInfo> result =
                    new MetadataResolver(selector(backend(daemon)), 250L).resolve(TRACK_ID);
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;

            assertThat(result).isEmpty();
            assertThat(elapsedMs).isLessThan(5_000L);
        }
    }

    @Test
    void blankTrackIdResolvesEmptyWithoutHttpCalls() throws Exception {
        try (FakeLibrespotDaemon daemon = new FakeLibrespotDaemon()) {
            daemon.start();
            MetadataResolver resolver = new MetadataResolver(selector(backend(daemon)));

            assertThat(resolver.resolve("")).isEmpty();
            assertThat(resolver.resolve("   ")).isEmpty();
            assertThat(daemon.getReceivedCommands()).isEmpty();
        }
    }

    // ------------------------------------------------------------------
    // Multi-backend fallback
    // ------------------------------------------------------------------

    @Test
    void fallsBackToSecondReadyBackendWhenFirstFails() throws Exception {
        try (FakeLibrespotDaemon first = new FakeLibrespotDaemon();
             FakeLibrespotDaemon second = new FakeLibrespotDaemon()) {
            first.scriptGet(TRACK_PATH, FakeLibrespotDaemon.Response.error(404));
            second.scriptGet(TRACK_PATH, FakeLibrespotDaemon.Response.ok(fullTrackJson()));
            first.start();
            second.start();

            MetadataResolver resolver = new MetadataResolver(selector(backend(first), backend(second)));

            Optional<AudioTrackInfo> result = resolver.resolve(TRACK_ID);

            assertThat(result).isPresent();
            assertThat(result.orElseThrow().title).isEqualTo("Livin' on a Prayer");
            // both backends were probed, in order; neither was leased via /player/play
            assertThat(first.getReceivedCommands()).extracting(FakeLibrespotDaemon.RecordedCommand::path)
                    .containsExactly(TRACK_PATH);
            assertThat(second.getReceivedCommands()).extracting(FakeLibrespotDaemon.RecordedCommand::path)
                    .containsExactly(TRACK_PATH);
            assertThat(first.getReceivedCommands()).noneMatch(c -> c.path().equals("/player/play"));
            assertThat(second.getReceivedCommands()).noneMatch(c -> c.path().equals("/player/play"));
        }
    }

    @Test
    void failsWhenAllReadyBackendsFail() throws Exception {
        try (FakeLibrespotDaemon first = new FakeLibrespotDaemon();
             FakeLibrespotDaemon second = new FakeLibrespotDaemon()) {
            first.scriptGet(TRACK_PATH, FakeLibrespotDaemon.Response.error(429));
            second.scriptGet(TRACK_PATH, FakeLibrespotDaemon.Response.error(403));
            first.start();
            second.start();

            MetadataResolver resolver = new MetadataResolver(selector(backend(first), backend(second)));

            assertThat(resolver.resolve(TRACK_ID)).isEmpty();
            assertThat(first.getReceivedCommands()).hasSize(1);
            assertThat(second.getReceivedCommands()).hasSize(1);
        }
    }

    // ------------------------------------------------------------------
    // Test seam: injected HttpClient + tokenUrl + apiBaseUrl
    // ------------------------------------------------------------------

    @Test
    void directApiFetchHitsInjectedApiBaseUrl() throws Exception {
        try (FakeSpotifyWebApi fake = new FakeSpotifyWebApi()) {
            fake.scriptGet(FAKE_TRACK_PATH, FakeSpotifyWebApi.Response.ok(fullTrackJson()));
            fake.start();

            MetadataResolver resolver = seamResolver(fake, List::of);

            Optional<AudioTrackInfo> result = resolver.resolve(TRACK_ID);

            assertThat(result).isPresent();
            assertThat(result.orElseThrow().title).isEqualTo("Livin' on a Prayer");
            // the request log is only reachable via the fake's own host:port,
            // so these recorded URLs prove the direct fetch hit the injected base
            assertThat(fake.getReceivedRequests()).extracting(FakeSpotifyWebApi.RecordedRequest::url)
                    .containsExactly(fake.getTokenUrl(), fake.getApiBaseUrl() + FAKE_TRACK_PATH + "?market=AR");
        }
    }

    @Test
    void tokenFetchHitsInjectedTokenUrl() throws Exception {
        try (FakeSpotifyWebApi fake = new FakeSpotifyWebApi()) {
            fake.scriptGet(FAKE_TRACK_PATH, FakeSpotifyWebApi.Response.ok(fullTrackJson()));
            fake.scriptPost("/api/token", FakeSpotifyWebApi.Response.ok(
                    FakeSpotifyWebApi.tokenJson("seam-access-token", 3600)));
            fake.start();

            MetadataResolver resolver = seamResolver(fake, List::of);

            assertThat(resolver.resolve(TRACK_ID)).isPresent();

            FakeSpotifyWebApi.RecordedRequest tokenRequest = fake.getReceivedRequests().get(0);
            assertThat(tokenRequest.url()).isEqualTo(fake.getTokenUrl());
            assertThat(tokenRequest.method()).isEqualTo("POST");
            assertThat(tokenRequest.body()).contains("grant_type=client_credentials");
            assertThat(fake.hitCount("POST", "/api/token")).isEqualTo(1);
        }
    }

    @Test
    void blankCredentialsSkipDirectPathAndUseDaemon() throws Exception {
        try (FakeSpotifyWebApi fake = new FakeSpotifyWebApi();
             FakeLibrespotDaemon daemon = new FakeLibrespotDaemon()) {
            fake.scriptGet(FAKE_TRACK_PATH, FakeSpotifyWebApi.Response.ok(fullTrackJson()));
            fake.start();
            daemon.scriptGet(TRACK_PATH, FakeLibrespotDaemon.Response.error(404));
            daemon.start();

            MetadataResolver resolver = new MetadataResolver(selector(backend(daemon)));

            assertThat(resolver.resolve(TRACK_ID)).isEmpty();
            assertThat(fake.getReceivedRequests()).isEmpty();
            assertThat(daemon.getReceivedCommands()).extracting(FakeLibrespotDaemon.RecordedCommand::path)
                    .containsExactly(TRACK_PATH);
        }
    }

    @Test
    void tokenFailureFallsThroughToDaemonPath() throws Exception {
        try (FakeSpotifyWebApi fake = new FakeSpotifyWebApi();
             FakeLibrespotDaemon daemon = new FakeLibrespotDaemon()) {
            fake.scriptPost("/api/token", FakeSpotifyWebApi.Response.error(500));
            fake.start();
            daemon.scriptGet(TRACK_PATH, FakeLibrespotDaemon.Response.ok(fullTrackJson()));
            daemon.start();

            MetadataResolver resolver = seamResolver(fake, selector(backend(daemon)));

            Optional<AudioTrackInfo> result = resolver.resolve(TRACK_ID);

            assertThat(result).isPresent();
            assertThat(result.orElseThrow().title).isEqualTo("Livin' on a Prayer");
            assertThat(fake.hitCount("POST", "/api/token")).isEqualTo(1);
            assertThat(daemon.getReceivedCommands()).extracting(FakeLibrespotDaemon.RecordedCommand::path)
                    .containsExactly(TRACK_PATH);
        }
    }

    @Test
    void tokenUnauthorizedFallsThroughToDaemonPath() throws Exception {
        try (FakeSpotifyWebApi fake = new FakeSpotifyWebApi();
             FakeLibrespotDaemon daemon = new FakeLibrespotDaemon()) {
            fake.scriptPost("/api/token", FakeSpotifyWebApi.Response.error(401));
            fake.start();
            daemon.scriptGet(TRACK_PATH, FakeLibrespotDaemon.Response.ok(fullTrackJson()));
            daemon.start();

            MetadataResolver resolver = seamResolver(fake, selector(backend(daemon)));

            Optional<AudioTrackInfo> result = resolver.resolve(TRACK_ID);

            assertThat(result).isPresent();
            assertThat(result.orElseThrow().title).isEqualTo("Livin' on a Prayer");
            assertThat(fake.hitCount("POST", "/api/token")).isEqualTo(1);
            assertThat(daemon.getReceivedCommands()).extracting(FakeLibrespotDaemon.RecordedCommand::path)
                    .containsExactly(TRACK_PATH);
        }
    }

    @Test
    void resolvesTwoPagePlaylistWithOwnerArtworkAndRewrittenNextHost() throws Exception {
        String secondTrackId = "0VjIjW4GlUZAMYd2vXMi3b";
        String productionNext = "https://api.spotify.com" + PLAYLIST_TRACKS_PATH + "?offset=1&limit=100";
        try (FakeSpotifyWebApi fake = new FakeSpotifyWebApi()) {
            fake.scriptGet(PLAYLIST_PATH, FakeSpotifyWebApi.Response.ok(playlistJson(
                    "[" + FakeSpotifyWebApi.playlistItem(collectionTrackJson(TRACK_ID, "First")) + "]",
                    productionNext)));
            fake.scriptGet(PLAYLIST_TRACKS_PATH, FakeSpotifyWebApi.Response.ok(FakeSpotifyWebApi.pageJson(
                    "[" + FakeSpotifyWebApi.playlistItem(collectionTrackJson(secondTrackId, "Second")) + "]",
                    null)));
            fake.start();

            MetadataResolver.CollectionMetadata metadata = seamResolver(fake, List::of)
                    .resolveCollection("playlist", PLAYLIST_ID).orElseThrow();

            assertThat(metadata.name()).isEqualTo("Fixture Playlist");
            assertThat(metadata.author()).isEqualTo("Fixture Owner");
            assertThat(metadata.artworkUrl()).isEqualTo("https://i.scdn.co/image/playlist");
            assertThat(metadata.tracks()).extracting(info -> info.identifier)
                    .containsExactly("spdirect:" + TRACK_ID, "spdirect:" + secondTrackId);
            assertThat(fake.getReceivedRequests()).anySatisfy(request -> {
                assertThat(request.path()).isEqualTo(PLAYLIST_TRACKS_PATH);
                assertThat(request.query()).isEqualTo("offset=1&limit=100");
                assertThat(request.url()).startsWith(fake.getApiBaseUrl());
            });
        }
    }

    @Test
    void pageOneRateLimitGatesImmediateRetryAndFallsBackToDaemon() throws Exception {
        try (FakeSpotifyWebApi fake = new FakeSpotifyWebApi();
             FakeLibrespotDaemon daemon = new FakeLibrespotDaemon()) {
            fake.scriptGet(PLAYLIST_PATH, FakeSpotifyWebApi.Response.error(429).withRetryAfter("60"));
            fake.start();
            daemon.scriptGet(DAEMON_PLAYLIST_PATH, FakeLibrespotDaemon.Response.error(429));
            daemon.start();
            MetadataResolver resolver = seamResolver(fake, selector(backend(daemon)));

            long beforeResolve = System.currentTimeMillis();
            assertThat(resolver.resolveCollection("playlist", PLAYLIST_ID)).isEmpty();
            assertThat(resolver.resolveCollection("playlist", PLAYLIST_ID)).isEmpty();

            assertThat(fake.hitCount("GET", PLAYLIST_PATH)).isEqualTo(1);
            assertThat(daemon.getReceivedCommands())
                    .extracting(FakeLibrespotDaemon.RecordedCommand::path)
                    .containsExactly(DAEMON_PLAYLIST_PATH, DAEMON_PLAYLIST_PATH);
            var field = MetadataResolver.class.getDeclaredField("rateLimitedUntilMs");
            field.setAccessible(true);
            assertThat(field.getLong(resolver)).isGreaterThanOrEqualTo(beforeResolve + 60_000L);
        }
    }

    @Test
    void retriesCollectionOnceAfterUnauthorized() throws Exception {
        try (FakeSpotifyWebApi fake = new FakeSpotifyWebApi()) {
            fake.enqueueGet(PLAYLIST_PATH,
                    FakeSpotifyWebApi.Response.error(401),
                    FakeSpotifyWebApi.Response.ok(playlistJson("[]", null)));
            fake.start();

            assertThat(seamResolver(fake, List::of).resolveCollection("playlist", PLAYLIST_ID)).isPresent();
            assertThat(fake.hitCount("GET", PLAYLIST_PATH)).isEqualTo(2);
        }
    }

    @Test
    void pageTwoRateLimitReturnsAccumulatedPageOneTracks() throws Exception {
        String next = "https://api.spotify.com" + PLAYLIST_TRACKS_PATH + "?offset=1&limit=100";
        try (FakeSpotifyWebApi fake = new FakeSpotifyWebApi()) {
            fake.scriptGet(PLAYLIST_PATH, FakeSpotifyWebApi.Response.ok(playlistJson(
                    "[" + FakeSpotifyWebApi.playlistItem(collectionTrackJson(TRACK_ID, "First")) + "]", next)));
            fake.scriptGet(PLAYLIST_TRACKS_PATH,
                    FakeSpotifyWebApi.Response.error(429).withRetryAfter("10"));
            fake.start();

            MetadataResolver.CollectionMetadata metadata = seamResolver(fake, List::of)
                    .resolveCollection("playlist", PLAYLIST_ID).orElseThrow();

            assertThat(metadata.tracks()).extracting(info -> info.identifier)
                    .containsExactly("spdirect:" + TRACK_ID);
            assertThat(fake.hitCount("GET", PLAYLIST_TRACKS_PATH)).isEqualTo(1);
        }
    }

    @Test
    void successfulEmptyPlaylistIsPresentAndCached() throws Exception {
        try (FakeSpotifyWebApi fake = new FakeSpotifyWebApi()) {
            fake.scriptGet(PLAYLIST_PATH, FakeSpotifyWebApi.Response.ok(playlistJson("[]", null)));
            fake.start();
            MetadataResolver resolver = seamResolver(fake, List::of);

            Optional<MetadataResolver.CollectionMetadata> first = resolver.resolveCollection("playlist", PLAYLIST_ID);
            Optional<MetadataResolver.CollectionMetadata> second = resolver.resolveCollection("playlist", PLAYLIST_ID);

            assertThat(first).isPresent();
            assertThat(first.orElseThrow().tracks()).isEmpty();
            assertThat(second).isPresent();
            assertThat(fake.hitCount("GET", PLAYLIST_PATH)).isEqualTo(1);
        }
    }

    @Test
    void playlistNullTrackItemsAreSkippedWithoutFailure() throws Exception {
        try (FakeSpotifyWebApi fake = new FakeSpotifyWebApi()) {
            fake.scriptGet(PLAYLIST_PATH, FakeSpotifyWebApi.Response.ok(playlistJson(
                    "[{\"track\":null}," + FakeSpotifyWebApi.playlistItem(
                            collectionTrackJson(TRACK_ID, "Playable")) + "]", null)));
            fake.start();

            MetadataResolver.CollectionMetadata metadata = seamResolver(fake, List::of)
                    .resolveCollection("playlist", PLAYLIST_ID).orElseThrow();

            assertThat(metadata.tracks()).extracting(info -> info.title).containsExactly("Playable");
        }
    }

    @Test
    void albumRequestUsesConfiguredMarketAndLimitAndParsesMetadata() throws Exception {
        try (FakeSpotifyWebApi fake = new FakeSpotifyWebApi()) {
            fake.scriptGet(ALBUM_PATH, FakeSpotifyWebApi.Response.ok(albumJson("[]", null)));
            fake.start();

            MetadataResolver.CollectionMetadata metadata = seamResolver(fake, List::of)
                    .resolveCollection("album", ALBUM_ID).orElseThrow();

            assertThat(metadata.author()).isEqualTo("Fixture Album Artist");
            assertThat(metadata.artworkUrl()).isEqualTo("https://i.scdn.co/image/album");
            assertThat(fake.getReceivedRequests()).anySatisfy(request -> {
                assertThat(request.path()).isEqualTo(ALBUM_PATH);
                assertThat(request.query()).isEqualTo("market=AR&limit=50");
            });
        }
    }
}
