package dev.lavalinkplugins.golibrespot.backend.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.lavalinkplugins.golibrespot.backend.model.PlayerCommandResult;
import dev.lavalinkplugins.golibrespot.backend.model.RootResult;
import dev.lavalinkplugins.golibrespot.backend.model.StatusDto;
import dev.lavalinkplugins.golibrespot.backend.model.StatusResult;
import dev.lavalinkplugins.golibrespot.backend.model.TrackDto;
import dev.lavalinkplugins.golibrespot.backend.model.WebApiResult;
import dev.lavalinkplugins.golibrespot.config.GoLibrespotConfig;
import dev.lavalinkplugins.golibrespot.logging.LogSanitizer;
import dev.lavalinkplugins.golibrespot.testfixtures.FakeLibrespotDaemon;
import dev.lavalinkplugins.golibrespot.testfixtures.FakeLibrespotDaemon.RecordedCommand;
import dev.lavalinkplugins.golibrespot.testfixtures.FakeLibrespotDaemon.Response;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * TDD suite for {@link GoLibrespotRestClient} against the scriptable
 * {@link FakeLibrespotDaemon} fixture (T6). Covers the v0.9.0 contract shapes
 * from {@code docs/API_CONTRACT.md} §2: exact request bodies, raw status+body
 * results (never success-from-200), 204 no-session sentinels, typed timeout /
 * I/O / cancellation errors, in-flight cancellation via {@code close()}, and
 * LogSanitizer-redacted logging via an injected sink.
 */
class GoLibrespotRestClientTest {

    private static final Duration AWAIT = Duration.ofSeconds(5);

    // ------------------------------------------------------------ helpers

    private static FakeLibrespotDaemon start(FakeLibrespotDaemon daemon) {
        try {
            daemon.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return daemon;
    }

    private static GoLibrespotRestClient newClient(FakeLibrespotDaemon daemon, int timeoutMs, Consumer<String> sink) {
        return new GoLibrespotRestClient(daemon.getHttpUrl(), timeoutMs, LogSanitizer.defaults(), sink);
    }

    private static RecordedCommand single(FakeLibrespotDaemon daemon) {
        try {
            List<RecordedCommand> cmds = daemon.awaitCommands(1, AWAIT);
            assertThat(cmds).hasSize(1);
            return cmds.get(0);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static int unusedPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    // ------------------------------------------------------------ play/pause/resume/seek

    @Test
    void playPostsSerializedBodyAndReturnsRawStatus() {
        FakeLibrespotDaemon daemon = start(new FakeLibrespotDaemon().play(Response.ok()));
        try (GoLibrespotRestClient client = newClient(daemon, 1_000, s -> {})) {
            PlayerCommandResult result = client.play("spotify:track:4uLU6hMCjMI75M1A2tKUQC", 15_000, false);

            assertThat(result.status()).isEqualTo(200);
            assertThat(result.is2xx()).isTrue();
            assertThat(result.isError()).isFalse();
            assertThat(result.isNoSession()).isFalse();

            RecordedCommand cmd = single(daemon);
            assertThat(cmd.method()).isEqualTo("POST");
            assertThat(cmd.path()).isEqualTo("/player/play");
            assertThat(cmd.body()).isEqualTo(
                    "{\"uri\":\"spotify:track:4uLU6hMCjMI75M1A2tKUQC\",\"paused\":false,\"position\":15000}");
        } finally {
            daemon.stop();
        }
    }

    @Test
    void playSendsPausedTrueAndZeroPosition() {
        FakeLibrespotDaemon daemon = start(new FakeLibrespotDaemon().play(Response.ok()));
        try (GoLibrespotRestClient client = newClient(daemon, 1_000, s -> {})) {
            client.play("spotify:track:abc", 0, true);
            RecordedCommand cmd = single(daemon);
            assertThat(cmd.body()).isEqualTo("{\"uri\":\"spotify:track:abc\",\"paused\":true,\"position\":0}");
        } finally {
            daemon.stop();
        }
    }

    @Test
    void playEscapesQuotesAndBackslashesInUri() {
        FakeLibrespotDaemon daemon = start(new FakeLibrespotDaemon().play(Response.ok()));
        try (GoLibrespotRestClient client = newClient(daemon, 1_000, s -> {})) {
            client.play("spotify:track:ab\"c\\d", 0, false);
            RecordedCommand cmd = single(daemon);
            assertThat(cmd.body()).isEqualTo("""
                    {"uri":"spotify:track:ab\\"c\\\\d","paused":false,"position":0}""");
        } finally {
            daemon.stop();
        }
    }

    @Test
    void playNon2xxReturnsTypedErrorWithoutThrowing() {
        FakeLibrespotDaemon daemon = start(new FakeLibrespotDaemon().play(Response.error(400)));
        try (GoLibrespotRestClient client = newClient(daemon, 1_000, s -> {})) {
            PlayerCommandResult result = client.play("spotify:track:abc", 0, false);
            assertThat(result.status()).isEqualTo(400);
            assertThat(result.isError()).isTrue();
            assertThat(result.is2xx()).isFalse();
        } finally {
            daemon.stop();
        }
    }

    @Test
    void pauseSendsNoBody() {
        FakeLibrespotDaemon daemon = start(new FakeLibrespotDaemon().pause(Response.ok()));
        try (GoLibrespotRestClient client = newClient(daemon, 1_000, s -> {})) {
            PlayerCommandResult result = client.pause();
            assertThat(result.status()).isEqualTo(200);
            RecordedCommand cmd = single(daemon);
            assertThat(cmd.method()).isEqualTo("POST");
            assertThat(cmd.path()).isEqualTo("/player/pause");
            assertThat(cmd.body()).isEmpty();
        } finally {
            daemon.stop();
        }
    }

    @Test
    void resumeSendsNoBody() {
        FakeLibrespotDaemon daemon = start(new FakeLibrespotDaemon().resume(Response.ok()));
        try (GoLibrespotRestClient client = newClient(daemon, 1_000, s -> {})) {
            PlayerCommandResult result = client.resume();
            assertThat(result.status()).isEqualTo(200);
            RecordedCommand cmd = single(daemon);
            assertThat(cmd.method()).isEqualTo("POST");
            assertThat(cmd.path()).isEqualTo("/player/resume");
            assertThat(cmd.body()).isEmpty();
        } finally {
            daemon.stop();
        }
    }

    @Test
    void seekPostsPositionWithRelativeFalse() {
        FakeLibrespotDaemon daemon = start(new FakeLibrespotDaemon().seek(Response.ok()));
        try (GoLibrespotRestClient client = newClient(daemon, 1_000, s -> {})) {
            PlayerCommandResult result = client.seek(120_000);
            assertThat(result.status()).isEqualTo(200);
            RecordedCommand cmd = single(daemon);
            assertThat(cmd.method()).isEqualTo("POST");
            assertThat(cmd.path()).isEqualTo("/player/seek");
            assertThat(cmd.body()).isEqualTo("{\"position\":120000,\"relative\":false}");
        } finally {
            daemon.stop();
        }
    }

    // ------------------------------------------------------------ status

    @Test
    void statusParsesFullStatusWithTrack() {
        String track = FakeLibrespotDaemon.trackJson(
                "spotify:track:4uLU6hMCjMI75M1A2tKUQC", "Track name",
                List.of("Artist A", "Artist B"), "Album", "https://i.scdn.co/image/abc",
                12_345, 240_000, "2020-01-01", 3, 1, "OGG_VORBIS_160", "vorbis",
                160, 44_100, null);
        FakeLibrespotDaemon daemon = start(
                new FakeLibrespotDaemon().status(Response.ok(FakeLibrespotDaemon.statusJson(false, false, track))));
        try (GoLibrespotRestClient client = newClient(daemon, 1_000, s -> {})) {
            StatusResult result = client.status();
            assertThat(result.status()).isEqualTo(200);
            assertThat(result.is2xx()).isTrue();
            assertThat(result.isNoSession()).isFalse();
            assertThat(result.parsed()).isPresent();

            StatusDto dto = result.parsed().orElseThrow();
            assertThat(dto.username()).isEqualTo("fake-user");
            assertThat(dto.deviceId()).isEqualTo("a1b2c3d4e5f6");
            assertThat(dto.deviceType()).isEqualTo("COMPUTER");
            assertThat(dto.deviceName()).isEqualTo("fake-daemon");
            assertThat(dto.playOrigin()).isEqualTo("go-librespot");
            assertThat(dto.stopped()).isFalse();
            assertThat(dto.paused()).isFalse();
            assertThat(dto.buffering()).isFalse();
            assertThat(dto.volume()).isEqualTo(100);
            assertThat(dto.volumeSteps()).isEqualTo(100);
            assertThat(dto.repeatContext()).isFalse();
            assertThat(dto.repeatTrack()).isFalse();
            assertThat(dto.shuffleContext()).isFalse();

            TrackDto t = dto.track();
            assertThat(t).isNotNull();
            assertThat(t.uri()).isEqualTo("spotify:track:4uLU6hMCjMI75M1A2tKUQC");
            assertThat(t.name()).isEqualTo("Track name");
            assertThat(t.artistNames()).containsExactly("Artist A", "Artist B");
            assertThat(t.albumName()).isEqualTo("Album");
            assertThat(t.albumCoverUrl()).isEqualTo("https://i.scdn.co/image/abc");
            assertThat(t.position()).isEqualTo(12_345);
            assertThat(t.duration()).isEqualTo(240_000);
            assertThat(t.releaseDate()).isEqualTo("2020-01-01");
            assertThat(t.trackNumber()).isEqualTo(3);
            assertThat(t.discNumber()).isEqualTo(1);
            assertThat(t.format()).isEqualTo("OGG_VORBIS_160");
            assertThat(t.codec()).isEqualTo("vorbis");
            assertThat(t.bitrate()).isEqualTo(160);
            assertThat(t.sampleRate()).isEqualTo(44_100);
            assertThat(t.bitDepth()).isNull();
        } finally {
            daemon.stop();
        }
    }

    @Test
    void statusParsesNullableFields() {
        String track = FakeLibrespotDaemon.trackJson(
                "spotify:track:1", "N", List.of("A"), "AL", null,
                0, 1_000, "", 1, 1, "OGG_VORBIS_160", "vorbis", null, null, null);
        FakeLibrespotDaemon daemon = start(
                new FakeLibrespotDaemon().status(Response.ok(FakeLibrespotDaemon.statusJson(true, false, track))));
        try (GoLibrespotRestClient client = newClient(daemon, 1_000, s -> {})) {
            StatusDto dto = client.status().parsed().orElseThrow();
            assertThat(dto.stopped()).isTrue();
            assertThat(dto.track()).isNotNull();
            assertThat(dto.track().albumCoverUrl()).isNull();
            assertThat(dto.track().bitrate()).isNull();
            assertThat(dto.track().sampleRate()).isNull();
            assertThat(dto.track().bitDepth()).isNull();
            assertThat(dto.track().releaseDate()).isEmpty();
        } finally {
            daemon.stop();
        }
    }

    @Test
    void statusWithNoStreamHasNullTrack() {
        FakeLibrespotDaemon daemon = start(
                new FakeLibrespotDaemon().status(Response.ok(FakeLibrespotDaemon.statusJson(true, false, null))));
        try (GoLibrespotRestClient client = newClient(daemon, 1_000, s -> {})) {
            StatusDto dto = client.status().parsed().orElseThrow();
            assertThat(dto.stopped()).isTrue();
            assertThat(dto.track()).isNull();
        } finally {
            daemon.stop();
        }
    }

    @Test
    void status204IsNoSessionSentinel() {
        FakeLibrespotDaemon daemon = start(new FakeLibrespotDaemon().hasSession(false));
        try (GoLibrespotRestClient client = newClient(daemon, 1_000, s -> {})) {
            StatusResult result = client.status();
            assertThat(result.status()).isEqualTo(204);
            assertThat(result.isNoSession()).isTrue();
            assertThat(result.parsed()).isEmpty();
        } finally {
            daemon.stop();
        }
    }

    @Test
    void status200WithUnparseableBodyYieldsEmptyParsed() {
        FakeLibrespotDaemon daemon = start(new FakeLibrespotDaemon().status(Response.ok("not json at all")));
        try (GoLibrespotRestClient client = newClient(daemon, 1_000, s -> {})) {
            StatusResult result = client.status();
            assertThat(result.status()).isEqualTo(200);
            assertThat(result.parsed()).isEmpty();
            assertThat(result.body()).isEqualTo("not json at all");
        } finally {
            daemon.stop();
        }
    }

    @Test
    void statusToleratesUnknownFields() {
        // Forward-compat: unknown fields at any level must never break parsing.
        FakeLibrespotDaemon daemon = start(new FakeLibrespotDaemon().status(Response.ok("""
                {"username":"u","device_id":"d","device_type":"t","device_name":"n","play_origin":"go-librespot",
                 "stopped":false,"paused":true,"buffering":false,"volume":50,"volume_steps":100,
                 "repeat_context":true,"repeat_track":false,"shuffle_context":true,
                 "future_top_level":{"x":[1,2,3]},
                 "track":{"uri":"spotify:track:1","name":"N","artist_names":["A"],
                          "album_name":"AL","album_cover_url":null,"position":0,"duration":1000,
                          "release_date":"","track_number":1,"disc_number":1,"format":"","codec":"vorbis",
                          "bitrate":null,"sample_rate":null,"bit_depth":null,
                          "future_track_field":"ignored"}}""")));
        try (GoLibrespotRestClient client = newClient(daemon, 1_000, s -> {})) {
            StatusDto dto = client.status().parsed().orElseThrow();
            assertThat(dto.username()).isEqualTo("u");
            assertThat(dto.paused()).isTrue();
            assertThat(dto.repeatContext()).isTrue();
            assertThat(dto.track()).isNotNull();
            assertThat(dto.track().artistNames()).containsExactly("A");
            assertThat(dto.track().codec()).isEqualTo("vorbis");
        } finally {
            daemon.stop();
        }
    }

    // ------------------------------------------------------------ root

    @Test
    void playbackReadyReflectsDaemonFlag() {
        FakeLibrespotDaemon daemon = start(new FakeLibrespotDaemon().playbackReady(true));
        try (GoLibrespotRestClient client = newClient(daemon, 1_000, s -> {})) {
            RootResult ready = client.playbackReady();
            assertThat(ready.status()).isEqualTo(200);
            assertThat(ready.isReady()).isTrue();
        } finally {
            daemon.stop();
        }
    }

    @Test
    void playbackReadyFalseWhenNotBootstrapped() {
        FakeLibrespotDaemon daemon = start(new FakeLibrespotDaemon().playbackReady(false));
        try (GoLibrespotRestClient client = newClient(daemon, 1_000, s -> {})) {
            RootResult notReady = client.playbackReady();
            assertThat(notReady.status()).isEqualTo(200);
            assertThat(notReady.isReady()).isFalse();
        } finally {
            daemon.stop();
        }
    }

    // ------------------------------------------------------------ /web-api passthrough

    @Test
    void metadataViaWebApiReturnsRawStatusAndBody() {
        FakeLibrespotDaemon daemon = start(
                new FakeLibrespotDaemon().webApi(Response.of(200, "{\"id\":\"4uLU6hMCjMI75M1A2tKUQC\",\"name\":\"T\"}")));
        try (GoLibrespotRestClient client = newClient(daemon, 1_000, s -> {})) {
            WebApiResult result = client.metadataViaWebApi("v1/tracks/4uLU6hMCjMI75M1A2tKUQC");
            assertThat(result.status()).isEqualTo(200);
            assertThat(result.is2xx()).isTrue();
            assertThat(result.body()).contains("4uLU6hMCjMI75M1A2tKUQC");

            RecordedCommand cmd = single(daemon);
            assertThat(cmd.method()).isEqualTo("GET");
            assertThat(cmd.path()).isEqualTo("/web-api/v1/tracks/4uLU6hMCjMI75M1A2tKUQC");
        } finally {
            daemon.stop();
        }
    }

    @Test
    void metadataViaWebApi404IsTypedFailure() {
        FakeLibrespotDaemon daemon = start(new FakeLibrespotDaemon().webApi(Response.error(404)));
        try (GoLibrespotRestClient client = newClient(daemon, 1_000, s -> {})) {
            WebApiResult result = client.metadataViaWebApi("v1/tracks/missing");
            assertThat(result.status()).isEqualTo(404);
            assertThat(result.isNotFound()).isTrue();
            assertThat(result.isError()).isTrue();
            assertThat(result.isNoSession()).isFalse();
        } finally {
            daemon.stop();
        }
    }

    @Test
    void metadataViaWebApiNoSessionIs204() {
        FakeLibrespotDaemon daemon = start(new FakeLibrespotDaemon().hasSession(false));
        try (GoLibrespotRestClient client = newClient(daemon, 1_000, s -> {})) {
            WebApiResult result = client.metadataViaWebApi("v1/tracks/abc");
            assertThat(result.status()).isEqualTo(204);
            assertThat(result.isNoSession()).isTrue();
        } finally {
            daemon.stop();
        }
    }

    @Test
    void metadataViaWebApiEmpty200IsReturnedRaw() {
        // Upstream non-listed statuses arrive as 200 with empty body — never fabricated.
        FakeLibrespotDaemon daemon = start(new FakeLibrespotDaemon().webApi(Response.ok()));
        try (GoLibrespotRestClient client = newClient(daemon, 1_000, s -> {})) {
            WebApiResult result = client.metadataViaWebApi("v1/tracks/abc");
            assertThat(result.status()).isEqualTo(200);
            assertThat(result.body()).isEmpty();
        } finally {
            daemon.stop();
        }
    }

    // ------------------------------------------------------------ timeouts / I/O / cancellation

    @Test
    void hungRequestFailsWithTypedTimeout() {
        FakeLibrespotDaemon daemon = start(new FakeLibrespotDaemon().hangRest("/player/play"));
        try (GoLibrespotRestClient client = newClient(daemon, 500, s -> {})) {
            long start = System.nanoTime();
            assertThatThrownBy(() -> client.play("spotify:track:abc", 0, false))
                    .isInstanceOf(RestException.class)
                    .satisfies(e -> {
                        RestException re = (RestException) e;
                        assertThat(re.kind()).isEqualTo(RestException.Kind.TIMEOUT);
                        assertThat(re.getMessage()).contains("/player/play");
                    });
            assertThat((System.nanoTime() - start) / 1_000_000).isLessThan(3_000);
        } finally {
            daemon.stop();
        }
    }

    @Test
    void connectionRefusedIsTypedIoError() throws Exception {
        GoLibrespotRestClient client = new GoLibrespotRestClient(
                "http://127.0.0.1:" + unusedPort(), 500, LogSanitizer.defaults(), s -> {});
        try (client) {
            assertThatThrownBy(client::status)
                    .isInstanceOf(RestException.class)
                    .satisfies(e -> assertThat(((RestException) e).kind()).isEqualTo(RestException.Kind.IO));
        }
    }

    @Test
    void cancelingInFlightRequestCompletesExceptionally() {
        FakeLibrespotDaemon daemon = start(new FakeLibrespotDaemon().hangRest("/player/play"));
        try (GoLibrespotRestClient client = newClient(daemon, 10_000, s -> {})) {
            CompletableFuture<PlayerCommandResult> future = client.playAsync("spotify:track:abc", 0, false);
            future.cancel(true);
            assertThat(future).failsWithin(2, TimeUnit.SECONDS);
        } finally {
            daemon.stop();
        }
    }

    @Test
    void closeCancelsOutstandingRequestsAndIsIdempotent() {
        FakeLibrespotDaemon daemon = start(new FakeLibrespotDaemon().hangRest("/player/play"));
        GoLibrespotRestClient client = newClient(daemon, 10_000, s -> {});
        CompletableFuture<PlayerCommandResult> future = client.playAsync("spotify:track:abc", 0, false);
        client.close();
        assertThat(future).failsWithin(2, TimeUnit.SECONDS);
        assertThat(client.isClosed()).isTrue();
        assertThatThrownBy(() -> client.play("spotify:track:abc", 0, false))
                .isInstanceOf(IllegalStateException.class);
        client.close(); // idempotent — no throw
        daemon.stop();
    }

    // ------------------------------------------------------------ config wiring

    @Test
    void fromConfigUsesEffectiveRestTimeoutMsOverride() {
        FakeLibrespotDaemon daemon = start(new FakeLibrespotDaemon().hangRest("/player/play"));
        GoLibrespotConfig config = GoLibrespotConfig.from(Map.of(
                "restTimeoutMs", 300,
                "backends", List.of(Map.of(
                        "name", "gb-1",
                        "restBaseUrl", daemon.getHttpUrl(),
                        "fifoPath", "/tmp/gb-1.fifo"))));
        try (GoLibrespotRestClient client = GoLibrespotRestClient.fromConfig(config, config.getBackends().get(0))) {
            long start = System.nanoTime();
            assertThatThrownBy(() -> client.play("spotify:track:x", 0, false))
                    .isInstanceOf(RestException.class)
                    .satisfies(e -> assertThat(((RestException) e).kind()).isEqualTo(RestException.Kind.TIMEOUT));
            // 300 ms override — must fail fast, nowhere near the 5 s default.
            assertThat((System.nanoTime() - start) / 1_000_000).isLessThan(2_000);
        } finally {
            daemon.stop();
        }
    }

    // ------------------------------------------------------------ async + logging

    @Test
    void asyncMethodsCompleteWithResult() {
        FakeLibrespotDaemon daemon = start(new FakeLibrespotDaemon().play(Response.ok()));
        try (GoLibrespotRestClient client = newClient(daemon, 1_000, s -> {})) {
            CompletableFuture<PlayerCommandResult> future = client.playAsync("spotify:track:abc", 0, false);
            assertThat(future).succeedsWithin(Duration.ofSeconds(2));
            assertThat(future.join().status()).isEqualTo(200);
        } finally {
            daemon.stop();
        }
    }

    @Test
    void logsAreSanitizedTokensNeverEmitted() {
        List<String> logs = new CopyOnWriteArrayList<>();
        FakeLibrespotDaemon daemon = start(new FakeLibrespotDaemon().webApi(Response.of(200, "{}")));
        try (GoLibrespotRestClient client = newClient(daemon, 1_000, logs::add)) {
            client.metadataViaWebApi("v1/tracks/abc?code=TOPSECRET&token=OTHER");
            assertThat(logs).anyMatch(line -> line.contains("/web-api/v1/tracks/abc"));
            assertThat(logs).noneMatch(line -> line.contains("TOPSECRET"));
            assertThat(logs).noneMatch(line -> line.contains("OTHER"));
        } finally {
            daemon.stop();
        }
    }

    // ------------------------------------------------------------ result-type semantics (no fixture)

    @Test
    void resultTypesExposeRawStatusSemantics() {
        assertThat(PlayerCommandResult.of(204, null).isNoSession()).isTrue();
        assertThat(PlayerCommandResult.of(204, null).body()).isEmpty();
        assertThat(PlayerCommandResult.of(500, "boom").isError()).isTrue();
        assertThat(StatusResult.of(204, null).isNoSession()).isTrue();
        assertThat(StatusResult.of(204, null).parsed()).isEmpty();
        assertThat(StatusResult.of(200, "garbage").parsed()).isEmpty();
        assertThat(RootResult.of(200, "{\"playback_ready\":true}").isReady()).isTrue();
        assertThat(RootResult.of(200, "garbage").isReady()).isFalse();
        assertThat(RootResult.of(500, "").isReady()).isFalse();
        assertThat(WebApiResult.of(404, null).isNotFound()).isTrue();
        assertThat(WebApiResult.of(403, null).isForbidden()).isTrue();
        assertThat(WebApiResult.of(429, null).isRateLimited()).isTrue();
        assertThat(WebApiResult.of(401, null).isUnauthorized()).isTrue();
        assertThat(WebApiResult.of(404, null).isNoSession()).isFalse();
    }
}
