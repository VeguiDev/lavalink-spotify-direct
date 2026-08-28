package dev.lavalinkplugins.golibrespot.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sedmelluq.discord.lavaplayer.track.AudioReference;
import com.sedmelluq.discord.lavaplayer.track.AudioTrackInfo;
import dev.lavalinkplugins.golibrespot.config.GoLibrespotConfig;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine;
import dev.lavalinkplugins.golibrespot.metadata.MetadataResolver;
import dev.lavalinkplugins.golibrespot.metadata.MetadataResolver.ReadyBackend;
import dev.lavalinkplugins.golibrespot.metadata.MetadataResolver.ReadyBackendSelector;
import dev.lavalinkplugins.golibrespot.pool.BackendState;
import dev.lavalinkplugins.golibrespot.pool.ExclusivePool;
import dev.lavalinkplugins.golibrespot.testfixtures.FakeLibrespotDaemon;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.util.concurrent.TimeUnit;

/**
 * Contract + acceptance tests for {@link GoLibrespotAudioSourceManager} (T18).
 *
 * <p>loadItem claims EXACTLY the spdirect namespace (bare {@code spdirect:<id>}
 * and {@code spdirect:spotify:track:<id>}), resolving metadata lease-free via
 * the T10 {@link MetadataResolver} against the scripted {@link FakeLibrespotDaemon}
 * {@code /web-api} passthrough. Everything else — ordinary Spotify URIs/URLs,
 * albums/playlists, and structurally malformed spdirect input — returns
 * {@code null} so Lavalink falls through to other sources, and NEVER issues a
 * REST command beyond the metadata fetch. A metadata failure (empty Optional)
 * is a clear load failure (null) — duration/metadata are never fabricated.</p>
 *
 * <p>The pool is only ever present; loadItem must NOT acquire a lease or touch
 * the daemon beyond {@code GET /web-api/v1/tracks/{id}} (asserted via the
 * fixture's recorded commands + pool state). encode/decode are unsupported
 * (isTrackEncodable false, UnsupportedOperationException), getSourceName is
 * {@code spdirect}, and shutdown closes the coordinator factory.</p>
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class GoLibrespotAudioSourceManagerTest {

  private static final String TRACK_ID = "4iV5W9uYEdYUVa79Axb7Rh";
  private static final String TRACK_ID_B = "0jv2SxQk2Tz29TlCVODMW7";

  private final List<Rig> rigs = new ArrayList<>();

  @AfterEach
  void tearDown() {
    for (Rig rig : rigs) {
      rig.close();
    }
    rigs.clear();
  }

  // ---------------------------------------------------------------- claiming

  @Test
  void getSourceNameIsSpdirect() {
    try (Rig rig = newRig()) {
      assertThat(rig.manager.getSourceName()).isEqualTo("spdirect");
    }
  }

  @Test
  void loadItemClaimsBareSpdirectId() {
    try (Rig rig = newRig()) {
      rig.daemon.webApi(FakeLibrespotDaemon.Response.ok(webApiTrackJson(TRACK_ID, 3_040_000L)));

      Object item = rig.manager.loadItem(null, new AudioReference("spdirect:" + TRACK_ID, null));

      assertThat(item).isInstanceOf(GoLibrespotAudioTrack.class);
      GoLibrespotAudioTrack track = (GoLibrespotAudioTrack) item;
      assertThat(track.trackId()).isEqualTo(TRACK_ID);
      assertThat(track.daemonUri()).isEqualTo("spotify:track:" + TRACK_ID);
      AudioTrackInfo info = track.getInfo();
      assertThat(info.identifier).isEqualTo("spdirect:" + TRACK_ID);
      assertThat(info.title).isEqualTo("夜に駆ける");
      assertThat(info.author).isEqualTo("YOASOBI");
      assertThat(info.length).isEqualTo(3_040_000L);
      // only the metadata fetch was issued — no play/pause/stop/seek, no acquire
      assertThat(rig.daemon.getReceivedCommands())
          .extracting(FakeLibrespotDaemon.RecordedCommand::path)
          .containsExactly("/web-api/v1/tracks/" + TRACK_ID);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY);
    }
  }

  @Test
  void loadItemClaimsSpdirectSpotifyTrackPrefixForm() {
    try (Rig rig = newRig()) {
      rig.daemon.webApi(FakeLibrespotDaemon.Response.ok(webApiTrackJson(TRACK_ID_B, 252_000L)));

      Object item = rig.manager.loadItem(
          null, new AudioReference("spdirect:spotify:track:" + TRACK_ID_B, null));

      assertThat(item).isInstanceOf(GoLibrespotAudioTrack.class);
      assertThat(((GoLibrespotAudioTrack) item).trackId()).isEqualTo(TRACK_ID_B);
      assertThat(rig.daemon.getReceivedCommands())
          .extracting(FakeLibrespotDaemon.RecordedCommand::path)
          .containsExactly("/web-api/v1/tracks/" + TRACK_ID_B);
    }
  }

  // ---------------------------------------------------------------- non-claiming

  @Test
  void loadItemReturnsNullForPlainSpotifyTrackUri() {
    try (Rig rig = newRig()) {
      Object item = rig.manager.loadItem(
          null, new AudioReference("spotify:track:" + TRACK_ID, null));

      assertThat(item).isNull();
      assertThat(rig.daemon.getReceivedCommands()).isEmpty();
    }
  }

  @Test
  void loadItemReturnsNullForOpenSpotifyTrackUrl() {
    try (Rig rig = newRig()) {
      Object item = rig.manager.loadItem(
          null, new AudioReference("https://open.spotify.com/track/" + TRACK_ID, null));

      assertThat(item).isNull();
      assertThat(rig.daemon.getReceivedCommands()).isEmpty();
    }
  }

  @Test
  void loadItemReturnsNullForAlbumAndPlaylistUris() {
    try (Rig rig = newRig()) {
      assertThat(rig.manager.loadItem(null, new AudioReference("spotify:album:aaaaaaaaaaaaaaaaaaaaaa", null)))
          .isNull();
      assertThat(rig.manager.loadItem(null, new AudioReference("spotify:playlist:aaaaaaaaaaaaaaaaaaaaaa", null)))
          .isNull();
      assertThat(rig.manager.loadItem(null, new AudioReference("spdirect:spotify:album:aaaaaaaaaaaaaaaaaaaaaa", null)))
          .isNull();
      assertThat(rig.daemon.getReceivedCommands()).isEmpty();
    }
  }

  @Test
  void loadItemReturnsNullForMalformedSpdirectIdentifier() {
    try (Rig rig = newRig()) {
      // wrong length / invalid charset after the spdirect: prefix — structurally
      // malformed, never claimed, never sent to metadata.
      assertThat(rig.manager.loadItem(null, new AudioReference("spdirect:short", null))).isNull();
      assertThat(rig.manager.loadItem(null, new AudioReference("spdirect:!!!!", null))).isNull();
      assertThat(rig.manager.loadItem(null, new AudioReference("spdirect:", null))).isNull();
      assertThat(rig.daemon.getReceivedCommands()).isEmpty();
    }
  }

  @Test
  void loadItemReturnsNullForNullAndBlankIdentifiers() {
    try (Rig rig = newRig()) {
      assertThat(rig.manager.loadItem(null, new AudioReference(null, null))).isNull();
      assertThat(rig.manager.loadItem(null, new AudioReference("", null))).isNull();
      assertThat(rig.daemon.getReceivedCommands()).isEmpty();
    }
  }

  // ---------------------------------------------------------------- metadata failure

  @Test
  void loadItemReturnsNullWhenMetadataUnavailableWithoutFabricatingDuration() {
    try (Rig rig = newRig()) {
      // daemon has no session → 204 no-session → resolver empty → clear null load
      rig.daemon.webApi(FakeLibrespotDaemon.Response.noContent());

      assertThat(rig.manager.loadItem(null, new AudioReference("spdirect:" + TRACK_ID, null)))
          .isNull();
      assertThat(rig.daemon.getReceivedCommands())
          .extracting(FakeLibrespotDaemon.RecordedCommand::path)
          .containsExactly("/web-api/v1/tracks/" + TRACK_ID);
    }
  }

  // ---------------------------------------------------------------- encode/decode

  @Test
  void tracksAreNotEncodable() {
    try (Rig rig = newRig()) {
      rig.daemon.webApi(FakeLibrespotDaemon.Response.ok(webApiTrackJson(TRACK_ID, 3_040_000L)));
      GoLibrespotAudioTrack track = (GoLibrespotAudioTrack) rig.manager.loadItem(
          null, new AudioReference("spdirect:" + TRACK_ID, null));

      assertThat(track).isNotNull();
      assertThat(rig.manager.isTrackEncodable(track)).isFalse();
    }
  }

  @Test
  void encodeTrackThrowsUnsupportedOperationException() {
    try (Rig rig = newRig()) {
      rig.daemon.webApi(FakeLibrespotDaemon.Response.ok(webApiTrackJson(TRACK_ID, 3_040_000L)));
      GoLibrespotAudioTrack track = (GoLibrespotAudioTrack) rig.manager.loadItem(
          null, new AudioReference("spdirect:" + TRACK_ID, null));

      assertThatThrownBy(() -> rig.manager.encodeTrack(track, null))
          .isInstanceOf(UnsupportedOperationException.class);
    }
  }

  @Test
  void decodeTrackThrowsUnsupportedOperationException() {
    try (Rig rig = newRig()) {
      assertThatThrownBy(() -> rig.manager.decodeTrack(null, null))
          .isInstanceOf(UnsupportedOperationException.class);
    }
  }

  // ---------------------------------------------------------------- lifecycle

  @Test
  void loadItemNeverAcquiresAPoolLease() {
    try (Rig rig = newRig()) {
      rig.daemon.webApi(FakeLibrespotDaemon.Response.ok(webApiTrackJson(TRACK_ID, 3_040_000L)));

      rig.manager.loadItem(null, new AudioReference("spdirect:" + TRACK_ID, null));

      // the backend stays READY — no lease was taken, and only the metadata GET was issued
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY);
      assertThat(rig.daemon.getReceivedCommands())
          .extracting(FakeLibrespotDaemon.RecordedCommand::method)
          .containsExactly("GET");
    }
  }

  @Test
  void shutdownClosesTheCoordinatorFactory() {
    try (Rig rig = newRig()) {
      rig.manager.shutdown();
      assertThat(rig.factory.closed.get()).isTrue();
    }
  }

  @Test
  void loadItemReturnsNullAfterShutdown() {
    try (Rig rig = newRig()) {
      rig.manager.shutdown();
      assertThat(rig.manager.loadItem(null, new AudioReference("spdirect:" + TRACK_ID, null)))
          .isNull();
    }
  }

  // ---------------------------------------------------------------- rig

  private Rig newRig() {
    Rig rig = new Rig();
    rig.start();
    rigs.add(rig);
    return rig;
  }

  /** Web API {@code GET /web-api/v1/tracks/{id}} body (T10 resolver shape). */
  private static String webApiTrackJson(String id, long durationMs) {
    return "{\"name\":\"夜に駆ける\","
        + "\"artists\":[{\"name\":\"YOASOBI\"}],"
        + "\"album\":{\"name\":\"THE BOOK\",\"images\":[{\"url\":\"https://example.com/art.jpg\"}]},"
        + "\"duration_ms\":" + durationMs + ","
        + "\"external_ids\":{\"isrc\":\"JPU901100390\"},"
        + "\"id\":" + FakeLibrespotDaemon.jsonString(id) + "}";
  }

  /**
   * ONE-SHOT selector: {@link MetadataResolver#resolve} walks the selector until
   * exhausted — an ever-present backend would make an empty (failed) resolution
   * retry the same backend forever (the resolver deliberately has no
   * per-call attempt cap). This mirrors the finite selector T10's tests use.
   */
  private static ReadyBackendSelector oneShotSelector(ReadyBackend backend) {
    return new ReadyBackendSelector() {
      private boolean used;

      @Override
      public Optional<ReadyBackend> next() {
        if (used) {
          return Optional.empty();
        }
        used = true;
        return Optional.of(backend);
      }
    };
  }

  /** Manager under test: fixture daemon (scripted web-api) + real pool + real resolver + fake factory. */
  static final class Rig implements AutoCloseable {

    final FakeLibrespotDaemon daemon = new FakeLibrespotDaemon();
    final RecordingCoordinatorFactory factory = new RecordingCoordinatorFactory();
    final List<String> logLines = Collections.synchronizedList(new ArrayList<>());

    GoLibrespotConfig config;
    ExclusivePool pool;
    MetadataResolver resolver;
    GoLibrespotAudioSourceManager manager;

    void start() {
      try {
        daemon.start();
        config = GoLibrespotConfig.from(Map.of(
            "enabled", true,
            "backends", List.of(Map.of(
                "name", "alpha",
                "restBaseUrl", daemon.getHttpUrl(),
                "wsUrl", daemon.getWsUrl(),
                "fifoPath", "C:/tmp/alpha.fifo"))));
        pool = new ExclusivePool(config.getBackends());
        resolver = new MetadataResolver(
            oneShotSelector(new ReadyBackend(daemon.getHttpUrl())), 1500);
        manager = new GoLibrespotAudioSourceManager(pool, resolver, factory);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {
      try {
        manager.shutdown();
      } finally {
        try {
          pool.shutdown();
        } finally {
          daemon.close();
        }
      }
    }
  }

  /** Factory seam recording create/close; created coordinators are inert stubs. */
  static final class RecordingCoordinatorFactory implements CoordinatorFactory {

    final List<Object> created = new ArrayList<>();
    final AtomicBoolean closed = new AtomicBoolean();

    @Override
    public PlaybackCoordinator create(
        dev.lavalinkplugins.golibrespot.pool.BackendHandle handle,
        dev.lavalinkplugins.golibrespot.config.BackendConfig config) {
      created.add(handle.getBackendId());
      return new InertCoordinator();
    }

    @Override
    public void close() {
      closed.set(true);
    }
  }

  /** Inert seam stub — the manager tests never reach play time. */
  static final class InertCoordinator implements PlaybackCoordinator {
    @Override public CompletableFuture<BackendStateMachine.Result> start(String uri, long positionMs) {
      return CompletableFuture.completedFuture(BackendStateMachine.Result.ok("inert"));
    }
    @Override public CompletableFuture<BackendStateMachine.Result> replace(String uri, long positionMs) {
      return CompletableFuture.completedFuture(BackendStateMachine.Result.ok("inert"));
    }
    @Override public CompletableFuture<BackendStateMachine.Result> logicalStop() {
      return CompletableFuture.completedFuture(BackendStateMachine.Result.ok("inert"));
    }
    @Override public CompletableFuture<BackendStateMachine.Result> destroy() {
      return CompletableFuture.completedFuture(BackendStateMachine.Result.ok("inert"));
    }
    @Override public CompletableFuture<BackendStateMachine.Result> pauseRemote() {
      return CompletableFuture.completedFuture(BackendStateMachine.Result.ok("inert"));
    }
    @Override public CompletableFuture<BackendStateMachine.Result> resumeRemote() {
      return CompletableFuture.completedFuture(BackendStateMachine.Result.ok("inert"));
    }
    @Override public CompletableFuture<BackendStateMachine.Result> quarantine(String reason) {
      return CompletableFuture.completedFuture(BackendStateMachine.Result.ok("inert"));
    }
    @Override public void awaitActivated(java.time.Duration timeout) {}
    @Override public short[] nextFrame(java.time.Duration timeout) {
      return new short[0];
    }
    @Override public BackendStateMachine.Result seek(long positionMs) {
      return BackendStateMachine.Result.ok("inert");
    }
    @Override public boolean isActive() {
      return false;
    }
    @Override public String expectedUri() {
      return null;
    }
    @Override public long positionMs() {
      return 0;
    }
    @Override public long generation() {
      return 0;
    }
  }
}
