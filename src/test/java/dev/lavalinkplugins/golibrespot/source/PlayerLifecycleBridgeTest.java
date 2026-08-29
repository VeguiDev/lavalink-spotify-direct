package dev.lavalinkplugins.golibrespot.source;

import static org.assertj.core.api.Assertions.assertThat;

import com.sedmelluq.discord.lavaplayer.filter.PcmFilterFactory;
import com.sedmelluq.discord.lavaplayer.player.AudioPlayer;
import com.sedmelluq.discord.lavaplayer.player.event.AudioEvent;
import com.sedmelluq.discord.lavaplayer.player.event.PlayerPauseEvent;
import com.sedmelluq.discord.lavaplayer.player.event.PlayerResumeEvent;
import com.sedmelluq.discord.lavaplayer.player.event.TrackEndEvent;
import com.sedmelluq.discord.lavaplayer.player.event.TrackExceptionEvent;
import com.sedmelluq.discord.lavaplayer.player.event.TrackStartEvent;
import com.sedmelluq.discord.lavaplayer.player.event.TrackStuckEvent;
import com.sedmelluq.discord.lavaplayer.tools.FriendlyException;
import com.sedmelluq.discord.lavaplayer.tools.FriendlyException.Severity;
import com.sedmelluq.discord.lavaplayer.track.AudioTrack;
import com.sedmelluq.discord.lavaplayer.track.AudioTrackEndReason;
import com.sedmelluq.discord.lavaplayer.track.AudioTrackInfo;
import com.sedmelluq.discord.lavaplayer.track.AudioTrackState;
import com.sedmelluq.discord.lavaplayer.track.TrackMarker;
import com.sedmelluq.discord.lavaplayer.track.playback.AudioFrame;
import com.sedmelluq.discord.lavaplayer.track.playback.MutableAudioFrame;
import dev.lavalinkplugins.golibrespot.config.GoLibrespotConfig;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Result;
import dev.lavalinkplugins.golibrespot.metadata.MetadataResolver;
import dev.lavalinkplugins.golibrespot.metadata.MetadataResolver.ReadyBackend;
import dev.lavalinkplugins.golibrespot.pool.ExclusivePool;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Contract + acceptance tests for {@link PlayerLifecycleBridge} (T18): the
 * per-player {@code AudioEventAdapter} routes Lavaplayer player events to the
 * spdirect lifecycle coordinator / stop sequence — exactly once, async, and
 * ONLY for tracks owned by this plugin.
 *
 * <p>Mappings (DECISIONS.md): onTrackStart → coordinator.start; onPlayerPause /
 * onPlayerResume → remote pause / resume (machine, bounded); onTrackEnd STOPPED
 * → logicalStop, REPLACED → replace (play-over-play) with the new spdirect
 * track, CLEANUP → destroy, FINISHED → nothing (natural completion already
 * released); onTrackException / onTrackStuck → quarantine. Events carrying a
 * NON-GoLibrespotAudioTrack (LavaSrc, YouTube, …) are completely ignored, and
 * detach removes the listener from the player.</p>
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class PlayerLifecycleBridgeTest {

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

  // ---------------------------------------------------------------- track start

  @Test
  void trackStartCallsCoordinatorStartWithDaemonUri() {
    try (Rig rig = newRig()) {
      rig.fire(new TrackStartEvent(rig.player, rig.trackA));

      assertThat(rig.coordinatorA.starts)
          .containsExactly(new String[] {"spotify:track:" + TRACK_ID, "0"});
    }
  }

  // ---------------------------------------------------------------- pause / resume

  @Test
  void playerPauseCallsRemotePause() {
    try (Rig rig = newRig()) {
      rig.player.playingTrack = rig.trackA;
      rig.fire(new PlayerPauseEvent(rig.player));
      assertThat(rig.coordinatorA.pauses).hasSize(1);
      assertThat(rig.coordinatorA.resumes).isEmpty();
    }
  }

  @Test
  void playerResumeCallsRemoteResume() {
    try (Rig rig = newRig()) {
      rig.player.playingTrack = rig.trackA;
      rig.fire(new PlayerResumeEvent(rig.player));
      assertThat(rig.coordinatorA.resumes).hasSize(1);
    }
  }

  @Test
  void pauseIsIgnoredWhenNoActiveSpdirectSession() {
    try (Rig rig = newRig()) {
      rig.player.playingTrack = rig.trackA;
      rig.coordinatorA.active = false;
      rig.fire(new PlayerPauseEvent(rig.player));
      assertThat(rig.coordinatorA.pauses).isEmpty();
    }
  }

  // ---------------------------------------------------------------- track end

  @Test
  void trackEndStoppedCallsLogicalStop() {
    try (Rig rig = newRig()) {
      rig.fire(new TrackEndEvent(rig.player, rig.trackA, AudioTrackEndReason.STOPPED));
      assertThat(rig.coordinatorA.logicalStops).hasSize(1);
      assertThat(rig.coordinatorA.destroys).isEmpty();
    }
  }

  @Test
  void trackEndStoppedIsIgnoredWhenSessionNotActive() {
    try (Rig rig = newRig()) {
      rig.coordinatorA.active = false;
      rig.fire(new TrackEndEvent(rig.player, rig.trackA, AudioTrackEndReason.STOPPED));
      assertThat(rig.coordinatorA.logicalStops).isEmpty();
    }
  }

  @Test
  void trackEndReplacedCallsReplaceAndReusesTheCoordinatorForTheNewTrack() {
    try (Rig rig = newRig()) {
      rig.player.playingTrack = rig.trackB;

      rig.fire(new TrackEndEvent(rig.player, rig.trackA, AudioTrackEndReason.REPLACED));

      assertThat(rig.coordinatorA.replaces)
          .containsExactly(new String[] {"spotify:track:" + TRACK_ID_B, "0"});
      // play-over-play on the HELD lease: the new track must reuse the old coordinator
      assertThat(rig.trackB.playbackCoordinator()).isSameAs(rig.coordinatorA);
      assertThat(rig.coordinatorA.logicalStops).isEmpty();
      assertThat(rig.coordinatorA.destroys).isEmpty();
    }
  }

  @Test
  void trackEndReplacedToForeignTrackRetiresTheSession() {
    try (Rig rig = newRig()) {
      rig.player.playingTrack = rig.foreignTrack;

      rig.fire(new TrackEndEvent(rig.player, rig.trackA, AudioTrackEndReason.REPLACED));

      assertThat(rig.coordinatorA.replaces).isEmpty();
      assertThat(rig.coordinatorA.logicalStops).hasSize(1);
    }
  }

  @Test
  void trackEndCleanupCallsDestroy() {
    try (Rig rig = newRig()) {
      rig.fire(new TrackEndEvent(rig.player, rig.trackA, AudioTrackEndReason.CLEANUP));
      assertThat(rig.coordinatorA.destroys).hasSize(1);
    }
  }

  @Test
  void trackEndFinishedRetiresUnexpectedlyActiveSession() {
    try (Rig rig = newRig()) {
      rig.fire(new TrackEndEvent(rig.player, rig.trackA, AudioTrackEndReason.FINISHED));
      assertThat(rig.coordinatorA.starts).isEmpty();
      assertThat(rig.coordinatorA.replaces).isEmpty();
      assertThat(rig.coordinatorA.logicalStops).hasSize(1);
      assertThat(rig.coordinatorA.destroys).isEmpty();
      assertThat(rig.coordinatorA.pauses).isEmpty();
      assertThat(rig.coordinatorA.resumes).isEmpty();
      assertThat(rig.coordinatorA.quarantines).isEmpty();
    }
  }

  // ---------------------------------------------------------------- exception / stuck

  @Test
  void trackExceptionQuarantinesTheBackend() {
    try (Rig rig = newRig()) {
      FriendlyException ex = new FriendlyException("boom", Severity.COMMON, null);
      rig.fire(new TrackExceptionEvent(rig.player, rig.trackA, ex));
      assertThat(rig.coordinatorA.quarantines).hasSize(1);
      assertThat(rig.coordinatorA.quarantines.get(0)).contains("boom");
    }
  }

  @Test
  void trackStuckQuarantinesTheBackend() {
    try (Rig rig = newRig()) {
      rig.fire(new TrackStuckEvent(rig.player, rig.trackA, 1_000, new StackTraceElement[0]));
      assertThat(rig.coordinatorA.quarantines).hasSize(1);
    }
  }

  // ---------------------------------------------------------------- foreign tracks

  @Test
  void eventsForForeignTracksAreCompletelyIgnored() {
    try (Rig rig = newRig()) {
      rig.player.playingTrack = rig.foreignTrack;

      rig.fire(new TrackStartEvent(rig.player, rig.foreignTrack));
      rig.fire(new PlayerPauseEvent(rig.player));
      rig.fire(new PlayerResumeEvent(rig.player));
      rig.fire(new TrackEndEvent(rig.player, rig.foreignTrack, AudioTrackEndReason.STOPPED));
      rig.fire(new TrackEndEvent(rig.player, rig.foreignTrack, AudioTrackEndReason.REPLACED));
      rig.fire(new TrackEndEvent(rig.player, rig.foreignTrack, AudioTrackEndReason.CLEANUP));
      rig.fire(new TrackEndEvent(rig.player, rig.foreignTrack, AudioTrackEndReason.FINISHED));
      rig.fire(new TrackExceptionEvent(
          rig.player, rig.foreignTrack, new FriendlyException("x", Severity.COMMON, null)));
      rig.fire(new TrackStuckEvent(rig.player, rig.foreignTrack, 1_000, new StackTraceElement[0]));

      assertThat(rig.coordinatorA.starts).isEmpty();
      assertThat(rig.coordinatorA.replaces).isEmpty();
      assertThat(rig.coordinatorA.logicalStops).isEmpty();
      assertThat(rig.coordinatorA.destroys).isEmpty();
      assertThat(rig.coordinatorA.pauses).isEmpty();
      assertThat(rig.coordinatorA.resumes).isEmpty();
      assertThat(rig.coordinatorA.quarantines).isEmpty();
      assertThat(rig.coordinatorB.starts).isEmpty();
      assertThat(rig.coordinatorB.replaces).isEmpty();
      assertThat(rig.coordinatorB.logicalStops).isEmpty();
      assertThat(rig.coordinatorB.destroys).isEmpty();
      assertThat(rig.coordinatorB.pauses).isEmpty();
      assertThat(rig.coordinatorB.resumes).isEmpty();
      assertThat(rig.coordinatorB.quarantines).isEmpty();
    }
  }

  // ---------------------------------------------------------------- attach / detach

  @Test
  void detachRemovesTheListenerSoEventsStopArriving() {
    try (Rig rig = newRig()) {
      rig.player.removeListener(rig.bridge); // detach

      rig.fire(new TrackStartEvent(rig.player, rig.trackA));
      rig.fire(new PlayerPauseEvent(rig.player));
      rig.fire(new TrackEndEvent(rig.player, rig.trackA, AudioTrackEndReason.STOPPED));

      assertThat(rig.coordinatorA.starts).isEmpty();
      assertThat(rig.coordinatorA.pauses).isEmpty();
      assertThat(rig.coordinatorA.logicalStops).isEmpty();
    }
  }

  // ---------------------------------------------------------------- rig

  private Rig newRig() {
    Rig rig = new Rig();
    rig.start();
    rigs.add(rig);
    return rig;
  }

  static final class Rig implements AutoCloseable {

    final FakeAudioPlayer player = new FakeAudioPlayer();
    final PlayerLifecycleBridge bridge = new PlayerLifecycleBridge();
    final FakeCoordinator coordinatorA = new FakeCoordinator();
    final FakeCoordinator coordinatorB = new FakeCoordinator();

    GoLibrespotConfig config;
    ExclusivePool pool;
    MetadataResolver resolver;
    GoLibrespotAudioSourceManager manager;
    GoLibrespotAudioTrack trackA;
    GoLibrespotAudioTrack trackB;
    ForeignTrack foreignTrack = new ForeignTrack("https://www.youtube.com/watch?v=abc123");

    void start() {
      try {
        config = GoLibrespotConfig.from(Map.of(
            "enabled", true,
            "backends", List.of(Map.of(
                "name", "alpha",
                "restBaseUrl", "http://127.0.0.1:1",
                "wsUrl", "ws://127.0.0.1:1/events",
                "fifoPath", "C:/tmp/alpha.fifo"))));
        pool = new ExclusivePool(config.getBackends());
        resolver = new MetadataResolver(() -> List.of(new ReadyBackend("http://127.0.0.1:1")), 1500);
        manager = new GoLibrespotAudioSourceManager(pool, resolver, (h, c) -> coordinatorA);

        trackA = spdirectTrack(TRACK_ID, coordinatorA);
        trackB = spdirectTrack(TRACK_ID_B, coordinatorB);
        player.addListener(bridge);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private GoLibrespotAudioTrack spdirectTrack(String id, FakeCoordinator coordinator) {
      AudioTrackInfo info = new AudioTrackInfo(
          "title", "artist", 3_040_000L, "spdirect:" + id, true, null, null, null);
      GoLibrespotAudioTrack track = new GoLibrespotAudioTrack(id, info, manager);
      track.setPlaybackCoordinator(coordinator);
      return track;
    }

    void fire(AudioEvent event) {
      player.dispatch(event);
    }

    @Override
    public void close() {
      try {
        player.removeListener(bridge);
      } finally {
        manager.shutdown();
        pool.shutdown();
      }
    }
  }

  /** Records every routed call; never blocks (the bridge contract is async lanes). */
  static final class FakeCoordinator implements PlaybackCoordinator {

    final List<String[]> starts = new CopyOnWriteArrayList<>();
    final List<String[]> replaces = new CopyOnWriteArrayList<>();
    final List<String> logicalStops = new CopyOnWriteArrayList<>();
    final List<String> destroys = new CopyOnWriteArrayList<>();
    final List<String> pauses = new CopyOnWriteArrayList<>();
    final List<String> resumes = new CopyOnWriteArrayList<>();
    final List<String> quarantines = new CopyOnWriteArrayList<>();
    volatile boolean active = true;

    @Override
    public CompletableFuture<Result> start(String uri, long positionMs) {
      starts.add(new String[] {uri, String.valueOf(positionMs)});
      return CompletableFuture.completedFuture(Result.ok("fake"));
    }

    @Override
    public CompletableFuture<Result> replace(String uri, long positionMs) {
      replaces.add(new String[] {uri, String.valueOf(positionMs)});
      return CompletableFuture.completedFuture(Result.ok("fake"));
    }

    @Override
    public CompletableFuture<Result> logicalStop() {
      logicalStops.add("stop");
      return CompletableFuture.completedFuture(Result.ok("fake"));
    }

    @Override
    public CompletableFuture<Result> destroy() {
      destroys.add("destroy");
      return CompletableFuture.completedFuture(Result.ok("fake"));
    }

    @Override
    public CompletableFuture<Result> pauseRemote() {
      pauses.add("pause");
      return CompletableFuture.completedFuture(Result.ok("fake"));
    }

    @Override
    public CompletableFuture<Result> resumeRemote() {
      resumes.add("resume");
      return CompletableFuture.completedFuture(Result.ok("fake"));
    }

    @Override
    public CompletableFuture<Result> quarantine(String reason) {
      quarantines.add(reason);
      return CompletableFuture.completedFuture(Result.ok("fake"));
    }

    @Override
    public void awaitActivated(Duration timeout) {
      // never reached via the bridge
    }

    @Override
    public short[] nextFrame(Duration timeout) {
      return new short[0];
    }

    @Override
    public Result seek(long positionMs) {
      return Result.ok("fake");
    }

    @Override
    public boolean isActive() {
      return active;
    }

    @Override
    public String expectedUri() {
      return null;
    }

    @Override
    public long positionMs() {
      return 0;
    }

    @Override
    public long generation() {
      return 1;
    }
  }

  /** Minimal AudioPlayer the bridge attaches to. */
  static final class FakeAudioPlayer implements AudioPlayer {

    final List<com.sedmelluq.discord.lavaplayer.player.event.AudioEventListener> listeners =
        new ArrayList<>();
    volatile AudioTrack playingTrack;

    @Override
    public AudioTrack getPlayingTrack() {
      return playingTrack;
    }

    @Override
    public void addListener(com.sedmelluq.discord.lavaplayer.player.event.AudioEventListener listener) {
      listeners.add(listener);
    }

    @Override
    public void removeListener(com.sedmelluq.discord.lavaplayer.player.event.AudioEventListener listener) {
      listeners.remove(listener);
    }

    /** Dispatches an event to every registered listener (mirrors Lavaplayer). */
    void dispatch(com.sedmelluq.discord.lavaplayer.player.event.AudioEvent event) {
      for (com.sedmelluq.discord.lavaplayer.player.event.AudioEventListener listener : listeners) {
        listener.onEvent(event);
      }
    }

    @Override
    public void playTrack(AudioTrack track) {
      playingTrack = track;
    }

    @Override
    public boolean startTrack(AudioTrack track, boolean noInterrupt) {
      playingTrack = track;
      return true;
    }

    @Override
    public void stopTrack() {
      playingTrack = null;
    }

    @Override
    public int getVolume() {
      return 100;
    }

    @Override
    public void setVolume(int volume) {
      // no-op
    }

    @Override
    public void setFilterFactory(PcmFilterFactory factory) {
      // no-op
    }

    @Override
    public void setFrameBufferDuration(Integer duration) {
      // no-op
    }

    @Override
    public boolean isPaused() {
      return false;
    }

    @Override
    public void setPaused(boolean paused) {
      // no-op
    }

    @Override
    public void destroy() {
      listeners.clear();
    }

    @Override
    public void checkCleanup(long threshold) {
      // no-op
    }

    @Override
    public AudioFrame provide() {
      return null;
    }

    @Override
    public AudioFrame provide(long timeout, TimeUnit unit) {
      return null;
    }

    @Override
    public boolean provide(MutableAudioFrame targetFrame) {
      return false;
    }

    @Override
    public boolean provide(MutableAudioFrame targetFrame, long timeout, TimeUnit unit) {
      return false;
    }
  }

  /** A non-spdirect track (stands in for LavaSrc / YouTube tracks). */
  static final class ForeignTrack implements AudioTrack {

    private final String identifier;

    ForeignTrack(String identifier) {
      this.identifier = identifier;
    }

    @Override
    public AudioTrackInfo getInfo() {
      return null;
    }

    @Override
    public String getIdentifier() {
      return identifier;
    }

    @Override
    public AudioTrackState getState() {
      return AudioTrackState.INACTIVE;
    }

    @Override
    public void stop() {
      // no-op
    }

    @Override
    public boolean isSeekable() {
      return true;
    }

    @Override
    public long getPosition() {
      return 0;
    }

    @Override
    public void setPosition(long position) {
      // no-op
    }

    @Override
    public void setMarker(TrackMarker marker) {
      // no-op
    }

    @Override
    public void addMarker(TrackMarker marker) {
      // no-op
    }

    @Override
    public void removeMarker(TrackMarker marker) {
      // no-op
    }

    @Override
    public long getDuration() {
      return 0;
    }

    @Override
    public AudioTrack makeClone() {
      return new ForeignTrack(identifier);
    }

    @Override
    public com.sedmelluq.discord.lavaplayer.source.AudioSourceManager getSourceManager() {
      return null;
    }

    @Override
    public void setUserData(Object data) {
      // no-op
    }

    @Override
    public Object getUserData() {
      return null;
    }

    @Override
    public <T> T getUserData(Class<T> klass) {
      return null;
    }
  }
}
