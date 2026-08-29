package dev.lavalinkplugins.golibrespot.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sedmelluq.discord.lavaplayer.filter.AudioPipeline;
import com.sedmelluq.discord.lavaplayer.filter.AudioPipelineFactory;
import com.sedmelluq.discord.lavaplayer.filter.PcmFormat;
import com.sedmelluq.discord.lavaplayer.format.StandardAudioDataFormats;
import com.sedmelluq.discord.lavaplayer.player.AudioConfiguration;
import com.sedmelluq.discord.lavaplayer.player.AudioPlayerOptions;
import com.sedmelluq.discord.lavaplayer.track.AudioTrackInfo;
import com.sedmelluq.discord.lavaplayer.track.TrackStateListener;
import com.sedmelluq.discord.lavaplayer.track.playback.AudioProcessingContext;
import com.sedmelluq.discord.lavaplayer.track.playback.LocalAudioTrackExecutor;
import dev.lavalinkplugins.golibrespot.config.GoLibrespotConfig;
import dev.lavalinkplugins.golibrespot.lifecycle.ActivationException;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Result;
import dev.lavalinkplugins.golibrespot.metadata.MetadataResolver;
import dev.lavalinkplugins.golibrespot.metadata.MetadataResolver.ReadyBackend;
import dev.lavalinkplugins.golibrespot.pool.ExclusivePool;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Contract + acceptance tests for {@link GoLibrespotAudioTrack} (T18) driving
 * the real Lavaplayer {@link LocalAudioTrackExecutor} against a fake
 * {@link PlaybackCoordinator}.
 *
 * <p>Covered:</p>
 * <ul>
 *   <li><b>Activation barrier contract</b> — {@code process()} blocks (bounded)
 *       on {@code coordinator.awaitActivated} while pending and never ends the
 *       track early; activation failure surfaces as a typed
 *       {@link ActivationException} that fails the track.</li>
 *   <li><b>Frame delivery</b> — after activation, decoded shorts from
 *       {@code coordinator.nextFrame} are fed to the audio pipeline.</li>
 *   <li><b>End-of-stream</b> — a {@code null} frame (EOS) stops feeding; the
 *       track does not error.</li>
 *   <li><b>Seek</b> — the seek executor maps to {@code coordinator.seek} and
 *       calls {@code pipeline.seekPerformed()} on OK; a failed seek throws a
 *       {@code FriendlyException} so the track fails (never a silent mis-seek).</li>
 *   <li><b>Interrupt / stop</b> — the track ends naturally on an executor
 *       interrupt (stop) and the pipeline is closed in {@code finally}.</li>
 * </ul>
 *
 * <p>The track is exercised through the real {@link LocalAudioTrackExecutor}
 * (assigned to the track so {@code setPosition} reaches the seek path) with a
 * recording pipeline seam so process/seekPerformed/close are observable.</p>
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class GoLibrespotAudioTrackTest {

  private static final String TRACK_ID = "4iV5W9uYEdYUVa79Axb7Rh";
  /** One 20 ms stereo chunk at 44100 Hz: 882 samples/channel → 1764 shorts. */
  private static final int CHUNK_SHORTS = 1764;

  private final List<Rig> rigs = new ArrayList<>();

  @AfterEach
  void tearDown() {
    for (Rig rig : rigs) {
      rig.close();
    }
    rigs.clear();
  }

  // ---------------------------------------------------------------- activation

  @Test
  void processBlocksWhileActivationIsPendingAndDoesNotEndTheTrack() throws Exception {
    try (Rig rig = newRig(false)) {
      Thread playback = rig.startPlayback();
      await().atMost(3, TimeUnit.SECONDS).until(playback::isAlive);
      // give the read loop a beat to reach the activation barrier — still pending
      Thread.sleep(250);
      assertThat(playback.isAlive()).as("track must not end while activation is pending").isTrue();
      assertThat(rig.pipeline.processCalls).isEmpty();

      rig.executor.stop(); // dispose + interrupt → clean exit, no error
      rig.joinPlayback(playback);
      assertThat(rig.exceptions).isEmpty();
    }
  }

  @Test
  void activationFailureFailsTheTrackWithTypedActivationException() throws Exception {
    try (Rig rig = newRig(false)) {
      rig.coordinator.failActivation = true;
      Thread playback = rig.startPlayback();
      rig.joinPlayback(playback);

      assertThat(rig.exceptions).as("track must fail on activation failure").isNotEmpty();
      assertThat(rig.exceptions).anyMatch(GoLibrespotAudioTrackTest::containsActivationException);
      assertThat(rig.pipeline.closed).as("pipeline closed on error").isTrue();
    }
  }

  // ---------------------------------------------------------------- frames

  @Test
  void deliversFramesToThePipelineAfterActivation() throws Exception {
    try (Rig rig = newRig(true)) {
      rig.coordinator.feed(chunk(10)); // 10 × 20 ms chunks
      Thread playback = rig.startPlayback();

      await().atMost(5, TimeUnit.SECONDS)
          .until(() -> rig.pipeline != null && !rig.pipeline.processCalls.isEmpty());
      assertThat(rig.pipeline.processCalls.get(0)).isEqualTo(chunk(10).length);

      rig.executor.stop();
      rig.joinPlayback(playback);
      assertThat(rig.exceptions).isEmpty();
      assertThat(rig.pipeline.closed).isTrue();
    }
  }

  @Test
  void keepsReadingAcrossMultipleChunksAndTransientEmptyPolls() throws Exception {
    try (Rig rig = newRig(true)) {
      rig.coordinator.feed(chunk(2));
      Thread playback = rig.startPlayback();
      await().atMost(5, TimeUnit.SECONDS)
          .until(() -> rig.pipeline != null && rig.pipeline.processCalls.size() == 1);

      // The coordinator returns empty arrays between writes. The read executor
      // must remain alive and consume a later FIFO chunk in the same track.
      rig.coordinator.feed(chunk(3));
      await().atMost(5, TimeUnit.SECONDS)
          .until(() -> rig.pipeline.processCalls.size() == 2);
      assertThat(rig.pipeline.processCalls)
          .containsExactly(chunk(2).length, chunk(3).length);

      rig.executor.stop();
      rig.joinPlayback(playback);
      assertThat(rig.exceptions).isEmpty();
    }
  }

  @Test
  void endOfStreamStopsFeedingFramesWithoutErroring() throws Exception {
    try (Rig rig = newRig(true)) {
      rig.coordinator.endOfStream = true;
      Thread playback = rig.startPlayback();
      Thread.sleep(300);

      // EOS → the read path returns (no frames fed) and the track neither ends
      // itself nor errors; it waits for the player to finish it.
      assertThat(playback.isAlive()).isTrue();
      assertThat(rig.pipeline.processCalls).isEmpty();
      assertThat(rig.exceptions).isEmpty();

      rig.executor.stop();
      rig.joinPlayback(playback);
      assertThat(rig.exceptions).isEmpty();
    }
  }

  // ---------------------------------------------------------------- seek

  @Test
  void seekMapsToCoordinatorAndCallsPipelineSeekPerformedOnOk() throws Exception {
    try (Rig rig = newRig(true)) {
      rig.coordinator.feed(chunk(10));
      Thread playback = rig.startPlayback();
      await().atMost(5, TimeUnit.SECONDS)
          .until(() -> rig.pipeline != null && !rig.pipeline.processCalls.isEmpty());

      // the buffered frames must actually reach the frame buffer so the drain
      // below can terminate it and let the loop return to the pending-seek check
      await().atMost(5, TimeUnit.SECONDS).until(() -> rig.executor.getAudioBuffer().hasReceivedFrames());
      drainBuffer(rig);

      rig.coordinator.seekResult = Result.ok("seeked");
      rig.track.setPosition(5_000);

      await().atMost(5, TimeUnit.SECONDS).until(() -> rig.coordinator.seekCalls.size() == 1);
      assertThat(rig.coordinator.seekCalls).containsExactly(5_000L);
      await().atMost(5, TimeUnit.SECONDS)
          .untilAsserted(() -> assertThat(rig.pipeline.seekPerformedCalls).hasSize(1));
      assertThat(rig.pipeline.seekPerformedCalls.get(0)[0]).isEqualTo(5_000L);

      rig.executor.stop();
      rig.joinPlayback(playback);
      assertThat(rig.exceptions).isEmpty();
    }
  }

  @Test
  void failedSeekThrowsFriendlyExceptionAndFailsTheTrack() throws Exception {
    try (Rig rig = newRig(true)) {
      rig.coordinator.feed(chunk(10));
      Thread playback = rig.startPlayback();
      await().atMost(5, TimeUnit.SECONDS)
          .until(() -> rig.pipeline != null && !rig.pipeline.processCalls.isEmpty());
      await().atMost(5, TimeUnit.SECONDS).until(() -> rig.executor.getAudioBuffer().hasReceivedFrames());
      drainBuffer(rig);

      rig.coordinator.seekResult = Result.failed("no active session to seek");
      rig.track.setPosition(12_345);

      rig.joinPlayback(playback);
      assertThat(rig.exceptions).as("a failed seek must fail the track").isNotEmpty();
      assertThat(rig.exceptions).anyMatch(e -> containsMessage(e, "no active session to seek"));
      assertThat(rig.coordinator.seekCalls).containsExactly(12_345L);
    }
  }

  /** Drains every buffered frame so waitOnEnd unblocks (terminates the empty buffer). */
  private static void drainBuffer(Rig rig) throws Exception {
    int drained = 0;
    while (drained < 10_000 && rig.executor.provide() != null) {
      drained++;
    }
    // give the playback loop a beat to re-enter the read path (interruptible)
    Thread.sleep(100);
  }

  // ---------------------------------------------------------------- stop / interrupt

  @Test
  void trackEndsNaturallyOnExecutorInterrupt() throws Exception {
    try (Rig rig = newRig(true)) {
      rig.coordinator.feed(chunk(5));
      Thread playback = rig.startPlayback();
      await().atMost(5, TimeUnit.SECONDS)
          .until(() -> rig.pipeline != null && !rig.pipeline.processCalls.isEmpty());

      rig.executor.stop(); // dispose + interrupt → natural stop
      rig.joinPlayback(playback);
      assertThat(rig.exceptions).as("interrupt-driven stop is a clean end").isEmpty();
      assertThat(rig.pipeline.closed).isTrue();
    }
  }

  // ---------------------------------------------------------------- misc

  @Test
  void makeShallowCloneCarriesSameIdentityAndManager() {
    try (Rig rig = newRig(true)) {
      GoLibrespotAudioTrack clone = (GoLibrespotAudioTrack) rig.track.makeClone();

      assertThat(clone).isNotSameAs(rig.track);
      assertThat(clone.trackId()).isEqualTo(TRACK_ID);
      assertThat(clone.getInfo().identifier).isEqualTo("spdirect:" + TRACK_ID);
      assertThat(clone.getSourceManager()).isSameAs(rig.track.getSourceManager());
    }
  }

  @Test
  void isSeekableIsTrueAndSourceManagerIsRoundTripped() {
    try (Rig rig = newRig(true)) {
      assertThat(rig.track.isSeekable()).isTrue();
      assertThat(rig.track.getSourceManager()).isSameAs(rig.manager);
      GoLibrespotAudioTrack other = rig.track;
      other.setSourceManager(rig.manager);
      assertThat(other.getSourceManager()).isSameAs(rig.manager);
    }
  }

  // ---------------------------------------------------------------- helpers

  private static boolean containsActivationException(Throwable t) {
    for (Throwable cur = t; cur != null; cur = cur.getCause()) {
      if (cur instanceof ActivationException) {
        return true;
      }
    }
    return false;
  }

  private static boolean containsMessage(Throwable t, String needle) {
    for (Throwable cur = t; cur != null; cur = cur.getCause()) {
      if (cur.getMessage() != null && cur.getMessage().contains(needle)) {
        return true;
      }
    }
    return false;
  }

  private static short[] chunk(int chunks) {
    short[] data = new short[CHUNK_SHORTS * chunks];
    for (int i = 0; i < data.length; i++) {
      data[i] = (short) (i % 1000); // deterministic non-silence PCM
    }
    return data;
  }

  private Rig newRig(boolean autoActivate) {
    Rig rig = new Rig(autoActivate);
    rig.start();
    rigs.add(rig);
    return rig;
  }

  /** Track under test wired to a fake coordinator + real executor + recording pipeline. */
  static final class Rig implements AutoCloseable {

    final boolean autoActivate;
    final FakeCoordinator coordinator = new FakeCoordinator();
    final List<Throwable> exceptions = new CopyOnWriteArrayList<>();

    GoLibrespotConfig config;
    ExclusivePool pool;
    MetadataResolver resolver;
    GoLibrespotAudioSourceManager manager;
    GoLibrespotAudioTrack track;
    LocalAudioTrackExecutor executor;
    RecordingPipeline pipeline;

    Rig(boolean autoActivate) {
      this.autoActivate = autoActivate;
    }

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
        manager = new GoLibrespotAudioSourceManager(pool, resolver, (h, c) -> coordinator);
        coordinator.autoActivate = autoActivate;

        AudioTrackInfo info = new AudioTrackInfo(
            "夜に駆ける", "YOASOBI", 3_040_000L, "spdirect:" + TRACK_ID, true, null, null, null);
        track = new GoLibrespotAudioTrack(TRACK_ID, info, manager,
            (ctx, fmt) -> {
              AudioPipeline real = AudioPipelineFactory.create(ctx, fmt);
              pipeline = new RecordingPipeline(real);
              return pipeline;
            });

        AudioConfiguration configuration = new AudioConfiguration();
        configuration.setOutputFormat(StandardAudioDataFormats.COMMON_PCM_S16_LE);
        executor = new LocalAudioTrackExecutor(track, configuration, new AudioPlayerOptions(), false, 0);
        track.assignExecutor(executor, true);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    Thread startPlayback() {
      Thread playback = new Thread(() -> {
        try {
          executor.execute(new TrackStateListener() {
            @Override
            public void onTrackException(
                com.sedmelluq.discord.lavaplayer.track.AudioTrack track,
                com.sedmelluq.discord.lavaplayer.tools.FriendlyException exception) {
              exceptions.add(exception);
            }

            @Override
            public void onTrackStuck(
                com.sedmelluq.discord.lavaplayer.track.AudioTrack track, long thresholdMs) {
              exceptions.add(new IllegalStateException("stuck " + thresholdMs));
            }
          });
        } catch (Throwable t) {
          exceptions.add(t);
        }
      }, "golibrespot-track-test");
      playback.setDaemon(true);
      playback.start();
      return playback;
    }

    void joinPlayback(Thread playback) throws InterruptedException {
      playback.join(TimeUnit.SECONDS.toMillis(10));
      assertThat(playback.isAlive()).as("playback thread must terminate").isFalse();
    }

    @Override
    public void close() {
      try {
        executor.stop();
      } catch (Throwable ignored) {
        // already stopped
      }
      manager.shutdown();
      pool.shutdown();
    }
  }

  /** Scripted coordinator seam: gates activation, queues frames, records seeks. */
  static final class FakeCoordinator implements PlaybackCoordinator {

    final CountDownLatch activationGate = new CountDownLatch(1);
    volatile boolean autoActivate;
    volatile boolean failActivation;
    final ArrayDeque<short[]> frames = new ArrayDeque<>();
    volatile boolean endOfStream;
    volatile Result seekResult = Result.ok("fake seek");
    final List<Long> seekCalls = Collections.synchronizedList(new ArrayList<>());

    void feed(short[] frameChunk) {
      synchronized (frames) {
        frames.addLast(frameChunk);
      }
    }

    @Override
    public void awaitActivated(Duration timeout) throws ActivationException, InterruptedException {
      if (failActivation) {
        throw new ActivationException(ActivationException.Kind.QUARANTINED, "fake backend quarantined");
      }
      if (autoActivate) {
        return;
      }
      if (!activationGate.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
        throw new ActivationException(ActivationException.Kind.TIMEOUT, "fake activation timeout");
      }
    }

    @Override
    public short[] nextFrame(Duration timeout) throws ActivationException, InterruptedException {
      if (endOfStream) {
        return null;
      }
      synchronized (frames) {
        if (!frames.isEmpty()) {
          return frames.poll();
        }
      }
      return new short[0]; // transient EOF — no frames this call, never terminal
    }

    @Override
    public Result seek(long positionMs) {
      seekCalls.add(positionMs);
      return seekResult;
    }

    // ---- unused by the track tests, inert impls --------------------------------
    @Override
    public CompletableFuture<Result> start(String uri, long positionMs) {
      return CompletableFuture.completedFuture(Result.ok("fake"));
    }

    @Override
    public CompletableFuture<Result> replace(String uri, long positionMs) {
      return CompletableFuture.completedFuture(Result.ok("fake"));
    }

    @Override
    public CompletableFuture<Result> logicalStop() {
      return CompletableFuture.completedFuture(Result.ok("fake"));
    }

    @Override
    public CompletableFuture<Result> destroy() {
      return CompletableFuture.completedFuture(Result.ok("fake"));
    }

    @Override
    public CompletableFuture<Result> pauseRemote() {
      return CompletableFuture.completedFuture(Result.ok("fake"));
    }

    @Override
    public CompletableFuture<Result> resumeRemote() {
      return CompletableFuture.completedFuture(Result.ok("fake"));
    }

    @Override
    public CompletableFuture<Result> quarantine(String reason) {
      return CompletableFuture.completedFuture(Result.ok("fake"));
    }

    @Override
    public boolean isActive() {
      return true;
    }

    @Override
    public String expectedUri() {
      return "spotify:track:" + TRACK_ID;
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

  /** Pipeline seam recording feed/seek/close while delegating to the real pipeline. */
  static final class RecordingPipeline implements PcmPipeline {

    final AudioPipeline delegate;
    final List<Integer> processCalls = Collections.synchronizedList(new ArrayList<>());
    final List<long[]> seekPerformedCalls = Collections.synchronizedList(new ArrayList<>());
    final AtomicBoolean closed = new AtomicBoolean();

    RecordingPipeline(AudioPipeline delegate) {
      this.delegate = delegate;
    }

    @Override
    public void process(short[] input, int offset, int length) throws InterruptedException {
      processCalls.add(length);
      delegate.process(input, offset, length);
    }

    @Override
    public void seekPerformed(long requestedPosition, long accuratePosition) {
      seekPerformedCalls.add(new long[] {requestedPosition, accuratePosition});
      delegate.seekPerformed(requestedPosition, accuratePosition);
    }

    @Override
    public void flush() throws InterruptedException {
      delegate.flush();
    }

    @Override
    public void close() {
      closed.set(true);
      delegate.close();
    }
  }
}
