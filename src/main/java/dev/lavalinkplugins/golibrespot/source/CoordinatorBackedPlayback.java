package dev.lavalinkplugins.golibrespot.source;

import dev.lavalinkplugins.golibrespot.fifo.FifoReader;
import dev.lavalinkplugins.golibrespot.fifo.PcmDecoder;
import dev.lavalinkplugins.golibrespot.lifecycle.ActivationException;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Phase;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Result;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Timing;
import dev.lavalinkplugins.golibrespot.lifecycle.FifoReaderFactory;
import dev.lavalinkplugins.golibrespot.lifecycle.LifecycleCoordinator;
import dev.lavalinkplugins.golibrespot.lifecycle.SeekHandshake;
import dev.lavalinkplugins.golibrespot.lifecycle.StopSequence;
import dev.lavalinkplugins.golibrespot.logging.LogSanitizer;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Production {@link PlaybackCoordinator} over the real T15
 * {@link LifecycleCoordinator} + T17 {@link StopSequence}.
 *
 * <p>The seek path maps to the T16 {@link SeekHandshake} built over the
 * coordinator's machine and the <b>current session's</b> FIFO reader — captured
 * by wrapping the {@link FifoReaderFactory} the coordinator is constructed with
 * (pass {@link #wrapReaderFactory} into the coordinator's ctor) — plus a
 * per-session {@link PcmDecoder} the wrapper resets on every {@code start} /
 * {@code replace}. The daemon's chunks are frame-aligned (16 KiB = 4096 s16le
 * frames), so the wrapper's drain-decoder and the coordinator's own decoder
 * stay in lockstep across the seek boundary (no partial-frame remainder to
 * clear). The {@code seekPerformed} hook is a no-op here: the T18 track calls
 * {@code pipeline.seekPerformed()} itself after the handshake returns OK.</p>
 *
 * <p>All bridge-facing operations ({@code logicalStop}/{@code destroy}/
 * {@code pauseRemote}/{@code resumeRemote}/{@code quarantine}) return the
 * machine/stop-sequence's bounded futures untouched — callers (the player
 * bridge) fire-and-forget, never blocking the Lavaplayer thread on a daemon
 * round-trip.</p>
 */
public final class CoordinatorBackedPlayback implements PlaybackCoordinator {

  private final LifecycleCoordinator coordinator;
  private final StopSequence stopSequence;
  private final Timing timing;
  private final SeekHandshake.Drain drain;
  private final Consumer<String> logSink;
  private final LogSanitizer sanitizer = LogSanitizer.defaults();
  private final ReentrantLock seekLock = new ReentrantLock();

  private volatile FifoReader currentReader;
  private volatile PcmDecoder decoder = new PcmDecoder();

  /**
   * @param coordinator   the T15 coordinator for this backend (also the source of
   *                      the {@link #wrapReaderFactory reader factory})
   * @param stopSequence  the T17 stop sequence for this backend
   * @param timing        bounded machine budgets (mirror what the machine was built with)
   * @param logSink       receives sanitized diagnostic lines (default no-op)
   */
  public CoordinatorBackedPlayback(
      LifecycleCoordinator coordinator,
      StopSequence stopSequence,
      Timing timing,
      Consumer<String> logSink) {
    this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
    this.stopSequence = Objects.requireNonNull(stopSequence, "stopSequence");
    this.timing = Objects.requireNonNull(timing, "timing");
    this.drain = SeekHandshake.Drain.defaults();
    this.logSink = Objects.requireNonNull(logSink, "logSink");
  }

  /**
   * Wraps the {@link FifoReaderFactory} the coordinator is constructed with so
   * the wrapper captures each session's reader — the {@link SeekHandshake}
   * drain reads THAT reader's queue (the lossless reader holds the
   * pre-seek kernel bytes). Must be called before the coordinator's ctor.
   */
  public FifoReaderFactory wrapReaderFactory(FifoReaderFactory inner) {
    Objects.requireNonNull(inner, "inner");
    return stream -> {
      FifoReader reader = inner.create(stream);
      currentReader = reader;
      return reader;
    };
  }

  @Override
  public CompletableFuture<Result> start(String uri, long positionMs) {
    decoder = new PcmDecoder(); // per-session decoder, matching the coordinator's reset
    return coordinator.start(uri, positionMs);
  }

  @Override
  public CompletableFuture<Result> replace(String uri, long positionMs, boolean paused) {
    decoder = new PcmDecoder();
    return coordinator.replace(uri, positionMs, paused);
  }

  @Override
  public CompletableFuture<Result> logicalStop() {
    return stopSequence.logicalStop();
  }

  @Override
  public CompletableFuture<Result> destroy() {
    return stopSequence.destroy();
  }

  @Override
  public CompletableFuture<Result> pauseRemote() {
    return coordinator.machine().pause();
  }

  @Override
  public CompletableFuture<Result> resumeRemote() {
    return coordinator.machine().resume();
  }

  @Override
  public CompletableFuture<Result> quarantine(String reason) {
    return coordinator.machine().quarantine(sanitizer.sanitize(reason), false);
  }

  @Override
  public void awaitActivated(Duration timeout) throws ActivationException, InterruptedException {
    coordinator.awaitActivated(timeout);
  }

  @Override
  public short[] nextFrame(Duration timeout) throws ActivationException, InterruptedException {
    return coordinator.nextFrame(timeout);
  }

  @Override
  public Result seek(long positionMs) {
    // Resume after the seek iff the backend is not paused (the bridge paused it
    // via pauseRemote when the player paused). A seek while paused stays paused.
    boolean resumeAfter = coordinator.machine().phase() != Phase.PAUSE_CONFIRMED;
    seekLock.lock();
    try {
      SeekHandshake handshake = new SeekHandshake(
          coordinator.machine(),
          currentReader,
          decoder,
          timing,
          drain,
          () -> { // the T18 track calls pipeline.seekPerformed() after OK
          },
          logSink);
      return handshake.seek(positionMs, resumeAfter);
    } finally {
      seekLock.unlock();
    }
  }

  @Override
  public boolean isActive() {
    return coordinator.isActive();
  }

  @Override
  public String expectedUri() {
    return coordinator.expectedUri();
  }

  @Override
  public long positionMs() {
    return coordinator.positionMs();
  }

  @Override
  public long generation() {
    return coordinator.generation();
  }
}
