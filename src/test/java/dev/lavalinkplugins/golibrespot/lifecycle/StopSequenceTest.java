package dev.lavalinkplugins.golibrespot.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.lavalinkplugins.golibrespot.backend.rest.GoLibrespotRestClient;
import dev.lavalinkplugins.golibrespot.backend.ws.EventsWebSocketClient;
import dev.lavalinkplugins.golibrespot.config.BackendConfig;
import dev.lavalinkplugins.golibrespot.config.GoLibrespotConfig;
import dev.lavalinkplugins.golibrespot.fifo.DummyWriterCancellation;
import dev.lavalinkplugins.golibrespot.fifo.FifoOpener;
import dev.lavalinkplugins.golibrespot.fifo.FifoReader;
import dev.lavalinkplugins.golibrespot.fifo.FifoTestUtil;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.MachineState;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Outcome;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Phase;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Result;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Timing;
import dev.lavalinkplugins.golibrespot.pool.BackendHandle;
import dev.lavalinkplugins.golibrespot.pool.BackendState;
import dev.lavalinkplugins.golibrespot.pool.ExclusivePool;
import dev.lavalinkplugins.golibrespot.pool.Lease;
import dev.lavalinkplugins.golibrespot.testfixtures.FakeLibrespotDaemon;
import dev.lavalinkplugins.golibrespot.testfixtures.FakeLibrespotDaemon.RecordedCommand;
import dev.lavalinkplugins.golibrespot.testfixtures.FakeLibrespotDaemon.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

/**
 * Contract + acceptance tests for {@link StopSequence} (T17) against the
 * {@link FakeLibrespotDaemon} fixture (T6), the real REST client (T8), the real
 * reconnecting WS client (T9), the real pool (T13), the real state machine
 * (T14) and the real coordinator (T15).
 *
 * <p>Everything under test here is the T17 cleanup contract:</p>
 * <ul>
 *   <li><b>Logical stop</b> = remote pause + pause confirmation (paused event or
 *       paused /status within the machine's ack+reconcile+poll budget) +
 *       generation retirement via {@code machine.retire()} — the daemon stop
 *       endpoint is NEVER issued (it would trip the v0.9.0 stop-race).</li>
 *   <li><b>Double stop</b> is idempotent (second stop is a no-op OK).</li>
 *   <li><b>Player destroy</b> releases the lease asynchronously — never blocking
 *       the calling thread — and serializes behind an in-flight machine command
 *       (a seek) without hanging or double-releasing.</li>
 *   <li><b>Plugin shutdown</b> cancels in-flight FIFO opens via the dummy-writer
 *       rendezvous, closes the WS, drains executors with bounded joins and
 *       leaves zero blocked opener threads.</li>
 *   <li><b>End reasons</b> FINISHED / STOPPED / REPLACED / CLEANUP each release
 *       the lease exactly once (a second release of a stale lease is a no-op).</li>
 *   <li><b>Threads</b> are never leaked across repeated stop/play cycles.</li>
 * </ul>
 *
 * <p>Test rig = the T15/T16 pattern: real REST + real WS (wired via
 * {@code coordinator.listener()}) + real pool + real machine + coordinator +
 * real {@link FifoReader} over a scripted {@link PipedInputStream} via an
 * injectable {@link FifoOpenerSeam} (cross-platform), plus one Linux-gated test
 * that drives the real {@link FifoOpener} against a real mkfifo FIFO with
 * reflection-injected executors (the FifoOpenerTest drain-inspection pattern)
 * to prove a blocked open is cancelled and the opener executor fully drained.</p>
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class StopSequenceTest {

  private static final String URI = "spotify:track:aaaaaaaaaaaaaaaaaaaaaa";
  private static final String URI_B = "spotify:track:bbbbbbbbbbbbbbbbbbbbbb";
  private static final long SEEK_POS = 60_000L;

  /** Short timings so every barrier/timeout test stays snappy and deterministic. */
  private static final Timing FAST =
      new Timing(800, 600, 1000, 800, 80, 5);

  private final List<Rig> rigs = new ArrayList<>();

  @AfterEach
  void tearDown() {
    for (Rig rig : rigs) {
      rig.close();
    }
    rigs.clear();
  }

  // ==================================================================
  // Logical stop: pause-ack + generation retirement (never the stop endpoint)
  // ==================================================================

  @Test
  void logicalStopConfirmsPausedAndRetiresGeneration() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      rig.daemon.play(Response.ok().emit("playing", playingData(URI)));
      rig.daemon.status(Response.ok(playingStatus(URI)));
      assertThat(rig.coordinator.start(URI, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();
      assertThat(rig.machine.state()).isEqualTo(MachineState.LEASED);
      assertThat(rig.machine.phase()).isEqualTo(Phase.PLAYING);
      assertThat(rig.coordinator.currentLease().isActive()).isTrue();

      rig.daemon.pause(Response.ok().emit("paused", sharedData(URI)));
      Result stopped = rig.stopSeq.logicalStop().get(5, TimeUnit.SECONDS);

      assertThat(stopped.isOk()).as("logical stop: " + stopped).isTrue();
      assertThat(rig.machine.state()).isEqualTo(MachineState.READY);
      assertThat(rig.machine.phase()).isEqualTo(Phase.IDLE);
      assertThat(rig.machine.generation()).isEqualTo(1); // retired, not bumped
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY);
      assertThat(rig.coordinator.currentLease()).isNull();
      assertThat(rig.coordinator.isActive()).isFalse();

      List<RecordedCommand> commands = rig.daemon.getReceivedCommands();
      assertThat(postPaths(commands)).as("play then pause — never a stop or resume")
          .containsExactly("/player/play", "/player/pause");
      assertThat(commands).filteredOn(c -> c.path().equals("/player/stop")).isEmpty();
    }
  }

  @Test
  void doubleStopIsIdempotentNoOp() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      rig.daemon.play(Response.ok().emit("playing", playingData(URI)));
      rig.daemon.status(Response.ok(playingStatus(URI)));
      assertThat(rig.coordinator.start(URI, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();
      rig.daemon.pause(Response.ok().emit("paused", sharedData(URI)));
      assertThat(rig.stopSeq.logicalStop().get(5, TimeUnit.SECONDS).isOk()).isTrue();
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY);

      Result second = rig.stopSeq.logicalStop().get(5, TimeUnit.SECONDS);

      assertThat(second.isOk()).as("second stop is a no-op OK: " + second).isTrue();
      // no extra commands were issued by the second stop
      assertThat(postPaths(rig.daemon.getReceivedCommands()))
          .containsExactly("/player/play", "/player/pause");
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY);
    }
  }

  @Test
  void logicalStopDuringInFlightSeekSerializesBehindTheSeek() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      rig.daemon.play(Response.ok().emit("playing", playingData(URI)));
      rig.daemon.status(Response.ok(playingStatus(URI)));
      assertThat(rig.coordinator.start(URI, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();

      // a seek that acks late (600ms < 1s seekAck) is still in flight when the
      // logical stop starts — the stop must wait for it to settle, then pause
      rig.daemon.seek(Response.ok());
      rig.daemon.emitAfter("seek", seekData(URI, SEEK_POS), 600);
      CompletableFuture<Result> seekFuture = rig.machine.seek(SEEK_POS);
      await().atMost(Duration.ofSeconds(2))
          .until(() -> rig.machine.phase() == Phase.SEEKING);

      rig.daemon.pause(Response.ok().emit("paused", sharedData(URI)));
      Result stopped = rig.stopSeq.logicalStop().get(8, TimeUnit.SECONDS);

      assertThat(stopped.isOk()).as("stop serialized behind the seek: " + stopped).isTrue();
      assertThat(seekFuture.get(5, TimeUnit.SECONDS).isOk()).isTrue();
      assertThat(rig.machine.state()).isEqualTo(MachineState.READY);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY);
      assertThat(postPaths(rig.daemon.getReceivedCommands()))
          .as("play, then the seek, then the stop's pause — strictly serialized")
          .containsExactly("/player/play", "/player/seek", "/player/pause");
    }
  }

  // ==================================================================
  // Player destroy: async release, exactly-once, serialized behind a seek
  // ==================================================================

  @Test
  void destroyDuringActivePlaybackReleasesAsync() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      rig.daemon.play(Response.ok().emit("playing", playingData(URI)));
      rig.daemon.status(Response.ok(playingStatus(URI)));
      assertThat(rig.coordinator.start(URI, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();
      Lease lease = rig.coordinator.currentLease();

      long startNanos = System.nanoTime();
      CompletableFuture<Result> destroyFuture = rig.stopSeq.destroy();
      long callMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
      assertThat(callMillis).as("destroy() itself must not block the caller").isLessThan(500);
      assertThat(destroyFuture.isDone()).as("release happens asynchronously").isFalse();

      Result destroyed = destroyFuture.get(5, TimeUnit.SECONDS);
      assertThat(destroyed.isOk()).as("destroy release: " + destroyed).isTrue();
      // completion contract: the destroy future completing OK implies the
      // coordinator already observed the release — asserted synchronously
      assertThat(rig.machine.state()).isEqualTo(MachineState.READY);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY);
      assertThat(rig.coordinator.currentLease()).isNull();
      assertThat(rig.coordinator.isActive()).isFalse();
      assertThat(lease.isActive()).isFalse();
      // destroy = pure release; no pause command, no stop endpoint
      assertThat(postPaths(rig.daemon.getReceivedCommands()))
          .containsExactly("/player/play");
      assertThat(rig.daemon.getReceivedCommands())
          .filteredOn(c -> c.path().equals("/player/stop")).isEmpty();
    }
  }

  @Test
  void destroyDuringInFlightSeekReleasesCleanly() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      rig.daemon.play(Response.ok().emit("playing", playingData(URI)));
      rig.daemon.status(Response.ok(playingStatus(URI)));
      assertThat(rig.coordinator.start(URI, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();

      rig.daemon.seek(Response.ok());
      rig.daemon.emitAfter("seek", seekData(URI, SEEK_POS), 600);
      CompletableFuture<Result> seekFuture = rig.machine.seek(SEEK_POS);
      await().atMost(Duration.ofSeconds(2))
          .until(() -> rig.machine.phase() == Phase.SEEKING);
      Lease lease = rig.coordinator.currentLease();

      Result destroyed = rig.stopSeq.destroy().get(8, TimeUnit.SECONDS);

      assertThat(destroyed.isOk()).as("destroy while seek in flight: " + destroyed).isTrue();
      assertThat(seekFuture.get(5, TimeUnit.SECONDS).isOk()).isTrue();
      assertThat(rig.machine.state()).isEqualTo(MachineState.READY);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY);
      assertThat(lease.isActive()).isFalse();
      lease.release(); // idempotent second release — no-op, no exception
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY);
      assertThat(postPaths(rig.daemon.getReceivedCommands()))
          .containsExactly("/player/play", "/player/seek");
    }
  }

  @Test
  void destroyDuringInFlightSeekThatNeverAcksReleasesViaQuarantine() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      rig.daemon.play(Response.ok().emit("playing", playingData(URI)));
      rig.daemon.status(Response.ok(playingStatus(URI)));
      assertThat(rig.coordinator.start(URI, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();

      // a seek that never acks and whose /status never reaches the position: the
      // machine's own seek-ack timeout quarantines and releases the lease
      rig.daemon.seek(Response.ok());
      rig.daemon.status(Response.ok(playingStatus(URI)));
      CompletableFuture<Result> seekFuture = rig.machine.seek(SEEK_POS);
      await().atMost(Duration.ofSeconds(2))
          .until(() -> rig.machine.phase() == Phase.SEEKING);
      Lease lease = rig.coordinator.currentLease();

      Result destroyed = rig.stopSeq.destroy().get(8, TimeUnit.SECONDS);

      assertThat(destroyed.isOk()).as("destroy released via quarantine: " + destroyed).isTrue();
      assertThat(rig.machine.state()).isEqualTo(MachineState.QUARANTINING);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.QUARANTINING);
      assertThat(lease.isActive()).isFalse();
      lease.release(); // idempotent — must not resurrect the backend
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.QUARANTINING);
      assertThat(seekFuture.get(5, TimeUnit.SECONDS).outcome())
          .isEqualTo(Outcome.QUARANTINED);
    }
  }

  // ==================================================================
  // Plugin shutdown: bounded joins, no hangs, zero blocked opener threads
  // ==================================================================

  @Test
  void shutdownDuringActivePlaybackDoesNotHang() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      rig.daemon.play(Response.ok().emit("playing", playingData(URI)));
      rig.daemon.status(Response.ok(playingStatus(URI)));
      assertThat(rig.coordinator.start(URI, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();

      long startNanos = System.nanoTime();
      rig.stopSeq.shutdown();
      long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

      assertThat(elapsedMillis).as("shutdown bounded join under deadline")
          .isLessThan(10_000);
      assertThat(rig.machine.state()).isEqualTo(MachineState.DEAD);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.DEAD);
      assertThat(rig.pool.isShutdown()).isTrue();
      assertThat(rig.stopSeq.isShutdown()).isTrue();
      // idempotent: a second shutdown is a no-op and still returns promptly
      rig.stopSeq.shutdown();
    }
  }

  @Test
  void shutdownDuringInFlightSeekDoesNotHang() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      rig.daemon.play(Response.ok().emit("playing", playingData(URI)));
      rig.daemon.status(Response.ok(playingStatus(URI)));
      assertThat(rig.coordinator.start(URI, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();

      rig.daemon.seek(Response.ok());
      CompletableFuture<Result> seekFuture = rig.machine.seek(SEEK_POS);
      await().atMost(Duration.ofSeconds(2))
          .until(() -> rig.machine.phase() == Phase.SEEKING);

      long startNanos = System.nanoTime();
      rig.stopSeq.shutdown();
      long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

      assertThat(elapsedMillis).as("shutdown during in-flight seek bounded join")
          .isLessThan(10_000);
      // the pending seek completes promptly — either the WS-close disconnect
      // quarantined the LEASED machine first or the machine close completed it
      // with DEAD; both are terminal, neither hangs
      Outcome seekOutcome = seekFuture.get(5, TimeUnit.SECONDS).outcome();
      assertThat(seekOutcome).as("pending seek completed terminally").isIn(Outcome.QUARANTINED, Outcome.DEAD);
      assertThat(rig.machine.state()).isEqualTo(MachineState.DEAD);
    }
  }

  @Test
  void shutdownCancelsInFlightScriptedOpenWithoutHang() throws Exception {
    try (Rig rig = newRig()) {
      NeverHandle never = new NeverHandle();
      rig.seam.supplier = () -> never;
      rig.daemon.play(Response.ok());

      CompletableFuture<Result> activation = rig.coordinator.start(URI, 0);
      await().atMost(Duration.ofSeconds(3)).until(never::entered);

      long startNanos = System.nanoTime();
      rig.stopSeq.shutdown();
      long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

      assertThat(elapsedMillis).as("shutdown cancels the pending open promptly")
          .isLessThan(10_000);
      assertThat(never.isCancelled()).as("in-flight open was cancelled").isTrue();
      assertThat(activation.get(5, TimeUnit.SECONDS)).isNotNull(); // start() completed
    }
  }

  /** Inspectable single-thread pool (delegated executor cannot be inspected). */
  private static ThreadPoolExecutor singleThreadPool(String name) {
    ThreadFactory factory = runnable -> {
      Thread t = new Thread(runnable, name);
      t.setDaemon(true);
      return t;
    };
    return new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), factory);
  }

  /**
   * Builds a real {@link FifoOpener} with injectable executors via reflection:
   * its package-private constructor is not visible from this package, and the
   * FifoOpenerTest drain-inspection assertions require a real
   * {@link ThreadPoolExecutor}. JDK 21 allows {@code setAccessible(true)} on
   * classpath (unnamed-module) classes.
   */
  private static FifoOpener newInjectedOpener(ExecutorService openPool, ExecutorService writerPool)
      throws Exception {
    Constructor<DummyWriterCancellation> writerCtor =
        DummyWriterCancellation.class.getDeclaredConstructor(ExecutorService.class);
    writerCtor.setAccessible(true);
    Constructor<FifoOpener> openerCtor =
        FifoOpener.class.getDeclaredConstructor(ExecutorService.class, DummyWriterCancellation.class);
    openerCtor.setAccessible(true);
    return openerCtor.newInstance(openPool, writerCtor.newInstance(writerPool));
  }

  @EnabledOnOs(OS.LINUX)
  @Test
  void shutdownCancelsBlockedFifoOpenAndDrainsOpenerExecutor() throws Exception {
    FifoTestUtil.requireMkfifo();
    Path fifo = FifoTestUtil.createTempFifo();
    try {
      ThreadPoolExecutor openPool = singleThreadPool("test-fifo-open");
      ThreadPoolExecutor writerPool = singleThreadPool("test-fifo-dummy-writer");
      FifoOpener opener = newInjectedOpener(openPool, writerPool);

      Rig rig = newRig();
      rig.fifoPath = fifo.toString();
      rig.realFifo = true;
      rig.injectedOpener = opener;
      rig.start();
      // a track whose FIFO read-open blocks in the native open (no writer yet):
      // the play is issued but activation is pending on the open rendezvous
      rig.daemon.play(Response.ok());
      CompletableFuture<Result> activation = rig.coordinator.start(URI, 0);
      await().atMost(Duration.ofSeconds(5)).until(() -> openPool.getActiveCount() == 1);

      long startNanos = System.nanoTime();
      rig.stopSeq.shutdown();
      long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
      assertThat(elapsedMillis).as("shutdown unblocks the native open within the bound")
          .isLessThan(10_000);

      // the open was cancelled via the dummy-writer rendezvous and the executors
      // fully drained — zero blocked opener threads (FifoOpenerTest pattern)
      assertThat(openPool.isTerminated()).as("open executor terminated").isTrue();
      assertThat(openPool.getPoolSize()).as("opener thread exited").isZero();
      assertThat(openPool.getActiveCount()).as("zero live opener tasks").isZero();
      assertThat(openPool.getCompletedTaskCount())
          .as("all submitted open tasks completed")
          .isEqualTo(openPool.getTaskCount());
      assertThat(writerPool.isTerminated()).as("writer executor terminated").isTrue();
      assertThat(writerPool.getPoolSize()).as("writer thread exited").isZero();
      assertThat(activation.get(5, TimeUnit.SECONDS)).isNotNull(); // start() completed
    } finally {
      FifoTestUtil.deleteTempFifo(fifo);
    }
  }

  // ==================================================================
  // Thread hygiene + exactly-once end reasons
  // ==================================================================

  @Test
  void repeatedStopPlayCyclesDoNotLeakThreads() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      // warm-up cycle so all lazy singleton threads exist before the baseline
      rig.daemon.play(Response.ok().emit("playing", playingData(URI)));
      rig.daemon.status(Response.ok(playingStatus(URI)));
      assertThat(rig.coordinator.start(URI, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();
      rig.daemon.pause(Response.ok().emit("paused", sharedData(URI)));
      assertThat(rig.stopSeq.logicalStop().get(5, TimeUnit.SECONDS).isOk()).isTrue();
      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY));

      long before = countThreads("golibrespot-");
      for (int i = 0; i < 5; i++) {
        rig.daemon.play(Response.ok().emit("playing", playingData(URI)));
        rig.daemon.status(Response.ok(playingStatus(URI)));
        assertThat(rig.coordinator.start(URI, 0).get(5, TimeUnit.SECONDS).isOk())
            .as("cycle " + i + " activation").isTrue();
        rig.daemon.pause(Response.ok().emit("paused", sharedData(URI)));
        assertThat(rig.stopSeq.logicalStop().get(5, TimeUnit.SECONDS).isOk())
            .as("cycle " + i + " stop").isTrue();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(
            () -> assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY));
      }

      // zero thread growth across the cycles (a lingering fifo-reader thread
      // exits within its bounded close join)
      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(countThreads("golibrespot-")).isLessThanOrEqualTo(before));
      assertThat(countThreads("golibrespot-stopseq"))
          .as("the stop sequence keeps exactly one daemon lane thread").isEqualTo(1);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY);
    }
  }

  @Test
  void stopEndpointNeverRecordedAcrossLifecycle() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      // activate → logical stop → activate → destroy → shutdown
      rig.daemon.play(Response.ok().emit("playing", playingData(URI)));
      rig.daemon.status(Response.ok(playingStatus(URI)));
      assertThat(rig.coordinator.start(URI, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();
      rig.daemon.pause(Response.ok().emit("paused", sharedData(URI)));
      assertThat(rig.stopSeq.logicalStop().get(5, TimeUnit.SECONDS).isOk()).isTrue();
      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY));

      rig.daemon.play(Response.ok().emit("playing", playingData(URI)));
      rig.daemon.status(Response.ok(playingStatus(URI)));
      assertThat(rig.coordinator.start(URI, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();
      assertThat(rig.stopSeq.destroy().get(5, TimeUnit.SECONDS).isOk()).isTrue();
      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY));

      rig.stopSeq.shutdown();

      assertThat(rig.daemon.getReceivedCommands())
          .filteredOn(c -> c.path().equals("/player/stop")).isEmpty();
    }
  }

  @Test
  void endReasonsReleaseLeaseExactlyOnce() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();

      // FINISHED — natural completion releases exactly once
      rig.daemon.play(Response.ok().emit("playing", playingData(URI)));
      rig.daemon.status(Response.ok(playingStatus(URI)));
      assertThat(rig.coordinator.start(URI, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();
      Lease finishedLease = rig.coordinator.currentLease();
      rig.daemon.status(Response.ok(idleStatus()));
      rig.daemon.emit("not_playing", sharedData(URI));
      await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
        assertThat(rig.machine.state()).isEqualTo(MachineState.READY);
        assertThat(rig.coordinator.currentLease()).isNull();
      });
      assertThat(finishedLease.isActive()).isFalse();
      finishedLease.release(); // second release — no-op, no exception
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY);

      // STOPPED — logical stop releases exactly once
      rig.daemon.play(Response.ok().emit("playing", playingData(URI)));
      rig.daemon.status(Response.ok(playingStatus(URI)));
      assertThat(rig.coordinator.start(URI, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();
      Lease stoppedLease = rig.coordinator.currentLease();
      rig.daemon.pause(Response.ok().emit("paused", sharedData(URI)));
      assertThat(rig.stopSeq.logicalStop().get(5, TimeUnit.SECONDS).isOk()).isTrue();
      // completion contract: the stop future completing OK implies the
      // coordinator already observed the release
      assertThat(rig.coordinator.currentLease()).isNull();
      assertThat(rig.coordinator.isActive()).isFalse();
      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY));
      assertThat(stoppedLease.isActive()).isFalse();
      stoppedLease.release(); // no-op
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY);

      // REPLACED — play-over-play keeps the lease held; a later STOPPED releases once
      rig.daemon.play(Response.ok().emit("playing", playingData(URI)));
      rig.daemon.status(Response.ok(playingStatus(URI)));
      assertThat(rig.coordinator.start(URI, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();
      rig.daemon.play(Response.ok().emit("playing", playingData(URI_B)));
      rig.daemon.status(Response.ok(playingStatus(URI_B)));
      assertThat(rig.coordinator.replace(URI_B, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();
      assertThat(rig.coordinator.currentLease().isActive()).as("replace keeps the lease").isTrue();
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.LEASED);
      Lease replacedLease = rig.coordinator.currentLease();
      rig.daemon.pause(Response.ok().emit("paused", sharedData(URI_B)));
      assertThat(rig.stopSeq.logicalStop().get(5, TimeUnit.SECONDS).isOk()).isTrue();
      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY));
      assertThat(replacedLease.isActive()).isFalse();
      replacedLease.release(); // no-op
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY);

      // CLEANUP — destroy releases exactly once
      rig.daemon.play(Response.ok().emit("playing", playingData(URI)));
      rig.daemon.status(Response.ok(playingStatus(URI)));
      assertThat(rig.coordinator.start(URI, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();
      Lease cleanupLease = rig.coordinator.currentLease();
      assertThat(rig.stopSeq.destroy().get(5, TimeUnit.SECONDS).isOk()).isTrue();
      // completion contract: the destroy future completing OK implies the
      // coordinator already observed the release
      assertThat(rig.coordinator.currentLease()).isNull();
      assertThat(rig.coordinator.isActive()).isFalse();
      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY));
      assertThat(cleanupLease.isActive()).isFalse();
      cleanupLease.release(); // no-op
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY);
    }
  }

  // ==================================================================
  // helpers
  // ==================================================================

  private static String playingData(String uri) {
    return FakeLibrespotDaemon.playingData("", uri, false, "go-librespot");
  }

  private static String sharedData(String uri) {
    return FakeLibrespotDaemon.sharedTrackData("", uri, "go-librespot");
  }

  private static String seekData(String uri, long position) {
    return FakeLibrespotDaemon.seekData("", uri, position, 240_000, "go-librespot");
  }

  private static String playingStatus(String uri) {
    return FakeLibrespotDaemon.statusJson(false, false,
        FakeLibrespotDaemon.trackJson(uri, "Track", List.of("Artist"), "Album", "",
            0, 240_000, "", 1, 1, "OGG_VORBIS_160", "vorbis", 160, 44100, null));
  }

  private static String idleStatus() {
    return FakeLibrespotDaemon.statusJson(true, false, null);
  }

  private static List<String> postPaths(List<RecordedCommand> commands) {
    return commands.stream()
        .filter(c -> c.method().equals("POST"))
        .map(RecordedCommand::path)
        .collect(Collectors.toList());
  }

  private static long countThreads(String namePrefix) {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(t -> t.isAlive() && t.getName() != null && t.getName().startsWith(namePrefix))
        .count();
  }

  private Rig newRig() {
    Rig rig = new Rig();
    rig.start();
    rigs.add(rig);
    return rig;
  }

  /** The full stack under test: fixture daemon + real REST + real WS + pool + machine + coordinator. */
  static final class Rig implements AutoCloseable {

    final FakeLibrespotDaemon daemon = new FakeLibrespotDaemon();
    final AtomicBoolean fifoReopenOk = new AtomicBoolean(true);
    final List<String> logLines = Collections.synchronizedList(new ArrayList<>());
    final LifecycleCoordinator.ListenerBridge bridge = new LifecycleCoordinator.ListenerBridge();
    final ScriptedSeam seam = new ScriptedSeam(
        () -> ScriptedSeam.immediate(new ByteArrayInputStream(new byte[0])));

    boolean realFifo;
    String fifoPath = "C:/tmp/alpha.fifo";
    FifoOpener injectedOpener; // for the Linux blocked-open test (inspectable executors)

    GoLibrespotConfig config;
    BackendConfig backend;
    GoLibrespotRestClient rest;
    ExclusivePool pool;
    EventsWebSocketClient ws;
    BackendStateMachine machine;
    LifecycleCoordinator coordinator;
    StopSequence stopSeq;
    FifoOpener opener;

    private PipedInputStream pipeIn;
    private PipedOutputStream pipeOut;

    void start() {
      try {
        daemon.start();
        config = GoLibrespotConfig.from(Map.of(
            "enabled", true,
            "backends", List.of(Map.of(
                "name", "alpha",
                "restBaseUrl", daemon.getHttpUrl(),
                "wsUrl", daemon.getWsUrl(),
                "fifoPath", fifoPath))));
        backend = config.getBackends().get(0);
        pool = new ExclusivePool(config.getBackends());
        rest = new GoLibrespotRestClient(backend.getRestBaseUrl(), 1500);
        machine = new BackendStateMachine(
            BackendHandle.of(backend), rest, pool, FAST, fifoReopenOk::get, bridge, logLines::add);
        opener = realFifo
            ? (injectedOpener != null ? injectedOpener : FifoOpener.create())
            : FifoOpener.create();
        coordinator = new LifecycleCoordinator(
            BackendHandle.of(backend), machine, rest, pool,
            realFifo ? FifoOpenerSeam.of(opener) : seam,
            FifoReader::new,
            FAST,
            new LifecycleCoordinator.Tuning(Duration.ofSeconds(5), Duration.ofSeconds(5)),
            logLines::add);
        bridge.setTarget(coordinator);
        ws = new EventsWebSocketClient(
            backend.getWsUrl(), coordinator.listener(), 40, 200, 5);
        machine.attachWebSocket(ws);
        ws.start();
        assertThat(daemon.awaitWsClients(1, Duration.ofSeconds(5))).isTrue();
        stopSeq = new StopSequence(machine, coordinator, ws, opener, pool, FAST, logLines::add);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    /** Creates a fresh PCM pipe and points the scripted seam at it. */
    PipedOutputStream newPipe() throws IOException {
      PipedInputStream in = new PipedInputStream(64 * 1024);
      PipedOutputStream out = new PipedOutputStream(in);
      pipeIn = in;
      pipeOut = out;
      seam.supplier = () -> ScriptedSeam.immediate(in);
      return out;
    }

    @Override
    public void close() {
      try {
        if (pipeOut != null) {
          pipeOut.close();
        }
      } catch (IOException ignored) {
        // best-effort: unblocks the reader
      }
      if (stopSeq != null) {
        stopSeq.close();
      }
      if (coordinator != null) {
        coordinator.close();
      }
      if (machine != null) {
        machine.close();
      }
      if (ws != null) {
        ws.close();
      }
      if (rest != null) {
        rest.close();
      }
      if (pool != null) {
        pool.shutdown();
      }
      if (opener != null) {
        opener.close();
      }
      daemon.stop();
    }
  }

  /** Injectable {@link FifoOpenerSeam} for pure-JVM tests. */
  static final class ScriptedSeam implements FifoOpenerSeam {
    volatile Supplier<FifoOpenerSeam.OpenHandleLike> supplier;

    ScriptedSeam(Supplier<FifoOpenerSeam.OpenHandleLike> supplier) {
      this.supplier = supplier;
    }

    @Override
    public FifoOpenerSeam.OpenHandleLike open(Path path, Duration timeout) {
      return supplier.get();
    }

    static FifoOpenerSeam.OpenHandleLike immediate(InputStream in) {
      return new ImmediateHandle(in);
    }

    static final class ImmediateHandle implements FifoOpenerSeam.OpenHandleLike {
      private final InputStream stream;
      private volatile boolean cancelled;

      ImmediateHandle(InputStream stream) {
        this.stream = stream;
      }

      @Override
      public InputStream await() {
        if (cancelled) {
          throw new CancellationException("cancelled");
        }
        return stream;
      }

      @Override
      public boolean cancel() {
        cancelled = true;
        return true;
      }

      @Override
      public boolean isDone() {
        return !cancelled;
      }

      @Override
      public boolean isCancelled() {
        return cancelled;
      }
    }
  }

  /** An open handle that never completes until cancelled (shutdown-during-open tests). */
  static final class NeverHandle implements FifoOpenerSeam.OpenHandleLike {
    private final CountDownLatch open = new CountDownLatch(1);
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private volatile boolean entered;

    @Override
    public InputStream await() throws InterruptedException, ExecutionException {
      entered = true;
      open.await();
      if (cancelled.get()) {
        throw new CancellationException("cancelled");
      }
      throw new ExecutionException(new IllegalStateException("no stream ever"));
    }

    boolean entered() {
      return entered;
    }

    @Override
    public boolean cancel() {
      cancelled.set(true);
      open.countDown();
      return true;
    }

    @Override
    public boolean isDone() {
      return cancelled.get();
    }

    @Override
    public boolean isCancelled() {
      return cancelled.get();
    }
  }
}
