package dev.lavalinkplugins.golibrespot.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import dev.lavalinkplugins.golibrespot.backend.rest.GoLibrespotRestClient;
import dev.lavalinkplugins.golibrespot.backend.ws.EventsWebSocketClient;
import dev.lavalinkplugins.golibrespot.config.BackendConfig;
import dev.lavalinkplugins.golibrespot.config.GoLibrespotConfig;
import dev.lavalinkplugins.golibrespot.fifo.FifoReader;
import dev.lavalinkplugins.golibrespot.fifo.FifoTestUtil;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.MachineState;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Outcome;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Result;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Timing;
import dev.lavalinkplugins.golibrespot.pool.BackendHandle;
import dev.lavalinkplugins.golibrespot.pool.BackendState;
import dev.lavalinkplugins.golibrespot.pool.ExclusivePool;
import dev.lavalinkplugins.golibrespot.testfixtures.FakeLibrespotDaemon;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

/**
 * Contract + acceptance tests for {@link LifecycleCoordinator} and
 * {@link ActivationBarrier} against the {@link FakeLibrespotDaemon} fixture
 * (T6), the real REST client (T8), the real reconnecting WS client (T9), the
 * real pool (T13) and the real state machine (T14).
 *
 * <p>The FIFO path is exercised in two ways: pure-JVM tests drive the real
 * {@link FifoReader} + {@link dev.lavalinkplugins.golibrespot.fifo.PcmDecoder}
 * over a scripted {@link PipedInputStream} via an injectable
 * {@link FifoOpenerSeam} (the reader factory seam — runs everywhere), while a
 * Linux-gated test uses the real {@link dev.lavalinkplugins.golibrespot.fifo.FifoOpener}
 * against a real mkfifo FIFO.</p>
 *
 * <p>Scenarios: activation success (lease at start, FIFO opened + reader
 * started, single /player/play, barrier satisfied, frames flow); activation
 * timeout → quarantine + track failed; replacement issues exactly one play (no
 * stop) and survives a stale-advance {@code not_playing} by re-issuing play;
 * natural completion → end-of-stream + exactly-once release; completion only
 * after activation; same-URI replay safe across generations; barrier contract
 * (waiting caller never gets null, unblocked by markActivated, typed failure on
 * quarantine); no double release under rapid start/stop; open failure and
 * close-during-open leak nothing.</p>
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class LifecycleCoordinatorTest {

  private static final String URI_A = "spotify:track:aaaaaaaaaaaaaaaaaaaaaa";
  private static final String URI_B = "spotify:track:bbbbbbbbbbbbbbbbbbbbbb";

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
  // Activation success
  // ==================================================================

  @Test
  void activationSuccessAcquiresLeaseOpensFifoStartsReaderSinglePlayAndDeliversFrames()
      throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));

      Result result = rig.coordinator.start(URI_A, 0).get(5, TimeUnit.SECONDS);

      assertThat(result.isOk()).as("activation result").isTrue();
      assertThat(rig.machine.state()).isEqualTo(MachineState.LEASED);
      assertThat(rig.machine.phase()).isEqualTo(BackendStateMachine.Phase.PLAYING);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.LEASED);
      assertThat(rig.coordinator.currentLease()).isNotNull();
      assertThat(rig.coordinator.currentLease().isActive()).isTrue();
      assertThat(rig.coordinator.expectedUri()).isEqualTo(URI_A);
      assertThat(rig.coordinator.generation()).isEqualTo(rig.machine.generation());
      assertThat(rig.coordinator.currentReader().isRunning()).isTrue();

      // exactly one play command, and never a pause or the stop endpoint
      List<FakeLibrespotDaemon.RecordedCommand> commands = rig.daemon.getReceivedCommands();
      assertThat(commands).filteredOn(c -> c.path().equals("/player/play")).hasSize(1);
      assertThat(commands).filteredOn(c -> c.path().equals("/player/pause")).isEmpty();
      assertThat(commands).filteredOn(c -> c.path().equals("/player/stop")).isEmpty();

      // the barrier is satisfied and the frames flow from the opened stream
      rig.coordinator.awaitActivated(Duration.ofSeconds(2));
      short[] golden = golden(200);
      rig.pipeOut().write(pcmBytes(golden));
      rig.pipeOut().flush();
      assertThat(drainFrames(rig.coordinator, golden.length, Duration.ofSeconds(3)))
          .containsExactly(golden);
    }
  }

  // ==================================================================
  // Activation failure
  // ==================================================================

  @Test
  void activationTimeoutQuarantinesAndFailsTrackWithTypedException() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()); // 200, no events ever
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(idleStatus()));

      Result result = rig.coordinator.start(URI_A, 0).get(5, TimeUnit.SECONDS);

      assertThat(result.outcome()).isEqualTo(Outcome.QUARANTINED);
      assertThat(rig.machine.state()).isEqualTo(MachineState.QUARANTINING);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.QUARANTINING);
      assertThat(rig.coordinator.currentLease()).isNull();

      // the barrier throws the typed activation exception — never null
      assertThatThrownBy(() -> rig.coordinator.awaitActivated(Duration.ofSeconds(1)))
          .isInstanceOf(ActivationException.class)
          .extracting(e -> ((ActivationException) e).kind())
          .isEqualTo(ActivationException.Kind.QUARANTINED);
    }
  }

  @Test
  void fifoOpenImmediateFailureFailsTrackAndReleasesLeaseWithoutQuarantine() throws Exception {
    try (Rig rig = newRig()) {
      rig.seam.supplier = () -> {
        throw new IllegalStateException("no such fifo");
      };

      Result result = rig.coordinator.start(URI_A, 0).get(5, TimeUnit.SECONDS);

      assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
      // the machine was never touched — the lease returns to the pool, READY
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY);
      assertThat(rig.machine.state()).isEqualTo(MachineState.READY);
      assertThat(rig.coordinator.currentLease()).isNull();
      assertThatThrownBy(() -> rig.coordinator.awaitActivated(Duration.ofMillis(100)))
          .isInstanceOf(ActivationException.class);
    }
  }

  @Test
  void fifoOpenAwaitFailureQuarantinesAndReleasesLease() throws Exception {
    try (Rig rig = newRig()) {
      rig.seam.supplier = () -> ScriptedSeam.failing(new FileNotFoundException("nope"));

      Result result = rig.coordinator.start(URI_A, 0).get(5, TimeUnit.SECONDS);

      assertThat(result.outcome()).isEqualTo(Outcome.QUARANTINED);
      assertThat(rig.machine.state()).isEqualTo(MachineState.QUARANTINING);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.QUARANTINING);
      assertThat(rig.coordinator.currentLease()).isNull();
    }
  }

  // ==================================================================
  // Replacement (play-over-play)
  // ==================================================================

  @Test
  void replacementIssuesExactlyOnePlayNoStopAndReusesFifoStream() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      assertThat(rig.coordinator.start(URI_A, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();

      // play-over-play replacement to B: exactly one play for B, no pause/stop
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_B)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_B)));

      Result replaced = rig.coordinator.replace(URI_B, 0).get(5, TimeUnit.SECONDS);

      assertThat(replaced.isOk()).as("replacement activation: " + replaced).isTrue();
      assertThat(rig.machine.state()).isEqualTo(MachineState.LEASED);
      assertThat(rig.machine.phase()).isEqualTo(BackendStateMachine.Phase.PLAYING);
      assertThat(rig.machine.generation()).isEqualTo(2);
      assertThat(rig.coordinator.expectedUri()).isEqualTo(URI_B);
      assertThat(rig.coordinator.currentLease().isActive()).isTrue();

      List<FakeLibrespotDaemon.RecordedCommand> commands = rig.daemon.getReceivedCommands();
      assertThat(commands).filteredOn(c -> c.path().equals("/player/play")).hasSize(2);
      assertThat(commands).filteredOn(c -> c.path().equals("/player/pause")).isEmpty();
      assertThat(commands).filteredOn(c -> c.path().equals("/player/stop")).isEmpty();

      // the same FIFO stream keeps flowing for the new track (reader is reused)
      short[] goldenB = golden(200);
      rig.pipeOut().write(pcmBytes(goldenB));
      rig.pipeOut().flush();
      assertThat(drainFrames(rig.coordinator, goldenB.length, Duration.ofSeconds(3)))
          .containsExactly(goldenB);
    }
  }

  @Test
  void replacementStaleAdvanceReissuesPlay() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      assertThat(rig.coordinator.start(URI_A, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();

      // play-over-play to B: only the stale-advance not_playing(B) is emitted by the
      // play response (no playing yet) — the coordinator must re-issue the play
      // (idempotent reload) instead of releasing; the delayed playing(B) confirms.
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("not_playing", sharedData(URI_B)));
      rig.daemon.emitAfter("playing", playingData(URI_B), 400);
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_B)));

      Result replaced = rig.coordinator.replace(URI_B, 0).get(5, TimeUnit.SECONDS);

      assertThat(replaced.isOk()).as("replacement survives stale-advance re-issue: " + replaced)
          .isTrue();
      assertThat(rig.machine.state()).isEqualTo(MachineState.LEASED);
      assertThat(rig.machine.phase()).isEqualTo(BackendStateMachine.Phase.PLAYING);
      assertThat(rig.machine.generation()).isEqualTo(2);

      List<FakeLibrespotDaemon.RecordedCommand> commands = rig.daemon.getReceivedCommands();
      // A, B, and the re-issued B
      assertThat(commands).filteredOn(c -> c.path().equals("/player/play")).hasSize(3);
      assertThat(commands).filteredOn(c -> c.path().equals("/player/pause")).isEmpty();
      assertThat(commands).filteredOn(c -> c.path().equals("/player/stop")).isEmpty();
    }
  }

  @Test
  void replaceWithoutHeldLeaseFails() throws Exception {
    try (Rig rig = newRig()) {
      Result result = rig.coordinator.replace(URI_B, 0).get(5, TimeUnit.SECONDS);
      assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
    }
  }

  // ==================================================================
  // Natural completion
  // ==================================================================

  @Test
  void naturalCompletionSignalsEndOfStreamAndReleasesExactlyOnce() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      assertThat(rig.coordinator.start(URI_A, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();

      // natural end: daemon goes idle then emits not_playing
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(idleStatus()));
      rig.daemon.emit("not_playing", sharedData(URI_A));

      await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
        assertThat(rig.machine.state()).isEqualTo(MachineState.READY);
        assertThat(rig.coordinator.currentLease()).isNull();
      });
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY);
      assertThat(rig.coordinator.isActive()).isFalse();
      assertThat(rig.coordinator.currentReader()).isNull();

      // end-of-stream: process() terminates with null, never while the barrier was pending
      assertThat(rig.coordinator.nextFrame(Duration.ofMillis(100))).isNull();
    }
  }

  @Test
  void completionOnlyAfterActivationIsIgnored() throws Exception {
    try (Rig rig = newRig()) {
      // a stale not_playing arrives while the coordinator is idle (no session) —
      // it must not prematurely end anything
      rig.daemon.emit("not_playing", sharedData(URI_A));
      await().atMost(Duration.ofSeconds(2)).untilAsserted(
          () -> assertThat(rig.machine.ignoredEvents()).isGreaterThan(0));

      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      Result result = rig.coordinator.start(URI_A, 0).get(5, TimeUnit.SECONDS);

      assertThat(result.isOk()).as("activation after a stale not_playing").isTrue();
      assertThat(rig.coordinator.isActive()).isTrue();
      assertThat(rig.coordinator.currentLease().isActive()).isTrue();
      assertThat(rig.machine.state()).isEqualTo(MachineState.LEASED);
    }
  }

  // ==================================================================
  // Same-URI replay across generations
  // ==================================================================

  @Test
  void sameUriReplaySafeAcrossGenerations() throws Exception {
    try (Rig rig = newRig()) {
      // generation 1: play A, complete naturally
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      assertThat(rig.coordinator.start(URI_A, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();
      assertThat(rig.machine.generation()).isEqualTo(1);

      rig.daemon.status(FakeLibrespotDaemon.Response.ok(idleStatus()));
      rig.daemon.emit("not_playing", sharedData(URI_A));
      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(rig.machine.state()).isEqualTo(MachineState.READY));

      // generation 2: same URI; a stale not_playing from gen 1 replays during
      // activation and must not end the new lease, then the real playing confirms
      rig.daemon.play(FakeLibrespotDaemon.Response.ok());
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      CompletableFuture<Result> activation = rig.coordinator.start(URI_A, 0);
      rig.daemon.awaitCommands(2, Duration.ofSeconds(5)); // gen1 play + gen2 play received
      rig.daemon.emit("not_playing", sharedData(URI_A)); // stale replay of gen 1 completion
      rig.daemon.emit("playing", playingData(URI_A));

      Result result = activation.get(5, TimeUnit.SECONDS);
      assertThat(result.isOk()).as("second activation survives the stale replay").isTrue();
      assertThat(rig.machine.state()).isEqualTo(MachineState.LEASED);
      assertThat(rig.machine.phase()).isEqualTo(BackendStateMachine.Phase.PLAYING);
      assertThat(rig.machine.generation()).isEqualTo(2);
      assertThat(rig.coordinator.expectedUri()).isEqualTo(URI_A);
      assertThat(rig.coordinator.currentLease().isActive()).isTrue();
    }
  }

  // ==================================================================
  // Barrier contract
  // ==================================================================

  @Test
  void barrierWaiterNeverGetsNullAndIsUnblockedByActivation() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()); // 200, no events yet
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      CompletableFuture<Result> activation = rig.coordinator.start(URI_A, 0);
      rig.daemon.awaitCommands(1, Duration.ofSeconds(5)); // play received → ACTIVATING

      AtomicReference<Object> outcome = new AtomicReference<>();
      Thread waiter = new Thread(() -> {
        try {
          outcome.set(rig.coordinator.nextFrame(Duration.ofSeconds(3)));
        } catch (Exception e) {
          outcome.set(e);
        }
      });
      waiter.start();

      // while the barrier is pending the caller must be blocked — never null
      await().atMost(Duration.ofSeconds(1)).untilAsserted(
          () -> assertThat(waiter.getState()).isIn(Thread.State.WAITING, Thread.State.TIMED_WAITING));
      assertThat(outcome.get()).isNull(); // still blocked, nothing returned

      // activation unblocks the waiter; it gets frames (empty — no PCM), never null
      rig.daemon.emit("playing", playingData(URI_A));
      assertThat(activation.get(5, TimeUnit.SECONDS).isOk()).isTrue();
      waiter.join(3_000);
      assertThat(outcome.get()).isNotNull();
      assertThat(outcome.get()).isNotInstanceOf(Throwable.class);
    }
  }

  @Test
  void barrierMarkActivatedUnblocksAndFailThrowsTyped() throws Exception {
    // pure unit: a caller blocked on the barrier is unblocked by markActivated
    ActivationBarrier barrier = new ActivationBarrier(1, URI_A);
    AtomicBoolean ok = new AtomicBoolean();
    Thread waiter = new Thread(() -> {
      try {
        barrier.awaitActivated(Duration.ofSeconds(5));
        ok.set(true);
      } catch (Exception ignored) {
        // unexpected
      }
    });
    waiter.start();
    await().atMost(Duration.ofSeconds(1)).until(
        () -> waiter.getState() == Thread.State.WAITING
            || waiter.getState() == Thread.State.TIMED_WAITING);
    barrier.markActivated();
    waiter.join(2_000);
    assertThat(ok).isTrue();
    assertThat(barrier.isSatisfied()).isTrue();

    // a second await returns immediately
    barrier.awaitActivated(Duration.ofSeconds(1));

    // typed failure on quarantine
    ActivationBarrier failed = new ActivationBarrier(1, URI_A);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread waiter2 = new Thread(() -> {
      try {
        failed.awaitActivated(Duration.ofSeconds(5));
      } catch (Exception e) {
        failure.set(e);
      }
    });
    waiter2.start();
    await().atMost(Duration.ofSeconds(1)).until(
        () -> waiter2.getState() == Thread.State.WAITING
            || waiter2.getState() == Thread.State.TIMED_WAITING);
    failed.fail(ActivationException.Kind.QUARANTINED, "boom");
    waiter2.join(2_000);
    assertThat(failure.get()).isInstanceOf(ActivationException.class);
    assertThat(((ActivationException) failure.get()).kind())
        .isEqualTo(ActivationException.Kind.QUARANTINED);
    assertThat(failed.isFailed()).isTrue();
    assertThat(failed.failureReason()).isEqualTo("boom");

    // awaiting a failed barrier re-throws immediately
    assertThatThrownBy(() -> failed.awaitActivated(Duration.ofSeconds(1)))
        .isInstanceOf(ActivationException.class)
        .extracting(e -> ((ActivationException) e).kind())
        .isEqualTo(ActivationException.Kind.QUARANTINED);
  }

  // ==================================================================
  // Exactly-once release under churn
  // ==================================================================

  @Test
  void noDoubleReleaseUnderRapidStartStop() throws Exception {
    try (Rig rig = newRig()) {
      for (int i = 1; i <= 4; i++) {
        rig.daemon.play(FakeLibrespotDaemon.Response.ok()
            .emit("playing", playingData(URI_A)));
        rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
        assertThat(rig.coordinator.start(URI_A, 0).get(5, TimeUnit.SECONDS).isOk())
            .as("session " + i + " activation").isTrue();
        assertThat(rig.machine.generation()).isEqualTo(i);

        // logical stop (T17 style): pause-ack then retire
        rig.daemon.pause(FakeLibrespotDaemon.Response.ok()
            .emit("paused", sharedData(URI_A)));
        assertThat(rig.machine.pause().get(5, TimeUnit.SECONDS).isOk()).isTrue();
        assertThat(rig.machine.retire().get(5, TimeUnit.SECONDS).isOk()).isTrue();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(
            () -> assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY));
        assertThat(rig.coordinator.currentLease()).isNull();
        assertThat(rig.coordinator.isActive()).isFalse();
      }
      // the pool is still healthy and hands out a fresh lease (no corruption)
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY);
    }
  }

  @Test
  void startWhileActivationInFlightFailsWithoutLeasing() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()); // no events yet
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      CompletableFuture<Result> first = rig.coordinator.start(URI_A, 0);
      rig.daemon.awaitCommands(1, Duration.ofSeconds(5)); // play received → ACTIVATING
      rig.daemon.emit("playing", playingData(URI_A)); // confirm before the machine's timeout

      // a second start while the first session is still active must fail without leasing
      Result second = rig.coordinator.start(URI_B, 0).get(5, TimeUnit.SECONDS);
      assertThat(second.outcome()).isEqualTo(Outcome.FAILED);

      assertThat(first.get(5, TimeUnit.SECONDS).isOk()).isTrue();
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.LEASED);
      assertThat(rig.coordinator.currentLease().isActive()).isTrue();
    }
  }

  // ==================================================================
  // Close-during-open
  // ==================================================================

  @Test
  void closeCancelsInFlightOpenWithoutHang() throws Exception {
    Rig rig = newRig();
    ScriptedSeam.NeverHandle never = new ScriptedSeam.NeverHandle();
    rig.seam.supplier = () -> never;
    rig.daemon.play(FakeLibrespotDaemon.Response.ok());

    CompletableFuture<Result> activation = rig.coordinator.start(URI_A, 0);
    await().atMost(Duration.ofSeconds(3)).until(() -> rig.seam.lastOpen() != null);

    rig.close();
    assertThat(never.isCancelled()).isTrue();
    assertThat(activation.get(5, TimeUnit.SECONDS)).isNotNull();
    // the lease never leaked: the machine invalidated it on the abort quarantine
    assertThat(rig.pool.stateOf("alpha")).isNotEqualTo(BackendState.LEASED);
    rigs.remove(rig);
  }

  // ==================================================================
  // Real FIFO (Linux only)
  // ==================================================================

  @EnabledOnOs(OS.LINUX)
  @Test
  void realFifoOpensStartsReaderAndDeliversFrames() throws Exception {
    FifoTestUtil.requireMkfifo();
    Path fifo = FifoTestUtil.createTempFifo();
    try {
      try (Rig rig = newRig()) {
        rig.fifoPath = fifo.toString();
        rig.realFifo = true;
        rig.start();
        rigs.add(rig);

        rig.daemon.play(FakeLibrespotDaemon.Response.ok()
            .emit("playing", playingData(URI_A)));
        rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));

        CompletableFuture<Result> activation = rig.coordinator.start(URI_A, 0);
        // the daemon-equivalent writer is GATED on the play command, mirroring the
        // real wait_for_reader=true daemon: its write-open (which rendezvouses with
        // our read-open) fires only as a consequence of the play being issued.
        List<FakeLibrespotDaemon.RecordedCommand> beforeWriter =
            rig.daemon.awaitCommands(1, Duration.ofSeconds(5));
        assertThat(beforeWriter).isNotEmpty();
        assertThat(beforeWriter.get(0).path()).as("play is the first recorded command").isEqualTo("/player/play");
        CompletableFuture<OutputStream> writer = CompletableFuture.supplyAsync(() -> {
          try {
            return new FileOutputStream(fifo.toFile());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });

        assertThat(activation.get(5, TimeUnit.SECONDS).isOk()).isTrue();
        OutputStream out = writer.get(5, TimeUnit.SECONDS);

        short[] golden = golden(200);
        out.write(pcmBytes(golden));
        out.flush();
        assertThat(drainFrames(rig.coordinator, golden.length, Duration.ofSeconds(3)))
            .containsExactly(golden);
        out.close();
      }
    } finally {
      FifoTestUtil.deleteTempFifo(fifo);
    }
  }

  // ==================================================================
  // FIFO rendezvous ordering (play before awaiting the open)
  // ==================================================================

  /**
   * Cross-platform ordering proof (runs on Windows too): the FIFO open is
   * awaited only AFTER the play command has reached the daemon. The gated seam
   * blocks its {@code await()} until the fake daemon has recorded a play — with
   * the play issued first, the open completes and activation succeeds; with the
   * buggy await-before-play order the coordinator lane would deadlock in the
   * open (play never issued) and this test would time out.
   */
  @Test
  void playIsIssuedBeforeAwaitingFifoOpen() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      ScriptedSeam.GatedHandle gated = new ScriptedSeam.GatedHandle(rig.daemon, rig.pipeIn());
      rig.seam.supplier = () -> gated;
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));

      Result result = rig.coordinator.start(URI_A, 0).get(5, TimeUnit.SECONDS);

      assertThat(result.isOk()).as("activation with play-gated FIFO open: " + result).isTrue();
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.LEASED);
      assertThat(rig.machine.state()).isEqualTo(MachineState.LEASED);
      assertThat(gated.awaitEntered()).as("the FIFO open was awaited").isTrue();
      assertThat(gated.playObservedBeforeReturn())
          .as("the play reached the daemon while the open was still pending").isTrue();
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

  private static String playingStatus(String uri) {
    return FakeLibrespotDaemon.statusJson(false, false,
        FakeLibrespotDaemon.trackJson(uri, "Track", List.of("Artist"), "Album", "",
            0, 240_000, "", 1, 1, "OGG_VORBIS_160", "vorbis", 160, 44100, null));
  }

  private static String idleStatus() {
    return FakeLibrespotDaemon.statusJson(true, false, null);
  }

  /** A deterministic golden L/R sample sequence (frame-aligned, s16le-encodable). */
  private static short[] golden(int count) {
    short[] out = new short[count];
    for (int i = 0; i < count; i++) {
      out[i] = (short) ((i % 2 == 0) ? i : -i);
    }
    return out;
  }

  private static byte[] pcmBytes(short[] shorts) {
    ByteBuffer bb = ByteBuffer.allocate(shorts.length * 2).order(ByteOrder.LITTLE_ENDIAN);
    for (short s : shorts) {
      bb.putShort(s);
    }
    return bb.array();
  }

  /**
   * Accumulates decoded frames until {@code expectedShorts} shorts have been
   * decoded (or the timeout elapses) — returns exactly the expected prefix.
   * Chunks are not frame-aligned, so accumulation is required.
   */
  private static short[] drainFrames(LifecycleCoordinator coordinator, int expectedShorts,
                                     Duration timeout) throws Exception {
    List<Short> acc = new ArrayList<>();
    long deadline = System.nanoTime() + timeout.toNanos();
    while (acc.size() < expectedShorts && System.nanoTime() < deadline) {
      short[] frames = coordinator.nextFrame(Duration.ofMillis(100));
      if (frames == null) {
        break; // end-of-stream
      }
      for (short s : frames) {
        acc.add(s);
      }
    }
    assertThat(acc.size()).as("decoded enough frames before timeout").isGreaterThanOrEqualTo(expectedShorts);
    short[] out = new short[expectedShorts];
    for (int i = 0; i < expectedShorts; i++) {
      out[i] = acc.get(i);
    }
    return out;
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

    GoLibrespotConfig config;
    BackendConfig backend;
    GoLibrespotRestClient rest;
    ExclusivePool pool;
    EventsWebSocketClient ws;
    BackendStateMachine machine;
    LifecycleCoordinator coordinator;
    dev.lavalinkplugins.golibrespot.fifo.FifoOpener opener;

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
        if (realFifo) {
          opener = dev.lavalinkplugins.golibrespot.fifo.FifoOpener.create();
        }
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
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    /** Creates a fresh PCM pipe and points the seam at it. */
    PipedOutputStream newPipe() throws IOException {
      PipedInputStream in = new PipedInputStream(64 * 1024);
      PipedOutputStream out = new PipedOutputStream(in);
      pipeIn = in;
      pipeOut = out;
      seam.supplier = () -> ScriptedSeam.immediate(in);
      return out;
    }

    PipedOutputStream pipeOut() {
      return pipeOut;
    }

    PipedInputStream pipeIn() {
      return pipeIn;
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
    private volatile FifoOpenerSeam.OpenHandleLike lastOpen;

    ScriptedSeam(Supplier<FifoOpenerSeam.OpenHandleLike> supplier) {
      this.supplier = supplier;
    }

    @Override
    public FifoOpenerSeam.OpenHandleLike open(Path path, Duration timeout) {
      FifoOpenerSeam.OpenHandleLike handle = supplier.get();
      lastOpen = handle;
      return handle;
    }

    FifoOpenerSeam.OpenHandleLike lastOpen() {
      return lastOpen;
    }

    static FifoOpenerSeam.OpenHandleLike immediate(InputStream in) {
      return new ImmediateHandle(in);
    }

    static FifoOpenerSeam.OpenHandleLike failing(Throwable cause) {
      return new FailingHandle(cause);
    }

    /**
     * A handle whose {@code await()} completes only after the fake daemon has
     * recorded a play command — the ordering regression seam. With the correct
     * play-before-await-open order the coordinator's play unblocks it and
     * activation succeeds; with the buggy await-before-play order it blocks
     * forever (play is never issued) and the test times out.
     */
    static final class GatedHandle implements FifoOpenerSeam.OpenHandleLike {
      private final FakeLibrespotDaemon daemon;
      private final InputStream stream;
      private final AtomicBoolean cancelled = new AtomicBoolean();
      private volatile boolean awaitEntered;
      private volatile boolean playObservedBeforeReturn;

      GatedHandle(FakeLibrespotDaemon daemon, InputStream stream) {
        this.daemon = daemon;
        this.stream = stream;
      }

      @Override
      public InputStream await() throws InterruptedException {
        awaitEntered = true;
        while (!playRecorded() && !cancelled.get()) {
          Thread.sleep(5); // test-only poll; the coordinator lane is interrupted by close()
        }
        if (cancelled.get()) {
          throw new CancellationException("cancelled");
        }
        playObservedBeforeReturn = true;
        return stream;
      }

      private boolean playRecorded() {
        for (FakeLibrespotDaemon.RecordedCommand c : daemon.getReceivedCommands()) {
          if (c.path().equals("/player/play")) {
            return true;
          }
        }
        return false;
      }

      boolean awaitEntered() {
        return awaitEntered;
      }

      boolean playObservedBeforeReturn() {
        return playObservedBeforeReturn;
      }

      @Override
      public boolean cancel() {
        cancelled.set(true);
        return true;
      }

      @Override
      public boolean isDone() {
        return cancelled.get() || playObservedBeforeReturn;
      }

      @Override
      public boolean isCancelled() {
        return cancelled.get();
      }
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

    static final class FailingHandle implements FifoOpenerSeam.OpenHandleLike {
      private final Throwable cause;

      FailingHandle(Throwable cause) {
        this.cause = cause;
      }

      @Override
      public InputStream await() throws ExecutionException {
        throw new ExecutionException(cause);
      }

      @Override
      public boolean cancel() {
        return false;
      }

      @Override
      public boolean isDone() {
        return true;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }
    }

    static final class NeverHandle implements FifoOpenerSeam.OpenHandleLike {
      private final CountDownLatch open = new CountDownLatch(1);
      private volatile boolean cancelled;

      @Override
      public InputStream await() throws InterruptedException, ExecutionException {
        open.await();
        if (cancelled) {
          throw new CancellationException("cancelled");
        }
        throw new ExecutionException(new IllegalStateException("no stream ever"));
      }

      @Override
      public boolean cancel() {
        cancelled = true;
        open.countDown();
        return true;
      }

      @Override
      public boolean isDone() {
        return cancelled;
      }

      @Override
      public boolean isCancelled() {
        return cancelled;
      }
    }
  }
}
