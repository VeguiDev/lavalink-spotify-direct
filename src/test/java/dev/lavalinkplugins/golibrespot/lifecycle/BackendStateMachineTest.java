package dev.lavalinkplugins.golibrespot.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.lavalinkplugins.golibrespot.backend.model.PlayerCommandResult;
import dev.lavalinkplugins.golibrespot.backend.rest.GoLibrespotRestClient;
import dev.lavalinkplugins.golibrespot.backend.ws.EventsWebSocketClient;
import dev.lavalinkplugins.golibrespot.config.BackendConfig;
import dev.lavalinkplugins.golibrespot.config.GoLibrespotConfig;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Decision;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.DecisionInput;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Contract + poison-sequence tests for {@link BackendStateMachine} against the
 * {@link FakeLibrespotDaemon} fixture (T6), the real REST client (T8) and the
 * real reconnecting WS client (T9), plus direct truth-table tests of the
 * quarantine decision function.
 *
 * <p>Poison sequences covered (DECISIONS.md / plan): stale play after stop
 * ignored, delayed activation handled, 200-but-noop detected, WS loss
 * mid-command, status-contradicts-events permanent quarantine, matching
 * not_playing completion, wrong-URI not_playing ignored, same-URI replay safe
 * across generations, stop-taint permanent, seek ack mismatch, paused-then-late
 * not_playing (stop-race analog, no crash), hang, unparseable status,
 * connect-refused → dead, and the re-admission path (fresh WS + idle status +
 * FIFO reopen predicate → READY).</p>
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class BackendStateMachineTest {

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
  // Quarantine decision table — pure function, every row tested
  // ==================================================================

  private static DecisionInput input(boolean taint, boolean contra, boolean wsFresh,
                                     boolean statusIdle, boolean fifoOk, int fails,
                                     int threshold, boolean unreachable) {
    return new DecisionInput(taint, contra, wsFresh, statusIdle, fifoOk, fails, threshold, unreachable);
  }

  @Test
  void decisionTableStopTaintAlwaysDegrades() {
    assertThat(BackendStateMachine.decide(input(true, false, true, true, true, 0, 5, false)))
        .isEqualTo(Decision.DEGRADE);
    assertThat(BackendStateMachine.decide(input(true, true, true, true, true, 0, 5, false)))
        .isEqualTo(Decision.DEGRADE);
  }

  @Test
  void decisionTableContradictoryStateAlwaysDegrades() {
    assertThat(BackendStateMachine.decide(input(false, true, true, true, true, 0, 5, false)))
        .isEqualTo(Decision.DEGRADE);
    assertThat(BackendStateMachine.decide(input(false, true, false, false, false, 7, 5, false)))
        .isEqualTo(Decision.DEGRADE);
  }

  @Test
  void decisionTableFreshWsIdleStatusAndFifoReopenReuses() {
    assertThat(BackendStateMachine.decide(input(false, false, true, true, true, 0, 5, false)))
        .isEqualTo(Decision.REUSE);
    assertThat(BackendStateMachine.decide(input(false, false, true, true, true, 9, 5, false)))
        .isEqualTo(Decision.REUSE);
  }

  @Test
  void decisionTableRequiresEveryReuseGate() {
    // stale WS connection → still quarantined
    assertThat(BackendStateMachine.decide(input(false, false, false, true, true, 0, 5, false)))
        .isEqualTo(Decision.QUARANTINE);
    // status not idle → still quarantined
    assertThat(BackendStateMachine.decide(input(false, false, true, false, true, 0, 5, false)))
        .isEqualTo(Decision.QUARANTINE);
    // FIFO not reopened → still quarantined
    assertThat(BackendStateMachine.decide(input(false, false, true, true, false, 0, 5, false)))
        .isEqualTo(Decision.QUARANTINE);
  }

  @Test
  void decisionTableQuarantineThresholdKeepsQuarantining() {
    assertThat(BackendStateMachine.decide(input(false, false, false, false, false, 5, 5, false)))
        .isEqualTo(Decision.QUARANTINE);
    assertThat(BackendStateMachine.decide(input(false, false, false, false, false, 12, 5, false)))
        .isEqualTo(Decision.QUARANTINE);
  }

  @Test
  void decisionTableDefaultIsQuarantine() {
    assertThat(BackendStateMachine.decide(input(false, false, false, false, false, 0, 5, false)))
        .isEqualTo(Decision.QUARANTINE);
  }

  @Test
  void decisionTableProcessUnreachableIsDead() {
    assertThat(BackendStateMachine.decide(input(false, false, false, false, false, 0, 5, true)))
        .isEqualTo(Decision.DEAD);
    assertThat(BackendStateMachine.decide(input(true, false, true, true, true, 0, 5, true)))
        .isEqualTo(Decision.DEAD);
  }

  // ==================================================================
  // Activation + natural completion
  // ==================================================================

  @Test
  void activateConfirmsOnMatchingPlayingEvent() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));

      Lease lease = rig.lease();
      Result result = activate(rig, lease, URI_A);

      assertThat(result.isOk()).as("activation result").isTrue();
      assertThat(rig.machine.state()).isEqualTo(MachineState.LEASED);
      assertThat(rig.machine.phase()).isEqualTo(Phase.PLAYING);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.LEASED);
      assertThat(rig.daemon.getReceivedCommands())
          .anyMatch(c -> c.path().equals("/player/play"));
    }
  }

  @Test
  void notPlayingMatchingUriCompletesAndReleasesLease() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      Lease lease = rig.lease();
      assertThat(activate(rig, lease, URI_A).isOk()).isTrue();

      // natural end: the daemon goes idle, then emits not_playing
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(idleStatus()));
      rig.daemon.emit("not_playing", sharedData(URI_A));

      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(rig.machine.state()).isEqualTo(MachineState.READY));
      assertThat(rig.machine.phase()).isEqualTo(Phase.IDLE);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY);
      assertThat(lease.isActive()).isFalse();
      assertThat(rig.listener.events).contains("completed");
    }
  }

  @Test
  void notPlayingWrongUriIgnored() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      Lease lease = rig.lease();
      assertThat(activate(rig, lease, URI_A).isOk()).isTrue();

      rig.daemon.emit("not_playing", sharedData(URI_B));

      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(rig.machine.ignoredEvents()).isGreaterThan(0));
      assertThat(rig.machine.state()).isEqualTo(MachineState.LEASED);
      assertThat(rig.machine.phase()).isEqualTo(Phase.PLAYING);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.LEASED);
    }
  }

  @Test
  void sameUriReplaySafeAcrossGenerations() throws Exception {
    try (Rig rig = newRig()) {
      // generation 1: play A, complete naturally
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      Lease lease1 = rig.lease();
      assertThat(lease1.generation()).isEqualTo(1);
      assertThat(activate(rig, lease1, URI_A).isOk()).isTrue();

      rig.daemon.status(FakeLibrespotDaemon.Response.ok(idleStatus()));
      rig.daemon.emit("not_playing", sharedData(URI_A));
      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(rig.machine.state()).isEqualTo(MachineState.READY));

      // generation 2: same URI again; a stale not_playing from gen 1 replays
      // during activation and must be dropped, then the real playing confirms
      rig.daemon.play(FakeLibrespotDaemon.Response.ok());
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      Lease lease2 = rig.lease();
      assertThat(lease2.generation()).isEqualTo(2);

      CompletableFuture<Result> activation = rig.machine.activate(lease2, URI_A, 0);
      rig.daemon.emit("not_playing", sharedData(URI_A)); // stale replay
      rig.daemon.emit("playing", playingData(URI_A));

      Result result = activation.get(5, TimeUnit.SECONDS);
      assertThat(result.isOk()).as("second activation survives the stale replay").isTrue();
      assertThat(rig.machine.state()).isEqualTo(MachineState.LEASED);
      assertThat(rig.machine.phase()).isEqualTo(Phase.PLAYING);
      assertThat(rig.machine.generation()).isEqualTo(2);
    }
  }

  @Test
  void stalePlayAfterStopIgnored() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      Lease lease = rig.lease();
      assertThat(activate(rig, lease, URI_A).isOk()).isTrue();

      // T17 logical stop: pause-ack then retire; generation is retired
      rig.daemon.pause(FakeLibrespotDaemon.Response.ok()
          .emit("paused", sharedData(URI_A)));
      assertThat(rig.machine.pause().get(5, TimeUnit.SECONDS).isOk()).isTrue();
      assertThat(rig.machine.retire().get(5, TimeUnit.SECONDS).isOk()).isTrue();
      assertThat(rig.machine.state()).isEqualTo(MachineState.READY);

      // a stale playing event for the retired URI must be ignored
      rig.daemon.emit("playing", playingData(URI_A));
      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(rig.machine.ignoredEvents()).isGreaterThan(0));
      assertThat(rig.machine.state()).isEqualTo(MachineState.READY);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY);
    }
  }

  // ==================================================================
  // Activation failure modes
  // ==================================================================

  @Test
  void delayedActivationHandledNoPrematureRelease() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()); // 200, no events yet
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      rig.daemon.emitAfter("active", null, 120);
      rig.daemon.emitAfter("playing", playingData(URI_A), 300);

      Lease lease = rig.lease();
      CompletableFuture<Result> activation = rig.machine.activate(lease, URI_A, 0);

      // while the barrier is still open the machine must stay LEASED (never
      // released early, never quarantined by the no-URI `active` event)
      await().atMost(Duration.ofSeconds(2)).untilAsserted(
          () -> assertThat(rig.machine.phase()).isEqualTo(Phase.ACTIVATING));
      assertThat(rig.machine.state()).isEqualTo(MachineState.LEASED);

      Result result = activation.get(5, TimeUnit.SECONDS);
      assertThat(result.isOk()).isTrue();
      assertThat(rig.machine.state()).isEqualTo(MachineState.LEASED);
      assertThat(rig.machine.phase()).isEqualTo(Phase.PLAYING);
    }
  }

  @Test
  void twoHundredButNoopQuarantinesOnActivationBarrier() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()); // 200, no events ever
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(idleStatus()));

      Lease lease = rig.lease();
      Result result = activate(rig, lease, URI_A);

      assertThat(result.outcome()).isEqualTo(Outcome.QUARANTINED);
      assertThat(rig.machine.state()).isEqualTo(MachineState.QUARANTINING);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.QUARANTINING);
    }
  }

  @Test
  void wsLossMidCommandQuarantines() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok().dropWs()); // sockets die, no playing
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));

      Lease lease = rig.lease();
      CompletableFuture<Result> activation = rig.machine.activate(lease, URI_A, 0);

      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(rig.machine.state()).isEqualTo(MachineState.QUARANTINING));
      Result result = activation.get(5, TimeUnit.SECONDS);
      assertThat(result.outcome()).isEqualTo(Outcome.QUARANTINED);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.QUARANTINING);
    }
  }

  @Test
  void wsLossDuringPlayingQuarantines() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      Lease lease = rig.lease();
      assertThat(activate(rig, lease, URI_A).isOk()).isTrue();

      rig.daemon.dropWsClients();

      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(rig.machine.state()).isEqualTo(MachineState.QUARANTINING));
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.QUARANTINING);
    }
  }

  @Test
  void statusContradictsPlayingEventDegradesPermanently() throws Exception {
    try (Rig rig = newRig()) {
      // the playing event arrives, but /status insists the backend is idle
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(idleStatus()));

      Lease lease = rig.lease();
      Result result = activate(rig, lease, URI_A);

      assertThat(result.outcome()).isEqualTo(Outcome.DEGRADED);
      assertThat(rig.machine.state()).isEqualTo(MachineState.DEGRADED);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.DEGRADED);
      assertThat(rig.listener.events).contains("degraded");
    }
  }

  @Test
  void unparseableStatusRoutesToQuarantine() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.of(200, "{ this is not json"));

      Lease lease = rig.lease();
      Result result = activate(rig, lease, URI_A);

      assertThat(result.outcome()).isEqualTo(Outcome.DEGRADED);
      assertThat(rig.machine.state()).isEqualTo(MachineState.DEGRADED);
    }
  }

  @Test
  void hungPlayTransportQuarantines() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.hang()); // never responds
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));

      Lease lease = rig.lease();
      Result result = activate(rig, lease, URI_A);

      assertThat(result.outcome()).isEqualTo(Outcome.QUARANTINED);
      assertThat(rig.machine.state()).isEqualTo(MachineState.QUARANTINING);
    }
  }

  @Test
  void connectRefusedRoutesToDead() throws Exception {
    Rig rig = newRig();
    rig.withWs = false;
    rig.start();
    rigs.add(rig);
    rig.daemon.stop(); // process gone → connect refused

    Lease lease = rig.lease();
    Result result = activate(rig, lease, URI_A);

    assertThat(result.outcome()).isEqualTo(Outcome.DEAD);
    assertThat(rig.machine.state()).isEqualTo(MachineState.DEAD);
  }

  // ==================================================================
  // pause / resume / seek
  // ==================================================================

  @Test
  void pauseAndResumeCycle() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      Lease lease = rig.lease();
      assertThat(activate(rig, lease, URI_A).isOk()).isTrue();

      rig.daemon.pause(FakeLibrespotDaemon.Response.ok()
          .emit("paused", sharedData(URI_A)));
      Result paused = rig.machine.pause().get(5, TimeUnit.SECONDS);
      assertThat(paused.isOk()).isTrue();
      assertThat(rig.machine.phase()).isEqualTo(Phase.PAUSE_CONFIRMED);

      rig.daemon.resume(FakeLibrespotDaemon.Response.ok()
          .emit("playing", resumedData(URI_A)));
      Result resumed = rig.machine.resume().get(5, TimeUnit.SECONDS);
      assertThat(resumed.isOk()).isTrue();
      assertThat(rig.machine.phase()).isEqualTo(Phase.PLAYING);
    }
  }

  @Test
  void pauseAckTimeoutFallsBackToStatus() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      Lease lease = rig.lease();
      assertThat(activate(rig, lease, URI_A).isOk()).isTrue();

      rig.daemon.pause(FakeLibrespotDaemon.Response.ok()); // 200, no paused event
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(pausedStatus(URI_A)));

      Result paused = rig.machine.pause().get(5, TimeUnit.SECONDS);
      assertThat(paused.isOk()).isTrue();
      assertThat(rig.machine.phase()).isEqualTo(Phase.PAUSE_CONFIRMED);
    }
  }

  @Test
  void seekConfirmedByMatchingEvent() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      Lease lease = rig.lease();
      assertThat(activate(rig, lease, URI_A).isOk()).isTrue();

      rig.daemon.seek(FakeLibrespotDaemon.Response.ok()
          .emit("seek", seekData(URI_A, 120_000)));
      Result seeked = rig.machine.seek(120_000).get(5, TimeUnit.SECONDS);
      assertThat(seeked.isOk()).isTrue();
      assertThat(rig.machine.phase()).isEqualTo(Phase.PLAYING);
    }
  }

  @Test
  void seekAckMismatchQuarantines() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      Lease lease = rig.lease();
      assertThat(activate(rig, lease, URI_A).isOk()).isTrue();

      rig.daemon.seek(FakeLibrespotDaemon.Response.ok()
          .emit("seek", seekData(URI_A, 125_000))); // 5s ahead of the request
      Result seeked = rig.machine.seek(120_000).get(5, TimeUnit.SECONDS);
      assertThat(seeked.outcome()).isEqualTo(Outcome.QUARANTINED);
      assertThat(rig.machine.state()).isEqualTo(MachineState.QUARANTINING);
    }
  }

  @Test
  void pausedThenLateNotPlayingIsContradictionNoCrash() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      Lease lease = rig.lease();
      assertThat(activate(rig, lease, URI_A).isOk()).isTrue();

      rig.daemon.pause(FakeLibrespotDaemon.Response.ok()
          .emit("paused", sharedData(URI_A)));
      assertThat(rig.machine.pause().get(5, TimeUnit.SECONDS).isOk()).isTrue();
      assertThat(rig.machine.phase()).isEqualTo(Phase.PAUSE_CONFIRMED);

      // stop-race analog: a buffered not_playing drains after the pause ack —
      // the machine must not crash and must treat it as contradictory state
      rig.daemon.emit("not_playing", sharedData(URI_A));

      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(rig.machine.state()).isEqualTo(MachineState.DEGRADED));
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.DEGRADED);
    }
  }

  @Test
  void stopTaintForcesPermanentDegrade() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      Lease lease = rig.lease();
      assertThat(activate(rig, lease, URI_A).isOk()).isTrue();

      rig.machine.markStopTainted();
      rig.daemon.dropWsClients(); // would be transient, but stop-taint wins

      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(rig.machine.state()).isEqualTo(MachineState.DEGRADED));
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.DEGRADED);
    }
  }

  // ==================================================================
  // Quarantine re-admission
  // ==================================================================

  @Test
  void readmissionRequiresFreshWsIdleStatusAndFifoReopen() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      Lease lease = rig.lease();
      assertThat(activate(rig, lease, URI_A).isOk()).isTrue();

      // transient quarantine; the FIFO is NOT reopened yet so no re-admission
      rig.fifoReopenOk.set(false);
      rig.daemon.dropWsClients();
      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(rig.machine.state()).isEqualTo(MachineState.QUARANTINING));

      // WS reconnects; status is still playing → not idle → stays quarantined
      assertThat(rig.daemon.awaitWsClients(1, Duration.ofSeconds(5))).isTrue();
      await().atMost(Duration.ofSeconds(5)).until(rig.machine::wsFresh);
      await().atMost(Duration.ofSeconds(2)).untilAsserted(
          () -> assertThat(rig.machine.state()).isEqualTo(MachineState.QUARANTINING));

      // idle status but FIFO still closed → still quarantined
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(idleStatus()));
      assertThat(rig.machine.checkReadmission().get(5, TimeUnit.SECONDS).outcome())
          .isEqualTo(Outcome.QUARANTINED);
      assertThat(rig.machine.state()).isEqualTo(MachineState.QUARANTINING);

      // FIFO reopened + idle status + fresh WS → READY
      rig.fifoReopenOk.set(true);
      Result readmitted = rig.machine.checkReadmission().get(5, TimeUnit.SECONDS);
      assertThat(readmitted.isOk()).isTrue();
      assertThat(rig.machine.state()).isEqualTo(MachineState.READY);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY);
      assertThat(rig.listener.events).contains("readmitted");

      // and the fresh connection delivers events again (restart EOF-then-data)
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      Lease lease2 = rig.lease();
      assertThat(activate(rig, lease2, URI_A).isOk()).isTrue();
    }
  }

  // ==================================================================
  // Explicit coordinator-facing controls
  // ==================================================================

  @Test
  void retireReleasesWithoutQuarantine() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      Lease lease = rig.lease();
      assertThat(activate(rig, lease, URI_A).isOk()).isTrue();

      Result retired = rig.machine.retire().get(5, TimeUnit.SECONDS);
      assertThat(retired.isOk()).isTrue();
      assertThat(rig.machine.state()).isEqualTo(MachineState.READY);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY);
      assertThat(lease.isActive()).isFalse();
    }
  }

  @Test
  void explicitQuarantineRoutesPermanentAndTransient() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      Lease lease = rig.lease();
      assertThat(activate(rig, lease, URI_A).isOk()).isTrue();

      // transient (e.g. player-side abort)
      Result transientQ = rig.machine.quarantine("track stuck", false)
          .get(5, TimeUnit.SECONDS);
      assertThat(transientQ.outcome()).isEqualTo(Outcome.QUARANTINED);
      assertThat(rig.machine.state()).isEqualTo(MachineState.QUARANTINING);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.QUARANTINING);

      // permanent (e.g. contradictory state from the player layer)
      Result permanentQ = rig.machine.quarantine("load failure", true)
          .get(5, TimeUnit.SECONDS);
      assertThat(permanentQ.outcome()).isEqualTo(Outcome.DEGRADED);
      assertThat(rig.machine.state()).isEqualTo(MachineState.DEGRADED);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.DEGRADED);
    }
  }

  @Test
  void commandsWhileDegradedOrDeadFailTyped() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      Lease lease = rig.lease();
      assertThat(activate(rig, lease, URI_A).isOk()).isTrue();

      rig.machine.quarantine("permanent", true).get(5, TimeUnit.SECONDS);
      // the stale lease still routes through the DEGRADED guard first
      assertThat(rig.machine.activate(lease, URI_A, 0).get(5, TimeUnit.SECONDS).outcome())
          .isEqualTo(Outcome.DEGRADED);
      assertThat(rig.machine.pause().get(5, TimeUnit.SECONDS).outcome())
          .isEqualTo(Outcome.DEGRADED);
      assertThat(rig.machine.seek(120_000).get(5, TimeUnit.SECONDS).outcome())
          .isEqualTo(Outcome.DEGRADED);
    }
  }

  // ==================================================================
  // Serialized lane
  // ==================================================================

  @Test
  void serializedLaneInterleavedCommandsStayConsistent() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A, 120_000)));
      Lease lease = rig.lease();
      assertThat(activate(rig, lease, URI_A).isOk()).isTrue();

      rig.daemon.pause(FakeLibrespotDaemon.Response.ok()
          .emit("paused", sharedData(URI_A)));
      rig.daemon.resume(FakeLibrespotDaemon.Response.ok()
          .emit("playing", resumedData(URI_A)));
      rig.daemon.seek(FakeLibrespotDaemon.Response.ok()
          .emit("seek", seekData(URI_A, 120_000)));

      int threads = 2;
      int perThread = 25;
      AtomicInteger counter = new AtomicInteger();
      List<Result> results = Collections.synchronizedList(new ArrayList<>());
      CountDownLatch startGate = new CountDownLatch(1);
      ExecutorService workers = Executors.newFixedThreadPool(threads);

      for (int t = 0; t < threads; t++) {
        workers.submit(() -> {
          try {
            startGate.await();
            for (int i = 0; i < perThread; i++) {
              int n = counter.getAndIncrement();
              CompletableFuture<Result> future = switch (n % 3) {
                case 0 -> rig.machine.pause();
                case 1 -> rig.machine.resume();
                default -> rig.machine.seek(120_000);
              };
              results.add(future.get(5, TimeUnit.SECONDS));
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
      }

      startGate.countDown();
      workers.shutdown();
      assertThat(workers.awaitTermination(20, TimeUnit.SECONDS)).isTrue();

      // the lane never let two commands interleave: no contradiction /
      // transport outcome ever escaped, every future resolved normally
      assertThat(results).hasSize(threads * perThread);
      for (Result result : results) {
        assertThat(result.outcome())
            .as("serialized lane outcome")
            .isIn(Outcome.OK, Outcome.FAILED);
      }
      assertThat(rig.machine.state()).isEqualTo(MachineState.LEASED);
      assertThat(rig.machine.phase()).isIn(Phase.PLAYING, Phase.PAUSE_CONFIRMED);
    }
  }

  // ==================================================================
  // helpers
  // ==================================================================

  private static String playingData(String uri) {
    return FakeLibrespotDaemon.playingData("", uri, false, "go-librespot");
  }

  private static String resumedData(String uri) {
    return FakeLibrespotDaemon.playingData("", uri, true, "go-librespot");
  }

  private static String sharedData(String uri) {
    return FakeLibrespotDaemon.sharedTrackData("", uri, "go-librespot");
  }

  private static String seekData(String uri, long position) {
    return FakeLibrespotDaemon.seekData("", uri, position, 240_000, "go-librespot");
  }

  private static String playingStatus(String uri) {
    return playingStatus(uri, 0);
  }

  private static String playingStatus(String uri, long position) {
    return FakeLibrespotDaemon.statusJson(false, false,
        FakeLibrespotDaemon.trackJson(uri, "Track", List.of("Artist"), "Album", "",
            position, 240_000, "", 1, 1, "OGG_VORBIS_160", "vorbis", 160, 44100, null));
  }

  private static String idleStatus() {
    return FakeLibrespotDaemon.statusJson(true, false, null);
  }

  private static String pausedStatus(String uri) {
    return FakeLibrespotDaemon.statusJson(true, true,
        FakeLibrespotDaemon.trackJson(uri, "Track", List.of("Artist"), "Album", "",
            0, 240_000, "", 1, 1, "OGG_VORBIS_160", "vorbis", 160, 44100, null));
  }

  private static Result activate(Rig rig, Lease lease, String uri) throws Exception {
    return rig.machine.activate(lease, uri, 0).get(8, TimeUnit.SECONDS);
  }

  private Rig newRig() {
    Rig rig = new Rig();
    rig.start();
    rigs.add(rig);
    return rig;
  }

  /** The full stack under test: fixture daemon + real REST + real WS + machine. */
  private static final class Rig implements AutoCloseable {

    final FakeLibrespotDaemon daemon = new FakeLibrespotDaemon();
    final AtomicBoolean fifoReopenOk = new AtomicBoolean(true);
    final RecordingListener listener = new RecordingListener();
    final List<String> logLines = Collections.synchronizedList(new ArrayList<>());

    boolean withWs = true;

    GoLibrespotConfig config;
    BackendConfig backend;
    GoLibrespotRestClient rest;
    ExclusivePool pool;
    EventsWebSocketClient ws;
    BackendStateMachine machine;

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
        backend = config.getBackends().get(0);
        pool = new ExclusivePool(config.getBackends());
        rest = new GoLibrespotRestClient(backend.getRestBaseUrl(), 1500);
        machine = new BackendStateMachine(
            BackendHandle.of(backend), rest, pool, FAST, fifoReopenOk::get, listener, logLines::add);
        if (withWs) {
          ws = new EventsWebSocketClient(
              backend.getWsUrl(), machine.eventsListener(), 40, 200, 5);
          machine.attachWebSocket(ws);
          ws.start();
          assertThat(daemon.awaitWsClients(1, Duration.ofSeconds(5))).isTrue();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    Lease lease() throws Exception {
      return pool.acquire(Duration.ofSeconds(5)).orElseThrow();
    }

    @Override
    public void close() {
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
      daemon.stop();
    }
  }

  /** Records lifecycle callbacks for assertions. */
  static final class RecordingListener implements BackendStateMachine.LifecycleListener {
    final List<String> events = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void onNaturalCompletion() {
      events.add("completed");
    }

    @Override
    public void onReAdmitted() {
      events.add("readmitted");
    }

    @Override
    public void onQuarantined(boolean permanent) {
      events.add(permanent ? "degraded" : "quarantined");
    }

    @Override
    public void onDead() {
      events.add("dead");
    }
  }
}
