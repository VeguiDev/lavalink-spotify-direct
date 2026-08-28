package dev.lavalinkplugins.golibrespot.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.lavalinkplugins.golibrespot.backend.rest.GoLibrespotRestClient;
import dev.lavalinkplugins.golibrespot.backend.ws.EventsWebSocketClient;
import dev.lavalinkplugins.golibrespot.config.BackendConfig;
import dev.lavalinkplugins.golibrespot.config.GoLibrespotConfig;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.MachineState;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Phase;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Result;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Timing;
import dev.lavalinkplugins.golibrespot.pool.BackendHandle;
import dev.lavalinkplugins.golibrespot.pool.ExclusivePool;
import dev.lavalinkplugins.golibrespot.pool.Lease;
import dev.lavalinkplugins.golibrespot.testfixtures.FakeLibrespotDaemon;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * F4: pause requested while the machine is ACTIVATING must not be silently
 * rejected — the daemon would keep playing while the player is paused. It is
 * queued and applied the moment activation confirms (PLAYING), and resolves
 * (failed, never dangling) when activation itself fails.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class PauseDuringActivationTest {

  private static final String URI_A = "spotify:track:aaaaaaaaaaaaaaaaaaaaaa";

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

  /**
   * Pause during ACTIVATING is queued (never FAILED), then applied once the
   * activation confirms: the daemon receives /player/pause and ends paused.
   */
  @Test
  void pauseDuringActivationIsQueuedAndAppliedAfterActivation() throws Exception {
    try (Rig rig = newRig()) {
      // play answers 200 with no events yet — activation stays in flight
      rig.daemon.play(FakeLibrespotDaemon.Response.ok());
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      // script the queued pause's answer BEFORE the pause is issued
      rig.daemon.pause(FakeLibrespotDaemon.Response.ok()
          .emit("paused", sharedData(URI_A)));

      Lease lease = rig.lease();
      CompletableFuture<Result> activation = rig.machine.activate(lease, URI_A, 0);
      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(rig.machine.phase()).isEqualTo(Phase.ACTIVATING));

      CompletableFuture<Result> pauseFuture = rig.machine.pause();
      // give the lane a moment to process the pause while STILL ACTIVATING: the
      // buggy code resolves it FAILED here; the fixed code keeps it queued
      await().pollDelay(Duration.ofMillis(50)).atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(rig.machine.phase()).isEqualTo(Phase.ACTIVATING));
      assertThat(pauseFuture).as("pause must be queued, not failed, during activation")
          .isNotCompleted();

      // activation confirms → the queued pause fires immediately
      rig.daemon.emit("playing", playingData(URI_A));
      assertThat(activation.get(5, TimeUnit.SECONDS).isOk()).isTrue();
      assertThat(pauseFuture.get(5, TimeUnit.SECONDS).isOk())
          .as("queued pause must apply after activation").isTrue();

      assertThat(rig.machine.phase()).isEqualTo(Phase.PAUSE_CONFIRMED);
      assertThat(rig.daemon.getReceivedCommands())
          .filteredOn(c -> c.path().equals("/player/play")).hasSize(1);
      assertThat(rig.daemon.getReceivedCommands())
          .filteredOn(c -> c.path().equals("/player/pause")).hasSize(1);
      assertThat(rig.daemon.getReceivedCommands())
          .filteredOn(c -> c.path().equals("/player/stop")).isEmpty();
    }
  }

  /**
   * Pause queued during activation resolves (failed — never a dangling future)
   * when the activation itself fails and quarantines.
   */
  @Test
  void queuedPauseResolvesFailedWhenActivationFails() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()); // no events ever → timeout
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(idleStatus()));

      Lease lease = rig.lease();
      CompletableFuture<Result> activation = rig.machine.activate(lease, URI_A, 0);
      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(rig.machine.phase()).isEqualTo(Phase.ACTIVATING));

      CompletableFuture<Result> pauseFuture = rig.machine.pause();
      await().pollDelay(Duration.ofMillis(50)).atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(rig.machine.phase()).isEqualTo(Phase.ACTIVATING));
      assertThat(pauseFuture).isNotCompleted();

      Result act = activation.get(8, TimeUnit.SECONDS);
      assertThat(act.isOk()).as("activation must fail (barrier timeout): " + act).isFalse();
      // the queued pause must resolve with the failure — never dangle
      Result paused = pauseFuture.get(5, TimeUnit.SECONDS);
      assertThat(paused.isOk()).isFalse();
      assertThat(rig.machine.state()).isEqualTo(MachineState.QUARANTINING);
    }
  }

  // ---------------------------------------------------------------- helpers

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

  private Rig newRig() {
    Rig rig = new Rig();
    rig.start();
    rigs.add(rig);
    return rig;
  }

  /** The machine-only stack: fixture daemon + real REST + real WS + pool + machine. */
  private static final class Rig implements AutoCloseable {

    final FakeLibrespotDaemon daemon = new FakeLibrespotDaemon();
    final AtomicBoolean fifoReopenOk = new AtomicBoolean(true);
    final List<String> logLines = Collections.synchronizedList(new ArrayList<>());

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
            BackendHandle.of(backend), rest, pool, FAST, fifoReopenOk::get,
            new BackendStateMachine.LifecycleListener() {}, logLines::add);
        ws = new EventsWebSocketClient(
            backend.getWsUrl(), machine.eventsListener(), 40, 200, 5);
        machine.attachWebSocket(ws);
        ws.start();
        assertThat(daemon.awaitWsClients(1, Duration.ofSeconds(5))).isTrue();
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
}
