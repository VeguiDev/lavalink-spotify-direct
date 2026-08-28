package dev.lavalinkplugins.golibrespot.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.lavalinkplugins.golibrespot.backend.rest.GoLibrespotRestClient;
import dev.lavalinkplugins.golibrespot.backend.ws.EventsWebSocketClient;
import dev.lavalinkplugins.golibrespot.config.BackendConfig;
import dev.lavalinkplugins.golibrespot.config.GoLibrespotConfig;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * P3 (F2-F2b): the machine's READY-branch lease adoption must require the
 * passed lease to be the pool's CURRENT active lease.
 *
 * <p>A replace-vs-natural-completion interleaving can deliver a stale
 * {@code activate(lease)} whose lease was already released and re-granted by
 * the pool to ANOTHER session (or merely released into READY). The buggy
 * adoption — after only a backend-id check — puts two sessions on one daemon.
 * The fixed branch rejects such a lease with a typed FAILED result and never
 * touches the backend.</p>
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class LeaseAdoptionRaceTest {

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
   * The lease was released by natural completion and the pool re-granted the
   * backend to ANOTHER session: the stale activate(lease) must be rejected —
   * no adoption, no play, the other session keeps its lease.
   */
  @Test
  void readReadyActivationRejectsAlreadyReleasedReGrantedLease() throws Exception {
    try (Rig rig = newRig()) {
      // session 1: play A fully
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      Lease lease1 = rig.lease();
      assertThat(rig.machine.activate(lease1, URI_A, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();

      // natural completion: the machine releases lease1; the pool goes READY
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(idleStatus()));
      rig.daemon.emit("not_playing", sharedData(URI_A));
      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(rig.machine.state()).isEqualTo(MachineState.READY));
      assertThat(lease1.isActive()).isFalse();

      // another session acquires the same backend
      Lease lease2 = rig.lease();
      assertThat(lease2.isActive()).isTrue();
      assertThat(lease2.generation()).isGreaterThan(lease1.generation());

      // the stale activate(lease1) reaches the machine's READY branch — the pool
      // already re-granted this backend to lease2, so it must be REJECTED
      Result result = rig.machine.activate(lease1, URI_A, 0).get(5, TimeUnit.SECONDS);
      assertThat(result.outcome()).as("stale lease adoption must be rejected: " + result)
          .isEqualTo(Outcome.FAILED);
      assertThat(rig.machine.state()).isEqualTo(MachineState.READY);
      assertThat(rig.machine.phase()).isEqualTo(Phase.IDLE);

      // lease2 still owns the backend: no second session, no play for the stale lease
      assertThat(lease2.isActive()).isTrue();
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.LEASED);
      assertThat(rig.daemon.getReceivedCommands())
          .filteredOn(c -> c.path().equals("/player/play")).hasSize(1);
      assertThat(rig.daemon.getReceivedCommands())
          .filteredOn(c -> c.path().equals("/player/stop")).isEmpty();
    }
  }

  /**
   * The lease was released by natural completion and nobody re-granted it (pool
   * READY): a stale activate(lease) must STILL be rejected — the machine must
   * never adopt a lease the pool no longer considers LEASED.
   */
  @Test
  void readReadyActivationRejectsAlreadyReleasedLease() throws Exception {
    try (Rig rig = newRig()) {
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      Lease lease = rig.lease();
      assertThat(rig.machine.activate(lease, URI_A, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();

      rig.daemon.status(FakeLibrespotDaemon.Response.ok(idleStatus()));
      rig.daemon.emit("not_playing", sharedData(URI_A));
      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(rig.machine.state()).isEqualTo(MachineState.READY));
      assertThat(lease.isActive()).isFalse();

      // no other session: the pool is READY. The stale lease must still be rejected.
      Result result = rig.machine.activate(lease, URI_A, 0).get(5, TimeUnit.SECONDS);
      assertThat(result.outcome()).as("released lease adoption must be rejected: " + result)
          .isEqualTo(Outcome.FAILED);
      assertThat(rig.machine.state()).isEqualTo(MachineState.READY);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.READY);
      assertThat(rig.daemon.getReceivedCommands())
          .filteredOn(c -> c.path().equals("/player/play")).hasSize(1);
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
