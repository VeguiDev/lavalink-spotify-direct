package dev.lavalinkplugins.golibrespot.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.lavalinkplugins.golibrespot.backend.rest.GoLibrespotRestClient;
import dev.lavalinkplugins.golibrespot.backend.ws.EventsWebSocketClient;
import dev.lavalinkplugins.golibrespot.config.BackendConfig;
import dev.lavalinkplugins.golibrespot.config.GoLibrespotConfig;
import dev.lavalinkplugins.golibrespot.fifo.FifoReader;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.MachineState;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Result;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Timing;
import dev.lavalinkplugins.golibrespot.pool.BackendHandle;
import dev.lavalinkplugins.golibrespot.pool.BackendState;
import dev.lavalinkplugins.golibrespot.pool.ExclusivePool;
import dev.lavalinkplugins.golibrespot.testfixtures.FakeLibrespotDaemon;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * P1 (F2-F1): the stale-advance reload guard must key on the MACHINE's confirmed
 * playing state, not on {@code barrier.isSatisfied()}.
 *
 * <p>The barrier is satisfied only after the machine's /status reconcile of the
 * {@code playing} event completes (up to {@code reconcileTimeoutMs} — 2s
 * default, 800ms with {@link #FAST} timings), but the machine commits to
 * {@link BackendStateMachine.Phase#PLAYING} the moment the {@code playing}
 * event arrives. A reload that fires while the machine is already PLAYING is
 * the healthy-path defect: the reload re-issues the same play and its re-emitted
 * {@code not_playing} lands in PLAYING → COMPLETING → /status mismatch →
 * process-permanent DEGRADE of a healthy backend.</p>
 *
 * <p>The two regression scenarios:</p>
 * <ul>
 *   <li><b>Healthy same-URI replacement + slow status probe</b> — the reload
 *       must NOT fire (exactly 2 plays total, backend stays LEASED/PLAYING).</li>
 *   <li><b>Genuine stale advance</b> — the reload DOES fire (3 plays) and the
 *       re-emitted {@code not_playing} that lands AFTER {@code playing} confirmed
 *       must be tolerated by the machine (no DEGRADE).</li>
 * </ul>
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class StaleAdvanceReloadTest {

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

  /**
   * Healthy same-URI replacement with a SLOW status probe must not fire the
   * reload. The gen-2 play re-emits the stale {@code not_playing} while the
   * machine is ACTIVATING; the {@code playing} event confirms quickly (machine
   * phase → PLAYING) but /status stays "idle" for 600ms, so the barrier's
   * reconcile lags past the 150ms reload grace. The buggy guard (keyed on
   * barrier.isSatisfied()) fires the reload; the fixed guard (keyed on the
   * machine's committed PLAYING phase) skips it.
   */
  @Test
  void healthySameUriReplacementWithSlowStatusProbeDoesNotFireReload() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      // gen 1: play A fully
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      assertThat(rig.coordinator.start(URI_A, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();

      // gen 2: SAME URI. The play response re-emits the stale not_playing(A)
      // (stale advance); the playing event confirms within ~30ms; but /status
      // stays idle past the reload grace so the barrier's reconcile lags — the
      // exact healthy path the buggy reload guard degrades. The probe is flipped
      // to playing on a background thread (the main thread blocks on the
      // replacement future) at t≈250ms: after the 150ms reload window, still
      // within the machine's 800ms reconcile deadline.
      rig.daemon.pause(FakeLibrespotDaemon.Response.ok()
          .emit("paused", sharedData(URI_A)));
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("not_playing", sharedData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(idleStatus()));
      rig.daemon.emitAfter("playing", playingData(URI_A), 100);
      Thread flipper = new Thread(() -> {
        try {
          Thread.sleep(500);
        } catch (InterruptedException ignored) {
          Thread.currentThread().interrupt();
        }
        rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      });
      flipper.start();

      Result replaced = rig.coordinator.replace(URI_A, 0, false).get(8, TimeUnit.SECONDS);
      flipper.join(3_000);

      assertThat(replaced.isOk()).as("healthy same-URI replacement: " + replaced).isTrue();
      assertThat(rig.machine.state()).isEqualTo(MachineState.LEASED);
      assertThat(rig.machine.phase()).isEqualTo(BackendStateMachine.Phase.PLAYING);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.LEASED);

      // the idempotent reload must NOT have fired in the healthy path: exactly
      // one play per generation, nothing more even after the grace window passes
      await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
          assertThat(playCount(rig)).isEqualTo(2));
      Thread.sleep(300); // the reload window has long passed — no late reload
      assertThat(playCount(rig)).as("no idempotent reload in the healthy path").isEqualTo(2);
      assertThat(rig.coordinator.currentLease()).isNotNull();
      assertThat(rig.coordinator.currentLease().isActive()).isTrue();
    }
  }

  /**
   * Genuine stale advance: the reload fires and its re-emitted
   * {@code not_playing} arrives AFTER {@code playing} confirmed — the machine
   * must tolerate it (the daemon is actually still playing), never DEGRADE a
   * healthy backend.
   */
  @Test
  void genuineStaleAdvanceReloadEchoAfterPlayingConfirmIsTolerated() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      // gen 1: play A fully
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      assertThat(rig.coordinator.start(URI_A, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();

      // gen 2 → B: the play response emits ONLY the stale not_playing(B); the
      // real playing(B) is delayed past the 150ms reload grace so the reload
      // fires; the reload's not_playing echo then drains AFTER playing confirmed.
      rig.daemon.pause(FakeLibrespotDaemon.Response.ok()
          .emit("paused", sharedData(URI_A)));
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("not_playing", sharedData(URI_B)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_B)));
      rig.daemon.emitAfter("playing", playingData(URI_B), 250);   // after the reload fires
      rig.daemon.emitAfter("not_playing", sharedData(URI_B), 340); // the reload echo

      Result replaced = rig.coordinator.replace(URI_B, 0, false).get(8, TimeUnit.SECONDS);

      assertThat(replaced.isOk()).as("stale-advance replacement: " + replaced).isTrue();
      // give the echo time to drain through the machine's reconcile before asserting
      Thread.sleep(500);
      assertThat(rig.machine.state()).as("machine state after reload echo")
          .isEqualTo(MachineState.LEASED);
      assertThat(rig.machine.phase()).as("machine phase after reload echo")
          .isEqualTo(BackendStateMachine.Phase.PLAYING);
      assertThat(rig.pool.stateOf("alpha")).as("no DEGRADE from the reload echo")
          .isEqualTo(BackendState.LEASED);

      // A + B + the reload's B = exactly 3 plays; the reload DID fire (genuine nudge)
      assertThat(playCount(rig)).isEqualTo(3);
      assertThat(rig.daemon.getReceivedCommands())
          .filteredOn(c -> c.path().equals("/player/pause")).hasSize(1);
      assertThat(rig.daemon.getReceivedCommands())
          .filteredOn(c -> c.path().equals("/player/stop")).isEmpty();
      assertThat(rig.coordinator.currentLease().isActive()).isTrue();
    }
  }

  // ---------------------------------------------------------------- helpers

  private static int playCount(Rig rig) {
    return (int) rig.daemon.getReceivedCommands().stream()
        .filter(c -> c.path().equals("/player/play")).count();
  }

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

  /** The full stack under test: fixture daemon + real REST + real WS + pool + machine + coordinator. */
  private static final class Rig implements AutoCloseable {

    final FakeLibrespotDaemon daemon = new FakeLibrespotDaemon();
    final AtomicBoolean fifoReopenOk = new AtomicBoolean(true);
    final List<String> logLines = Collections.synchronizedList(new ArrayList<>());
    final LifecycleCoordinator.ListenerBridge bridge = new LifecycleCoordinator.ListenerBridge();
    final ScriptedSeam seam = new ScriptedSeam();

    GoLibrespotConfig config;
    BackendConfig backend;
    GoLibrespotRestClient rest;
    ExclusivePool pool;
    EventsWebSocketClient ws;
    BackendStateMachine machine;
    LifecycleCoordinator coordinator;
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
                "fifoPath", "C:/tmp/alpha.fifo"))));
        backend = config.getBackends().get(0);
        pool = new ExclusivePool(config.getBackends());
        rest = new GoLibrespotRestClient(backend.getRestBaseUrl(), 1500);
        machine = new BackendStateMachine(
            BackendHandle.of(backend), rest, pool, FAST, fifoReopenOk::get, bridge, logLines::add);
        coordinator = new LifecycleCoordinator(
            BackendHandle.of(backend), machine, rest, pool, seam, FifoReader::new,
            FAST, new LifecycleCoordinator.Tuning(Duration.ofSeconds(5), Duration.ofSeconds(5)),
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
      seam.stream = in;
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
      daemon.stop();
    }
  }

  /** Minimal {@link FifoOpenerSeam}: returns the current piped stream immediately. */
  private static final class ScriptedSeam implements FifoOpenerSeam {
    volatile InputStream stream;

    @Override
    public OpenHandleLike open(Path path, Duration timeout) {
      InputStream in = stream;
      return new OpenHandleLike() {
        @Override
        public InputStream await() {
          if (in == null) {
            throw new CancellationException("cancelled");
          }
          return in;
        }

        @Override
        public boolean cancel() {
          return true;
        }

        @Override
        public boolean isDone() {
          return true;
        }

        @Override
        public boolean isCancelled() {
          return false;
        }
      };
    }
  }
}
