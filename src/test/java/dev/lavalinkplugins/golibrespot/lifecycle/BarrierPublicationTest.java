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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * P2 (F2-F2a): the new session's activation barrier must be published at SESSION
 * CREATION (synchronously on the lane, before the blocking lease acquire), so a
 * concurrently-starting track's {@code awaitActivated()} can never observe the
 * PREVIOUS session's retained barrier:
 *
 * <ul>
 *   <li>a retained SATISFIED barrier returns immediately and feeds stale
 *       pre-switch PCM (or end-of-stream from the closed reader);</li>
 *   <li>a retained FAILED barrier fails the new track instantly.</li>
 * </ul>
 *
 * <p>Both tests block the pool acquire (backend quarantined) after the previous
 * session ended, then assert the coordinator's {@code currentBarrier()} already
 * points at the NEW session's PENDING barrier while the acquire is still
 * blocked — impossible with the buggy "publish only inside the async lane task
 * after the acquire" ordering.</p>
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class BarrierPublicationTest {

  private static final String URI_A = "spotify:track:aaaaaaaaaaaaaaaaaaaaaa";
  private static final String URI_B = "spotify:track:bbbbbbbbbbbbbbbbbbbbbb";

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
   * After a natural completion (retained SATISFIED barrier), a new session whose
   * lease acquire is blocked must already expose its fresh PENDING barrier —
   * and a process()-style {@code awaitActivated()} caller must BLOCK on it
   * rather than sail through the old satisfied barrier.
   */
  @Test
  void newSessionBarrierIsPublishedBeforeBlockingAcquireAfterCompletion() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      // session A: fully activate, then complete naturally (session ends)
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_A)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_A)));
      assertThat(rig.coordinator.start(URI_A, 0).get(5, TimeUnit.SECONDS).isOk()).isTrue();
      ActivationBarrier firstBarrier = rig.coordinator.currentBarrier();
      assertThat(firstBarrier.isSatisfied()).isTrue();

      rig.daemon.status(FakeLibrespotDaemon.Response.ok(idleStatus()));
      rig.daemon.emit("not_playing", sharedData(URI_A));
      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(rig.machine.state()).isEqualTo(MachineState.READY));
      assertThat(rig.coordinator.currentLease()).isNull();

      // block the pool: quarantine the backend so the next acquire joins the
      // FIFO waiter queue (the coordinator's lane blocks INSIDE the acquire)
      rig.pool.markQuarantined("alpha", false);

      // script session B's success path
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_B)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_B)));

      CompletableFuture<Result> startB = rig.coordinator.start(URI_B, 0);

      // P2: the new session's barrier must ALREADY be published while the
      // acquire is still blocked (the buggy ordering leaves the previous
      // session's retained barrier in place and this await times out)
      await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
          assertThat(rig.coordinator.currentBarrier()).isNotSameAs(firstBarrier));
      ActivationBarrier current = rig.coordinator.currentBarrier();
      assertThat(current.expectedUri()).isEqualTo(URI_B);
      assertThat(current.state()).isEqualTo(ActivationBarrier.State.PENDING);

      // a process()-style caller must BLOCK on the fresh pending barrier —
      // never return from the previous session's satisfied barrier
      AtomicReference<Boolean> released = new AtomicReference<>(false);
      Thread waiter = new Thread(() -> {
        try {
          rig.coordinator.awaitActivated(Duration.ofSeconds(5));
          released.set(true);
        } catch (Exception e) {
          released.set(false);
        }
      });
      waiter.start();
      await().atMost(Duration.ofSeconds(1)).until(
          () -> waiter.getState() == Thread.State.WAITING
              || waiter.getState() == Thread.State.TIMED_WAITING);
      assertThat(released.get()).as("waiter must stay blocked on the fresh barrier").isFalse();

      // unblock: re-admit alpha — the grant delivers B's lease and activation proceeds
      rig.pool.markReady("alpha");
      Result result = startB.get(8, TimeUnit.SECONDS);
      assertThat(result.isOk()).as("activation after re-admission: " + result).isTrue();
      assertThat(rig.coordinator.currentBarrier().isSatisfied()).isTrue();
      assertThat(rig.coordinator.currentLease()).isNotNull();
      assertThat(rig.coordinator.currentLease().isActive()).isTrue();
      waiter.join(3_000);
      assertThat(released.get()).isTrue();
    }
  }

  /**
   * After an ABORTED activation (retained FAILED barrier), a new session must
   * publish its fresh PENDING barrier at session creation — the buggy ordering
   * lets the new track's {@code awaitActivated()} re-throw the previous
   * session's failure instantly.
   */
  @Test
  void newSessionBarrierIsPublishedFreshAfterFailedActivation() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      // session A aborts locally (FIFO open fails) → FAILED barrier retained, machine READY
      rig.seam.failOpen = true;
      Result failed = rig.coordinator.start(URI_A, 0).get(5, TimeUnit.SECONDS);
      assertThat(failed.isOk()).isFalse();
      ActivationBarrier failedBarrier = rig.coordinator.currentBarrier();
      assertThat(failedBarrier.isFailed()).isTrue();
      assertThat(rig.machine.state()).isEqualTo(MachineState.READY);
      assertThat(rig.coordinator.currentLease()).isNull();
      rig.seam.failOpen = false; // session B's FIFO open succeeds again

      // block the pool so the next session's lane is observed mid-acquire
      rig.pool.markQuarantined("alpha", false);
      rig.daemon.play(FakeLibrespotDaemon.Response.ok()
          .emit("playing", playingData(URI_B)));
      rig.daemon.status(FakeLibrespotDaemon.Response.ok(playingStatus(URI_B)));

      CompletableFuture<Result> startB = rig.coordinator.start(URI_B, 0);

      await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
          assertThat(rig.coordinator.currentBarrier()).isNotSameAs(failedBarrier));
      ActivationBarrier current = rig.coordinator.currentBarrier();
      assertThat(current.expectedUri()).isEqualTo(URI_B);
      assertThat(current.state()).isEqualTo(ActivationBarrier.State.PENDING);

      rig.pool.markReady("alpha");
      Result resultB = startB.get(8, TimeUnit.SECONDS);
      assertThat(resultB.isOk()).as("activation B: " + resultB).isTrue();
      assertThat(rig.coordinator.currentBarrier().isSatisfied()).isTrue();
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
    volatile boolean failOpen;

    @Override
    public OpenHandleLike open(Path path, Duration timeout) {
      // immediate synchronous failure: the machine never takes ownership and
      // the session's barrier is failed CANCELED with the machine left READY
      if (failOpen) {
        throw new IllegalStateException("no such fifo");
      }
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
