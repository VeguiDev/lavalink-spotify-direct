package dev.lavalinkplugins.golibrespot.config;

import dev.lavalinkplugins.golibrespot.backend.rest.GoLibrespotRestClient;
import dev.lavalinkplugins.golibrespot.backend.ws.EventsWebSocketClient;
import dev.lavalinkplugins.golibrespot.fifo.FifoOpener;
import dev.lavalinkplugins.golibrespot.fifo.FifoReader;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine;
import dev.lavalinkplugins.golibrespot.lifecycle.FifoOpenerSeam;
import dev.lavalinkplugins.golibrespot.lifecycle.FifoReaderFactory;
import dev.lavalinkplugins.golibrespot.lifecycle.LifecycleCoordinator;
import dev.lavalinkplugins.golibrespot.lifecycle.StopSequence;
import dev.lavalinkplugins.golibrespot.metadata.MetadataResolver;
import dev.lavalinkplugins.golibrespot.pool.BackendHandle;
import dev.lavalinkplugins.golibrespot.pool.BackendState;
import dev.lavalinkplugins.golibrespot.pool.ExclusivePool;
import dev.lavalinkplugins.golibrespot.source.CoordinatorBackedPlayback;
import dev.lavalinkplugins.golibrespot.source.CoordinatorFactory;
import dev.lavalinkplugins.golibrespot.source.PlaybackCoordinator;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Production wiring factory (T19): builds the whole go-librespot backend chain
 * from a validated {@link GoLibrespotConfig}.
 *
 * <p>One {@link ExclusivePool} (lease-free backend selection) is shared by all
 * backends. Per backend an eager chain is built: the T8 REST client, the T14
 * state machine (late-bound {@link LifecycleCoordinator.ListenerBridge}), the
 * T11 {@link FifoOpener} behind the T15 {@link FifoOpenerSeam}, the T9 events
 * websocket (wired through the coordinator's stale-advance listener and armed
 * before {@code start()}), the T15 {@link LifecycleCoordinator}, the T17
 * {@link StopSequence} and the T18 {@link CoordinatorBackedPlayback}. The
 * coordinator's {@link FifoReaderFactory} is routed through the playback's
 * {@link CoordinatorBackedPlayback#wrapReaderFactory wrapReaderFactory} so the
 * seek path drains the CURRENT session's reader; the cycle (the playback is
 * constructed over the coordinator) is broken with an {@link AtomicReference}
 * that the deferred factory resolves on the first reader creation.</p>
 *
 * <p>The resulting {@link CoordinatorFactory} returns the prebuilt
 * {@link PlaybackCoordinator} of a backend and tears every chain down
 * idempotently on {@link CoordinatorFactory#close()} (each chain's
 * {@code StopSequence.shutdown()} — the DECISIONS.md order: coordinator/reader,
 * websocket, machine, opener — plus the shared pool and each REST client).</p>
 *
 * <p>The metadata resolver gets a finite, pool-backed snapshot of READY
 * backends for every resolve. The snapshot uses a rotating start and never
 * contains the same backend twice.</p>
 */
public final class ConfigFactory {

  private static final Logger log = LoggerFactory.getLogger(ConfigFactory.class);

  /**
   * Slack beyond the machine's activation budget bound for the FIFO open
   * (ms). The read-end open self-cancels at this deadline; with
   * {@code wait_for_reader:true} the daemon only opens its write end as a
   * consequence of the play command, so the bound must comfortably exceed the
   * activation barrier (which quarantines first anyway).
   */
  private static final long FIFO_OPEN_SLACK_MS = 2_000L;

  private ConfigFactory() {
  }

  /**
   * Builds the complete runtime graph from a validated config.
   *
   * @param config  validated {@link GoLibrespotConfig} (structural errors are
   *                the caller's job to reject before this)
   * @param logSink receives sanitized diagnostic lines from every chain
   *                component (default no-op)
   */
  public static Runtime build(GoLibrespotConfig config, Consumer<String> logSink) {
    Consumer<String> sink = logSink == null ? s -> {} : logSink;
    ExclusivePool pool = new ExclusivePool(config.getBackends(), config.getPoolAcquireTimeoutMs());

    Map<String, BackendChain> chains = new ConcurrentHashMap<>();
    for (BackendConfig backend : config.getBackends()) {
      chains.put(backend.getName(), buildChain(config, backend, pool, sink));
    }

    MetadataResolver metadataResolver =
        new MetadataResolver(poolReadyBackendSelector(pool), config.getMetadataTimeoutMs(),
            config.getSpotifyClientId(), config.getSpotifyClientSecret(), config.getSpotifyMarket());

    CoordinatorFactory coordinatorFactory = new CoordinatorFactory() {
      private final AtomicBoolean closed = new AtomicBoolean();

      @Override
      public PlaybackCoordinator create(BackendHandle handle, BackendConfig backendConfig) {
        if (closed.get()) {
          throw new IllegalStateException("go-librespot plugin is shut down");
        }
        BackendChain chain = chains.get(handle.getBackendId());
        if (chain == null) {
          throw new IllegalStateException(
              "no go-librespot backend chain for '" + handle.getBackendId() + "'");
        }
        return chain.playback();
      }

      @Override
      public void close() {
        if (closed.compareAndSet(false, true)) {
          chains.values().forEach(BackendChain::close);
          pool.shutdown();
        }
      }
    };

    return new Runtime(pool, metadataResolver, coordinatorFactory);
  }

  // ------------------------------------------------------------ chain building

  private static BackendChain buildChain(
      GoLibrespotConfig config, BackendConfig backend, ExclusivePool pool, Consumer<String> logSink) {
    BackendHandle handle = BackendHandle.of(backend);
    GoLibrespotRestClient rest = GoLibrespotRestClient.fromConfig(config, backend);
    BackendStateMachine.Timing timing = timingFor(config, backend);
    LifecycleCoordinator.Tuning tuning = tuningFor(config, backend);

    LifecycleCoordinator.ListenerBridge bridge = new LifecycleCoordinator.ListenerBridge();
    BackendStateMachine machine =
        new BackendStateMachine(handle, rest, pool, timing, fifoReopenOk(backend), bridge, logSink);

    FifoOpener opener = FifoOpener.create();
    FifoOpenerSeam seam = FifoOpenerSeam.of(opener);

    // Deferred reader capture: the coordinator's reader factory routes through
    // the playback's wrapReaderFactory once the playback exists (the playback
    // is constructed over the coordinator, so the cycle is broken with a
    // reference resolved at the first reader creation — at activation time the
    // playback is long since wired).
    AtomicReference<CoordinatorBackedPlayback> playbackRef = new AtomicReference<>();
    FifoReaderFactory readerFactory = stream -> {
      CoordinatorBackedPlayback playback = playbackRef.get();
      if (playback == null) {
        throw new IllegalStateException("go-librespot playback wiring is not complete");
      }
      return playback.wrapReaderFactory(FifoReader::new).create(stream);
    };

    LifecycleCoordinator coordinator = new LifecycleCoordinator(
        handle, machine, rest, pool, seam, readerFactory, timing, tuning, logSink);
    bridge.setTarget(coordinator);

    EventsWebSocketClient ws = new EventsWebSocketClient(
        backend.getWsUrl(),
        coordinator.listener(),
        config.effectiveWsReconnectInitialMs(backend),
        config.effectiveWsReconnectMaxMs(backend),
        config.effectiveWsFailuresBeforeQuarantine(backend));
    machine.attachWebSocket(ws);
    ws.start();

    StopSequence stopSequence =
        new StopSequence(machine, coordinator, ws, opener, pool, timing, logSink);
    CoordinatorBackedPlayback playback =
        new CoordinatorBackedPlayback(coordinator, stopSequence, timing, logSink);
    playbackRef.set(playback);

    return new BackendChain(rest, stopSequence, playback);
  }

  /** Bounded machine budgets: config-driven values, DECISIONS.md defaults elsewhere. */
  private static BackendStateMachine.Timing timingFor(GoLibrespotConfig config, BackendConfig backend) {
    BackendStateMachine.Timing defaults = BackendStateMachine.Timing.defaults();
    return new BackendStateMachine.Timing(
        config.effectiveActivationTimeoutMs(backend),
        defaults.pauseAckTimeoutMs(),
        config.effectiveSeekTimeoutMs(backend),
        defaults.reconcileTimeoutMs(),
        defaults.statusPollIntervalMs(),
        config.effectiveWsFailuresBeforeQuarantine(backend));
  }

  /** Coordinator-specific budgets: pool acquire + FIFO open (activation + slack). */
  private static LifecycleCoordinator.Tuning tuningFor(GoLibrespotConfig config, BackendConfig backend) {
    return new LifecycleCoordinator.Tuning(
        Duration.ofMillis(config.effectivePoolAcquireTimeoutMs(backend)),
        Duration.ofMillis(config.effectiveActivationTimeoutMs(backend) + FIFO_OPEN_SLACK_MS));
  }

  /**
   * FIFO re-open gate for quarantine re-admission: the daemon can only be
   * re-admitted when its FIFO still exists (a deleted pipe makes re-admission
   * pointless — the next activation would fail to open it).
   */
  private static BooleanSupplier fifoReopenOk(BackendConfig backend) {
    Path path = backend.getFifoPath();
    return () -> path != null && Files.exists(path);
  }

  // ------------------------------------------------------------ metadata selector

  /**
   * A finite, pool-backed {@link MetadataResolver.ReadyBackendSelector}.
   *
   * <p>Each invocation returns an immutable ordered snapshot containing every
   * currently READY backend at most once. The starting backend rotates between
   * invocations. Snapshot creation is synchronized; callers iterate independently.</p>
   */
  static MetadataResolver.ReadyBackendSelector poolReadyBackendSelector(ExclusivePool pool) {
    return new MetadataResolver.ReadyBackendSelector() {
      private final List<BackendHandle> handles = pool.handles();
      private int cursor;

      @Override
      public synchronized List<MetadataResolver.ReadyBackend> readyBackends() {
        int n = handles.size();
        if (n == 0) {
          return List.of();
        }
        List<MetadataResolver.ReadyBackend> ready = new java.util.ArrayList<>(n);
        for (int i = 0; i < n; i++) {
          int index = (cursor + i) % n;
          BackendHandle handle = handles.get(index);
          if (pool.stateOf(handle.getBackendId()) == BackendState.READY) {
            ready.add(new MetadataResolver.ReadyBackend(handle.getConfig().getRestBaseUrl()));
          }
        }
        cursor = (cursor + 1) % n;
        return List.copyOf(ready);
      }
    };
  }

  // ------------------------------------------------------------ runtime

  /** The complete production runtime graph (one pool, one resolver, one factory). */
  public record Runtime(
      ExclusivePool pool,
      MetadataResolver metadataResolver,
      CoordinatorFactory coordinatorFactory) {
  }

  /** One backend's eager chain. {@link #close()} is idempotent. */
  private static final class BackendChain {
    private final GoLibrespotRestClient rest;
    private final StopSequence stopSequence;
    private final CoordinatorBackedPlayback playback;
    private final AtomicBoolean closed = new AtomicBoolean();

    BackendChain(GoLibrespotRestClient rest, StopSequence stopSequence,
                 CoordinatorBackedPlayback playback) {
      this.rest = rest;
      this.stopSequence = stopSequence;
      this.playback = playback;
    }

    PlaybackCoordinator playback() {
      return playback;
    }

    /**
     * DECISIONS.md teardown: the stop sequence shuts down the coordinator
     * (cancel FIFO open + close reader), the websocket, the machine and the
     * opener, and idempotently the shared pool; the REST client (owned by no
     * other component) is closed last.
     */
    void close() {
      if (closed.compareAndSet(false, true)) {
        try {
          stopSequence.shutdown();
        } catch (RuntimeException e) {
          log.warn("go-librespot stop-sequence shutdown failed: {}",
              String.valueOf(e.getMessage()));
        }
        try {
          rest.close();
        } catch (RuntimeException e) {
          log.warn("go-librespot REST client close failed: {}",
              String.valueOf(e.getMessage()));
        }
      }
    }
  }
}
