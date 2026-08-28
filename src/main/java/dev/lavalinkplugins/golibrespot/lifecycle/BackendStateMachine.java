package dev.lavalinkplugins.golibrespot.lifecycle;

import dev.lavalinkplugins.golibrespot.backend.model.PlayerCommandResult;
import dev.lavalinkplugins.golibrespot.backend.model.StatusDto;
import dev.lavalinkplugins.golibrespot.backend.model.StatusResult;
import dev.lavalinkplugins.golibrespot.backend.rest.GoLibrespotRestClient;
import dev.lavalinkplugins.golibrespot.backend.rest.RestException;
import dev.lavalinkplugins.golibrespot.backend.ws.EventType;
import dev.lavalinkplugins.golibrespot.backend.ws.EventsWebSocketClient;
import dev.lavalinkplugins.golibrespot.backend.ws.EventsWebSocketClient.EventsListener;
import dev.lavalinkplugins.golibrespot.backend.ws.PlayerEvent;
import dev.lavalinkplugins.golibrespot.logging.LogSanitizer;
import dev.lavalinkplugins.golibrespot.pool.BackendHandle;
import dev.lavalinkplugins.golibrespot.pool.ExclusivePool;
import dev.lavalinkplugins.golibrespot.pool.Lease;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * The serialized per-backend lifecycle state machine — the correctness heart of
 * the plugin (T14; consumed by T15 coordinator, T16 seek handshake, T17 stop
 * sequence and T18 source manager).
 *
 * <p><b>States</b> (mirroring {@link dev.lavalinkplugins.golibrespot.pool.BackendState}):
 * {@code READY → LEASED → (natural completion → READY | QUARANTINING → READY |
 * DEGRADED | DEAD)}. {@link MachineState} tracks the coarse lifecycle; the
 * fine-grained {@link Phase} is the event-correlation position inside
 * {@code LEASED}: {@code IDLE, ACTIVATING, PLAYING, PAUSING, PAUSE_CONFIRMED,
 * RESUMING, SEEKING, COMPLETING}.</p>
 *
 * <p><b>Serialized lane.</b> Every command ({@link #activate}, {@link #pause},
 * {@link #resume}, {@link #seek}, {@link #retire}, {@link #quarantine},
 * {@link #checkReadmission}) and every dispatched event runs on one
 * single-threaded executor per machine. There is never more than one command
 * in flight at a time (the {@code pending} guard rejects overlap with a typed
 * {@link Outcome#FAILED}), so two racing callers can never interleave state
 * transitions. Command futures are completed on the lane; callers await them
 * from their own threads.</p>
 *
 * <p><b>URI + phase event correlation.</b> Events have no id/sequence/timestamp
 * (API_CONTRACT.md §3) — correlation is {@code uri == expected-uri} of the
 * current lease generation plus the phase that can receive the event. The
 * acceptance matrix (all rows require the event URI to equal the expected URI
 * of the current generation):</p>
 * <ul>
 *   <li>{@code playing}: accepted in {@code ACTIVATING} (fresh activation) and
 *       {@code RESUMING} (resume ack) → {@code PLAYING}; tolerated-ignored as a
 *       duplicate in {@code PLAYING}/{@code PAUSING}/{@code SEEKING};
 *       contradiction in {@code PAUSE_CONFIRMED}/{@code COMPLETING}.</li>
 *   <li>{@code paused}: accepted in {@code PAUSING} → {@code PAUSE_CONFIRMED};
 *       duplicate tolerated in {@code PAUSE_CONFIRMED}; contradiction in every
 *       other phase with a matching URI.</li>
 *   <li>{@code not_playing}: accepted in {@code PLAYING} → {@code COMPLETING},
 *       then /status reconciliation → release → {@code READY}; tolerated-ignored
 *       in {@code ACTIVATING} (stale replay of the previous lease's completion);
 *       contradiction in every other phase with a matching URI.</li>
 *   <li>{@code seek}: accepted in {@code SEEKING} when the event position equals
 *       the requested position → back to the originating phase; tolerated
 *       everywhere else.</li>
 *   <li>Events whose URI does not match, events with no URI ({@code active},
 *       {@code inactive}, {@code stopped}, {@code metadata}, {@code volume},
 *       toggles, unknown types) and events while not {@code LEASED} are always
 *       tolerated-ignored and counted ({@link #ignoredEvents()}).</li>
 * </ul>
 * <p>A <em>contradiction</em> is an event (or /status) that directly conflicts
 * with the machine's committed steady phase — it routes to a
 * <b>process-permanent</b> quarantine ({@code DEGRADED}, never re-admitted), per
 * the decision table below. There is no swallow-and-continue path: every
 * recognized failure funnels through {@link #fail(String, boolean, boolean)},
 * logs a sanitized line, and transitions.</p>
 *
 * <p><b>/status reconciliation.</b> After activation, natural completion and
 * quarantine re-admission the machine reconciles against {@code GET /status}
 * with a bounded poll ({@link Timing#reconcileTimeoutMs()}); {@code 204} = no
 * session, an unparseable 200 = reconciliation failure; both route to
 * quarantine. Pause/resume/seek fall back to a status probe when their event
 * ack times out.</p>
 *
 * <p><b>Quarantine decision table</b> (pure function {@link #decide(DecisionInput)},
 * rows tested directly):</p>
 * <pre>
 *   processUnreachable                              → DEAD
 *   wasStopTainted || contradictoryState            → DEGRADE   (process-permanent)
 *   wsFresh && statusIdle && fifoReopenOk           → REUSE
 *   consecutiveFailures >= quarantineThreshold      → QUARANTINE
 *   default                                         → QUARANTINE
 * </pre>
 * <p>Re-admission (transient {@code QUARANTINING}) therefore requires a fresh
 * WebSocket connection + an idle /status (200 with {@code stopped && track ==
 * null}) + the injected FIFO-reopen {@link BooleanSupplier} (T15 owns the real
 * FIFO reopen — this machine never opens FIFOs).</p>
 *
 * <p><b>Generation coordination with T9/T13.</b> The machine owns a generation
 * counter kept in lockstep with the pool's lease generation (set from
 * {@link Lease#generation()} on activate, +1 on quarantine and on
 * play-over-play replacement; {@link #generation()}). The T9 WS filter
 * (a {@link java.util.function.LongSupplier} sampled at connection open;
 * {@code captured < current} drops the event) is armed with the machine's
 * <em>quarantine epoch</em>, which rises only on quarantine — a rising
 * per-activation value would starve the persistent connection's events. The
 * machine ALSO does its own stale-connection check (events delivered by a
 * connection that opened before the current quarantine epoch are dropped),
 * so correctness never depends on the WS filter.</p>
 *
 * <p><b>No stop endpoint.</b> This machine never issues the daemon stop
 * endpoint (v0.9.0 stop-race, API_CONTRACT.md §5). A logical stop is T17's
 * remote-pause + confirmation + {@link #retire()} (generation retired, stale
 * events dropped by the {@code READY} guard).</p>
 */
public final class BackendStateMachine implements AutoCloseable {

  /** Coarse lifecycle state; mirrors {@link dev.lavalinkplugins.golibrespot.pool.BackendState}. */
  public enum MachineState {
    READY, LEASED, QUARANTINING, DEGRADED, DEAD
  }

  /** Fine-grained event-correlation phase while {@link MachineState#LEASED}. */
  public enum Phase {
    IDLE, ACTIVATING, PLAYING, PAUSING, PAUSE_CONFIRMED, RESUMING, SEEKING, COMPLETING
  }

  /** Quarantine decision table output. */
  public enum Decision { REUSE, QUARANTINE, DEGRADE, DEAD }

  /** Typed command outcome. */
  public enum Outcome {
    /** Command succeeded (event ack or status fallback confirmed it). */
    OK,
    /** Command rejected without quarantining (wrong phase, command in flight, idle backend). */
    FAILED,
    /** The backend was transiently quarantined by this command's failure. */
    QUARANTINED,
    /** The backend was permanently quarantined (stop-taint / contradiction). */
    DEGRADED,
    /** The machine is dead (process unreachable / closed). */
    DEAD
  }

  /** Result of one command. */
  public record Result(Outcome outcome, String reason) {
    public boolean isOk() {
      return outcome == Outcome.OK;
    }

    public static Result ok(String reason) {
      return new Result(Outcome.OK, reason);
    }

    public static Result failed(String reason) {
      return new Result(Outcome.FAILED, reason);
    }

    public static Result quarantined(String reason) {
      return new Result(Outcome.QUARANTINED, reason);
    }

    public static Result degraded(String reason) {
      return new Result(Outcome.DEGRADED, reason);
    }

    public static Result dead(String reason) {
      return new Result(Outcome.DEAD, reason);
    }
  }

  /** Inputs of the quarantine decision table (see class javadoc for the rows). */
  public record DecisionInput(
      boolean wasStopTainted,
      boolean contradictoryState,
      boolean wsFresh,
      boolean statusIdle,
      boolean fifoReopenOk,
      int consecutiveFailures,
      int quarantineThreshold,
      boolean processUnreachable) {}

  /** Bounded wait budgets. Defaults mirror DECISIONS.md constants. */
  public record Timing(
      long activationTimeoutMs,
      long pauseAckTimeoutMs,
      long seekAckTimeoutMs,
      long reconcileTimeoutMs,
      long statusPollIntervalMs,
      int quarantineThreshold) {
    public static Timing defaults() {
      return new Timing(15_000, 5_000, 10_000, 2_000, 250, 5);
    }
  }

  /** Lifecycle callbacks for the coordinator (T15) — all fired on the lane. */
  public interface LifecycleListener {
    /** The backend completed a track naturally and released its lease (→ READY). */
    default void onNaturalCompletion() {}

    /** A transient quarantine was lifted and the backend is READY again. */
    default void onReAdmitted() {}

    /** The backend was quarantined; {@code permanent} ⇒ never re-admitted. */
    default void onQuarantined(boolean permanent) {}

    /** The machine reached the terminal DEAD state. */
    default void onDead() {}
  }

  /** What a /status reconciliation probe is trying to confirm. */
  private enum ReconcileKind { PLAYING, IDLE, PAUSED, SEEKED }

  /** Outcome of one /status reconciliation probe. */
  private enum ReconcileOutcome { OK, NO_SESSION, UNPARSEABLE, MISMATCH, TIMEOUT, UNREACHABLE }

  // ------------------------------------------------------------ fields

  private final BackendHandle handle;
  private final GoLibrespotRestClient rest;
  private final ExclusivePool pool;
  private final Timing timing;
  private final BooleanSupplier fifoReopenOk;
  private final LifecycleListener listener;
  private final Consumer<String> logSink;
  private final LogSanitizer sanitizer = LogSanitizer.defaults();

  private final ExecutorService lane;
  private final ScheduledExecutorService scheduler;

  /** Quarantine epoch fed to the T9 WS generation filter; rises ONLY on quarantine. */
  private final AtomicLong wsEpoch = new AtomicLong();
  /** Lease-generation counter in lockstep with the pool (see class javadoc). */
  private final AtomicLong generation = new AtomicLong();
  private final AtomicLong ignoredEvents = new AtomicLong();
  private final AtomicLong consecutiveFailures = new AtomicLong();

  // ---- lane-guarded mutable state (volatile only for cross-thread reads)
  private volatile MachineState state = MachineState.READY;
  private volatile Phase phase = Phase.IDLE;
  private volatile boolean stopTainted;
  private volatile boolean wsLive;
  private volatile long connectedEpoch = -1;
  private volatile String expectedUri;
  private volatile Lease lease;
  private volatile Phase phaseBeforeSeek;
  private volatile long expectedSeekPosition;
  private volatile boolean closed;
  private CompletableFuture<Result> pending;
  /** F4: a pause queued while the machine was ACTIVATING, applied on activation confirm. */
  private CompletableFuture<Result> queuedPause;
  /** P1: armed by the coordinator before an idempotent reload; the reload's echo is tolerated. */
  private volatile boolean activationReloadArmed;
  private volatile long activationReloadGeneration;

  // ------------------------------------------------------------ construction

  /**
   * @param handle      the registered backend handle (id = config name)
   * @param rest        the T8 REST client for this backend
   * @param pool        the T13 pool the machine keeps in lockstep with
   * @param timing      bounded wait budgets (see {@link Timing#defaults()})
   * @param fifoReopenOk injectable FIFO-health predicate (T15 wires the real
   *                    FIFO reopen; this machine never opens FIFOs); {@code true}
   *                    when a re-admission may proceed
   * @param listener    lifecycle callbacks
   * @param logSink     receives sanitized diagnostic lines (every line already
   *                    passed through {@link LogSanitizer}; default no-op)
   */
  public BackendStateMachine(
      BackendHandle handle,
      GoLibrespotRestClient rest,
      ExclusivePool pool,
      Timing timing,
      BooleanSupplier fifoReopenOk,
      LifecycleListener listener,
      Consumer<String> logSink) {
    this(handle, rest, pool, timing, fifoReopenOk, listener, logSink,
        Executors.newSingleThreadExecutor(r -> daemonThread(r, "golibrespot-machine-" + handle.getBackendId())),
        Executors.newSingleThreadScheduledExecutor(r -> daemonThread(r, "golibrespot-machine-timer-" + handle.getBackendId())));
  }

  /** Convenience constructor: fifo re-open assumed OK, no-op listener/log sink. */
  public BackendStateMachine(BackendHandle handle, GoLibrespotRestClient rest,
                             ExclusivePool pool, Timing timing) {
    this(handle, rest, pool, timing, () -> true, new LifecycleListener() {}, s -> {});
  }

  /** Test-friendly constructor with injectable executors (caller owns their shutdown). */
  BackendStateMachine(
      BackendHandle handle,
      GoLibrespotRestClient rest,
      ExclusivePool pool,
      Timing timing,
      BooleanSupplier fifoReopenOk,
      LifecycleListener listener,
      Consumer<String> logSink,
      ExecutorService lane,
      ScheduledExecutorService scheduler) {
    this.handle = Objects.requireNonNull(handle, "handle");
    this.rest = Objects.requireNonNull(rest, "rest");
    this.pool = Objects.requireNonNull(pool, "pool");
    this.timing = Objects.requireNonNull(timing, "timing");
    this.fifoReopenOk = Objects.requireNonNull(fifoReopenOk, "fifoReopenOk");
    this.listener = Objects.requireNonNull(listener, "listener");
    this.logSink = Objects.requireNonNull(logSink, "logSink");
    this.lane = Objects.requireNonNull(lane, "lane");
    this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
  }

  private static Thread daemonThread(Runnable runnable, String name) {
    Thread thread = new Thread(runnable, name);
    thread.setDaemon(true);
    return thread;
  }

  // ------------------------------------------------------------ WS wiring

  /**
   * The T9 {@link EventsListener} adapter. The coordinator constructs its
   * {@link EventsWebSocketClient} with this listener; every callback is routed
   * onto the serialized lane and never blocks the WS drain thread.
   */
  public EventsListener eventsListener() {
    return new EventsListener() {
      @Override
      public void onEvent(PlayerEvent event) {
        submit(() -> handleEvent(event));
      }

      @Override
      public void onUnknownEvent(PlayerEvent event) {
        submit(() -> ignoredEvents.incrementAndGet());
      }

      @Override
      public void onQuarantine() {
        submit(() -> fail("ws client quarantined after repeated failures", false, false));
      }

      @Override
      public void onConnected() {
        submit(() -> handleWsConnected());
      }

      @Override
      public void onDisconnected() {
        submit(() -> handleWsDisconnected());
      }
    };
  }

  /**
   * Arms the T9 generation filter with this machine's quarantine epoch. Call
   * <b>before</b> {@code ws.start()} so the first connection samples the
   * current epoch. Optional — the machine's own stale-connection check keeps
   * correctness even when this is never called.
   */
  public void attachWebSocket(EventsWebSocketClient ws) {
    Objects.requireNonNull(ws, "ws");
    ws.setGenerationSupplier(() -> wsEpoch.get());
  }

  // ------------------------------------------------------------ commands

  /**
   * Starts playback of {@code uri} on the held {@code lease} and awaits the
   * current-generation {@code playing} event (activation barrier), then
   * reconciles /status. READY → LEASED/ACTIVATING → PLAYING. May also be called
   * while already LEASED for play-over-play replacement (generation bumped).
   *
   * @param positionMs start position in ms
   * @return OK when activated; QUARANTINED on barrier timeout / transport /
   *         HTTP failure; DEGRADED when /status contradicts the playing event
   */
  public CompletableFuture<Result> activate(Lease lease, String uri, long positionMs) {
    Objects.requireNonNull(lease, "lease");
    Objects.requireNonNull(uri, "uri");
    return enqueue(f -> startActivation(lease, uri, positionMs, f));
  }

  /** Remote pause → matching {@code paused} event (or paused /status on timeout). */
  public CompletableFuture<Result> pause() {
    return enqueue(this::startPause);
  }

  /** Remote resume → matching {@code playing} event (or playing /status on timeout). */
  public CompletableFuture<Result> resume() {
    return enqueue(this::startResume);
  }

  /** Absolute seek → matching {@code seek} event with the requested position. */
  public CompletableFuture<Result> seek(long positionMs) {
    return enqueue(f -> startSeek(positionMs, f));
  }

  /**
   * Retires the current lease without quarantining (logical stop / cleanup):
   * releases the lease, clears the expected URI and returns to READY. Stale
   * events for the retired generation are dropped by the READY guard.
   */
  public CompletableFuture<Result> retire() {
    return enqueue(this::startRetire);
  }

  /**
   * Quarantines the backend from the player layer (track stuck / load
   * failure). {@code permanent} ⇒ DEGRADED (contradictory state / stop-taint);
   * otherwise transient QUARANTINING.
   */
  public CompletableFuture<Result> quarantine(String reason, boolean permanent) {
    Objects.requireNonNull(reason, "reason");
    return enqueue(f -> {
      Result result = fail(reason, permanent, false);
      f.complete(result);
    });
  }

  /**
   * Evaluates the quarantine decision table for re-admission: fresh WS + idle
   * /status + FIFO-reopen predicate → REUSE (pool {@code markReady}, READY).
   */
  public CompletableFuture<Result> checkReadmission() {
    return enqueue(f -> checkReadmissionInternal(f));
  }

  // ------------------------------------------------------------ accessors

  public MachineState state() {
    return state;
  }

  public Phase phase() {
    return phase;
  }

  /** Current event-correlation generation (lockstep with the pool lease generation). */
  public long generation() {
    return generation.get();
  }

  /** Events tolerated-ignored so far (wrong URI / no URI / stale connection / dup). */
  public long ignoredEvents() {
    return ignoredEvents.get();
  }

  public boolean isStopTainted() {
    return stopTainted;
  }

  /** Marks the backend stop-tainted: every future failure decision becomes permanent. */
  public void markStopTainted() {
    stopTainted = true;
  }

  /** True when a connection opened at the current quarantine epoch is live. */
  boolean wsFresh() {
    return wsLive && connectedEpoch >= wsEpoch.get();
  }

  // ------------------------------------------------------------ lane plumbing

  private <T> CompletableFuture<T> enqueue(Consumer<CompletableFuture<T>> task) {
    CompletableFuture<T> future = new CompletableFuture<>();
    try {
      lane.execute(() -> {
        try {
          task.accept(future);
        } catch (Throwable t) {
          future.completeExceptionally(t);
        }
      });
    } catch (RejectedExecutionException e) {
      future.completeExceptionally(new IllegalStateException("state machine closed", e));
    }
    return future;
  }

  private void submit(Runnable task) {
    try {
      lane.execute(() -> {
        try {
          task.run();
        } catch (Throwable t) {
          log("internal handler error: " + sanitizer.sanitize(String.valueOf(t)));
        }
      });
    } catch (RejectedExecutionException ignored) {
      // machine closed — nothing left to process
    }
  }

  private void scheduleTimeout(CompletableFuture<Result> expected, long ms, Phase guardPhase,
                               Runnable onTimeout) {
    scheduler.schedule(() -> submit(() -> {
      // phase guard: a stale timeout must never fire once the barrier phase has
      // moved on (e.g. the playing event arrived and the status reconcile is
      // running — that reconcile owns the outcome now)
      if (pending == expected && phase == guardPhase) {
        onTimeout.run();
      }
    }), ms, TimeUnit.MILLISECONDS);
  }

  private void completePending(Result result) {
    CompletableFuture<Result> current = pending;
    pending = null;
    if (current != null) {
      current.complete(result);
    }
  }

  // ------------------------------------------------------------ commands (on the lane)

  private void startActivation(Lease lease, String uri, long positionMs, CompletableFuture<Result> f) {
    CompletableFuture<Result> stalePause = queuedPause;
    queuedPause = null;
    if (stalePause != null) {
      stalePause.complete(Result.failed("activation superseded"));
    }
    activationReloadArmed = false; // a fresh activation has no reload in flight
    switch (state) {
      case DEAD -> {
        f.complete(Result.dead("machine dead"));
        return;
      }
      case DEGRADED -> {
        f.complete(Result.degraded("backend degraded"));
        return;
      }
      case QUARANTINING -> {
        f.complete(Result.quarantined("quarantined; checkReadmission first"));
        return;
      }
      case LEASED -> {
        if (pending != null) {
          f.complete(Result.failed("command in flight"));
          return;
        }
        generation.incrementAndGet(); // play-over-play replacement
      }
      case READY -> {
        if (!lease.backend().getBackendId().equals(handle.getBackendId())) {
          f.complete(Result.failed("lease belongs to backend '"
              + lease.backend().getBackendId() + "', machine is '" + handle.getBackendId() + "'"));
          return;
        }
        // P3: adopt the lease only when it is still the pool's CURRENT active
        // lease — a replace-vs-natural-completion interleaving can otherwise
        // adopt an already-released lease the pool has re-granted to another
        // session (two sessions on one daemon). isActive() verifies the pool's
        // (state, generation, leaseId) tuple still names this lease.
        if (!lease.isActive()) {
          f.complete(Result.failed("lease is no longer active; stale adoption rejected"));
          return;
        }
        this.lease = lease;
        state = MachineState.LEASED;
        generation.set(lease.generation());
      }
    }
    expectedUri = uri;
    phase = Phase.ACTIVATING;
    pending = f;
    rest.playAsync(uri, positionMs, false).whenComplete(
        (r, ex) -> submit(() -> handlePlayResponse(r, unwrapRest(ex))));
    scheduleTimeout(f, timing.activationTimeoutMs(), Phase.ACTIVATING,
        () -> fail("activation barrier timeout for '" + uri + "'", false, false));
  }

  private void handlePlayResponse(PlayerCommandResult r, RestException ex) {
    if (ex != null) {
      fail("play transport: " + ex.kind(), false, processUnreachable(ex));
      return;
    }
    if (r.is2xx()) {
      return; // accepted for processing — the barrier awaits the playing event
    }
    if (r.isNoSession()) {
      fail("play answered 204 (no session)", false, false);
      return;
    }
    fail("play answered http " + r.status(), false, false);
  }

  private void startPause(CompletableFuture<Result> f) {
    if (!requireLeased(f)) {
      return;
    }
    // F4: a pause requested while activation is in flight cannot be issued yet
    // (the daemon has no playing session) and must not be silently dropped —
    // the daemon would keep playing while the player is paused. Queue it and
    // apply it the moment the activation confirms.
    if (phase == Phase.ACTIVATING) {
      if (queuedPause != null) {
        f.complete(Result.failed("pause already queued during activation"));
        return;
      }
      queuedPause = f;
      return;
    }
    if (!requireIdleCommand(f)) {
      return;
    }
    switch (phase) {
      case PLAYING -> issuePause(f);
      case PAUSE_CONFIRMED -> f.complete(Result.ok("already paused"));
      default -> f.complete(Result.failed("cannot pause from " + phase));
    }
  }

  /** Issues the remote pause and awaits the matching {@code paused} event (or a paused /status). */
  private void issuePause(CompletableFuture<Result> f) {
    phase = Phase.PAUSING;
    pending = f;
    rest.pauseAsync().whenComplete((r, ex) -> submit(() -> handlePauseResponse(r, unwrapRest(ex))));
    scheduleTimeout(f, timing.pauseAckTimeoutMs(), Phase.PAUSING, () -> {
      reconcile(ReconcileKind.PAUSED, expectedUri, -1, reconcileDeadline(), outcome -> {
        if (pending == f) { // the paused event did not already confirm
          if (outcome == ReconcileOutcome.OK) {
            phase = Phase.PAUSE_CONFIRMED;
            completePending(Result.ok("paused (status)"));
          } else {
            fail("pause ack timeout (" + outcome + ")", false, outcome == ReconcileOutcome.UNREACHABLE);
          }
        }
      });
    });
  }

  /** F4: applies a pause queued while the machine was ACTIVATING. Runs on the lane. */
  private void applyQueuedPause() {
    CompletableFuture<Result> queued = queuedPause;
    queuedPause = null;
    if (queued == null) {
      return;
    }
    if (phase != Phase.PLAYING || pending != null) {
      queued.complete(Result.failed("cannot pause from " + phase));
      return;
    }
    issuePause(queued);
  }

  private void handlePauseResponse(PlayerCommandResult r, RestException ex) {
    if (ex != null) {
      fail("pause transport: " + ex.kind(), false, processUnreachable(ex));
      return;
    }
    if (r.is2xx()) {
      return;
    }
    if (r.isNoSession()) {
      fail("pause answered 204 (no session)", false, false);
      return;
    }
    fail("pause answered http " + r.status(), false, false);
  }

  private void startResume(CompletableFuture<Result> f) {
    if (!requireLeased(f) || !requireIdleCommand(f)) {
      return;
    }
    switch (phase) {
      case PAUSE_CONFIRMED -> {
        phase = Phase.RESUMING;
        pending = f;
        rest.resumeAsync().whenComplete((r, ex) -> submit(() -> handleResumeResponse(r, unwrapRest(ex))));
        scheduleTimeout(f, timing.pauseAckTimeoutMs(), Phase.RESUMING, () -> {
          reconcile(ReconcileKind.PLAYING, expectedUri, -1, reconcileDeadline(), outcome -> {
            if (pending == f) { // the playing event did not already confirm
              if (outcome == ReconcileOutcome.OK) {
                phase = Phase.PLAYING;
                completePending(Result.ok("resumed (status)"));
              } else {
                fail("resume ack timeout (" + outcome + ")", false, outcome == ReconcileOutcome.UNREACHABLE);
              }
            }
          });
        });
      }
      case PLAYING -> f.complete(Result.ok("already playing"));
      default -> f.complete(Result.failed("cannot resume from " + phase));
    }
  }

  private void handleResumeResponse(PlayerCommandResult r, RestException ex) {
    if (ex != null) {
      fail("resume transport: " + ex.kind(), false, processUnreachable(ex));
      return;
    }
    if (r.is2xx()) {
      return;
    }
    if (r.isNoSession()) {
      fail("resume answered 204 (no session)", false, false);
      return;
    }
    fail("resume answered http " + r.status(), false, false);
  }

  private void startSeek(long positionMs, CompletableFuture<Result> f) {
    if (!requireLeased(f) || !requireIdleCommand(f)) {
      return;
    }
    switch (phase) {
      case PLAYING, PAUSE_CONFIRMED -> {
        phaseBeforeSeek = phase;
        phase = Phase.SEEKING;
        expectedSeekPosition = positionMs;
        pending = f;
        rest.seekAsync(positionMs).whenComplete((r, ex) -> submit(() -> handleSeekResponse(r, unwrapRest(ex))));
        scheduleTimeout(f, timing.seekAckTimeoutMs(), Phase.SEEKING, () -> {
          reconcile(ReconcileKind.SEEKED, expectedUri, positionMs, reconcileDeadline(), outcome -> {
            if (pending == f) { // the seek event did not already confirm
              if (outcome == ReconcileOutcome.OK) {
                phase = phaseBeforeSeek;
                completePending(Result.ok("seeked to " + positionMs + " (status)"));
              } else {
                fail("seek ack timeout (" + outcome + ")", false, outcome == ReconcileOutcome.UNREACHABLE);
              }
            }
          });
        });
      }
      default -> f.complete(Result.failed("cannot seek from " + phase));
    }
  }

  private void handleSeekResponse(PlayerCommandResult r, RestException ex) {
    if (ex != null) {
      fail("seek transport: " + ex.kind(), false, processUnreachable(ex));
      return;
    }
    if (r.is2xx()) {
      return;
    }
    if (r.isNoSession()) {
      fail("seek answered 204 (no session)", false, false);
      return;
    }
    fail("seek answered http " + r.status(), false, false);
  }

  private void startRetire(CompletableFuture<Result> f) {
    if (state == MachineState.DEAD) {
      f.complete(Result.dead("machine dead"));
      return;
    }
    if (state != MachineState.LEASED) {
      f.complete(Result.failed("not leased (state=" + state + ")"));
      return;
    }
    if (pending != null) {
      f.complete(Result.failed("command in flight"));
      return;
    }
    releaseToReady("retired");
    f.complete(Result.ok("lease released"));
  }

  private boolean requireLeased(CompletableFuture<Result> f) {
    switch (state) {
      case DEAD -> {
        f.complete(Result.dead("machine dead"));
        return false;
      }
      case DEGRADED -> {
        f.complete(Result.degraded("backend degraded"));
        return false;
      }
      case QUARANTINING -> {
        f.complete(Result.quarantined("quarantined"));
        return false;
      }
      case READY -> {
        f.complete(Result.failed("backend idle"));
        return false;
      }
      default -> {
        return true;
      }
    }
  }

  private boolean requireIdleCommand(CompletableFuture<Result> f) {
    if (pending != null) {
      f.complete(Result.failed("command in flight"));
      return false;
    }
    return true;
  }

  // ------------------------------------------------------------ events (on the lane)

  private void handleEvent(PlayerEvent event) {
    if (state != MachineState.LEASED) {
      ignoredEvents.incrementAndGet();
      return;
    }
    if (connectedEpoch < 0) {
      connectedEpoch = wsEpoch.get(); // lazy capture, mirrors T9
    }
    if (connectedEpoch < wsEpoch.get()) {
      ignoredEvents.incrementAndGet(); // stale pre-quarantine connection
      return;
    }
    EventType type = event.type();
    String uri = uriOf(event);
    boolean matches = expectedUri != null && expectedUri.equals(uri);
    switch (type) {
      case PLAYING -> handlePlaying(event, matches);
      case PAUSED -> handlePaused(matches);
      case NOT_PLAYING -> handleNotPlaying(matches);
      case SEEK -> handleSeek(event, matches);
      default -> ignoredEvents.incrementAndGet(); // active/inactive/stopped/metadata/volume/toggles/unknown
    }
  }

  private void handlePlaying(PlayerEvent event, boolean matches) {
    if (!matches) {
      ignoredEvents.incrementAndGet();
      return;
    }
    switch (phase) {
      case ACTIVATING -> {
        phase = Phase.PLAYING;
        reconcile(ReconcileKind.PLAYING, expectedUri, -1, reconcileDeadline(), outcome -> {
          if (outcome == ReconcileOutcome.OK) {
            completePending(Result.ok("activated: " + expectedUri));
            applyQueuedPause(); // F4: a pause queued during activation applies now
          } else {
            fail("status contradicts playing event (" + outcome + ")", true,
                outcome == ReconcileOutcome.UNREACHABLE);
          }
        });
      }
      case RESUMING -> {
        phase = Phase.PLAYING;
        completePending(Result.ok("resumed"));
      }
      case PLAYING, PAUSING, SEEKING -> ignoredEvents.incrementAndGet(); // re-affirmation / dup
      case PAUSE_CONFIRMED, COMPLETING ->
          fail("contradictory playing while " + phase, true, false);
      case IDLE -> ignoredEvents.incrementAndGet();
    }
  }

  private void handlePaused(boolean matches) {
    if (!matches) {
      ignoredEvents.incrementAndGet();
      return;
    }
    switch (phase) {
      case PAUSING -> {
        phase = Phase.PAUSE_CONFIRMED;
        completePending(Result.ok("paused"));
      }
      case PAUSE_CONFIRMED -> ignoredEvents.incrementAndGet(); // duplicate ack
      default -> fail("contradictory paused while " + phase, true, false);
    }
  }

  private void handleNotPlaying(boolean matches) {
    if (!matches) {
      ignoredEvents.incrementAndGet();
      return;
    }
    switch (phase) {
      case PLAYING -> {
        if (activationReloadArmed && activationReloadGeneration == generation.get()) {
          probeReloadEcho();
        } else {
          startCompletionReconcile();
        }
      }
      case ACTIVATING -> ignoredEvents.incrementAndGet(); // stale replay of the previous lease
      default -> fail("contradictory not_playing while " + phase, true, false);
    }
  }

  private void handleSeek(PlayerEvent event, boolean matches) {
    if (!matches) {
      ignoredEvents.incrementAndGet();
      return;
    }
    if (phase != Phase.SEEKING) {
      ignoredEvents.incrementAndGet(); // tolerated outside the seek handshake
      return;
    }
    long position = positionOf(event);
    if (position == expectedSeekPosition) {
      phase = phaseBeforeSeek;
      completePending(Result.ok("seeked to " + position));
    } else {
      fail("seek ack mismatch: expected " + expectedSeekPosition + " got " + position, false, false);
    }
  }

  private void handleWsConnected() {
    wsLive = true;
    connectedEpoch = wsEpoch.get();
    if (state == MachineState.QUARANTINING) {
      checkReadmissionInternal(null);
    }
  }

  private void handleWsDisconnected() {
    wsLive = false;
    if (state == MachineState.LEASED) {
      fail("ws connection lost", false, false);
    }
  }

  /**
   * P1: arms the reload-echo tolerance for the CURRENT activation. Called by
   * the coordinator immediately before it re-issues the idempotent play
   * (stale-advance reload); a subsequent matching {@code not_playing} that the
   * /status reconcile finds while the daemon is still playing is that reload's
   * echo and is tolerated by {@link #handleNotPlaying} instead of degrading.
   * The arming is per-generation and reset on every new activation.
   */
  void noteActivationReload() {
    activationReloadArmed = true;
    activationReloadGeneration = generation.get();
  }

  /**
   * P1: a {@code not_playing} in PLAYING while a reload is armed for the current
   * generation may be the idempotent reload's echo — the re-issued play re-emits
   * {@code not_playing} while the track actually keeps playing. The normal
   * completion reconcile would only ever deliver TIMEOUT for a "still playing"
   * daemon (MISMATCH is retried until the deadline), so probe /status once:
   * still playing ⇒ echo (revert to PLAYING); idle ⇒ genuine completion; any
   * ambiguous answer ⇒ fall back to the bounded completion reconcile.
   */
  private void probeReloadEcho() {
    rest.statusAsync().whenComplete((status, ex) -> submit(() -> {
      if (phase != Phase.PLAYING) {
        return; // a command interleaved — it owns the outcome
      }
      if (ex != null || status == null || status.isNoSession()
          || !status.is2xx() || status.parsed().isEmpty()) {
        startCompletionReconcile();
        return;
      }
      if (status.parsed().get().stopped()) {
        releaseToReady("natural completion");
      } else {
        activationReloadArmed = false;
        phase = Phase.PLAYING;
        ignoredEvents.incrementAndGet();
        log("tolerated reload-echo not_playing for '" + expectedUri + "' (daemon still playing)");
      }
    }));
  }

  /** The bounded completion reconcile: release on idle, DEGRADE on contradiction. */
  private void startCompletionReconcile() {
    phase = Phase.COMPLETING;
    reconcile(ReconcileKind.IDLE, expectedUri, -1, reconcileDeadline(), outcome -> {
      if (outcome == ReconcileOutcome.OK) {
        releaseToReady("natural completion");
      } else {
        fail("completion status mismatch (" + outcome + ")", true,
            outcome == ReconcileOutcome.UNREACHABLE);
      }
    });
  }

  // ------------------------------------------------------------ /status reconciliation

  private long reconcileDeadline() {
    return System.currentTimeMillis() + timing.reconcileTimeoutMs();
  }

  private void reconcile(ReconcileKind kind, String uri, long position, long deadlineMs,
                         Consumer<ReconcileOutcome> onDone) {
    if (System.currentTimeMillis() >= deadlineMs) {
      onDone.accept(ReconcileOutcome.TIMEOUT);
      return;
    }
    rest.statusAsync().whenComplete((status, ex) -> submit(() -> {
      if (ex != null) {
        RestException cause = unwrapRest(ex);
        if (cause != null && cause.kind() == RestException.Kind.IO) {
          onDone.accept(ReconcileOutcome.UNREACHABLE);
          return;
        }
        retryReconcile(kind, uri, position, deadlineMs, onDone);
        return;
      }
      ReconcileOutcome outcome = evaluate(kind, uri, position, status);
      if (outcome == ReconcileOutcome.OK || System.currentTimeMillis() >= deadlineMs) {
        onDone.accept(outcome);
        return;
      }
      if (outcome == ReconcileOutcome.NO_SESSION
          || outcome == ReconcileOutcome.UNPARSEABLE
          || outcome == ReconcileOutcome.UNREACHABLE) {
        onDone.accept(outcome); // fail fast — no amount of polling fixes these
        return;
      }
      retryReconcile(kind, uri, position, deadlineMs, onDone);
    }));
  }

  private void retryReconcile(ReconcileKind kind, String uri, long position, long deadlineMs,
                              Consumer<ReconcileOutcome> onDone) {
    if (System.currentTimeMillis() < deadlineMs) {
      scheduler.schedule(
          () -> submit(() -> reconcile(kind, uri, position, deadlineMs, onDone)),
          timing.statusPollIntervalMs(), TimeUnit.MILLISECONDS);
    } else {
      onDone.accept(ReconcileOutcome.TIMEOUT);
    }
  }

  private ReconcileOutcome evaluate(ReconcileKind kind, String uri, long position, StatusResult status) {
    if (status.isNoSession()) {
      return ReconcileOutcome.NO_SESSION; // 204 — no session
    }
    if (!status.is2xx()) {
      return ReconcileOutcome.MISMATCH;
    }
    if (status.parsed().isEmpty()) {
      return ReconcileOutcome.UNPARSEABLE; // 200 with a body that will not parse
    }
    StatusDto dto = status.parsed().get();
    return switch (kind) {
      case PLAYING ->
          dto.stopped() == false && dto.track() != null && uri.equals(dto.track().uri())
              ? ReconcileOutcome.OK : ReconcileOutcome.MISMATCH;
      case IDLE -> dto.stopped() ? ReconcileOutcome.OK : ReconcileOutcome.MISMATCH;
      case PAUSED -> dto.paused() ? ReconcileOutcome.OK : ReconcileOutcome.MISMATCH;
      case SEEKED ->
          dto.track() != null && dto.track().position() == position
              ? ReconcileOutcome.OK : ReconcileOutcome.MISMATCH;
    };
  }

  // ------------------------------------------------------------ quarantine decision

  /**
   * The quarantine decision table (pure; see class javadoc for the documented
   * rows). Every row is exercised by {@code BackendStateMachineTest}.
   */
  public static Decision decide(DecisionInput in) {
    if (in.processUnreachable()) {
      return Decision.DEAD;
    }
    if (in.wasStopTainted() || in.contradictoryState()) {
      return Decision.DEGRADE;
    }
    if (in.wsFresh() && in.statusIdle() && in.fifoReopenOk()) {
      return Decision.REUSE;
    }
    return Decision.QUARANTINE;
  }

  /**
   * The single failure funnel: sanitized log + decision-table transition. Every
   * recognized failure (timeout, non-2xx, hang, WS loss, contradiction, status
   * mismatch, transport error) routes through here — there is no
   * swallow-and-continue path.
   *
   * @param reason            human-readable, sanitized before logging
   * @param contradictory     {@code true} = committed-state contradiction
   *                          (process-permanent)
   * @param processUnreachable {@code true} = the daemon process is gone (DEAD)
   * @return the applied result
   */
  private Result fail(String reason, boolean contradictory, boolean processUnreachable) {
    if (state == MachineState.DEAD) {
      Result result = Result.dead("machine dead");
      completePending(result);
      return result;
    }
    if (state == MachineState.DEGRADED) {
      Result result = Result.degraded("already degraded");
      completePending(result);
      return result;
    }
    if (pool.isShutdown()) {
      transitionDead();
      Result result = Result.dead("pool shut down");
      completePending(result);
      return result;
    }
    consecutiveFailures.incrementAndGet();
    Decision decision = decide(new DecisionInput(
        stopTainted, contradictory, false, false,
        fifoReopenOk.getAsBoolean(), consecutiveFailures.intValue(),
        timing.quarantineThreshold(), processUnreachable));
    log("failure on backend '" + handle.getBackendId() + "': " + sanitizer.sanitize(reason)
        + " (consecutive=" + consecutiveFailures + ") -> " + decision);
    Result result = applyDecision(decision, reason, true);
    completePending(result);
    CompletableFuture<Result> queued = queuedPause;
    queuedPause = null;
    if (queued != null) {
      queued.complete(result); // resolve a pause queued during the failed activation
    }
    return result;
  }

  private Result applyDecision(Decision decision, String reason, boolean bumpEpochOnQuarantine) {
    switch (decision) {
      case REUSE -> {
        pool.markReady(handle.getBackendId());
        state = MachineState.READY;
        phase = Phase.IDLE;
        expectedUri = null;
        consecutiveFailures.set(0);
        log("backend '" + handle.getBackendId() + "' re-admitted (" + sanitizer.sanitize(reason) + ")");
        listener.onReAdmitted();
        return Result.ok("re-admitted");
      }
      case QUARANTINE -> {
        if (bumpEpochOnQuarantine) {
          // coming from a live lease: the connection that delivered the failure
          // is stale — drop its events until the WS reconnects and samples the
          // new epoch. Re-admission re-checks never bump (the connection there
          // may already be the fresh post-quarantine one).
          wsEpoch.incrementAndGet();
        }
        pool.markQuarantined(handle.getBackendId(), false);
        state = MachineState.QUARANTINING;
        phase = Phase.IDLE;
        expectedUri = null;
        lease = null;
        log("backend '" + handle.getBackendId() + "' quarantined (transient): "
            + sanitizer.sanitize(reason));
        listener.onQuarantined(false);
        return Result.quarantined(reason);
      }
      case DEGRADE -> {
        pool.markQuarantined(handle.getBackendId(), true);
        state = MachineState.DEGRADED;
        phase = Phase.IDLE;
        expectedUri = null;
        lease = null;
        log("backend '" + handle.getBackendId() + "' degraded (permanent): "
            + sanitizer.sanitize(reason));
        listener.onQuarantined(true);
        return Result.degraded(reason);
      }
      case DEAD -> {
        pool.markQuarantined(handle.getBackendId(), true);
        transitionDead();
        log("backend '" + handle.getBackendId() + "' dead: " + sanitizer.sanitize(reason));
        listener.onDead();
        return Result.dead(reason);
      }
      default -> throw new IllegalStateException("unhandled decision " + decision);
    }
  }

  private void transitionDead() {
    state = MachineState.DEAD;
    phase = Phase.IDLE;
    expectedUri = null;
    lease = null;
  }

  private void releaseToReady(String reason) {
    Lease held = lease;
    lease = null;
    if (held != null) {
      held.release(); // idempotent; invalidates nothing when already released
    }
    state = MachineState.READY;
    phase = Phase.IDLE;
    expectedUri = null;
    log("backend '" + handle.getBackendId() + "' -> READY (" + sanitizer.sanitize(reason) + ")");
    listener.onNaturalCompletion();
  }

  private void checkReadmissionInternal(CompletableFuture<Result> future) {
    if (state != MachineState.QUARANTINING) {
      if (future != null) {
        future.complete(Result.failed("not quarantining (state=" + state + ")"));
      }
      return;
    }
    probeStatusIdle(System.currentTimeMillis() + timing.reconcileTimeoutMs(), future);
  }

  private void probeStatusIdle(long deadlineMs, CompletableFuture<Result> future) {
    if (System.currentTimeMillis() >= deadlineMs) {
      finishReadmission(false, false, future);
      return;
    }
    rest.statusAsync().whenComplete((status, ex) -> submit(() -> {
      if (ex != null) {
        RestException cause = unwrapRest(ex);
        if (cause != null && cause.kind() == RestException.Kind.IO) {
          finishReadmission(false, true, future); // process unreachable → DEAD
          return;
        }
        retryReadmission(deadlineMs, future);
        return;
      }
      if (status.is2xx() && status.parsed().isPresent()) {
        StatusDto dto = status.parsed().get();
        if (dto.stopped() && dto.track() == null) {
          finishReadmission(true, false, future); // 200 idle — good enough to reuse
          return;
        }
      } else if (status.is2xx()) {
        finishReadmission(false, false, future); // unparseable 200 — fail closed
        return;
      } else if (status.isNoSession()) {
        finishReadmission(false, false, future); // 204 is not an idle session
        return;
      }
      retryReadmission(deadlineMs, future);
    }));
  }

  private void retryReadmission(long deadlineMs, CompletableFuture<Result> future) {
    if (System.currentTimeMillis() < deadlineMs) {
      scheduler.schedule(() -> submit(() -> probeStatusIdle(deadlineMs, future)),
          timing.statusPollIntervalMs(), TimeUnit.MILLISECONDS);
    } else {
      finishReadmission(false, false, future);
    }
  }

  private void finishReadmission(boolean statusIdle, boolean unreachable, CompletableFuture<Result> future) {
    boolean fresh = wsFresh();
    Decision decision = decide(new DecisionInput(
        stopTainted, false, fresh, statusIdle, fifoReopenOk.getAsBoolean(),
        consecutiveFailures.intValue(), timing.quarantineThreshold(), unreachable));
    log("re-admission check on backend '" + handle.getBackendId()
        + "': wsFresh=" + fresh + " statusIdle=" + statusIdle
        + " fifoReopenOk=" + fifoReopenOk.getAsBoolean()
        + " consecutive=" + consecutiveFailures + " -> " + decision);
    Result result = applyDecision(decision,
        "re-admission (wsFresh=" + fresh + ", statusIdle=" + statusIdle + ")", false);
    if (future != null) {
      future.complete(result);
    }
  }

  // ------------------------------------------------------------ shutdown

  /**
   * Terminal shutdown: releases any held lease, marks DEAD, completes any
   * pending command, drains the lane and cancels the timer. Idempotent.
   */
  @Override
  public void close() {
    closed = true;
    submit(() -> {
      Lease held = lease;
      lease = null;
      if (held != null) {
        held.release();
      }
      if (state != MachineState.DEAD) {
        state = MachineState.DEAD;
        phase = Phase.IDLE;
        expectedUri = null;
      }
      completePending(Result.dead("machine closed"));
      CompletableFuture<Result> queued = queuedPause;
      queuedPause = null;
      if (queued != null) {
        queued.complete(Result.dead("machine closed"));
      }
    });
    lane.shutdown();
    try {
      lane.awaitTermination(2, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    scheduler.shutdownNow();
  }

  // ------------------------------------------------------------ helpers

  private static String uriOf(PlayerEvent event) {
    if (event.data() == null) {
      return null;
    }
    Object value = event.data().get("uri");
    return value instanceof String s ? s : null;
  }

  private static long positionOf(PlayerEvent event) {
    if (event.data() == null) {
      return -1;
    }
    Object value = event.data().get("position");
    return value instanceof Number n ? n.longValue() : -1;
  }

  private static boolean processUnreachable(RestException ex) {
    return ex != null && (ex.kind() == RestException.Kind.IO || ex.kind() == RestException.Kind.CANCELED);
  }

  private static RestException unwrapRest(Throwable error) {
    if (error == null) {
      return null;
    }
    Throwable cause = error instanceof java.util.concurrent.CompletionException ce && ce.getCause() != null
        ? ce.getCause()
        : error;
    return cause instanceof RestException re ? re : null;
  }

  private void log(String line) {
    logSink.accept(sanitizer.sanitize(line));
  }
}
