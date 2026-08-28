package dev.lavalinkplugins.golibrespot.lifecycle;

import dev.lavalinkplugins.golibrespot.backend.rest.GoLibrespotRestClient;
import dev.lavalinkplugins.golibrespot.backend.ws.EventType;
import dev.lavalinkplugins.golibrespot.backend.ws.EventsWebSocketClient.EventsListener;
import dev.lavalinkplugins.golibrespot.backend.ws.PlayerEvent;
import dev.lavalinkplugins.golibrespot.fifo.FifoReader;
import dev.lavalinkplugins.golibrespot.fifo.PcmDecoder;
import dev.lavalinkplugins.golibrespot.logging.LogSanitizer;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.MachineState;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Result;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Timing;
import dev.lavalinkplugins.golibrespot.pool.BackendHandle;
import dev.lavalinkplugins.golibrespot.pool.ExclusivePool;
import dev.lavalinkplugins.golibrespot.pool.Lease;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * The Wave-3 coordination layer: drives one backend lease (from the T13 pool)
 * through activation → playback → natural completion / play-over-play
 * replacement on top of the T14 {@link BackendStateMachine}, and owns the FIFO
 * open/read lifecycle (T11 opener + T12 reader) that the machine never touches.
 * T16 (seek handshake), T17 (cleanup) and T18 (source manager / track) consume
 * this API.
 *
 * <p>One coordinator per backend (one per machine); sessions are sequential on
 * the backend's exclusive lease. Each track play is a session:
 * {@link #start(String, long)} acquires the lease (at play start, never at
 * load), and {@link #replace(String, long)} switches tracks play-over-play on
 * the already-held lease.</p>
 *
 * <p><b>Activation sequence</b> (all steps bounded; nothing runs on the
 * machine's lane): acquire lease → open the FIFO read-end (async, cancellable)
 * → issue the single {@code POST /player/play} through
 * {@link BackendStateMachine#activate} (with the daemon's
 * {@code wait_for_reader} contract the play is what triggers the writer to
 * rendezvous with the read-open) → await the open → start the {@link FifoReader}
 * → await the current-generation {@code playing} confirmation (the machine's
 * bounded activation barrier, ≤15s by default; on timeout the machine
 * quarantines and the track fails). The lease is never released-then-acquired:
 * a held lease is either replaced play-over-play or retired by the machine.</p>
 *
 * <p><b>Replacement.</b> {@link #replace} calls the machine's
 * {@code activate()} with the new URI while still LEASED — the machine bumps
 * its generation and issues exactly one play; there is no stop and no
 * release-then-acquire. A {@code not_playing} for the current expected URI that
 * arrives before {@code playing} is observed is the known v0.9.0 stale-advance
 * artifact: the coordinator re-issues the play (idempotent reload) via the raw
 * REST client instead of releasing, once per activation.</p>
 *
 * <p><b>Natural completion.</b> The machine's {@link LifecycleListener#onNaturalCompletion()}
 * (current-generation {@code not_playing} reconciled against an idle
 * {@code /status}) releases the lease through the machine; the coordinator then
 * signals end-of-stream (a waiting {@link #nextFrame} returns {@code null}) and
 * clears its bookkeeping. Completion is only acted on after activation — a
 * stale completion for an older session is ignored by the lease-identity check,
 * so same-URI replays across generations are safe.</p>
 *
 * <p><b>process()/barrier contract</b> (T18): {@code process()} blocks on
 * {@link #awaitActivated(Duration)} (bounded) and then delivers frames via
 * {@link #nextFrame(Duration)}; it never returns {@code null} while the barrier
 * is pending (a null return would prematurely end the track in Lavaplayer) —
 * {@code null} is returned only once the session has ended (completion /
 * shutdown). Activation failure surfaces as a typed {@link ActivationException}.</p>
 *
 * <p><b>Exactly-once release.</b> The machine owns the actual lease release
 * (natural completion / quarantine / retire / close invalidate it). The
 * coordinator only ever releases directly when the machine never took
 * ownership (FIFO open failed before the play was issued); every release path
 * is idempotent, so the lease is returned to the pool exactly once.</p>
 *
 * <p><b>No stop endpoint.</b> Like the machine, this coordinator never issues
 * the daemon's stop endpoint (v0.9.0 stop-race). Logical stop is T17's remote
 * pause + confirmation + machine {@code retire()}.</p>
 *
 * <p><b>Threading.</b> {@link #start}/{@link #replace} run on a per-coordinator
 * single-thread daemon executor (sessions serialize); {@link #awaitActivated} /
 * {@link #nextFrame} run on the T18 playback thread; the stale-advance observer
 * runs on the WS drain thread via {@link #listener()}; lifecycle callbacks run
 * on the machine lane. All cross-thread state is volatile / atomics.</p>
 */
public final class LifecycleCoordinator implements BackendStateMachine.LifecycleListener, AutoCloseable {

  /** Bounded-wait budgets for the coordinator's own steps. */
  public record Tuning(Duration poolAcquireTimeout, Duration fifoOpenTimeout) {
    public Tuning {
      Objects.requireNonNull(poolAcquireTimeout, "poolAcquireTimeout");
      Objects.requireNonNull(fifoOpenTimeout, "fifoOpenTimeout");
      if (poolAcquireTimeout.isNegative() || poolAcquireTimeout.isZero()) {
        throw new IllegalArgumentException("poolAcquireTimeout must be positive: " + poolAcquireTimeout);
      }
      if (fifoOpenTimeout.isNegative() || fifoOpenTimeout.isZero()) {
        throw new IllegalArgumentException("fifoOpenTimeout must be positive: " + fifoOpenTimeout);
      }
    }

    /** Defaults mirror DECISIONS.md: pool acquire 30s, FIFO open bounded by activation. */
    public static Tuning defaults() {
      return new Tuning(Duration.ofMillis(ExclusivePool.DEFAULT_ACQUIRE_TIMEOUT_MS), Duration.ofSeconds(15));
    }
  }

  private static final short[] NO_FRAMES = new short[0];
  /** Slack beyond activation+reconcile for the coordinator's own activation await. */
  private static final long ACTIVATION_GRACE_MS = 500L;
  private static final long QUARANTINE_AWAIT_MS = 3_000L;
  /**
   * Grace after observing a stale-advance {@code not_playing} before re-issuing
   * the play (idempotent reload). If the real {@code playing} confirms within
   * this window the reload is skipped — a duplicate play whose response re-emits
   * {@code not_playing} would otherwise land in the machine's PLAYING phase and
   * read as a spurious completion.
   */
  private static final long STALE_ADVANCE_REISSUE_DELAY_MS = 150L;

  private final BackendHandle handle;
  private final BackendStateMachine machine;
  private final GoLibrespotRestClient rest;
  private final ExclusivePool pool;
  private final FifoOpenerSeam opener;
  private final FifoReaderFactory readerFactory;
  private final Timing timing;
  private final Tuning tuning;
  private final Consumer<String> logSink;
  private final LogSanitizer sanitizer = LogSanitizer.defaults();
  private final ExecutorService lane;
  private final ScheduledExecutorService scheduler;
  private final Path fifoPath;
  private final AtomicBoolean closed = new AtomicBoolean();

  // ---- session state (volatile for cross-thread reads; written on the lane /
  // ---- WS thread / machine lane as documented)
  private volatile Lease lease;
  private volatile boolean machineTouched;
  private volatile ActivationBarrier barrier;
  private volatile FifoReader reader;
  private volatile FifoOpenerSeam.OpenHandleLike openHandle;
  private volatile boolean completed;
  private volatile boolean reissueFired;
  private volatile String sessionUri;
  private volatile long sessionPositionMs;
  private volatile PcmDecoder decoder = new PcmDecoder();

  /**
   * @param handle       the backend this coordinator drives (id = config name)
   * @param machine      the T14 machine for this backend (constructed with a
   *                     {@link ListenerBridge} whose target is this coordinator)
   * @param rest         the T8 REST client (also used for the stale-advance reload)
   * @param pool         the T13 pool the lease is acquired from
   * @param opener       FIFO read-end open seam (production: {@link FifoOpenerSeam#of})
   * @param readerFactory FIFO reader factory (production: {@code FifoReader::new})
   * @param timing       bounded wait budgets (shared with the machine)
   * @param tuning       coordinator-specific budgets (pool acquire / FIFO open)
   * @param logSink      receives sanitized diagnostic lines (default no-op)
   */
  public LifecycleCoordinator(
      BackendHandle handle,
      BackendStateMachine machine,
      GoLibrespotRestClient rest,
      ExclusivePool pool,
      FifoOpenerSeam opener,
      FifoReaderFactory readerFactory,
      Timing timing,
      Tuning tuning,
      Consumer<String> logSink) {
    this.handle = Objects.requireNonNull(handle, "handle");
    this.machine = Objects.requireNonNull(machine, "machine");
    this.rest = Objects.requireNonNull(rest, "rest");
    this.pool = Objects.requireNonNull(pool, "pool");
    this.opener = Objects.requireNonNull(opener, "opener");
    this.readerFactory = Objects.requireNonNull(readerFactory, "readerFactory");
    this.timing = Objects.requireNonNull(timing, "timing");
    this.tuning = Objects.requireNonNull(tuning, "tuning");
    this.logSink = Objects.requireNonNull(logSink, "logSink");
    this.fifoPath = handle.getConfig().getFifoPath();
    this.lane = Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, "golibrespot-coordinator-" + handle.getBackendId());
      t.setDaemon(true);
      return t;
    });
    this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "golibrespot-coordinator-timer-" + handle.getBackendId());
      t.setDaemon(true);
      return t;
    });
  }

  // ------------------------------------------------------------ track lifecycle

  /**
   * Starts a new track session: acquires the backend lease (bounded, fair),
   * runs the activation sequence and returns its result. Called at play start,
   * never at load. Fails when the machine is not READY (e.g. a session is
   * already active — use {@link #replace}).
   */
  public CompletableFuture<Result> start(String uri, long positionMs) {
    Objects.requireNonNull(uri, "uri");
    CompletableFuture<Result> future = new CompletableFuture<>();
    try {
      lane.execute(() -> {
        try {
          if (machine.state() != MachineState.READY || lease != null
              || (barrier != null && barrier.state() == ActivationBarrier.State.PENDING)) {
            future.complete(Result.failed("backend not ready for a new track"));
            return;
          }
          Optional<Lease> acquired = acquireMatching(tuning.poolAcquireTimeout());
          if (acquired.isEmpty()) {
            future.complete(Result.failed("no backend lease available within " + tuning.poolAcquireTimeout()));
            return;
          }
          future.complete(activateInternal(acquired.get(), uri, positionMs, false));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          future.complete(Result.failed("start interrupted"));
        } catch (Throwable t) {
          future.complete(Result.failed("start failed: " + sanitize(String.valueOf(t))));
        }
      });
    } catch (RejectedExecutionException e) {
      future.complete(Result.failed("coordinator closed"));
    }
    return future;
  }

  /**
   * Play-over-play replacement on the held lease: issues exactly one play for
   * the new URI via the machine (generation bumped internally) — never a stop,
   * never release-then-acquire. The FIFO stream is reused; the decoded-frame
   * remainder is cleared at the track boundary.
   */
  public CompletableFuture<Result> replace(String uri, long positionMs) {
    Objects.requireNonNull(uri, "uri");
    CompletableFuture<Result> future = new CompletableFuture<>();
    try {
      lane.execute(() -> {
        try {
          Lease held = lease;
          if (held == null || !machineTouched || machine.state() != MachineState.LEASED) {
            future.complete(Result.failed("no held lease; cannot replace"));
            return;
          }
          future.complete(activateInternal(held, uri, positionMs, true));
        } catch (Throwable t) {
          future.complete(Result.failed("replace failed: " + sanitize(String.valueOf(t))));
        }
      });
    } catch (RejectedExecutionException e) {
      future.complete(Result.failed("coordinator closed"));
    }
    return future;
  }

  // ------------------------------------------------------------ barrier + frames

  /**
   * Blocks on the activation barrier (bounded by {@code timeout}).
   *
   * <p><b>process()/barrier contract:</b> returns normally when the
   * current-generation {@code playing} was confirmed; throws a typed
   * {@link ActivationException} on activation failure, quarantine, degradation,
   * death or timeout. Never returns "pending" to the caller.</p>
   */
  public void awaitActivated(Duration timeout) throws ActivationException, InterruptedException {
    Objects.requireNonNull(timeout, "timeout");
    ActivationBarrier b = barrier;
    if (b == null) {
      throw new ActivationException(ActivationException.Kind.FAILED, "no activation in progress");
    }
    b.awaitActivated(timeout);
  }

  /**
   * Delivers the next decoded stereo frames for the current session.
   *
   * <p><b>Never returns {@code null} while the barrier is pending</b> (a null
   * return ends the track in Lavaplayer): when activation has not completed it
   * blocks on {@link #awaitActivated} first. {@code null} is returned only once
   * the session has ended (natural completion / shutdown) — the end-of-stream
   * signal T18's {@code process()} loop terminates on. Before frames are
   * available this may return an empty array (no data yet / transient writer
   * close), which the loop treats as "nothing this call", never as end.</p>
   */
  public short[] nextFrame(Duration timeout) throws ActivationException, InterruptedException {
    Objects.requireNonNull(timeout, "timeout");
    if (completed) {
      return null; // end-of-stream
    }
    ActivationBarrier b = barrier;
    if (b == null) {
      throw new ActivationException(ActivationException.Kind.FAILED, "no activation in progress");
    }
    b.awaitActivated(timeout); // immediate when satisfied; typed on failure; bounded otherwise
    if (completed) {
      return null; // session ended concurrently
    }
    FifoReader r = reader;
    if (r == null) {
      return null; // torn down — end-of-stream
    }
    FifoReader.Event event = r.take(timeout.toMillis());
    if (event instanceof FifoReader.Event.Data data) {
      PcmDecoder d = decoder;
      return d == null ? NO_FRAMES : d.decode(data.bytes());
    }
    return NO_FRAMES; // no data this call, or transient writer close — not terminal
  }

  // ------------------------------------------------------------ accessors

  /** The current session's expected URI, or {@code null} when idle. */
  public String expectedUri() {
    return sessionUri;
  }

  /** The current session's requested start position (used for the reload). */
  public long positionMs() {
    return sessionPositionMs;
  }

  /** The machine's current generation (lockstep with the pool lease generation). */
  public long generation() {
    return machine.generation();
  }

  /** True while this coordinator holds (or is activating) a lease. */
  public boolean isActive() {
    return lease != null;
  }

  /** The machine this coordinator drives. */
  public BackendStateMachine machine() {
    return machine;
  }

  /**
   * The events listener to wire the T9 client with: chains to the machine's
   * own listener and additionally observes {@code not_playing} for the
   * stale-advance guard. Use this (instead of {@code machine.eventsListener()})
   * when constructing the {@code EventsWebSocketClient}.
   */
  public EventsListener listener() {
    EventsListener inner = machine.eventsListener();
    return new EventsListener() {
      @Override
      public void onEvent(PlayerEvent event) {
        observe(event);
        inner.onEvent(event);
      }

      @Override
      public void onUnknownEvent(PlayerEvent event) {
        inner.onUnknownEvent(event);
      }

      @Override
      public void onQuarantine() {
        inner.onQuarantine();
      }

      @Override
      public void onConnected() {
        inner.onConnected();
      }

      @Override
      public void onDisconnected() {
        inner.onDisconnected();
      }
    };
  }

  // ------------------------------------------------------------ activation sequence

  /**
   * The activation sequence for one session. For a first activation
   * ({@code replacement=false}): open the FIFO (async, cancellable), issue the
   * single play through the machine, await the open (the play triggers the
   * daemon's writer rendezvous), start the reader. For play-over-play
   * replacement ({@code replacement=true}) the already-open FIFO stream and its
   * reader are reused — only the play is re-issued and a fresh barrier awaited.
   * On any failure the barrier fails typed and the lease is returned exactly once.
   */
  private Result activateInternal(Lease lease, String uri, long positionMs, boolean replacement) {
    this.lease = lease;
    this.machineTouched = replacement;
    this.sessionUri = uri;
    this.sessionPositionMs = positionMs;
    this.reissueFired = false;
    this.completed = false;
    this.decoder = new PcmDecoder(); // clear the partial-frame remainder at the track boundary
    long generation = machine.state() == MachineState.LEASED
        ? machine.generation() + 1 // replacement: the machine bumps internally
        : lease.generation();
    ActivationBarrier b = new ActivationBarrier(generation, uri);
    this.barrier = b;

    try {
      FifoOpenerSeam.OpenHandleLike handle = null;
      if (!replacement) {
        // submit the read-open first (async, cancellable — runs on the opener
        // thread, never on this lane) ...
        handle = opener.open(fifoPath, tuning.fifoOpenTimeout());
        this.openHandle = handle;
        machineTouched = true;
      }
      // ... then issue the play BEFORE awaiting the open: with
      // wait_for_reader=true the daemon's write-open (and therefore our
      // read-open rendezvous) fires only as a consequence of the play command.
      CompletableFuture<Result> activation = machine.activate(lease, uri, positionMs);
      if (!replacement) {
        InputStream stream = handle.await(); // bounded: rendezvous with the daemon's write-open
        this.openHandle = null;
        FifoReader r = readerFactory.create(stream);
        r.start();
        this.reader = r;
      }
      Result result = awaitActivation(activation, b);
      if (!result.isOk()) {
        teardownAfterFailure(result);
      }
      return result;
    } catch (CancellationException ce) {
      return failAfterOpen(b, "fifo open cancelled/timed out", ce);
    } catch (ExecutionException ee) {
      return failAfterOpen(b, "fifo open failed: " + ee.getCause(), ee);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      return failAfterOpen(b, "activation interrupted", ie);
    } catch (Throwable t) {
      return failAfterOpen(b, "activation error: " + t, t);
    }
  }

  /** Awaits the machine's activation result; satisfies or fails the barrier. */
  private Result awaitActivation(CompletableFuture<Result> activation, ActivationBarrier b) {
    Result result;
    try {
      result = activation.get(activationBudgetMs(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException te) {
      // belt-and-braces: the machine's own activation timeout should have
      // quarantined already; nudge it so the pending future completes.
      try {
        machine.quarantine("activation barrier timeout for '" + b.expectedUri() + "'", false)
            .get(QUARANTINE_AWAIT_MS, TimeUnit.MILLISECONDS);
      } catch (Exception ignored) {
        // the machine may be mid-transition; the second get below resolves
      }
      try {
        result = activation.get(QUARANTINE_AWAIT_MS, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e2) {
        result = Result.failed("activation timed out: " + b.expectedUri());
      } catch (Exception e) {
        result = Result.failed("activation interrupted: " + sanitize(String.valueOf(e)));
      }
    } catch (Exception e) {
      result = Result.failed("activation failed: " + sanitize(String.valueOf(e)));
    }
    if (result.isOk()) {
      b.markActivated();
      log("activated '" + b.expectedUri() + "' on backend '" + handle.getBackendId()
          + "' (generation " + b.generation() + ")");
    } else {
      b.fail(kindOf(result), result.reason());
    }
    return result;
  }

  /** Local abort (open failed / interrupted / error) — lease returned exactly once. */
  private Result failAfterOpen(ActivationBarrier b, String reason, Throwable cause) {
    cancelOpenQuietly();
    log("activation aborted on backend '" + handle.getBackendId() + "': "
        + sanitize(reason) + (cause != null ? " (" + sanitize(String.valueOf(cause)) + ")" : ""));
    if (machineTouched) {
      // the machine is mid-activation (play issued / replacement) and owns the
      // lease — quarantine to stop it
      try {
        Result r = machine.quarantine(reason, false).get(QUARANTINE_AWAIT_MS, TimeUnit.MILLISECONDS);
        b.fail(ActivationException.Kind.QUARANTINED, reason);
        clearSession();
        return r;
      } catch (Exception e) {
        Thread.currentThread().interrupt();
        b.fail(ActivationException.Kind.FAILED, reason);
        clearSession();
        return Result.failed(reason);
      }
    }
    // the machine never took ownership — return the lease directly (idempotent)
    Lease held = lease;
    lease = null;
    if (held != null) {
      held.release();
    }
    b.fail(ActivationException.Kind.CANCELED, reason);
    clearSession();
    return Result.failed(reason);
  }

  /** Activation result failure — the machine owns the lease; return it exactly once. */
  private void teardownAfterFailure(Result result) {
    closeReader();
    cancelOpenQuietly();
    if (machineTouched && machine.state() == MachineState.LEASED && lease != null) {
      // the machine rejected activation without changing state — return the lease
      try {
        machine.retire().get(QUARANTINE_AWAIT_MS, TimeUnit.MILLISECONDS);
      } catch (Exception ignored) {
        // best-effort; the pool release CAS keeps the slot consistent
      }
    }
    clearSession();
  }

  private void clearSession() {
    closeReader();
    cancelOpenQuietly();
    completed = true;
    lease = null;
    machineTouched = false;
  }

  // ------------------------------------------------------------ lifecycle listener

  /**
   * The machine released the lease (natural completion or T17 retire). Signals
   * end-of-stream and clears bookkeeping — only when the completion belongs to
   * the current session: the check {@code lease != null && !lease.isActive()}
   * ignores a stale completion from an older lease that drained late (same-URI
   * replay safety) while the current lease is still held.
   */
  @Override
  public void onNaturalCompletion() {
    Lease held = lease;
    if (held == null || held.isActive()) {
      return; // stale completion for an older session — ignore
    }
    // settle bookkeeping BEFORE the potentially slow reader close: machine
    // retire/natural-completion futures complete with the coordinator already
    // observing the release (currentLease()==null), so consumers that key off
    // the future never see the machine READY but the coordinator unsettled
    completed = true;
    lease = null;
    machineTouched = false;
    log("natural completion on backend '" + handle.getBackendId() + "' for '" + sessionUri + "'");
    ActivationBarrier b = barrier;
    if (b != null && b.state() == ActivationBarrier.State.PENDING) {
      b.fail(ActivationException.Kind.FAILED, "backend completed before activation");
    }
    closeReader();
    cancelOpenQuietly();
  }

  @Override
  public void onReAdmitted() {
    // the backend is READY again; a future start() picks it up normally
  }

  /** The machine quarantined/degraded — fail any pending barrier and stop frames. */
  @Override
  public void onQuarantined(boolean permanent) {
    if (lease == null) {
      return; // no session being managed — nothing to fail
    }
    log("backend '" + handle.getBackendId() + "' "
        + (permanent ? "degraded" : "quarantined") + " while session active");
    ActivationBarrier b = barrier;
    if (b != null) {
      b.fail(permanent ? ActivationException.Kind.DEGRADED : ActivationException.Kind.QUARANTINED,
          permanent ? "backend degraded" : "backend quarantined");
    }
    closeReader();
    cancelOpenQuietly();
    completed = true;
    lease = null;
    machineTouched = false;
  }

  @Override
  public void onDead() {
    if (lease == null) {
      return;
    }
    ActivationBarrier b = barrier;
    if (b != null) {
      b.fail(ActivationException.Kind.DEAD, "backend dead");
    }
    closeReader();
    cancelOpenQuietly();
    completed = true;
    lease = null;
    machineTouched = false;
  }

  // ------------------------------------------------------------ stale-advance guard

  /**
   * Observed on the WS drain thread (via {@link #listener()}): a
   * {@code not_playing} for the current expected URI while the activation
   * barrier is pending is the known stale-advance artifact (a buffered
   * completion drained after the play reset the daemon's track). Schedules one
   * idempotent play reload; it fires only if the real {@code playing} has not
   * confirmed within {@link #STALE_ADVANCE_REISSUE_DELAY_MS} — so a healthy
   * replacement never pays the reload, and a stuck daemon is nudged instead of
   * the track being released or quarantined.
   */
  private void observe(PlayerEvent event) {
    if (event.type() != EventType.NOT_PLAYING || reissueFired) {
      return;
    }
    ActivationBarrier b = barrier;
    if (b == null || b.isSatisfied() || b.isFailed()) {
      return;
    }
    String uri = uriOf(event);
    if (uri == null || !uri.equals(b.expectedUri())) {
      return;
    }
    reissueFired = true;
    log("stale-advance not_playing for expected URI '" + b.expectedUri()
        + "' before playing — scheduling play reload");
    scheduler.schedule(() -> {
      if (closed.get()) {
        return;
      }
      ActivationBarrier current = barrier;
      if (current == null || current.isSatisfied() || current.isFailed()) {
        return; // the real playing confirmed within the grace — nothing to reload
      }
      log("re-issuing play (idempotent reload) for '" + current.expectedUri() + "'");
      rest.playAsync(sessionUri, sessionPositionMs, false)
          .whenComplete((r, ex) -> {
            if (ex != null) {
              log("re-issued play failed: " + sanitize(String.valueOf(ex)));
            }
          });
    }, STALE_ADVANCE_REISSUE_DELAY_MS, TimeUnit.MILLISECONDS);
  }

  private static String uriOf(PlayerEvent event) {
    if (event.data() == null) {
      return null;
    }
    Object value = event.data().get("uri");
    return value instanceof String s ? s : null;
  }

  // ------------------------------------------------------------ pool

  /**
   * Acquires a lease, skipping (and immediately returning) leases for other
   * backends until this coordinator's backend is granted or the budget elapses.
   * The pool round-robins, so retries make progress.
   */
  private Optional<Lease> acquireMatching(Duration timeout) throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    String backendId = handle.getBackendId();
    while (true) {
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        return Optional.empty();
      }
      Optional<Lease> maybe = pool.acquire(Duration.ofNanos(Math.max(1L, remaining)));
      if (maybe.isEmpty()) {
        return Optional.empty(); // pool gave up (timeout or shutdown)
      }
      Lease candidate = maybe.get();
      if (candidate.backend().getBackendId().equals(backendId)) {
        return Optional.of(candidate);
      }
      candidate.release(); // not ours — hand it back; the round-robin cursor advances
    }
  }

  // ------------------------------------------------------------ shutdown / helpers

  private long activationBudgetMs() {
    return timing.activationTimeoutMs() + timing.reconcileTimeoutMs() + ACTIVATION_GRACE_MS;
  }

  private void closeReader() {
    FifoReader r = reader;
    reader = null;
    if (r != null) {
      r.close(); // idempotent
    }
  }

  private void cancelOpenQuietly() {
    FifoOpenerSeam.OpenHandleLike h = openHandle;
    openHandle = null;
    if (h != null) {
      try {
        h.cancel();
      } catch (Throwable ignored) {
        // best-effort — never let a failed cancel mask the original abort
      }
    }
  }

  /**
   * Shuts the coordinator down: fails any pending barrier, cancels an in-flight
   * FIFO open, closes the reader, returns the lease when the machine never took
   * ownership, and drains the session executor within a bound. Idempotent.
   * (T17 owns the full stop sequence; the machine itself is closed by the wiring.)
   */
  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    completed = true;
    ActivationBarrier b = barrier;
    if (b != null) {
      b.fail(ActivationException.Kind.CANCELED, "coordinator closed");
    }
    cancelOpenQuietly();
    closeReader();
    boolean touched = machineTouched;
    Lease held = lease;
    lease = null;
    machineTouched = false;
    if (held != null && !touched) {
      held.release(); // the machine never took ownership — return it to the pool
    }
    lane.shutdownNow();
    try {
      lane.awaitTermination(2, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    scheduler.shutdownNow();
  }

  private static ActivationException.Kind kindOf(Result result) {
    return switch (result.outcome()) {
      case QUARANTINED -> ActivationException.Kind.QUARANTINED;
      case DEGRADED -> ActivationException.Kind.DEGRADED;
      case DEAD -> ActivationException.Kind.DEAD;
      default -> ActivationException.Kind.FAILED;
    };
  }

  private void log(String line) {
    logSink.accept(sanitizer.sanitize(line));
  }

  private static String sanitize(String line) {
    return LogSanitizer.defaults().sanitize(line);
  }

  // ------------------------------------------------------------ wiring bridge

  /**
   * Mutable late-binding lifecycle listener for the machine: the wiring
   * constructs the machine with a bridge, then {@link #setTarget}s the
   * coordinator after construction (the machine and the coordinator need each
   * other, so the target is bound in two phases).
   */
  public static final class ListenerBridge implements BackendStateMachine.LifecycleListener {
    private volatile LifecycleCoordinator target;

    /** Binds the coordinator that receives the forwarded callbacks. */
    public void setTarget(LifecycleCoordinator coordinator) {
      this.target = coordinator;
    }

    @Override
    public void onNaturalCompletion() {
      LifecycleCoordinator t = target;
      if (t != null) {
        t.onNaturalCompletion();
      }
    }

    @Override
    public void onReAdmitted() {
      LifecycleCoordinator t = target;
      if (t != null) {
        t.onReAdmitted();
      }
    }

    @Override
    public void onQuarantined(boolean permanent) {
      LifecycleCoordinator t = target;
      if (t != null) {
        t.onQuarantined(permanent);
      }
    }

    @Override
    public void onDead() {
      LifecycleCoordinator t = target;
      if (t != null) {
        t.onDead();
      }
    }
  }

  // ------------------------------------------------------------ test accessors

  /** The lease currently held (test/diagnostic aid); {@code null} when idle. */
  Lease currentLease() {
    return lease;
  }

  /** The current session's activation barrier (test/diagnostic aid). */
  ActivationBarrier currentBarrier() {
    return barrier;
  }

  /** The current session's FIFO reader (test/diagnostic aid); {@code null} when idle. */
  FifoReader currentReader() {
    return reader;
  }
}
