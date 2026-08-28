package dev.lavalinkplugins.golibrespot.lifecycle;

import dev.lavalinkplugins.golibrespot.backend.ws.EventsWebSocketClient;
import dev.lavalinkplugins.golibrespot.fifo.FifoOpener;
import dev.lavalinkplugins.golibrespot.logging.LogSanitizer;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.MachineState;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Outcome;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Result;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Timing;
import dev.lavalinkplugins.golibrespot.pool.ExclusivePool;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The T17 cleanup layer: logical stop, async player-destroy release and plugin
 * shutdown for one backend, on top of the T14 {@link BackendStateMachine}, the
 * T15 {@link LifecycleCoordinator}, the T9 WS client, the T11
 * {@link FifoOpener} and the T13 {@link ExclusivePool}.
 *
 * <p><b>Logical stop</b> ({@link #logicalStop()}) = remote pause + pause
 * confirmation (the machine's {@code paused} event, or a paused {@code /status}
 * fallback — both inside the machine's ack + reconcile + poll budget) +
 * generation retirement via {@code machine.retire()}. The daemon's stop
 * endpoint is NEVER issued (v0.9.0 stop-race, API_CONTRACT.md §5): a stopped
 * backend is left paused, the lease returned to READY, and the next track is a
 * fresh play. A second stop while no session is active is an idempotent no-op.</p>
 *
 * <p><b>Exactly-once release.</b> The machine owns the real lease release;
 * this class only ever drives {@code machine.retire()} (or surfaces the
 * machine's own quarantine/degrade/dead result, which already released the
 * lease). {@code Lease.release()} is idempotent, so every path returns the
 * lease to the pool exactly once — a stale second release is a no-op that can
 * never resurrect the backend.</p>
 *
 * <p><b>Serialization behind an in-flight command.</b> The machine's lane
 * rejects a command while another is pending ("command in flight"). A stop or
 * destroy that arrives while a seek/pause is mid-flight therefore waits —
 * bounded — for the machine to settle (steady phase or non-LEASED state) and
 * then retries, so a logical stop during an in-flight seek serializes instead
 * of failing or racing. The wait is a park-based idle loop ({@link LockSupport}),
 * never an arbitrary blocking sleep, and is bounded by the machine's worst-case
 * command budget (ack + reconcile + poll + slack).</p>
 *
 * <p><b>Plugin shutdown</b> ({@link #shutdown()}) follows the DECISIONS.md
 * order: cancel in-flight FIFO opens (the coordinator's open-handle cancel runs
 * the dummy-writer rendezvous — the only way to unblock a native FIFO read-end
 * open) → close the WS → drain executors with bounded joins → release the held
 * lease (the machine's close) → drain the opener and the pool. Every join is
 * bounded; no shutdown path can hang, including shutdown while a track is
 * playing or a seek is in flight. Afterwards the opener's executor has zero
 * live tasks (verified by tests).</p>
 *
 * <p><b>Threading.</b> One single-thread daemon executor serializes stop /
 * destroy operations so callers never block on remote work and no per-call
 * thread is ever created. {@code logicalStop()}/{@code destroy()} are async
 * (futures); {@code shutdown()} runs the bounded teardown on the calling
 * thread.</p>
 */
public final class StopSequence implements AutoCloseable {

  /** Slack beyond the machine's ack+reconcile+poll budgets (T16 pattern). */
  private static final long SLACK_MS = 250L;
  /** Bound for a single retire attempt (retire is instant when the lane is idle). */
  private static final long RETIRE_BUDGET_MS = 2_000L;
  /** Park granularity while idle-waiting for the machine's in-flight command. */
  private static final long IDLE_POLL_NANOS = 25_000_000L; // 25 ms
  /** Bound for draining the stop lane and the pool's grant executor. */
  private static final long DRAIN_BOUND_SECONDS = 5L;

  private final BackendStateMachine machine;
  private final LifecycleCoordinator coordinator;
  private final EventsWebSocketClient ws;
  private final FifoOpener opener;
  private final ExclusivePool pool;
  private final Timing timing;
  private final Consumer<String> logSink;
  private final LogSanitizer sanitizer = LogSanitizer.defaults();
  private final ExecutorService lane;
  private final AtomicBoolean closed = new AtomicBoolean();

  /**
   * @param machine     the T14 state machine (owns the real lease release)
   * @param coordinator the T15 coordinator for this backend (owns the FIFO open/reader)
   * @param ws          the T9 WS client (closed on shutdown, never reconnected after)
   * @param opener      the T11 FIFO opener (drained on shutdown — zero blocked threads)
   * @param pool        the T13 pool (idempotently shut down)
   * @param timing      bounded wait budgets (shared with the machine)
   * @param logSink     receives sanitized diagnostic lines (default no-op)
   */
  public StopSequence(
      BackendStateMachine machine,
      LifecycleCoordinator coordinator,
      EventsWebSocketClient ws,
      FifoOpener opener,
      ExclusivePool pool,
      Timing timing,
      Consumer<String> logSink) {
    this.machine = Objects.requireNonNull(machine, "machine");
    this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
    this.ws = Objects.requireNonNull(ws, "ws");
    this.opener = Objects.requireNonNull(opener, "opener");
    this.pool = Objects.requireNonNull(pool, "pool");
    this.timing = Objects.requireNonNull(timing, "timing");
    this.logSink = Objects.requireNonNull(logSink, "logSink");
    this.lane = Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, "golibrespot-stopseq");
      t.setDaemon(true);
      return t;
    });
  }

  // ------------------------------------------------------------ logical stop

  /**
   * Logical stop (never the daemon stop endpoint): remote pause + confirmation,
   * then generation retirement via {@code machine.retire()} — the lease returns
   * to READY without quarantine. Idempotent: with no active session this is an
   * immediate no-op OK. Serializes behind an in-flight machine command (bounded).
   */
  public CompletableFuture<Result> logicalStop() {
    return submitOp(this::logicalStopInternal);
  }

  /**
   * Async player-destroy release: returns the lease to the pool exactly once
   * without blocking the calling thread (the release runs on the stop lane).
   * Waits — bounded — for an in-flight machine command (e.g. a seek) to settle
   * before retiring; if the machine quarantined meanwhile, the lease is already
   * released and the result is OK. Idempotent.
   */
  public CompletableFuture<Result> destroy() {
    return submitOp(this::destroyInternal);
  }

  // ------------------------------------------------------------ shutdown

  /**
   * Plugin shutdown (bounded, idempotent): drains the stop lane, then tears the
   * stack down in the DECISIONS.md order — cancel in-flight FIFO opens via the
   * dummy-writer rendezvous, close the WS, drain executors with bounded joins,
   * release the held lease, and shut the pool down. Afterwards the opener's
   * executor holds zero live tasks and no shutdown path can hang.
   */
  public void shutdown() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    lane.shutdown();
    awaitTermination(lane, DRAIN_BOUND_SECONDS);
    coordinator.close(); // cancels the in-flight FIFO open (rendezvous), closes the reader
    ws.close();          // clean WS close — no reconnect after shutdown
    machine.close();     // releases the held lease, marks DEAD, drains the machine lane
    opener.close();      // cancels outstanding opens, drains opener/timer/writer executors
    pool.shutdown();     // idempotent — marks backends DEAD, drains the grant executor
  }

  /** Alias for {@link #shutdown()}. */
  @Override
  public void close() {
    shutdown();
  }

  /** True once {@link #shutdown()} has run. */
  public boolean isShutdown() {
    return closed.get();
  }

  // ------------------------------------------------------------ internals

  private Result logicalStopInternal() {
    if (closed.get()) {
      return Result.failed("stop sequence closed");
    }
    if (machine.state() != MachineState.LEASED) {
      log("logical stop: no active session (state=" + machine.state() + ") - no-op");
      return Result.ok("no active session");
    }
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(
        pauseBudgetMs() + maxCommandBudgetMs());
    while (true) {
      if (machine.state() != MachineState.LEASED) {
        return awaitCoordinatorSettled("logical stop"); // completed/quarantined meanwhile
      }
      Result pause = awaitPause();
      if (pause.isOk()) {
        return retireAfter("logical stop");
      }
      if (pause.outcome() != Outcome.FAILED) {
        return pause; // QUARANTINED / DEGRADED / DEAD — the machine released the lease
      }
      if (machine.state() != MachineState.LEASED) {
        return awaitCoordinatorSettled("logical stop"); // released while the pause was rejected
      }
      if (System.nanoTime() >= deadline) {
        return Result.failed("logical stop timed out: " + pause.reason());
      }
      idleWait(); // in-flight machine command — park until it settles
    }
  }

  private Result destroyInternal() {
    if (closed.get()) {
      return Result.failed("stop sequence closed");
    }
    if (machine.state() != MachineState.LEASED) {
      return awaitCoordinatorSettled("destroy"); // no active lease to release
    }
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxCommandBudgetMs());
    while (true) {
      if (machine.state() != MachineState.LEASED) {
        return awaitCoordinatorSettled("destroy"); // quarantine/completion released it
      }
      Result retire = awaitRetire();
      if (retire.isOk()) {
        log("lease released (destroy)");
        return awaitCoordinatorSettled("destroy");
      }
      if (retire.outcome() != Outcome.FAILED) {
        return retire; // the machine owns the release path
      }
      if (machine.state() != MachineState.LEASED) {
        return awaitCoordinatorSettled("destroy");
      }
      if (System.nanoTime() >= deadline) {
        return Result.failed(
            "destroy timed out while a machine command was in flight: " + retire.reason());
      }
      idleWait(); // in-flight machine command — park until it settles
    }
  }

  /** Retires after a successful pause; a concurrent release makes it an OK no-op. */
  private Result retireAfter(String why) {
    Result retire = awaitRetire();
    if (retire.isOk()) {
      log("retired lease after " + why);
      return awaitCoordinatorSettled(why);
    }
    if (retire.outcome() != Outcome.FAILED) {
      return retire; // the machine owns the release path
    }
    if (machine.state() != MachineState.LEASED) {
      return awaitCoordinatorSettled(why);
    }
    return retire;
  }

  /**
   * The completion contract: a stop/destroy future only completes OK once the
   * coordinator has observed the release ({@code currentLease() == null}). The
   * machine flips to READY and returns the lease to the pool BEFORE its release
   * notification settles the coordinator's bookkeeping, so completing off
   * machine/pool state alone can observe the pre-settlement window. This waits
   * — bounded, park-based — for the coordinator to settle, and fails loudly
   * instead of completing OK against an unsettled coordinator.
   */
  private Result awaitCoordinatorSettled(String why) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(settleBudgetMs());
    while (coordinator.currentLease() != null) {
      if (System.nanoTime() >= deadline) {
        return Result.failed(
            why + ": coordinator did not settle (lease still held) within " + settleBudgetMs() + "ms");
      }
      idleWait();
    }
    return Result.ok(why);
  }

  private Result awaitPause() {
    try {
      return machine.pause().get(pauseBudgetMs(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Result.failed("pause interrupted");
    } catch (Exception e) {
      return Result.failed("pause failed: " + sanitize(String.valueOf(e)));
    }
  }

  private Result awaitRetire() {
    try {
      return machine.retire().get(RETIRE_BUDGET_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Result.failed("retire interrupted");
    } catch (Exception e) {
      return Result.failed("retire failed: " + sanitize(String.valueOf(e)));
    }
  }

  /** Idle-wait for external state (the machine lane settling) — never a blocking sleep. */
  private void idleWait() {
    LockSupport.parkNanos(IDLE_POLL_NANOS);
  }

  private long pauseBudgetMs() {
    return timing.pauseAckTimeoutMs() + timing.reconcileTimeoutMs()
        + timing.statusPollIntervalMs() + SLACK_MS;
  }

  private long maxCommandBudgetMs() {
    long maxAck = Math.max(timing.activationTimeoutMs(),
        Math.max(timing.pauseAckTimeoutMs(), timing.seekAckTimeoutMs()));
    return maxAck + timing.reconcileTimeoutMs() + timing.statusPollIntervalMs() + SLACK_MS;
  }

  private long settleBudgetMs() {
    return maxCommandBudgetMs() + RETIRE_BUDGET_MS;
  }

  private CompletableFuture<Result> submitOp(Supplier<Result> op) {
    CompletableFuture<Result> future = new CompletableFuture<>();
    try {
      lane.execute(() -> {
        try {
          future.complete(op.get());
        } catch (Throwable t) {
          future.complete(Result.failed("stop operation failed: " + sanitize(String.valueOf(t))));
        }
      });
    } catch (RejectedExecutionException e) {
      future.complete(Result.failed("stop sequence closed"));
    }
    return future;
  }

  private static void awaitTermination(ExecutorService executor, long seconds) {
    try {
      executor.awaitTermination(seconds, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void log(String line) {
    logSink.accept(sanitizer.sanitize(line));
  }

  private static String sanitize(String line) {
    return LogSanitizer.defaults().sanitize(line);
  }
}
