package dev.lavalinkplugins.golibrespot.lifecycle;

import dev.lavalinkplugins.golibrespot.fifo.FifoReader;
import dev.lavalinkplugins.golibrespot.fifo.PcmDecoder;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.MachineState;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Outcome;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Result;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Timing;
import dev.lavalinkplugins.golibrespot.logging.LogSanitizer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * The seek handshake (T16): the strict, fully bounded sequence that makes a
 * Lavaplayer seek safe on the go-librespot pipe backend. Consumed by T17/T18 —
 * constructed with the coordinator's {@link FifoReader} + {@link PcmDecoder}
 * and invoked from the Lavaplayer seek executor (which runs on the playback
 * thread, so every step is bounded — there is no arbitrary blocking sleep and
 * no unbounded wait anywhere in this class).
 *
 * <p><b>Strict order</b> (each step awaited before the next):</p>
 * <ol>
 *   <li>remote {@link BackendStateMachine#pause()} — the machine issues
 *       {@code POST /player/pause} and awaits the matching {@code paused} event
 *       (falling back to a paused {@code /status} probe on timeout);</li>
 *   <li>bounded wait for that confirmation (the machine's {@code pauseAck}
 *       budget);</li>
 *   <li>drain + discard the pre-seek FIFO bytes under the byte/time caps
 *       (5 s / 4 MiB by default) via {@link PcmDecoder#setDiscardMode} — the
 *       daemon's {@code Drop()} is a no-op for the pipe (docs/API_CONTRACT.md
 *       §6), so pre-seek kernel bytes WILL arrive after the seek; they must be
 *       consumed and dropped here, never delivered to the pipeline post-seek.
 *       The drain consumes from the reader's bounded queue (never the FIFO read
 *       itself — the always-draining reader keeps the daemon's write end from
 *       backing up) and stops when the queue AND the pipe are stable-empty for a
 *       full quiet window, or when a cap is reached;</li>
 *   <li>clear the partial-frame remainder at the seek boundary
 *       ({@link PcmDecoder#reset()});</li>
 *   <li>absolute {@link BackendStateMachine#seek} — the machine POSTs
 *       {@code /player/seek} with {@code relative:false} and awaits the
 *       matching {@code seek} event (or the /status position on timeout); the
 *       position is <b>daemon-authoritative</b>, never PCM-derived;</li>
 *   <li>bounded wait for that seek ack (the machine's {@code seekAck}
 *       budget);</li>
 *   <li>signal the pipeline that the seek was performed (the injected
 *       {@code seekPerformed} hook — the T18 pipeline clear / Lavaplayer
 *       position bookkeeping);</li>
 *   <li>resume <b>iff</b> the desired player state is playing
 *       ({@link BackendStateMachine#resume()}); a seek while paused stays
 *       paused.</li>
 * </ol>
 *
 * <p><b>Never a bare seek.</b> The seek command is issued only after the pause
 * confirmation and the drain barrier have both completed; a failure at any step
 * aborts the track before the seek is ever issued.</p>
 *
 * <p><b>Abort + quarantine.</b> Any failure at any step aborts the track. The
 * machine owns quarantine decisions — when a step returns a non-OK result the
 * machine has already transitioned (or we nudge it for a plain {@code FAILED}),
 * so this class never double-quarantines: it surfaces the machine's result as
 * its own. Exceptions: {@code seek()} called with no active session returns
 * {@code FAILED} without quarantining (a programming error, not a backend
 * failure), and an interrupted drain aborts the track (the interrupt flag is
 * restored and the machine is nudged so the backend is not left half-paused).</p>
 *
 * <p><b>Serialization.</b> Concurrent {@link #seek} invocations are serialized
 * by an internal lock (two rapid seeks: the second waits for the first instead
 * of colliding on the machine's single in-flight command slot).</p>
 *
 * <p>No new runtime dependencies; 2-space indent (lifecycle package).</p>
 */
public final class SeekHandshake {

  /** Slack on top of the machine's own command budget when awaiting its futures. */
  private static final long AWAIT_SLACK_MS = 250L;

  /** Bound for awaiting our own quarantine nudge on a FAILED machine step. */
  private static final long QUARANTINE_AWAIT_MS = 3_000L;

  /** How the drain phase ended. Caps are a soft bound — the seek still proceeds. */
  public enum DrainOutcome {
    /** The reader queue AND the underlying pipe are drained (no new data for a full quiet window). */
    STABLE_EMPTY,
    /** The byte cap was reached; the drain stopped at the bound. */
    BYTE_CAP,
    /** The time cap elapsed; the drain stopped at the bound. */
    TIME_CAP
  }

  /** Bounded drain budgets (DECISIONS.md: 5 s time cap, 4 MiB byte cap). */
  public record Drain(long timeoutMs, long byteCap, long quietWindowMs) {
    public Drain {
      if (timeoutMs <= 0) {
        throw new IllegalArgumentException("timeoutMs must be positive: " + timeoutMs);
      }
      if (byteCap <= 0) {
        throw new IllegalArgumentException("byteCap must be positive: " + byteCap);
      }
      if (quietWindowMs <= 0) {
        throw new IllegalArgumentException("quietWindowMs must be positive: " + quietWindowMs);
      }
    }

    /** Defaults mirror DECISIONS.md: 5 s time cap, 4 MiB byte cap, 80 ms quiet window. */
    public static Drain defaults() {
      return new Drain(5_000, 4 * 1024 * 1024, 80);
    }
  }

  private final BackendStateMachine machine;
  private final FifoReader reader;
  private final PcmDecoder decoder;
  private final Timing timing;
  private final Drain drain;
  private final Runnable seekPerformed;
  private final Consumer<String> logSink;
  private final LogSanitizer sanitizer = LogSanitizer.defaults();
  private final ReentrantLock lock = new ReentrantLock();

  /**
   * @param machine       the T14 machine this handshake drives (never re-implements
   *                      machine logic — commands + awaited results only)
   * @param reader        the T12 reader whose queue holds the pre-seek PCM
   *                      ({@code null} until the FIFO is open — guarded in seek)
   * @param decoder       the T12 decoder that consumes-and-drops the drain and is
   *                      reset at the seek boundary (the shared per-session decoder)
   * @param timing        the machine's bounded wait budgets (pauseAck / seekAck /
   *                      reconcile / poll — shared with the machine)
   * @param drain         the drain caps and stable-empty quiet window
   * @param seekPerformed pipeline signal fired after the seek ack and before any
   *                      resume (the T18 ghost-buffer / position clear)
   * @param logSink       receives sanitized diagnostic lines (default no-op)
   */
  public SeekHandshake(
      BackendStateMachine machine,
      FifoReader reader,
      PcmDecoder decoder,
      Timing timing,
      Drain drain,
      Runnable seekPerformed,
      Consumer<String> logSink) {
    this.machine = Objects.requireNonNull(machine, "machine");
    this.reader = reader; // nullable until the FIFO is open (guarded in seek)
    this.decoder = Objects.requireNonNull(decoder, "decoder");
    this.timing = Objects.requireNonNull(timing, "timing");
    this.drain = Objects.requireNonNull(drain, "drain");
    this.seekPerformed = Objects.requireNonNull(seekPerformed, "seekPerformed");
    this.logSink = Objects.requireNonNull(logSink, "logSink");
  }

  /**
   * Runs the seek handshake in the strict order documented on the class.
   *
   * @param positionMs   absolute daemon position to seek to (daemon-authoritative)
   * @param resumeAfter  {@code true} iff the desired player state is playing —
   *                     the handshake resumes only then; a seek while paused
   *                     stays paused
   * @return the machine outcome of the handshake; OK only when every step
   *     (pause → drain → seek ack → optional resume) succeeded. Any failure
   *     surfaces as the machine's quarantine/degrade/dead result (never
   *     double-quarantined).
   */
  public Result seek(long positionMs, boolean resumeAfter) {
    if (positionMs < 0) {
      throw new IllegalArgumentException("positionMs must be >= 0: " + positionMs);
    }
    lock.lock();
    try {
      return seekInternal(positionMs, resumeAfter);
    } finally {
      lock.unlock();
    }
  }

  private Result seekInternal(long positionMs, boolean resumeAfter) {
    if (machine.state() != MachineState.LEASED) {
      // programming error / session already gone — nothing to quarantine
      return Result.failed("no active session to seek (state=" + machine.state() + ")");
    }
    if (reader == null) {
      return abortTrack(Result.failed("no fifo reader for the seek drain"), "pre-seek");
    }

    // (1)+(2) remote pause + bounded ack (paused event, or paused /status on timeout)
    Result paused = awaitCommand(machine.pause(), pauseBudgetMs());
    if (!paused.isOk()) {
      return abortTrack(paused, "pause before seek");
    }

    // (3) drain + discard the pre-seek FIFO bytes under the byte/time caps. With
    // the daemon paused, the only pre-seek bytes still in flight are kernel/queue
    // buffered; consuming them here — and never delivering them post-seek — is the
    // drain barrier that makes the seek safe.
    DrainOutcome drained;
    try {
      drained = drain();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      return abortTrack(Result.failed("seek interrupted during the drain"), "drain");
    }
    if (drained != DrainOutcome.STABLE_EMPTY) {
      log("seek drain stopped at " + drained + " (byte cap " + drain.byteCap()
          + " bytes, time cap " + drain.timeoutMs() + " ms) — continuing with the bound");
    }

    // (4) clear the partial-frame remainder at the seek boundary
    decoder.reset();

    // (5)+(6) absolute seek + bounded ack (seek event with the requested
    // position, or /status position on timeout). The position is
    // daemon-authoritative — never PCM-derived.
    Result seeked = awaitCommand(machine.seek(positionMs), seekBudgetMs());
    if (!seeked.isOk()) {
      return abortTrack(seeked, "seek to " + positionMs);
    }

    // (7) signal the pipeline that the seek was performed (T18 clears its ghost
    // buffer / position bookkeeping here)
    try {
      seekPerformed.run();
    } catch (Throwable t) {
      log("seek-performed hook failed: " + sanitizer.sanitize(String.valueOf(t)));
    }

    // (8) resume iff the desired player state is playing
    if (resumeAfter) {
      Result resumed = awaitCommand(machine.resume(), pauseBudgetMs());
      if (!resumed.isOk()) {
        return abortTrack(resumed, "resume after seek");
      }
    }

    return Result.ok("seeked to " + positionMs);
  }

  /**
   * Drains the reader's queue and discards the pre-seek PCM under the byte/time
   * caps: consumes every queued chunk through the decoder in discard mode
   * (consume-and-drop, remainder preserved) until the queue AND the pipe are
   * stable-empty — no new data for a full quiet window, confirmed with a final
   * poll and a second quiet window to close the reader-thread delivery race —
   * or until a cap is reached. Never drains the FIFO read itself and never
   * sleeps: all waits are bounded {@link FifoReader#take(long)} polls.
   *
   * <p>A {@code Eof} event (writer closed) ends the drain as stable-empty — the
   * daemon is paused during the drain, so no writer is producing pre-seek data
   * (this is the writer-close / test path only).</p>
   *
   * <p>Package-private for the drain-cap tests; the decoder's discard mode is
   * always restored (even on interruption).</p>
   *
   * @return how the drain ended; caps are a soft termination — the seek proceeds
   * @throws InterruptedException if the calling thread is interrupted mid-drain
   */
  DrainOutcome drain() throws InterruptedException {
    decoder.setDiscardMode(true);
    try {
      long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(drain.timeoutMs());
      long bytes = 0;
      while (true) {
        if (bytes >= drain.byteCap()) {
          return DrainOutcome.BYTE_CAP;
        }
        long firstWindow = Math.min(drain.quietWindowMs(), millisUntil(deadlineNanos));
        if (firstWindow <= 0) {
          return DrainOutcome.TIME_CAP;
        }
        FifoReader.Event event = reader.take(firstWindow);
        if (event == null) {
          // one full quiet window with no data: the queue is empty AND (because
          // the reader thread never stops draining) the pipe is drained. Confirm
          // stable-empty with a final poll + one more quiet window before
          // declaring the drain complete.
          FifoReader.Event late = reader.poll();
          if (late == null) {
            long secondWindow = Math.min(drain.quietWindowMs(), millisUntil(deadlineNanos));
            if (secondWindow <= 0) {
              return DrainOutcome.TIME_CAP;
            }
            FifoReader.Event confirmed = reader.take(secondWindow);
            if (confirmed == null) {
              return DrainOutcome.STABLE_EMPTY;
            }
            if (confirmed instanceof FifoReader.Event.Eof) {
              return DrainOutcome.STABLE_EMPTY;
            }
            bytes += ((FifoReader.Event.Data) confirmed).bytes().length;
            decoder.decode(((FifoReader.Event.Data) confirmed).bytes());
            continue;
          }
          if (late instanceof FifoReader.Event.Eof) {
            return DrainOutcome.STABLE_EMPTY;
          }
          bytes += ((FifoReader.Event.Data) late).bytes().length;
          decoder.decode(((FifoReader.Event.Data) late).bytes());
          continue;
        }
        if (event instanceof FifoReader.Event.Eof) {
          return DrainOutcome.STABLE_EMPTY;
        }
        bytes += ((FifoReader.Event.Data) event).bytes().length;
        decoder.decode(((FifoReader.Event.Data) event).bytes()); // discard mode: consume-and-drop
      }
    } finally {
      decoder.setDiscardMode(false);
    }
  }

  private static long millisUntil(long deadlineNanos) {
    return TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
  }

  /**
   * Awaits one machine command within a bound that covers the machine's own
   * worst-case latency (ack timeout + reconcile + poll). On timeout / interrupt
   * / execution failure the track is aborted through {@link #abortTrack}.
   */
  private Result awaitCommand(CompletableFuture<Result> future, long budgetMs) {
    try {
      return future.get(budgetMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException te) {
      return abortTrack(Result.failed("state machine command timed out after " + budgetMs + " ms"), "await");
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      return abortTrack(Result.failed("seek interrupted awaiting the state machine"), "await");
    } catch (ExecutionException ee) {
      return abortTrack(Result.failed("state machine command failed: "
          + sanitizer.sanitize(String.valueOf(ee.getCause()))), "await");
    }
  }

  /**
   * Aborts the track on a failed handshake step. The machine owns quarantine
   * decisions: when a step already returned QUARANTINED / DEGRADED / DEAD the
   * machine has transitioned — this surfaces that result unchanged and NEVER
   * double-quarantines. Only a plain FAILED (e.g. command rejected) is nudged
   * into a transient machine quarantine so the backend is not left in a half
   * state.
   */
  private Result abortTrack(Result stepResult, String step) {
    log("seek aborted at " + step + ": " + stepResult.outcome() + " " + stepResult.reason());
    if (stepResult.outcome() != Outcome.FAILED) {
      return stepResult;
    }
    try {
      return machine.quarantine("seek aborted at " + step + ": " + stepResult.reason(), false)
          .get(QUARANTINE_AWAIT_MS, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      Thread.currentThread().interrupt();
      log("seek abort quarantine could not be applied: " + sanitizer.sanitize(String.valueOf(e)));
      return stepResult;
    }
  }

  /** The machine's pause/resume futures may fall back to a /status reconcile. */
  private long pauseBudgetMs() {
    return timing.pauseAckTimeoutMs() + timing.reconcileTimeoutMs()
        + timing.statusPollIntervalMs() + AWAIT_SLACK_MS;
  }

  private long seekBudgetMs() {
    return timing.seekAckTimeoutMs() + timing.reconcileTimeoutMs()
        + timing.statusPollIntervalMs() + AWAIT_SLACK_MS;
  }

  private void log(String line) {
    logSink.accept(sanitizer.sanitize(line));
  }
}
