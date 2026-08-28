package dev.lavalinkplugins.golibrespot.fifo;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Opens FIFO read-ends on a dedicated single-thread executor so no caller or
 * state-machine thread ever blocks in the native open, with pure-Java
 * cancellation via {@link DummyWriterCancellation}.
 *
 * <p>A read-end open ({@code new FileInputStream(fifo)}) blocks until a writer
 * opens the FIFO (daemon contract: with {@code wait_for_reader=true} the daemon
 * blocks its own write-end open until this reader is present — see
 * docs/API_CONTRACT.md). The block happens in native code and cannot be
 * interrupted. Cancellation, timeout and shutdown therefore all drive the
 * <em>dummy-writer rendezvous</em>: a temporary write-end open completes the
 * reader's open, the dummy fd closes, and the opener thread exits. The handle
 * then completes cancelled. The FIFO path is never unlinked.
 *
 * <p>Threading: one single-thread executor runs the blocking read-end opens
 * (one opener thread per backend — fully drained after {@link #close()}), one
 * single-thread scheduler fires the per-open timeouts, and the writer side
 * runs on its own executor owned by the {@link DummyWriterCancellation}. All
 * threads are daemons, so a pathological stuck native open can never prevent
 * JVM shutdown.
 *
 * <p>One instance per backend; never share an opener across backends.
 */
public final class FifoOpener implements AutoCloseable {

  private static final long CANCEL_GRACE_MILLIS = 2_000L;
  private static final long TERMINATION_BOUND_SECONDS = 5L;

  private final ExecutorService openExecutor;
  private final ScheduledExecutorService timerExecutor;
  private final DummyWriterCancellation dummyWriter;
  private final Set<OpenHandle> outstanding = ConcurrentHashMap.newKeySet();
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /** Creates an opener with its own daemon threads. */
  public static FifoOpener create() {
    return new FifoOpener(
        Executors.newSingleThreadExecutor(DummyWriterCancellation.daemonThreadFactory("golibrespot-fifo-open")),
        new DummyWriterCancellation(),
        Executors.newSingleThreadScheduledExecutor(
            DummyWriterCancellation.daemonThreadFactory("golibrespot-fifo-timer")));
  }

  /** Package-private: allows tests to inject an inspectable open executor. */
  FifoOpener(ExecutorService openExecutor, DummyWriterCancellation dummyWriter) {
    this(
        openExecutor,
        dummyWriter,
        Executors.newSingleThreadScheduledExecutor(
            DummyWriterCancellation.daemonThreadFactory("golibrespot-fifo-timer")));
  }

  private FifoOpener(
      ExecutorService openExecutor,
      DummyWriterCancellation dummyWriter,
      ScheduledExecutorService timerExecutor) {
    this.openExecutor = Objects.requireNonNull(openExecutor, "openExecutor");
    this.dummyWriter = Objects.requireNonNull(dummyWriter, "dummyWriter");
    this.timerExecutor = Objects.requireNonNull(timerExecutor, "timerExecutor");
  }

  /**
   * Asynchronously opens the FIFO read-end on the dedicated opener executor.
   * The returned handle never blocks the caller until {@link
   * OpenHandle#await()} is invoked.
   *
   * <p>If the open has not succeeded within {@code timeout} the handle
   * self-cancels via the dummy-writer rendezvous, so an open that never finds
   * a writer (or is never activated) still unblocks deterministically.
   *
   * @param fifoPath the FIFO path (absolute, existing — never unlinked here)
   * @param timeout  bound after which the open self-cancels; also bounds the
   *     rendezvous of a later {@link OpenHandle#cancel()}
   * @throws IllegalStateException if this opener is already closed
   */
  public OpenHandle open(Path fifoPath, Duration timeout) {
    Objects.requireNonNull(fifoPath, "fifoPath");
    Objects.requireNonNull(timeout, "timeout");
    if (timeout.isZero() || timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must be positive: " + timeout);
    }
    if (closed.get()) {
      throw new IllegalStateException("FifoOpener is closed");
    }
    OpenHandle handle = new OpenHandle(fifoPath, timeout);
    outstanding.add(handle);
    try {
      openExecutor.execute(handle::openReader);
    } catch (RejectedExecutionException e) {
      outstanding.remove(handle);
      throw new IllegalStateException("FifoOpener is closed", e);
    }
    try {
      timerExecutor.schedule(handle::selfCancel, timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException e) {
      // close() raced in between: the reader task may be running, so abort it
      // (bounded by the rendezvous timeout) instead of leaving it blocked.
      handle.cancel();
      outstanding.remove(handle);
      throw new IllegalStateException("FifoOpener is closed", e);
    }
    return handle;
  }

  /**
   * Aborts every outstanding open via the dummy-writer rendezvous and drains
   * all executors: after this returns, the opener thread is terminated with
   * zero live tasks (verified by tests via injected executors).
   */
  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    for (OpenHandle handle : outstanding) {
      handle.cancel();
    }
    openExecutor.shutdownNow();
    timerExecutor.shutdownNow();
    dummyWriter.close();
    awaitTermination(openExecutor);
    awaitTermination(timerExecutor);
  }

  private static void awaitTermination(ExecutorService executor) {
    try {
      executor.awaitTermination(TERMINATION_BOUND_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Runs the dummy-writer rendezvous; see {@link DummyWriterCancellation}. */
  private boolean rendezvous(Path fifoPath, Duration timeout) throws InterruptedException {
    return dummyWriter.openWriter(fifoPath, timeout);
  }

  /**
   * Future-like handle for one FIFO read-end open.
   *
   * <p>A handle is either awaited ({@link #await()}) or aborted
   * ({@link #cancel()}) by its owner; it is not designed for concurrent use
   * from multiple threads. Awaiting past the configured open timeout may race
   * with the automatic self-cancellation, which always wins: the caller then
   * receives {@link CancellationException}, never a closed stream.
   */
  public final class OpenHandle {

    private final Path fifoPath;
    private final Duration timeout;
    private final CompletableFuture<InputStream> openFuture = new CompletableFuture<>();
    private final AtomicBoolean cancelStarted = new AtomicBoolean(false);
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    private OpenHandle(Path fifoPath, Duration timeout) {
      this.fifoPath = fifoPath;
      this.timeout = timeout;
    }

    /** Runs on the dedicated opener executor: the blocking read-end open. */
    private void openReader() {
      try {
        InputStream in = new FileInputStream(fifoPath.toFile());
        openFuture.complete(in);
      } catch (Throwable t) {
        openFuture.completeExceptionally(t);
      } finally {
        outstanding.remove(this);
      }
    }

    /** Scheduled at the open timeout: self-cancel if the open never completed. */
    private void selfCancel() {
      cancel();
    }

    /**
     * Blocks until the FIFO read-end is open. The open runs on the dedicated
     * opener executor; cancellation unblocks it via the rendezvous, so this
     * call always terminates once the handle is cancelled, timed out, or
     * closed.
     *
     * @return the opened stream (the caller owns and must close it)
     * @throws CancellationException if the open was cancelled, timed out, or
     *     aborted by {@link FifoOpener#close()}
     * @throws ExecutionException if the open failed (cause is the typed error,
     *     e.g. {@link java.io.FileNotFoundException} for a missing path)
     * @throws InterruptedException if the calling thread is interrupted
     */
    public InputStream await() throws InterruptedException, ExecutionException {
      InputStream in = openFuture.get();
      if (cancelled.get() || openFuture.isCancelled()) {
        try {
          in.close(); // best-effort: the aborted fd must not leak
        } catch (IOException ignored) {
          // fd already closed or unusable — nothing more to do
        }
        throw new CancellationException("FIFO open cancelled: " + fifoPath);
      }
      return in;
    }

    /**
     * Aborts the open via the dummy-writer rendezvous: a temporary write-end
     * open completes the blocked read-end open, the reader fd is closed, and
     * the handle completes cancelled. Idempotent — concurrent or repeated
     * invocations join the in-progress cancellation. Bounded by the handle's
     * open timeout.
     *
     * @return true if this invocation performed or joined a cancellation of an
     *     in-flight open; false if the open had already completed
     */
    public boolean cancel() {
      if (openFuture.isDone()) {
        return false;
      }
      if (!cancelStarted.compareAndSet(false, true)) {
        return true; // another cancellation is already in progress
      }
      // Order matters: await() observes this flag after the future completes,
      // and the future can only complete after the rendezvous below — so a
      // caller blocked in await() deterministically sees the cancellation.
      cancelled.set(true);
      boolean rendezvoused = false;
      try {
        rendezvoused = rendezvous(fifoPath, timeout);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      if (rendezvoused) {
        try {
          InputStream in = openFuture.get(CANCEL_GRACE_MILLIS, TimeUnit.MILLISECONDS);
          try {
            in.close(); // aborted — the caller never sees this fd
          } catch (IOException ignored) {
            // fd already closed — nothing more to do
          }
        } catch (ExecutionException ignored) {
          // the reader open itself failed (e.g. the path vanished) — nothing to close
        } catch (TimeoutException | CancellationException e) {
          // pathological: the reader task did not deliver within the grace bound
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      openFuture.cancel(false); // mark the handle cancelled for await()
      return rendezvoused;
    }

    /** True iff the open completed (success, failure, or cancellation). */
    public boolean isDone() {
      return openFuture.isDone();
    }

    /** True iff this handle was cancelled (or is being cancelled). */
    public boolean isCancelled() {
      return cancelled.get();
    }
  }
}
