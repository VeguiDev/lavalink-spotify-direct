package dev.lavalinkplugins.golibrespot.lifecycle;

import dev.lavalinkplugins.golibrespot.fifo.FifoOpener;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * Injectable seam for opening the daemon's FIFO read-end, decoupling the
 * coordinator from the concrete {@link FifoOpener} so pure-JVM tests can
 * script the open (the coordinator is constructed with a seam, not with a
 * concrete opener).
 *
 * <p>Production wires {@link #of(FifoOpener)} — the real T11 opener, which
 * performs a blocking read-end open on its own dedicated executor and supports
 * dummy-writer cancellation. The coordinator owns the returned handle's
 * lifecycle and always {@link OpenHandleLike#cancel()}s an in-flight open on
 * abort / close so no opener thread is ever left blocked.</p>
 */
@FunctionalInterface
public interface FifoOpenerSeam {

  /**
   * Asynchronously opens the FIFO read-end. The returned handle never blocks
   * the caller until {@link OpenHandleLike#await()} is invoked.
   *
   * @param fifoPath the FIFO path (absolute, existing)
   * @param timeout  bound after which the open self-cancels
   */
  OpenHandleLike open(Path fifoPath, Duration timeout);

  /**
   * Future-like handle for one FIFO read-end open (mirrors
   * {@link FifoOpener.OpenHandle}). Single-owner: either awaited or cancelled.
   */
  interface OpenHandleLike {
    /**
     * Blocks until the FIFO read-end is open. Bounded by the open timeout,
     * which self-cancels.
     *
     * @return the opened stream (the caller owns and must close it)
     * @throws CancellationException if the open was cancelled, timed out, or aborted
     * @throws ExecutionException if the open failed (cause is the typed error)
     * @throws InterruptedException if the calling thread is interrupted
     */
    InputStream await() throws InterruptedException, ExecutionException;

    /** Aborts the open (idempotent); {@code true} when it joined an in-flight cancellation. */
    boolean cancel();

    /** True iff the open completed (success, failure, or cancellation). */
    boolean isDone();

    /** True iff this handle was cancelled (or is being cancelled). */
    boolean isCancelled();
  }

  /** Adapts the real T11 {@link FifoOpener} to this seam. */
  static FifoOpenerSeam of(FifoOpener opener) {
    Objects.requireNonNull(opener, "opener");
    return (path, timeout) -> {
      FifoOpener.OpenHandle inner = opener.open(path, timeout);
      return new OpenHandleLike() {
        @Override
        public InputStream await() throws InterruptedException, ExecutionException {
          return inner.await();
        }

        @Override
        public boolean cancel() {
          return inner.cancel();
        }

        @Override
        public boolean isDone() {
          return inner.isDone();
        }

        @Override
        public boolean isCancelled() {
          return inner.isCancelled();
        }
      };
    };
  }
}
