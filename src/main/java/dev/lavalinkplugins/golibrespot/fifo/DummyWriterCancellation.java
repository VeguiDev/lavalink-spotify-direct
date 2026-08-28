package dev.lavalinkplugins.golibrespot.fifo;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cancellation mechanism for blocking FIFO read-end opens: the
 * <em>dummy-writer rendezvous</em>.
 *
 * <p>A {@code new FileInputStream(fifoPath)} blocks in native code until a
 * writer opens the FIFO, and {@link Thread#interrupt()} cannot unblock it —
 * pure Java has no way to abort such an open. This class instead opens the
 * FIFO <em>write-end</em> ({@code new FileOutputStream(fifoPath)}) on its own
 * dedicated thread: the two blocking opens rendezvous — the write-end open
 * succeeds because a reader is present, which in turn completes the reader's
 * open — then the write-end fd is closed immediately. The FIFO path is never
 * unlinked.
 *
 * <p>The write-end open is bounded by the same timeout as the read-end open:
 * {@link #openWriter} waits up to {@code timeout} for the rendezvous and
 * reports whether it happened. Because the caller (see {@link FifoOpener})
 * only invokes it while a reader open is in flight on a live executor, the
 * rendezvous completes in microseconds in every non-pathological flow.
 */
public final class DummyWriterCancellation implements AutoCloseable {

  private static final long TERMINATION_BOUND_SECONDS = 5L;

  private final ExecutorService writerExecutor;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /** Creates a cancellation helper with its own dedicated daemon writer thread. */
  public DummyWriterCancellation() {
    this(Executors.newSingleThreadExecutor(daemonThreadFactory("golibrespot-fifo-dummy-writer")));
  }

  /** Package-private: allows tests to inject an inspectable executor. */
  DummyWriterCancellation(ExecutorService writerExecutor) {
    this.writerExecutor = Objects.requireNonNull(writerExecutor, "writerExecutor");
  }

  /**
   * Opens the FIFO write-end on the dedicated writer thread and blocks the
   * calling thread up to {@code timeout} for the rendezvous.
   *
   * <p>The write-end open blocks until a reader is present; a reader blocked
   * in its own open counts as present, so both opens complete each other. The
   * writer fd is closed immediately after the open succeeds.
   *
   * @param fifoPath the FIFO path (never unlinked by this class)
   * @param timeout  bound for the rendezvous wait
   * @return true iff the write-end opened within the timeout (rendezvous
   *     achieved and the reader's open has completed)
   * @throws InterruptedException if the calling thread is interrupted while waiting
   */
  public boolean openWriter(Path fifoPath, Duration timeout) throws InterruptedException {
    Objects.requireNonNull(fifoPath, "fifoPath");
    Objects.requireNonNull(timeout, "timeout");
    if (closed.get()) {
      return false;
    }
    CountDownLatch opened = new CountDownLatch(1);
    AtomicReference<IOException> failure = new AtomicReference<>();
    try {
      writerExecutor.execute(() -> {
        try (FileOutputStream out = new FileOutputStream(fifoPath.toFile())) {
          // Blocking O_WRONLY open succeeded: a reader is present, so the
          // reader's open has completed too. Close the dummy fd at once.
          opened.countDown();
        } catch (IOException e) {
          failure.set(e);
          opened.countDown();
        }
      });
    } catch (RejectedExecutionException e) {
      return false; // executor already shut down — nothing to rendezvous with
    }
    boolean signalled = opened.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    return signalled && failure.get() == null;
  }

  /** Shuts down the writer executor and awaits its termination (bounded). */
  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    writerExecutor.shutdownNow();
    try {
      writerExecutor.awaitTermination(TERMINATION_BOUND_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Package-private: shared by the fifo package for named daemon threads. */
  static ThreadFactory daemonThreadFactory(String name) {
    return runnable -> {
      Thread t = new Thread(runnable, name);
      t.setDaemon(true);
      return t;
    };
  }
}
