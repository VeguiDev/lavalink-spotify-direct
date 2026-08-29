package dev.lavalinkplugins.golibrespot.fifo;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Always-draining reader over the daemon's s16le FIFO (docs/API_CONTRACT.md
 * §6, docs/DECISIONS.md).
 *
 * <p>A dedicated daemon thread continuously reads fixed-size chunks (default
 * 16 KiB — DECISIONS.md {@code fifoReadBufferBytes}) from the FIFO {@link
 * InputStream} and delivers them to a bounded consumer queue. When the queue
 * fills, the reader deliberately stops draining so FIFO backpressure reaches
 * the daemon's blocking write. Raw PCM is never dropped: Lavalink's real-time
 * consumer is the playback clock for the otherwise unpaced pipe backend.
 *
 * <p>EOF semantics (Linux FIFO): a blocking read returns -1 when ALL writers
 * close, and data resumes when a new writer opens. On EOF the reader records
 * an {@link Event.Eof} (deduplicated to data→EOF transitions — one per writer
 * session), KEEPS the read end open, and keeps reading. While no writer is
 * attached it idles with a short bounded park ({@code eofPollMillis},
 * {@link LockSupport#parkNanos} — the designed exception to the no-sleep rule:
 * an idle-wait for external state, not test/thread synchronization) and
 * resumes delivering within that bound when the writer reopens (play-over-play
 * replacement). The read end is never closed on EOF and the FIFO is never
 * unlinked — a missing reader makes the daemon's write-end open fail with
 * ENXIO and a full pipe wedges the daemon's Run loop.
 *
 * <p>{@link #close()} is the only termination path (plugin shutdown /
 * quarantine) and is never invoked on EOF. It closes the stream (best-effort
 * unblock of a blocked read), then joins the drain thread within a bound.
 * The drain thread is a daemon, so a leaked reader can never hang the JVM.
 */
public final class FifoReader implements AutoCloseable {

  /** Default read chunk size in bytes (DECISIONS.md: FIFO read buffer 16 KiB). */
  public static final int DEFAULT_CHUNK_SIZE = 16 * 1024;

  /** Default consumer queue capacity in chunks (64 × 16 KiB = 1 MiB). */
  public static final int DEFAULT_QUEUE_CAPACITY = 64;

  /** Default idle park (millis) while no writer is attached to the FIFO. */
  public static final long DEFAULT_EOF_POLL_MILLIS = 10L;

  private static final long CLOSE_JOIN_MILLIS = 2_000L;

  /** Event delivered to consumers: PCM bytes or end-of-stream. */
  public sealed interface Event {
    /** A chunk of PCM bytes read from the FIFO (exact length; NOT frame-aligned). */
    record Data(byte[] bytes) implements Event {}

    /**
     * The FIFO writer closed (EOF). The read end stays open and data may
     * resume when a writer reopens.
     */
    final class Eof implements Event {
      private Eof() {}

      /** Singleton instance. */
      public static final Eof INSTANCE = new Eof();
    }
  }

  private final InputStream in;
  private final int chunkSize;
  private final long eofPollNanos;
  private final ArrayBlockingQueue<Event> queue;
  private final AtomicLong eofCount = new AtomicLong();
  private final AtomicLong droppedChunks = new AtomicLong();
  private final AtomicLong bytesRead = new AtomicLong();
  private final AtomicLong backpressureWaits = new AtomicLong();
  private final AtomicInteger maxPendingChunks = new AtomicInteger();
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final Thread drainThread;

  private volatile boolean closed;
  private volatile Throwable failure;

  /** Creates a reader with the defaults (16 KiB chunks, 64-chunk queue, 10 ms EOF park). */
  public FifoReader(InputStream in) {
    this(in, DEFAULT_CHUNK_SIZE, DEFAULT_QUEUE_CAPACITY, DEFAULT_EOF_POLL_MILLIS);
  }

  /**
   * Creates a reader with explicit tuning.
   *
   * @param in             the FIFO read-end stream (owned by this reader until
   *     {@link #close()}; typically from {@link FifoOpener.OpenHandle#await()})
   * @param chunkSize      fixed read buffer size in bytes
   * @param queueCapacity  consumer queue capacity in chunks
   * @param eofPollMillis  idle park between EOF reads while no writer is attached
   */
  public FifoReader(InputStream in, int chunkSize, int queueCapacity, long eofPollMillis) {
    this.in = Objects.requireNonNull(in, "in");
    if (chunkSize <= 0) {
      throw new IllegalArgumentException("chunkSize must be > 0: " + chunkSize);
    }
    if (queueCapacity <= 0) {
      throw new IllegalArgumentException("queueCapacity must be > 0: " + queueCapacity);
    }
    if (eofPollMillis <= 0) {
      throw new IllegalArgumentException("eofPollMillis must be > 0: " + eofPollMillis);
    }
    this.chunkSize = chunkSize;
    this.eofPollNanos = TimeUnit.MILLISECONDS.toNanos(eofPollMillis);
    this.queue = new ArrayBlockingQueue<>(queueCapacity);
    this.drainThread = new Thread(this::drainLoop, "golibrespot-fifo-reader");
    this.drainThread.setDaemon(true);
  }

  /** Starts the dedicated drain thread. Must be called exactly once. */
  public void start() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("FifoReader already started");
    }
    drainThread.start();
  }

  /**
   * Blocks up to {@code timeoutMs} for the next event ({@link Event.Data} or
   * {@link Event.Eof}).
   *
   * @return the next event, or null when the timeout elapses with the queue
   *     still empty
   */
  public Event take(long timeoutMs) throws InterruptedException {
    return queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
  }

  /** Non-blocking: the next event, or null if the queue is empty. */
  public Event poll() {
    return queue.poll();
  }

  /** Number of data→EOF transitions observed (writer-close sessions). */
  public long eofCount() {
    return eofCount.get();
  }

  /** Number of undelivered events currently queued (never exceeds capacity). */
  public int pendingChunks() {
    return queue.size();
  }

  /** Legacy diagnostic retained for compatibility; lossless backpressure keeps this at zero. */
  public long droppedChunks() {
    return droppedChunks.get();
  }

  /** Total PCM bytes read from the FIFO. */
  public long bytesRead() {
    return bytesRead.get();
  }

  /** Number of bounded waits caused by a full consumer queue. */
  public long backpressureWaits() {
    return backpressureWaits.get();
  }

  /** Highest observed queue depth in chunks. */
  public int maxPendingChunks() {
    return maxPendingChunks.get();
  }

  /** True until {@link #close()} is called. */
  public boolean isRunning() {
    return !closed;
  }

  /** Fatal non-EOF read failure that terminated the drain loop, if any. */
  public Throwable failure() {
    return failure;
  }

  /**
   * The drain loop: reads fixed-size chunks, delivers them to the bounded
   * queue (blocking on overflow to preserve PCM and pace the writer), survives
   * EOF (writer close) without closing the read end, and
   * resumes when the writer reopens.
   */
  private void drainLoop() {
    byte[] buf = new byte[chunkSize];
    boolean hadData = false;
    while (!closed) {
      int n;
      try {
        n = in.read(buf);
      } catch (IOException e) {
        if (closed) {
          return; // close() raced the blocked read
        }
        failure = e;
        return;
      }
      if (n == -1) {
        // Writer closed (EOF). Record once per writer session (or the very
        // first EOF), keep the read end open, and idle-wait for a writer to
        // reopen. LockSupport.parkNanos is the designed exception to the
        // no-sleep rule: an idle-wait for external state, not synchronization.
        if (hadData || eofCount.get() == 0) {
          eofCount.incrementAndGet();
          enqueue(Event.Eof.INSTANCE);
        }
        hadData = false;
        if (!closed) {
          LockSupport.parkNanos(eofPollNanos);
        }
        continue;
      }
      if (n > 0) {
        hadData = true;
        bytesRead.addAndGet(n);
        if (!enqueue(new Event.Data(Arrays.copyOf(buf, n)))) {
          return;
        }
      }
    }
  }

  /** Lossless bounded enqueue: a slow consumer applies backpressure to the FIFO writer. */
  private boolean enqueue(Event event) {
    while (!closed) {
      try {
        if (queue.offer(event, 100, TimeUnit.MILLISECONDS)) {
          maxPendingChunks.accumulateAndGet(queue.size(), Math::max);
          return true;
        }
        backpressureWaits.incrementAndGet();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return false;
  }

  /**
   * Terminates the reader (plugin shutdown / quarantine only — NEVER called
   * on EOF): closes the stream (best-effort unblock of a blocked read), then
   * joins the drain thread within a bound. Idempotent.
   */
  @Override
  public void close() {
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
    }
    drainThread.interrupt(); // unblock a backpressured queue offer
    try {
      in.close();
    } catch (IOException ignored) {
      // fd already closed — nothing more to do
    }
    try {
      drainThread.join(CLOSE_JOIN_MILLIS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
