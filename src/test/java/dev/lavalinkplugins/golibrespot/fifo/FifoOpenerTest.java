package dev.lavalinkplugins.golibrespot.fifo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

/**
 * Cancellable FIFO reader open (T11). Linux-only: FIFOs need mkfifo and the
 * daemon pipe backend is Linux-only. On Windows the whole suite is skipped by
 * {@code @EnabledOnOs(OS.LINUX)} plus the {@code requireMkfifo()} assumption —
 * the build stays green and CI (ubuntu) executes these tests.
 *
 * <p>Central property under test: a read-end open blocked in native code with
 * NO writer is unblocked by the dummy-writer rendezvous
 * ({@link DummyWriterCancellation}) — the two blocking opens complete each
 * other — never by interrupts (which pure Java cannot deliver to a blocked
 * native open).
 */
@EnabledOnOs(OS.LINUX)
class FifoOpenerTest {

  private static final Duration LONG_TIMEOUT = Duration.ofSeconds(30);

  /** Writer-side helper: opens the FIFO write-end (blocks until a reader). */
  private static Thread openWriter(Path fifo, byte[] payload, AtomicReference<Throwable> failure) {
    Thread t = new Thread(() -> {
      try (FileOutputStream writer = new FileOutputStream(fifo.toFile())) {
        if (payload != null) {
          writer.write(payload);
          writer.flush();
        }
      } catch (Exception e) {
        failure.set(e);
      }
    });
    t.setDaemon(true);
    t.start();
    return t;
  }

  /**
   * Reads exactly {@code length} bytes without {@link InputStream#readNBytes(int)}.
   * {@link FileInputStream}'s optimized implementation queries the descriptor
   * position, which fails with {@code ESPIPE} ("Illegal seek") for a FIFO.
   */
  private static byte[] readExactly(InputStream in, int length) throws IOException {
    byte[] bytes = new byte[length];
    int offset = 0;
    while (offset < length) {
      int read = in.read(bytes, offset, length - offset);
      if (read == -1) {
        break;
      }
      offset += read;
    }
    if (offset == length) {
      return bytes;
    }
    return java.util.Arrays.copyOf(bytes, offset);
  }

  /** Inspectable single-thread pool (delegated executor cannot be inspected). */
  private static ThreadPoolExecutor singleThreadPool(String name) {
    ThreadFactory factory = runnable -> {
      Thread t = new Thread(runnable, name);
      t.setDaemon(true);
      return t;
    };
    return new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), factory);
  }

  /**
   * Fixture with injectable executors so the tests can prove drain: after
   * {@link FifoOpener#close()} the opener thread is terminated, zero live
   * tasks remain and every submitted task completed. Assertion mechanism:
   * {@link ThreadPoolExecutor} stats — {@code isTerminated()}, {@code
   * getPoolSize()==0} (worker thread exited), {@code getActiveCount()==0}
   * (nothing executing), {@code completedTaskCount==taskCount} (queue drained).
   */
  private static final class OpenerFixture implements AutoCloseable {
    final ThreadPoolExecutor openPool;
    final ThreadPoolExecutor writerPool;
    final FifoOpener opener;

    OpenerFixture() {
      openPool = singleThreadPool("test-fifo-open");
      writerPool = singleThreadPool("test-fifo-dummy-writer");
      opener = new FifoOpener(openPool, new DummyWriterCancellation(writerPool));
    }

    @Override
    public void close() {
      opener.close();
      assertThat(openPool.isTerminated()).as("open executor terminated").isTrue();
      assertThat(openPool.getPoolSize()).as("opener thread exited").isZero();
      assertThat(openPool.getActiveCount()).as("zero live opener tasks").isZero();
      assertThat(openPool.getCompletedTaskCount())
          .as("all submitted open tasks completed")
          .isEqualTo(openPool.getTaskCount());
      assertThat(writerPool.isTerminated()).as("writer executor terminated").isTrue();
      assertThat(writerPool.getPoolSize()).as("writer thread exited").isZero();
    }
  }

  @Test
  void openSucceedsWhenWriterIsAlreadyPresent() throws Exception {
    FifoTestUtil.requireMkfifo();
    Path fifo = FifoTestUtil.createTempFifo();
    try (OpenerFixture fixture = new OpenerFixture()) {
      byte[] payload = {1, 2, 3, 4};
      AtomicReference<Throwable> writerFailure = new AtomicReference<>();
      Thread writer = openWriter(fifo, payload, writerFailure); // blocks until reader
      FifoOpener.OpenHandle handle = fixture.opener.open(fifo, Duration.ofSeconds(5));
      try (InputStream in = handle.await()) {
        assertThat(in).isNotNull();
        // the stream is usable end to end: bytes written by the writer arrive
        assertThat(readExactly(in, payload.length)).isEqualTo(payload);
      }
      writer.join(TimeUnit.SECONDS.toMillis(5));
      assertThat(writer.isAlive()).isFalse();
      assertThat(writerFailure).hasValue(null);
    } finally {
      FifoTestUtil.deleteTempFifo(fifo);
    }
  }

  @Test
  void openSucceedsWhenWriterArrivesConcurrently() throws Exception {
    FifoTestUtil.requireMkfifo();
    Path fifo = FifoTestUtil.createTempFifo();
    try (OpenerFixture fixture = new OpenerFixture()) {
      FifoOpener.OpenHandle handle = fixture.opener.open(fifo, LONG_TIMEOUT); // reader blocks first
      byte[] payload = {9, 8, 7, 6};
      AtomicReference<Throwable> writerFailure = new AtomicReference<>();
      Thread writer = openWriter(fifo, payload, writerFailure);
      try (InputStream in = handle.await()) {
        assertThat(readExactly(in, payload.length)).isEqualTo(payload);
      }
      writer.join(TimeUnit.SECONDS.toMillis(5));
      assertThat(writer.isAlive()).isFalse();
      assertThat(writerFailure).hasValue(null);
    } finally {
      FifoTestUtil.deleteTempFifo(fifo);
    }
  }

  @Test
  void cancelUnblocksReaderWithNoWriterWithinBound() throws Exception {
    FifoTestUtil.requireMkfifo();
    Path fifo = FifoTestUtil.createTempFifo();
    try (OpenerFixture fixture = new OpenerFixture()) {
      FifoOpener.OpenHandle handle = fixture.opener.open(fifo, LONG_TIMEOUT);
      long startNanos = System.nanoTime();
      assertThat(handle.cancel())
          .as("dummy-writer rendezvous must unblock the reader")
          .isTrue();
      long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
      assertThat(elapsedMillis)
          .as("cancellation must unblock well inside the 30s open timeout")
          .isLessThan(5_000L);
      assertThat(handle.isDone()).isTrue();
      assertThatThrownBy(handle::await).isInstanceOf(CancellationException.class);
      // the opener thread exited: zero live tasks on the dedicated executor
      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(() -> assertThat(fixture.openPool.getActiveCount()).isZero());
    } finally {
      FifoTestUtil.deleteTempFifo(fifo);
    }
  }

  @Test
  void missingFifoPathFailsWithTypedError() throws Exception {
    FifoTestUtil.requireMkfifo();
    Path missing = Path.of(
        System.getProperty("java.io.tmpdir"),
        "golibrespot-no-such-fifo-" + UUID.randomUUID());
    try (FifoOpener opener = FifoOpener.create()) {
      FifoOpener.OpenHandle handle = opener.open(missing, Duration.ofSeconds(5));
      // typed failure: the open fails fast and is surfaced as ExecutionException
      assertThatThrownBy(handle::await)
          .isInstanceOf(ExecutionException.class)
          .hasCauseInstanceOf(FileNotFoundException.class);
      assertThat(handle.isDone()).isTrue();
      // cancelling a completed handle is a no-op
      assertThat(handle.cancel()).isFalse();
    } finally {
      Files.deleteIfExists(missing);
    }
  }

  @Test
  void repeatedCancelOpenCyclesAreSafe() throws Exception {
    FifoTestUtil.requireMkfifo();
    Path fifo = FifoTestUtil.createTempFifo();
    try (OpenerFixture fixture = new OpenerFixture()) {
      for (int i = 0; i < 3; i++) {
        FifoOpener.OpenHandle handle = fixture.opener.open(fifo, LONG_TIMEOUT);
        assertThat(handle.cancel()).as("cycle %d", i).isTrue();
        // idempotent: once cancellation completed, a second call is a no-op
        assertThat(handle.cancel()).isFalse();
        assertThat(handle.isDone()).isTrue();
        assertThatThrownBy(handle::await).isInstanceOf(CancellationException.class);
      }
      // after cancellation cycles the opener still works normally
      byte[] payload = {0, 0, 1, 0};
      AtomicReference<Throwable> writerFailure = new AtomicReference<>();
      Thread writer = openWriter(fifo, payload, writerFailure);
      FifoOpener.OpenHandle handle = fixture.opener.open(fifo, Duration.ofSeconds(5));
      try (InputStream in = handle.await()) {
        assertThat(readExactly(in, payload.length)).isEqualTo(payload);
      }
      writer.join(TimeUnit.SECONDS.toMillis(5));
      assertThat(writer.isAlive()).isFalse();
      assertThat(writerFailure).hasValue(null);
    } finally {
      FifoTestUtil.deleteTempFifo(fifo);
    }
  }

  @Test
  void openWithoutWriterTimesOutAndSelfCancels() throws Exception {
    FifoTestUtil.requireMkfifo();
    Path fifo = FifoTestUtil.createTempFifo();
    try (OpenerFixture fixture = new OpenerFixture()) {
      long startNanos = System.nanoTime();
      FifoOpener.OpenHandle handle = fixture.opener.open(fifo, Duration.ofSeconds(1));
      assertThatThrownBy(handle::await).isInstanceOf(CancellationException.class);
      long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
      assertThat(elapsedMillis)
          .as("open must unblock deterministically on timeout (1s + rendezvous)")
          .isGreaterThanOrEqualTo(800L)
          .isLessThan(10_000L);
      assertThat(handle.isDone()).isTrue();
    } finally {
      FifoTestUtil.deleteTempFifo(fifo);
    }
  }

  @Test
  void closeDrainsExecutorWithBlockedOpenInFlight() throws Exception {
    FifoTestUtil.requireMkfifo();
    Path fifo = FifoTestUtil.createTempFifo();
    ThreadPoolExecutor openPool = singleThreadPool("test-fifo-open");
    ThreadPoolExecutor writerPool = singleThreadPool("test-fifo-dummy-writer");
    FifoOpener opener = new FifoOpener(openPool, new DummyWriterCancellation(writerPool));
    try {
      // a reader blocked with no writer is in flight when close() is invoked
      FifoOpener.OpenHandle inFlight = opener.open(fifo, Duration.ofSeconds(300));
      opener.close();
      // shutdown unblocked the in-flight open via the rendezvous
      assertThat(inFlight.isDone()).isTrue();
      assertThatThrownBy(inFlight::await).isInstanceOf(CancellationException.class);
      // executor drained: terminated, zero live tasks, opener thread exited
      assertThat(openPool.isTerminated()).isTrue();
      assertThat(openPool.getPoolSize()).as("opener thread exited").isZero();
      assertThat(openPool.getActiveCount()).as("zero live opener tasks").isZero();
      assertThat(openPool.getCompletedTaskCount())
          .as("all submitted open tasks completed")
          .isEqualTo(openPool.getTaskCount());
      assertThat(writerPool.isTerminated()).isTrue();
      assertThat(writerPool.getPoolSize()).as("writer thread exited").isZero();
      // opening after close is rejected
      assertThatThrownBy(() -> opener.open(fifo, Duration.ofSeconds(5)))
          .isInstanceOf(IllegalStateException.class);
    } finally {
      FifoTestUtil.deleteTempFifo(fifo);
    }
  }
}
