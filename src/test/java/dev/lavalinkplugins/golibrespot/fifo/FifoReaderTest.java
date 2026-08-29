package dev.lavalinkplugins.golibrespot.fifo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.lavalinkplugins.golibrespot.fifo.FifoReader.Event;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

/**
 * Always-draining FIFO reader (T12). Linux-only: tests drive real mkfifo
 * FIFOs, gated by {@code @EnabledOnOs(OS.LINUX)} plus {@link
 * FifoTestUtil#requireMkfifo()} — on Windows the suite is skipped and the
 * build stays green; CI (ubuntu) executes it.
 *
 * <p>Contract under test (docs/API_CONTRACT.md §6): a dedicated thread reads
 * fixed 16 KiB chunks off the FIFO into a bounded consumer queue; when the
 * writer closes (EOF) the reader records an {@link Event.Eof}, KEEPS the read
 * end open, and resumes delivering when the writer reopens (play-over-play
 * replacement). A slow consumer applies lossless backpressure through the
 * bounded queue and FIFO to the daemon writer.
 */
@EnabledOnOs(OS.LINUX)
class FifoReaderTest {

  private static final Duration BOUNDED = Duration.ofSeconds(10);

  private Path fifo;
  private FifoReader reader;
  private Rendezvous pair;

  @BeforeEach
  void setUp() throws Exception {
    FifoTestUtil.requireMkfifo();
    fifo = FifoTestUtil.createTempFifo();
    pair = openRendezvous(fifo);
    reader = new FifoReader(pair.read);
    reader.start();
  }

  @AfterEach
  void tearDown() throws Exception {
    if (reader != null) {
      reader.close();
    }
    if (pair != null) {
      pair.write.close();
    }
    FifoTestUtil.deleteTempFifo(fifo);
  }

  @Test
  void deliversBytesWrittenToFifo() throws Exception {
    byte[] expected = "s16le pcm".getBytes(StandardCharsets.US_ASCII);
    pair.write.write(expected);
    pair.write.flush();

    byte[] received = collectBytes(reader, expected.length, 5_000);

    assertThat(received).isEqualTo(expected);
    assertThat(reader.eofCount()).isZero();
    assertThat(reader.isRunning()).isTrue();
  }

  @Test
  void recordsEofAndStaysAliveWhenWriterCloses() throws Exception {
    byte[] chunk = {1, 2, 3, 4};
    pair.write.write(chunk);
    pair.write.flush();
    assertThat(collectBytes(reader, chunk.length, 5_000)).isEqualTo(chunk);

    pair.write.close(); // writer EOF

    await().atMost(BOUNDED).until(() -> reader.eofCount() >= 1);
    assertThat(reader.isRunning()).as("reader survives EOF with the read end open").isTrue();
    assertThat(reader.failure()).isNull();
  }

  @Test
  void resumesDeliveringAfterWriterReopens() throws Exception {
    byte[] first = {0, 1, 0, 2};
    pair.write.write(first);
    pair.write.flush();
    assertThat(collectBytes(reader, first.length, 5_000)).isEqualTo(first);

    pair.write.close();
    awaitAndConsumeEof(reader, 1);

    // a brand-new writer session (play-over-play replacement): the reader must
    // still be draining, so the blocking write-end open completes immediately
    try (OutputStream reopened = new FileOutputStream(fifo.toFile())) {
      byte[] second = {9, 8, 9, 7};
      reopened.write(second);
      reopened.flush();
      assertThat(collectBytes(reader, second.length, 5_000)).isEqualTo(second);
    }
    assertThat(reader.isRunning()).isTrue();
  }

  @Test
  void survivesRepeatedEofDataCycles() throws Exception {
    pair.write.write(new byte[] {1});
    pair.write.flush();
    assertThat(collectBytes(reader, 1, 5_000)).isEqualTo(new byte[] {1});
    pair.write.close();
    awaitAndConsumeEof(reader, 1);

    try (OutputStream w2 = new FileOutputStream(fifo.toFile())) {
      w2.write(new byte[] {2, 3});
      w2.flush();
      assertThat(collectBytes(reader, 2, 5_000)).isEqualTo(new byte[] {2, 3});
    }
    awaitAndConsumeEof(reader, 2);

    try (OutputStream w3 = new FileOutputStream(fifo.toFile())) {
      w3.write(new byte[] {4, 5, 6});
      w3.flush();
      assertThat(collectBytes(reader, 3, 5_000)).isEqualTo(new byte[] {4, 5, 6});
    }
    assertThat(reader.isRunning()).isTrue();
  }

  @Test
  void appliesLosslessBackpressureWhileConsumerIsSlow() throws Exception {
    int chunks = 200;
    int chunkBytes = 16 * 1024;
    int queueCapacity = FifoReader.DEFAULT_QUEUE_CAPACITY;

    CountDownLatch writerDone = new CountDownLatch(1);
    AtomicReference<Throwable> writerFailure = new AtomicReference<>();
    Thread writer = new Thread(() -> {
      try {
        byte[] buf = new byte[chunkBytes];
        for (int i = 0; i < chunks; i++) {
          Arrays.fill(buf, (byte) i);
          pair.write.write(buf); // OutputStream.write writes all bytes (blocking)
        }
        writerDone.countDown();
      } catch (Throwable t) {
        writerFailure.set(t);
        writerDone.countDown();
      }
    }, "test-storm-writer");
    writer.setDaemon(true);
    writer.start();

    // With no consumer, the bounded queue and kernel FIFO fill and pace the
    // writer instead of dropping old PCM.
    assertThat(writerDone.await(500, TimeUnit.MILLISECONDS))
        .as("writer is backpressured while the consumer is idle")
        .isFalse();
    assertThat(writerFailure.get()).isNull();
    assertThat(reader.isRunning()).isTrue();
    assertThat(reader.pendingChunks()).isLessThanOrEqualTo(queueCapacity);

    // Consume concurrently so the producer can complete, then verify every
    // byte arrives in order. FIFO reads may split writes arbitrarily.
    byte[] retained = new byte[chunks * chunkBytes];
    int offset = 0;
    while (offset < retained.length) {
      Event event = reader.take(5_000);
      assertThat(event).isInstanceOf(Event.Data.class);
      byte[] bytes = ((Event.Data) event).bytes();
      System.arraycopy(bytes, 0, retained, offset, bytes.length);
      offset += bytes.length;
    }
    assertThat(writerDone.await(5, TimeUnit.SECONDS)).isTrue();
    pair.write.close();
    awaitAndConsumeEof(reader, 1);
    assertThat(retained[0]).isEqualTo((byte) 0);
    assertThat(retained[retained.length - 1])
        .as("last byte retained in order")
        .isEqualTo((byte) (chunks - 1));
    assertThat(reader.droppedChunks()).isZero();
    assertThat(reader.backpressureWaits()).isGreaterThan(0);
    assertThat(reader.maxPendingChunks()).isEqualTo(queueCapacity);
  }

  @Test
  void deliversAcrossAnIdlePausedWriterWithoutEof() throws Exception {
    pair.write.write(new byte[] {1, 2});
    pair.write.flush();
    // first data delivered: the reader is alive and draining
    await().atMost(BOUNDED).until(() -> reader.pendingChunks() > 0);

    // "paused" daemon (API_CONTRACT.md §6): writer open but idle — the reader
    // must keep the read end open, emit no Eof, and not die
    assertThat(reader.eofCount()).isZero();

    pair.write.write(new byte[] {3, 4});
    pair.write.flush();
    assertThat(collectBytes(reader, 4, 5_000)).isEqualTo(new byte[] {1, 2, 3, 4});
    assertThat(reader.eofCount()).isZero();
    assertThat(reader.isRunning()).isTrue();
  }

  @Test
  void eofBoundaryLeavesNoPartialFrameWithDecoder() throws Exception {
    PcmDecoder decoder = new PcmDecoder();
    // 6 bytes = 1.5 stereo frames: L=100 R=200, then L=300 with R still pending
    byte[] partial = {(byte) 100, 0, (byte) 200, 0, 0x2c, 0x01};
    pair.write.write(partial);
    pair.write.flush();
    pair.write.close(); // EOF mid-frame
    await().atMost(BOUNDED).until(() -> reader.eofCount() >= 1);

    List<Short> shorts = drainDecoded(decoder, reader, 5_000);
    assertThat(shorts).containsExactly((short) 100, (short) 200);
    assertThat(decoder.pendingBytes()).as("1–3 trailing bytes preserved across EOF").isEqualTo(2);

    // writer reopens: the two trailing bytes complete the straddling frame
    try (OutputStream reopened = new FileOutputStream(fifo.toFile())) {
      reopened.write(new byte[] {(byte) 0x90, 0x01});
      reopened.flush();
      List<Short> completed = drainDecoded(decoder, reader, 5_000);
      assertThat(completed).containsExactly((short) 300, (short) 400);
      assertThat(decoder.pendingBytes()).isZero();
    }
  }

  @Test
  void closeTerminatesReaderAndIsIdempotent() throws Exception {
    assertThat(reader.isRunning()).isTrue();

    reader.close();
    reader.close(); // idempotent

    assertThat(reader.isRunning()).isFalse();
    assertThat(reader.take(200)).isNull(); // no events produced after close
  }

  // ------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------

  /**
   * Opens both ends of a fresh FIFO with the blocking-open rendezvous: the
   * read end opens on a helper thread, the write end opens on the caller's
   * thread — the two blocking opens complete each other in the kernel.
   */
  private static Rendezvous openRendezvous(Path fifo) throws Exception {
    CountDownLatch readOpened = new CountDownLatch(1);
    AtomicReference<InputStream> readRef = new AtomicReference<>();
    AtomicReference<Throwable> openError = new AtomicReference<>();
    Thread opener = new Thread(() -> {
      try {
        readRef.set(new FileInputStream(fifo.toFile()));
      } catch (Throwable t) {
        openError.set(t);
      } finally {
        readOpened.countDown();
      }
    }, "test-fifo-read-open");
    opener.setDaemon(true);
    opener.start();

    OutputStream write = new FileOutputStream(fifo.toFile());
    if (!readOpened.await(10, TimeUnit.SECONDS)) {
      write.close();
      throw new IOException("read-end open rendezvous timed out for " + fifo);
    }
    Throwable error = openError.get();
    if (error != null) {
      write.close();
      throw new IOException("read-end open failed for " + fifo, error);
    }
    return new Rendezvous(readRef.get(), write);
  }

  /** Read + write ends of one FIFO. */
  private record Rendezvous(InputStream read, OutputStream write) {}

  /**
   * Collects exactly {@code wantBytes} PCM bytes from Data events; stops
   * early on Eof (all Data events precede Eof in the FIFO queue). Returns
   * whatever arrived by the deadline — callers assert on the length.
   */
  private static byte[] collectBytes(FifoReader reader, int wantBytes, long timeoutMs)
      throws InterruptedException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    while (out.size() < wantBytes) {
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        break;
      }
      Event event = reader.take(Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remaining)));
      if (event instanceof Event.Data data) {
        out.writeBytes(data.bytes());
      } else if (event instanceof Event.Eof) {
        break;
      }
    }
    return out.toByteArray();
  }

  /** Waits for the requested EOF transition and removes its event from the queue. */
  private static void awaitAndConsumeEof(FifoReader reader, long expectedCount)
      throws InterruptedException {
    await().atMost(BOUNDED).until(() -> reader.eofCount() >= expectedCount);
    long deadline = System.nanoTime() + BOUNDED.toNanos();
    while (true) {
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        throw new AssertionError("EOF event was not delivered");
      }
      Event event = reader.take(Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remaining)));
      if (event instanceof Event.Eof) {
        return;
      }
    }
  }

  /** Drains all retained data in FIFO order until the writer's EOF event. */
  private static byte[] drainBytesUntilEof(FifoReader reader, long timeoutMs)
      throws InterruptedException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    while (true) {
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        break;
      }
      Event event = reader.take(Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remaining)));
      if (event == null || event instanceof Event.Eof) {
        break;
      }
      out.writeBytes(((Event.Data) event).bytes());
    }
    return out.toByteArray();
  }

  /**
   * Drains the reader until Eof (or deadline), decoding every Data event with
   * the given decoder. Eof is guaranteed to be enqueued after all Data events.
   */
  private static List<Short> drainDecoded(PcmDecoder decoder, FifoReader reader, long timeoutMs)
      throws InterruptedException {
    List<Short> shorts = new ArrayList<>();
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    while (true) {
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        break;
      }
      Event event = reader.take(Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remaining)));
      if (event == null) {
        break;
      }
      if (event instanceof Event.Data data) {
        for (short s : decoder.decode(data.bytes())) {
          shorts.add(s);
        }
      } else if (event instanceof Event.Eof) {
        break;
      }
    }
    return shorts;
  }

}
